package dashboard

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/model"
)

type fakeService struct {
	submitted string
	updates   <-chan struct{}
	listCalls atomic.Int64
}

func (f *fakeService) ListAvailable(context.Context, string, int, int, bool) (model.AvailableResult, error) {
	return model.AvailableResult{Datasets: []model.AvailableDataset{{ID: "testwiki", Available: true}}}, nil
}
func (f *fakeService) ListLocal() ([]model.LocalDataset, error) {
	f.listCalls.Add(1)
	return []model.LocalDataset{{Manifest: model.Manifest{Dataset: "testwiki", TitleReady: true}}}, nil
}
func (f *fakeService) ListUpgrades(context.Context) ([]model.AvailableDataset, error) {
	return []model.AvailableDataset{{ID: "testwiki", Installed: true, UpdateAvailable: true}}, nil
}
func (f *fakeService) ListJobs() []model.Job { return []model.Job{{ID: "job-1"}} }
func (f *fakeService) Submit(dataset, variant, kind string) (model.Job, error) {
	f.submitted = dataset + "/" + kind
	return model.Job{ID: "job-2", Dataset: dataset, Kind: kind}, nil
}
func (f *fakeService) DeleteDataset(dataset string) error {
	f.submitted = dataset + "/delete"
	return nil
}
func (f *fakeService) JobAction(id, action string) (model.Job, error) {
	f.submitted = id + "/" + action
	return model.Job{ID: id, State: action}, nil
}
func (f *fakeService) Subscribe(ctx context.Context) <-chan struct{} {
	if f.updates != nil {
		return f.updates
	}
	updates := make(chan struct{})
	go func() {
		<-ctx.Done()
		close(updates)
	}()
	return updates
}

func TestWebSocketCoalescesBurstUpdates(t *testing.T) {
	t.Parallel()
	updates := make(chan struct{}, 20)
	service := &fakeService{updates: updates}
	server := httptest.NewServer(Handler(service))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	connection, response, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/api/dashboard/events", nil)
	if response != nil && response.Body != nil {
		defer func() { _ = response.Body.Close() }()
	}
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connection.CloseNow() }()
	var snapshot stateSnapshot
	if err := wsjson.Read(ctx, connection, &snapshot); err != nil {
		t.Fatal(err)
	}
	for range 10 {
		updates <- struct{}{}
	}
	if err := wsjson.Read(ctx, connection, &snapshot); err != nil {
		t.Fatal(err)
	}
	if calls := service.listCalls.Load(); calls != 2 {
		t.Fatalf("ListLocal calls after update burst = %d, want 2", calls)
	}
}

func TestDashboardAndMaintenanceAPI(t *testing.T) {
	t.Parallel()
	service := &fakeService{}
	handler := Handler(service)
	ctx := context.Background()

	page := httptest.NewRecorder()
	handler.ServeHTTP(page, httptest.NewRequestWithContext(ctx, http.MethodGet, "/", nil))
	if page.Code != http.StatusOK || !strings.Contains(page.Body.String(), "Knowledge Dataset MCP") || !strings.Contains(page.Body.String(), `href="https://github.com/lkarlslund/wikipedia-multistream-mcp"`) || !strings.Contains(page.Body.String(), `aria-label="View Knowledge Dataset MCP on GitHub"`) || strings.Contains(page.Body.String(), "<th>Upgrade</th>") || !strings.Contains(page.Body.String(), "Total documents") || !strings.Contains(page.Body.String(), "bi-trash3") || !strings.Contains(page.Body.String(), `id="onlineDatasetsModal"`) || !strings.Contains(page.Body.String(), "limit=-1") || !strings.Contains(page.Body.String(), "All Languages") || !strings.Contains(page.Body.String(), "hideInstalled") || !strings.Contains(page.Body.String(), "filteredAvailable") || !strings.Contains(page.Body.String(), "provider metadata") || !strings.Contains(page.Body.String(), "storage-column") {
		t.Fatalf("unexpected dashboard response: %d %q", page.Code, page.Body.String())
	}

	forbidden := httptest.NewRecorder()
	handler.ServeHTTP(forbidden, httptest.NewRequestWithContext(ctx, http.MethodPost, "/api/dashboard/dataset/testwiki/update", nil))
	if forbidden.Code != http.StatusForbidden {
		t.Fatalf("maintenance without header returned %d", forbidden.Code)
	}

	request := httptest.NewRequestWithContext(ctx, http.MethodPost, "/api/dashboard/dataset/testwiki/update", nil)
	request.Header.Set("X-Knowledge-MCP", "1")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || service.submitted != "testwiki/update" {
		t.Fatalf("maintenance response %d, submitted %q", response.Code, service.submitted)
	}
	deleteRequest := httptest.NewRequestWithContext(ctx, http.MethodPost, "/api/dashboard/dataset/testwiki/delete", nil)
	deleteRequest.Header.Set("X-Knowledge-MCP", "1")
	deleteResponse := httptest.NewRecorder()
	handler.ServeHTTP(deleteResponse, deleteRequest)
	if deleteResponse.Code != http.StatusOK || service.submitted != "testwiki/delete" {
		t.Fatalf("delete response %d, submitted %q", deleteResponse.Code, service.submitted)
	}
	jobRequest := httptest.NewRequestWithContext(ctx, http.MethodPost, "/api/dashboard/job/job-1/pause", nil)
	jobRequest.Header.Set("X-Knowledge-MCP", "1")
	jobResponse := httptest.NewRecorder()
	handler.ServeHTTP(jobResponse, jobRequest)
	if jobResponse.Code != http.StatusOK || service.submitted != "job-1/pause" {
		t.Fatalf("job action response %d, submitted %q", jobResponse.Code, service.submitted)
	}
}

func TestEmbeddedAssetsAndWebSocketSnapshot(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(Handler(&fakeService{}))
	defer server.Close()

	assetRequest, err := http.NewRequestWithContext(context.Background(), http.MethodGet, server.URL+"/assets/alpinejs-3.16.2.min.js", nil)
	if err != nil {
		t.Fatal(err)
	}
	assetResponse, err := http.DefaultClient.Do(assetRequest)
	if err != nil {
		t.Fatal(err)
	}
	_ = assetResponse.Body.Close()
	if assetResponse.StatusCode != http.StatusOK || !strings.Contains(assetResponse.Header.Get("Cache-Control"), "immutable") {
		t.Fatalf("asset response = %d, cache %q", assetResponse.StatusCode, assetResponse.Header.Get("Cache-Control"))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	connection, response, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/api/dashboard/events", nil)
	if response != nil && response.Body != nil {
		defer func() { _ = response.Body.Close() }()
	}
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connection.CloseNow() }()
	var snapshot stateSnapshot
	if err := wsjson.Read(ctx, connection, &snapshot); err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Local) != 1 || len(snapshot.Jobs) != 1 {
		t.Fatalf("unexpected snapshot: %#v", snapshot)
	}
}
