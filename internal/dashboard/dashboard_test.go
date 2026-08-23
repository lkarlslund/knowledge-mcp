package dashboard

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/model"
)

type fakeService struct {
	submitted string
}

func (f *fakeService) ListAvailable(context.Context, string, int, int, bool) (model.AvailableResult, error) {
	return model.AvailableResult{Wikis: []model.OnlineWiki{{Name: "testwiki", Available: true}}}, nil
}
func (f *fakeService) ListLocal() ([]model.LocalWiki, error) {
	return []model.LocalWiki{{Manifest: model.Manifest{Wiki: "testwiki", TitleReady: true}}}, nil
}
func (f *fakeService) ListUpgrades(context.Context) ([]model.OnlineWiki, error) {
	return []model.OnlineWiki{{Name: "testwiki", Installed: true, UpdateAvailable: true}}, nil
}
func (f *fakeService) ListJobs() []model.Job { return []model.Job{{ID: "job-1"}} }
func (f *fakeService) Submit(wiki, kind string) (model.Job, error) {
	f.submitted = wiki + "/" + kind
	return model.Job{ID: "job-2", Wiki: wiki, Kind: kind}, nil
}
func (f *fakeService) JobAction(id, action string) (model.Job, error) {
	f.submitted = id + "/" + action
	return model.Job{ID: id, State: action}, nil
}

func TestDashboardAndMaintenanceAPI(t *testing.T) {
	t.Parallel()
	service := &fakeService{}
	handler := Handler(service)
	ctx := context.Background()

	page := httptest.NewRecorder()
	handler.ServeHTTP(page, httptest.NewRequestWithContext(ctx, http.MethodGet, "/", nil))
	if page.Code != http.StatusOK || !strings.Contains(page.Body.String(), "Wikipedia Multistream MCP") {
		t.Fatalf("unexpected dashboard response: %d %q", page.Code, page.Body.String())
	}

	forbidden := httptest.NewRecorder()
	handler.ServeHTTP(forbidden, httptest.NewRequestWithContext(ctx, http.MethodPost, "/api/dashboard/wiki/testwiki/update", nil))
	if forbidden.Code != http.StatusForbidden {
		t.Fatalf("maintenance without header returned %d", forbidden.Code)
	}

	request := httptest.NewRequestWithContext(ctx, http.MethodPost, "/api/dashboard/wiki/testwiki/update", nil)
	request.Header.Set("X-Wikipedia-MCP", "1")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || service.submitted != "testwiki/update" {
		t.Fatalf("maintenance response %d, submitted %q", response.Code, service.submitted)
	}
	jobRequest := httptest.NewRequestWithContext(ctx, http.MethodPost, "/api/dashboard/job/job-1/pause", nil)
	jobRequest.Header.Set("X-Wikipedia-MCP", "1")
	jobResponse := httptest.NewRecorder()
	handler.ServeHTTP(jobResponse, jobRequest)
	if jobResponse.Code != http.StatusOK || service.submitted != "job-1/pause" {
		t.Fatalf("job action response %d, submitted %q", jobResponse.Code, service.submitted)
	}
}
