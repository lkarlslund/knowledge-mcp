package mcpserver

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/lkarlslund/knowledge-mcp/internal/model"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type fakeService struct{}

type recordingService struct {
	fakeService
	readOptions model.ReadOptions
	readRef     string
}

func (service *recordingService) Read(_ context.Context, dataset, title, id string, options model.ReadOptions) (model.Document, error) {
	service.readOptions = options
	return model.Document{Dataset: dataset, Title: title, ID: id}, nil
}
func (service *recordingService) ReadReference(_ context.Context, ref string, options model.ReadOptions) (model.Document, error) {
	service.readRef, service.readOptions = ref, options
	return model.Document{Ref: ref}, nil
}

func (fakeService) ListAvailable(context.Context, string, int, int, bool) (model.AvailableResult, error) {
	return model.AvailableResult{Datasets: []model.AvailableDataset{{Provider: "wikimedia", ID: "closedwiki", DisplayName: "Archived Wiki", Closed: true, Available: true}}, Total: 1}, nil
}
func (fakeService) ListLocalSummary() ([]model.LocalDatasetSummary, error) {
	return []model.LocalDatasetSummary{{Dataset: "testwiki", Name: "Test Wikipedia", Description: "A test-language general-purpose encyclopedia.", Project: "wikipedia", ContentType: "general-purpose encyclopedia", Profile: model.DatasetProfile{Topics: []string{"encyclopedia"}, GeographicScope: []string{"global"}, TimeCoverage: "current snapshot", DocumentTypes: []string{"articles"}, UpdateCadence: "monthly", SourceFeatures: []string{"links"}}, OnlineSourceURL: "https://test.wikipedia.org", Language: model.Language{Code: "test", Name: "Test"}, SourceDocuments: 40, IndexedDocuments: 42, SearchAvailable: true, SearchCapabilities: []string{"title"}}}, nil
}
func (fakeService) OperationalStatus() model.OperationalStatus { return model.OperationalStatus{} }

func TestLocalDatasetSummariesExposeSelectionMetadata(t *testing.T) {
	t.Parallel()
	backend := fakeService{}
	summaries, err := backend.ListLocalSummary()
	if err != nil {
		t.Fatal(err)
	}
	if len(summaries) != 1 || summaries[0].Name != "Test Wikipedia" || summaries[0].SourceDocuments != 40 || summaries[0].IndexedDocuments != 42 || summaries[0].OnlineSourceURL != "https://test.wikipedia.org" {
		t.Fatalf("unexpected summaries: %#v", summaries)
	}
}
func (fakeService) Submit(dataset, variant, kind string) (model.Job, error) {
	return model.Job{ID: "job", Dataset: dataset, Variant: variant, Kind: kind}, nil
}
func (fakeService) Job(id, wiki string) (model.Job, error) {
	return model.Job{ID: id, Dataset: wiki}, nil
}
func (fakeService) JobAction(id, action string) (model.Job, error) {
	return model.Job{ID: id, State: action}, nil
}
func (fakeService) Search(_ context.Context, wiki, query string, options model.SearchOptions) (model.SearchResult, error) {
	return model.SearchResult{Dataset: wiki}, nil
}
func (fakeService) Read(_ context.Context, dataset, title, id string, options model.ReadOptions) (model.Document, error) {
	return model.Document{Dataset: dataset, Title: title, ID: id}, nil
}
func (fakeService) ReadReference(_ context.Context, ref string, _ model.ReadOptions) (model.Document, error) {
	return model.Document{Ref: ref}, nil
}

func TestToolsAndStructuredCall(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	server := New(fakeService{})
	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	if _, err := server.Connect(ctx, serverTransport, nil); err != nil {
		t.Fatal(err)
	}
	client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil)
	session, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = session.Close() }()
	tools, err := session.ListTools(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	type annotationExpectation struct {
		found       bool
		readOnly    bool
		destructive *bool
		openWorld   bool
	}
	boolPointer := func(value bool) *bool { return &value }
	want := map[string]annotationExpectation{
		"knowledge_list_available": {readOnly: true, openWorld: true},
		"knowledge_list_local":     {readOnly: true, openWorld: false},
		"knowledge_status":         {readOnly: true, openWorld: false},
		"knowledge_download":       {destructive: boolPointer(false), openWorld: true},
		"knowledge_update":         {destructive: boolPointer(true), openWorld: true},
		"knowledge_job_status":     {readOnly: true, openWorld: false},
		"knowledge_job":            {destructive: boolPointer(true), openWorld: false},
		"knowledge_search":         {readOnly: true, openWorld: false},
		"knowledge_read":           {readOnly: true, openWorld: false},
	}
	for _, tool := range tools.Tools {
		expectation, ok := want[tool.Name]
		if !ok {
			continue
		}
		expectation.found = true
		want[tool.Name] = expectation
		if tool.Annotations == nil {
			t.Errorf("tool %s has no annotations", tool.Name)
			continue
		}
		if tool.OutputSchema == nil {
			t.Errorf("tool %s has no output schema", tool.Name)
		}
		if tool.Name == "knowledge_list_available" {
			schemaJSON, marshalErr := json.Marshal(tool.OutputSchema)
			if marshalErr != nil || strings.Contains(string(schemaJSON), `"closed"`) || !strings.Contains(string(schemaJSON), `"display_name"`) {
				t.Errorf("knowledge_list_available exposes upstream state or lacks selection metadata: %s, %v", schemaJSON, marshalErr)
			}
		}
		if tool.Name == "knowledge_read" {
			schemaJSON, marshalErr := json.Marshal(tool.InputSchema)
			if marshalErr != nil || !strings.Contains(string(schemaJSON), `"oneOf"`) || !strings.Contains(string(schemaJSON), `"maximum":500000`) || strings.Contains(string(schemaJSON), `"format"`) {
				t.Errorf("knowledge_read schema lacks conditional/bound constraints: %s, %v", schemaJSON, marshalErr)
			}
			if !strings.Contains(tool.Description, "temporary ref") || !strings.Contains(tool.Description, "Legacy") {
				t.Errorf("knowledge_read description lacks reference guidance: %q", tool.Description)
			}
		}
		if tool.Annotations.ReadOnlyHint != expectation.readOnly {
			t.Errorf("tool %s readOnlyHint = %v, want %v", tool.Name, tool.Annotations.ReadOnlyHint, expectation.readOnly)
		}
		if tool.Annotations.OpenWorldHint == nil || *tool.Annotations.OpenWorldHint != expectation.openWorld {
			t.Errorf("tool %s openWorldHint = %v, want %v", tool.Name, tool.Annotations.OpenWorldHint, expectation.openWorld)
		}
		if expectation.destructive == nil {
			if tool.Annotations.DestructiveHint != nil {
				t.Errorf("tool %s destructiveHint = %v, want omitted", tool.Name, *tool.Annotations.DestructiveHint)
			}
		} else if tool.Annotations.DestructiveHint == nil || *tool.Annotations.DestructiveHint != *expectation.destructive {
			t.Errorf("tool %s destructiveHint = %v, want %v", tool.Name, tool.Annotations.DestructiveHint, *expectation.destructive)
		}
	}
	invalidJob, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "knowledge_job", Arguments: map[string]any{"job_id": "job", "action": "status"}})
	if err != nil {
		t.Fatal(err)
	}
	if !invalidJob.IsError {
		t.Fatalf("knowledge_job status action unexpectedly succeeded: %#v", invalidJob)
	}
	for name, expectation := range want {
		if !expectation.found {
			t.Errorf("tool %s was not registered", name)
		}
	}
	result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "knowledge_list_local", Arguments: map[string]any{}})
	if err != nil {
		t.Fatal(err)
	}
	if result.IsError || result.StructuredContent == nil {
		t.Fatalf("unexpected call result: %#v", result)
	}
	payload, err := json.Marshal(result.StructuredContent)
	if err != nil {
		t.Fatal(err)
	}
	text := string(payload)
	for _, field := range []string{`"description"`, `"content_type"`, `"language"`, `"online_source_url"`, `"source_documents"`, `"indexed_documents"`, `"search_available"`, `"search_capabilities"`, `"topics"`, `"geographic_scope"`, `"time_coverage"`, `"document_types"`, `"update_cadence"`, `"source_features"`} {
		if !strings.Contains(text, field) {
			t.Errorf("knowledge_list_local response lacks %s: %s", field, text)
		}
	}
	for _, field := range []string{`"dump_sha1"`, `"disk_bytes"`, `"body_index_version"`, `"closed"`, `"search_mode"`} {
		if strings.Contains(text, field) {
			t.Errorf("knowledge_list_local exposes operational field %s: %s", field, text)
		}
	}
	available, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "knowledge_list_available", Arguments: map[string]any{}})
	if err != nil {
		t.Fatal(err)
	}
	availablePayload, err := json.Marshal(available.StructuredContent)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(availablePayload), `"closed"`) || !strings.Contains(string(availablePayload), `"Archived Wiki"`) {
		t.Fatalf("knowledge_list_available payload = %s", availablePayload)
	}
}

func TestWikiReadAppliesAgentSizedDefaults(t *testing.T) {
	t.Parallel()
	service := &recordingService{}
	server := New(service)
	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	if _, err := server.Connect(context.Background(), serverTransport, nil); err != nil {
		t.Fatal(err)
	}
	client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil)
	session, err := client.Connect(context.Background(), clientTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = session.Close() }()
	result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "knowledge_read", Arguments: map[string]any{"dataset": "testwiki", "id": "7"}})
	if err != nil || result.IsError {
		t.Fatalf("knowledge_read = %#v, %v", result, err)
	}
	if service.readOptions.Format != "markdown" || service.readOptions.MaxChars != 50_000 || !service.readOptions.AlignBoundaries || service.readOptions.ReferenceBudgetChars != 10_000 || service.readOptions.ReferenceMaxChars != 4_000 || !service.readOptions.IncludeOutline {
		t.Fatalf("read options = %#v", service.readOptions)
	}
	result, err = session.CallTool(context.Background(), &mcp.CallToolParams{Name: "knowledge_read", Arguments: map[string]any{"ref": "r_123456"}})
	if err != nil || result.IsError || service.readRef != "r_123456" {
		payload, _ := json.Marshal(result)
		t.Fatalf("knowledge_read by ref = %s, %v; ref %q", payload, err, service.readRef)
	}
}

func TestHTTPHandlerIsStatelessAndRejectsCrossOriginRequests(t *testing.T) {
	t.Parallel()
	handler := httpHandler(fakeService{})

	get := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "http://127.0.0.1/mcp", nil)
	get.Host = "127.0.0.1"
	getResponse := httptest.NewRecorder()
	handler.ServeHTTP(getResponse, get)
	if getResponse.Code != http.StatusMethodNotAllowed {
		t.Fatalf("stateless GET status = %d, want 405", getResponse.Code)
	}

	crossOrigin := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "http://127.0.0.1/mcp", strings.NewReader(`{}`))
	crossOrigin.Host = "127.0.0.1"
	crossOrigin.Header.Set("Origin", "https://attacker.example")
	crossOriginResponse := httptest.NewRecorder()
	handler.ServeHTTP(crossOriginResponse, crossOrigin)
	if crossOriginResponse.Code != http.StatusForbidden {
		t.Fatalf("cross-origin POST status = %d, want 403", crossOriginResponse.Code)
	}
}

func TestPProfHandlerExposesGoroutineProfile(t *testing.T) {
	t.Parallel()
	mux := http.NewServeMux()
	registerPProf(mux)

	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "http://127.0.0.1/debug/pprof/goroutine?debug=1", nil)
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("pprof status = %d, want 200", response.Code)
	}
	if !strings.Contains(response.Body.String(), "goroutine profile") {
		t.Fatalf("pprof response lacks goroutine profile: %q", response.Body.String())
	}
}
