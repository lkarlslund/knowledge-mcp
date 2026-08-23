package mcpserver

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/model"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type fakeService struct{}

func (fakeService) ListAvailable(context.Context, string, int, int, bool) (model.AvailableResult, error) {
	return model.AvailableResult{}, nil
}
func (fakeService) ListLocalSummary() ([]model.LocalWikiSummary, error) {
	return []model.LocalWikiSummary{{Wiki: "testwiki", Name: "Test Wikipedia", Project: "wikipedia", ContentType: "general-purpose encyclopedia", OnlineSourceURL: "https://test.wikipedia.org", Language: model.WikiLanguage{Code: "test", Name: "Test"}, ContentArticles: 40, IndexedPages: 42, SearchMode: "title"}}, nil
}

func TestLocalWikiSummariesExposeSelectionMetadata(t *testing.T) {
	t.Parallel()
	backend := fakeService{}
	summaries, err := backend.ListLocalSummary()
	if err != nil {
		t.Fatal(err)
	}
	if len(summaries) != 1 || summaries[0].Name != "Test Wikipedia" || summaries[0].ContentArticles != 40 || summaries[0].IndexedPages != 42 || summaries[0].OnlineSourceURL != "https://test.wikipedia.org" {
		t.Fatalf("unexpected summaries: %#v", summaries)
	}
}
func (fakeService) Submit(wiki, kind string) (model.Job, error) {
	return model.Job{ID: "job", Wiki: wiki, Kind: kind}, nil
}
func (fakeService) Job(id, wiki string) (model.Job, error) {
	return model.Job{ID: id, Wiki: wiki}, nil
}
func (fakeService) JobAction(id, action string) (model.Job, error) {
	return model.Job{ID: id, State: action}, nil
}
func (fakeService) Search(_ context.Context, wiki, query string, options model.SearchOptions) (model.SearchResult, error) {
	return model.SearchResult{Wiki: wiki}, nil
}
func (fakeService) Read(_ context.Context, wiki, title string, pageID uint64, format string, offset, maxChars int, _ bool) (model.Page, error) {
	return model.Page{Wiki: wiki, Title: title, PageID: pageID}, nil
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
		"wiki_list_available": {readOnly: true, openWorld: true},
		"wiki_list_local":     {readOnly: true, openWorld: false},
		"wiki_download":       {destructive: boolPointer(false), openWorld: true},
		"wiki_update":         {destructive: boolPointer(true), openWorld: true},
		"wiki_job_status":     {readOnly: true, openWorld: false},
		"wiki_job":            {destructive: boolPointer(true), openWorld: false},
		"wiki_search":         {readOnly: true, openWorld: false},
		"wiki_read":           {readOnly: true, openWorld: false},
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
	invalidJob, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "wiki_job", Arguments: map[string]any{"job_id": "job", "action": "status"}})
	if err != nil {
		t.Fatal(err)
	}
	if !invalidJob.IsError {
		t.Fatalf("wiki_job status action unexpectedly succeeded: %#v", invalidJob)
	}
	for name, expectation := range want {
		if !expectation.found {
			t.Errorf("tool %s was not registered", name)
		}
	}
	result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "wiki_list_local", Arguments: map[string]any{}})
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
	for _, field := range []string{`"content_type"`, `"language"`, `"online_source_url"`, `"content_articles"`, `"indexed_pages"`, `"search_mode"`} {
		if !strings.Contains(text, field) {
			t.Errorf("wiki_list_local response lacks %s: %s", field, text)
		}
	}
	for _, field := range []string{`"dump_sha1"`, `"disk_bytes"`, `"body_index_version"`} {
		if strings.Contains(text, field) {
			t.Errorf("wiki_list_local exposes operational field %s: %s", field, text)
		}
	}
}

func TestHTTPHandlerIsStatelessAndRejectsCrossOriginRequests(t *testing.T) {
	t.Parallel()
	handler := httpHandler(fakeService{})

	get := httptest.NewRequest(http.MethodGet, "http://127.0.0.1/mcp", nil)
	get.Host = "127.0.0.1"
	getResponse := httptest.NewRecorder()
	handler.ServeHTTP(getResponse, get)
	if getResponse.Code != http.StatusMethodNotAllowed {
		t.Fatalf("stateless GET status = %d, want 405", getResponse.Code)
	}

	crossOrigin := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/mcp", strings.NewReader(`{}`))
	crossOrigin.Host = "127.0.0.1"
	crossOrigin.Header.Set("Origin", "https://attacker.example")
	crossOriginResponse := httptest.NewRecorder()
	handler.ServeHTTP(crossOriginResponse, crossOrigin)
	if crossOriginResponse.Code != http.StatusForbidden {
		t.Fatalf("cross-origin POST status = %d, want 403", crossOriginResponse.Code)
	}
}
