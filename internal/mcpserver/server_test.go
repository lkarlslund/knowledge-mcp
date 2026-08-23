package mcpserver

import (
	"context"
	"encoding/json"
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
func (fakeService) Search(_ context.Context, wiki, query string, offset, limit int) (model.SearchResult, error) {
	return model.SearchResult{Wiki: wiki}, nil
}
func (fakeService) Read(_ context.Context, wiki, title string, pageID uint64, format string, offset, maxChars int) (model.Page, error) {
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
	want := map[string]bool{
		"wiki_list_available": false,
		"wiki_list_local":     false,
		"wiki_download":       false,
		"wiki_update":         false,
		"wiki_job_status":     false,
		"wiki_job":            false,
		"wiki_search":         false,
		"wiki_read":           false,
	}
	for _, tool := range tools.Tools {
		if _, ok := want[tool.Name]; ok {
			want[tool.Name] = true
		}
	}
	for name, found := range want {
		if !found {
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
