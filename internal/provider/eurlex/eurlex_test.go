package eurlex

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/model"
	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/provider"
)

func TestEURLexLifecycleAndMarkdownLinks(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/sparql" {
			_ = json.NewEncoder(response).Encode(map[string]any{"results": map[string]any{"bindings": []any{map[string]any{"celex": map[string]string{"value": "http://publications.europa.eu/resource/celex/32016R0679"}, "expr": map[string]string{"value": "http://publications.europa.eu/resource/cellar/expression.0001"}, "title": map[string]string{"value": "Data protection regulation"}, "date": map[string]string{"value": "2016-04-27"}}}}})
			return
		}
		if request.URL.Path == "/celex/expression.0001" {
			if accept := request.Header.Get("Accept"); accept != "text/html" {
				t.Errorf("Accept=%q, want text/html", accept)
			}
			response.Header().Set("Content-Type", "application/xhtml+xml")
			_, _ = response.Write([]byte(`<html><body><h1>Data protection regulation</h1><p>Protects personal data; see <a href="https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32002L0058">Directive</a>.</p><table><tr><th>Article</th><th>Subject</th></tr><tr><td>1</td><td>Purpose</td></tr></table></body></html>`))
			return
		}
		http.NotFound(response, request)
	}))
	defer server.Close()
	backend := NewWithURLs(server.URL+"/sparql", server.URL+"/celex")
	available, err := backend.Discover(context.Background(), "EU law", false)
	if err != nil || len(available) != 1 || len(available[0].Variants) != 24 {
		t.Fatalf("discover: %+v %v", available, err)
	}
	release, err := backend.Latest(context.Background(), datasetID, "en")
	if err != nil {
		t.Fatal(err)
	}
	stage := t.TempDir()
	manifest, err := backend.Acquire(context.Background(), datasetID, "en", release, stage, "", func(string, int64, int64, string, float64, string) {})
	if err != nil {
		t.Fatal(err)
	}
	corpus, err := backend.OpenCorpus(stage, manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = corpus.Close() }()
	var record provider.Record
	if err := corpus.ScanBodies(context.Background(), "", provider.ScanOptions{}, func(item provider.Record, _ provider.ScanPosition) error { record = item; return nil }); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(record.Body, "| Article | Subject |") || !strings.Contains(record.Body, "knowledge-read://read?dataset=eurlex-in-force&id=32002L0058") {
		t.Fatalf("markdown=%s", record.Body)
	}
	document, err := corpus.Read(context.Background(), record, model.ReadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if document.Format != "markdown" {
		t.Fatalf("format=%q", document.Format)
	}
}

func TestEURLexRetriesTransientCatalogFailure(t *testing.T) {
	t.Parallel()
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		attempts++
		if request.Method != http.MethodPost {
			t.Errorf("method=%s, want POST", request.Method)
		}
		if err := request.ParseForm(); err != nil {
			t.Fatal(err)
		}
		if query := request.Form.Get("query"); strings.Contains(query, "OFFSET") {
			t.Errorf("query uses offset pagination: %s", query)
		}
		if attempts == 1 {
			response.Header().Set("Retry-After", "0")
			http.Error(response, "temporary failure", http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(response).Encode(map[string]any{"results": map[string]any{"bindings": []any{map[string]any{"celex": map[string]string{"value": "http://publications.europa.eu/resource/celex/32016R0679"}, "expr": map[string]string{"value": "http://publications.europa.eu/resource/cellar/expression.0001"}, "title": map[string]string{"value": "Data protection regulation"}}}}})
	}))
	defer server.Close()

	backend := NewWithURLs(server.URL, server.URL)
	latest, err := backend.Latest(context.Background(), datasetID, "en")
	if err != nil {
		t.Fatal(err)
	}
	resolved, ok := latest.Value.(*release)
	if !ok || len(resolved.Entries) != 1 || attempts != 2 {
		t.Fatalf("release=%+v attempts=%d", latest, attempts)
	}
}
