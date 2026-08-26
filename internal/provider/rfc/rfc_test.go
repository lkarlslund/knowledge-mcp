package rfc

import (
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/knowledgeindex"
	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/model"
	sourceprovider "github.com/lkarlslund/wikipedia-multistream-mcp/internal/provider"
)

const testRFCIndex = `<?xml version="1.0"?><rfc-index>
<rfc-entry><doc-id>RFC0001</doc-id><title>Host Software</title><author><name>S. Crocker</name></author><date><month>April</month><year>1969</year></date><format><file-format>TXT</file-format></format><obsoleted-by><doc-id>RFC9110</doc-id></obsoleted-by><current-status>UNKNOWN</current-status><stream>Legacy</stream></rfc-entry>
<rfc-entry><doc-id>RFC9110</doc-id><title>HTTP Semantics</title><author><name>R. Fielding</name></author><date><month>June</month><year>2022</year></date><format><file-format>TXT</file-format></format><keywords><kw>Hypertext Transfer Protocol</kw></keywords><obsoletes><doc-id>RFC0001</doc-id></obsoletes><is-also><doc-id>STD0097</doc-id></is-also><current-status>INTERNET STANDARD</current-status><stream>IETF</stream></rfc-entry>
</rfc-index>`

type rfcTransport struct {
	mu    sync.Mutex
	reads map[string]int
}

func (transport *rfcTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	transport.mu.Lock()
	transport.reads[request.URL.Path]++
	transport.mu.Unlock()
	content := map[string]string{
		"/rfc-index.xml":   testRFCIndex,
		"/rfc/rfc1.txt":    "1. Introduction\n\nThe host software defined an early protocol. See RFC 9110.\n",
		"/rfc/rfc9110.txt": "1. Introduction\n\nHTTP semantics define methods, status codes, and field values.\n",
	}[request.URL.Path]
	status := http.StatusOK
	if content == "" {
		status = http.StatusNotFound
	}
	return &http.Response{StatusCode: status, Status: http.StatusText(status), Header: make(http.Header), Body: io.NopCloser(strings.NewReader(content)), Request: request}, nil
}

func TestRFCProviderLifecycleAndOpaqueIDs(t *testing.T) {
	transport := &rfcTransport{reads: make(map[string]int)}
	provider := NewWithBaseURL("https://rfc.test")
	provider.http = &http.Client{Transport: transport}
	ctx := context.Background()

	available, err := provider.Discover(ctx, "internet standards", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(available) != 1 || available[0].ID != "rfc" || available[0].Provider != "rfc" || available[0].Description == "" || len(available[0].Variants) != 1 || available[0].Variants[0].ID != "text" {
		t.Fatalf("available = %#v", available)
	}
	release, err := provider.Latest(ctx, "rfc", "text")
	if err != nil {
		t.Fatal(err)
	}
	path := t.TempDir()
	manifest, err := provider.Acquire(ctx, "rfc", "text", release, path, "", func(string, int64, int64, string, float64, string) {})
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Provider != "rfc" || manifest.Dataset != "rfc" || manifest.Site.Description == "" || manifest.Site.SourceDocuments != 2 {
		t.Fatalf("manifest = %#v", manifest)
	}
	corpus, err := provider.OpenCorpus(path, manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = corpus.Close() }()
	count, err := knowledgeindex.BuildTitle(ctx, path, manifest.Fingerprint, corpus, sourceprovider.ScanOptions{}, func(uint64, int64, int64, knowledgeindex.BuildPhase) {})
	if err != nil || count != 2 {
		t.Fatalf("title index = %d, %v", count, err)
	}
	if err := os.Rename(filepath.Join(path, knowledgeindex.TitleDirectory+".building"), filepath.Join(path, knowledgeindex.TitleDirectory)); err != nil {
		t.Fatal(err)
	}
	if err := knowledgeindex.BuildBody(ctx, path, manifest.Fingerprint, corpus, sourceprovider.ScanOptions{}, func(int64, int64, knowledgeindex.BuildPhase) {}); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(path, knowledgeindex.BodyDirectory+".building"), filepath.Join(path, knowledgeindex.BodyDirectory)); err != nil {
		t.Fatal(err)
	}
	reader, err := knowledgeindex.Open(path, corpus, true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	result, err := reader.Search(ctx, "HTTP status codes", model.SearchOptions{Limit: 5, Snippets: true}, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) == 0 || result.Hits[0].ID != "9110" || result.Hits[0].NumericID != 0 {
		t.Fatalf("search = %#v", result)
	}
	identifierResult, err := reader.Search(ctx, "RFC 9110", model.SearchOptions{Limit: 5}, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(identifierResult.Hits) == 0 || identifierResult.Hits[0].ID != "9110" || identifierResult.Hits[0].Status != "internet standard" || len(identifierResult.Hits[0].Identifiers) < 2 {
		t.Fatalf("identifier search = %#v", identifierResult)
	}
	document, err := reader.Read(ctx, "", "1", model.ReadOptions{Format: "markdown", MaxChars: 10_000})
	if err != nil {
		t.Fatal(err)
	}
	if document.ID != "1" || !strings.Contains(document.Content, "knowledge-read://read?dataset=rfc&id=9110") || !strings.Contains(document.Content, "# RFC 1: Host Software") || !strings.Contains(document.Content, "**Lifecycle:** obsoleted") || len(document.Relationships) != 1 || document.Relationships[0].ID != "9110" {
		t.Fatalf("document = %#v", document)
	}
}

func TestRFCUpdateReusesExistingRawDocuments(t *testing.T) {
	transport := &rfcTransport{reads: make(map[string]int)}
	provider := NewWithBaseURL("https://rfc.test")
	provider.http = &http.Client{Transport: transport}
	release, err := provider.Latest(context.Background(), "rfc", "text")
	if err != nil {
		t.Fatal(err)
	}
	current, next := t.TempDir(), t.TempDir()
	if _, err := provider.Acquire(context.Background(), "rfc", "text", release, current, "", func(string, int64, int64, string, float64, string) {}); err != nil {
		t.Fatal(err)
	}
	before := transport.reads["/rfc/rfc9110.txt"]
	if _, err := provider.Acquire(context.Background(), "rfc", "text", release, next, current, func(string, int64, int64, string, float64, string) {}); err != nil {
		t.Fatal(err)
	}
	if got := transport.reads["/rfc/rfc9110.txt"]; got != before {
		t.Fatalf("existing RFC fetched again: %d -> %d", before, got)
	}
}
