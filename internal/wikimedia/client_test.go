package wikimedia

import (
	"bytes"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	dsbzip2 "github.com/dsnet/compress/bzip2"
	"github.com/lkarlslund/knowledge-mcp/internal/model"
)

func TestCatalogMetadataAndDownload(t *testing.T) {
	t.Parallel()
	payload := []byte("compressed fixture")
	hash := sha256.Sum256(payload)
	sha := hex.EncodeToString(hash[:])
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/other/mediawiki_content_current/":
			_, _ = fmt.Fprint(w, `<a href="testwiki/">testwiki/</a> 04-Aug-2026 00:00 -`)
		case "/sitematrix":
			_, _ = fmt.Fprint(w, `{"sitematrix":{"count":1,"0":{"code":"test","name":"Testish","localname":"Test","dir":"ltr","site":[{"url":"https://test.wikipedia.org","dbname":"testwiki","code":"wiki","sitename":"Wikipedia"}]}}}`)
		case "/other/mediawiki_content_current/testwiki/2026-08-01/xml/bzip2/SHA256SUMS":
			_, _ = fmt.Fprintf(w, "%s  testwiki-2026-08-01-p1p9.xml.bz2\n", sha)
		case "/other/mediawiki_content_current/testwiki/2026-08-01/xml/bzip2/":
			_, _ = fmt.Fprintf(w, `<a href="testwiki-2026-08-01-p1p9.xml.bz2">testwiki-2026-08-01-p1p9.xml.bz2</a> 01-Aug-2026 00:00 %d`, len(payload))
		case "/other/mediawiki_content_current/testwiki/2026-08-01/xml/bzip2/testwiki-2026-08-01-p1p9.xml.bz2":
			if r.Header.Get("Range") == "bytes=4-" {
				w.Header().Set("Content-Range", fmt.Sprintf("bytes 4-%d/%d", len(payload)-1, len(payload)))
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write(payload[4:])
				return
			}
			_, _ = w.Write(payload)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := NewClientWithBaseURL(server.URL)
	result, err := client.ListAvailable(context.Background(), "test", 0, 20, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Datasets) != 1 || !result.Datasets[0].Available || result.Datasets[0].RawHash != sha || result.Datasets[0].DisplayName != "Test Wikipedia" {
		t.Fatalf("unexpected catalog: %#v", result)
	}
	destination := filepath.Join(t.TempDir(), "partial")
	if err := os.WriteFile(destination, payload[:4], 0o644); err != nil {
		t.Fatal(err)
	}
	metadata, err := client.LatestMetadata(context.Background(), "testwiki")
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Download(context.Background(), metadata.Parts[0].Raw, destination, func(int64, int64, float64) {}); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(destination)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(payload) {
		t.Fatalf("download = %q, want %q", got, payload)
	}
}

func TestEmptyExportRootFallsBackToUnavailableSiteMatrixCatalog(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/other/mediawiki_content_current/":
			_, _ = fmt.Fprint(w, "<html><body>No completed exports</body></html>")
		case "/sitematrix":
			_, _ = fmt.Fprint(w, `{"sitematrix":{"count":1,"0":{"code":"da","name":"dansk","localname":"Danish","dir":"ltr","site":[{"url":"https://da.wikipedia.org","dbname":"dawiki","code":"wiki","sitename":"Wikipedia"}]}}}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := NewClientWithBaseURL(server.URL)
	result, err := client.ListAvailable(context.Background(), "dawiki", 0, -1, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Datasets) != 1 || result.Datasets[0].Available || result.Datasets[0].ReleaseDate != "" {
		t.Fatalf("fallback=%+v", result.Datasets)
	}
}

func TestFullCatalogIsCachedAndFilteredLocally(t *testing.T) {
	t.Parallel()
	var catalogRequests, matrixRequests, metadataRequests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/other/mediawiki_content_current/":
			catalogRequests.Add(1)
			_, _ = fmt.Fprint(w, `<a href="abstractwiki/">abstractwiki/</a> 01-Aug-2026 00:00 -
<a href="dawiki/">dawiki/</a> 01-Aug-2026 00:00 -`)
		case "/sitematrix":
			matrixRequests.Add(1)
			_, _ = fmt.Fprint(w, `{"sitematrix":{"count":2,"0":{"code":"da","name":"dansk","localname":"Danish","dir":"ltr","site":[{"url":"https://da.wikipedia.org","dbname":"dawiki","code":"wiki","sitename":"Wikipedia"}]},"specials":[{"url":"https://abstract.wikipedia.org","dbname":"abstractwiki","code":"abstract","lang":"en","sitename":"Abstract Wikipedia"}]}}`)
		default:
			metadataRequests.Add(1)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := NewClientWithBaseURL(server.URL)
	all, err := client.ListAvailable(context.Background(), "", 0, -1, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(all.Datasets) != 2 || all.Datasets[0].DisplayName == "" || all.Datasets[1].ContentType == "" {
		t.Fatalf("full catalog = %#v", all.Datasets)
	}
	filtered, err := client.ListAvailable(context.Background(), "encyclopedia", 0, -1, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(filtered.Datasets) != 2 {
		t.Fatalf("content filter returned %#v", filtered.Datasets)
	}
	if catalogRequests.Load() != 1 || matrixRequests.Load() != 1 || metadataRequests.Load() != 0 {
		t.Fatalf("requests: catalog=%d matrix=%d metadata=%d", catalogRequests.Load(), matrixRequests.Load(), metadataRequests.Load())
	}
	client.mu.Lock()
	client.cached, client.sitesCached = time.Now().Add(-25*time.Hour), time.Now().Add(-25*time.Hour)
	client.mu.Unlock()
	if _, err := client.ListAvailable(context.Background(), "", 0, -1, false); err != nil {
		t.Fatal(err)
	}
	if catalogRequests.Load() != 2 || matrixRequests.Load() != 2 {
		t.Fatalf("stale caches were not refreshed: catalog=%d matrix=%d", catalogRequests.Load(), matrixRequests.Load())
	}
}

func TestSiteMetadataIncludesSelectionFields(t *testing.T) {
	t.Parallel()
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/sitematrix":
			_, _ = fmt.Fprintf(w, `{"sitematrix":{"count":1,"0":{"code":"da","name":"dansk","localname":"Danish","dir":"ltr","site":[{"url":%q,"dbname":"dawiki","code":"wiki","sitename":"Wikipedia"}]}}}`, server.URL)
		case "/w/api.php":
			_, _ = fmt.Fprint(w, `{"query":{"general":{"lang":"da"},"statistics":{"articles":321},"rightsinfo":{"text":"CC BY-SA","url":"https://example.test/license"}}}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := NewClientWithBaseURL(server.URL)
	metadata := client.SiteMetadata(context.Background(), "dawiki")
	if metadata.Name != "Danish Wikipedia" || metadata.Project != "wikipedia" || metadata.Language.LocalName != "dansk" || metadata.SourceDocuments != 321 || metadata.OnlineSourceURL != server.URL || metadata.License != "CC BY-SA" {
		t.Fatalf("site metadata = %#v", metadata)
	}
}

func TestReadDumpSiteMetadata(t *testing.T) {
	t.Parallel()
	var compressed bytes.Buffer
	writer, err := dsbzip2.NewWriter(&compressed, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte(`<mediawiki xml:lang="da"><siteinfo><sitename>Wikipedia</sitename><dbname>dawiki</dbname><base>https://da.wikipedia.org/wiki/Forside</base></siteinfo><page>`)); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "dump.bz2")
	if err := os.WriteFile(path, compressed.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	metadata := ReadDumpSiteMetadata(context.Background(), "dawiki", path)
	if metadata.Name != "Wikipedia" || metadata.Language.Code != "da" || metadata.Project != "wikipedia" || metadata.OnlineSourceURL != "https://da.wikipedia.org" {
		t.Fatalf("dump site metadata = %#v", metadata)
	}
}

func TestValidWikiName(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"enwiki", "enwiktionary", "commonswiki", "be_x_oldwiki"} {
		if !ValidWikiName(name) {
			t.Errorf("ValidWikiName(%q) = false", name)
		}
	}
	for _, name := range []string{"../enwiki", "ENWIKI", "example.com", "wiki"} {
		if ValidWikiName(name) {
			t.Errorf("ValidWikiName(%q) = true", name)
		}
	}
}

func TestMultipartMetadataIsOrderedAndSummed(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/other/mediawiki_content_current/":
			_, _ = fmt.Fprint(w, `<a href="enwiki/">enwiki/</a> 01-Aug-2026 00:00 -`)
		case "/other/mediawiki_content_current/enwiki/2026-08-01/xml/bzip2/SHA256SUMS":
			_, _ = fmt.Fprint(w, strings.Repeat("a", 64)+"  enwiki-2026-08-01-p20p29.xml.bz2\n"+strings.Repeat("b", 64)+"  enwiki-2026-08-01-p1p19.xml.bz2\n")
		case "/other/mediawiki_content_current/enwiki/2026-08-01/xml/bzip2/":
			_, _ = fmt.Fprint(w, `<a href="enwiki-2026-08-01-p20p29.xml.bz2">enwiki-2026-08-01-p20p29.xml.bz2</a> 01-Aug-2026 00:00 200
<a href="enwiki-2026-08-01-p1p19.xml.bz2">enwiki-2026-08-01-p1p19.xml.bz2</a> 01-Aug-2026 00:00 100`)
		case "/other/mediawiki_content_current/enwiki/2026-08-01/xml/bzip2/enwiki-2026-08-01-p1p19.xml.bz2":
			w.Header().Set("Content-Length", "100")
		case "/other/mediawiki_content_current/enwiki/2026-08-01/xml/bzip2/enwiki-2026-08-01-p20p29.xml.bz2":
			w.Header().Set("Content-Length", "200")
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := NewClientWithBaseURL(server.URL)
	result, err := client.ListAvailable(context.Background(), "enwiki", 0, 20, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Datasets) != 1 {
		t.Fatalf("wikis = %#v", result.Datasets)
	}
	wiki := result.Datasets[0]
	if !wiki.Available || wiki.PartCount != 2 || wiki.RawSize != 300 || wiki.ProviderMetadataSize != 0 || wiki.Fingerprint == "" {
		t.Fatalf("unexpected multipart wiki: %#v", wiki)
	}
	metadata, err := client.LatestMetadata(context.Background(), "enwiki")
	if err != nil {
		t.Fatal(err)
	}
	if metadata.Parts[0].Key != "enwiki-2026-08-01-p1p19.xml.bz2" || metadata.Parts[1].Key != "enwiki-2026-08-01-p20p29.xml.bz2" {
		t.Fatalf("parts are not ordered: %#v", metadata.Parts)
	}
}

func TestParallelDownloadResumesExistingPrefix(t *testing.T) {
	t.Parallel()
	payload := bytes.Repeat([]byte("parallel-range-fixture-"), 50_000)
	hash := sha1.Sum(payload)
	sha := hex.EncodeToString(hash[:])
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var start, end int64
		if _, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &start, &end); err != nil || start < 0 || end < start || end >= int64(len(payload)) {
			http.Error(w, "bad range", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		requests.Add(1)
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(payload)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(payload[start : end+1])
	}))
	defer server.Close()

	destination := filepath.Join(t.TempDir(), "parallel")
	prefix := int64(12_345)
	if err := os.WriteFile(destination, payload[:prefix], 0o644); err != nil {
		t.Fatal(err)
	}
	file := model.FileMetadata{URL: server.URL + "/file", Size: int64(len(payload)), SHA1: sha}
	client := NewClientWithBaseURL(server.URL, 3)
	if err := client.downloadParallel(context.Background(), file, destination, destination+".resume.json", prefix, func(int64, int64, float64) {}); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(destination)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("parallel download content differs")
	}
	if requests.Load() != 3 {
		t.Fatalf("range requests = %d, want 3", requests.Load())
	}
	if _, err := os.Stat(destination + ".resume.json"); !os.IsNotExist(err) {
		t.Fatalf("resume state remains after verification: %v", err)
	}
}
