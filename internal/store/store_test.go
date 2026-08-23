package store

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	dsbzip2 "github.com/dsnet/compress/bzip2"
	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/model"
	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/wikimedia"
)

func TestBackgroundDownloadPublishesTitleThenBody(t *testing.T) {
	t.Parallel()
	xmlData := compress(t, []byte(`<mediawiki><page><title>Test Article</title><id>7</id><revision><id>70</id><timestamp>2026-08-01T00:00:00Z</timestamp><text>A remarkable capybara appears here.</text></revision></page></mediawiki>`))
	indexData := compress(t, []byte("0:7:Test Article\n"))
	dumpSHA, indexSHA := sha1String(xmlData), sha1String(indexData)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/backup-index-bydb.html":
			_, _ = fmt.Fprint(w, `<li>2026-08-04 00:00:00 <a href="testwiki/20260801">testwiki</a>: <span class='done'>Dump complete</span></li>`)
		case "/testwiki/20260801/dumpstatus.json":
			_, _ = fmt.Fprintf(w, `{"jobs":{"articlesmultistreamdump":{"status":"done","files":{"testwiki-20260801-pages-articles-multistream.xml.bz2":{"size":%d,"url":"/dump","sha1":"%s"},"testwiki-20260801-pages-articles-multistream-index.txt.bz2":{"size":%d,"url":"/index","sha1":"%s"}}}}}`, len(xmlData), dumpSHA, len(indexData), indexSHA)
		case "/dump":
			_, _ = w.Write(xmlData)
		case "/index":
			_, _ = w.Write(indexData)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	backend, err := Open(t.TempDir(), wikimedia.NewClientWithBaseURL(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	job, err := backend.Submit("testwiki", "download")
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := backend.Submit("testwiki", "download")
	if err != nil {
		t.Fatal(err)
	}
	if duplicate.ID != job.ID {
		t.Fatalf("duplicate job ID = %q, want %q", duplicate.ID, job.ID)
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		status, statusErr := backend.Job(job.ID, "")
		if statusErr != nil {
			t.Fatal(statusErr)
		}
		if status.State == model.StateReady {
			break
		}
		if status.State == model.StateFailed {
			t.Fatalf("job failed: %s", status.Error)
		}
		if time.Now().After(deadline) {
			t.Fatalf("job did not complete; last status: %#v", status)
		}
		time.Sleep(10 * time.Millisecond)
	}

	locals, err := backend.ListLocal()
	if err != nil {
		t.Fatal(err)
	}
	if len(locals) != 1 || !locals[0].TitleReady || !locals[0].BodyReady {
		t.Fatalf("unexpected local state: %#v", locals)
	}
	result, err := backend.Search("testwiki", "capybara", 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if result.SearchMode != "full_text" || len(result.Hits) != 1 {
		t.Fatalf("unexpected search result: %#v", result)
	}
	page, err := backend.Read("testwiki", "Test Article", 0, "text", 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if page.PageID != 7 || page.Content != "A remarkable capybara appears here." {
		t.Fatalf("unexpected page: %#v", page)
	}
}

func TestMigrateLegacyStagePreservesPartialDownloads(t *testing.T) {
	t.Parallel()
	stage := t.TempDir()
	partsDir := filepath.Join(stage, "parts")
	if err := os.Mkdir(partsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	wantDump := []byte("partial dump")
	wantIndex := []byte("complete index")
	if err := os.WriteFile(filepath.Join(stage, "dump.xml.bz2"), wantDump, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stage, "multistream-index.txt.bz2"), wantIndex, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := migrateLegacyStage(stage, partsDir); err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string][]byte{"000.dump.bz2": wantDump, "000.index.bz2": wantIndex} {
		got, err := os.ReadFile(filepath.Join(partsDir, name))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("%s = %q, want %q", name, got, want)
		}
	}
}

func compress(t *testing.T, data []byte) []byte {
	t.Helper()
	var out bytes.Buffer
	w, err := dsbzip2.NewWriter(&out, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func sha1String(data []byte) string {
	sum := sha1.Sum(data)
	return hex.EncodeToString(sum[:])
}
