package store

import (
	"bytes"
	"context"
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
	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/wikiindex"
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
		if status.State == model.StateDownloaded {
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
	indexJob, err := backend.Job("", "testwiki")
	if err != nil {
		t.Fatal(err)
	}
	if indexJob.Kind != "index" || indexJob.ID == job.ID {
		t.Fatalf("automatic index job = %#v", indexJob)
	}
	for {
		status, statusErr := backend.Job(indexJob.ID, "")
		if statusErr != nil {
			t.Fatal(statusErr)
		}
		if status.State == model.StateReady {
			break
		}
		if status.State == model.StateFailed {
			t.Fatalf("index job failed: %s", status.Error)
		}
		if time.Now().After(deadline) {
			t.Fatalf("index job did not complete; last status: %#v", status)
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

func TestJobPauseResumeCancelAndRetry(t *testing.T) {
	t.Parallel()
	job := &model.Job{ID: "job-1", Wiki: "testwiki", Kind: "download", State: model.StateDownloading, CreatedAt: time.Now(), UpdatedAt: time.Now()}
	backend := &Store{
		root:          t.TempDir(),
		jobs:          map[string]*model.Job{job.ID: job},
		active:        map[string]string{job.Wiki: job.ID},
		downloadQueue: make(chan string, 4),
		indexQueue:    make(chan string, 4),
		running:       make(map[string]context.CancelFunc),
	}
	paused, err := backend.JobAction(job.ID, "pause")
	if err != nil || paused.State != model.StatePaused {
		t.Fatalf("pause = %#v, %v", paused, err)
	}
	backend.setJob(job.ID, model.StateDownloading, "downloading", 1, 2, "bytes", 1, "late progress", "")
	if afterProgress, _ := backend.Job(job.ID, ""); afterProgress.State != model.StatePaused {
		t.Fatalf("late progress overwrote pause: %#v", afterProgress)
	}
	resumed, err := backend.JobAction(job.ID, "resume")
	if err != nil || resumed.State != model.StateQueued {
		t.Fatalf("resume = %#v, %v", resumed, err)
	}
	if queued := <-backend.downloadQueue; queued != job.ID {
		t.Fatalf("queued ID = %q", queued)
	}
	canceled, err := backend.JobAction(job.ID, "cancel")
	if err != nil || canceled.State != model.StateCanceled {
		t.Fatalf("cancel = %#v, %v", canceled, err)
	}
	retried, err := backend.JobAction(job.ID, "retry")
	if err != nil || retried.State != model.StateQueued {
		t.Fatalf("retry = %#v, %v", retried, err)
	}
}

func TestLocalStorageBreakdown(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	files := map[string]int{
		"parts/000.dump.bz2":                      11,
		"parts/000.index.bz2":                     7,
		wikiindex.TitleIndexDir + "/data":         13,
		wikiindex.BodyIndexDir + ".building/data": 17,
		"manifest.json":                           5,
	}
	for name, size := range files {
		path := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, make([]byte, size), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	total, dump, sourceIndex, title, body := localStorage(root)
	if total != 53 || dump != 11 || sourceIndex != 7 || title != 13 || body != 17 {
		t.Fatalf("storage = total %d dump %d source %d title %d body %d", total, dump, sourceIndex, title, body)
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
