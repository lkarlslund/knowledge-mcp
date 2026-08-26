package store

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	dsbzip2 "github.com/dsnet/compress/bzip2"
	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/knowledgeindex"
	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/model"
	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/provider"
	wikimediaprovider "github.com/lkarlslund/wikipedia-multistream-mcp/internal/provider/wikimedia"
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

	registry, err := provider.NewRegistry(wikimediaprovider.New(wikimedia.NewClientWithBaseURL(server.URL)))
	if err != nil {
		t.Fatal(err)
	}
	backend, err := Open(t.TempDir(), registry)
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	job, err := backend.Submit("testwiki", "", "download")
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := backend.Submit("testwiki", "", "download")
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
	result, err := backend.Search(context.Background(), "testwiki", "capybara", model.SearchOptions{Limit: 10, Snippets: true})
	if err != nil {
		t.Fatal(err)
	}
	if result.SearchMode != "full_text" || len(result.Hits) != 1 {
		t.Fatalf("unexpected search result: %#v", result)
	}
	page, err := backend.Read(context.Background(), "testwiki", "Test Article", "", model.ReadOptions{Format: "text", MaxChars: 1000, FollowRedirects: true})
	if err != nil {
		t.Fatal(err)
	}
	if page.NumericID != 7 || page.Content != "A remarkable capybara appears here." {
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

func TestMigrateFlatDatasetIntoProviderDirectory(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	flat := filepath.Join(root, "datasets", "testwiki")
	if err := os.MkdirAll(flat, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := writeJSON(filepath.Join(flat, "manifest.json"), model.Manifest{Dataset: "testwiki", Provider: "wikimedia"}); err != nil {
		t.Fatal(err)
	}
	registry := testProviderRegistry(t)
	if err := migrateFlatDatasetDirectories(root, registry); err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(root, "datasets", "wikimedia", "testwiki", "manifest.json")
	if _, err := os.Stat(want); err != nil {
		t.Fatalf("migrated manifest missing: %v", err)
	}
	if _, err := os.Stat(flat); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("flat dataset still exists: %v", err)
	}
}

func TestReadManifestTranslatesLegacyDatasetFields(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "manifest.json")
	legacy := []byte(`{"wiki":"dawiki","dump_date":"20260801","dump_sha1":"raw","index_sha1":"metadata","dump_size":123,"index_size":45,"page_count":678,"site":{"content_articles":321}}`)
	if err := os.WriteFile(path, legacy, 0o644); err != nil {
		t.Fatal(err)
	}
	manifest, err := readManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Dataset != "dawiki" || manifest.ReleaseDate != "20260801" || manifest.RawHash != "raw" || manifest.ProviderMetadataHash != "metadata" || manifest.RawSize != 123 || manifest.ProviderMetadataSize != 45 || manifest.DocumentCount != 678 || manifest.Site.SourceDocuments != 321 {
		t.Fatalf("translated manifest = %#v", manifest)
	}
}

func TestJobPauseResumeCancelAndRetry(t *testing.T) {
	t.Parallel()
	job := &model.Job{ID: "job-1", Dataset: "testwiki", Kind: "download", State: model.StateDownloading, CreatedAt: time.Now(), UpdatedAt: time.Now()}
	backend := &Store{
		root:          t.TempDir(),
		jobs:          map[string]*model.Job{job.ID: job},
		active:        map[string]string{job.Dataset: job.ID},
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

func TestDeleteDatasetRemovesLocalAndStagedData(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	wiki := "testwiki"
	job := &model.Job{ID: "job-1", Dataset: wiki, Kind: "download", State: model.StateDownloaded, CreatedAt: time.Now(), UpdatedAt: time.Now()}
	backend := &Store{
		root:         root,
		providers:    testProviderRegistry(t),
		jobs:         map[string]*model.Job{job.ID: job},
		active:       map[string]string{},
		watchers:     map[chan struct{}]struct{}{},
		readers:      map[string]*knowledgeindex.Reader{},
		storage:      map[string]storageSnapshot{},
		lastProgress: map[string]time.Time{},
	}
	wikiPath := backend.datasetPath("wikimedia", wiki)
	if err := os.MkdirAll(filepath.Join(wikiPath, "parts"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := writeJSON(filepath.Join(wikiPath, "manifest.json"), model.Manifest{Dataset: wiki}); err != nil {
		t.Fatal(err)
	}
	stagePath := filepath.Join(root, ".staging", job.ID)
	if err := os.MkdirAll(stagePath, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := backend.DeleteDataset(wiki); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{wikiPath, stagePath} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("deleted path %s still exists: %v", path, err)
		}
	}
	if _, err := backend.Job(job.ID, ""); err != nil {
		t.Fatalf("job history was removed: %v", err)
	}
}

func TestDeleteDatasetRejectsActiveJob(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	wiki := "testwiki"
	wikiPath := filepath.Join(root, "datasets", "wikimedia", wiki)
	if err := os.MkdirAll(wikiPath, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := writeJSON(filepath.Join(wikiPath, "manifest.json"), model.Manifest{Dataset: wiki}); err != nil {
		t.Fatal(err)
	}
	backend := &Store{root: root, providers: testProviderRegistry(t), active: map[string]string{wiki: "job-1"}}
	if err := backend.DeleteDataset(wiki); err == nil {
		t.Fatal("DeleteDataset succeeded with an active job")
	}
	if _, err := os.Stat(wikiPath); err != nil {
		t.Fatalf("active wiki was removed: %v", err)
	}
}

func testProviderRegistry(t *testing.T) *provider.Registry {
	t.Helper()
	registry, err := provider.NewRegistry(wikimediaprovider.New(wikimedia.NewClient()))
	if err != nil {
		t.Fatal(err)
	}
	return registry
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

func TestLocalStorageCachesDirectoryWalk(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	path := filepath.Join(root, "parts", "000.dump.bz2")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, make([]byte, 3), 0o644); err != nil {
		t.Fatal(err)
	}
	backend := &Store{}
	if total, _, _, _, _ := backend.localStorage(root); total != 3 {
		t.Fatalf("initial storage = %d, want 3", total)
	}
	if err := os.WriteFile(path, make([]byte, 9), 0o644); err != nil {
		t.Fatal(err)
	}
	if total, _, _, _, _ := backend.localStorage(root); total != 3 {
		t.Fatalf("cached storage = %d, want 3", total)
	}
	backend.storageMu.Lock()
	snapshot := backend.storage[root]
	snapshot.updated = time.Now().Add(-storageCacheDuration)
	backend.storage[root] = snapshot
	backend.storageMu.Unlock()
	if total, _, _, _, _ := backend.localStorage(root); total != 9 {
		t.Fatalf("refreshed storage = %d, want 9", total)
	}
}

func TestListJobsUsesStablePipelineOrder(t *testing.T) {
	t.Parallel()
	now := time.Now()
	backend := &Store{jobs: map[string]*model.Job{
		"new-active": {ID: "new-active", State: model.StateDownloading, CreatedAt: now.Add(time.Minute), UpdatedAt: now.Add(4 * time.Minute)},
		"old-active": {ID: "old-active", State: model.StateBodyIndexing, CreatedAt: now, UpdatedAt: now.Add(5 * time.Minute)},
		"old-done":   {ID: "old-done", State: model.StateReady, CreatedAt: now.Add(-time.Hour), UpdatedAt: now.Add(2 * time.Minute)},
		"new-done":   {ID: "new-done", State: model.StateDownloaded, CreatedAt: now.Add(-time.Minute), UpdatedAt: now.Add(3 * time.Minute)},
	}}
	jobs := backend.ListJobs()
	want := []string{"old-active", "new-active", "new-done", "old-done"}
	for i, id := range want {
		if jobs[i].ID != id {
			t.Fatalf("jobs[%d] = %q, want %q; all: %#v", i, jobs[i].ID, id, jobs)
		}
	}
}

func TestJobPersistenceNotifiesSubscribers(t *testing.T) {
	t.Parallel()
	backend := &Store{root: t.TempDir(), jobs: map[string]*model.Job{}, watchers: map[chan struct{}]struct{}{}}
	ctx, cancel := context.WithCancel(context.Background())
	updates := backend.Subscribe(ctx)
	backend.mu.Lock()
	err := backend.saveJobsLocked()
	backend.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-updates:
	case <-time.After(time.Second):
		t.Fatal("subscriber received no persisted-state notification")
	}
	cancel()
}

func TestJobProgressPersistenceIsThrottled(t *testing.T) {
	t.Parallel()
	job := &model.Job{ID: "job-1", State: model.StateQueued, CreatedAt: time.Now(), UpdatedAt: time.Now()}
	backend := &Store{
		root:         t.TempDir(),
		jobs:         map[string]*model.Job{job.ID: job},
		watchers:     map[chan struct{}]struct{}{},
		lastProgress: map[string]time.Time{},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	updates := backend.Subscribe(ctx)
	backend.setJob(job.ID, model.StateDownloading, "downloading", 1, 10, "bytes", 1, "downloading", "")
	select {
	case <-updates:
	case <-time.After(time.Second):
		t.Fatal("state transition was not persisted")
	}
	backend.setJob(job.ID, model.StateDownloading, "downloading", 2, 10, "bytes", 1, "downloading", "")
	select {
	case <-updates:
		t.Fatal("rapid progress update was persisted")
	case <-time.After(50 * time.Millisecond):
	}
	if status, _ := backend.Job(job.ID, ""); status.Completed != 2 {
		t.Fatalf("in-memory progress = %d, want 2", status.Completed)
	}
	backend.setJob(job.ID, model.StateDownloading, "downloading", 1, 10, "bytes", 1, "stale worker update", "")
	if status, _ := backend.Job(job.ID, ""); status.Completed != 2 {
		t.Fatalf("regressing progress update was accepted: %d", status.Completed)
	}
	backend.mu.Lock()
	backend.lastProgress[job.ID] = time.Now().Add(-progressSaveInterval)
	backend.mu.Unlock()
	backend.setJob(job.ID, model.StateDownloading, "downloading", 3, 10, "bytes", 1, "downloading", "")
	select {
	case <-updates:
	case <-time.After(time.Second):
		t.Fatal("progress was not persisted after throttle interval")
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
