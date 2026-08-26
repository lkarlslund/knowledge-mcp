package knowledgeindex

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/lkarlslund/knowledge-mcp/internal/model"
	"github.com/lkarlslund/knowledge-mcp/internal/provider"
)

func TestSharedIndexPublishesDatasetGenerationsAtomically(t *testing.T) {
	root := t.TempDir()
	index, err := OpenShared(root)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = index.Close() }()

	first := &testCorpus{records: []provider.Record{{ID: "one", Title: "First document", Body: "capybara protocol", Primary: true}}}
	documents, indexedBytes, err := index.Build(context.Background(), "example", "alpha", "release-1", first, provider.ScanOptions{}, func(uint64, int64, int64, string) {})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := index.Search(context.Background(), "", "capybara", model.SearchOptions{Limit: 10}); err == nil {
		t.Fatal("unpublished generation was searchable")
	}
	if err := index.Activate("example", "alpha", "release-1", documents, indexedBytes); err != nil {
		t.Fatal(err)
	}
	assertSharedHit(t, index, "", "capybara", "alpha", "one")

	second := &testCorpus{records: []provider.Record{{ID: "two", Title: "Replacement document", Body: "wombat protocol", Primary: true}}}
	documents, indexedBytes, err = index.Build(context.Background(), "example", "alpha", "release-2", second, provider.ScanOptions{}, func(uint64, int64, int64, string) {})
	if err != nil {
		t.Fatal(err)
	}
	assertSharedHit(t, index, "alpha", "capybara", "alpha", "one")
	if result, searchErr := index.Search(context.Background(), "alpha", "wombat", model.SearchOptions{Limit: 10}); searchErr != nil || len(result.Hits) != 0 {
		t.Fatalf("unpublished replacement leaked into search: %#v, %v", result, searchErr)
	}
	if err := index.Activate("example", "alpha", "release-2", documents, indexedBytes); err != nil {
		t.Fatal(err)
	}
	assertSharedHit(t, index, "alpha", "wombat", "alpha", "two")
	if result, searchErr := index.Search(context.Background(), "alpha", "capybara", model.SearchOptions{Limit: 10}); searchErr != nil || len(result.Hits) != 0 {
		t.Fatalf("retired generation remained visible: %#v, %v", result, searchErr)
	}
	if err := index.Cleanup(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestSharedIndexSearchesAcrossDatasetsAndPersistsRemoval(t *testing.T) {
	root := t.TempDir()
	index, err := OpenShared(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range []struct {
		dataset string
		body    string
	}{{"alpha", "shared discovery token"}, {"beta", "shared discovery token"}} {
		corpus := &testCorpus{records: []provider.Record{{ID: item.dataset + "-doc", Title: item.dataset + " document", Body: item.body, Primary: true}}}
		documents, indexedBytes, buildErr := index.Build(context.Background(), "example", item.dataset, "release-1", corpus, provider.ScanOptions{}, func(uint64, int64, int64, string) {})
		if buildErr != nil {
			t.Fatal(buildErr)
		}
		if activateErr := index.Activate("example", item.dataset, "release-1", documents, indexedBytes); activateErr != nil {
			t.Fatal(activateErr)
		}
	}
	result, err := index.Search(context.Background(), "", "discovery token", model.SearchOptions{Limit: 10})
	if err != nil || len(result.Hits) != 2 || len(result.SearchedDatasets) != 2 {
		t.Fatalf("combined search = %#v, %v", result, err)
	}
	if err := index.Remove("alpha"); err != nil {
		t.Fatal(err)
	}
	if err := index.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenShared(root)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	result, err = reopened.Search(context.Background(), "", "discovery token", model.SearchOptions{Limit: 10})
	if err != nil || len(result.Hits) != 1 || result.Hits[0].Dataset != "beta" {
		t.Fatalf("search after persisted removal = %#v, %v", result, err)
	}
	if _, err := reopened.Search(context.Background(), "alpha", "discovery", model.SearchOptions{Limit: 10}); err == nil {
		t.Fatal("removed dataset remained addressable")
	}
}

func TestSharedIndexSearchesWhileAnotherGenerationCommits(t *testing.T) {
	root := t.TempDir()
	index, err := OpenShared(root)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = index.Close() }()
	current := &testCorpus{records: []provider.Record{{ID: "current", Title: "Current", Body: "stable searchable phrase", Primary: true}}}
	documents, indexedBytes, err := index.build(context.Background(), "example", "alpha", "release-1", current, provider.ScanOptions{}, 4<<10, func(uint64, int64, int64, string) {})
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Activate("example", "alpha", "release-1", documents, indexedBytes); err != nil {
		t.Fatal(err)
	}
	records := make([]provider.Record, 250)
	for item := range records {
		records[item] = provider.Record{ID: fmt.Sprintf("next-%03d", item), Title: fmt.Sprintf("Next %d", item), Body: strings.Repeat("concurrent indexing payload ", 80), Primary: true}
	}
	done := make(chan error, 1)
	go func() {
		_, _, buildErr := index.build(context.Background(), "example", "beta", "release-1", &testCorpus{records: records}, provider.ScanOptions{}, 8<<10, func(uint64, int64, int64, string) {})
		done <- buildErr
	}()
	for {
		select {
		case buildErr := <-done:
			if buildErr != nil {
				t.Fatal(buildErr)
			}
			return
		default:
			assertSharedHit(t, index, "alpha", "stable searchable", "alpha", "current")
		}
	}
}

func TestSharedIndexEstimatedBytesDoesNotOverflow(t *testing.T) {
	index := &SharedIndex{
		manifest: sharedManifest{Datasets: map[string]sharedDataset{
			"large": {IndexedBytes: 37_000_000_000},
			"small": {IndexedBytes: 18_000_000_000},
		}},
		storageAt:    time.Now(),
		storageBytes: 54_000_000_000,
	}
	got := index.EstimatedBytes("large")
	want := int64(36_327_272_727)
	if got < want-1 || got > want+1 {
		t.Fatalf("EstimatedBytes() = %d, want approximately %d", got, want)
	}
}

func TestSharedIndexFiltersAndSortsPublicationDates(t *testing.T) {
	root := t.TempDir()
	index, err := OpenShared(root)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = index.Close() }()
	date := func(value string) *time.Time {
		parsed, parseErr := time.Parse("2006-01-02", value)
		if parseErr != nil {
			t.Fatal(parseErr)
		}
		return &parsed
	}
	corpus := &testCorpus{records: []provider.Record{
		{ID: "old", Title: "Older research", Body: "alzheimer evidence", Primary: true, Temporal: model.TemporalMetadata{PublishedAt: date("2020-01-02"), PublishedPrecision: "day"}},
		{ID: "new", Title: "Newer research", Body: "alzheimer evidence", Primary: true, Temporal: model.TemporalMetadata{PublishedAt: date("2025-03-04"), PublishedPrecision: "day"}},
	}}
	documents, indexedBytes, err := index.Build(context.Background(), "example", "dated", "release-1", corpus, provider.ScanOptions{}, func(uint64, int64, int64, string) {})
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Activate("example", "dated", "release-1", documents, indexedBytes); err != nil {
		t.Fatal(err)
	}
	result, err := index.Search(context.Background(), "dated", "alzheimer evidence", model.SearchOptions{Limit: 10, PublishedAfter: date("2024-01-01"), Sort: "newest"})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) != 1 || result.Hits[0].ID != "new" || result.Hits[0].PublishedAt == nil || result.Hits[0].PublishedPrecision != "day" {
		t.Fatalf("date-filtered search = %#v", result)
	}
}

func assertSharedHit(t *testing.T, index *SharedIndex, dataset, query, wantDataset, wantID string) {
	t.Helper()
	result, err := index.Search(context.Background(), dataset, query, model.SearchOptions{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) != 1 || result.Hits[0].Dataset != wantDataset || result.Hits[0].ID != wantID {
		t.Fatalf("search %q = %#v", query, result)
	}
}
