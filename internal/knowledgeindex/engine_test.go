package knowledgeindex

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/model"
	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/provider"
)

type testCorpus struct {
	records       []provider.Record
	failAfter     int
	failure       error
	observedAfter []string
}

func (c *testCorpus) ScanTitles(ctx context.Context, after string, _ provider.ScanOptions, sink provider.RecordSink) error {
	return c.scan(ctx, after, false, sink)
}
func (c *testCorpus) ScanBodies(ctx context.Context, after string, _ provider.ScanOptions, sink provider.RecordSink) error {
	return c.scan(ctx, after, true, sink)
}
func (c *testCorpus) scan(ctx context.Context, after string, body bool, sink provider.RecordSink) error {
	c.observedAfter = append(c.observedAfter, after)
	start := 0
	if after != "" {
		for index := range c.records {
			if c.records[index].ID == after {
				start = index + 1
				break
			}
		}
	}
	for index := start; index < len(c.records); index++ {
		record := c.records[index]
		if err := ctx.Err(); err != nil {
			return err
		}
		if c.failure != nil && index == c.failAfter {
			return c.failure
		}
		if !body {
			record.Body = ""
		}
		if err := sink(record, provider.ScanPosition{Cursor: record.ID, Completed: int64(index + 1), Total: int64(len(c.records)), Boundary: true}); err != nil {
			return err
		}
	}
	return nil
}
func (*testCorpus) Close() error { return nil }
func (*testCorpus) Read(_ context.Context, record provider.Record, options model.ReadOptions) (model.Document, error) {
	return model.Document{ID: record.ID, Title: record.Title, URL: record.URL, Format: options.Format, Content: "rendered " + record.Locator}, nil
}

func TestTitleBuildResumesAtCommittedProviderBoundary(t *testing.T) {
	t.Parallel()
	path := t.TempDir()
	records := make([]provider.Record, 20_200)
	for index := range records {
		records[index] = provider.Record{ID: fmt.Sprintf("doc-%04d", index), Title: fmt.Sprintf("Document %d", index), Primary: true}
	}
	interrupted := errors.New("source interrupted")
	corpus := &testCorpus{records: records, failAfter: 20_050, failure: interrupted}
	progress := func(uint64, int64, int64) {}
	if _, err := BuildTitle(context.Background(), path, "release-a", corpus, provider.ScanOptions{}, progress); !errors.Is(err, interrupted) {
		t.Fatalf("first BuildTitle error = %v", err)
	}
	corpus.failure = nil
	count, err := BuildTitle(context.Background(), path, "release-a", corpus, provider.ScanOptions{}, progress)
	if err != nil {
		t.Fatal(err)
	}
	if count != uint64(len(records)) {
		t.Fatalf("resumed count = %d, want %d", count, len(records))
	}
	if got := corpus.observedAfter[len(corpus.observedAfter)-1]; got != "doc-19999" {
		t.Fatalf("resume cursor = %q, want doc-19999", got)
	}
	if _, err := os.Stat(filepath.Join(path, TitleDirectory+".building.checkpoint.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("checkpoint still exists: %v", err)
	}
}

func TestBodyBuildResumesAtCommittedProviderBoundary(t *testing.T) {
	t.Parallel()
	path := t.TempDir()
	records := make([]provider.Record, 10)
	body := strings.Repeat("x", 1<<20)
	for index := range records {
		records[index] = provider.Record{ID: fmt.Sprintf("doc-%02d", index), Title: fmt.Sprintf("Document %d", index), Body: body, Primary: true}
	}
	interrupted := errors.New("source interrupted")
	corpus := &testCorpus{records: records, failAfter: 9, failure: interrupted}
	if err := buildBody(context.Background(), path, "release-a", corpus, provider.ScanOptions{}, 8<<20, func(uint64, int64, int64, string) {}); !errors.Is(err, interrupted) {
		t.Fatalf("first BuildBody error = %v", err)
	}
	corpus.failure = nil
	if err := buildBody(context.Background(), path, "release-a", corpus, provider.ScanOptions{}, 8<<20, func(uint64, int64, int64, string) {}); err != nil {
		t.Fatal(err)
	}
	if got := corpus.observedAfter[len(corpus.observedAfter)-1]; got != "doc-07" {
		t.Fatalf("resume cursor = %q, want doc-07", got)
	}
	if _, err := os.Stat(filepath.Join(path, BodyDirectory+".building.checkpoint.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("checkpoint still exists: %v", err)
	}
}

func TestGenericEngineIndexesProviderRecords(t *testing.T) {
	t.Parallel()
	path := t.TempDir()
	corpus := &testCorpus{records: []provider.Record{
		{ID: "http", Title: "HTTP Semantics", Body: "methods status codes fields", URL: "https://example/http", Locator: "raw:1", Primary: true, Identifiers: []string{"RFC 9110"}},
		{ID: "mail", Title: "Mail Status", Body: "email delivery status codes", URL: "https://example/mail", Locator: "raw:2", Primary: true},
		{ID: "talk", Title: "Talk: HTTP", Body: "discussion of semantics", Locator: "raw:3", Namespace: 1},
		{ID: "empty", Title: " ", Body: "invalid source record"},
	}}
	count, err := BuildTitle(context.Background(), path, "test-source", corpus, provider.ScanOptions{}, func(uint64, int64, int64) {})
	if err != nil || count != 3 {
		t.Fatalf("BuildTitle = %d, %v", count, err)
	}
	if err := os.Rename(filepath.Join(path, TitleDirectory+".building"), filepath.Join(path, TitleDirectory)); err != nil {
		t.Fatal(err)
	}
	if err := BuildBody(context.Background(), path, "test-source", corpus, provider.ScanOptions{}, func(uint64, int64, int64, string) {}); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(path, BodyDirectory+".building"), filepath.Join(path, BodyDirectory)); err != nil {
		t.Fatal(err)
	}
	reader, err := Open(path, corpus, true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	result, err := reader.Search(context.Background(), "HTTP semantics", model.SearchOptions{Limit: 10}, true)
	if err != nil {
		t.Fatal(err)
	}
	identifierResult, err := reader.Search(context.Background(), "RFC 9110", model.SearchOptions{Limit: 10}, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(identifierResult.Hits) != 1 || identifierResult.Hits[0].ID != "http" {
		t.Fatalf("identifier search = %#v", identifierResult)
	}
	if len(result.Hits) == 0 || result.Hits[0].ID != "http" {
		t.Fatalf("search = %#v", result)
	}
	for _, hit := range result.Hits {
		if hit.ID == "talk" {
			t.Fatalf("secondary record leaked through primary filter: %#v", result)
		}
	}
	document, err := reader.Read(context.Background(), "", "http", model.ReadOptions{Format: "markdown"})
	if err != nil {
		t.Fatal(err)
	}
	if document.Content != "rendered raw:1" {
		t.Fatalf("document = %#v", document)
	}
}
