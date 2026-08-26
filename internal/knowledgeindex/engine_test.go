package knowledgeindex

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/model"
	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/provider"
)

type testCorpus struct{ records []provider.Record }

func (c *testCorpus) ScanTitles(ctx context.Context, after string, sink provider.RecordSink) error {
	return c.scan(ctx, after, false, sink)
}
func (c *testCorpus) ScanBodies(ctx context.Context, after string, sink provider.RecordSink) error {
	return c.scan(ctx, after, true, sink)
}
func (c *testCorpus) scan(ctx context.Context, _ string, body bool, sink provider.RecordSink) error {
	for index, record := range c.records {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !body {
			record.Body = ""
		}
		if err := sink(record, provider.ScanPosition{Cursor: record.ID, Completed: int64(index + 1), Total: int64(len(c.records))}); err != nil {
			return err
		}
	}
	return nil
}
func (*testCorpus) Close() error { return nil }
func (*testCorpus) Read(_ context.Context, record provider.Record, options model.ReadOptions) (model.Document, error) {
	return model.Document{ID: record.ID, Title: record.Title, URL: record.URL, Format: options.Format, Content: "rendered " + record.Locator}, nil
}

func TestGenericEngineIndexesProviderRecords(t *testing.T) {
	t.Parallel()
	path := t.TempDir()
	corpus := &testCorpus{records: []provider.Record{
		{ID: "http", Title: "HTTP Semantics", Body: "methods status codes fields", URL: "https://example/http", Locator: "raw:1", Primary: true},
		{ID: "mail", Title: "Mail Status", Body: "email delivery status codes", URL: "https://example/mail", Locator: "raw:2", Primary: true},
		{ID: "talk", Title: "Talk: HTTP", Body: "discussion of semantics", Locator: "raw:3", Namespace: 1},
	}}
	count, err := BuildTitle(context.Background(), path, corpus, func(uint64, int64, int64) {})
	if err != nil || count != 3 {
		t.Fatalf("BuildTitle = %d, %v", count, err)
	}
	if err := os.Rename(filepath.Join(path, TitleDirectory+".building"), filepath.Join(path, TitleDirectory)); err != nil {
		t.Fatal(err)
	}
	if err := BuildBody(context.Background(), path, corpus, func(int64, int64) {}); err != nil {
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
