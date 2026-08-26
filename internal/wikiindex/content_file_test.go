package wikiindex

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	dsbzip2 "github.com/dsnet/compress/bzip2"
)

func TestCurrentContentFileStreamsAndReadsPages(t *testing.T) {
	t.Parallel()
	source := []byte(`<mediawiki><page><title>Alpha</title><ns>0</ns><id>1</id><revision><id>11</id><timestamp>2026-08-01T00:00:00Z</timestamp><text>Useful [[Beta]] body.</text></revision></page><page><title>Alias</title><ns>0</ns><id>2</id><redirect title="Alpha"/><revision><id>22</id><text>#REDIRECT [[Alpha]]</text></revision></page></mediawiki>`)
	path := compressContentFile(t, source)
	var titles []SourcePage
	if err := ScanBzip2File(context.Background(), path, false, func(page SourcePage) error { titles = append(titles, page); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(titles) != 2 || titles[0].Title != "Alpha" || titles[0].Wikitext != "" {
		t.Fatalf("titles=%+v", titles)
	}
	var bodies []SourcePage
	if err := ScanBzip2File(context.Background(), path, true, func(page SourcePage) error { bodies = append(bodies, page); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(bodies) != 2 || bodies[0].Wikitext != "Useful [[Beta]] body." || !bodies[1].IsRedirect() {
		t.Fatalf("bodies=%+v", bodies)
	}
	page, err := ReadBzip2Page(context.Background(), path, 1)
	if err != nil {
		t.Fatal(err)
	}
	if page.ID != 1 || page.RevisionID != 11 || page.Timestamp == "" {
		t.Fatalf("page=%+v", page)
	}
}

func compressContentFile(t *testing.T, source []byte) string {
	t.Helper()
	var compressed bytes.Buffer
	writer, err := dsbzip2.NewWriter(&compressed, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write(source); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "part.xml.bz2")
	if err := os.WriteFile(path, compressed.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}
