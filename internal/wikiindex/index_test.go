package wikiindex

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	dsbzip2 "github.com/dsnet/compress/bzip2"
)

func TestTitleAndBodyIndexes(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	first := compressForTest(t, []byte(`<mediawiki><page><title>Alpha Page</title><id>1</id><revision><id>11</id><timestamp>2026-01-01T00:00:00Z</timestamp><text>Alpha has a [[Useful link|useful label]] and {{template|noise}}.</text></revision></page>`))
	second := compressForTest(t, []byte(`<page><title>Beta: Details</title><id>2</id><revision><id>22</id><timestamp>2026-01-02T00:00:00Z</timestamp><text>Beta contains a distinctive platypus phrase.</text></revision></page></mediawiki>`))
	dump := append(append([]byte(nil), first...), second...)
	indexText := []byte("0:1:Alpha Page\n" + intString(len(first)) + ":2:Beta: Details\n")
	index := compressForTest(t, indexText)
	dumpPath := filepath.Join(dir, "dump.xml.bz2")
	indexPath := filepath.Join(dir, "multistream-index.txt.bz2")
	if err := os.WriteFile(dumpPath, dump, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(indexPath, index, 0o644); err != nil {
		t.Fatal(err)
	}

	count, err := BuildTitle(context.Background(), indexPath, filepath.Join(dir, TitleIndexDir), func(int64, int64) {})
	if err != nil {
		t.Fatalf("BuildTitle: %v", err)
	}
	if count != 2 {
		t.Fatalf("page count = %d, want 2", count)
	}
	titleResult, err := Search(dir, "Beta", 0, 10, false)
	if err != nil {
		t.Fatalf("title Search: %v", err)
	}
	if len(titleResult.Hits) != 1 || titleResult.Hits[0].Title != "Beta: Details" {
		t.Fatalf("unexpected title hits: %#v", titleResult.Hits)
	}
	page, err := ReadPage(dir, "Beta: Details", 0, "text", 0, 1000)
	if err != nil {
		t.Fatalf("ReadPage: %v", err)
	}
	if page.PageID != 2 || page.Content != "Beta contains a distinctive platypus phrase." {
		t.Fatalf("unexpected page: %#v", page)
	}

	if err := BuildBody(context.Background(), dumpPath, indexPath, filepath.Join(dir, BodyIndexDir), func(int64, int64) {}); err != nil {
		t.Fatalf("BuildBody: %v", err)
	}
	bodyResult, err := Search(dir, "platypus", 0, 10, true)
	if err != nil {
		t.Fatalf("body Search: %v", err)
	}
	if len(bodyResult.Hits) != 1 || bodyResult.Hits[0].PageID != 2 {
		t.Fatalf("unexpected body hits: %#v", bodyResult.Hits)
	}
}

func TestPlainText(t *testing.T) {
	t.Parallel()
	input := `== Heading == <!-- hidden --> Text [[Target|label]] <ref>citation</ref> {{Infobox|x}} &amp; [https://example.test external]`
	want := "Heading Text label & external"
	if got := PlainText(input); got != want {
		t.Fatalf("PlainText() = %q, want %q", got, want)
	}
}

func TestParseIndexLineWithColonInTitle(t *testing.T) {
	t.Parallel()
	offset, id, title, err := parseIndexLine("123:456:Category:Example")
	if err != nil {
		t.Fatal(err)
	}
	if offset != 123 || id != 456 || title != "Category:Example" {
		t.Fatalf("got %d %d %q", offset, id, title)
	}
}

func compressForTest(t *testing.T, data []byte) []byte {
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

func intString(value int) string {
	if value == 0 {
		return "0"
	}
	var digits [32]byte
	i := len(digits)
	for value > 0 {
		i--
		digits[i] = byte('0' + value%10)
		value /= 10
	}
	return string(digits[i:])
}
