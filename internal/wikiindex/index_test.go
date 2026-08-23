package wikiindex

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/blevesearch/bleve/v2/mapping"
	dsbzip2 "github.com/dsnet/compress/bzip2"
)

func TestTitleAndBodyIndexes(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	partsDir := filepath.Join(dir, "parts")
	if err := os.MkdirAll(partsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	first := compressForTest(t, []byte(`<mediawiki><page><title>Alpha Page</title><id>1</id><revision><id>11</id><timestamp>2026-01-01T00:00:00Z</timestamp><text>Alpha has a [[Useful link|useful label]] and {{template|noise}}.</text></revision></page>`))
	second := compressForTest(t, []byte(`<page><title>Beta: Details</title><id>2</id><revision><id>22</id><timestamp>2026-01-02T00:00:00Z</timestamp><text>Beta contains a distinctive platypus phrase.</text></revision></page></mediawiki>`))
	firstDumpPath := filepath.Join(partsDir, "000.dump.bz2")
	firstIndexPath := filepath.Join(partsDir, "000.index.bz2")
	secondDumpPath := filepath.Join(partsDir, "001.dump.bz2")
	secondIndexPath := filepath.Join(partsDir, "001.index.bz2")
	if err := os.WriteFile(firstDumpPath, first, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(firstIndexPath, compressForTest(t, []byte("0:1:Alpha Page\n")), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(secondDumpPath, second, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(secondIndexPath, compressForTest(t, []byte("0:2:Beta: Details\n")), 0o644); err != nil {
		t.Fatal(err)
	}

	parts := []Part{
		{Number: 0, DumpPath: firstDumpPath, IndexPath: firstIndexPath},
		{Number: 1, DumpPath: secondDumpPath, IndexPath: secondIndexPath},
	}
	count, err := BuildTitle(context.Background(), parts, filepath.Join(dir, TitleIndexDir), func(uint64, int64, int64) {})
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
	pageByID, err := ReadPage(dir, "", 2, "wikitext", 0, 1000)
	if err != nil || pageByID.Title != "Beta: Details" {
		t.Fatalf("ReadPage by document ID = %#v, %v", pageByID, err)
	}

	if err := BuildBody(context.Background(), parts, filepath.Join(dir, BodyIndexDir), func(int64, int64) {}); err != nil {
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

func TestLeanMappings(t *testing.T) {
	t.Parallel()
	for name, indexMapping := range map[string]mapping.IndexMapping{"title": titleMapping(), "body": bodyMapping()} {
		impl, ok := indexMapping.(*mapping.IndexMappingImpl)
		if !ok {
			t.Fatalf("%s mapping has type %T", name, indexMapping)
		}
		if impl.IndexDynamic || impl.StoreDynamic || impl.DocValuesDynamic {
			t.Errorf("%s mapping retains dynamic fields", name)
		}
		for property, document := range impl.DefaultMapping.Properties {
			for _, field := range document.Fields {
				if field.IncludeTermVectors || field.IncludeInAll || field.DocValues {
					t.Errorf("%s.%s retains redundant index data: %#v", name, property, field)
				}
			}
		}
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

func TestBodyIndexResumesFromStreamCheckpoint(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dumpPath := filepath.Join(dir, "dump.bz2")
	indexPath := filepath.Join(dir, "index.bz2")
	var dump, sourceIndex bytes.Buffer
	const streamCount = 130
	for pageID := 1; pageID <= streamCount; pageID++ {
		offset := dump.Len()
		page := fmt.Sprintf(`<page><title>Page %d</title><id>%d</id><revision><id>%d</id><text>resume token %d</text></revision></page>`, pageID, pageID, pageID+1000, pageID)
		dump.Write(compressForTest(t, []byte(page)))
		fmt.Fprintf(&sourceIndex, "%d:%d:Page %d\n", offset, pageID, pageID)
	}
	if err := os.WriteFile(dumpPath, dump.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(indexPath, compressForTest(t, sourceIndex.Bytes()), 0o644); err != nil {
		t.Fatal(err)
	}

	destination := filepath.Join(dir, BodyIndexDir)
	ctx, cancel := context.WithCancel(context.Background())
	err := BuildBody(ctx, []Part{{DumpPath: dumpPath, IndexPath: indexPath}}, destination, func(done, _ int64) {
		if done >= 64 {
			cancel()
		}
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("first BuildBody error = %v, want context.Canceled", err)
	}
	checkpoint, ok := loadBodyCheckpoint(destination+".checkpoint.json", destination, mustReadStreams(t, Part{DumpPath: dumpPath, IndexPath: indexPath}))
	if !ok || checkpoint.Done < 64 || checkpoint.Done >= streamCount {
		t.Fatalf("checkpoint = %#v, valid %v", checkpoint, ok)
	}

	firstProgress := int64(-1)
	if err := BuildBody(context.Background(), []Part{{DumpPath: dumpPath, IndexPath: indexPath}}, destination, func(done, _ int64) {
		if firstProgress < 0 {
			firstProgress = done
		}
	}); err != nil {
		t.Fatalf("resumed BuildBody: %v", err)
	}
	if firstProgress != checkpoint.Done {
		t.Fatalf("resumed progress = %d, want saved checkpoint %d", firstProgress, checkpoint.Done)
	}
	if _, err := os.Stat(destination + ".checkpoint.json"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("completed checkpoint still exists: %v", err)
	}

	for _, pageID := range []int{1, streamCount} {
		result, err := Search(dir, fmt.Sprintf("resume token %d", pageID), 0, 10, true)
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Hits) == 0 || result.Hits[0].PageID != uint64(pageID) {
			t.Fatalf("resumed index missing page %d: %#v", pageID, result.Hits)
		}
	}
}

func TestTitleIndexResumesFromLineCheckpoint(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	indexPath := filepath.Join(dir, "index.bz2")
	var sourceIndex bytes.Buffer
	const pageCount = 5100
	for pageID := 1; pageID <= pageCount; pageID++ {
		fmt.Fprintf(&sourceIndex, "0:%d:Checkpoint Page %d\n", pageID, pageID)
	}
	if err := os.WriteFile(indexPath, compressForTest(t, sourceIndex.Bytes()), 0o644); err != nil {
		t.Fatal(err)
	}
	parts := []Part{{IndexPath: indexPath}}
	destination := filepath.Join(dir, TitleIndexDir)

	ctx, cancel := context.WithCancel(context.Background())
	_, err := BuildTitle(ctx, parts, destination, func(pages uint64, _, _ int64) {
		if pages >= 5000 {
			cancel()
		}
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("first BuildTitle error = %v, want context.Canceled", err)
	}
	checkpoint, _, ok := loadTitleCheckpoint(destination+".checkpoint.json", destination, parts)
	if !ok || checkpoint.Pages != 5000 || checkpoint.Parts[0].Lines != 5000 || checkpoint.Parts[0].Complete {
		t.Fatalf("checkpoint = %#v, valid %v", checkpoint, ok)
	}

	firstProgress := uint64(0)
	count, err := BuildTitle(context.Background(), parts, destination, func(pages uint64, _, _ int64) {
		if firstProgress == 0 {
			firstProgress = pages
		}
	})
	if err != nil {
		t.Fatalf("resumed BuildTitle: %v", err)
	}
	if firstProgress != checkpoint.Pages || count != pageCount {
		t.Fatalf("resumed pages = %d, final count = %d; want %d and %d", firstProgress, count, checkpoint.Pages, pageCount)
	}
	if _, err := os.Stat(destination + ".checkpoint.json"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("completed checkpoint still exists: %v", err)
	}
	result, err := Search(dir, "Checkpoint Page 5100", 0, 10, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) == 0 || result.Hits[0].PageID != pageCount {
		t.Fatalf("resumed title index missing final page: %#v", result.Hits)
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

func mustReadStreams(t *testing.T, part Part) []stream {
	t.Helper()
	streams, err := readStreams(context.Background(), part)
	if err != nil {
		t.Fatal(err)
	}
	for i := range streams {
		streams[i].Index = i
	}
	return streams
}
