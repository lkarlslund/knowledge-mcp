package wikiindex

import (
	"bytes"
	stdbzip2 "compress/bzip2"
	"context"
	"encoding/xml"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/lkarlslund/knowledge-mcp/internal/model"
)

var liveProfileQueries = []string{
	"US hybrid warfare against Greenland",
	"Attack on Mette Frederiksen",
	"Go programming language",
	"Trump Frederiksen phone call Greenland",
	"greenland vs america",
	"Apollo moon landing guidance computer",
}

func BenchmarkLiveSearchRankOnly(b *testing.B) {
	benchmarkLiveSearch(b, false)
}

func BenchmarkLiveSearchWithSnippets(b *testing.B) {
	benchmarkLiveSearch(b, true)
}

func TestLiveSelectedSAXDecoderParity(t *testing.T) {
	for index, xmlData := range liveProfileXMLSamples(t, 9) {
		pages, err := decodeXMLPages(xml.NewDecoder(bytes.NewReader(xmlData)))
		if err != nil {
			t.Fatalf("sample %d: %v", index, err)
		}
		wanted := make(map[uint64]struct{}, len(pages))
		for _, page := range pages {
			wanted[page.ID] = struct{}{}
		}
		want, err := decodeSelectedXMLPages(xml.NewDecoder(bytes.NewReader(xmlData)), wanted)
		if err != nil {
			t.Fatalf("sample %d: %v", index, err)
		}
		got, err := decodeSelectedXMLPagesSAX(bytes.NewReader(xmlData), wanted)
		if err != nil {
			t.Fatalf("sample %d: %v", index, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("sample %d: gosax decoded pages differ from encoding/xml\n got: %#v\nwant: %#v", index, got, want)
		}
		gotAll, err := decodeSelectedXMLPagesSAX(bytes.NewReader(xmlData), nil)
		if err != nil {
			t.Fatalf("sample %d full decode: %v", index, err)
		}
		if !reflect.DeepEqual(gotAll, pages) {
			t.Fatalf("sample %d: gosax full decode differs from encoding/xml\n got: %#v\nwant: %#v", index, gotAll, pages)
		}
	}
}

func BenchmarkLiveSelectedPageParser(b *testing.B) {
	xmlData, wanted := liveProfileXMLBlock(b)
	b.Run("encoding_xml", func(b *testing.B) {
		b.SetBytes(int64(len(xmlData)))
		b.ReportAllocs()
		for b.Loop() {
			if _, err := decodeSelectedXMLPages(xml.NewDecoder(bytes.NewReader(xmlData)), wanted); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("gosax", func(b *testing.B) {
		b.SetBytes(int64(len(xmlData)))
		b.ReportAllocs()
		for b.Loop() {
			if _, err := decodeSelectedXMLPagesSAX(bytes.NewReader(xmlData), wanted); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkLiveFullPageParser(b *testing.B) {
	xmlData := liveProfileXMLSamples(b, 1)[0]
	b.Run("encoding_xml", func(b *testing.B) {
		b.SetBytes(int64(len(xmlData)))
		b.ReportAllocs()
		for b.Loop() {
			if _, err := decodeXMLPages(xml.NewDecoder(bytes.NewReader(xmlData))); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("gosax", func(b *testing.B) {
		b.SetBytes(int64(len(xmlData)))
		b.ReportAllocs()
		for b.Loop() {
			if _, err := decodeSelectedXMLPagesSAX(bytes.NewReader(xmlData), nil); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func benchmarkLiveSearch(b *testing.B, snippets bool) {
	path := os.Getenv("WIKI_PROFILE_PATH")
	if path == "" {
		b.Skip("WIKI_PROFILE_PATH is not set")
	}
	reader, err := OpenReader(path, true)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = reader.Close() })
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		for _, query := range liveProfileQueries {
			if _, err := reader.Search(context.Background(), query, model.SearchOptions{Limit: 10, Snippets: snippets}, true); err != nil {
				b.Fatal(err)
			}
		}
	}
	b.ReportMetric(float64(len(liveProfileQueries)), "searches/op")
}

func liveProfileXMLBlock(tb testing.TB) ([]byte, map[uint64]struct{}) {
	tb.Helper()
	samples := liveProfileXMLSamples(tb, 1)
	decompressed := samples[0]
	pages, err := decodeXMLPages(xml.NewDecoder(bytes.NewReader(decompressed)))
	if err != nil {
		tb.Fatal(err)
	}
	if len(pages) < 3 {
		tb.Fatalf("profile stream contains only %d pages", len(pages))
	}
	wanted := map[uint64]struct{}{
		pages[0].ID:            {},
		pages[len(pages)/2].ID: {},
		pages[len(pages)-1].ID: {},
	}
	return decompressed, wanted
}

func liveProfileXMLSamples(tb testing.TB, count int) [][]byte {
	tb.Helper()
	path := os.Getenv("WIKI_PROFILE_PATH")
	if path == "" {
		tb.Skip("WIKI_PROFILE_PATH is not set")
	}
	part := Part{
		Number:    0,
		DumpPath:  filepath.Join(path, "parts", "000.dump.bz2"),
		IndexPath: filepath.Join(path, "parts", "000.index.bz2"),
	}
	streams, err := readStreams(context.Background(), part)
	if err != nil {
		tb.Fatal(err)
	}
	if count > len(streams) {
		count = len(streams)
	}
	selected := make([]stream, 0, count)
	if count == 1 {
		largest := streams[0]
		for _, candidate := range streams[1:] {
			if candidate.End-candidate.Offset > largest.End-largest.Offset {
				largest = candidate
			}
		}
		selected = append(selected, largest)
	} else {
		for index := range count {
			selected = append(selected, streams[index*(len(streams)-1)/(count-1)])
		}
	}
	dump, err := os.Open(part.DumpPath)
	if err != nil {
		tb.Fatal(err)
	}
	decompressed := make([][]byte, 0, len(selected))
	for _, stream := range selected {
		data, readErr := io.ReadAll(stdbzip2.NewReader(io.NewSectionReader(dump, stream.Offset, stream.End-stream.Offset)))
		if readErr != nil {
			_ = dump.Close()
			tb.Fatal(readErr)
		}
		decompressed = append(decompressed, data)
	}
	if err := dump.Close(); err != nil {
		tb.Fatal(err)
	}
	return decompressed
}
