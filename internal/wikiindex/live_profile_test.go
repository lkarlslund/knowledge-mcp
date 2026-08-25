package wikiindex

import (
	"context"
	"os"
	"testing"

	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/model"
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
