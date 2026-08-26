package store

import (
	"testing"

	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/model"
)

func TestRankFederatedPrefersExactTitleAcrossIncomparableScores(t *testing.T) {
	t.Parallel()
	candidates := []federatedCandidate{
		{hit: model.SearchHit{Title: "A loosely related result", Dataset: "large", Score: 10_000}, rank: 0, peak: 10_000},
		{hit: model.SearchHit{Title: "Greenland crisis", Dataset: "small", Score: 2}, rank: 3, peak: 5},
	}
	rankFederated("Greenland crisis", candidates)
	if candidates[0].hit.Dataset != "small" {
		t.Fatalf("exact-title result was not first: %#v", candidates)
	}
}

func TestDiversifyFederatedAvoidsSingleDatasetMonopoly(t *testing.T) {
	t.Parallel()
	candidates := make([]federatedCandidate, 0, 8)
	for index := range 6 {
		candidates = append(candidates, federatedCandidate{hit: model.SearchHit{Dataset: "one", ID: string(rune('a' + index))}})
	}
	candidates = append(candidates,
		federatedCandidate{hit: model.SearchHit{Dataset: "two", ID: "a"}},
		federatedCandidate{hit: model.SearchHit{Dataset: "three", ID: "a"}},
	)
	selected := diversifyFederated(candidates, 6)
	seen := map[string]bool{}
	for _, candidate := range selected {
		seen[candidate.hit.Dataset] = true
	}
	if len(seen) != 3 {
		t.Fatalf("selected datasets = %v, want all three", seen)
	}
}
