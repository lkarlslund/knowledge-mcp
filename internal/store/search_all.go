package store

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"unicode"

	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/model"
)

const federatedParallelism = 8

type federatedCandidate struct {
	hit   model.SearchHit
	rank  int
	peak  float64
	meta  model.DatasetMetadata
	score float64
}

func (s *Store) searchAll(ctx context.Context, query string, options model.SearchOptions) (model.SearchResult, error) {
	locals, err := s.ListLocal()
	if err != nil {
		return model.SearchResult{}, err
	}
	limit := options.Limit
	if limit <= 0 {
		limit = 10
	}
	limit = min(limit, 50)
	offset := max(options.Offset, 0)
	candidateLimit := min(100, max(20, (offset+limit)*3))
	type response struct {
		local  model.LocalDataset
		result model.SearchResult
		err    error
	}
	responses := make(chan response, len(locals))
	semaphore := make(chan struct{}, federatedParallelism)
	var group sync.WaitGroup
	skipped := make([]string, 0)
	for _, local := range locals {
		if !local.TitleReady || options.Mode == "full_text" && !local.BodyReady {
			skipped = append(skipped, local.Dataset)
			continue
		}
		local := local
		group.Add(1)
		go func() {
			defer group.Done()
			select {
			case semaphore <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-semaphore }()
			searchOptions := options
			searchOptions.Offset, searchOptions.Limit, searchOptions.Snippets = 0, candidateLimit, false
			result, _, searchErr := s.searchDataset(ctx, local.Dataset, query, searchOptions)
			responses <- response{local: local, result: result, err: searchErr}
		}()
	}
	go func() { group.Wait(); close(responses) }()

	result := model.SearchResult{Query: query, SearchMode: "federated", Offset: offset, Hits: []model.SearchHit{}, SkippedDatasets: skipped}
	candidates := make([]federatedCandidate, 0)
	for response := range responses {
		if response.err != nil {
			result.PartialErrors = append(result.PartialErrors, fmt.Sprintf("%s: %v", response.local.Dataset, response.err))
			continue
		}
		result.SearchedDatasets = append(result.SearchedDatasets, response.local.Dataset)
		result.Total += response.result.Total
		peak := 0.0
		if len(response.result.Hits) > 0 {
			peak = response.result.Hits[0].Score
		}
		for rank, hit := range response.result.Hits {
			hit.Dataset, hit.Provider = response.local.Dataset, response.local.Provider
			candidates = append(candidates, federatedCandidate{hit: hit, rank: rank, peak: peak, meta: response.local.Site})
		}
	}
	if err := ctx.Err(); err != nil {
		return model.SearchResult{}, err
	}
	sort.Strings(result.SearchedDatasets)
	sort.Strings(result.SkippedDatasets)
	sort.Strings(result.PartialErrors)
	rankFederated(query, candidates)
	selected := diversifyFederated(candidates, offset+limit)
	if offset < len(selected) {
		end := min(offset+limit, len(selected))
		result.Hits = make([]model.SearchHit, end-offset)
		for index := offset; index < end; index++ {
			result.Hits[index-offset] = selected[index].hit
		}
	}
	if uint64(offset+len(result.Hits)) < result.Total && len(result.Hits) > 0 {
		result.NextOffset = offset + len(result.Hits)
	}
	for index := range result.Hits {
		if err := s.decorateSearchHit(&result.Hits[index]); err != nil {
			return model.SearchResult{}, err
		}
	}
	return result, nil
}

func rankFederated(query string, candidates []federatedCandidate) {
	queryTokens, normalizedQuery := tokenSet(query), normalizeSearchText(query)
	for index := range candidates {
		candidate := &candidates[index]
		score := 1 / (60.0 + float64(candidate.rank+1))
		title := normalizeSearchText(candidate.hit.Title)
		if title == normalizedQuery {
			score += .20
		}
		score += .05 * tokenCoverage(queryTokens, tokenSet(candidate.hit.Title))
		if candidate.peak > 0 {
			score += .01 * candidate.hit.Score / candidate.peak
		}
		metadata := candidate.meta.Name + " " + candidate.meta.Description + " " + candidate.meta.Project + " " + candidate.meta.ContentType
		score += .015 * tokenCoverage(queryTokens, tokenSet(metadata))
		candidate.score, candidate.hit.Score = score, score
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].score != candidates[j].score {
			return candidates[i].score > candidates[j].score
		}
		if candidates[i].hit.Title != candidates[j].hit.Title {
			return candidates[i].hit.Title < candidates[j].hit.Title
		}
		return candidates[i].hit.Dataset < candidates[j].hit.Dataset
	})
}

func diversifyFederated(candidates []federatedCandidate, wanted int) []federatedCandidate {
	if wanted <= 0 {
		return nil
	}
	capPerDataset := max(2, (wanted+2)/3)
	selected, deferred := make([]federatedCandidate, 0, wanted), make([]federatedCandidate, 0)
	counts, seen := map[string]int{}, map[string]bool{}
	for _, candidate := range candidates {
		key := candidate.hit.URL
		if key == "" {
			key = candidate.hit.Dataset + "\x00" + candidate.hit.ID
		}
		if seen[key] {
			continue
		}
		seen[key] = true
		if counts[candidate.hit.Dataset] >= capPerDataset {
			deferred = append(deferred, candidate)
			continue
		}
		selected, counts[candidate.hit.Dataset] = append(selected, candidate), counts[candidate.hit.Dataset]+1
		if len(selected) == wanted {
			return selected
		}
	}
	for _, candidate := range deferred {
		selected = append(selected, candidate)
		if len(selected) == wanted {
			break
		}
	}
	return selected
}

func normalizeSearchText(value string) string {
	var normalized strings.Builder
	space := true
	for _, character := range strings.ToLower(value) {
		if unicode.IsLetter(character) || unicode.IsNumber(character) {
			normalized.WriteRune(character)
			space = false
		} else if !space {
			normalized.WriteByte(' ')
			space = true
		}
	}
	return strings.TrimSpace(normalized.String())
}

func tokenSet(value string) map[string]struct{} {
	result := map[string]struct{}{}
	for _, token := range strings.Fields(normalizeSearchText(value)) {
		result[token] = struct{}{}
	}
	return result
}

func tokenCoverage(query, candidate map[string]struct{}) float64 {
	if len(query) == 0 {
		return 0
	}
	matches := 0
	for token := range query {
		if _, found := candidate[token]; found {
			matches++
		}
	}
	return float64(matches) / float64(len(query))
}
