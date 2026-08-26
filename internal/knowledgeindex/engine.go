package knowledgeindex

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/analysis/analyzer/custom"
	"github.com/blevesearch/bleve/v2/analysis/token/lowercase"
	"github.com/blevesearch/bleve/v2/analysis/tokenizer/unicode"
	"github.com/blevesearch/bleve/v2/mapping"
	blevequery "github.com/blevesearch/bleve/v2/search/query"
	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/model"
	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/provider"
)

const (
	bodyShards     = 8
	titleBatchSize = 1_000
	bodyBatchSize  = 8 << 20
)

type TitleProgress func(documents uint64, completed, total int64)
type BodyProgress func(completed, total int64)

type indexDocument struct {
	Title      string `json:"title"`
	TitleExact string `json:"title_exact"`
	Body       string `json:"body,omitempty"`
	URL        string `json:"url,omitempty"`
	Locator    string `json:"locator,omitempty"`
	Part       int    `json:"part"`
	Offset     int64  `json:"offset"`
	End        int64  `json:"end"`
	Namespace  int    `json:"namespace"`
	Primary    int    `json:"primary"`
}

func Current(manifest model.Manifest) (title, body bool) {
	return manifest.TitleReady && manifest.TitleIndexVersion == TitleVersion,
		manifest.BodyReady && manifest.BodyIndexVersion == BodyVersion
}

func BuildTitle(ctx context.Context, path string, corpus provider.Corpus, progress TitleProgress) (uint64, error) {
	destination := filepath.Join(path, TitleDirectory+".building")
	if err := os.RemoveAll(destination); err != nil {
		return 0, err
	}
	idx, err := newIndex(destination, indexMapping(false))
	if err != nil {
		return 0, err
	}
	closed := false
	defer func() {
		if !closed {
			_ = idx.Close()
		}
	}()
	batch := idx.NewBatch()
	var count uint64
	err = corpus.ScanTitles(ctx, "", func(record provider.Record, position provider.ScanPosition) error {
		if record.ID == "" || strings.TrimSpace(record.Title) == "" {
			return errors.New("provider emitted title record without ID or title")
		}
		if err := batch.Index(record.ID, toIndexDocument(record, false)); err != nil {
			return err
		}
		count++
		if batch.Size() >= titleBatchSize {
			if err := idx.Batch(batch); err != nil {
				return err
			}
			batch = idx.NewBatch()
		}
		progress(count, position.Completed, position.Total)
		return nil
	})
	if err != nil {
		return 0, err
	}
	if batch.Size() > 0 {
		if err := idx.Batch(batch); err != nil {
			return 0, err
		}
	}
	if err := idx.Close(); err != nil {
		return 0, err
	}
	closed = true
	return count, nil
}

func BuildBody(ctx context.Context, path string, corpus provider.Corpus, progress BodyProgress) error {
	destination := filepath.Join(path, BodyDirectory+".building")
	if err := os.RemoveAll(destination); err != nil {
		return err
	}
	if err := os.MkdirAll(destination, 0o755); err != nil {
		return err
	}
	indexes := make([]bleve.Index, bodyShards)
	for shard := range indexes {
		idx, err := newIndex(bodyShardPath(destination, shard), indexMapping(true))
		if err != nil {
			for _, opened := range indexes {
				if opened != nil {
					_ = opened.Close()
				}
			}
			return err
		}
		indexes[shard] = idx
	}
	closed := false
	defer func() {
		if !closed {
			for _, idx := range indexes {
				_ = idx.Close()
			}
		}
	}()
	type batchState struct {
		batch *bleve.Batch
		bytes int
	}
	batches := make([]batchState, bodyShards)
	for shard := range batches {
		batches[shard].batch = indexes[shard].NewBatch()
	}
	flush := func(shard int) error {
		state := &batches[shard]
		if state.batch.Size() == 0 {
			return nil
		}
		if err := indexes[shard].Batch(state.batch); err != nil {
			return err
		}
		state.batch, state.bytes = indexes[shard].NewBatch(), 0
		return nil
	}
	err := corpus.ScanBodies(ctx, "", func(record provider.Record, position provider.ScanPosition) error {
		if record.ID == "" || strings.TrimSpace(record.Title) == "" {
			return errors.New("provider emitted body record without ID or title")
		}
		shard := recordShard(record.ID)
		if err := batches[shard].batch.Index(record.ID, toIndexDocument(record, true)); err != nil {
			return err
		}
		batches[shard].bytes += len(record.Title) + len(record.Body) + len(record.URL) + len(record.Locator)
		if batches[shard].bytes >= bodyBatchSize {
			if err := flush(shard); err != nil {
				return err
			}
		}
		progress(position.Completed, position.Total)
		return nil
	})
	if err != nil {
		return err
	}
	for shard := range indexes {
		if err := flush(shard); err != nil {
			return err
		}
		if err := indexes[shard].Close(); err != nil {
			return err
		}
	}
	closed = true
	return nil
}

func toIndexDocument(record provider.Record, body bool) indexDocument {
	primary := 0
	if record.Primary {
		primary = 1
	}
	document := indexDocument{Title: record.Title, TitleExact: normalize(record.Title), URL: record.URL, Locator: record.Locator, Part: record.Part, Offset: record.Offset, End: record.End, Namespace: record.Namespace, Primary: primary}
	if body {
		document.Body = record.Body
	}
	return document
}

type Reader struct {
	corpus     provider.Corpus
	title      bleve.Index
	body       bleve.Index
	bodyShards []bleve.Index
	mu         sync.RWMutex
	closed     bool
}

func Open(path string, corpus provider.Corpus, fullText bool) (*Reader, error) {
	reader := &Reader{corpus: corpus}
	title, err := bleve.OpenUsing(filepath.Join(path, TitleDirectory), map[string]any{"read_only": true})
	if err != nil {
		_ = corpus.Close()
		return nil, fmt.Errorf("open title index: %w", err)
	}
	reader.title = title
	if !fullText {
		return reader, nil
	}
	for shard := range bodyShards {
		idx, openErr := bleve.OpenUsing(bodyShardPath(filepath.Join(path, BodyDirectory), shard), map[string]any{"read_only": true})
		if openErr != nil {
			_ = reader.Close()
			return nil, fmt.Errorf("open body shard %d: %w", shard, openErr)
		}
		reader.bodyShards = append(reader.bodyShards, idx)
	}
	alias := bleve.NewIndexAlias(reader.bodyShards...)
	if err := alias.SetIndexMapping(indexMapping(true)); err != nil {
		_ = reader.Close()
		return nil, err
	}
	reader.body = alias
	return reader, nil
}

func (r *Reader) Retain() (func(), error) {
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return nil, errors.New("index reader is closed")
	}
	return r.mu.RUnlock, nil
}

func (r *Reader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	var errs []error
	if r.body != nil {
		errs = append(errs, r.body.Close())
	}
	for _, idx := range r.bodyShards {
		errs = append(errs, idx.Close())
	}
	if r.title != nil {
		errs = append(errs, r.title.Close())
	}
	if r.corpus != nil {
		errs = append(errs, r.corpus.Close())
	}
	return errors.Join(errs...)
}

func (r *Reader) Search(ctx context.Context, query string, options model.SearchOptions, fullText bool) (model.SearchResult, error) {
	query = strings.TrimSpace(query)
	if query == "" {
		return model.SearchResult{}, errors.New("query cannot be empty")
	}
	if options.Limit <= 0 || options.Limit > 50 {
		options.Limit = 10
	}
	idx, mode := r.title, "title"
	if fullText {
		idx, mode = r.body, "full_text"
	}
	if idx == nil {
		return model.SearchResult{}, fmt.Errorf("%s index is not open", mode)
	}
	searchQuery := rankedQuery(query, fullText)
	if !options.IncludeSecondary {
		primary := bleve.NewNumericRangeInclusiveQuery(floatPointer(1), floatPointer(1), boolPointer(true), boolPointer(true))
		primary.SetField("primary")
		searchQuery = bleve.NewConjunctionQuery(searchQuery, primary)
	}
	request := bleve.NewSearchRequestOptions(searchQuery, 500, 0, false)
	request.Fields = []string{"title", "url", "namespace", "primary"}
	response, err := idx.SearchInContext(ctx, request)
	if err != nil {
		return model.SearchResult{}, err
	}
	hits := make([]model.SearchHit, 0, len(response.Hits))
	for _, hit := range response.Hits {
		title, _ := hit.Fields["title"].(string)
		url, _ := hit.Fields["url"].(string)
		namespace, _ := numberField(hit.Fields["namespace"])
		hits = append(hits, model.SearchHit{ID: hit.ID, Title: title, URL: url, Namespace: int(namespace), Score: hit.Score, MatchMode: mode})
	}
	sort.SliceStable(hits, func(i, j int) bool {
		iExact, jExact := normalize(hits[i].Title) == normalize(query), normalize(hits[j].Title) == normalize(query)
		if iExact != jExact {
			return iExact
		}
		return hits[i].Score > hits[j].Score
	})
	total := uint64(len(hits))
	start := min(max(options.Offset, 0), len(hits))
	end := min(start+options.Limit, len(hits))
	hits = hits[start:end]
	if options.Snippets && fullText {
		for index := range hits {
			document, readErr := r.Read(ctx, "", hits[index].ID, model.ReadOptions{Format: "text", MaxChars: 50_000})
			if readErr == nil {
				hits[index].Snippet = querySnippet(document.Content, query, 500)
			}
		}
	}
	result := model.SearchResult{Query: query, SearchMode: mode, Total: total, Offset: options.Offset, PrimaryFilterApplied: !options.IncludeSecondary, SnippetsAvailable: fullText, SnippetsComplete: fullText, Hits: hits}
	if end < int(total) {
		result.NextOffset = end
	}
	return result, nil
}

func (r *Reader) Read(ctx context.Context, title, id string, options model.ReadOptions) (model.Document, error) {
	record, err := r.lookup(ctx, title, id)
	if err != nil {
		return model.Document{}, err
	}
	return r.corpus.Read(ctx, record, options)
}

func (r *Reader) lookup(ctx context.Context, title, id string) (provider.Record, error) {
	var query blevequery.Query
	if id != "" {
		query = bleve.NewDocIDQuery([]string{id})
	} else {
		term := bleve.NewTermQuery(normalize(title))
		term.SetField("title_exact")
		query = term
	}
	request := bleve.NewSearchRequestOptions(query, 1, 0, false)
	request.Fields = []string{"title", "url", "locator", "namespace", "primary"}
	response, err := r.title.SearchInContext(ctx, request)
	if err != nil {
		return provider.Record{}, err
	}
	if len(response.Hits) == 0 {
		return provider.Record{}, provider.ErrDocumentNotFound
	}
	hit := response.Hits[0]
	titleValue, _ := hit.Fields["title"].(string)
	url, _ := hit.Fields["url"].(string)
	locator, _ := hit.Fields["locator"].(string)
	namespace, _ := numberField(hit.Fields["namespace"])
	primary, _ := numberField(hit.Fields["primary"])
	return provider.Record{ID: hit.ID, Title: titleValue, URL: url, Locator: locator, Namespace: int(namespace), Primary: primary == 1}, nil
}

func rankedQuery(text string, fullText bool) blevequery.Query {
	queries := make([]blevequery.Query, 0, 4)
	title := bleve.NewMatchQuery(text)
	title.SetField("title")
	title.SetBoost(6)
	queries = append(queries, title)
	exact := bleve.NewTermQuery(normalize(text))
	exact.SetField("title_exact")
	exact.SetBoost(20)
	queries = append(queries, exact)
	if fullText {
		body := bleve.NewMatchQuery(text)
		body.SetField("body")
		queries = append(queries, body)
		terms := strings.Fields(text)
		if len(terms) > 1 {
			all := make([]blevequery.Query, 0, len(terms))
			for _, value := range terms {
				term := bleve.NewMatchQuery(value)
				term.SetField("body")
				all = append(all, term)
			}
			conjunction := bleve.NewConjunctionQuery(all...)
			conjunction.SetBoost(2)
			queries = append(queries, conjunction)
		}
	}
	return bleve.NewDisjunctionQuery(queries...)
}

func indexMapping(body bool) mapping.IndexMapping {
	m := bleve.NewIndexMapping()
	if err := m.AddCustomAnalyzer("knowledge_unicode", map[string]any{"type": custom.Name, "tokenizer": unicode.Name, "token_filters": []string{lowercase.Name}}); err != nil {
		panic(err)
	}
	m.DefaultAnalyzer = "knowledge_unicode"
	m.IndexDynamic, m.StoreDynamic, m.DocValuesDynamic = false, false, false
	doc := bleve.NewDocumentMapping()
	for _, fieldName := range []string{"title", "url", "locator"} {
		field := bleve.NewTextFieldMapping()
		field.Store, field.IncludeTermVectors, field.IncludeInAll, field.DocValues = true, false, false, false
		doc.AddFieldMappingsAt(fieldName, field)
	}
	exact := bleve.NewTextFieldMapping()
	exact.Analyzer = "keyword"
	exact.Store, exact.IncludeTermVectors, exact.IncludeInAll, exact.DocValues = false, false, false, false
	doc.AddFieldMappingsAt("title_exact", exact)
	for _, fieldName := range []string{"part", "offset", "end", "namespace", "primary"} {
		field := bleve.NewNumericFieldMapping()
		field.Store, field.Index, field.IncludeInAll, field.DocValues = true, true, false, false
		doc.AddFieldMappingsAt(fieldName, field)
	}
	if body {
		field := bleve.NewTextFieldMapping()
		field.Store, field.IncludeTermVectors, field.IncludeInAll, field.DocValues = false, false, false, false
		doc.AddFieldMappingsAt("body", field)
		m.ScoringModel = "bm25"
	}
	doc.AddSubDocumentMapping("_all", bleve.NewDocumentDisabledMapping())
	m.DefaultMapping = doc
	return m
}

func newIndex(path string, indexMapping mapping.IndexMapping) (bleve.Index, error) {
	return bleve.NewUsing(path, indexMapping, bleve.Config.DefaultIndexType, bleve.Config.DefaultMemKVStore, map[string]any{
		"scorchPersisterOptions": map[string]any{"NumPersisterWorkers": 2, "MaxSizeInMemoryMergePerWorker": 64 << 20},
		"scorchMergePlanOptions": map[string]any{"FloorSegmentFileSize": 10 << 20},
	})
}

func bodyShardPath(path string, shard int) string {
	return filepath.Join(path, fmt.Sprintf("%03d.bleve", shard))
}

func recordShard(id string) int {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(id))
	return int(hash.Sum32() % bodyShards)
}

func normalize(value string) string {
	return strings.ToLower(strings.TrimSpace(strings.ReplaceAll(value, "_", " ")))
}
func boolPointer(value bool) *bool        { return &value }
func floatPointer(value float64) *float64 { return &value }

func numberField(value any) (float64, bool) {
	switch number := value.(type) {
	case float64:
		return number, true
	case int:
		return float64(number), true
	case uint64:
		return float64(number), true
	default:
		return 0, false
	}
}

func querySnippet(content, query string, maximum int) string {
	compact := strings.Join(strings.Fields(content), " ")
	lower := strings.ToLower(compact)
	position := -1
	for _, term := range strings.Fields(strings.ToLower(query)) {
		if len([]rune(term)) < 3 {
			continue
		}
		if found := strings.Index(lower, term); found >= 0 && (position < 0 || found < position) {
			position = found
		}
	}
	if position < 0 {
		position = 0
	}
	start := max(0, position-maximum/3)
	end := min(len(compact), start+maximum)
	return strings.TrimSpace(compact[start:end])
}
