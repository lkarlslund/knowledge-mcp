package knowledgeindex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

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
	Title           string   `json:"title"`
	TitleExact      string   `json:"title_exact"`
	Body            string   `json:"body,omitempty"`
	URL             string   `json:"url,omitempty"`
	Locator         string   `json:"locator,omitempty"`
	Part            int      `json:"part"`
	Offset          int64    `json:"offset"`
	End             int64    `json:"end"`
	Namespace       int      `json:"namespace"`
	Primary         int      `json:"primary"`
	Identifiers     []string `json:"identifiers,omitempty"`
	IdentifierExact []string `json:"identifier_exact,omitempty"`
	Aliases         []string `json:"aliases,omitempty"`
	Keywords        []string `json:"keywords,omitempty"`
	Status          string   `json:"status,omitempty"`
	RankWeight      float64  `json:"rank_weight"`
}

type buildCheckpoint struct {
	Kind        string    `json:"kind"`
	Version     int       `json:"version"`
	Fingerprint string    `json:"fingerprint"`
	Cursor      string    `json:"cursor"`
	Documents   uint64    `json:"documents"`
	Completed   int64     `json:"completed"`
	Total       int64     `json:"total"`
	UpdatedAt   time.Time `json:"updated_at"`
}

func Current(manifest model.Manifest) (title, body bool) {
	return manifest.TitleReady && manifest.TitleIndexVersion == TitleVersion,
		manifest.BodyReady && manifest.BodyIndexVersion == BodyVersion
}

func BuildTitle(ctx context.Context, path, fingerprint string, corpus provider.Corpus, scanOptions provider.ScanOptions, progress TitleProgress) (uint64, error) {
	destination := filepath.Join(path, TitleDirectory+".building")
	checkpointPath := destination + ".checkpoint.json"
	checkpoint, resume := loadCheckpoint(checkpointPath, destination, "title", TitleVersion, fingerprint)
	var idx bleve.Index
	var err error
	if resume {
		idx, err = openWritableIndex(destination)
	} else {
		if err := os.RemoveAll(destination); err != nil {
			return 0, err
		}
		_ = os.Remove(checkpointPath)
		checkpoint = buildCheckpoint{Kind: "title", Version: TitleVersion, Fingerprint: fingerprint, UpdatedAt: time.Now().UTC()}
		idx, err = newIndex(destination, indexMapping(false))
	}
	if err != nil {
		return 0, err
	}
	closed := false
	defer func() {
		if !closed {
			_ = idx.Close()
		}
	}()
	if !resume {
		if err := saveCheckpoint(checkpointPath, checkpoint); err != nil {
			return 0, err
		}
	}
	batch := idx.NewBatch()
	count := checkpoint.Documents
	lastBoundary := true
	commit := func(position provider.ScanPosition) error {
		if batch.Size() > 0 {
			if err := idx.Batch(batch); err != nil {
				return err
			}
			batch = idx.NewBatch()
		}
		checkpoint.Cursor, checkpoint.Documents = position.Cursor, count
		checkpoint.Completed, checkpoint.Total, checkpoint.UpdatedAt = position.Completed, position.Total, time.Now().UTC()
		if err := saveCheckpoint(checkpointPath, checkpoint); err != nil {
			return err
		}
		progress(count, position.Completed, position.Total)
		return nil
	}
	lastPosition := provider.ScanPosition{Cursor: checkpoint.Cursor, Completed: checkpoint.Completed, Total: checkpoint.Total, Boundary: true}
	err = corpus.ScanTitles(ctx, checkpoint.Cursor, scanOptions, func(record provider.Record, position provider.ScanPosition) error {
		if record.ID == "" || strings.TrimSpace(record.Title) == "" {
			return errors.New("provider emitted title record without ID or title")
		}
		if err := batch.Index(record.ID, toIndexDocument(record, false)); err != nil {
			return err
		}
		count++
		lastPosition, lastBoundary = position, position.Boundary
		if batch.Size() >= titleBatchSize && position.Boundary {
			return commit(position)
		}
		progress(count, position.Completed, position.Total)
		return nil
	})
	if err != nil {
		return 0, err
	}
	if !lastBoundary {
		return 0, errors.New("provider title scan ended outside a safe checkpoint boundary")
	}
	if batch.Size() > 0 {
		if err := commit(lastPosition); err != nil {
			return 0, err
		}
	}
	if err := idx.Close(); err != nil {
		return 0, err
	}
	closed = true
	if err := os.Remove(checkpointPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return 0, err
	}
	return count, nil
}

func BuildBody(ctx context.Context, path, fingerprint string, corpus provider.Corpus, scanOptions provider.ScanOptions, progress BodyProgress) error {
	destination := filepath.Join(path, BodyDirectory+".building")
	checkpointPath := destination + ".checkpoint.json"
	checkpoint, resume := loadCheckpoint(checkpointPath, destination, "body", BodyVersion, fingerprint)
	if !resume {
		if err := os.RemoveAll(destination); err != nil {
			return err
		}
		if err := os.MkdirAll(destination, 0o755); err != nil {
			return err
		}
		_ = os.Remove(checkpointPath)
		checkpoint = buildCheckpoint{Kind: "body", Version: BodyVersion, Fingerprint: fingerprint, UpdatedAt: time.Now().UTC()}
	}
	indexes := make([]bleve.Index, bodyShards)
	for shard := range indexes {
		var idx bleve.Index
		var err error
		if resume {
			idx, err = openWritableIndex(bodyShardPath(destination, shard))
		} else {
			idx, err = newIndex(bodyShardPath(destination, shard), indexMapping(true))
		}
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
	if !resume {
		if err := saveCheckpoint(checkpointPath, checkpoint); err != nil {
			return err
		}
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
	documentCount := checkpoint.Documents
	flushAll := func(position provider.ScanPosition) error {
		for shard := range indexes {
			if err := flush(shard); err != nil {
				return err
			}
		}
		checkpoint.Cursor, checkpoint.Documents = position.Cursor, documentCount
		checkpoint.Completed, checkpoint.Total, checkpoint.UpdatedAt = position.Completed, position.Total, time.Now().UTC()
		if err := saveCheckpoint(checkpointPath, checkpoint); err != nil {
			return err
		}
		progress(position.Completed, position.Total)
		return nil
	}
	pendingBytes := 0
	lastBoundary := true
	lastPosition := provider.ScanPosition{Cursor: checkpoint.Cursor, Completed: checkpoint.Completed, Total: checkpoint.Total, Boundary: true}
	err := corpus.ScanBodies(ctx, checkpoint.Cursor, scanOptions, func(record provider.Record, position provider.ScanPosition) error {
		if record.ID == "" || strings.TrimSpace(record.Title) == "" {
			return errors.New("provider emitted body record without ID or title")
		}
		shard := recordShard(record.ID)
		if err := batches[shard].batch.Index(record.ID, toIndexDocument(record, true)); err != nil {
			return err
		}
		batches[shard].bytes += len(record.Title) + len(record.Body) + len(record.URL) + len(record.Locator)
		pendingBytes += len(record.Title) + len(record.Body) + len(record.URL) + len(record.Locator)
		documentCount++
		lastPosition, lastBoundary = position, position.Boundary
		if pendingBytes >= bodyBatchSize && position.Boundary {
			if err := flushAll(position); err != nil {
				return err
			}
			pendingBytes = 0
		}
		progress(position.Completed, position.Total)
		return nil
	})
	if err != nil {
		return err
	}
	if !lastBoundary {
		return errors.New("provider body scan ended outside a safe checkpoint boundary")
	}
	if pendingBytes > 0 {
		if err := flushAll(lastPosition); err != nil {
			return err
		}
	}
	for shard := range indexes {
		if err := indexes[shard].Close(); err != nil {
			return err
		}
	}
	closed = true
	if err := os.Remove(checkpointPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func toIndexDocument(record provider.Record, body bool) indexDocument {
	primary := 0
	if record.Primary {
		primary = 1
	}
	rankWeight := record.RankWeight
	if rankWeight <= 0 {
		rankWeight = 1
	}
	identifiers := make([]string, 0, len(record.Identifiers))
	for _, identifier := range record.Identifiers {
		if normalized := normalizeIdentifier(identifier); normalized != "" {
			identifiers = append(identifiers, normalized)
		}
	}
	document := indexDocument{Title: record.Title, TitleExact: normalize(record.Title), URL: record.URL, Locator: record.Locator, Part: record.Part, Offset: record.Offset, End: record.End, Namespace: record.Namespace, Primary: primary, Identifiers: record.Identifiers, IdentifierExact: identifiers, Aliases: record.Aliases, Keywords: record.Keywords, Status: record.Status, RankWeight: rankWeight}
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
	request.Fields = []string{"title", "url", "namespace", "primary", "identifiers", "status", "rank_weight"}
	response, err := idx.SearchInContext(ctx, request)
	if err != nil {
		return model.SearchResult{}, err
	}
	hits := make([]model.SearchHit, 0, len(response.Hits))
	for _, hit := range response.Hits {
		title, _ := hit.Fields["title"].(string)
		url, _ := hit.Fields["url"].(string)
		namespace, _ := numberField(hit.Fields["namespace"])
		rankWeight, ok := numberField(hit.Fields["rank_weight"])
		if !ok || rankWeight <= 0 {
			rankWeight = 1
		}
		status, _ := hit.Fields["status"].(string)
		hits = append(hits, model.SearchHit{ID: hit.ID, Title: title, URL: url, Namespace: int(namespace), Score: hit.Score * rankWeight, MatchMode: mode, Identifiers: stringSliceField(hit.Fields["identifiers"]), Status: status})
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
	queries := make([]blevequery.Query, 0, 7)
	title := bleve.NewMatchQuery(text)
	title.SetField("title")
	title.SetBoost(6)
	queries = append(queries, title)
	exact := bleve.NewTermQuery(normalize(text))
	exact.SetField("title_exact")
	exact.SetBoost(20)
	queries = append(queries, exact)
	if normalizedIdentifier := normalizeIdentifier(text); normalizedIdentifier != "" {
		identifier := bleve.NewTermQuery(normalizedIdentifier)
		identifier.SetField("identifier_exact")
		identifier.SetBoost(30)
		queries = append(queries, identifier)
	}
	aliases := bleve.NewMatchQuery(text)
	aliases.SetField("aliases")
	aliases.SetBoost(5)
	queries = append(queries, aliases)
	keywords := bleve.NewMatchQuery(text)
	keywords.SetField("keywords")
	keywords.SetBoost(2.5)
	queries = append(queries, keywords)
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
	identifier := bleve.NewTextFieldMapping()
	identifier.Analyzer = "keyword"
	identifier.Store, identifier.Index, identifier.IncludeTermVectors, identifier.IncludeInAll, identifier.DocValues = true, false, false, false, false
	doc.AddFieldMappingsAt("identifiers", identifier)
	identifierExact := bleve.NewTextFieldMapping()
	identifierExact.Analyzer = "keyword"
	identifierExact.Store, identifierExact.IncludeTermVectors, identifierExact.IncludeInAll, identifierExact.DocValues = false, false, false, false
	doc.AddFieldMappingsAt("identifier_exact", identifierExact)
	for _, fieldName := range []string{"aliases", "keywords", "status"} {
		field := bleve.NewTextFieldMapping()
		field.Store, field.IncludeTermVectors, field.IncludeInAll, field.DocValues = fieldName == "status", false, false, false
		doc.AddFieldMappingsAt(fieldName, field)
	}
	for _, fieldName := range []string{"part", "offset", "end", "namespace", "primary", "rank_weight"} {
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
	return bleve.NewUsing(path, indexMapping, bleve.Config.DefaultIndexType, bleve.Config.DefaultMemKVStore, scorchConfig())
}

func openWritableIndex(path string) (bleve.Index, error) {
	return bleve.OpenUsing(path, scorchConfig())
}

func scorchConfig() map[string]any {
	return map[string]any{
		"scorchPersisterOptions": map[string]any{"NumPersisterWorkers": 2, "MaxSizeInMemoryMergePerWorker": 64 << 20},
		"scorchMergePlanOptions": map[string]any{"FloorSegmentFileSize": 10 << 20},
	}
}

func loadCheckpoint(path, destination, kind string, version int, fingerprint string) (buildCheckpoint, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return buildCheckpoint{}, false
	}
	var checkpoint buildCheckpoint
	if json.Unmarshal(data, &checkpoint) != nil || checkpoint.Kind != kind || checkpoint.Version != version || checkpoint.Fingerprint != fingerprint {
		return buildCheckpoint{}, false
	}
	if info, err := os.Stat(destination); err != nil || !info.IsDir() {
		return buildCheckpoint{}, false
	}
	return checkpoint, true
}

func saveCheckpoint(path string, checkpoint buildCheckpoint) error {
	data, err := json.MarshalIndent(checkpoint, "", "  ")
	if err != nil {
		return err
	}
	temporary := path + ".tmp"
	if err := os.WriteFile(temporary, append(data, '\n'), 0o644); err != nil {
		return err
	}
	return os.Rename(temporary, path)
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

func normalizeIdentifier(value string) string {
	var output strings.Builder
	for _, character := range strings.ToLower(value) {
		if character >= 'a' && character <= 'z' || character >= '0' && character <= '9' {
			output.WriteRune(character)
		}
	}
	return output.String()
}

func stringSliceField(value any) []string {
	switch values := value.(type) {
	case string:
		return []string{values}
	case []string:
		return values
	case []any:
		result := make([]string, 0, len(values))
		for _, value := range values {
			if text, ok := value.(string); ok {
				result = append(result, text)
			}
		}
		return result
	default:
		return nil
	}
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
