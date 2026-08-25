package wikiindex

import (
	"bufio"
	"compress/bzip2"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"html"
	"io"
	"math"
	"math/bits"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"unicode"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/analysis/analyzer/custom"
	"github.com/blevesearch/bleve/v2/analysis/analyzer/keyword"
	"github.com/blevesearch/bleve/v2/analysis/token/lowercase"
	unicodeTokenizer "github.com/blevesearch/bleve/v2/analysis/tokenizer/unicode"
	"github.com/blevesearch/bleve/v2/mapping"
	blevequery "github.com/blevesearch/bleve/v2/search/query"
	"github.com/cosnicolaou/pbzip2"
	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/model"
)

const (
	TitleIndexDir            = "titles.bleve"
	BodyIndexDir             = "bodies.bleve"
	CurrentTitleIndexVersion = 6
	CurrentBodyIndexVersion  = 9
	DefaultReadMaxChars      = 100_000
	MaximumReadMaxChars      = 1_000_000
	titleBatchDocs           = 50_000
	bodyBatchBytes           = 32 << 20
	bodyBatchStreams         = 512
	bodyShardCount           = 4
	bodyShardStreams         = bodyBatchStreams / bodyShardCount
)

type titleDocument struct {
	Title      string `json:"title"`
	TitleExact string `json:"title_exact"`
	Offset     int64  `json:"offset"`
	End        int64  `json:"end"`
	Part       int    `json:"part"`
}

type bodyDocument struct {
	ID         uint64 `json:"-"`
	Title      string `json:"title"`
	TitleExact string `json:"title_exact"`
	Namespace  int    `json:"namespace"`
	Body       string `json:"body"`
}

type stream struct {
	Index  int
	Offset int64
	End    int64
	Part   int
	Path   string
}

type bodyCheckpoint struct {
	Version     int    `json:"version"`
	Shards      int    `json:"shards"`
	Fingerprint string `json:"fingerprint"`
	Total       int    `json:"total"`
	Done        int64  `json:"done"`
	Completed   []byte `json:"completed"`
}

type titlePartCheckpoint struct {
	Lines    int64 `json:"lines"`
	Complete bool  `json:"complete,omitempty"`
}

type titleCheckpoint struct {
	Version        int                   `json:"version"`
	Fingerprint    string                `json:"fingerprint"`
	Pages          uint64                `json:"pages"`
	CompressedDone int64                 `json:"compressed_done"`
	Parts          []titlePartCheckpoint `json:"parts"`
}

type countingReader struct {
	reader io.Reader
	bytes  atomic.Int64
}

func (r *countingReader) Read(buffer []byte) (int, error) {
	n, err := r.reader.Read(buffer)
	r.bytes.Add(int64(n))
	return n, err
}

type Part struct {
	Number    int
	DumpPath  string
	IndexPath string
}

type xmlPage struct {
	Title     string `xml:"title"`
	Namespace int    `xml:"ns"`
	ID        uint64 `xml:"id"`
	Redirect  *struct {
		Title string `xml:"title,attr"`
	} `xml:"redirect"`
	Revision struct {
		ID        uint64 `xml:"id"`
		Timestamp string `xml:"timestamp"`
		Text      string `xml:"text"`
	} `xml:"revision"`
}

type BuildProgress func(done, total int64)
type TitleBuildProgress func(pages uint64, compressedDone, compressedTotal int64)

var redirectDirectiveRE = regexp.MustCompile(`(?i)^#redirect\s*:?\s*\[\[([^\]|]+)`)
var wikiLinkDestinationRE = regexp.MustCompile(`\(wiki:([^)]+)\)`)

var ErrPageNotFound = errors.New("page not found")

func BuildTitle(ctx context.Context, parts []Part, destination string, progress TitleBuildProgress) (uint64, error) {
	checkpointPath := destination + ".checkpoint.json"
	checkpoint, compressedTotal, resume := loadTitleCheckpoint(checkpointPath, destination, parts)
	var idx bleve.Index
	var err error
	if resume {
		idx, err = openWritableIndex(destination)
	} else {
		if err := os.RemoveAll(destination); err != nil {
			return 0, err
		}
		_ = os.Remove(checkpointPath)
		checkpoint, compressedTotal, err = newTitleCheckpoint(parts)
		if err != nil {
			return 0, err
		}
		idx, err = newIndex(destination, titleMapping())
	}
	if err != nil {
		return 0, fmt.Errorf("open title index: %w", err)
	}
	if !resume {
		if err := saveCheckpoint(checkpointPath, checkpoint); err != nil {
			_ = idx.Close()
			return 0, fmt.Errorf("create title checkpoint: %w", err)
		}
	}
	closed := false
	defer func() {
		if !closed {
			_ = idx.Close()
		}
	}()
	count := checkpoint.Pages
	progress(count, checkpoint.CompressedDone, compressedTotal)
	compressedBefore := int64(0)
	for partIndex, part := range parts {
		partSize, err := fileSize(part.IndexPath)
		if err != nil {
			return 0, err
		}
		if checkpoint.Parts[partIndex].Complete {
			compressedBefore += partSize
			continue
		}
		f, err := os.Open(part.IndexPath)
		if err != nil {
			return 0, err
		}
		compressed := &countingReader{reader: f}
		scanner := bufio.NewScanner(pbzip2.NewReader(ctx, compressed))
		scanner.Buffer(make([]byte, 64<<10), 4<<20)
		batch := idx.NewBatch()
		batchCount := 0
		lines := int64(0)
		indexedLines := checkpoint.Parts[partIndex].Lines
		type titleEntry struct {
			pageID uint64
			title  string
		}
		groupOffset := int64(-1)
		var group []titleEntry
		flushGroup := func(end int64) error {
			for _, entry := range group {
				doc := titleDocument{Title: entry.title, TitleExact: normalizeTitle(entry.title), Offset: groupOffset, End: end, Part: part.Number}
				if err := batch.Index(strconv.FormatUint(entry.pageID, 10), doc); err != nil {
					return err
				}
				count++
				batchCount++
				indexedLines++
			}
			group = group[:0]
			return nil
		}
		commit := func(complete bool) error {
			if batchCount > 0 {
				if err := idx.Batch(batch); err != nil {
					return fmt.Errorf("write title index: %w", err)
				}
				batch, batchCount = idx.NewBatch(), 0
			}
			checkpoint.Pages = count
			checkpoint.Parts[partIndex] = titlePartCheckpoint{Lines: indexedLines, Complete: complete}
			checkpoint.CompressedDone = max(checkpoint.CompressedDone, compressedBefore+compressed.bytes.Load())
			if complete {
				checkpoint.CompressedDone = compressedBefore + partSize
			}
			if err := saveCheckpoint(checkpointPath, checkpoint); err != nil {
				return fmt.Errorf("save title checkpoint: %w", err)
			}
			progress(count, checkpoint.CompressedDone, compressedTotal)
			return nil
		}
		for scanner.Scan() {
			select {
			case <-ctx.Done():
				_ = f.Close()
				return 0, ctx.Err()
			default:
			}
			lines++
			if lines <= checkpoint.Parts[partIndex].Lines {
				continue
			}
			offset, pageID, title, parseErr := parseIndexLine(scanner.Text())
			if parseErr != nil {
				_ = f.Close()
				return 0, parseErr
			}
			if groupOffset >= 0 && offset != groupOffset {
				if err := flushGroup(offset); err != nil {
					_ = f.Close()
					return 0, err
				}
				if batchCount >= titleBatchDocs {
					if err := commit(false); err != nil {
						_ = f.Close()
						return 0, err
					}
				}
			}
			groupOffset = offset
			group = append(group, titleEntry{pageID: pageID, title: title})
		}
		if err := scanner.Err(); err != nil {
			_ = f.Close()
			return 0, fmt.Errorf("read multistream index: %w", err)
		}
		dumpSize := int64(1<<63 - 1)
		if part.DumpPath != "" {
			dumpSize, err = fileSize(part.DumpPath)
			if err != nil {
				_ = f.Close()
				return 0, err
			}
		}
		if err := flushGroup(dumpSize); err != nil {
			_ = f.Close()
			return 0, err
		}
		if err := f.Close(); err != nil {
			return 0, err
		}
		if err := commit(true); err != nil {
			return 0, err
		}
		compressedBefore += partSize
	}
	progress(count, compressedTotal, compressedTotal)
	if err := idx.Close(); err != nil {
		return 0, fmt.Errorf("close title index: %w", err)
	}
	closed = true
	if err := os.Remove(checkpointPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return 0, fmt.Errorf("remove title checkpoint: %w", err)
	}
	return count, nil
}

func BuildBody(ctx context.Context, parts []Part, destination string, progress BuildProgress) error {
	var streams []stream
	for _, part := range parts {
		partStreams, err := readStreams(ctx, part)
		if err != nil {
			return err
		}
		streams = append(streams, partStreams...)
	}
	for i := range streams {
		streams[i].Index = i
	}
	checkpointPath := destination + ".checkpoint.json"
	checkpoint, resume := loadBodyCheckpoint(checkpointPath, destination, streams)
	if !resume {
		if err := os.RemoveAll(destination); err != nil {
			return err
		}
		if err := os.MkdirAll(destination, 0o755); err != nil {
			return err
		}
		_ = os.Remove(checkpointPath)
		checkpoint = newBodyCheckpoint(streams)
	}
	indexes := make([]bleve.Index, 0, bodyShardCount)
	for shard := range bodyShardCount {
		var idx bleve.Index
		var err error
		shardPath := bodyShardPath(destination, shard)
		if resume {
			idx, err = openWritableIndex(shardPath)
		} else {
			idx, err = newIndex(shardPath, bodyMapping())
		}
		if err != nil {
			for _, opened := range indexes {
				_ = opened.Close()
			}
			return fmt.Errorf("open body shard %d: %w", shard, err)
		}
		indexes = append(indexes, idx)
	}
	if !resume {
		if err := saveBodyCheckpoint(checkpointPath, checkpoint); err != nil {
			for _, idx := range indexes {
				_ = idx.Close()
			}
			return fmt.Errorf("create body checkpoint: %w", err)
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
	dumps := make(map[string]*os.File, len(parts))
	for _, part := range parts {
		dump, err := os.Open(part.DumpPath)
		if err != nil {
			return err
		}
		dumps[part.DumpPath] = dump
	}
	defer func() {
		for _, dump := range dumps {
			_ = dump.Close()
		}
	}()
	type decoded struct {
		part stream
		docs []bodyDocument
		err  error
	}
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	workerCount := min(runtime.GOMAXPROCS(0), 8)
	jobs := make(chan stream)
	results := make(chan decoded, workerCount)
	var workers sync.WaitGroup
	for range workerCount {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for part := range jobs {
				pages, decodeErr := decodeStream(workerCtx, io.NewSectionReader(dumps[part.Path], part.Offset, part.End-part.Offset))
				docs := make([]bodyDocument, 0, len(pages))
				for _, page := range pages {
					body := ""
					if redirectTarget(page) == "" {
						body = PlainText(page.Revision.Text)
					}
					docs = append(docs, bodyDocument{ID: page.ID, Title: page.Title, TitleExact: normalizeTitle(page.Title), Namespace: page.Namespace, Body: body})
				}
				select {
				case results <- decoded{part: part, docs: docs, err: decodeErr}:
				case <-workerCtx.Done():
					return
				}
			}
		}()
	}
	remaining := make([]stream, 0, len(streams)-int(checkpoint.Done))
	for _, part := range streams {
		if !checkpointComplete(checkpoint, part.Index) {
			remaining = append(remaining, part)
		}
	}
	go func() {
		defer close(jobs)
		for _, part := range remaining {
			select {
			case jobs <- part:
			case <-workerCtx.Done():
				return
			}
		}
	}()
	go func() {
		workers.Wait()
		close(results)
	}()

	type shardState struct {
		batch      *bleve.Batch
		pending    []int
		committing bool
		inFlight   int
	}
	type commitResult struct {
		shard   int
		streams []int
		err     error
	}
	shards := make([]shardState, bodyShardCount)
	for shard := range shards {
		shards[shard] = shardState{batch: indexes[shard].NewBatch(), pending: make([]int, 0, bodyShardStreams)}
	}
	commitResults := make(chan commitResult, bodyShardCount)
	runningCommits := 0
	startCommit := func(shard int) {
		state := &shards[shard]
		batch, pending := state.batch, state.pending
		state.batch = indexes[shard].NewBatch()
		state.pending = make([]int, 0, bodyShardStreams)
		state.committing, state.inFlight = true, len(pending)
		runningCommits++
		go func() {
			commitResults <- commitResult{shard: shard, streams: pending, err: indexes[shard].Batch(batch)}
		}()
	}
	pendingCount := func() int64 {
		var count int64
		for shard := range shards {
			count += int64(len(shards[shard].pending) + shards[shard].inFlight)
		}
		return count
	}
	handleCommit := func(result commitResult) error {
		state := &shards[result.shard]
		state.committing, state.inFlight = false, 0
		runningCommits--
		if result.err != nil {
			return fmt.Errorf("write body shard %d: %w", result.shard, result.err)
		}
		for _, streamIndex := range result.streams {
			checkpointMark(&checkpoint, streamIndex)
		}
		if err := saveBodyCheckpoint(checkpointPath, checkpoint); err != nil {
			return fmt.Errorf("save body checkpoint: %w", err)
		}
		progress(checkpoint.Done+pendingCount(), int64(len(streams)))
		return nil
	}
	ready := func(shard int) bool {
		state := &shards[shard]
		return state.batch.TotalDocsSize() >= bodyBatchBytes || len(state.pending) >= bodyShardStreams
	}
	progress(checkpoint.Done, int64(len(streams)))
	var buildErr error
	for result := range results {
		if result.err != nil {
			cancel()
			if buildErr == nil {
				buildErr = fmt.Errorf("decode stream at %d: %w", result.part.Offset, result.err)
			}
			continue
		}
		if buildErr != nil {
			continue
		}
		shard := result.part.Index % bodyShardCount
		for shards[shard].committing && ready(shard) {
			if err := handleCommit(<-commitResults); err != nil {
				buildErr = err
				cancel()
				break
			}
		}
		if buildErr != nil {
			continue
		}
		for _, doc := range result.docs {
			if err := shards[shard].batch.Index(strconv.FormatUint(doc.ID, 10), doc); err != nil {
				buildErr = err
				cancel()
				break
			}
		}
		if buildErr != nil {
			continue
		}
		shards[shard].pending = append(shards[shard].pending, result.part.Index)
		if ready(shard) && !shards[shard].committing {
			startCommit(shard)
		}
		for runningCommits > 0 {
			select {
			case committed := <-commitResults:
				if err := handleCommit(committed); err != nil {
					buildErr = err
					cancel()
				}
			default:
				goto commitsDrained
			}
		}
	commitsDrained:
		progress(checkpoint.Done+pendingCount(), int64(len(streams)))
	}
	if buildErr == nil {
		buildErr = ctx.Err()
	}
	for runningCommits > 0 {
		if err := handleCommit(<-commitResults); err != nil && buildErr == nil {
			buildErr = err
		}
	}
	if buildErr == nil {
		buildErr = ctx.Err()
	}
	if buildErr == nil {
		for shard := range shards {
			if len(shards[shard].pending) > 0 {
				startCommit(shard)
			}
		}
		for runningCommits > 0 {
			if err := handleCommit(<-commitResults); err != nil && buildErr == nil {
				buildErr = err
			}
		}
	}
	if buildErr != nil {
		return buildErr
	}
	for shard, idx := range indexes {
		if err := idx.Close(); err != nil {
			return fmt.Errorf("close body shard %d: %w", shard, err)
		}
	}
	closed = true
	if err := os.Remove(checkpointPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove body checkpoint: %w", err)
	}
	return nil
}

func newBodyCheckpoint(streams []stream) bodyCheckpoint {
	return bodyCheckpoint{
		Version:     CurrentBodyIndexVersion,
		Shards:      bodyShardCount,
		Fingerprint: bodyCheckpointFingerprint(streams),
		Total:       len(streams),
		Completed:   make([]byte, (len(streams)+7)/8),
	}
}

func newTitleCheckpoint(parts []Part) (titleCheckpoint, int64, error) {
	total, fingerprint, err := titleCheckpointMetadata(parts)
	if err != nil {
		return titleCheckpoint{}, 0, err
	}
	return titleCheckpoint{Version: CurrentTitleIndexVersion, Fingerprint: fingerprint, Parts: make([]titlePartCheckpoint, len(parts))}, total, nil
}

func loadTitleCheckpoint(path, destination string, parts []Part) (titleCheckpoint, int64, bool) {
	total, fingerprint, err := titleCheckpointMetadata(parts)
	if err != nil {
		return titleCheckpoint{}, 0, false
	}
	if info, err := os.Stat(destination); err != nil || !info.IsDir() {
		return titleCheckpoint{}, total, false
	}
	var checkpoint titleCheckpoint
	data, err := os.ReadFile(path)
	if err != nil || json.Unmarshal(data, &checkpoint) != nil {
		return titleCheckpoint{}, total, false
	}
	if checkpoint.Version != CurrentTitleIndexVersion || checkpoint.Fingerprint != fingerprint || len(checkpoint.Parts) != len(parts) || checkpoint.CompressedDone < 0 || checkpoint.CompressedDone > total {
		return titleCheckpoint{}, total, false
	}
	return checkpoint, total, true
}

func titleCheckpointMetadata(parts []Part) (int64, string, error) {
	hash := sha256.New()
	var total int64
	for _, part := range parts {
		size, err := fileSize(part.IndexPath)
		if err != nil {
			return 0, "", err
		}
		total += size
		_, _ = fmt.Fprintf(hash, "%d:%d\x00", part.Number, size)
	}
	return total, hex.EncodeToString(hash.Sum(nil)), nil
}

func fileSize(path string) (int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func loadBodyCheckpoint(path, destination string, streams []stream) (bodyCheckpoint, bool) {
	var checkpoint bodyCheckpoint
	if info, err := os.Stat(destination); err != nil || !info.IsDir() {
		return checkpoint, false
	}
	data, err := os.ReadFile(path)
	if err != nil || json.Unmarshal(data, &checkpoint) != nil {
		return bodyCheckpoint{}, false
	}
	wantBytes := (len(streams) + 7) / 8
	if checkpoint.Version != CurrentBodyIndexVersion || checkpoint.Shards != bodyShardCount || checkpoint.Total != len(streams) || len(checkpoint.Completed) != wantBytes || checkpoint.Fingerprint != bodyCheckpointFingerprint(streams) {
		return bodyCheckpoint{}, false
	}
	var done int64
	for _, value := range checkpoint.Completed {
		done += int64(bits.OnesCount8(value))
	}
	if done > int64(len(streams)) {
		return bodyCheckpoint{}, false
	}
	checkpoint.Done = done
	return checkpoint, true
}

func saveBodyCheckpoint(path string, checkpoint bodyCheckpoint) error {
	return saveCheckpoint(path, checkpoint)
}

func saveCheckpoint(path string, checkpoint any) error {
	data, err := json.Marshal(checkpoint)
	if err != nil {
		return err
	}
	temporary := path + ".tmp"
	if err := os.WriteFile(temporary, append(data, '\n'), 0o644); err != nil {
		return err
	}
	if err := os.Rename(temporary, path); err != nil {
		_ = os.Remove(temporary)
		return err
	}
	return nil
}

func bodyCheckpointFingerprint(streams []stream) string {
	hash := sha256.New()
	_, _ = fmt.Fprintf(hash, "shards:%d\x00", bodyShardCount)
	for _, part := range streams {
		_, _ = fmt.Fprintf(hash, "%d:%d:%d\x00", part.Part, part.Offset, part.End)
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func bodyShardPath(destination string, shard int) string {
	return filepath.Join(destination, fmt.Sprintf("%03d.bleve", shard))
}

func checkpointComplete(checkpoint bodyCheckpoint, index int) bool {
	return index >= 0 && index < checkpoint.Total && checkpoint.Completed[index/8]&(1<<uint(index%8)) != 0
}

func checkpointMark(checkpoint *bodyCheckpoint, index int) {
	if checkpointComplete(*checkpoint, index) {
		return
	}
	checkpoint.Completed[index/8] |= 1 << uint(index%8)
	checkpoint.Done++
}

type Reader struct {
	generationPath string
	title          bleve.Index
	body           bleve.Index
	bodyShards     []bleve.Index
	mu             sync.RWMutex
	closed         bool
}

func OpenReader(generationPath string, fullText bool) (*Reader, error) {
	reader := &Reader{generationPath: generationPath}
	if fullText {
		shards := make([]bleve.Index, 0, bodyShardCount)
		for shard := range bodyShardCount {
			body, err := bleve.OpenUsing(bodyShardPath(filepath.Join(generationPath, BodyIndexDir), shard), map[string]interface{}{"read_only": true})
			if err != nil {
				for _, opened := range shards {
					_ = opened.Close()
				}
				return nil, fmt.Errorf("open full_text shard %d: %w", shard, err)
			}
			shards = append(shards, body)
		}
		reader.bodyShards = shards
		alias := bleve.NewIndexAlias(shards...)
		if err := alias.SetIndexMapping(bodyMapping()); err != nil {
			for _, opened := range shards {
				_ = opened.Close()
			}
			return nil, fmt.Errorf("configure full_text scoring: %w", err)
		}
		reader.body = alias
		if _, err := os.Stat(filepath.Join(generationPath, TitleIndexDir)); errors.Is(err, os.ErrNotExist) {
			return reader, nil
		}
	}
	title, err := bleve.OpenUsing(filepath.Join(generationPath, TitleIndexDir), map[string]interface{}{"read_only": true})
	if err != nil {
		return nil, fmt.Errorf("open title index: %w", err)
	}
	reader.title = title
	return reader, nil
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
	for _, shard := range r.bodyShards {
		errs = append(errs, shard.Close())
	}
	if r.title != nil {
		errs = append(errs, r.title.Close())
	}
	return errors.Join(errs...)
}

func (r *Reader) Retain() (func(), error) {
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return nil, errors.New("index reader is closed")
	}
	return r.mu.RUnlock, nil
}

func Search(generationPath, query string, offset, limit int, fullText bool) (model.SearchResult, error) {
	reader, err := OpenReader(generationPath, fullText)
	if err != nil {
		return model.SearchResult{}, err
	}
	defer func() { _ = reader.Close() }()
	return reader.Search(context.Background(), query, model.SearchOptions{Offset: offset, Limit: limit, Snippets: fullText}, fullText)
}

func (r *Reader) Search(ctx context.Context, query string, options model.SearchOptions, fullText bool) (model.SearchResult, error) {
	if strings.TrimSpace(query) == "" {
		return model.SearchResult{}, errors.New("query cannot be empty")
	}
	if options.Offset < 0 {
		options.Offset = 0
	}
	if options.Limit <= 0 || options.Limit > 50 {
		options.Limit = 10
	}
	mode := "title"
	idx := r.title
	if fullText {
		mode = "full_text"
		idx = r.body
		if idx == nil {
			return model.SearchResult{}, errors.New("full-text index is not open")
		}
	}
	if idx == nil {
		return model.SearchResult{}, errors.New("title index is not open")
	}
	ranked := rankingQuery(query, fullText)
	if fullText && !options.IncludeNonArticles {
		ranked = namespaceZeroQuery(ranked)
	}
	// Retrieve a substantially wider pool than the caller requested. BM25 and
	// the inexpensive phrase/all-term signals rank this pool; redirect
	// canonicalization and deduplication happen before the requested page is cut.
	candidateLimit := max(250, options.Offset+options.Limit+64)
	candidateLimit = min(candidateLimit, 5_000)
	total, candidates, err := searchTier(ctx, idx, ranked, 0, candidateLimit, map[bool]string{true: "bm25", false: "title"}[fullText])
	if err != nil {
		return model.SearchResult{}, err
	}
	if exact, ok := r.exactCanonicalSearchHit(ctx, query, options.IncludeNonArticles); ok {
		candidates = append([]model.SearchHit{exact}, candidates...)
	}
	candidates = deduplicateSearchHits(candidates)
	start := min(options.Offset, len(candidates))
	end := min(start+options.Limit, len(candidates))
	hits := candidates[start:end]
	if total < uint64(len(candidates)) {
		total = uint64(len(candidates))
	}
	result := model.SearchResult{Query: query, SearchMode: mode, Total: total, Offset: options.Offset, NamespaceFilterApplied: fullText && !options.IncludeNonArticles, SnippetsAvailable: fullText, SnippetsComplete: fullText, Hits: hits}
	if options.Offset+len(hits) < int(total) {
		result.NextOffset = options.Offset + len(hits)
	}
	if fullText && options.Snippets && r.title != nil {
		pageIDs := make([]uint64, 0, len(result.Hits))
		for _, hit := range result.Hits {
			pageIDs = append(pageIDs, hit.PageID)
		}
		pages := r.loadPagesByID(ctx, pageIDs)
		for index := range result.Hits {
			page, ok := pages[result.Hits[index].PageID]
			if !ok {
				result.SnippetsComplete = false
				result.SnippetErrors++
				continue
			}
			result.Hits[index].Snippet = querySnippet(PlainText(page.Revision.Text), query, 500)
		}
	}
	return result, nil
}

func (r *Reader) exactCanonicalSearchHit(ctx context.Context, query string, includeNonArticles bool) (model.SearchHit, bool) {
	if r.title == nil {
		return model.SearchHit{}, false
	}
	page, err := r.loadPage(ctx, query, 0)
	if err != nil {
		return model.SearchHit{}, false
	}
	matchedTitle := page.Title
	redirected := false
	visited := map[uint64]struct{}{page.ID: {}}
	for range 8 {
		target := redirectTarget(page)
		if target == "" {
			break
		}
		targetTitle, _ := splitRedirectTarget(target)
		next, loadErr := r.loadPage(ctx, targetTitle, 0)
		if loadErr != nil {
			return model.SearchHit{}, false
		}
		if _, exists := visited[next.ID]; exists {
			return model.SearchHit{}, false
		}
		visited[next.ID] = struct{}{}
		page, redirected = next, true
	}
	if !includeNonArticles && page.Namespace != 0 {
		return model.SearchHit{}, false
	}
	mode := "exact_title"
	if redirected {
		mode = "exact_redirect"
	}
	return model.SearchHit{PageID: page.ID, Title: page.Title, Namespace: page.Namespace, Score: math.MaxFloat64, MatchMode: mode, MatchedTitle: matchedTitle}, true
}

func deduplicateSearchHits(hits []model.SearchHit) []model.SearchHit {
	seenPageIDs := make(map[uint64]struct{}, len(hits))
	seenTitles := make(map[string]struct{}, len(hits))
	result := make([]model.SearchHit, 0, len(hits))
	for _, hit := range hits {
		if _, exists := seenPageIDs[hit.PageID]; exists {
			continue
		}
		if _, exists := seenTitles[normalizeTitle(hit.Title)]; exists {
			continue
		}
		seenPageIDs[hit.PageID] = struct{}{}
		seenTitles[normalizeTitle(hit.Title)] = struct{}{}
		if hit.MatchedTitle != "" {
			seenTitles[normalizeTitle(hit.MatchedTitle)] = struct{}{}
		}
		result = append(result, hit)
	}
	return result
}

func (r *Reader) loadPagesByID(ctx context.Context, pageIDs []uint64) map[uint64]xmlPage {
	loaded := make(map[uint64]xmlPage, len(pageIDs))
	if len(pageIDs) == 0 || r.title == nil {
		return loaded
	}
	docIDs := make([]string, 0, len(pageIDs))
	for _, pageID := range pageIDs {
		docIDs = append(docIDs, strconv.FormatUint(pageID, 10))
	}
	req := bleve.NewSearchRequestOptions(bleve.NewDocIDQuery(docIDs), len(docIDs), 0, false)
	req.Fields = []string{"offset", "end", "part"}
	response, err := r.title.SearchInContext(ctx, req)
	if err != nil {
		return loaded
	}
	type location struct {
		part        int
		offset, end int64
	}
	groups := make(map[location]map[uint64]struct{})
	for _, hit := range response.Hits {
		pageID, parseErr := strconv.ParseUint(hit.ID, 10, 64)
		offset, offsetOK := hit.Fields["offset"].(float64)
		end, endOK := hit.Fields["end"].(float64)
		part, partOK := hit.Fields["part"].(float64)
		if parseErr != nil || !offsetOK || !endOK || !partOK {
			continue
		}
		key := location{part: int(part), offset: int64(offset), end: int64(end)}
		if groups[key] == nil {
			groups[key] = make(map[uint64]struct{})
		}
		groups[key][pageID] = struct{}{}
	}
	for key, wanted := range groups {
		if ctx.Err() != nil {
			break
		}
		dump, openErr := os.Open(filepath.Join(r.generationPath, "parts", fmt.Sprintf("%03d.dump.bz2", key.part)))
		if openErr != nil {
			continue
		}
		pages, decodeErr := decodeStream(ctx, io.NewSectionReader(dump, key.offset, key.end-key.offset))
		_ = dump.Close()
		if decodeErr != nil {
			continue
		}
		for _, page := range pages {
			if _, ok := wanted[page.ID]; ok {
				loaded[page.ID] = page
			}
		}
	}
	return loaded
}

func rankingQuery(query string, fullText bool) blevequery.Query {
	title := bleve.NewMatchQuery(query)
	title.SetField("title")
	title.SetBoost(4)
	title.SetOperator(blevequery.MatchQueryOperatorOr)
	queries := []blevequery.Query{title}
	if fullText {
		exact := bleve.NewTermQuery(normalizeTitle(query))
		exact.SetField("title_exact")
		exact.SetBoost(20)
		body := bleve.NewMatchQuery(query)
		body.SetField("body")
		body.SetOperator(blevequery.MatchQueryOperatorOr)
		titlePhrase := bleve.NewMatchPhraseQuery(query)
		titlePhrase.SetField("title")
		titlePhrase.SetBoost(8)
		bodyPhrase := bleve.NewMatchPhraseQuery(query)
		bodyPhrase.SetField("body")
		bodyPhrase.SetBoost(3)
		must := make([]blevequery.Query, 0, len(strings.Fields(query)))
		for _, term := range strings.Fields(query) {
			titleTerm := bleve.NewMatchQuery(term)
			titleTerm.SetField("title")
			titleTerm.SetBoost(4)
			bodyTerm := bleve.NewMatchQuery(term)
			bodyTerm.SetField("body")
			must = append(must, bleve.NewDisjunctionQuery(titleTerm, bodyTerm))
		}
		allTerms := bleve.NewConjunctionQuery(must...)
		allTerms.SetBoost(2)
		queries = []blevequery.Query{exact, titlePhrase, bodyPhrase, allTerms, title, body}
	}
	return bleve.NewDisjunctionQuery(queries...)
}

func namespaceZeroQuery(query blevequery.Query) blevequery.Query {
	zero := float64(0)
	inclusive := true
	namespace := bleve.NewNumericRangeInclusiveQuery(&zero, &zero, &inclusive, &inclusive)
	namespace.SetField("namespace")
	filtered := bleve.NewBooleanQuery()
	filtered.AddMust(query)
	filtered.AddFilter(namespace)
	return filtered
}

func searchTier(ctx context.Context, idx bleve.Index, query blevequery.Query, offset, limit int, matchMode string) (uint64, []model.SearchHit, error) {
	req := bleve.NewSearchRequestOptions(query, limit, max(0, offset), false)
	req.Fields = []string{"title", "namespace"}
	req.SortBy([]string{"-_score", "_id"})
	response, err := idx.SearchInContext(ctx, req)
	if err != nil {
		return 0, nil, fmt.Errorf("search: %w", err)
	}
	hits := make([]model.SearchHit, 0, len(response.Hits))
	for _, hit := range response.Hits {
		pageID, _ := strconv.ParseUint(hit.ID, 10, 64)
		title, _ := hit.Fields["title"].(string)
		namespace, _ := hit.Fields["namespace"].(float64)
		score := hit.Score
		if math.IsNaN(score) || math.IsInf(score, 0) {
			score = 0
		}
		hits = append(hits, model.SearchHit{PageID: pageID, Title: title, Namespace: int(namespace), Score: score, MatchMode: matchMode})
	}
	return response.Total, hits, nil
}

func querySnippet(content, query string, maximum int) string {
	runes := []rune(strings.TrimSpace(content))
	if len(runes) <= maximum {
		return string(runes)
	}
	terms := strings.Fields(strings.ToLower(query))
	lower := strings.ToLower(string(runes))
	candidates := []int{0}
	for _, term := range terms {
		remaining, byteBase := lower, 0
		for range 64 {
			byteIndex := strings.Index(remaining, term)
			if byteIndex < 0 {
				break
			}
			absolute := byteBase + byteIndex
			candidates = append(candidates, len([]rune(lower[:absolute])))
			advance := byteIndex + len(term)
			byteBase += advance
			remaining = remaining[advance:]
		}
	}
	bestCenter, bestScore := 0, -1
	for _, center := range candidates {
		start := max(0, center-maximum/3)
		end := min(len(runes), start+maximum)
		window := strings.ToLower(string(runes[start:end]))
		score := 0
		for _, term := range terms {
			if strings.Contains(window, term) {
				score += 100 + min(strings.Count(window, term), 10)
			}
		}
		if score > bestScore {
			bestCenter, bestScore = center, score
		}
	}
	start := max(0, bestCenter-maximum/3)
	end := min(len(runes), start+maximum)
	start = max(0, end-maximum)
	for index := start; index < min(end, start+maximum/5); index++ {
		if index > 0 && unicode.IsSpace(runes[index]) && (runes[index-1] == '.' || runes[index-1] == '!' || runes[index-1] == '?') {
			start = index + 1
			break
		}
	}
	for index := end; index > max(start, end-maximum/5); index-- {
		if unicode.IsSpace(runes[index-1]) && index >= 2 && (runes[index-2] == '.' || runes[index-2] == '!' || runes[index-2] == '?') {
			end = index - 1
			break
		}
	}
	for start > 0 && start < len(runes) && !unicode.IsSpace(runes[start-1]) {
		start++
	}
	for end < len(runes) && end > start && !unicode.IsSpace(runes[end-1]) {
		end--
	}
	snippet := strings.TrimSpace(string(runes[start:end]))
	if start > 0 {
		snippet = "…" + snippet
	}
	if end < len(runes) {
		snippet += "…"
	}
	return snippet
}

func ReadPage(generationPath, title string, pageID uint64, format string, start, maxChars int) (model.Page, error) {
	reader, err := OpenReader(generationPath, false)
	if err != nil {
		return model.Page{}, err
	}
	defer func() { _ = reader.Close() }()
	return reader.ReadPage(context.Background(), title, pageID, model.ReadOptions{Format: format, Offset: start, MaxChars: maxChars, FollowRedirects: true}, "")
}

func (r *Reader) ReadPage(ctx context.Context, title string, pageID uint64, options model.ReadOptions, baseURL string) (model.Page, error) {
	if title == "" && pageID == 0 || title != "" && pageID != 0 {
		return model.Page{}, errors.New("provide exactly one of title or page_id")
	}
	if r.title == nil {
		return model.Page{}, errors.New("title index is not open")
	}
	source, err := r.loadPage(ctx, title, pageID)
	if err != nil {
		return model.Page{}, err
	}
	requestedTitle, requestedPageID := source.Title, source.ID
	visited := map[uint64]struct{}{source.ID: {}}
	chain := make([]model.RedirectHop, 0)
	redirectSection := ""
	for target := redirectTarget(source); target != ""; target = redirectTarget(source) {
		targetTitle, fragment := splitRedirectTarget(target)
		chain = append(chain, model.RedirectHop{FromTitle: source.Title, FromPageID: source.ID, ToTitle: targetTitle, Fragment: fragment})
		if fragment != "" {
			redirectSection = fragment
		}
		if !options.FollowRedirects {
			break
		}
		if len(chain) > 8 {
			return model.Page{}, fmt.Errorf("redirect chain from %q exceeds 8 hops", requestedTitle)
		}
		source, err = r.loadPage(ctx, targetTitle, 0)
		if err != nil {
			return model.Page{}, fmt.Errorf("follow redirect from %q to %q: %w", chain[len(chain)-1].FromTitle, targetTitle, err)
		}
		if _, exists := visited[source.ID]; exists {
			return model.Page{}, fmt.Errorf("redirect loop from %q through %q", requestedTitle, source.Title)
		}
		visited[source.ID] = struct{}{}
	}

	content := source.Revision.Text
	var references []MarkdownReference
	var sectionFound *bool
	section := strings.TrimSpace(options.Section)
	if section == "" && options.FollowRedirects {
		section = redirectSection
	}
	outline := pageSectionOutline(content)
	switch options.Format {
	case "", "markdown":
		linkBaseURL := baseURL
		if options.LinkWiki != "" {
			linkBaseURL = ""
		}
		document := RenderMarkdown(content, linkBaseURL)
		options.Format, content, references = "markdown", document.Content, document.References
		if section != "" {
			var found bool
			content, found = extractMarkdownSection(content, section)
			sectionFound = boolPointer(found)
			if !found && options.Section != "" {
				content = ""
			}
		}
	case "text":
		if section != "" {
			var found bool
			content, found = extractWikitextSection(content, section)
			sectionFound = boolPointer(found)
			if !found && options.Section != "" {
				content = ""
			}
		}
		content = PlainText(content)
	case "wikitext":
		if section != "" {
			var found bool
			content, found = extractWikitextSection(content, section)
			sectionFound = boolPointer(found)
			if !found && options.Section != "" {
				content = ""
			}
		}
	default:
		return model.Page{}, errors.New("format must be markdown, text, or wikitext")
	}
	if options.Offset < 0 {
		options.Offset = 0
	}
	runes := []rune(content)
	if options.Offset > len(runes) {
		options.Offset = len(runes)
	}
	if options.MaxChars <= 0 {
		options.MaxChars = DefaultReadMaxChars
	} else if options.MaxChars > MaximumReadMaxChars {
		options.MaxChars = MaximumReadMaxChars
	}
	end := min(options.Offset+options.MaxChars, len(runes))
	if options.AlignBoundaries && end < len(runes) {
		end = readableChunkEnd(runes, options.Offset, end)
	}
	excerpt := string(runes[options.Offset:end])
	pageReferences := make([]model.PageReference, 0)
	referencesTruncated := false
	var omittedReferenceIDs []int
	referenceBudget := options.ReferenceBudgetChars
	if referenceBudget <= 0 {
		referenceBudget = int(^uint(0) >> 1)
	}
	referenceMaximum := options.ReferenceMaxChars
	if referenceMaximum <= 0 {
		referenceMaximum = int(^uint(0) >> 1)
	}
	for _, reference := range referencedMarkdownDefinitions(excerpt, references) {
		original := []rune(reference.Content)
		allowed := min(len(original), referenceMaximum, referenceBudget)
		if allowed <= 0 {
			omittedReferenceIDs = append(omittedReferenceIDs, reference.ID)
			referencesTruncated = true
			continue
		}
		truncated := allowed < len(original)
		pageReferences = append(pageReferences, model.PageReference{ID: reference.ID, Name: reference.Name, Content: string(original[:allowed]), Truncated: truncated, OriginalChars: len(original)})
		referenceBudget -= allowed
		referencesTruncated = referencesTruncated || truncated
	}
	if options.Format == "markdown" && options.LinkWiki != "" {
		documents := make([]string, 1, len(pageReferences)+1)
		documents[0] = excerpt
		for _, reference := range pageReferences {
			documents = append(documents, reference.Content)
		}
		documents = r.rewriteWikiReadLinks(ctx, options.LinkWiki, source.ID, documents)
		excerpt = documents[0]
		for index := range pageReferences {
			pageReferences[index].Content = documents[index+1]
		}
	}
	pageURLTarget := source.Title
	returnedSection := ""
	if section != "" {
		pageURLTarget += "#" + section
		returnedSection = section
	}
	page := model.Page{PageID: source.ID, RevisionID: source.Revision.ID, Title: source.Title, Timestamp: source.Revision.Timestamp, PageURL: PageURL(baseURL, pageURLTarget), Redirected: len(chain) > 0, RedirectChain: chain, Section: returnedSection, SectionFound: sectionFound, Format: options.Format, Content: excerpt, Offset: options.Offset, ReturnedChars: end - options.Offset, TotalChars: len(runes), References: pageReferences, ReferencesTruncated: referencesTruncated, OmittedReferenceIDs: omittedReferenceIDs, Truncated: end < len(runes)}
	if page.Truncated {
		page.NextOffset = end
	}
	if options.IncludeOutline || options.Section != "" && sectionFound != nil && !*sectionFound {
		page.Sections = outline
		if len(page.Sections) > 200 {
			page.Sections = page.Sections[:200]
			page.OutlineTruncated = true
		}
	}
	if len(chain) > 0 {
		page.RequestedTitle, page.RequestedPageID = requestedTitle, requestedPageID
	}
	return page, nil
}

func (r *Reader) rewriteWikiReadLinks(ctx context.Context, wiki string, currentPageID uint64, documents []string) []string {
	targets := make(map[string]string)
	for _, document := range documents {
		for _, match := range wikiLinkDestinationRE.FindAllStringSubmatch(document, -1) {
			title, _ := decodeWikiLinkTarget(match[1])
			if title != "" && len(targets) < 256 {
				targets[normalizeTitle(title)] = title
			}
		}
	}
	pageIDs := r.resolveWikiLinkPageIDs(ctx, targets)
	for index, document := range documents {
		documents[index] = wikiLinkDestinationRE.ReplaceAllStringFunc(document, func(link string) string {
			raw := link[len("(wiki:") : len(link)-1]
			title, section := decodeWikiLinkTarget(raw)
			resolvedPageID := pageIDs[normalizeTitle(title)]
			if title == "" {
				resolvedPageID = currentPageID
			}
			destination, hint := wikiReadLink(wiki, title, resolvedPageID, section)
			return "(" + destination + ` "` + escapeMarkdownLinkTitle(hint) + `")`
		})
	}
	return documents
}

func decodeWikiLinkTarget(raw string) (string, string) {
	title, section, _ := strings.Cut(raw, "#")
	if decoded, err := url.PathUnescape(title); err == nil {
		title = decoded
	}
	if decoded, err := url.PathUnescape(section); err == nil {
		section = decoded
	}
	return strings.ReplaceAll(title, "_", " "), strings.ReplaceAll(section, "_", " ")
}

func (r *Reader) resolveWikiLinkPageIDs(ctx context.Context, targets map[string]string) map[string]uint64 {
	resolved := make(map[string]uint64, len(targets))
	if r.title == nil || len(targets) == 0 {
		return resolved
	}
	queries := make([]blevequery.Query, 0, len(targets))
	for normalized := range targets {
		term := bleve.NewTermQuery(normalized)
		term.SetField("title_exact")
		queries = append(queries, term)
	}
	request := bleve.NewSearchRequestOptions(bleve.NewDisjunctionQuery(queries...), min(len(targets)*20, 5_000), 0, false)
	request.Fields = []string{"title"}
	response, err := r.title.SearchInContext(ctx, request)
	if err != nil {
		return resolved
	}
	for _, hit := range response.Hits {
		title, _ := hit.Fields["title"].(string)
		normalized := normalizeTitle(title)
		if _, wanted := targets[normalized]; !wanted {
			continue
		}
		pageID, parseErr := strconv.ParseUint(hit.ID, 10, 64)
		if parseErr == nil {
			resolved[normalized] = pageID
		}
	}
	return resolved
}

func wikiReadLink(wiki, title string, pageID uint64, section string) (string, string) {
	destination := "wiki-read://read?wiki=" + url.QueryEscape(wiki)
	hint := "Call wiki_read with wiki=" + wiki
	if pageID != 0 {
		destination += "&page_id=" + strconv.FormatUint(pageID, 10)
		hint += " and page_id=" + strconv.FormatUint(pageID, 10)
	} else {
		destination += "&title=" + url.QueryEscape(title)
		hint += " and title=" + title
	}
	if section != "" {
		destination += "&section=" + url.QueryEscape(section)
		hint += " and section=" + section
	}
	return destination, hint
}

func escapeMarkdownLinkTitle(title string) string {
	return strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(title)
}

func (r *Reader) loadPage(ctx context.Context, title string, pageID uint64) (xmlPage, error) {
	var q blevequery.Query
	if pageID != 0 {
		q = bleve.NewDocIDQuery([]string{strconv.FormatUint(pageID, 10)})
	} else {
		term := bleve.NewTermQuery(normalizeTitle(title))
		term.SetField("title_exact")
		q = term
	}
	limit := 1
	if title != "" {
		limit = 20
	}
	req := bleve.NewSearchRequestOptions(q, limit, 0, false)
	req.Fields = []string{"title", "offset", "end", "part"}
	res, err := r.title.SearchInContext(ctx, req)
	if err != nil {
		return xmlPage{}, err
	}
	if len(res.Hits) == 0 {
		return xmlPage{}, ErrPageNotFound
	}
	hit := res.Hits[0]
	wantedTitle := strings.ReplaceAll(strings.TrimSpace(title), "_", " ")
	for _, candidate := range res.Hits {
		candidateTitle, _ := candidate.Fields["title"].(string)
		if candidateTitle == wantedTitle {
			hit = candidate
			break
		}
	}
	wantedID, _ := strconv.ParseUint(hit.ID, 10, 64)
	streamOffset := int64(hit.Fields["offset"].(float64))
	streamEnd := int64(hit.Fields["end"].(float64))
	partNumber := int(hit.Fields["part"].(float64))
	dump, err := os.Open(filepath.Join(r.generationPath, "parts", fmt.Sprintf("%03d.dump.bz2", partNumber)))
	if err != nil {
		return xmlPage{}, err
	}
	defer func() { _ = dump.Close() }()
	pages, err := decodeStream(ctx, io.NewSectionReader(dump, streamOffset, streamEnd-streamOffset))
	if err != nil {
		return xmlPage{}, fmt.Errorf("decode page stream: %w", err)
	}
	for _, source := range pages {
		if source.ID != wantedID {
			continue
		}
		return source, nil
	}
	return xmlPage{}, errors.New("page was absent from its indexed stream")
}

func redirectTarget(page xmlPage) string {
	source := strings.TrimPrefix(page.Revision.Text, "\ufeff")
	match := redirectDirectiveRE.FindStringSubmatch(source)
	if len(match) >= 2 {
		return strings.TrimSpace(match[1])
	}
	if page.Redirect != nil {
		return strings.TrimSpace(page.Redirect.Title)
	}
	return ""
}

func splitRedirectTarget(target string) (string, string) {
	title, fragment, _ := strings.Cut(strings.TrimSpace(target), "#")
	if decoded, err := url.PathUnescape(fragment); err == nil {
		fragment = decoded
	}
	return strings.TrimSpace(strings.ReplaceAll(title, "_", " ")), strings.TrimSpace(strings.ReplaceAll(fragment, "_", " "))
}

func extractWikitextSection(content, fragment string) (string, bool) {
	lines := strings.Split(content, "\n")
	start, level := -1, 0
	for index, line := range lines {
		headingLevel, heading, ok := wikiHeading(line)
		if !ok || normalizeSectionHeading(heading) != normalizeSectionHeading(fragment) {
			continue
		}
		start, level = index, headingLevel
		break
	}
	if start < 0 {
		return content, false
	}
	end := len(lines)
	for index := start + 1; index < len(lines); index++ {
		headingLevel, _, ok := wikiHeading(lines[index])
		if ok && headingLevel <= level {
			end = index
			break
		}
	}
	return strings.TrimSpace(strings.Join(lines[start:end], "\n")), true
}

func extractMarkdownSection(content, fragment string) (string, bool) {
	lines := strings.Split(content, "\n")
	start, level := -1, 0
	for index, line := range lines {
		headingLevel, heading, ok := markdownHeading(line)
		if !ok || normalizeSectionHeading(heading) != normalizeSectionHeading(fragment) {
			continue
		}
		start, level = index, headingLevel
		break
	}
	if start < 0 {
		return content, false
	}
	end := len(lines)
	for index := start + 1; index < len(lines); index++ {
		headingLevel, _, ok := markdownHeading(lines[index])
		if ok && headingLevel <= level {
			end = index
			break
		}
	}
	return strings.TrimSpace(strings.Join(lines[start:end], "\n")), true
}

func markdownHeading(line string) (int, string, bool) {
	trimmed := strings.TrimSpace(line)
	level := 0
	for level < len(trimmed) && trimmed[level] == '#' {
		level++
	}
	if level == 0 || level > 6 || level >= len(trimmed) || trimmed[level] != ' ' {
		return 0, "", false
	}
	return level, strings.TrimSpace(trimmed[level+1:]), true
}

func normalizeSectionHeading(value string) string {
	value = html.UnescapeString(value)
	value = strings.ReplaceAll(value, "_", " ")
	return strings.ToLower(strings.Join(strings.Fields(value), " "))
}

func pageSectionOutline(content string) []model.PageSection {
	sections := make([]model.PageSection, 0)
	for _, line := range strings.Split(content, "\n") {
		level, heading, ok := wikiHeading(line)
		if !ok {
			continue
		}
		heading = strings.TrimSpace(PlainText(heading))
		if heading == "" {
			continue
		}
		sections = append(sections, model.PageSection{Heading: heading, Anchor: strings.ReplaceAll(strings.Join(strings.Fields(heading), " "), " ", "_"), Level: level})
	}
	return sections
}

func readableChunkEnd(content []rune, start, hardEnd int) int {
	if hardEnd <= start || hardEnd >= len(content) {
		return hardEnd
	}
	minimum := start + (hardEnd-start)*7/10
	for index := hardEnd; index > minimum; index-- {
		if index >= 2 && content[index-1] == '\n' && content[index-2] == '\n' {
			return index
		}
	}
	for index := hardEnd; index > minimum; index-- {
		if content[index-1] == '\n' {
			return index
		}
	}
	for index := hardEnd; index > minimum; index-- {
		if index < len(content) && unicode.IsSpace(content[index]) && (content[index-1] == '.' || content[index-1] == '!' || content[index-1] == '?') {
			return index
		}
	}
	return hardEnd
}

func boolPointer(value bool) *bool { return &value }

func referencedMarkdownDefinitions(content string, references []MarkdownReference) []MarkdownReference {
	if len(references) == 0 {
		return nil
	}
	result := make([]MarkdownReference, 0)
	for _, reference := range references {
		marker := "[^" + strconv.Itoa(reference.ID) + "]"
		if strings.Contains(content, marker) {
			result = append(result, reference)
		}
	}
	return result
}

func parseIndexLine(line string) (int64, uint64, string, error) {
	parts := strings.SplitN(line, ":", 3)
	if len(parts) != 3 {
		return 0, 0, "", fmt.Errorf("invalid multistream index line %q", line)
	}
	offset, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || offset < 0 {
		return 0, 0, "", fmt.Errorf("invalid stream offset in %q", line)
	}
	pageID, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil || pageID == 0 {
		return 0, 0, "", fmt.Errorf("invalid page id in %q", line)
	}
	return offset, pageID, parts[2], nil
}

func readStreams(ctx context.Context, part Part) ([]stream, error) {
	f, err := os.Open(part.IndexPath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	info, err := os.Stat(part.DumpPath)
	if err != nil {
		return nil, err
	}
	scanner := bufio.NewScanner(pbzip2.NewReader(ctx, f))
	scanner.Buffer(make([]byte, 64<<10), 4<<20)
	var offsets []int64
	last := int64(-1)
	for scanner.Scan() {
		offset, _, _, err := parseIndexLine(scanner.Text())
		if err != nil {
			return nil, err
		}
		if offset != last {
			offsets = append(offsets, offset)
			last = offset
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	streams := make([]stream, len(offsets))
	for i, offset := range offsets {
		end := info.Size()
		if i+1 < len(offsets) {
			end = offsets[i+1]
		}
		streams[i] = stream{Offset: offset, End: end, Part: part.Number, Path: part.DumpPath}
	}
	return streams, nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r contextReader) Read(buffer []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := r.reader.Read(buffer)
	if err == nil {
		err = r.ctx.Err()
	}
	return n, err
}

func decodeStream(ctx context.Context, r io.Reader) ([]xmlPage, error) {
	decoder := xml.NewDecoder(bzip2.NewReader(contextReader{ctx: ctx, reader: r}))
	// Individual multistream blocks are XML fragments: only the first has the
	// opening mediawiki element and only the last has its closing element.
	decoder.Strict = false
	var pages []xmlPage
	for {
		token, err := decoder.Token()
		if errors.Is(err, io.EOF) {
			return pages, nil
		}
		if err != nil {
			var syntaxErr *xml.SyntaxError
			if len(pages) > 0 && errors.As(err, &syntaxErr) && (strings.Contains(syntaxErr.Msg, "unexpected end element") || strings.Contains(syntaxErr.Msg, "unexpected EOF")) {
				return pages, nil
			}
			return nil, err
		}
		start, ok := token.(xml.StartElement)
		if !ok || start.Name.Local != "page" {
			continue
		}
		var page xmlPage
		if err := decoder.DecodeElement(&page, &start); err != nil {
			return nil, err
		}
		pages = append(pages, page)
	}
}

func normalizeTitle(title string) string {
	return strings.ToLower(strings.TrimSpace(strings.ReplaceAll(title, "_", " ")))
}

func titleMapping() mapping.IndexMapping {
	m := bleve.NewIndexMapping()
	m.IndexDynamic, m.StoreDynamic, m.DocValuesDynamic = false, false, false
	doc := bleve.NewDocumentMapping()
	storedText := bleve.NewTextFieldMapping()
	storedText.Store, storedText.IncludeTermVectors, storedText.IncludeInAll, storedText.DocValues = true, false, false, false
	exact := bleve.NewTextFieldMapping()
	exact.Analyzer = keyword.Name
	exact.Store, exact.IncludeTermVectors, exact.IncludeInAll, exact.DocValues, exact.SkipFreqNorm = false, false, false, false, false
	number := bleve.NewNumericFieldMapping()
	number.Store, number.Index, number.IncludeInAll, number.DocValues = true, false, false, false
	doc.AddFieldMappingsAt("title", storedText)
	doc.AddFieldMappingsAt("title_exact", exact)
	doc.AddFieldMappingsAt("offset", number)
	doc.AddFieldMappingsAt("end", number)
	doc.AddFieldMappingsAt("part", number)
	doc.AddSubDocumentMapping("_all", bleve.NewDocumentDisabledMapping())
	m.DefaultMapping = doc
	return m
}

func bodyMapping() mapping.IndexMapping {
	m := newMultilingualMapping()
	m.ScoringModel = "bm25"
	m.IndexDynamic, m.StoreDynamic, m.DocValuesDynamic = false, false, false
	doc := bleve.NewDocumentMapping()
	title := bleve.NewTextFieldMapping()
	title.Store, title.IncludeTermVectors, title.IncludeInAll, title.DocValues = true, false, false, false
	body := bleve.NewTextFieldMapping()
	body.Store, body.IncludeTermVectors, body.IncludeInAll, body.DocValues = false, false, false, false
	doc.AddFieldMappingsAt("title", title)
	exact := bleve.NewTextFieldMapping()
	exact.Analyzer = keyword.Name
	exact.Store, exact.IncludeTermVectors, exact.IncludeInAll, exact.DocValues, exact.SkipFreqNorm = false, false, false, false, false
	namespace := bleve.NewNumericFieldMapping()
	namespace.Store, namespace.Index, namespace.IncludeInAll, namespace.DocValues = true, true, false, false
	doc.AddFieldMappingsAt("title_exact", exact)
	doc.AddFieldMappingsAt("namespace", namespace)
	doc.AddFieldMappingsAt("body", body)
	doc.AddSubDocumentMapping("_all", bleve.NewDocumentDisabledMapping())
	m.DefaultMapping = doc
	return m
}

const multilingualAnalyzerName = "unicode_lowercase"

func newMultilingualMapping() *mapping.IndexMappingImpl {
	m := bleve.NewIndexMapping()
	err := m.AddCustomAnalyzer(multilingualAnalyzerName, map[string]interface{}{
		"type":          custom.Name,
		"tokenizer":     unicodeTokenizer.Name,
		"token_filters": []string{lowercase.Name},
	})
	if err != nil {
		panic("define multilingual analyzer: " + err.Error())
	}
	m.DefaultAnalyzer = multilingualAnalyzerName
	return m
}

func scorchConfig() map[string]interface{} {
	return map[string]interface{}{
		"scorchPersisterOptions": map[string]interface{}{
			"NumPersisterWorkers":           2,
			"MaxSizeInMemoryMergePerWorker": 64 << 20,
		},
		"scorchMergePlanOptions": map[string]interface{}{
			"FloorSegmentFileSize": 10 << 20,
		},
	}
}

func newIndex(path string, indexMapping mapping.IndexMapping) (bleve.Index, error) {
	return bleve.NewUsing(path, indexMapping, bleve.Config.DefaultIndexType, bleve.Config.DefaultMemKVStore, scorchConfig())
}

func openWritableIndex(path string) (bleve.Index, error) {
	return bleve.OpenUsing(path, scorchConfig())
}

func PlainText(source string) string {
	var out strings.Builder
	out.Grow(len(source))
	for i := 0; i < len(source); {
		switch {
		case strings.HasPrefix(source[i:], "<!--"):
			end := strings.Index(source[i+4:], "-->")
			if end < 0 {
				i = len(source)
			} else {
				i += 4 + end + 3
			}
			out.WriteByte(' ')
		case strings.HasPrefix(source[i:], "{{"):
			i = skipMarkup(source, i, "{{", "}}")
			out.WriteByte(' ')
		case strings.HasPrefix(source[i:], "{|"):
			i = skipMarkup(source, i, "{|", "|}")
			out.WriteByte(' ')
		case strings.HasPrefix(source[i:], "[["):
			end := strings.Index(source[i+2:], "]]")
			if end < 0 {
				out.WriteByte(source[i])
				i++
				continue
			}
			label := source[i+2 : i+2+end]
			if pipe := strings.LastIndexByte(label, '|'); pipe >= 0 {
				label = label[pipe+1:]
			}
			out.WriteString(label)
			i += 2 + end + 2
		case source[i] == '[':
			end := strings.IndexByte(source[i+1:], ']')
			if end < 0 {
				out.WriteByte(source[i])
				i++
				continue
			}
			value := source[i+1 : i+1+end]
			space := strings.IndexAny(value, " \t\r\n")
			if space > 0 && isExternalURL(value[:space]) {
				out.WriteString(strings.TrimSpace(value[space+1:]))
			} else {
				out.WriteString(value)
			}
			i += 1 + end + 1
		case source[i] == '<':
			i = skipHTML(source, i)
			out.WriteByte(' ')
		case strings.HasPrefix(source[i:], "''"):
			for i < len(source) && source[i] == '\'' {
				i++
			}
		case source[i] == '=' && i+1 < len(source) && source[i+1] == '=':
			for i < len(source) && source[i] == '=' {
				i++
			}
			out.WriteByte(' ')
		case hasASCIIPrefixFold(source[i:], "__TOC__"):
			i += len("__TOC__")
			out.WriteByte(' ')
		default:
			out.WriteByte(source[i])
			i++
		}
	}
	source = html.UnescapeString(out.String())
	out.Reset()
	out.Grow(len(source))
	space := true
	for _, r := range source {
		if unicode.IsSpace(r) {
			if !space {
				out.WriteByte(' ')
				space = true
			}
			continue
		}
		out.WriteRune(r)
		space = false
	}
	return strings.TrimSpace(out.String())
}

func skipMarkup(source string, start int, open, closing string) int {
	depth := 0
	for i := start; i < len(source); {
		if strings.HasPrefix(source[i:], open) {
			depth++
			i += len(open)
			continue
		}
		if depth > 0 && strings.HasPrefix(source[i:], closing) {
			depth--
			i += len(closing)
			if depth == 0 {
				return i
			}
			continue
		}
		i++
	}
	return len(source)
}

func skipHTML(source string, start int) int {
	end := strings.IndexByte(source[start:], '>')
	if end < 0 {
		return len(source)
	}
	tagEnd := start + end + 1
	if !isRefTag(source[start:tagEnd]) || strings.HasSuffix(strings.TrimSpace(source[start:tagEnd-1]), "/") {
		return tagEnd
	}
	closing := indexASCIIFold(source, tagEnd, "</ref")
	if closing < 0 {
		return tagEnd
	}
	closingEnd := strings.IndexByte(source[closing:], '>')
	if closingEnd < 0 {
		return len(source)
	}
	return closing + closingEnd + 1
}

func isRefTag(tag string) bool {
	if !hasASCIIPrefixFold(tag, "<ref") {
		return false
	}
	return len(tag) == len("<ref") || tag[len("<ref")] == '>' || tag[len("<ref")] == '/' || unicode.IsSpace(rune(tag[len("<ref")]))
}

func indexASCIIFold(source string, start int, value string) int {
	for i := start; i+len(value) <= len(source); i++ {
		if hasASCIIPrefixFold(source[i:], value) {
			return i
		}
	}
	return -1
}

func hasASCIIPrefixFold(source, prefix string) bool {
	if len(source) < len(prefix) {
		return false
	}
	for i := range len(prefix) {
		a, b := source[i], prefix[i]
		if 'A' <= a && a <= 'Z' {
			a += 'a' - 'A'
		}
		if 'A' <= b && b <= 'Z' {
			b += 'a' - 'A'
		}
		if a != b {
			return false
		}
	}
	return true
}

func isExternalURL(value string) bool {
	return hasASCIIPrefixFold(value, "http://") || hasASCIIPrefixFold(value, "https://") || hasASCIIPrefixFold(value, "ftp://")
}
