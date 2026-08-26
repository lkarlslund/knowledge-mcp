package knowledgeindex

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/blevesearch/bleve/v2"
	blevequery "github.com/blevesearch/bleve/v2/search/query"
	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/model"
	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/provider"
)

const (
	sharedShards    = 16
	sharedBatchSize = 64 << 20
)

type sharedDataset struct {
	Provider     string `json:"provider"`
	Dataset      string `json:"dataset"`
	Fingerprint  string `json:"fingerprint"`
	Generation   string `json:"generation"`
	Documents    uint64 `json:"documents"`
	IndexedBytes int64  `json:"indexed_bytes"`
}

type sharedManifest struct {
	Version  int                      `json:"version"`
	Datasets map[string]sharedDataset `json:"datasets"`
	Retired  []string                 `json:"retired,omitempty"`
}

// SharedIndex is one logical corpus striped uniformly over physical Bleve
// shards. Generations make publishing one dataset atomic while all shards share
// approximately the same corpus statistics.
type SharedIndex struct {
	root         string
	indexes      []bleve.Index
	alias        bleve.Index
	shardMu      []sync.RWMutex
	manifest     sharedManifest
	mu           sync.RWMutex
	storageMu    sync.Mutex
	storageAt    time.Time
	storageBytes int64
}

func OpenShared(root string) (*SharedIndex, error) {
	if err := prepareSharedRoot(root); err != nil {
		return nil, err
	}
	manifest, err := loadSharedManifest(filepath.Join(root, "manifest.json"))
	if err != nil {
		return nil, err
	}
	shared := &SharedIndex{root: root, manifest: manifest, shardMu: make([]sync.RWMutex, sharedShards)}
	for shard := range sharedShards {
		path := sharedShardPath(root, shard)
		var idx bleve.Index
		if _, statErr := os.Stat(path); statErr == nil {
			idx, err = openWritableIndexUsing(path, sharedScorchConfig())
		} else if errors.Is(statErr, os.ErrNotExist) {
			idx, err = newIndexUsing(path, indexMapping(true), sharedScorchConfig())
		} else {
			err = statErr
		}
		if err != nil {
			for _, opened := range shared.indexes {
				_ = opened.Close()
			}
			return nil, fmt.Errorf("open shared search shard %d: %w", shard, err)
		}
		shared.indexes = append(shared.indexes, idx)
	}
	shared.alias = bleve.NewIndexAlias(shared.indexes...)
	return shared, nil
}

func prepareSharedRoot(root string) error {
	manifestPath := filepath.Join(root, "manifest.json")
	data, err := os.ReadFile(manifestPath)
	if err == nil {
		var manifest sharedManifest
		if json.Unmarshal(data, &manifest) == nil && manifest.Version == SharedVersion {
			return nil
		}
		if err := os.RemoveAll(root); err != nil {
			return fmt.Errorf("replace incompatible shared search index: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return err
	}
	return writeSharedManifest(manifestPath, sharedManifest{Version: SharedVersion, Datasets: map[string]sharedDataset{}})
}

func loadSharedManifest(path string) (sharedManifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return sharedManifest{}, err
	}
	var manifest sharedManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return sharedManifest{}, err
	}
	if manifest.Version != SharedVersion {
		return sharedManifest{}, fmt.Errorf("shared search index version %d is unsupported", manifest.Version)
	}
	if manifest.Datasets == nil {
		manifest.Datasets = map[string]sharedDataset{}
	}
	return manifest, nil
}

func writeSharedManifest(path string, manifest sharedManifest) error {
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	temporary := path + ".tmp"
	if err := os.WriteFile(temporary, append(data, '\n'), 0o644); err != nil {
		return err
	}
	return os.Rename(temporary, path)
}

func (s *SharedIndex) Close() error {
	var errs []error
	for _, idx := range s.indexes {
		errs = append(errs, idx.Close())
	}
	return errors.Join(errs...)
}

func (s *SharedIndex) Active(dataset, fingerprint string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, found := s.manifest.Datasets[dataset]
	return found && entry.Fingerprint == fingerprint
}

func (s *SharedIndex) ActiveDatasets() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]string, 0, len(s.manifest.Datasets))
	for dataset := range s.manifest.Datasets {
		result = append(result, dataset)
	}
	sort.Strings(result)
	return result
}

func (s *SharedIndex) EstimatedBytes(dataset string) int64 {
	s.mu.RLock()
	entry, found := s.manifest.Datasets[dataset]
	var indexedTotal int64
	for _, item := range s.manifest.Datasets {
		indexedTotal += item.IndexedBytes
	}
	s.mu.RUnlock()
	if !found || indexedTotal <= 0 || entry.IndexedBytes <= 0 {
		return 0
	}
	diskBytes := s.diskBytes()
	return diskBytes * entry.IndexedBytes / indexedTotal
}

func (s *SharedIndex) diskBytes() int64 {
	s.storageMu.Lock()
	defer s.storageMu.Unlock()
	if time.Since(s.storageAt) < 2*time.Second {
		return s.storageBytes
	}
	var total int64
	_ = filepath.WalkDir(s.root, func(_ string, item fs.DirEntry, err error) error {
		if err != nil || item.IsDir() {
			return nil
		}
		if info, infoErr := item.Info(); infoErr == nil {
			total += info.Size()
		}
		return nil
	})
	s.storageAt, s.storageBytes = time.Now(), total
	return total
}

func (s *SharedIndex) Build(ctx context.Context, providerID, dataset, fingerprint string, corpus provider.Corpus, scanOptions provider.ScanOptions, progress BodyProgress) (uint64, int64, error) {
	return s.build(ctx, providerID, dataset, fingerprint, corpus, scanOptions, sharedBatchSize, progress)
}

func (s *SharedIndex) build(ctx context.Context, providerID, dataset, fingerprint string, corpus provider.Corpus, scanOptions provider.ScanOptions, batchSize int, progress BodyProgress) (uint64, int64, error) {
	generation := sharedGeneration(providerID, dataset, fingerprint)
	checkpointPath := sharedCheckpointPath(s.root, providerID, dataset)
	if err := os.MkdirAll(filepath.Dir(checkpointPath), 0o755); err != nil {
		return 0, 0, err
	}
	if err := s.removeAbandonedBuild(ctx, checkpointPath, providerID, dataset, generation); err != nil {
		return 0, 0, err
	}
	checkpoint, resume := loadCheckpoint(checkpointPath, s.root, "shared", SharedVersion, fingerprint)
	if !resume {
		checkpoint = buildCheckpoint{Kind: "shared", Version: SharedVersion, Fingerprint: fingerprint, UpdatedAt: time.Now().UTC()}
		if err := saveCheckpoint(checkpointPath, checkpoint); err != nil {
			return 0, 0, err
		}
	}
	type batchState struct {
		batch *bleve.Batch
		bytes int
	}
	batches := make([]batchState, sharedShards)
	for shard := range batches {
		batches[shard].batch = s.indexes[shard].NewBatch()
	}
	documents, indexedBytes := checkpoint.Documents, checkpoint.IndexedBytes
	lastBoundary := true
	lastPosition := provider.ScanPosition{Cursor: checkpoint.Cursor, Boundary: true}
	flush := func(position provider.ScanPosition) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		for shard := range batches {
			if batches[shard].batch.Size() == 0 {
				continue
			}
			s.shardMu[shard].Lock()
			err := s.indexes[shard].Batch(batches[shard].batch)
			s.shardMu[shard].Unlock()
			if err != nil {
				return fmt.Errorf("commit shared shard %d: %w", shard, err)
			}
			batches[shard] = batchState{batch: s.indexes[shard].NewBatch()}
		}
		checkpoint.Cursor, checkpoint.Documents = position.Cursor, documents
		checkpoint.Completed, checkpoint.Total, checkpoint.IndexedBytes, checkpoint.UpdatedAt = position.Completed, position.Total, indexedBytes, time.Now().UTC()
		if err := saveCheckpoint(checkpointPath, checkpoint); err != nil {
			return err
		}
		progress(documents, position.Completed, position.Total, position.Units)
		return nil
	}
	pendingBytes := 0
	err := corpus.ScanBodies(ctx, checkpoint.Cursor, scanOptions, func(record provider.Record, position provider.ScanPosition) error {
		lastBoundary, lastPosition = position.Boundary, position
		if record.ID == "" || strings.TrimSpace(record.Title) == "" {
			return nil
		}
		document := toIndexDocument(record, true)
		document.Provider, document.Dataset, document.Generation, document.DocumentID = providerID, dataset, generation, record.ID
		key := sharedDocumentKey(generation, record.ID)
		shard := sharedDocumentShard(key)
		if err := batches[shard].batch.Index(key, document); err != nil {
			return err
		}
		size := len(record.Title) + len(record.Body) + len(record.URL) + len(record.Locator)
		batches[shard].bytes += size
		pendingBytes, indexedBytes, documents = pendingBytes+size, indexedBytes+int64(size), documents+1
		if pendingBytes >= batchSize && position.Boundary {
			if err := flush(position); err != nil {
				return err
			}
			pendingBytes = 0
		} else if position.Boundary {
			progress(documents, position.Completed, position.Total, position.Units)
		}
		return nil
	})
	if err != nil {
		return 0, 0, err
	}
	if !lastBoundary {
		return 0, 0, errors.New("provider shared scan ended outside a safe checkpoint boundary")
	}
	if pendingBytes > 0 {
		if err := flush(lastPosition); err != nil {
			return 0, 0, err
		}
	}
	return documents, indexedBytes, nil
}

func (s *SharedIndex) Activate(providerID, dataset, fingerprint string, documents uint64, indexedBytes int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := sharedManifest{Version: SharedVersion, Datasets: make(map[string]sharedDataset, len(s.manifest.Datasets)+1), Retired: append([]string(nil), s.manifest.Retired...)}
	for key, value := range s.manifest.Datasets {
		next.Datasets[key] = value
	}
	generation := sharedGeneration(providerID, dataset, fingerprint)
	if previous, found := next.Datasets[dataset]; found && previous.Generation != generation {
		next.Retired = appendUniqueString(next.Retired, previous.Generation)
	}
	next.Datasets[dataset] = sharedDataset{Provider: providerID, Dataset: dataset, Fingerprint: fingerprint, Generation: generation, Documents: documents, IndexedBytes: indexedBytes}
	if err := writeSharedManifest(filepath.Join(s.root, "manifest.json"), next); err != nil {
		return err
	}
	s.manifest = next
	_ = os.Remove(sharedCheckpointPath(s.root, providerID, dataset))
	return nil
}

func (s *SharedIndex) Remove(dataset string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, found := s.manifest.Datasets[dataset]
	if !found {
		return nil
	}
	next := sharedManifest{Version: SharedVersion, Datasets: make(map[string]sharedDataset, len(s.manifest.Datasets)-1), Retired: appendUniqueString(append([]string(nil), s.manifest.Retired...), entry.Generation)}
	for key, value := range s.manifest.Datasets {
		if key != dataset {
			next.Datasets[key] = value
		}
	}
	if err := writeSharedManifest(filepath.Join(s.root, "manifest.json"), next); err != nil {
		return err
	}
	s.manifest = next
	return nil
}

// Cleanup removes inactive generations in bounded batches. They are already
// invisible because every search is constrained by the active manifest.
func (s *SharedIndex) Cleanup(ctx context.Context) error {
	s.mu.RLock()
	retired := append([]string(nil), s.manifest.Retired...)
	s.mu.RUnlock()
	for _, generation := range retired {
		if err := s.deleteGeneration(ctx, generation); err != nil {
			return err
		}
		s.mu.Lock()
		next := cloneSharedManifest(s.manifest)
		next.Retired = removeString(next.Retired, generation)
		if err := writeSharedManifest(filepath.Join(s.root, "manifest.json"), next); err != nil {
			s.mu.Unlock()
			return err
		}
		s.manifest = next
		s.mu.Unlock()
	}
	return nil
}

func (s *SharedIndex) deleteGeneration(ctx context.Context, generation string) error {
	for shard, idx := range s.indexes {
		for {
			term := bleve.NewTermQuery(generation)
			term.SetField("generation")
			s.shardMu[shard].RLock()
			response, err := idx.SearchInContext(ctx, bleve.NewSearchRequestOptions(term, 10_000, 0, false))
			s.shardMu[shard].RUnlock()
			if err != nil {
				return fmt.Errorf("find retired generation in shard %d: %w", shard, err)
			}
			if len(response.Hits) == 0 {
				break
			}
			batch := idx.NewBatch()
			for _, hit := range response.Hits {
				batch.Delete(hit.ID)
			}
			s.shardMu[shard].Lock()
			err = idx.Batch(batch)
			s.shardMu[shard].Unlock()
			if err != nil {
				return fmt.Errorf("delete retired generation from shard %d: %w", shard, err)
			}
		}
	}
	return nil
}

func (s *SharedIndex) Search(ctx context.Context, dataset, text string, options model.SearchOptions) (model.SearchResult, error) {
	text = strings.TrimSpace(text)
	if text == "" {
		return model.SearchResult{}, errors.New("query cannot be empty")
	}
	if options.Limit <= 0 || options.Limit > 50 {
		options.Limit = 10
	}
	if options.Mode == "" || options.Mode == "auto" {
		options.Mode = "full_text"
	}
	if options.Mode != "title" && options.Mode != "full_text" {
		return model.SearchResult{}, errors.New("search mode must be auto, title, or full_text")
	}
	s.mu.RLock()
	active := make([]sharedDataset, 0, len(s.manifest.Datasets))
	if dataset != "" {
		entry, found := s.manifest.Datasets[dataset]
		if !found {
			s.mu.RUnlock()
			return model.SearchResult{}, fmt.Errorf("dataset %s is not shared-search ready", dataset)
		}
		active = append(active, entry)
	} else {
		for _, entry := range s.manifest.Datasets {
			active = append(active, entry)
		}
	}
	s.mu.RUnlock()
	if len(active) == 0 {
		return model.SearchResult{}, errors.New("no datasets are shared-search ready")
	}
	for shard := range s.shardMu {
		s.shardMu[shard].RLock()
	}
	defer func() {
		for shard := len(s.shardMu) - 1; shard >= 0; shard-- {
			s.shardMu[shard].RUnlock()
		}
	}()
	filters := make([]blevequery.Query, 0, len(active))
	searched := make([]string, 0, len(active))
	for _, entry := range active {
		term := bleve.NewTermQuery(entry.Generation)
		term.SetField("generation")
		filters = append(filters, term)
		searched = append(searched, entry.Dataset)
	}
	sort.Strings(searched)
	activeFilter := bleve.NewDisjunctionQuery(filters...)
	searchQuery := bleve.NewConjunctionQuery(rankedQuery(text, options.Mode != "title"), activeFilter)
	if !options.IncludeSecondary {
		primary := bleve.NewTermQuery("1")
		primary.SetField("primary")
		searchQuery = bleve.NewConjunctionQuery(searchQuery, primary)
	}
	request := bleve.NewSearchRequestOptions(searchQuery, min(500, max(100, options.Offset+options.Limit*10)), 0, false)
	request.Fields = []string{"provider", "dataset", "document_id", "title", "url", "namespace", "identifiers", "status", "rank_weight"}
	response, err := s.alias.SearchInContext(ctx, request)
	if err != nil {
		return model.SearchResult{}, err
	}
	hits := make([]model.SearchHit, 0, len(response.Hits))
	for _, hit := range response.Hits {
		title, _ := hit.Fields["title"].(string)
		providerID, _ := hit.Fields["provider"].(string)
		datasetID, _ := hit.Fields["dataset"].(string)
		documentID, _ := hit.Fields["document_id"].(string)
		url, _ := hit.Fields["url"].(string)
		namespace, _ := numberField(hit.Fields["namespace"])
		rankWeight, ok := numberField(hit.Fields["rank_weight"])
		if !ok || rankWeight <= 0 {
			rankWeight = 1
		}
		status, _ := hit.Fields["status"].(string)
		hits = append(hits, model.SearchHit{Provider: providerID, Dataset: datasetID, ID: documentID, Title: title, URL: url, Namespace: int(namespace), Score: hit.Score * rankWeight, MatchMode: "shared", Identifiers: stringSliceField(hit.Fields["identifiers"]), Status: status})
	}
	sort.SliceStable(hits, func(i, j int) bool {
		iExact, jExact := normalize(hits[i].Title) == normalize(text), normalize(hits[j].Title) == normalize(text)
		if iExact != jExact {
			return iExact
		}
		return hits[i].Score > hits[j].Score
	})
	total := uint64(response.Total)
	start := min(max(options.Offset, 0), len(hits))
	end := min(start+options.Limit, len(hits))
	hits = hits[start:end]
	result := model.SearchResult{Dataset: dataset, Query: text, SearchMode: "shared", Total: total, Offset: options.Offset, PrimaryFilterApplied: !options.IncludeSecondary, SnippetsAvailable: false, SearchedDatasets: searched, Hits: hits}
	if uint64(end) < total {
		result.NextOffset = end
	}
	return result, nil
}

func sharedGeneration(providerID, dataset, fingerprint string) string {
	sum := sha256.Sum256([]byte(providerID + "\x1f" + dataset + "\x1f" + fingerprint))
	return hex.EncodeToString(sum[:16])
}

func sharedDocumentKey(generation, documentID string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(generation)) + "." + base64.RawURLEncoding.EncodeToString([]byte(documentID))
}

func sharedDocumentShard(key string) int {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(key))
	return int(hash.Sum32() % sharedShards)
}

func sharedShardPath(root string, shard int) string {
	return filepath.Join(root, "shard-"+strconv.Itoa(shard)+".bleve")
}

func sharedCheckpointPath(root, providerID, dataset string) string {
	return filepath.Join(root, "checkpoints", base64.RawURLEncoding.EncodeToString([]byte(providerID+"\x00"+dataset))+".json")
}

func (s *SharedIndex) removeAbandonedBuild(ctx context.Context, checkpointPath, providerID, dataset, generation string) error {
	data, err := os.ReadFile(checkpointPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var checkpoint buildCheckpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return fmt.Errorf("read shared build checkpoint: %w", err)
	}
	previous := sharedGeneration(providerID, dataset, checkpoint.Fingerprint)
	if checkpoint.Version == SharedVersion && previous == generation {
		return nil
	}
	if checkpoint.Fingerprint != "" {
		if err := s.deleteGeneration(ctx, previous); err != nil {
			return fmt.Errorf("remove abandoned shared generation: %w", err)
		}
	}
	return os.Remove(checkpointPath)
}

func cloneSharedManifest(manifest sharedManifest) sharedManifest {
	result := sharedManifest{Version: manifest.Version, Datasets: make(map[string]sharedDataset, len(manifest.Datasets)), Retired: append([]string(nil), manifest.Retired...)}
	for key, value := range manifest.Datasets {
		result.Datasets[key] = value
	}
	return result
}

func appendUniqueString(values []string, value string) []string {
	for _, current := range values {
		if current == value {
			return values
		}
	}
	return append(values, value)
}

func removeString(values []string, value string) []string {
	result := make([]string, 0, len(values))
	for _, current := range values {
		if current != value {
			result = append(result, current)
		}
	}
	return result
}

func sharedScorchConfig() map[string]any {
	return map[string]any{
		"scorchPersisterOptions": map[string]any{"NumPersisterWorkers": 1, "MaxSizeInMemoryMergePerWorker": 64 << 20, "PersisterNapUnderNumFiles": 0},
		"scorchMergePlanOptions": map[string]any{"FloorSegmentFileSize": 10 << 20},
	}
}
