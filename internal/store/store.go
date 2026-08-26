package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/knowledgeindex"
	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/model"
	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/provider"
)

type Store struct {
	root             string
	providers        *provider.Registry
	mu               sync.RWMutex
	jobs             map[string]*model.Job
	active           map[string]string
	downloadQueue    chan string
	indexQueue       chan string
	running          map[string]context.CancelFunc
	watchers         map[chan struct{}]struct{}
	readerMu         sync.Mutex
	readers          map[string]*knowledgeindex.Reader
	storageMu        sync.Mutex
	storage          map[string]storageSnapshot
	lastProgress     map[string]time.Time
	settings         model.Settings
	providerState    map[string]model.ProviderStatus
	downloadSettings chan struct{}
	indexSettings    chan struct{}
	scheduleSettings chan struct{}
	lastUpdateCheck  time.Time
	cancel           context.CancelFunc
	wg               sync.WaitGroup
}

type storageSnapshot struct {
	updated                                                        time.Time
	total, compressedDump, multistreamIndex, titleIndex, bodyIndex int64
}

const (
	progressSaveInterval = 500 * time.Millisecond
	storageCacheDuration = 2 * time.Second
)

type Options struct {
	DownloadWorkers int
	IndexWorkers    int
}

func Open(root string, registry *provider.Registry, options ...Options) (*Store, error) {
	if root == "" {
		return nil, errors.New("data directory cannot be empty")
	}
	if registry == nil {
		return nil, errors.New("source provider registry cannot be nil")
	}
	if err := migrateLegacyDatasetDirectory(root); err != nil {
		return nil, err
	}
	for _, dir := range []string{root, filepath.Join(root, "datasets"), filepath.Join(root, ".staging")} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create data directory: %w", err)
		}
	}
	if err := migrateFlatDatasetDirectories(root, registry); err != nil {
		return nil, err
	}
	config := Options{DownloadWorkers: 3, IndexWorkers: 1}
	if len(options) > 0 {
		config = options[0]
	}
	if config.DownloadWorkers < 1 || config.IndexWorkers < 1 {
		return nil, errors.New("download and index worker counts must be positive")
	}
	settings, err := loadSettings(root, model.Settings{DownloadWorkers: config.DownloadWorkers, IndexWorkers: config.IndexWorkers, IndexingParallelism: min(runtime.GOMAXPROCS(0), 8), UpdateCheckHours: 24})
	if err != nil {
		return nil, err
	}
	s := &Store{root: root, providers: registry, jobs: make(map[string]*model.Job), active: make(map[string]string), downloadQueue: make(chan string, 256), indexQueue: make(chan string, 256), running: make(map[string]context.CancelFunc), watchers: make(map[chan struct{}]struct{}), readers: make(map[string]*knowledgeindex.Reader), storage: make(map[string]storageSnapshot), lastProgress: make(map[string]time.Time), settings: settings, providerState: make(map[string]model.ProviderStatus), downloadSettings: make(chan struct{}, 1), indexSettings: make(chan struct{}, 1), scheduleSettings: make(chan struct{}, 1)}
	for _, backend := range registry.Providers() {
		s.providerState[backend.ID()] = model.ProviderStatus{Provider: backend.ID(), State: "unknown", CatalogState: "not_checked"}
	}
	if err := s.loadJobs(); err != nil {
		return nil, err
	}
	metadataCtx, metadataCancel := context.WithTimeout(context.Background(), 30*time.Second)
	s.backfillSiteMetadata(metadataCtx)
	metadataCancel()
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.wg.Add(3)
	go s.dispatch(ctx, "download", s.downloadQueue)
	go s.dispatch(ctx, "index", s.indexQueue)
	go s.scheduleUpdates(ctx)
	if err := s.queueStaleIndexes(); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

func migrateFlatDatasetDirectories(root string, registry *provider.Registry) error {
	datasets := filepath.Join(root, "datasets")
	entries, err := os.ReadDir(datasets)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		source := filepath.Join(datasets, entry.Name())
		manifest, readErr := readManifest(filepath.Join(source, "manifest.json"))
		if errors.Is(readErr, os.ErrNotExist) {
			// A directory without a manifest is already a provider namespace.
			continue
		}
		if readErr != nil {
			return fmt.Errorf("read flat dataset %q during migration: %w", entry.Name(), readErr)
		}
		backend, providerErr := providerForDatasetManifest(registry, manifest)
		if providerErr != nil {
			return fmt.Errorf("resolve provider for flat dataset %q: %w", entry.Name(), providerErr)
		}
		providerDir := filepath.Join(datasets, backend.ID())
		if err := os.MkdirAll(providerDir, 0o755); err != nil {
			return fmt.Errorf("create provider directory %q: %w", backend.ID(), err)
		}
		destination := filepath.Join(providerDir, entry.Name())
		if _, statErr := os.Stat(destination); statErr == nil {
			return fmt.Errorf("cannot migrate flat dataset %q: destination already exists", entry.Name())
		} else if !errors.Is(statErr, os.ErrNotExist) {
			return statErr
		}
		if err := os.Rename(source, destination); err != nil {
			return fmt.Errorf("migrate flat dataset %q into provider %q: %w", entry.Name(), backend.ID(), err)
		}
	}
	return nil
}

func providerForDatasetManifest(registry *provider.Registry, manifest model.Manifest) (provider.Provider, error) {
	if manifest.Provider != "" {
		return registry.ByID(manifest.Provider)
	}
	return registry.ForCollection(manifest.Dataset)
}

type localDatasetEntry struct {
	provider string
	dataset  string
	path     string
}

func (s *Store) localDatasetEntries() ([]localDatasetEntry, error) {
	providerEntries, err := os.ReadDir(filepath.Join(s.root, "datasets"))
	if err != nil {
		return nil, err
	}
	var result []localDatasetEntry
	for _, providerEntry := range providerEntries {
		if !providerEntry.IsDir() {
			continue
		}
		providerID := providerEntry.Name()
		if _, err := s.providers.ByID(providerID); err != nil {
			continue
		}
		datasetEntries, err := os.ReadDir(filepath.Join(s.root, "datasets", providerID))
		if err != nil {
			return nil, err
		}
		for _, datasetEntry := range datasetEntries {
			if datasetEntry.IsDir() {
				result = append(result, localDatasetEntry{
					provider: providerID,
					dataset:  datasetEntry.Name(),
					path:     s.datasetPath(providerID, datasetEntry.Name()),
				})
			}
		}
	}
	return result, nil
}

func migrateLegacyDatasetDirectory(root string) error {
	legacy, datasets := filepath.Join(root, "wikis"), filepath.Join(root, "datasets")
	if _, err := os.Stat(legacy); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	if _, err := os.Stat(datasets); errors.Is(err, os.ErrNotExist) {
		if err := os.Rename(legacy, datasets); err != nil {
			return fmt.Errorf("migrate legacy dataset directory: %w", err)
		}
		return nil
	} else if err != nil {
		return err
	}
	entries, err := os.ReadDir(legacy)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		destination := filepath.Join(datasets, entry.Name())
		if _, statErr := os.Stat(destination); statErr == nil {
			return fmt.Errorf("cannot migrate legacy dataset %q: destination already exists", entry.Name())
		} else if !errors.Is(statErr, os.ErrNotExist) {
			return statErr
		}
		if err := os.Rename(filepath.Join(legacy, entry.Name()), destination); err != nil {
			return fmt.Errorf("migrate legacy dataset %q: %w", entry.Name(), err)
		}
	}
	if err := os.Remove(legacy); err != nil {
		return fmt.Errorf("migrate legacy dataset directory: %w", err)
	}
	return nil
}

func (s *Store) queueStaleIndexes() error {
	entries, err := s.localDatasetEntries()
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, entry := range entries {
		if s.active[entry.dataset] != "" {
			continue
		}
		manifest, readErr := readManifest(filepath.Join(entry.path, "manifest.json"))
		owner, ownerErr := s.providerForManifest(manifest)
		if readErr != nil || ownerErr != nil {
			continue
		}
		titleCurrent, bodyCurrent := knowledgeindex.Current(manifest)
		if titleCurrent && bodyCurrent {
			continue
		}
		id, idErr := newID()
		if idErr != nil {
			return idErr
		}
		now := time.Now().UTC()
		job := &model.Job{ID: id, Dataset: entry.dataset, Provider: owner.ID(), Variant: manifest.Variant, Kind: "index", State: model.StateQueued, Phase: "queued", Message: "queued index schema upgrade", CreatedAt: now, UpdatedAt: now}
		s.jobs[id], s.active[entry.dataset] = job, id
		if err := s.saveJobsLocked(); err != nil {
			return err
		}
		s.enqueue(job)
	}
	return nil
}

func (s *Store) Close() {
	_ = s.CloseContext(context.Background())
}

func (s *Store) CloseContext(ctx context.Context) error {
	s.cancel()
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		s.closeReaders("")
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Store) ListAvailable(ctx context.Context, filter string, offset, limit int, refresh bool) (model.AvailableResult, error) {
	items, reports, err := s.providers.DiscoverReport(ctx, filter, refresh)
	s.recordDiscovery(reports, refresh)
	if err != nil {
		return model.AvailableResult{}, err
	}
	if offset < 0 {
		offset = 0
	}
	if limit == 0 || limit < -1 || limit > 50 {
		limit = 20
	}
	result := model.AvailableResult{Offset: offset, Total: len(items)}
	if offset < len(items) {
		end := len(items)
		if limit != -1 {
			end = min(end, offset+limit)
		}
		result.Datasets = append([]model.AvailableDataset(nil), items[offset:end]...)
		if end < len(items) {
			result.NextOffset = end
		}
	}
	locals, err := s.ListLocal()
	if err != nil {
		return result, err
	}
	byName := make(map[string]model.LocalDataset, len(locals))
	for _, local := range locals {
		byName[local.Dataset] = local
	}
	for i := range result.Datasets {
		local, ok := byName[result.Datasets[i].ID]
		if !ok {
			continue
		}
		result.Datasets[i].Installed = true
		result.Datasets[i].UpdateAvailable = result.Datasets[i].Fingerprint != "" && result.Datasets[i].Fingerprint != local.Fingerprint
	}
	return result, nil
}

func (s *Store) BrowseAvailable(ctx context.Context, filter, language string, hideInstalled bool, offset, limit int, refresh bool) (model.AvailableResult, error) {
	all, err := s.ListAvailable(ctx, filter, 0, -1, refresh)
	if err != nil {
		return model.AvailableResult{}, err
	}
	languages := make(map[string]model.Language)
	filtered := make([]model.AvailableDataset, 0, len(all.Datasets))
	for _, dataset := range all.Datasets {
		for _, item := range datasetLanguages(dataset) {
			current, exists := languages[item.Code]
			if !exists || current.Name == current.Code && item.Name != item.Code {
				languages[item.Code] = item
			}
		}
		if (language != "" && !datasetHasLanguage(dataset, language)) || (hideInstalled && dataset.Installed) {
			continue
		}
		filtered = append(filtered, dataset)
	}
	languageList := make([]model.Language, 0, len(languages))
	for _, item := range languages {
		languageList = append(languageList, item)
	}
	sort.Slice(languageList, func(i, j int) bool {
		left, right := languageSortName(languageList[i]), languageSortName(languageList[j])
		if left == right {
			return languageList[i].Code < languageList[j].Code
		}
		return left < right
	})
	if offset < 0 {
		offset = 0
	}
	if limit < 1 || limit > 100 {
		limit = 40
	}
	result := model.AvailableResult{Offset: offset, Total: len(filtered)}
	if offset == 0 {
		result.Languages = languageList
	}
	if offset >= len(filtered) {
		return result, nil
	}
	end := min(offset+limit, len(filtered))
	result.Datasets = append([]model.AvailableDataset(nil), filtered[offset:end]...)
	if end < len(filtered) {
		result.NextOffset = end
	}
	return result, nil
}

func datasetLanguages(dataset model.AvailableDataset) []model.Language {
	if len(dataset.Languages) > 0 {
		return dataset.Languages
	}
	if dataset.Language.Code == "" {
		return []model.Language{}
	}
	return []model.Language{dataset.Language}
}

func datasetHasLanguage(dataset model.AvailableDataset, code string) bool {
	for _, item := range datasetLanguages(dataset) {
		if item.Code == code {
			return true
		}
	}
	return false
}

func languageSortName(language model.Language) string {
	for _, value := range []string{language.Name, language.LocalName, language.Code} {
		if value != "" {
			return strings.ToLower(value)
		}
	}
	return ""
}

func (s *Store) ListLocal() ([]model.LocalDataset, error) {
	s.mu.RLock()
	active := make(map[string]string, len(s.active))
	states := make(map[string]string, len(s.active))
	for wiki, jobID := range s.active {
		active[wiki] = jobID
		if job := s.jobs[jobID]; job != nil {
			states[wiki] = job.State
		}
	}
	s.mu.RUnlock()
	entries, err := s.localDatasetEntries()
	if err != nil {
		return nil, err
	}
	result := make([]model.LocalDataset, 0, len(entries))
	for _, entry := range entries {
		manifest, err := readManifest(filepath.Join(entry.path, "manifest.json"))
		if err != nil {
			continue
		}
		local := model.LocalDataset{Manifest: manifest, State: model.StateDownloaded, SearchMode: "none"}
		local.TitleReady, local.BodyReady = knowledgeindex.Current(manifest)
		if local.TitleReady {
			local.State, local.SearchMode = model.StateTitleReady, "title"
		}
		if local.BodyReady {
			local.State, local.SearchMode = model.StateReady, "full_text"
		}
		if jobID := active[entry.dataset]; jobID != "" {
			local.ActiveJob = jobID
			local.State = states[entry.dataset]
		}
		local.DiskBytes, local.RawBytes, local.ProviderMetadataBytes, local.TitleIndexBytes, local.BodyIndexBytes = s.localStorage(entry.path)
		local.OtherBytes = max(local.DiskBytes-local.RawBytes-local.ProviderMetadataBytes-local.TitleIndexBytes-local.BodyIndexBytes, 0)
		result = append(result, local)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Dataset < result[j].Dataset })
	return result, nil
}

func (s *Store) ListLocalSummary() ([]model.LocalDatasetSummary, error) {
	locals, err := s.ListLocal()
	if err != nil {
		return nil, err
	}
	result := make([]model.LocalDatasetSummary, 0, len(locals))
	for _, local := range locals {
		name := local.Site.Name
		if name == "" {
			name = local.Dataset
		}
		result = append(result, model.LocalDatasetSummary{
			Provider: local.Provider, Variant: local.Variant, Dataset: local.Dataset, Name: name, Description: local.Site.Description, Project: local.Site.Project, ContentType: local.Site.ContentType,
			Language: local.Site.Language, OnlineSourceURL: local.Site.OnlineSourceURL, Profile: local.Site.Profile,
			SourceDocuments: local.Site.SourceDocuments, IndexedDocuments: local.DocumentCount,
			ReleaseDate: local.ReleaseDate, SearchMode: local.SearchMode, Closed: local.Site.Closed,
		})
	}
	return result, nil
}

func (s *Store) ListJobs() []model.Job {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]model.Job, 0, len(s.jobs))
	for _, job := range s.jobs {
		result = append(result, *job)
	}
	sort.Slice(result, func(i, j int) bool {
		activeI, activeJ := !isTerminal(result[i].State), !isTerminal(result[j].State)
		if activeI != activeJ {
			return activeI
		}
		if activeI && !result[i].CreatedAt.Equal(result[j].CreatedAt) {
			return result[i].CreatedAt.Before(result[j].CreatedAt)
		}
		if !activeI && !result[i].UpdatedAt.Equal(result[j].UpdatedAt) {
			return result[i].UpdatedAt.After(result[j].UpdatedAt)
		}
		return result[i].ID < result[j].ID
	})
	return result
}

func (s *Store) Subscribe(ctx context.Context) <-chan struct{} {
	updates := make(chan struct{}, 1)
	s.mu.Lock()
	s.watchers[updates] = struct{}{}
	s.mu.Unlock()
	go func() {
		<-ctx.Done()
		s.mu.Lock()
		delete(s.watchers, updates)
		close(updates)
		s.mu.Unlock()
	}()
	return updates
}

func (s *Store) ListUpgrades(ctx context.Context) ([]model.AvailableDataset, error) {
	locals, err := s.ListLocal()
	if err != nil {
		return nil, err
	}
	upgrades := make([]model.AvailableDataset, 0, len(locals))
	for _, local := range locals {
		provider, providerErr := s.providers.ByID(local.Provider)
		if providerErr != nil {
			continue
		}
		release, releaseErr := provider.Latest(ctx, local.Dataset, local.Variant)
		if releaseErr != nil {
			if errors.Is(releaseErr, context.Canceled) {
				return nil, releaseErr
			}
			continue
		}
		upgrades = append(upgrades, model.AvailableDataset{Provider: local.Provider, Variant: local.Variant, ID: local.Dataset, DisplayName: local.Site.Name, ReleaseDate: release.Date, Available: true, Fingerprint: release.Fingerprint, Installed: true, UpdateAvailable: release.Fingerprint != local.Fingerprint})
	}
	sort.Slice(upgrades, func(i, j int) bool { return upgrades[i].ID < upgrades[j].ID })
	return upgrades, nil
}

func (s *Store) Submit(dataset, variant, kind string) (model.Job, error) {
	provider, err := s.providers.ForCollection(dataset)
	if err != nil {
		return model.Job{}, err
	}
	if kind != "download" && kind != "update" {
		return model.Job{}, errors.New("job kind must be download or update")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, installedErr := os.Stat(filepath.Join(s.datasetPath(provider.ID(), dataset), "manifest.json"))
	installed := installedErr == nil
	if kind == "download" && installed {
		return model.Job{}, fmt.Errorf("dataset %s is already installed; use update", dataset)
	}
	if kind == "update" && !installed {
		return model.Job{}, fmt.Errorf("dataset %s is not installed; use download", dataset)
	}
	if id := s.active[dataset]; id != "" {
		return *s.jobs[id], nil
	}
	if kind == "update" {
		var failedIndex *model.Job
		for _, existing := range s.jobs {
			if existing.Dataset == dataset && existing.Kind == "index" && existing.State == model.StateFailed && (failedIndex == nil || existing.UpdatedAt.After(failedIndex.UpdatedAt)) {
				failedIndex = existing
			}
		}
		if failedIndex != nil {
			failedIndex.State, failedIndex.Phase, failedIndex.Error, failedIndex.Message, failedIndex.UpdatedAt = model.StateQueued, "queued", "", "retrying indexing without another download", time.Now().UTC()
			s.active[dataset] = failedIndex.ID
			if err := s.saveJobsLocked(); err != nil {
				delete(s.active, dataset)
				return model.Job{}, err
			}
			s.enqueue(failedIndex)
			return *failedIndex, nil
		}
	}
	var retry *model.Job
	for _, existing := range s.jobs {
		if existing.Dataset == dataset && existing.Kind == kind && existing.State == model.StateFailed && (retry == nil || existing.UpdatedAt.After(retry.UpdatedAt)) {
			retry = existing
		}
	}
	if retry != nil {
		retry.State, retry.Phase, retry.Error, retry.Message, retry.UpdatedAt = model.StateQueued, "queued", "", "retrying with existing partial downloads", time.Now().UTC()
		s.active[dataset] = retry.ID
		if err := s.saveJobsLocked(); err != nil {
			delete(s.active, dataset)
			return model.Job{}, err
		}
		s.enqueue(retry)
		return *retry, nil
	}
	id, err := newID()
	if err != nil {
		return model.Job{}, err
	}
	now := time.Now().UTC()
	job := &model.Job{ID: id, Dataset: dataset, Provider: provider.ID(), Variant: variant, Kind: kind, State: model.StateQueued, Phase: "queued", CreatedAt: now, UpdatedAt: now}
	s.jobs[id], s.active[dataset] = job, id
	if err := s.saveJobsLocked(); err != nil {
		delete(s.jobs, id)
		delete(s.active, dataset)
		return model.Job{}, err
	}
	s.enqueue(job)
	return *job, nil
}

func (s *Store) DeleteDataset(dataset string) error {
	backend, err := s.providers.ForCollection(dataset)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if id := s.active[dataset]; id != "" {
		return fmt.Errorf("dataset %s has active job %s; cancel it before deleting", dataset, id)
	}
	path := s.datasetPath(backend.ID(), dataset)
	if _, err := os.Stat(filepath.Join(path, "manifest.json")); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("dataset %s is not installed", dataset)
		}
		return err
	}
	s.closeReaders(path)
	if err := os.RemoveAll(path); err != nil {
		return fmt.Errorf("delete dataset %s: %w", dataset, err)
	}
	var cleanupErr error
	for _, job := range s.jobs {
		if job.Dataset != dataset {
			continue
		}
		for _, id := range []string{job.ID, job.SourceJobID} {
			if id != "" {
				cleanupErr = errors.Join(cleanupErr, os.RemoveAll(filepath.Join(s.root, ".staging", id)))
			}
		}
	}
	s.storageMu.Lock()
	delete(s.storage, path)
	s.storageMu.Unlock()
	s.notifyLocked()
	return cleanupErr
}

func (s *Store) Job(id, wiki string) (model.Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if id == "" && wiki != "" {
		id = s.active[wiki]
		if id == "" {
			for candidateID, job := range s.jobs {
				if job.Dataset == wiki && (id == "" || job.UpdatedAt.After(s.jobs[id].UpdatedAt)) {
					id = candidateID
				}
			}
		}
	}
	job, ok := s.jobs[id]
	if !ok {
		return model.Job{}, errors.New("job not found")
	}
	return *job, nil
}

func (s *Store) JobAction(id, action string) (model.Job, error) {
	if action == "status" {
		return s.Job(id, "")
	}
	s.mu.Lock()
	job, ok := s.jobs[id]
	if !ok {
		s.mu.Unlock()
		return model.Job{}, errors.New("job not found")
	}
	now := time.Now().UTC()
	var cancel context.CancelFunc
	switch action {
	case "pause":
		if job.State == model.StateQueued {
			s.mu.Unlock()
			return model.Job{}, errors.New("only a running job can be paused; cancel a queued job instead")
		}
		if isTerminal(job.State) || job.State == model.StatePaused {
			s.mu.Unlock()
			return model.Job{}, fmt.Errorf("job cannot be paused from state %s", job.State)
		}
		job.State, job.Phase, job.Message, job.UpdatedAt = model.StatePaused, "paused", "paused by user; partial work is preserved", now
		cancel = s.running[id]
	case "resume":
		if job.State != model.StatePaused {
			s.mu.Unlock()
			return model.Job{}, fmt.Errorf("job cannot be resumed from state %s", job.State)
		}
		if _, stillStopping := s.running[id]; stillStopping {
			s.mu.Unlock()
			return model.Job{}, errors.New("job is still pausing; retry resume shortly")
		}
		job.State, job.Phase, job.Message, job.Error, job.UpdatedAt = model.StateQueued, "queued", "resuming preserved work", "", now
		s.active[job.Dataset] = job.ID
	case "cancel":
		if isTerminal(job.State) || job.State == model.StateCanceled {
			s.mu.Unlock()
			return model.Job{}, fmt.Errorf("job cannot be canceled from state %s", job.State)
		}
		job.State, job.Phase, job.Message, job.Error, job.UpdatedAt = model.StateCanceled, "canceled", "canceled by user; partial work is preserved for retry", "", now
		cancel = s.running[id]
		delete(s.active, job.Dataset)
	case "retry":
		if job.State != model.StateFailed && job.State != model.StateCanceled {
			s.mu.Unlock()
			return model.Job{}, fmt.Errorf("job cannot be retried from state %s", job.State)
		}
		if activeID := s.active[job.Dataset]; activeID != "" && activeID != job.ID {
			s.mu.Unlock()
			return model.Job{}, fmt.Errorf("dataset %s already has active job %s", job.Dataset, activeID)
		}
		job.State, job.Phase, job.Message, job.Error, job.UpdatedAt = model.StateQueued, "queued", "retrying preserved work", "", now
		s.active[job.Dataset] = job.ID
	default:
		s.mu.Unlock()
		return model.Job{}, errors.New("action must be status, pause, resume, cancel, or retry")
	}
	if err := s.saveJobsLocked(); err != nil {
		s.mu.Unlock()
		return model.Job{}, err
	}
	result := *job
	if action == "resume" || action == "retry" {
		s.enqueue(job)
	}
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return result, nil
}

func isTerminal(state string) bool {
	return state == model.StateDownloaded || state == model.StateReady || state == model.StateUpToDate || state == model.StateFailed || state == model.StateCanceled
}

func (s *Store) Search(ctx context.Context, dataset, query string, options model.SearchOptions) (model.SearchResult, error) {
	owner, ownerErr := s.providers.ForCollection(dataset)
	if ownerErr != nil {
		return model.SearchResult{}, ownerErr
	}
	path := s.datasetPath(owner.ID(), dataset)
	s.mu.RLock()
	manifest, err := readManifest(filepath.Join(path, "manifest.json"))
	if err != nil || !manifest.TitleReady {
		s.mu.RUnlock()
		return model.SearchResult{}, fmt.Errorf("dataset %s is not title-ready", dataset)
	}
	_, fullText := knowledgeindex.Current(manifest)
	switch options.Mode {
	case "", "auto":
	case "title":
		fullText = false
	case "full_text":
		if !fullText {
			s.mu.RUnlock()
			return model.SearchResult{}, fmt.Errorf("dataset %s is not full-text ready", dataset)
		}
	default:
		s.mu.RUnlock()
		return model.SearchResult{}, errors.New("search mode must be auto, title, or full_text")
	}
	reader, err := s.reader(owner, path, manifest, fullText)
	if err != nil {
		s.mu.RUnlock()
		return model.SearchResult{}, err
	}
	release, err := reader.Retain()
	s.mu.RUnlock()
	if err != nil {
		return model.SearchResult{}, err
	}
	defer release()
	result, err := reader.Search(ctx, query, options, fullText)
	if err != nil {
		return result, err
	}
	result.Dataset = dataset
	return result, nil
}

func (s *Store) Read(ctx context.Context, dataset, title, id string, options model.ReadOptions) (model.Document, error) {
	owner, ownerErr := s.providers.ForCollection(dataset)
	if ownerErr != nil {
		return model.Document{}, ownerErr
	}
	path := s.datasetPath(owner.ID(), dataset)
	s.mu.RLock()
	manifest, err := readManifest(filepath.Join(path, "manifest.json"))
	if err != nil || !manifest.TitleReady {
		s.mu.RUnlock()
		return model.Document{}, fmt.Errorf("dataset %s is not title-ready", dataset)
	}
	reader, err := s.reader(owner, path, manifest, false)
	if err != nil {
		s.mu.RUnlock()
		return model.Document{}, err
	}
	release, err := reader.Retain()
	s.mu.RUnlock()
	if err != nil {
		return model.Document{}, err
	}
	defer release()
	options.LinkDataset = dataset
	page, err := reader.Read(ctx, title, id, options)
	page.Dataset = dataset
	if errors.Is(err, provider.ErrDocumentNotFound) && strings.TrimSpace(title) != "" {
		result, searchErr := reader.Search(ctx, title, model.SearchOptions{Limit: 5}, false)
		if searchErr == nil && len(result.Hits) > 0 {
			candidates := make([]string, 0, len(result.Hits))
			for _, hit := range result.Hits {
				candidates = append(candidates, fmt.Sprintf("%q (id %s)", hit.Title, hit.ID))
			}
			return page, fmt.Errorf("document %q not found in %s; call knowledge_search and retry knowledge_read with its id; candidates: %s", title, dataset, strings.Join(candidates, ", "))
		}
		return page, fmt.Errorf("document %q not found in %s; call knowledge_search and retry knowledge_read with its id", title, dataset)
	}
	return page, err
}

func (s *Store) reader(owner provider.Provider, path string, manifest model.Manifest, fullText bool) (*knowledgeindex.Reader, error) {
	key := fmt.Sprintf("%s:%t", path, fullText)
	s.readerMu.Lock()
	defer s.readerMu.Unlock()
	if reader := s.readers[key]; reader != nil {
		return reader, nil
	}
	corpus, err := owner.OpenCorpus(path, manifest)
	if err != nil {
		return nil, err
	}
	reader, err := knowledgeindex.Open(path, corpus, fullText)
	if err != nil {
		_ = corpus.Close()
		return nil, err
	}
	s.readers[key] = reader
	return reader, nil
}

func (s *Store) closeReaders(path string) {
	s.readerMu.Lock()
	defer s.readerMu.Unlock()
	for key, reader := range s.readers {
		if path != "" && !strings.HasPrefix(key, path+":") {
			continue
		}
		_ = reader.Close()
		delete(s.readers, key)
	}
}

func (s *Store) dispatch(ctx context.Context, kind string, queue <-chan string) {
	defer s.wg.Done()
	done := make(chan struct{}, 32)
	active := 0
	settingsChanged := s.downloadSettings
	if kind == "index" {
		settingsChanged = s.indexSettings
	}
	for {
		var next <-chan string
		if active < s.workerLimit(kind) {
			next = queue
		}
		select {
		case <-ctx.Done():
			return
		case <-settingsChanged:
		case <-done:
			active--
		case id := <-next:
			active++
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				s.processQueuedJob(ctx, id)
				select {
				case done <- struct{}{}:
				case <-ctx.Done():
				}
			}()
		}
	}
}

func (s *Store) workerLimit(kind string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if kind == "index" {
		return s.settings.IndexWorkers
	}
	return s.settings.DownloadWorkers
}

func (s *Store) processQueuedJob(ctx context.Context, id string) {
	s.mu.Lock()
	job := s.jobs[id]
	if job == nil || job.State != model.StateQueued {
		s.mu.Unlock()
		return
	}
	jobCtx, cancel := context.WithCancel(ctx)
	s.running[id] = cancel
	s.mu.Unlock()
	requeueBody := s.runJob(jobCtx, id)
	cancel()
	s.mu.Lock()
	delete(s.running, id)
	job = s.jobs[id]
	shouldEnqueue := requeueBody && job != nil && job.State != model.StatePaused && job.State != model.StateCanceled
	if shouldEnqueue {
		job.State, job.Phase, job.Message, job.UpdatedAt = model.StateQueued, "body_queued", "title index is available; full-text indexing requeued behind title work", time.Now().UTC()
		_ = s.saveJobsLocked()
	}
	s.mu.Unlock()
	if shouldEnqueue {
		s.indexQueue <- id
	}
}

func (s *Store) runJob(ctx context.Context, id string) bool {
	job, err := s.Job(id, "")
	if err != nil {
		return false
	}
	if job.Kind == "index" {
		return s.runIndexJob(ctx, id)
	}
	s.runDownloadJob(ctx, id)
	return false
}

func (s *Store) runDownloadJob(ctx context.Context, id string) {
	s.setJob(id, model.StateDiscovering, "discovering", 0, 0, "", 0, "checking provider metadata", "")
	job, err := s.Job(id, "")
	if err != nil {
		return
	}
	provider, err := s.providers.ByID(job.Provider)
	if err != nil {
		provider, err = s.providers.ForCollection(job.Dataset)
	}
	if err != nil {
		s.failJob(id, err)
		return
	}
	release, err := provider.Latest(ctx, job.Dataset, job.Variant)
	if err != nil {
		s.failJob(id, err)
		return
	}
	datasetPath := s.datasetPath(provider.ID(), job.Dataset)
	current, currentErr := readManifest(filepath.Join(datasetPath, "manifest.json"))
	if currentErr == nil && current.Fingerprint == release.Fingerprint {
		if current.BodyReady {
			s.finishJob(id, model.StateUpToDate, "local dataset and indexes are current")
			return
		}
		s.finishDownloadAndQueueIndex(id, job.Dataset, "", "dataset is already local; indexing queued")
		return
	}
	stage := filepath.Join(s.root, ".staging", id)
	if err := os.MkdirAll(stage, 0o755); err != nil {
		s.failJob(id, err)
		return
	}
	manifest, err := provider.Acquire(ctx, job.Dataset, job.Variant, release, stage, datasetPath, func(phase string, completed, total int64, units string, rate float64, message string) {
		s.setJob(id, model.StateDownloading, phase, completed, total, units, rate, message, "")
	})
	if err != nil {
		s.failJob(id, err)
		return
	}
	if err := writeJSON(filepath.Join(stage, "manifest.json"), manifest); err != nil {
		s.failJob(id, err)
		return
	}
	sourceJobID := id
	if job.Kind == "download" {
		if err := s.publish(provider.ID(), job.Dataset, stage); err != nil {
			s.failJob(id, err)
			return
		}
		sourceJobID = ""
	}
	s.finishDownloadAndQueueIndex(id, job.Dataset, sourceJobID, "download verified; indexing queued")
}

func (s *Store) finishDownloadAndQueueIndex(id, wiki, sourceJobID, message string) {
	indexID, err := newID()
	if err != nil {
		s.failJob(id, err)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	downloadJob := s.jobs[id]
	if downloadJob == nil {
		return
	}
	if downloadJob.State == model.StatePaused || downloadJob.State == model.StateCanceled {
		return
	}
	now := time.Now().UTC()
	downloadJob.State, downloadJob.Phase, downloadJob.Message, downloadJob.Error, downloadJob.UpdatedAt = model.StateDownloaded, "complete", message, "", now
	delete(s.active, wiki)
	indexJob := &model.Job{ID: indexID, Dataset: wiki, Provider: downloadJob.Provider, Variant: downloadJob.Variant, Kind: "index", State: model.StateQueued, Phase: "queued", Message: "queued after download", CreatedAt: now, UpdatedAt: now, SourceJobID: sourceJobID}
	s.jobs[indexID], s.active[wiki] = indexJob, indexID
	if err := s.saveJobsLocked(); err != nil {
		downloadJob.State, downloadJob.Phase, downloadJob.Error = model.StateFailed, "failed", err.Error()
		delete(s.jobs, indexID)
		delete(s.active, wiki)
		return
	}
	s.enqueue(indexJob)
}

func (s *Store) enqueue(job *model.Job) {
	if job.Kind == "index" {
		s.indexQueue <- job.ID
		return
	}
	s.downloadQueue <- job.ID
}

func (s *Store) runIndexJob(ctx context.Context, id string) bool {
	job, err := s.Job(id, "")
	if err != nil {
		return false
	}
	path := s.datasetPath(job.Provider, job.Dataset)
	publishAfterTitle := false
	if job.SourceJobID != "" {
		staged := filepath.Join(s.root, ".staging", job.SourceJobID)
		if _, statErr := os.Stat(filepath.Join(staged, "manifest.json")); statErr == nil {
			path, publishAfterTitle = staged, true
		}
	}
	manifest, err := readManifest(filepath.Join(path, "manifest.json"))
	if err != nil {
		s.failJob(id, err)
		return false
	}
	owner, err := s.providerForManifest(manifest)
	if err != nil {
		s.failJob(id, err)
		return false
	}
	titleCurrent, bodyCurrent := knowledgeindex.Current(manifest)
	corpus, err := owner.OpenCorpus(path, manifest)
	if err != nil {
		s.failJob(id, err)
		return false
	}
	defer func() { _ = corpus.Close() }()
	titleBuilt := false
	scanOptions := provider.ScanOptions{Parallelism: s.Settings().IndexingParallelism}
	if !titleCurrent {
		temporary := filepath.Join(path, knowledgeindex.TitleDirectory+".building")
		s.setJob(id, model.StateTitleIndexing, "title_indexing", 0, 0, "pages", 0, "building title index", "")
		count, buildErr := knowledgeindex.BuildTitle(ctx, path, manifest.Fingerprint, corpus, scanOptions, func(pages uint64, compressedDone, compressedTotal int64) {
			s.setTitleProgress(id, pages, compressedDone, compressedTotal)
		})
		if buildErr != nil {
			s.failJob(id, buildErr)
			return false
		}
		s.mu.Lock()
		s.closeReaders(path)
		final := filepath.Join(path, knowledgeindex.TitleDirectory)
		replaceErr := os.RemoveAll(final)
		if replaceErr == nil {
			replaceErr = os.Rename(temporary, final)
		}
		if replaceErr == nil {
			manifest.DocumentCount, manifest.TitleReady, manifest.TitleIndexVersion, manifest.PublishedAt = count, true, knowledgeindex.TitleVersion, time.Now().UTC()
			if manifest.BodyIndexVersion != knowledgeindex.BodyVersion {
				manifest.BodyReady = false
			}
			replaceErr = writeJSON(filepath.Join(path, "manifest.json"), manifest)
		}
		s.mu.Unlock()
		if replaceErr != nil {
			s.failJob(id, replaceErr)
			return false
		}
		titleBuilt = true
	}
	if publishAfterTitle {
		if err := s.publish(job.Provider, job.Dataset, path); err != nil {
			s.failJob(id, err)
			return false
		}
	}
	s.mu.Lock()
	if currentJob := s.jobs[id]; currentJob != nil {
		currentJob.TitleAvailable = true
		_ = s.saveJobsLocked()
	}
	s.mu.Unlock()
	if bodyCurrent {
		s.finishJob(id, model.StateReady, "title and full-text indexes are ready")
		return false
	}
	if titleBuilt {
		return true
	}
	s.buildBody(ctx, id, job.Dataset, manifest, owner, corpus)
	return false
}

func (s *Store) buildBody(ctx context.Context, id, dataset string, manifest model.Manifest, owner provider.Provider, corpus provider.Corpus) {
	path := s.datasetPath(manifest.Provider, dataset)
	if manifest.Provider == "" {
		path = s.datasetPath(owner.ID(), dataset)
	}
	temporary := filepath.Join(path, knowledgeindex.BodyDirectory+".building")
	s.setJob(id, model.StateBodyIndexing, "body_indexing", 0, 0, "streams", 0, "title search and page reads are available; building full-text index", "")
	if err := knowledgeindex.BuildBody(ctx, path, manifest.Fingerprint, corpus, provider.ScanOptions{Parallelism: s.Settings().IndexingParallelism}, func(documents uint64, completed, total int64, units string) {
		message := "title search and document reads are available; building full-text index"
		if units != "" && total > 0 {
			message = fmt.Sprintf("%s; source progress %d / %d %s", message, completed, total, units)
		}
		s.setJob(id, model.StateBodyIndexing, "body_indexing", int64(documents), int64(manifest.DocumentCount), "documents", 0, message, "")
	}); err != nil {
		s.failJob(id, err)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeReaders(path)
	final := filepath.Join(path, knowledgeindex.BodyDirectory)
	if err := os.RemoveAll(final); err != nil {
		s.failJobLocked(id, err)
		return
	}
	if err := os.Rename(temporary, final); err != nil {
		s.failJobLocked(id, err)
		return
	}
	manifest.BodyReady, manifest.BodyIndexVersion = true, knowledgeindex.BodyVersion
	if err := writeJSON(filepath.Join(path, "manifest.json"), manifest); err != nil {
		s.failJobLocked(id, err)
		return
	}
	s.finishJobLocked(id, model.StateReady, "full-text index is ready")
}

func migrateLegacyStage(stage, partsDir string) error {
	legacy := map[string]string{
		filepath.Join(stage, "multistream-index.txt.bz2"): filepath.Join(partsDir, "000.index.bz2"),
		filepath.Join(stage, "dump.xml.bz2"):              filepath.Join(partsDir, "000.dump.bz2"),
	}
	for source, destination := range legacy {
		if _, err := os.Stat(destination); err == nil {
			continue
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if _, err := os.Stat(source); errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			return err
		}
		if err := os.Rename(source, destination); err != nil {
			return fmt.Errorf("migrate partial download: %w", err)
		}
	}
	return nil
}

func (s *Store) publish(providerID, dataset, stage string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	destination := s.datasetPath(providerID, dataset)
	if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
		return err
	}
	s.closeReaders(destination)
	old := destination + ".old"
	if err := os.RemoveAll(old); err != nil {
		return err
	}
	if _, err := os.Stat(destination); err == nil {
		if err := os.Rename(destination, old); err != nil {
			return err
		}
	}
	if err := os.Rename(stage, destination); err != nil {
		_ = os.Rename(old, destination)
		return err
	}
	return os.RemoveAll(old)
}

func (s *Store) setJob(id, state, phase string, completed, total int64, units string, rate float64, message, errText string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	job := s.jobs[id]
	if job == nil {
		return
	}
	if job.State == model.StatePaused || job.State == model.StateCanceled {
		return
	}
	if job.State == state && job.Phase == phase && job.Units == units && job.Total == total && completed < job.Completed {
		return
	}
	transition := job.State != state || job.Phase != phase || errText != ""
	job.State, job.Phase, job.Completed, job.Total, job.Units, job.Rate, job.Message, job.Error = state, phase, completed, total, units, rate, message, errText
	job.ProgressPercent, job.ProgressApprox = 0, false
	now := time.Now().UTC()
	if s.lastProgress == nil {
		s.lastProgress = make(map[string]time.Time)
	}
	if !transition && now.Sub(s.lastProgress[id]) < progressSaveInterval {
		return
	}
	s.lastProgress[id], job.UpdatedAt = now, now
	_ = s.saveJobsLocked()
}

func (s *Store) setTitleProgress(id string, pages uint64, compressedDone, compressedTotal int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	job := s.jobs[id]
	if job == nil || job.State == model.StatePaused || job.State == model.StateCanceled {
		return
	}
	transition := job.State != model.StateTitleIndexing || job.Phase != "title_indexing"
	job.State, job.Phase = model.StateTitleIndexing, "title_indexing"
	job.Completed, job.Total, job.Units, job.Rate = int64(pages), 0, "pages", 0
	job.Message, job.Error = "building title index", ""
	job.ProgressPercent, job.ProgressApprox = 0, true
	if compressedTotal > 0 {
		job.ProgressPercent = min(100, float64(compressedDone)/float64(compressedTotal)*100)
	}
	now := time.Now().UTC()
	if s.lastProgress == nil {
		s.lastProgress = make(map[string]time.Time)
	}
	if !transition && now.Sub(s.lastProgress[id]) < progressSaveInterval {
		return
	}
	s.lastProgress[id], job.UpdatedAt = now, now
	_ = s.saveJobsLocked()
}

func (s *Store) failJob(id string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failJobLocked(id, err)
}

func (s *Store) failJobLocked(id string, err error) {
	job := s.jobs[id]
	if job == nil {
		return
	}
	if job.State == model.StatePaused || job.State == model.StateCanceled {
		_ = s.saveJobsLocked()
		return
	}
	if errors.Is(err, context.Canceled) {
		job.State, job.Phase, job.Error, job.Message, job.UpdatedAt = model.StateQueued, "queued", "", "paused for server shutdown; will resume on restart", time.Now().UTC()
		_ = s.saveJobsLocked()
		return
	}
	job.State, job.Phase, job.Error, job.Message, job.UpdatedAt = model.StateFailed, "failed", err.Error(), "job failed", time.Now().UTC()
	delete(s.active, job.Dataset)
	_ = s.saveJobsLocked()
}

func (s *Store) finishJob(id, state, message string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.finishJobLocked(id, state, message)
}

func (s *Store) finishJobLocked(id, state, message string) {
	job := s.jobs[id]
	if job == nil {
		return
	}
	if job.State == model.StatePaused || job.State == model.StateCanceled {
		return
	}
	job.State, job.Phase, job.Message, job.UpdatedAt = state, "complete", message, time.Now().UTC()
	job.TitleAvailable = state == model.StateReady || job.TitleAvailable
	delete(s.active, job.Dataset)
	_ = s.saveJobsLocked()
}

func (s *Store) datasetPath(providerID, dataset string) string {
	return filepath.Join(s.root, "datasets", providerID, dataset)
}

func (s *Store) providerForManifest(manifest model.Manifest) (provider.Provider, error) {
	return providerForDatasetManifest(s.providers, manifest)
}

func (s *Store) jobsPath() string { return filepath.Join(s.root, "jobs.json") }

func (s *Store) backfillSiteMetadata(ctx context.Context) {
	entries, err := s.localDatasetEntries()
	if err != nil {
		return
	}
	for _, entry := range entries {
		if ctx.Err() != nil {
			return
		}
		path := entry.path
		manifestPath := filepath.Join(path, "manifest.json")
		manifest, readErr := readManifest(manifestPath)
		if readErr != nil {
			continue
		}
		provider, providerErr := s.providerForManifest(manifest)
		if providerErr == nil && provider.Backfill(ctx, path, &manifest) {
			_ = writeJSON(manifestPath, manifest)
		}
	}
}

func (s *Store) loadJobs() error {
	data, err := os.ReadFile(s.jobsPath())
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var jobs []*model.Job
	if err := json.Unmarshal(data, &jobs); err != nil {
		return fmt.Errorf("decode jobs: %w", err)
	}
	var legacy []struct{ ID, Wiki string }
	_ = json.Unmarshal(data, &legacy)
	legacyDataset := make(map[string]string, len(legacy))
	for _, job := range legacy {
		legacyDataset[job.ID] = job.Wiki
	}
	for _, job := range jobs {
		if job.Dataset == "" {
			job.Dataset = legacyDataset[job.ID]
		}
		if job.Provider == "" {
			if provider, providerErr := s.providers.ForCollection(job.Dataset); providerErr == nil {
				job.Provider = provider.ID()
			}
		}
		if job.State == model.StatePaused {
			s.active[job.Dataset] = job.ID
		} else if job.State != model.StateDownloaded && job.State != model.StateReady && job.State != model.StateUpToDate && job.State != model.StateFailed && job.State != model.StateCanceled {
			job.State, job.Phase, job.Message = model.StateQueued, "queued", "resuming after server restart"
			s.active[job.Dataset] = job.ID
			s.enqueue(job)
		}
		s.jobs[job.ID] = job
	}
	return nil
}

func (s *Store) saveJobsLocked() error {
	jobs := make([]*model.Job, 0, len(s.jobs))
	for _, job := range s.jobs {
		jobCopy := *job
		jobs = append(jobs, &jobCopy)
	}
	sort.Slice(jobs, func(i, j int) bool { return jobs[i].CreatedAt.Before(jobs[j].CreatedAt) })
	if err := writeJSON(s.jobsPath(), jobs); err != nil {
		return err
	}
	s.notifyLocked()
	return nil
}

func (s *Store) notifyLocked() {
	for watcher := range s.watchers {
		select {
		case watcher <- struct{}{}:
		default:
		}
	}
}

func readManifest(path string) (model.Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return model.Manifest{}, err
	}
	var manifest model.Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return model.Manifest{}, err
	}
	var legacy struct {
		Wiki            string `json:"wiki"`
		DumpDate        string `json:"dump_date"`
		DumpSHA1        string `json:"dump_sha1"`
		IndexSHA1       string `json:"index_sha1"`
		DumpSize        int64  `json:"dump_size"`
		IndexSize       int64  `json:"index_size"`
		PageCount       uint64 `json:"page_count"`
		ContentArticles uint64 `json:"content_articles"`
		Site            struct {
			ContentArticles uint64 `json:"content_articles"`
		} `json:"site"`
	}
	if err := json.Unmarshal(data, &legacy); err == nil {
		if manifest.Dataset == "" {
			manifest.Dataset = legacy.Wiki
		}
		if manifest.ReleaseDate == "" {
			manifest.ReleaseDate = legacy.DumpDate
		}
		if manifest.RawHash == "" {
			manifest.RawHash = legacy.DumpSHA1
		}
		if manifest.ProviderMetadataHash == "" {
			manifest.ProviderMetadataHash = legacy.IndexSHA1
		}
		if manifest.RawSize == 0 {
			manifest.RawSize = legacy.DumpSize
		}
		if manifest.ProviderMetadataSize == 0 {
			manifest.ProviderMetadataSize = legacy.IndexSize
		}
		if manifest.DocumentCount == 0 {
			manifest.DocumentCount = legacy.PageCount
		}
		if manifest.Site.SourceDocuments == 0 {
			manifest.Site.SourceDocuments = legacy.Site.ContentArticles
			if manifest.Site.SourceDocuments == 0 {
				manifest.Site.SourceDocuments = legacy.ContentArticles
			}
		}
	}
	return manifest, nil
}

func writeJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	temporary := path + ".tmp"
	if err := os.WriteFile(temporary, append(data, '\n'), 0o644); err != nil {
		return err
	}
	return os.Rename(temporary, path)
}

func newID() (string, error) {
	buf := make([]byte, 12)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

func localStorage(path string) (total, compressedDump, multistreamIndex, titleIndex, bodyIndex int64) {
	_ = filepath.WalkDir(path, func(filePath string, entry fs.DirEntry, err error) error {
		if err != nil || !entry.Type().IsRegular() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return nil
		}
		size := info.Size()
		total += size
		relative, err := filepath.Rel(path, filePath)
		if err != nil {
			return nil
		}
		switch {
		case strings.HasPrefix(relative, "raw"+string(filepath.Separator)):
			compressedDump += size
		case relative == "rfc-index.xml" || relative == "documents.json":
			multistreamIndex += size
		case strings.HasPrefix(relative, "parts"+string(filepath.Separator)) && strings.HasSuffix(relative, ".dump.bz2"):
			compressedDump += size
		case strings.HasPrefix(relative, "parts"+string(filepath.Separator)) && strings.HasSuffix(relative, ".index.bz2"):
			multistreamIndex += size
		case pathInIndex(relative, knowledgeindex.TitleDirectory):
			titleIndex += size
		case pathInIndex(relative, knowledgeindex.BodyDirectory):
			bodyIndex += size
		}
		return nil
	})
	return total, compressedDump, multistreamIndex, titleIndex, bodyIndex
}

func (s *Store) localStorage(datasetPath string) (total, compressedDump, multistreamIndex, titleIndex, bodyIndex int64) {
	s.storageMu.Lock()
	defer s.storageMu.Unlock()
	if s.storage == nil {
		s.storage = make(map[string]storageSnapshot)
	}
	if cached, ok := s.storage[datasetPath]; ok && time.Since(cached.updated) < storageCacheDuration {
		return cached.total, cached.compressedDump, cached.multistreamIndex, cached.titleIndex, cached.bodyIndex
	}
	total, compressedDump, multistreamIndex, titleIndex, bodyIndex = localStorage(datasetPath)
	s.storage[datasetPath] = storageSnapshot{updated: time.Now(), total: total, compressedDump: compressedDump, multistreamIndex: multistreamIndex, titleIndex: titleIndex, bodyIndex: bodyIndex}
	return total, compressedDump, multistreamIndex, titleIndex, bodyIndex
}

func pathInIndex(relative, name string) bool {
	separator := string(filepath.Separator)
	return strings.HasPrefix(relative, name+separator) || strings.HasPrefix(relative, name+".building"+separator)
}
