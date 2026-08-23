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
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/model"
	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/wikiindex"
	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/wikimedia"
)

type Store struct {
	root   string
	remote *wikimedia.Client
	mu     sync.RWMutex
	jobs   map[string]*model.Job
	active map[string]string
	queue  chan string
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func Open(root string, remote *wikimedia.Client) (*Store, error) {
	if root == "" {
		return nil, errors.New("data directory cannot be empty")
	}
	for _, dir := range []string{root, filepath.Join(root, "wikis"), filepath.Join(root, ".staging")} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create data directory: %w", err)
		}
	}
	s := &Store{root: root, remote: remote, jobs: make(map[string]*model.Job), active: make(map[string]string), queue: make(chan string, 256)}
	if err := s.loadJobs(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.wg.Add(1)
	go s.worker(ctx)
	return s, nil
}

func (s *Store) Close() {
	s.cancel()
	s.wg.Wait()
}

func (s *Store) ListAvailable(ctx context.Context, filter string, offset, limit int, refresh bool) (model.AvailableResult, error) {
	result, err := s.remote.ListAvailable(ctx, filter, offset, limit, refresh)
	if err != nil {
		return result, err
	}
	locals, err := s.ListLocal()
	if err != nil {
		return result, err
	}
	byName := make(map[string]model.LocalWiki, len(locals))
	for _, local := range locals {
		byName[local.Wiki] = local
	}
	for i := range result.Wikis {
		local, ok := byName[result.Wikis[i].Name]
		if !ok {
			continue
		}
		result.Wikis[i].Installed = true
		result.Wikis[i].UpdateAvailable = result.Wikis[i].Fingerprint != "" && result.Wikis[i].Fingerprint != local.Fingerprint
	}
	return result, nil
}

func (s *Store) ListLocal() ([]model.LocalWiki, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entries, err := os.ReadDir(filepath.Join(s.root, "wikis"))
	if err != nil {
		return nil, err
	}
	result := make([]model.LocalWiki, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		manifest, err := readManifest(filepath.Join(s.wikiPath(entry.Name()), "manifest.json"))
		if err != nil {
			continue
		}
		local := model.LocalWiki{Manifest: manifest, State: model.StateTitleReady, SearchMode: "title"}
		if manifest.BodyReady {
			local.State, local.SearchMode = model.StateReady, "full_text"
		}
		if jobID := s.active[entry.Name()]; jobID != "" {
			local.ActiveJob = jobID
			local.State = s.jobs[jobID].State
		}
		local.DiskBytes, _ = directorySize(s.wikiPath(entry.Name()))
		result = append(result, local)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Wiki < result[j].Wiki })
	return result, nil
}

func (s *Store) ListJobs() []model.Job {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]model.Job, 0, len(s.jobs))
	for _, job := range s.jobs {
		result = append(result, *job)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].UpdatedAt.After(result[j].UpdatedAt) })
	return result
}

func (s *Store) ListUpgrades(ctx context.Context) ([]model.OnlineWiki, error) {
	locals, err := s.ListLocal()
	if err != nil {
		return nil, err
	}
	type result struct {
		wiki model.OnlineWiki
		err  error
	}
	results := make(chan result, len(locals))
	sem := make(chan struct{}, 3)
	var wg sync.WaitGroup
	for _, local := range locals {
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				results <- result{err: ctx.Err()}
				return
			}
			defer func() { <-sem }()
			metadata, metadataErr := s.remote.LatestMetadata(ctx, local.Wiki)
			if metadataErr != nil {
				results <- result{err: metadataErr}
				return
			}
			online := model.OnlineWiki{Name: local.Wiki, DumpDate: metadata.DumpDate, Available: true, Fingerprint: metadata.Fingerprint, PartCount: len(metadata.Parts), Installed: true, UpdateAvailable: metadata.Fingerprint != local.Fingerprint}
			for _, part := range metadata.Parts {
				online.DumpSize += part.Dump.Size
				online.IndexSize += part.Index.Size
			}
			if len(metadata.Parts) == 1 {
				online.DumpSHA1, online.IndexSHA1 = metadata.Parts[0].Dump.SHA1, metadata.Parts[0].Index.SHA1
			}
			results <- result{wiki: online}
		}()
	}
	go func() {
		wg.Wait()
		close(results)
	}()
	upgrades := make([]model.OnlineWiki, 0, len(locals))
	for item := range results {
		if item.err != nil {
			if errors.Is(item.err, context.Canceled) {
				return nil, item.err
			}
			continue
		}
		upgrades = append(upgrades, item.wiki)
	}
	sort.Slice(upgrades, func(i, j int) bool { return upgrades[i].Name < upgrades[j].Name })
	return upgrades, nil
}

func (s *Store) Submit(wiki, kind string) (model.Job, error) {
	if !wikimedia.ValidWikiName(wiki) {
		return model.Job{}, fmt.Errorf("invalid Wikimedia database name %q", wiki)
	}
	if kind != "download" && kind != "update" {
		return model.Job{}, errors.New("job kind must be download or update")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, installedErr := os.Stat(filepath.Join(s.wikiPath(wiki), "manifest.json"))
	installed := installedErr == nil
	if kind == "download" && installed {
		return model.Job{}, fmt.Errorf("wiki %s is already installed; use update", wiki)
	}
	if kind == "update" && !installed {
		return model.Job{}, fmt.Errorf("wiki %s is not installed; use download", wiki)
	}
	if id := s.active[wiki]; id != "" {
		return *s.jobs[id], nil
	}
	var retry *model.Job
	for _, existing := range s.jobs {
		if existing.Wiki == wiki && existing.Kind == kind && existing.State == model.StateFailed && (retry == nil || existing.UpdatedAt.After(retry.UpdatedAt)) {
			retry = existing
		}
	}
	if retry != nil {
		retry.State, retry.Phase, retry.Error, retry.Message, retry.UpdatedAt = model.StateQueued, "queued", "", "retrying with existing partial downloads", time.Now().UTC()
		s.active[wiki] = retry.ID
		if err := s.saveJobsLocked(); err != nil {
			delete(s.active, wiki)
			return model.Job{}, err
		}
		s.queue <- retry.ID
		return *retry, nil
	}
	id, err := newID()
	if err != nil {
		return model.Job{}, err
	}
	now := time.Now().UTC()
	job := &model.Job{ID: id, Wiki: wiki, Kind: kind, State: model.StateQueued, Phase: "queued", CreatedAt: now, UpdatedAt: now}
	s.jobs[id], s.active[wiki] = job, id
	if err := s.saveJobsLocked(); err != nil {
		delete(s.jobs, id)
		delete(s.active, wiki)
		return model.Job{}, err
	}
	s.queue <- id
	return *job, nil
}

func (s *Store) Job(id, wiki string) (model.Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if id == "" && wiki != "" {
		id = s.active[wiki]
		if id == "" {
			for candidateID, job := range s.jobs {
				if job.Wiki == wiki && (id == "" || job.UpdatedAt.After(s.jobs[id].UpdatedAt)) {
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

func (s *Store) Search(wiki, query string, offset, limit int) (model.SearchResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	manifest, err := readManifest(filepath.Join(s.wikiPath(wiki), "manifest.json"))
	if err != nil || !manifest.TitleReady {
		return model.SearchResult{}, fmt.Errorf("wiki %s is not title-ready", wiki)
	}
	result, err := wikiindex.Search(s.wikiPath(wiki), query, offset, limit, manifest.BodyReady)
	if err != nil {
		return result, err
	}
	result.Wiki = wiki
	for i := range result.Hits {
		page, readErr := wikiindex.ReadPage(s.wikiPath(wiki), "", result.Hits[i].PageID, "text", 0, 1200)
		if readErr == nil {
			result.Hits[i].Snippet = snippet(page.Content, query, 280)
		}
	}
	return result, nil
}

func (s *Store) Read(wiki, title string, pageID uint64, format string, start, maxChars int) (model.Page, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	manifest, err := readManifest(filepath.Join(s.wikiPath(wiki), "manifest.json"))
	if err != nil || !manifest.TitleReady {
		return model.Page{}, fmt.Errorf("wiki %s is not title-ready", wiki)
	}
	page, err := wikiindex.ReadPage(s.wikiPath(wiki), title, pageID, format, start, maxChars)
	page.Wiki = wiki
	return page, err
}

func (s *Store) worker(ctx context.Context) {
	defer s.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case id := <-s.queue:
			s.runJob(ctx, id)
		}
	}
}

func (s *Store) runJob(ctx context.Context, id string) {
	s.setJob(id, model.StateDiscovering, "discovering", 0, 0, "", 0, "checking Wikimedia dump metadata", "")
	job, err := s.Job(id, "")
	if err != nil {
		return
	}
	metadata, err := s.remote.LatestMetadata(ctx, job.Wiki)
	if err != nil {
		s.failJob(id, err)
		return
	}
	current, currentErr := readManifest(filepath.Join(s.wikiPath(job.Wiki), "manifest.json"))
	if currentErr == nil && current.Fingerprint == metadata.Fingerprint {
		if current.BodyReady {
			s.finishJob(id, model.StateUpToDate, "local dump and indexes are current")
			return
		}
		s.buildBody(ctx, id, job.Wiki, current)
		return
	}
	stage := filepath.Join(s.root, ".staging", id)
	if err := os.MkdirAll(stage, 0o755); err != nil {
		s.failJob(id, err)
		return
	}
	partsDir := filepath.Join(stage, "parts")
	if err := os.MkdirAll(partsDir, 0o755); err != nil {
		s.failJob(id, err)
		return
	}
	if len(metadata.Parts) == 1 {
		if err := migrateLegacyStage(stage, partsDir); err != nil {
			s.failJob(id, err)
			return
		}
	}
	var totalBytes int64
	for _, part := range metadata.Parts {
		totalBytes += part.Index.Size + part.Dump.Size
	}
	completedBytes := int64(0)
	download := func(label string, file model.FileMetadata, path string) error {
		s.setJob(id, model.StateDownloading, "downloading_"+label, completedBytes, totalBytes, "bytes", 0, "downloading "+label, "")
		return s.remote.Download(ctx, file, path, func(done, total int64, rate float64) {
			s.setJob(id, model.StateDownloading, "downloading_"+label, completedBytes+done, totalBytes, "bytes", rate, "downloading "+label, "")
		})
	}
	localParts := make([]wikiindex.Part, 0, len(metadata.Parts))
	for i, part := range metadata.Parts {
		indexPath := filepath.Join(partsDir, fmt.Sprintf("%03d.index.bz2", i))
		dumpPath := filepath.Join(partsDir, fmt.Sprintf("%03d.dump.bz2", i))
		partLabel := fmt.Sprintf("index part %d/%d", i+1, len(metadata.Parts))
		if err := download(partLabel, part.Index, indexPath); err != nil {
			s.failJob(id, err)
			return
		}
		completedBytes += part.Index.Size
		partLabel = fmt.Sprintf("dump part %d/%d", i+1, len(metadata.Parts))
		if err := download(partLabel, part.Dump, dumpPath); err != nil {
			s.failJob(id, err)
			return
		}
		completedBytes += part.Dump.Size
		localParts = append(localParts, wikiindex.Part{Number: i, DumpPath: dumpPath, IndexPath: indexPath})
	}
	s.setJob(id, model.StateTitleIndexing, "title_indexing", 0, 0, "pages", 0, "building title index", "")
	count, err := wikiindex.BuildTitle(ctx, localParts, filepath.Join(stage, wikiindex.TitleIndexDir), func(done, total int64) {
		s.setJob(id, model.StateTitleIndexing, "title_indexing", done, total, "pages", 0, "building title index", "")
	})
	if err != nil {
		s.failJob(id, err)
		return
	}
	manifest := model.Manifest{Wiki: job.Wiki, DumpDate: metadata.DumpDate, Fingerprint: metadata.Fingerprint, PartCount: len(metadata.Parts), PageCount: count, TitleReady: true, PublishedAt: time.Now().UTC()}
	for _, part := range metadata.Parts {
		manifest.DumpSize += part.Dump.Size
		manifest.IndexSize += part.Index.Size
	}
	if len(metadata.Parts) == 1 {
		manifest.DumpSHA1, manifest.IndexSHA1 = metadata.Parts[0].Dump.SHA1, metadata.Parts[0].Index.SHA1
	}
	if err := writeJSON(filepath.Join(stage, "manifest.json"), manifest); err != nil {
		s.failJob(id, err)
		return
	}
	if err := s.publish(job.Wiki, stage); err != nil {
		s.failJob(id, err)
		return
	}
	s.mu.Lock()
	if currentJob := s.jobs[id]; currentJob != nil {
		currentJob.TitleAvailable = true
	}
	s.mu.Unlock()
	s.buildBody(ctx, id, job.Wiki, manifest)
}

func (s *Store) buildBody(ctx context.Context, id, wiki string, manifest model.Manifest) {
	path := s.wikiPath(wiki)
	temporary := filepath.Join(path, wikiindex.BodyIndexDir+".building")
	s.setJob(id, model.StateBodyIndexing, "body_indexing", 0, 0, "streams", 0, "title search and page reads are available; building full-text index", "")
	if err := wikiindex.BuildBody(ctx, generationParts(path, manifest.PartCount), temporary, func(done, total int64) {
		s.setJob(id, model.StateBodyIndexing, "body_indexing", done, total, "streams", 0, "title search and page reads are available; building full-text index", "")
	}); err != nil {
		s.failJob(id, err)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	final := filepath.Join(path, wikiindex.BodyIndexDir)
	if err := os.RemoveAll(final); err != nil {
		s.failJobLocked(id, err)
		return
	}
	if err := os.Rename(temporary, final); err != nil {
		s.failJobLocked(id, err)
		return
	}
	manifest.BodyReady = true
	if err := writeJSON(filepath.Join(path, "manifest.json"), manifest); err != nil {
		s.failJobLocked(id, err)
		return
	}
	s.finishJobLocked(id, model.StateReady, "full-text index is ready")
}

func generationParts(path string, count int) []wikiindex.Part {
	parts := make([]wikiindex.Part, 0, count)
	for i := range count {
		parts = append(parts, wikiindex.Part{Number: i, DumpPath: filepath.Join(path, "parts", fmt.Sprintf("%03d.dump.bz2", i)), IndexPath: filepath.Join(path, "parts", fmt.Sprintf("%03d.index.bz2", i))})
	}
	return parts
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

func (s *Store) publish(wiki, stage string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	destination := s.wikiPath(wiki)
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
	job.State, job.Phase, job.Completed, job.Total, job.Units, job.Rate, job.Message, job.Error = state, phase, completed, total, units, rate, message, errText
	job.UpdatedAt = time.Now().UTC()
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
	if errors.Is(err, context.Canceled) {
		job.State, job.Phase, job.Error, job.Message, job.UpdatedAt = model.StateQueued, "queued", "", "paused for server shutdown; will resume on restart", time.Now().UTC()
		_ = s.saveJobsLocked()
		return
	}
	job.State, job.Phase, job.Error, job.Message, job.UpdatedAt = model.StateFailed, "failed", err.Error(), "job failed", time.Now().UTC()
	delete(s.active, job.Wiki)
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
	job.State, job.Phase, job.Message, job.UpdatedAt = state, "complete", message, time.Now().UTC()
	job.TitleAvailable = state == model.StateReady || job.TitleAvailable
	delete(s.active, job.Wiki)
	_ = s.saveJobsLocked()
}

func (s *Store) wikiPath(wiki string) string { return filepath.Join(s.root, "wikis", wiki) }

func (s *Store) jobsPath() string { return filepath.Join(s.root, "jobs.json") }

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
	for _, job := range jobs {
		if job.State != model.StateReady && job.State != model.StateUpToDate && job.State != model.StateFailed {
			job.State, job.Phase, job.Message = model.StateQueued, "queued", "resuming after server restart"
			s.active[job.Wiki] = job.ID
			s.queue <- job.ID
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
	return writeJSON(s.jobsPath(), jobs)
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

func directorySize(path string) (int64, error) {
	var size int64
	err := filepath.WalkDir(path, func(_ string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.Type().IsRegular() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		size += info.Size()
		return nil
	})
	return size, err
}

func snippet(text, query string, length int) string {
	runes := []rune(text)
	if len(runes) <= length {
		return text
	}
	position := strings.Index(strings.ToLower(text), strings.ToLower(strings.Fields(query)[0]))
	if position < 0 {
		return string(runes[:length]) + "…"
	}
	runePosition := len([]rune(text[:position]))
	start := max(0, runePosition-length/3)
	end := min(len(runes), start+length)
	return "…" + string(runes[start:end]) + "…"
}
