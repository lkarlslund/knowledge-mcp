package wikimedia

import (
	"bufio"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/model"
)

const defaultBaseURL = "https://dumps.wikimedia.org"
const userAgent = "wikipedia-multistream-mcp/0.1 (+https://github.com/lkarlslund/wikipedia-multistream-mcp)"

var (
	wikiNameRE  = regexp.MustCompile(`^[a-z0-9_]+(?:wiki|wikibooks|wikinews|wikiquote|wikisource|wikispecies|wikiversity|wikivoyage|wiktionary)$`)
	catalogRE   = regexp.MustCompile(`<li>([^<]+)<a href="([a-z0-9_]+)/([0-9]{8})">([^<]+)</a>(?: \(closed\))?: <span class='([^']+)'>([^<]+)</span></li>`)
	dumpPartRE  = regexp.MustCompile(`-pages-articles-multistream([0-9]*)\.xml(?:-(p[0-9]+p[0-9]+))?\.bz2$`)
	indexPartRE = regexp.MustCompile(`-pages-articles-multistream-index([0-9]*)\.txt(?:-(p[0-9]+p[0-9]+))?\.bz2$`)
)

type catalogEntry struct {
	Name     string
	DumpDate string
	Closed   bool
}

type Client struct {
	baseURL       string
	http          *http.Client
	parallel      int
	downloadSlots chan struct{}
	mu            sync.Mutex
	catalog       []catalogEntry
	cached        time.Time
	metadata      map[string]cachedMetadata
}

type cachedMetadata struct {
	value  model.DumpMetadata
	cached time.Time
}

func NewClient(parallel ...int) *Client {
	return NewClientWithBaseURL(defaultBaseURL, parallel...)
}

func NewClientWithBaseURL(baseURL string, parallel ...int) *Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ResponseHeaderTimeout = 30 * time.Second
	connections := 4
	if len(parallel) > 0 && parallel[0] > 0 {
		connections = parallel[0]
	}
	return &Client{
		baseURL:       strings.TrimRight(baseURL, "/"),
		http:          &http.Client{Transport: transport},
		parallel:      connections,
		downloadSlots: make(chan struct{}, connections),
		metadata:      make(map[string]cachedMetadata),
	}
}

func ValidWikiName(name string) bool {
	return wikiNameRE.MatchString(name)
}

func (c *Client) ListAvailable(ctx context.Context, filter string, offset, limit int, refresh bool) (model.AvailableResult, error) {
	entries, err := c.loadCatalog(ctx, refresh)
	if err != nil {
		return model.AvailableResult{}, err
	}
	filter = strings.ToLower(strings.TrimSpace(filter))
	filtered := make([]catalogEntry, 0, len(entries))
	for _, entry := range entries {
		if filter == "" || strings.Contains(entry.Name, filter) {
			filtered = append(filtered, entry)
		}
	}
	if offset < 0 {
		offset = 0
	}
	if limit <= 0 || limit > 50 {
		limit = 20
	}
	result := model.AvailableResult{Offset: offset, Total: len(filtered)}
	if offset >= len(filtered) {
		return result, nil
	}
	end := min(offset+limit, len(filtered))
	selected := filtered[offset:end]
	result.Wikis = make([]model.OnlineWiki, len(selected))

	sem := make(chan struct{}, 3)
	var wg sync.WaitGroup
	for i, entry := range selected {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			wiki := model.OnlineWiki{Name: entry.Name, DumpDate: entry.DumpDate, Closed: entry.Closed}
			metadata, metadataErr := c.Metadata(ctx, entry.Name, entry.DumpDate)
			if metadataErr == nil {
				wiki.Available = true
				wiki.Fingerprint = metadata.Fingerprint
				wiki.PartCount = len(metadata.Parts)
				for _, part := range metadata.Parts {
					wiki.DumpSize += part.Dump.Size
					wiki.IndexSize += part.Index.Size
				}
				if len(metadata.Parts) == 1 {
					wiki.DumpSHA1 = metadata.Parts[0].Dump.SHA1
					wiki.IndexSHA1 = metadata.Parts[0].Index.SHA1
				}
			}
			result.Wikis[i] = wiki
		}()
	}
	wg.Wait()
	if end < len(filtered) {
		result.NextOffset = end
	}
	return result, nil
}

func (c *Client) LatestMetadata(ctx context.Context, wiki string) (model.DumpMetadata, error) {
	if !ValidWikiName(wiki) {
		return model.DumpMetadata{}, fmt.Errorf("invalid Wikimedia database name %q", wiki)
	}
	entries, err := c.loadCatalog(ctx, false)
	if err != nil {
		return model.DumpMetadata{}, err
	}
	for _, entry := range entries {
		if entry.Name == wiki {
			return c.Metadata(ctx, wiki, entry.DumpDate)
		}
	}
	return model.DumpMetadata{}, fmt.Errorf("wiki %q was not found in the Wikimedia dump catalog", wiki)
}

func (c *Client) Metadata(ctx context.Context, wiki, dumpDate string) (model.DumpMetadata, error) {
	if !ValidWikiName(wiki) || len(dumpDate) != 8 {
		return model.DumpMetadata{}, errors.New("invalid wiki or dump date")
	}
	cacheKey := wiki + "/" + dumpDate
	c.mu.Lock()
	if cached, ok := c.metadata[cacheKey]; ok && time.Since(cached.cached) < time.Hour {
		c.mu.Unlock()
		return cached.value, nil
	}
	c.mu.Unlock()
	metadataCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	url := fmt.Sprintf("%s/%s/%s/dumpstatus.json", c.baseURL, wiki, dumpDate)
	req, err := http.NewRequestWithContext(metadataCtx, http.MethodGet, url, nil)
	if err != nil {
		return model.DumpMetadata{}, err
	}
	req.Header.Set("User-Agent", userAgent)
	resp, release, err := c.doDownloadRequest(metadataCtx, req)
	if err != nil {
		return model.DumpMetadata{}, fmt.Errorf("fetch dump metadata: %w", err)
	}
	defer release()
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return model.DumpMetadata{}, fmt.Errorf("fetch dump metadata: %s", resp.Status)
	}
	var status struct {
		Jobs map[string]struct {
			Status string `json:"status"`
			Files  map[string]struct {
				Size int64  `json:"size"`
				URL  string `json:"url"`
				SHA1 string `json:"sha1"`
			} `json:"files"`
		} `json:"jobs"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 16<<20)).Decode(&status); err != nil {
		return model.DumpMetadata{}, fmt.Errorf("decode dump metadata: %w", err)
	}
	job, ok := status.Jobs["articlesmultistreamdump"]
	if !ok || job.Status != "done" {
		return model.DumpMetadata{}, errors.New("completed multistream article dump is unavailable")
	}
	type partialPart struct {
		key   string
		dump  model.FileMetadata
		index model.FileMetadata
		order int64
	}
	parts := make(map[string]*partialPart)
	for name, file := range job.Files {
		item := model.FileMetadata{URL: c.baseURL + file.URL, Size: file.Size, SHA1: file.SHA1}
		if match := dumpPartRE.FindStringSubmatch(name); match != nil {
			key, order := partKey(match[1], match[2])
			part := parts[key]
			if part == nil {
				part = &partialPart{key: key, order: order}
				parts[key] = part
			}
			part.dump = item
		}
		if match := indexPartRE.FindStringSubmatch(name); match != nil {
			key, order := partKey(match[1], match[2])
			part := parts[key]
			if part == nil {
				part = &partialPart{key: key, order: order}
				parts[key] = part
			}
			part.index = item
		}
	}
	ordered := make([]*partialPart, 0, len(parts))
	for _, part := range parts {
		if part.dump.URL == "" || part.index.URL == "" {
			return model.DumpMetadata{}, fmt.Errorf("multistream part %q is missing its dump or index", part.key)
		}
		ordered = append(ordered, part)
	}
	if len(ordered) == 0 {
		return model.DumpMetadata{}, errors.New("multistream dump metadata is incomplete")
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].order < ordered[j].order })
	metadata := model.DumpMetadata{Wiki: wiki, DumpDate: dumpDate, Parts: make([]model.DumpPart, 0, len(ordered))}
	fingerprint := sha256.New()
	for _, part := range ordered {
		metadata.Parts = append(metadata.Parts, model.DumpPart{Key: part.key, Dump: part.dump, Index: part.index})
		_, _ = fmt.Fprintf(fingerprint, "%s\x00%s\x00%s\x00", part.key, part.dump.SHA1, part.index.SHA1)
	}
	metadata.Fingerprint = hex.EncodeToString(fingerprint.Sum(nil))
	c.mu.Lock()
	c.metadata[cacheKey] = cachedMetadata{value: metadata, cached: time.Now()}
	c.mu.Unlock()
	return metadata, nil
}

func (c *Client) loadCatalog(ctx context.Context, refresh bool) ([]catalogEntry, error) {
	c.mu.Lock()
	if !refresh && len(c.catalog) > 0 && time.Since(c.cached) < time.Hour {
		entries := append([]catalogEntry(nil), c.catalog...)
		c.mu.Unlock()
		return entries, nil
	}
	c.mu.Unlock()

	metadataCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(metadataCtx, http.MethodGet, c.baseURL+"/backup-index-bydb.html", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", userAgent)
	resp, release, err := c.doDownloadRequest(metadataCtx, req)
	if err != nil {
		return nil, fmt.Errorf("fetch Wikimedia catalog: %w", err)
	}
	defer release()
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch Wikimedia catalog: %s", resp.Status)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return nil, fmt.Errorf("read Wikimedia catalog: %w", err)
	}
	matches := catalogRE.FindAllStringSubmatch(string(body), -1)
	entries := make([]catalogEntry, 0, len(matches))
	for _, match := range matches {
		name := match[2]
		if !ValidWikiName(name) {
			continue
		}
		entries = append(entries, catalogEntry{Name: name, DumpDate: match[3], Closed: strings.Contains(match[0], "(closed)")})
	}
	if len(entries) == 0 {
		return nil, errors.New("wikimedia catalog contained no valid entries")
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	c.mu.Lock()
	c.catalog, c.cached = entries, time.Now()
	c.mu.Unlock()
	return append([]catalogEntry(nil), entries...), nil
}

func partKey(number, pageRange string) (string, int64) {
	if pageRange == "" {
		return "single", 0
	}
	startText := strings.TrimPrefix(strings.SplitN(pageRange, "p", 3)[1], "p")
	start, _ := strconv.ParseInt(startText, 10, 64)
	return number + "/" + pageRange, start
}

type ProgressFunc func(completed, total int64, rate float64)

const parallelDownloadThreshold = 64 << 20

type resumeState struct {
	Size   int64         `json:"size"`
	SHA1   string        `json:"sha1"`
	Chunks []resumeChunk `json:"chunks"`
}

type resumeChunk struct {
	Start     int64 `json:"start"`
	End       int64 `json:"end"`
	Completed int64 `json:"completed"`
}

func (c *Client) Download(ctx context.Context, file model.FileMetadata, destination string, progress ProgressFunc) error {
	if !strings.HasPrefix(file.URL, c.baseURL+"/") {
		return errors.New("refusing download outside configured Wikimedia origin")
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
		return err
	}
	var start int64
	if info, err := os.Stat(destination); err == nil {
		start = info.Size()
		if start > file.Size {
			return errors.New("partial download is larger than expected")
		}
	}
	resumePath := destination + ".resume.json"
	_, resumeErr := os.Stat(resumePath)
	if c.parallel > 1 && file.Size >= parallelDownloadThreshold && (start < file.Size || resumeErr == nil) {
		supported, err := c.supportsRanges(ctx, file.URL)
		if err != nil {
			return err
		}
		if supported {
			return c.downloadParallel(ctx, file, destination, resumePath, start, progress)
		}
	}
	if start == file.Size {
		if err := verifySHA1(destination, file.SHA1); err == nil {
			progress(start, file.Size, 0)
			return nil
		}
		if err := os.Truncate(destination, 0); err != nil {
			return fmt.Errorf("reset invalid completed download: %w", err)
		}
		start = 0
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, file.URL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", userAgent)
	if start > 0 {
		req.Header.Set("Range", "bytes="+strconv.FormatInt(start, 10)+"-")
	}
	resp, release, err := c.doDownloadRequest(ctx, req)
	if err != nil {
		return fmt.Errorf("download %s: %w", filepath.Base(destination), err)
	}
	defer release()
	defer func() { _ = resp.Body.Close() }()
	if start > 0 && resp.StatusCode != http.StatusPartialContent {
		start = 0
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("download %s: %s", filepath.Base(destination), resp.Status)
	}
	flags := os.O_CREATE | os.O_WRONLY
	if start > 0 {
		flags |= os.O_APPEND
	} else {
		flags |= os.O_TRUNC
	}
	out, err := os.OpenFile(destination, flags, 0o644)
	if err != nil {
		return err
	}
	defer func() { _ = out.Close() }()
	reader := bufio.NewReaderSize(resp.Body, 256<<10)
	buf := make([]byte, 256<<10)
	completed := start
	lastBytes, lastTime := completed, time.Now()
	for {
		n, readErr := reader.Read(buf)
		if n > 0 {
			if _, err := out.Write(buf[:n]); err != nil {
				return err
			}
			completed += int64(n)
			now := time.Now()
			if now.Sub(lastTime) >= 500*time.Millisecond {
				progress(completed, file.Size, float64(completed-lastBytes)/now.Sub(lastTime).Seconds())
				lastBytes, lastTime = completed, now
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return readErr
		}
	}
	progress(completed, file.Size, 0)
	if completed != file.Size {
		return fmt.Errorf("downloaded size %d, expected %d", completed, file.Size)
	}
	if err := out.Close(); err != nil {
		return fmt.Errorf("close download: %w", err)
	}
	return verifySHA1(destination, file.SHA1)
}

func (c *Client) supportsRanges(ctx context.Context, url string) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Range", "bytes=0-0")
	resp, release, err := c.doDownloadRequest(ctx, req)
	if err != nil {
		return false, fmt.Errorf("probe ranged download: %w", err)
	}
	defer release()
	defer func() { _ = resp.Body.Close() }()
	return resp.StatusCode == http.StatusPartialContent && strings.HasPrefix(resp.Header.Get("Content-Range"), "bytes 0-0/"), nil
}

func (c *Client) downloadParallel(ctx context.Context, file model.FileMetadata, destination, resumePath string, existingSize int64, progress ProgressFunc) error {
	state, err := loadResumeState(resumePath, file)
	if err != nil {
		state = newResumeState(file, c.parallel, existingSize)
		if err := saveResumeState(resumePath, state); err != nil {
			return err
		}
	}
	out, err := os.OpenFile(destination, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer func() { _ = out.Close() }()
	if err := out.Truncate(file.Size); err != nil {
		return err
	}
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var mu sync.Mutex
	completed := int64(0)
	for _, chunk := range state.Chunks {
		completed += chunk.Completed
	}
	lastBytes, lastTime, lastSave := completed, time.Now(), time.Now()
	progress(completed, file.Size, 0)
	errorsCh := make(chan error, len(state.Chunks))
	var workers sync.WaitGroup
	for i := range state.Chunks {
		chunk := state.Chunks[i]
		if chunk.Start+chunk.Completed > chunk.End {
			continue
		}
		workers.Add(1)
		go func(index int, initial resumeChunk) {
			defer workers.Done()
			position := initial.Start + initial.Completed
			req, requestErr := http.NewRequestWithContext(workerCtx, http.MethodGet, file.URL, nil)
			if requestErr != nil {
				errorsCh <- requestErr
				cancel()
				return
			}
			req.Header.Set("User-Agent", userAgent)
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", position, initial.End))
			resp, release, requestErr := c.doDownloadRequest(workerCtx, req)
			if requestErr != nil {
				errorsCh <- requestErr
				cancel()
				return
			}
			defer release()
			defer func() { _ = resp.Body.Close() }()
			contentRangePrefix := fmt.Sprintf("bytes %d-", position)
			if resp.StatusCode != http.StatusPartialContent || !strings.HasPrefix(resp.Header.Get("Content-Range"), contentRangePrefix) {
				errorsCh <- fmt.Errorf("ranged download: %s", resp.Status)
				cancel()
				return
			}
			buf := make([]byte, 256<<10)
			for position <= initial.End {
				n, readErr := resp.Body.Read(buf)
				if n > 0 {
					remaining := initial.End - position + 1
					if int64(n) > remaining {
						n = int(remaining)
					}
					if _, writeErr := out.WriteAt(buf[:n], position); writeErr != nil {
						errorsCh <- writeErr
						cancel()
						return
					}
					position += int64(n)
					mu.Lock()
					state.Chunks[index].Completed += int64(n)
					completed += int64(n)
					now := time.Now()
					if now.Sub(lastTime) >= 500*time.Millisecond {
						progress(completed, file.Size, float64(completed-lastBytes)/now.Sub(lastTime).Seconds())
						lastBytes, lastTime = completed, now
					}
					if now.Sub(lastSave) >= time.Second {
						_ = saveResumeState(resumePath, state)
						lastSave = now
					}
					mu.Unlock()
				}
				if readErr != nil {
					if errors.Is(readErr, io.EOF) && position > initial.End {
						break
					}
					errorsCh <- fmt.Errorf("read ranged download: %w", readErr)
					cancel()
					return
				}
			}
		}(i, chunk)
	}
	workers.Wait()
	close(errorsCh)
	mu.Lock()
	saveErr := saveResumeState(resumePath, state)
	progress(completed, file.Size, 0)
	mu.Unlock()
	if saveErr != nil {
		return saveErr
	}
	for workerErr := range errorsCh {
		if workerErr != nil {
			return workerErr
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if completed != file.Size {
		return fmt.Errorf("downloaded size %d, expected %d", completed, file.Size)
	}
	if err := out.Sync(); err != nil {
		return err
	}
	if err := verifySHA1(destination, file.SHA1); err != nil {
		_ = os.Remove(resumePath)
		_ = os.Truncate(destination, 0)
		return err
	}
	return os.Remove(resumePath)
}

func newResumeState(file model.FileMetadata, connections int, existingSize int64) resumeState {
	connections = min(connections, int(file.Size))
	state := resumeState{Size: file.Size, SHA1: file.SHA1, Chunks: make([]resumeChunk, connections)}
	chunkSize := (file.Size + int64(connections) - 1) / int64(connections)
	for i := range connections {
		start := int64(i) * chunkSize
		end := min(start+chunkSize, file.Size) - 1
		completed := min(max(existingSize-start, 0), end-start+1)
		state.Chunks[i] = resumeChunk{Start: start, End: end, Completed: completed}
	}
	return state
}

func loadResumeState(path string, file model.FileMetadata) (resumeState, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return resumeState{}, err
	}
	var state resumeState
	if err := json.Unmarshal(data, &state); err != nil {
		return resumeState{}, err
	}
	if state.Size != file.Size || !strings.EqualFold(state.SHA1, file.SHA1) || len(state.Chunks) == 0 {
		return resumeState{}, errors.New("resume state does not match remote file")
	}
	for i, chunk := range state.Chunks {
		if chunk.Start < 0 || chunk.End < chunk.Start || chunk.End >= file.Size || chunk.Completed < 0 || chunk.Completed > chunk.End-chunk.Start+1 || i > 0 && chunk.Start != state.Chunks[i-1].End+1 {
			return resumeState{}, errors.New("resume state has invalid chunks")
		}
	}
	if state.Chunks[0].Start != 0 || state.Chunks[len(state.Chunks)-1].End != file.Size-1 {
		return resumeState{}, errors.New("resume state does not cover remote file")
	}
	return state, nil
}

func saveResumeState(path string, state resumeState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	temporary := path + ".tmp"
	if err := os.WriteFile(temporary, data, 0o644); err != nil {
		return err
	}
	return os.Rename(temporary, path)
}

func (c *Client) doDownloadRequest(ctx context.Context, req *http.Request) (*http.Response, func(), error) {
	for attempt := range 6 {
		select {
		case c.downloadSlots <- struct{}{}:
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}
		release := func() { <-c.downloadSlots }
		resp, err := c.http.Do(req.Clone(ctx))
		if err != nil {
			release()
			return nil, nil, err
		}
		if resp.StatusCode != http.StatusTooManyRequests && resp.StatusCode != http.StatusServiceUnavailable {
			return resp, release, nil
		}
		_ = resp.Body.Close()
		release()
		if attempt == 5 {
			return nil, nil, fmt.Errorf("download throttled after retries: %s", resp.Status)
		}
		delay := time.Duration(1<<attempt) * time.Second
		if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil && seconds > 0 {
			delay = time.Duration(seconds) * time.Second
		}
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil, nil, ctx.Err()
		}
	}
	panic("unreachable")
}

func verifySHA1(path, expected string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	h := sha1.New() // Wikimedia dump manifests still use SHA-1 for integrity.
	if _, err := io.Copy(h, f); err != nil {
		return err
	}
	got := hex.EncodeToString(h.Sum(nil))
	if !strings.EqualFold(got, expected) {
		return fmt.Errorf("SHA-1 mismatch: got %s, expected %s", got, expected)
	}
	return nil
}
