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
	"hash"
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
	wikiNameRE   = regexp.MustCompile(`^[a-z0-9_]+(?:wiki|wikibooks|wikinews|wikiquote|wikisource|wikispecies|wikiversity|wikivoyage|wiktionary)$`)
	catalogRE    = regexp.MustCompile(`<a href="([a-z0-9_]+)/">[^<]*</a>\s+([0-9]{2}-[A-Za-z]{3}-[0-9]{4})`)
	exportFileRE = regexp.MustCompile(`^[a-z0-9_]+-[0-9]{4}-[0-9]{2}-[0-9]{2}-(?:p[0-9]+p[0-9]+|p[0-9]+r[0-9]+r[0-9]+)\.xml\.bz2$`)
)

const currentExportPath = "/other/mediawiki_content_current"

type catalogEntry struct {
	Name        string
	ReleaseDate string
	Closed      bool
}

type Client struct {
	baseURL       string
	siteMatrixURL string
	http          *http.Client
	parallel      int
	downloadSlots chan struct{}
	mu            sync.Mutex
	catalog       []catalogEntry
	cached        time.Time
	metadata      map[string]cachedMetadata
	sites         map[string]model.DatasetMetadata
	sitesCached   time.Time
}

type cachedMetadata struct {
	value  model.ReleaseMetadata
	cached time.Time
}

func NewClient(parallel ...int) *Client {
	return NewClientWithBaseURL(defaultBaseURL, parallel...)
}

func NewClientWithBaseURL(baseURL string, parallel ...int) *Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ResponseHeaderTimeout = 30 * time.Second
	connections := 3
	if len(parallel) > 0 && parallel[0] > 0 {
		connections = parallel[0]
	}
	siteMatrixURL := strings.TrimRight(baseURL, "/") + "/sitematrix"
	if strings.TrimRight(baseURL, "/") == defaultBaseURL {
		siteMatrixURL = "https://meta.wikimedia.org/w/api.php?action=sitematrix&format=json&formatversion=2&smlimit=max&smtype=special%7Clanguage&smstate=all"
	}
	return &Client{
		baseURL:       strings.TrimRight(baseURL, "/"),
		siteMatrixURL: siteMatrixURL,
		http:          &http.Client{Transport: transport},
		parallel:      connections,
		downloadSlots: make(chan struct{}, connections),
		metadata:      make(map[string]cachedMetadata),
		sites:         make(map[string]model.DatasetMetadata),
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
	sites, _ := c.loadSiteMatrix(ctx)
	filter = strings.ToLower(strings.TrimSpace(filter))
	filtered := make([]model.AvailableDataset, 0, len(entries))
	for _, entry := range entries {
		wiki := onlineWikiFromCatalog(entry, sites[entry.Name])
		haystack := strings.ToLower(strings.Join([]string{wiki.ID, wiki.DisplayName, wiki.Project, wiki.ContentType, wiki.Language.Code, wiki.Language.Name, wiki.Language.LocalName}, " "))
		if filter == "" || strings.Contains(haystack, filter) {
			filtered = append(filtered, wiki)
		}
	}
	if offset < 0 {
		offset = 0
	}
	fullCatalog := limit == -1
	if !fullCatalog && (limit <= 0 || limit > 50) {
		limit = 20
	}
	result := model.AvailableResult{Offset: offset, Total: len(filtered)}
	if offset >= len(filtered) {
		return result, nil
	}
	end := len(filtered)
	if !fullCatalog {
		end = min(offset+limit, len(filtered))
	}
	selected := filtered[offset:end]
	result.Datasets = append([]model.AvailableDataset(nil), selected...)
	if fullCatalog {
		return result, nil
	}

	sem := make(chan struct{}, 3)
	var wg sync.WaitGroup
	for i, wiki := range selected {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			metadata, metadataErr := c.Metadata(ctx, wiki.ID, wiki.ReleaseDate)
			if metadataErr == nil {
				wiki.Available = true
				wiki.Fingerprint = metadata.Fingerprint
				wiki.PartCount = len(metadata.Parts)
				for _, part := range metadata.Parts {
					wiki.RawSize += part.Raw.Size
					wiki.ProviderMetadataSize += part.ProviderMetadata.Size
				}
				if len(metadata.Parts) == 1 {
					wiki.RawHash = firstHash(metadata.Parts[0].Raw)
					wiki.ProviderMetadataHash = metadata.Parts[0].ProviderMetadata.SHA1
				}
			}
			result.Datasets[i] = wiki
		}()
	}
	wg.Wait()
	if end < len(filtered) {
		result.NextOffset = end
	}
	return result, nil
}

func onlineWikiFromCatalog(entry catalogEntry, site model.DatasetMetadata) model.AvailableDataset {
	if site.Project == "" {
		site = inferSiteMetadata(entry.Name)
	}
	return model.AvailableDataset{
		ID: entry.Name, DisplayName: site.Name, Project: site.Project, ContentType: site.ContentType,
		Language: site.Language, OnlineSourceURL: site.OnlineSourceURL, ReleaseDate: entry.ReleaseDate,
		Closed: entry.Closed || site.Closed, Available: entry.ReleaseDate != "",
	}
}

func (c *Client) LatestMetadata(ctx context.Context, wiki string) (model.ReleaseMetadata, error) {
	if !ValidWikiName(wiki) {
		return model.ReleaseMetadata{}, fmt.Errorf("invalid Wikimedia database name %q", wiki)
	}
	entries, err := c.loadCatalog(ctx, false)
	if err != nil {
		return model.ReleaseMetadata{}, err
	}
	for _, entry := range entries {
		if entry.Name == wiki {
			if entry.ReleaseDate != "" {
				return c.Metadata(ctx, wiki, entry.ReleaseDate)
			}
			return c.latestCurrentMetadata(ctx, wiki)
		}
	}
	return model.ReleaseMetadata{}, fmt.Errorf("wiki %q was not found in the Wikimedia dump catalog", wiki)
}

func (c *Client) Metadata(ctx context.Context, wiki, dumpDate string) (model.ReleaseMetadata, error) {
	if !ValidWikiName(wiki) || len(dumpDate) != 8 {
		return model.ReleaseMetadata{}, errors.New("invalid wiki or dump date")
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
	datePath := dumpDate[:4] + "-" + dumpDate[4:6] + "-" + dumpDate[6:]
	directoryURL := fmt.Sprintf("%s%s/%s/%s/xml/bzip2", c.baseURL, currentExportPath, wiki, datePath)
	url := directoryURL + "/SHA256SUMS"
	req, err := http.NewRequestWithContext(metadataCtx, http.MethodGet, url, nil)
	if err != nil {
		return model.ReleaseMetadata{}, err
	}
	req.Header.Set("User-Agent", userAgent)
	resp, release, err := c.doDownloadRequest(metadataCtx, req)
	if err != nil {
		return model.ReleaseMetadata{}, fmt.Errorf("fetch dump metadata: %w", err)
	}
	defer release()
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return model.ReleaseMetadata{}, fmt.Errorf("fetch dump metadata: %s", resp.Status)
	}
	checksums, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return model.ReleaseMetadata{}, fmt.Errorf("read dump checksums: %w", err)
	}
	hashes := make(map[string]string)
	for _, line := range strings.Split(string(checksums), "\n") {
		fields := strings.Fields(line)
		name := ""
		if len(fields) == 2 {
			name = strings.TrimPrefix(fields[1], "*")
		}
		if len(fields) == 2 && len(fields[0]) == sha256.Size*2 && exportFileRE.MatchString(name) {
			hashes[name] = strings.ToLower(fields[0])
		}
	}
	type inspectedFile struct {
		name     string
		metadata model.FileMetadata
		err      error
	}
	inspectCtx, cancelInspect := context.WithCancel(ctx)
	defer cancelInspect()
	tasks, inspected := make(chan string), make(chan inspectedFile, max(1, c.parallel))
	var inspectors sync.WaitGroup
	for range min(max(1, c.parallel), len(hashes)) {
		inspectors.Add(1)
		go func() {
			defer inspectors.Done()
			for name := range tasks {
				address := directoryURL + "/" + name
				size, sizeErr := c.remoteSize(inspectCtx, address)
				select {
				case inspected <- inspectedFile{name: name, metadata: model.FileMetadata{URL: address, Size: size, SHA256: hashes[name]}, err: sizeErr}:
				case <-inspectCtx.Done():
					return
				}
			}
		}()
	}
	go func() {
		defer close(tasks)
		for name := range hashes {
			select {
			case tasks <- name:
			case <-inspectCtx.Done():
				return
			}
		}
	}()
	go func() { inspectors.Wait(); close(inspected) }()
	files := make(map[string]model.FileMetadata, len(hashes))
	for result := range inspected {
		if result.err != nil {
			cancelInspect()
			return model.ReleaseMetadata{}, fmt.Errorf("inspect %s: %w", result.name, result.err)
		}
		files[result.name] = result.metadata
	}
	if len(files) == 0 {
		return model.ReleaseMetadata{}, errors.New("current-content export checksum manifest contained no XML parts")
	}
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	metadata := model.ReleaseMetadata{Dataset: wiki, Format: "mediawiki-content-current", ReleaseDate: dumpDate, Parts: make([]model.ReleasePart, 0, len(names))}
	fingerprint := sha256.New()
	for _, name := range names {
		file := files[name]
		metadata.Parts = append(metadata.Parts, model.ReleasePart{Key: name, Raw: file})
		_, _ = fmt.Fprintf(fingerprint, "%s\x00%s\x00", name, file.SHA256)
	}
	metadata.Fingerprint = hex.EncodeToString(fingerprint.Sum(nil))
	c.mu.Lock()
	c.metadata[cacheKey] = cachedMetadata{value: metadata, cached: time.Now()}
	c.mu.Unlock()
	return metadata, nil
}

func (c *Client) loadCatalog(ctx context.Context, refresh bool) ([]catalogEntry, error) {
	c.mu.Lock()
	if !refresh && len(c.catalog) > 0 && time.Since(c.cached) < 24*time.Hour {
		entries := append([]catalogEntry(nil), c.catalog...)
		c.mu.Unlock()
		return entries, nil
	}
	c.mu.Unlock()

	metadataCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(metadataCtx, http.MethodGet, c.baseURL+currentExportPath+"/", nil)
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
		name := match[1]
		if !ValidWikiName(name) {
			continue
		}
		modified, parseErr := time.Parse("02-Jan-2006", match[2])
		if parseErr != nil {
			continue
		}
		releaseDate := time.Date(modified.Year(), modified.Month(), 1, 0, 0, 0, 0, time.UTC).Format("20060102")
		entries = append(entries, catalogEntry{Name: name, ReleaseDate: releaseDate})
	}
	if len(entries) == 0 {
		sites, siteErr := c.loadSiteMatrix(ctx)
		if siteErr != nil {
			return nil, errors.Join(errors.New("wikimedia current-content catalog contained no completed exports"), siteErr)
		}
		for name, site := range sites {
			if ValidWikiName(name) {
				entries = append(entries, catalogEntry{Name: name, Closed: site.Closed})
			}
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	c.mu.Lock()
	c.catalog, c.cached = entries, time.Now()
	c.mu.Unlock()
	return append([]catalogEntry(nil), entries...), nil
}

func (c *Client) latestCurrentMetadata(ctx context.Context, wiki string) (model.ReleaseMetadata, error) {
	now := time.Now().UTC()
	for monthsBack := 0; monthsBack < 6; monthsBack++ {
		date := time.Date(now.Year(), now.Month()-time.Month(monthsBack), 1, 0, 0, 0, 0, time.UTC).Format("20060102")
		metadata, err := c.Metadata(ctx, wiki, date)
		if err == nil {
			return metadata, nil
		}
	}
	return model.ReleaseMetadata{}, fmt.Errorf("no completed current-content export found for %s in the last six months", wiki)
}

func (c *Client) remoteSize(ctx context.Context, address string) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, address, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("User-Agent", userAgent)
	resp, release, err := c.doDownloadRequest(ctx, req)
	if err != nil {
		return 0, err
	}
	defer release()
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("%s", resp.Status)
	}
	if resp.ContentLength <= 0 {
		return 0, errors.New("missing Content-Length")
	}
	return resp.ContentLength, nil
}

type ProgressFunc func(completed, total int64, rate float64)

const parallelDownloadThreshold = 64 << 20

type resumeState struct {
	Size   int64         `json:"size"`
	Hash   string        `json:"hash"`
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
		if err := verifyFile(destination, file); err == nil {
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
	return verifyFile(destination, file)
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
	if err := verifyFile(destination, file); err != nil {
		_ = os.Remove(resumePath)
		_ = os.Truncate(destination, 0)
		return err
	}
	return os.Remove(resumePath)
}

func newResumeState(file model.FileMetadata, connections int, existingSize int64) resumeState {
	connections = min(connections, int(file.Size))
	state := resumeState{Size: file.Size, Hash: fileHash(file), Chunks: make([]resumeChunk, connections)}
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
	if state.Size != file.Size || !strings.EqualFold(state.Hash, fileHash(file)) || len(state.Chunks) == 0 {
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
	for attempt := 0; ; attempt++ {
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
		delay := time.Duration(1<<min(attempt, 5)) * time.Second
		if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil && seconds > 0 {
			delay = min(time.Duration(seconds)*time.Second, time.Minute)
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
}

func verifyFile(path string, metadata model.FileMetadata) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	var h hash.Hash
	expected, algorithm := metadata.SHA256, "SHA-256"
	if expected != "" {
		h = sha256.New()
	} else {
		expected, algorithm, h = metadata.SHA1, "SHA-1", sha1.New()
	}
	if expected == "" {
		return errors.New("wikimedia file has no integrity hash")
	}
	if _, err := io.Copy(h, f); err != nil {
		return err
	}
	got := hex.EncodeToString(h.Sum(nil))
	if !strings.EqualFold(got, expected) {
		return fmt.Errorf("%s mismatch: got %s, expected %s", algorithm, got, expected)
	}
	return nil
}

func fileHash(metadata model.FileMetadata) string {
	if metadata.SHA256 != "" {
		return "sha256:" + strings.ToLower(metadata.SHA256)
	}
	return "sha1:" + strings.ToLower(metadata.SHA1)
}

func firstHash(metadata model.FileMetadata) string {
	if metadata.SHA256 != "" {
		return metadata.SHA256
	}
	return metadata.SHA1
}
