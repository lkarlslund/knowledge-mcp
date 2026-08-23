package wikimedia

import (
	"bufio"
	"context"
	"crypto/sha1"
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
	wikiNameRE = regexp.MustCompile(`^[a-z0-9_]+(?:wiki|wikibooks|wikinews|wikiquote|wikisource|wikispecies|wikiversity|wikivoyage|wiktionary)$`)
	catalogRE  = regexp.MustCompile(`<li>([^<]+)<a href="([a-z0-9_]+)/([0-9]{8})">([^<]+)</a>(?: \(closed\))?: <span class='([^']+)'>([^<]+)</span></li>`)
)

type catalogEntry struct {
	Name     string
	DumpDate string
	Closed   bool
}

type Client struct {
	baseURL  string
	http     *http.Client
	mu       sync.Mutex
	catalog  []catalogEntry
	cached   time.Time
	metadata map[string]cachedMetadata
}

type cachedMetadata struct {
	value  model.DumpMetadata
	cached time.Time
}

func NewClient() *Client {
	return NewClientWithBaseURL(defaultBaseURL)
}

func NewClientWithBaseURL(baseURL string) *Client {
	return &Client{baseURL: strings.TrimRight(baseURL, "/"), http: &http.Client{Timeout: 30 * time.Second}, metadata: make(map[string]cachedMetadata)}
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
				wiki.DumpSize = metadata.Dump.Size
				wiki.IndexSize = metadata.Index.Size
				wiki.DumpSHA1 = metadata.Dump.SHA1
				wiki.IndexSHA1 = metadata.Index.SHA1
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
	url := fmt.Sprintf("%s/%s/%s/dumpstatus.json", c.baseURL, wiki, dumpDate)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return model.DumpMetadata{}, err
	}
	req.Header.Set("User-Agent", userAgent)
	resp, err := c.http.Do(req)
	if err != nil {
		return model.DumpMetadata{}, fmt.Errorf("fetch dump metadata: %w", err)
	}
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
	metadata := model.DumpMetadata{Wiki: wiki, DumpDate: dumpDate}
	for name, file := range job.Files {
		item := model.FileMetadata{URL: c.baseURL + file.URL, Size: file.Size, SHA1: file.SHA1}
		switch {
		case strings.HasSuffix(name, "-pages-articles-multistream.xml.bz2"):
			metadata.Dump = item
		case strings.HasSuffix(name, "-pages-articles-multistream-index.txt.bz2"):
			metadata.Index = item
		}
	}
	if metadata.Dump.URL == "" || metadata.Index.URL == "" {
		return model.DumpMetadata{}, errors.New("multistream dump metadata is incomplete")
	}
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

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/backup-index-bydb.html", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", userAgent)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch Wikimedia catalog: %w", err)
	}
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

type ProgressFunc func(completed, total int64, rate float64)

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
	if start == file.Size {
		return verifySHA1(destination, file.SHA1)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, file.URL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", userAgent)
	if start > 0 {
		req.Header.Set("Range", "bytes="+strconv.FormatInt(start, 10)+"-")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("download %s: %w", filepath.Base(destination), err)
	}
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
