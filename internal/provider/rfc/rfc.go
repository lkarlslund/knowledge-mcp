package rfc

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
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
	"unicode"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/analysis/analyzer/custom"
	"github.com/blevesearch/bleve/v2/analysis/analyzer/keyword"
	"github.com/blevesearch/bleve/v2/analysis/token/lowercase"
	unicodeTokenizer "github.com/blevesearch/bleve/v2/analysis/tokenizer/unicode"
	blevequery "github.com/blevesearch/bleve/v2/search/query"
	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/knowledgeindex"
	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/model"
	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/provider"
)

const (
	RFCProviderID  = "rfc"
	rfcCollection  = "rfc"
	rfcVariant     = "text"
	rfcIndexURL    = "https://www.rfc-editor.org/rfc-index.xml"
	rfcDocumentURL = "https://www.rfc-editor.org/rfc/rfc%s.txt"
)

var rfcReferenceRE = regexp.MustCompile(`(?i)\bRFC[ -]?(\d{1,5})\b`)

type RFC struct {
	baseURL string
	http    *http.Client
	mu      sync.Mutex
	cached  *rfcRelease
	cacheAt time.Time
}

type rfcRelease struct {
	Catalog []byte
	Entries []rfcEntry
}

type rfcEntry struct {
	ID      string   `json:"id"`
	Title   string   `json:"title"`
	Authors []string `json:"authors,omitempty"`
	Date    string   `json:"date,omitempty"`
	Status  string   `json:"status,omitempty"`
	Stream  string   `json:"stream,omitempty"`
}

type rfcXMLIndex struct {
	Entries []struct {
		DocID   string `xml:"doc-id"`
		Title   string `xml:"title"`
		Authors []struct {
			Name string `xml:"name"`
		} `xml:"author"`
		Date struct {
			Month string `xml:"month"`
			Year  string `xml:"year"`
		} `xml:"date"`
		Formats []string `xml:"format>file-format"`
		Status  string   `xml:"current-status"`
		Stream  string   `xml:"stream"`
	} `xml:"rfc-entry"`
}

func New() *RFC {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ResponseHeaderTimeout = 30 * time.Second
	return &RFC{baseURL: "https://www.rfc-editor.org", http: &http.Client{Transport: transport}}
}

func NewWithBaseURL(baseURL string) *RFC {
	provider := New()
	provider.baseURL = strings.TrimRight(baseURL, "/")
	return provider
}

func (*RFC) ID() string                  { return RFCProviderID }
func (*RFC) Owns(collection string) bool { return collection == rfcCollection }

func (p *RFC) Discover(ctx context.Context, filter string, refresh bool) ([]model.AvailableDataset, error) {
	haystack := "rfc request for comments internet standards ietf irtf iab reference technical specifications"
	if filter != "" && !strings.Contains(haystack, strings.ToLower(strings.TrimSpace(filter))) {
		return nil, nil
	}
	release, err := p.loadRelease(ctx, refresh)
	if err != nil {
		return nil, err
	}
	return []model.AvailableDataset{{
		Provider: p.ID(), Variant: rfcVariant, ID: rfcCollection, DisplayName: "RFC Series",
		Description: "Authoritative technical specifications, standards, protocols, operational guidance, and historical records published in the RFC Series by the RFC Editor.",
		Project:     "RFC Editor", ContentType: "Internet standards and technical references",
		Language:        model.Language{Code: "en", Name: "English", LocalName: "English", Direction: "ltr"},
		OnlineSourceURL: p.baseURL + "/", ReleaseDate: time.Now().UTC().Format("20060102"),
		Available: true, PartCount: len(release.Entries), Fingerprint: hashBytes(release.Catalog),
		Variants: []model.Variant{{ID: rfcVariant, Name: "Plain text", Description: "The complete RFC Series in canonical text form, converted to Markdown when read", Format: "text/plain"}},
	}}, nil
}

func (p *RFC) Latest(ctx context.Context, collection, variant string) (provider.Release, error) {
	if !p.Owns(collection) || variant != "" && variant != rfcVariant {
		return provider.Release{}, fmt.Errorf("unknown RFC dataset or variant %q/%q", collection, variant)
	}
	resolved, err := p.loadRelease(ctx, true)
	if err != nil {
		return provider.Release{}, err
	}
	date := time.Now().UTC().Format("20060102")
	return provider.Release{Fingerprint: hashBytes(resolved.Catalog), Date: date, Value: resolved}, nil
}

func (p *RFC) Acquire(ctx context.Context, collection, _ string, release provider.Release, stage, current string, progress provider.Progress) (model.Manifest, error) {
	resolved, ok := release.Value.(*rfcRelease)
	if !ok {
		return model.Manifest{}, errors.New("invalid RFC release metadata")
	}
	rawDir := filepath.Join(stage, "raw")
	if err := os.MkdirAll(rawDir, 0o755); err != nil {
		return model.Manifest{}, err
	}
	if err := os.WriteFile(filepath.Join(stage, "rfc-index.xml"), resolved.Catalog, 0o644); err != nil {
		return model.Manifest{}, err
	}
	metadata, err := json.MarshalIndent(resolved.Entries, "", "  ")
	if err != nil {
		return model.Manifest{}, err
	}
	if err := os.WriteFile(filepath.Join(stage, "documents.json"), metadata, 0o644); err != nil {
		return model.Manifest{}, err
	}

	type task struct{ entry rfcEntry }
	tasks := make(chan task)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	var progressMu sync.Mutex
	var completed int64
	workers := min(12, max(1, len(resolved.Entries)))
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range tasks {
				if ctx.Err() != nil {
					return
				}
				destination := filepath.Join(rawDir, task.entry.ID+".txt")
				if _, statErr := os.Stat(destination); errors.Is(statErr, os.ErrNotExist) && current != "" {
					sourcePath := filepath.Join(current, "raw", task.entry.ID+".txt")
					if linkErr := os.Link(sourcePath, destination); linkErr != nil && !errors.Is(linkErr, os.ErrNotExist) {
						_ = copyFile(sourcePath, destination)
					}
				}
				if _, statErr := os.Stat(destination); errors.Is(statErr, os.ErrNotExist) {
					if downloadErr := p.download(ctx, task.entry.ID, destination); downloadErr != nil {
						select {
						case errCh <- downloadErr:
						default:
						}
						return
					}
				}
				progressMu.Lock()
				completed++
				progress("downloading_documents", completed, int64(len(resolved.Entries)), "documents", 0, "downloading RFC documents")
				progressMu.Unlock()
			}
		}()
	}
sendLoop:
	for _, entry := range resolved.Entries {
		select {
		case tasks <- task{entry: entry}:
		case err = <-errCh:
			break sendLoop
		case <-ctx.Done():
			err = ctx.Err()
			break sendLoop
		}
	}
	close(tasks)
	wg.Wait()
	select {
	case workerErr := <-errCh:
		if err == nil {
			err = workerErr
		}
	default:
	}
	if err != nil {
		return model.Manifest{}, err
	}
	var bytes int64
	for _, entry := range resolved.Entries {
		if info, statErr := os.Stat(filepath.Join(rawDir, entry.ID+".txt")); statErr == nil {
			bytes += info.Size()
		}
	}
	return model.Manifest{
		Provider: p.ID(), Variant: rfcVariant, Dataset: collection, ReleaseDate: release.Date,
		Fingerprint: release.Fingerprint, PartCount: len(resolved.Entries), RawSize: bytes,
		PublishedAt: time.Now().UTC(), Site: model.DatasetMetadata{
			Name: "RFC Series", Description: "Authoritative technical specifications, standards, protocols, operational guidance, and historical records published in the RFC Series by the RFC Editor.", Project: "RFC Editor", ContentType: "Internet standards and technical references",
			Language:        model.Language{Code: "en", Name: "English", LocalName: "English", Direction: "ltr"},
			OnlineSourceURL: p.baseURL + "/", SourceDocuments: uint64(len(resolved.Entries)),
			MetadataUpdatedAt: time.Now().UTC(),
		},
	}, nil
}

func (p *RFC) BuildTitle(ctx context.Context, path string, _ model.Manifest, progress provider.TitleProgress) (uint64, error) {
	entries, err := readRFCEntries(path)
	if err != nil {
		return 0, err
	}
	destination := filepath.Join(path, knowledgeindex.TitleDirectory+".building")
	if err := os.RemoveAll(destination); err != nil {
		return 0, err
	}
	idx, err := newRFCIndex(destination, false)
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
	for i, entry := range entries {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		doc := map[string]any{"title": entry.Title, "title_exact": strings.ToLower(entry.Title), "status": entry.Status, "authors": strings.Join(entry.Authors, " "), "date": entry.Date}
		if err := batch.Index(entry.ID, doc); err != nil {
			return 0, err
		}
		if batch.Size() >= 1_000 {
			if err := idx.Batch(batch); err != nil {
				return 0, err
			}
			batch = idx.NewBatch()
		}
		progress(uint64(i+1), int64(i+1), int64(len(entries)))
	}
	if err := idx.Batch(batch); err != nil {
		return 0, err
	}
	if err := idx.Close(); err != nil {
		return 0, err
	}
	closed = true
	return uint64(len(entries)), nil
}

func (p *RFC) BuildBody(ctx context.Context, path string, _ model.Manifest, progress provider.BodyProgress) error {
	entries, err := readRFCEntries(path)
	if err != nil {
		return err
	}
	destination := filepath.Join(path, knowledgeindex.BodyDirectory+".building")
	if err := os.RemoveAll(destination); err != nil {
		return err
	}
	idx, err := newRFCIndex(destination, true)
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			_ = idx.Close()
		}
	}()
	batch := idx.NewBatch()
	batchBytes := 0
	for i, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		body, readErr := os.ReadFile(filepath.Join(path, "raw", entry.ID+".txt"))
		if readErr != nil {
			return readErr
		}
		doc := map[string]any{"title": entry.Title, "title_exact": strings.ToLower(entry.Title), "body": string(body), "status": entry.Status, "authors": strings.Join(entry.Authors, " "), "date": entry.Date}
		if err := batch.Index(entry.ID, doc); err != nil {
			return err
		}
		batchBytes += len(body) + len(entry.Title) + len(entry.Status) + len(entry.Date)
		if batch.Size() >= 256 || batchBytes >= 8<<20 {
			if err := idx.Batch(batch); err != nil {
				return err
			}
			batch = idx.NewBatch()
			batchBytes = 0
		}
		progress(int64(i+1), int64(len(entries)))
	}
	if err := idx.Batch(batch); err != nil {
		return err
	}
	if err := idx.Close(); err != nil {
		return err
	}
	closed = true
	return nil
}

func (*RFC) IndexCurrent(manifest model.Manifest) (bool, bool) {
	return manifest.TitleReady && manifest.TitleIndexVersion == knowledgeindex.TitleVersion,
		manifest.BodyReady && manifest.BodyIndexVersion == knowledgeindex.BodyVersion
}

func (p *RFC) Open(path string, fullText bool) (provider.Reader, error) {
	indexPath := filepath.Join(path, knowledgeindex.TitleDirectory)
	if fullText {
		indexPath = filepath.Join(path, knowledgeindex.BodyDirectory)
	}
	idx, err := bleve.OpenUsing(indexPath, map[string]any{"read_only": true})
	if err != nil {
		return nil, err
	}
	return &rfcReader{path: path, baseURL: p.baseURL, index: idx}, nil
}

func (*RFC) Backfill(_ context.Context, _ string, manifest *model.Manifest) bool {
	if manifest.Provider != "" {
		return false
	}
	return false
}

func (p *RFC) loadRelease(ctx context.Context, refresh bool) (*rfcRelease, error) {
	p.mu.Lock()
	if !refresh && p.cached != nil && time.Since(p.cacheAt) < 24*time.Hour {
		cached := p.cached
		p.mu.Unlock()
		return cached, nil
	}
	p.mu.Unlock()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.baseURL+"/rfc-index.xml", nil)
	if err != nil {
		return nil, err
	}
	resp, err := p.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch RFC index: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch RFC index: %s", resp.Status)
	}
	catalog, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	entries, err := parseRFCIndex(catalog)
	if err != nil {
		return nil, err
	}
	release := &rfcRelease{Catalog: catalog, Entries: entries}
	p.mu.Lock()
	p.cached, p.cacheAt = release, time.Now()
	p.mu.Unlock()
	return release, nil
}

func parseRFCIndex(data []byte) ([]rfcEntry, error) {
	var index rfcXMLIndex
	if err := xml.Unmarshal(data, &index); err != nil {
		return nil, fmt.Errorf("parse RFC index: %w", err)
	}
	entries := make([]rfcEntry, 0, len(index.Entries))
	for _, raw := range index.Entries {
		textAvailable := false
		for _, format := range raw.Formats {
			if strings.EqualFold(strings.TrimSpace(format), "TXT") {
				textAvailable = true
				break
			}
		}
		if !textAvailable {
			continue
		}
		id := strings.TrimLeft(strings.TrimPrefix(strings.ToUpper(strings.TrimSpace(raw.DocID)), "RFC"), "0")
		if id == "" {
			id = "0"
		}
		if _, err := strconv.ParseUint(id, 10, 32); err != nil {
			continue
		}
		authors := make([]string, 0, len(raw.Authors))
		for _, author := range raw.Authors {
			if name := strings.TrimSpace(author.Name); name != "" {
				authors = append(authors, name)
			}
		}
		entries = append(entries, rfcEntry{ID: id, Title: strings.TrimSpace(raw.Title), Authors: authors, Date: strings.TrimSpace(raw.Date.Month + " " + raw.Date.Year), Status: strings.TrimSpace(raw.Status), Stream: strings.TrimSpace(raw.Stream)})
	}
	sort.Slice(entries, func(i, j int) bool {
		a, _ := strconv.Atoi(entries[i].ID)
		b, _ := strconv.Atoi(entries[j].ID)
		return a < b
	})
	return entries, nil
}

func (p *RFC) download(ctx context.Context, id, destination string) error {
	partial := destination + ".partial"
	var offset int64
	if info, err := os.Stat(partial); err == nil {
		offset = info.Size()
	}
	requestURL := fmt.Sprintf(p.baseURL+"/rfc/rfc%s.txt", id)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return err
	}
	if offset > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
	}
	resp, err := p.http.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("RFC %s text is unavailable", id)
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("download RFC %s: %s", id, resp.Status)
	}
	flags := os.O_CREATE | os.O_WRONLY
	if offset > 0 && resp.StatusCode == http.StatusPartialContent {
		flags |= os.O_APPEND
	} else {
		flags |= os.O_TRUNC
	}
	file, err := os.OpenFile(partial, flags, 0o644)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(file, resp.Body)
	closeErr := file.Close()
	if err := errors.Join(copyErr, closeErr); err != nil {
		return err
	}
	return os.Rename(partial, destination)
}

type rfcReader struct {
	path, baseURL string
	index         bleve.Index
	mu            sync.RWMutex
	closed        bool
}

func (r *rfcReader) Retain() (func(), error) {
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return nil, errors.New("index reader is closed")
	}
	return r.mu.RUnlock, nil
}
func (r *rfcReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	return r.index.Close()
}

func (r *rfcReader) Search(ctx context.Context, query string, options model.SearchOptions, fullText bool) (model.SearchResult, error) {
	if strings.TrimSpace(query) == "" {
		return model.SearchResult{}, errors.New("query cannot be empty")
	}
	if options.Limit <= 0 || options.Limit > 50 {
		options.Limit = 10
	}
	if options.Offset < 0 {
		options.Offset = 0
	}
	queries := make([]blevequery.Query, 0, 3)
	title := bleve.NewMatchQuery(query)
	title.SetField("title")
	title.SetBoost(5)
	queries = append(queries, title)
	identifier := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(query)), "rfc")
	if _, err := strconv.Atoi(strings.TrimSpace(identifier)); err == nil {
		id := bleve.NewDocIDQuery([]string{strings.TrimSpace(identifier)})
		id.SetBoost(20)
		queries = append(queries, id)
	}
	if fullText {
		body := bleve.NewMatchQuery(query)
		body.SetField("body")
		queries = append(queries, body)
	}
	request := bleve.NewSearchRequestOptions(bleve.NewDisjunctionQuery(queries...), options.Limit, options.Offset, false)
	request.Fields = []string{"title", "status", "authors", "date"}
	response, err := r.index.SearchInContext(ctx, request)
	if err != nil {
		return model.SearchResult{}, err
	}
	result := model.SearchResult{Query: query, SearchMode: map[bool]string{false: "title", true: "full_text"}[fullText], Total: response.Total, Offset: options.Offset, SnippetsAvailable: fullText, SnippetsComplete: fullText}
	for _, hit := range response.Hits {
		titleValue, _ := hit.Fields["title"].(string)
		item := model.SearchHit{ID: hit.ID, Title: titleValue, URL: fmt.Sprintf(r.baseURL+"/info/rfc%s", hit.ID), Score: hit.Score, MatchMode: result.SearchMode}
		if fullText && options.Snippets {
			item.Snippet = r.snippet(hit.ID, query)
		}
		result.Hits = append(result.Hits, item)
	}
	if options.Offset+len(result.Hits) < int(result.Total) {
		result.NextOffset = options.Offset + len(result.Hits)
	}
	return result, nil
}

func (r *rfcReader) Read(ctx context.Context, title, id string, options model.ReadOptions) (model.Document, error) {
	if id == "" {
		result, err := r.Search(ctx, title, model.SearchOptions{Limit: 1}, false)
		if err != nil || len(result.Hits) == 0 {
			return model.Document{}, provider.ErrDocumentNotFound
		}
		id = result.Hits[0].ID
	}
	if _, err := strconv.ParseUint(id, 10, 32); err != nil {
		return model.Document{}, fmt.Errorf("invalid RFC document ID %q", id)
	}
	raw, err := os.ReadFile(filepath.Join(r.path, "raw", id+".txt"))
	if errors.Is(err, os.ErrNotExist) {
		return model.Document{}, provider.ErrDocumentNotFound
	}
	if err != nil {
		return model.Document{}, err
	}
	entry, _ := findRFCEntry(r.path, id)
	content := string(raw)
	format := options.Format
	if format == "" || format == "markdown" {
		format = "markdown"
		content = rfcMarkdown(id, entry, content)
	}
	if format == "source" {
		content = string(raw)
	}
	maxChars := options.MaxChars
	if maxChars <= 0 {
		maxChars = knowledgeindex.DefaultReadCharacters
	}
	start := min(max(options.Offset, 0), len(content))
	end := min(start+maxChars, len(content))
	page := model.Document{ID: id, Title: entry.Title, URL: fmt.Sprintf(r.baseURL+"/info/rfc%s", id), Format: format, Content: content[start:end], Offset: start, ReturnedChars: end - start, TotalChars: len(content), Truncated: end < len(content)}
	if page.Title == "" {
		page.Title = "RFC " + id
	}
	if page.Truncated {
		page.NextOffset = end
	}
	return page, nil
}

func (r *rfcReader) snippet(id, query string) string {
	data, err := os.ReadFile(filepath.Join(r.path, "raw", id+".txt"))
	if err != nil {
		return ""
	}
	text := strings.Join(strings.Fields(string(data)), " ")
	words := strings.Fields(strings.ToLower(query))
	position := -1
	for _, word := range words {
		if len(word) > 2 {
			position = strings.Index(strings.ToLower(text), word)
			if position >= 0 {
				break
			}
		}
	}
	if position < 0 {
		position = 0
	}
	start := max(0, position-120)
	end := min(len(text), position+280)
	return strings.TrimSpace(text[start:end])
}

func rfcMarkdown(id string, entry rfcEntry, raw string) string {
	raw = strings.ReplaceAll(raw, "\f", "\n")
	var output strings.Builder
	fmt.Fprintf(&output, "# RFC %s: %s\n\n", id, entry.Title)
	if entry.Status != "" {
		fmt.Fprintf(&output, "**Status:** %s  \n", entry.Status)
	}
	if entry.Date != "" {
		fmt.Fprintf(&output, "**Published:** %s  \n", entry.Date)
	}
	if len(entry.Authors) > 0 {
		fmt.Fprintf(&output, "**Authors:** %s  \n", strings.Join(entry.Authors, ", "))
	}
	output.WriteString("\n")
	scanner := bufio.NewScanner(strings.NewReader(raw))
	scanner.Buffer(make([]byte, 64<<10), 2<<20)
	for scanner.Scan() {
		line := strings.TrimRightFunc(scanner.Text(), unicode.IsSpace)
		line = rfcReferenceRE.ReplaceAllStringFunc(line, func(match string) string {
			parts := rfcReferenceRE.FindStringSubmatch(match)
			target := strings.TrimLeft(parts[1], "0")
			if target == "" {
				target = "0"
			}
			return "[" + match + "](knowledge-read://read?dataset=rfc&id=" + target + ` "Call knowledge_read with dataset=rfc and id=` + target + `")`
		})
		output.WriteString(line)
		output.WriteByte('\n')
	}
	return output.String()
}

func newRFCIndex(path string, body bool) (bleve.Index, error) {
	indexMapping := bleve.NewIndexMapping()
	if err := indexMapping.AddCustomAnalyzer("knowledge_unicode", map[string]any{"type": custom.Name, "tokenizer": unicodeTokenizer.Name, "token_filters": []string{lowercase.Name}}); err != nil {
		return nil, err
	}
	document := bleve.NewDocumentMapping()
	for _, name := range []string{"title", "status", "authors", "date"} {
		field := bleve.NewTextFieldMapping()
		field.Analyzer = "knowledge_unicode"
		field.Store = true
		field.IncludeTermVectors = false
		document.AddFieldMappingsAt(name, field)
	}
	exact := bleve.NewTextFieldMapping()
	exact.Analyzer = keyword.Name
	exact.Store = false
	exact.IncludeTermVectors = false
	document.AddFieldMappingsAt("title_exact", exact)
	if body {
		field := bleve.NewTextFieldMapping()
		field.Analyzer = "knowledge_unicode"
		field.Store = false
		field.IncludeTermVectors = false
		document.AddFieldMappingsAt("body", field)
	}
	indexMapping.DefaultMapping = document
	indexMapping.TypeField = "_type"
	indexMapping.DefaultType = "document"
	indexMapping.StoreDynamic = false
	indexMapping.IndexDynamic = false
	indexMapping.DocValuesDynamic = false
	return bleve.NewUsing(path, indexMapping, bleve.Config.DefaultIndexType, bleve.Config.DefaultKVStore, map[string]any{"create_if_missing": true})
}

func readRFCEntries(path string) ([]rfcEntry, error) {
	data, err := os.ReadFile(filepath.Join(path, "documents.json"))
	if err != nil {
		return nil, err
	}
	var entries []rfcEntry
	err = json.Unmarshal(data, &entries)
	return entries, err
}
func findRFCEntry(path, id string) (rfcEntry, error) {
	entries, err := readRFCEntries(path)
	if err != nil {
		return rfcEntry{}, err
	}
	for _, entry := range entries {
		if entry.ID == id {
			return entry, nil
		}
	}
	return rfcEntry{}, os.ErrNotExist
}
func hashBytes(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }
func copyFile(source, destination string) error {
	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer func() { _ = input.Close() }()
	output, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(output, input)
	return errors.Join(copyErr, output.Close())
}
