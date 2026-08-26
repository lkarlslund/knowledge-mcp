// Package kiwix implements discovery and native reading of Kiwix ZIM archives.
package kiwix

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/knowledgeindex"
	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/model"
	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/provider"
	"golang.org/x/text/language"
	"golang.org/x/text/language/display"
)

const (
	ProviderID       = "kiwix"
	catalogURL       = "https://opds.library.kiwix.org/catalog/v2/entries"
	defaultVariantID = "default"
)

type Kiwix struct {
	catalogURL string
	http       *http.Client
	mu         sync.Mutex
	cached     []catalogEntry
	cacheAt    time.Time
}

type catalogEntry struct {
	UUID, Title, Summary, Language, Name, Flavour, Category, Tags string
	Updated, Issued, Author, Publisher, BrowseURL, MetalinkURL    string
	ArticleCount, MediaCount                                      uint64
	Size                                                          int64
}

type opdsFeed struct {
	Total int         `xml:"totalResults"`
	Items []opdsEntry `xml:"entry"`
}

type opdsEntry struct {
	ID           string `xml:"id"`
	Title        string `xml:"title"`
	Updated      string `xml:"updated"`
	Summary      string `xml:"summary"`
	Language     string `xml:"language"`
	Name         string `xml:"name"`
	Flavour      string `xml:"flavour"`
	Category     string `xml:"category"`
	Tags         string `xml:"tags"`
	ArticleCount uint64 `xml:"articleCount"`
	MediaCount   uint64 `xml:"mediaCount"`
	Issued       string `xml:"issued"`
	Author       struct {
		Name string `xml:"name"`
	} `xml:"author"`
	Publisher struct {
		Name string `xml:"name"`
	} `xml:"publisher"`
	Links []struct {
		Rel    string `xml:"rel,attr"`
		Type   string `xml:"type,attr"`
		Href   string `xml:"href,attr"`
		Length int64  `xml:"length,attr"`
	} `xml:"link"`
}

type metalink struct {
	File struct {
		Name   string `xml:"name,attr"`
		Size   int64  `xml:"size"`
		Hashes []struct {
			Type  string `xml:"type,attr"`
			Value string `xml:",chardata"`
		} `xml:"hash"`
		URLs []struct {
			Priority int    `xml:"priority,attr"`
			Value    string `xml:",chardata"`
		} `xml:"url"`
	} `xml:"file"`
}

type resolvedRelease struct {
	Entry  catalogEntry
	URL    string
	SHA256 string
}

func New() *Kiwix {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ResponseHeaderTimeout = 30 * time.Second
	return &Kiwix{catalogURL: catalogURL, http: &http.Client{Transport: transport}}
}

func NewWithCatalogURL(value string) *Kiwix {
	provider := New()
	provider.catalogURL = value
	return provider
}

func (*Kiwix) ID() string                  { return ProviderID }
func (*Kiwix) Owns(collection string) bool { return strings.HasPrefix(collection, ProviderID+"-") }

func (p *Kiwix) Discover(ctx context.Context, filter string, refresh bool) ([]model.AvailableDataset, error) {
	entries, err := p.loadCatalog(ctx, refresh)
	if err != nil {
		return nil, err
	}
	filter = strings.ToLower(strings.TrimSpace(filter))
	groups := make(map[string][]catalogEntry)
	for _, entry := range entries {
		haystack := strings.ToLower(strings.Join([]string{entry.Name, entry.Title, entry.Summary, entry.Category, entry.Tags, entry.Language, entry.Author}, " "))
		if filter != "" && !strings.Contains(haystack, filter) {
			continue
		}
		id := collectionID(entry.Name)
		groups[id] = append(groups[id], entry)
	}
	result := make([]model.AvailableDataset, 0, len(groups))
	for id, variants := range groups {
		sort.Slice(variants, func(i, j int) bool { return variantID(variants[i]) < variantID(variants[j]) })
		primary := variants[0]
		languages := languageMetadataList(primary.Language)
		item := model.AvailableDataset{Provider: ProviderID, ID: id, DisplayName: primary.Title, Description: firstNonempty(primary.Summary, primary.Title+" offline archive from Kiwix"), Project: firstNonempty(primary.Author, primary.Publisher, "Kiwix"), ContentType: firstNonempty(primary.Category, "Offline web archive"), Profile: kiwixProfile(primary, languages), Language: languageSummary(languages), Languages: languages, OnlineSourceURL: firstNonempty(primary.BrowseURL, "https://library.kiwix.org"), ReleaseDate: releaseDate(primary), Available: true, RawSize: primary.Size, PartCount: 1, Fingerprint: entryFingerprint(primary), Variant: variantID(primary)}
		for _, entry := range variants {
			item.Variants = append(item.Variants, model.Variant{ID: variantID(entry), Name: variantName(entry), Description: variantDescription(entry), Format: "application/x-zim"})
		}
		result = append(result, item)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].DisplayName != result[j].DisplayName {
			return result[i].DisplayName < result[j].DisplayName
		}
		return result[i].ID < result[j].ID
	})
	return result, nil
}

func (p *Kiwix) Latest(ctx context.Context, collection, variant string) (provider.Release, error) {
	entries, err := p.loadCatalog(ctx, true)
	if err != nil {
		return provider.Release{}, err
	}
	var found *catalogEntry
	for index := range entries {
		if collectionID(entries[index].Name) != collection {
			continue
		}
		if variant == "" || variantID(entries[index]) == variant {
			copy := entries[index]
			found = &copy
			if variant != "" {
				break
			}
		}
	}
	if found == nil {
		return provider.Release{}, fmt.Errorf("unknown Kiwix dataset or variant %q/%q", collection, variant)
	}
	resolved, err := p.resolveMetalink(ctx, *found)
	if err != nil {
		return provider.Release{}, err
	}
	fingerprint := entryFingerprint(*found) + ":" + resolved.SHA256
	return provider.Release{Fingerprint: fingerprint, Date: releaseDate(*found), Value: resolved}, nil
}

func (p *Kiwix) Acquire(ctx context.Context, collection, variant string, release provider.Release, stage, _ string, progress provider.Progress) (model.Manifest, error) {
	resolved, ok := release.Value.(resolvedRelease)
	if !ok {
		return model.Manifest{}, errors.New("invalid Kiwix release metadata")
	}
	if err := os.MkdirAll(filepath.Join(stage, "raw"), 0o755); err != nil {
		return model.Manifest{}, err
	}
	destination := filepath.Join(stage, "raw", "archive.zim")
	if err := p.download(ctx, resolved, destination, progress); err != nil {
		return model.Manifest{}, err
	}
	archive, err := openZIM(destination)
	if err != nil {
		return model.Manifest{}, fmt.Errorf("verify ZIM archive: %w", err)
	}
	documents := archive.header.EntryCount
	if err := archive.Close(); err != nil {
		return model.Manifest{}, err
	}
	metadata, err := json.MarshalIndent(resolved.Entry, "", "  ")
	if err != nil {
		return model.Manifest{}, err
	}
	if err := os.WriteFile(filepath.Join(stage, "catalog-entry.json"), append(metadata, '\n'), 0o644); err != nil {
		return model.Manifest{}, err
	}
	languages := languageMetadataList(resolved.Entry.Language)
	return model.Manifest{Provider: ProviderID, Variant: firstNonempty(variant, variantID(resolved.Entry)), Dataset: collection, ReleaseDate: release.Date, Fingerprint: release.Fingerprint, PartCount: 1, RawSize: resolved.Entry.Size, PublishedAt: time.Now().UTC(), Site: model.DatasetMetadata{Name: resolved.Entry.Title, Description: resolved.Entry.Summary, Project: firstNonempty(resolved.Entry.Author, resolved.Entry.Publisher, "Kiwix"), ContentType: firstNonempty(resolved.Entry.Category, "Offline web archive"), Profile: kiwixProfile(resolved.Entry, languages), Language: languageSummary(languages), OnlineSourceURL: firstNonempty(resolved.Entry.BrowseURL, "https://library.kiwix.org"), SourceDocuments: uint64(documents), MetadataUpdatedAt: time.Now().UTC()}}, nil
}

func (p *Kiwix) OpenCorpus(path string, manifest model.Manifest) (provider.Corpus, error) {
	archive, err := openZIM(filepath.Join(path, "raw", "archive.zim"))
	if err != nil {
		return nil, err
	}
	return &zimCorpus{archive: archive, dataset: manifest.Dataset, sourceURL: manifest.Site.OnlineSourceURL}, nil
}

func (*Kiwix) Backfill(context.Context, string, *model.Manifest) bool { return false }

type zimCorpus struct {
	archive            *zimArchive
	dataset, sourceURL string
}

func (c *zimCorpus) Close() error { return c.archive.Close() }
func (c *zimCorpus) ScanTitles(ctx context.Context, after string, _ provider.ScanOptions, sink provider.RecordSink) error {
	return c.scan(ctx, after, false, sink)
}
func (c *zimCorpus) ScanBodies(ctx context.Context, after string, _ provider.ScanOptions, sink provider.RecordSink) error {
	return c.scan(ctx, after, true, sink)
}

func (c *zimCorpus) scan(ctx context.Context, after string, body bool, sink provider.RecordSink) error {
	start, err := parseCursor(after, c.archive.header.EntryCount)
	if err != nil {
		return err
	}
	for index := start; index < c.archive.header.EntryCount; index++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		entry, err := c.archive.entry(index)
		if err != nil {
			return fmt.Errorf("read ZIM entry %d: %w", index, err)
		}
		if !readableEntry(c.archive, entry) {
			continue
		}
		record := recordForEntry(c, entry)
		if body && !entry.isRedirect() {
			raw, err := c.archive.blob(entry)
			if err != nil {
				return fmt.Errorf("read ZIM entry %d body: %w", index, err)
			}
			if strings.Contains(c.archive.mime(entry), "html") {
				record.Body = htmlMarkdown(raw, c.dataset, entry.Namespace, entry.Path)
			} else {
				record.Body = string(raw)
			}
		}
		position := provider.ScanPosition{Cursor: strconv.FormatUint(uint64(index+1), 10), Completed: int64(index + 1), Total: int64(c.archive.header.EntryCount), Units: "entries", Boundary: true}
		if err := sink(record, position); err != nil {
			return err
		}
	}
	return nil
}

func (c *zimCorpus) Read(_ context.Context, record provider.Record, options model.ReadOptions) (model.Document, error) {
	index, err := strconv.ParseUint(record.Locator, 10, 32)
	if err != nil {
		return model.Document{}, provider.ErrDocumentNotFound
	}
	entry, err := c.archive.entry(uint32(index))
	if err != nil {
		return model.Document{}, err
	}
	redirected := false
	chain := make([]model.RedirectHop, 0, 2)
	for entry.isRedirect() {
		if len(chain) >= 16 {
			return model.Document{}, errors.New("ZIM redirect chain is too long")
		}
		target, err := c.archive.entry(entry.Redirect)
		if err != nil {
			return model.Document{}, err
		}
		chain = append(chain, model.RedirectHop{FromTitle: entry.Title, FromID: zimDocumentID(entry.Namespace, entry.Path), ToTitle: target.Title})
		entry, redirected = target, true
	}
	if !readableEntry(c.archive, entry) {
		return model.Document{}, provider.ErrDocumentNotFound
	}
	raw, err := c.archive.blob(entry)
	if err != nil {
		return model.Document{}, err
	}
	format, content := options.Format, string(raw)
	switch format {
	case "", "markdown":
		format = "markdown"
		if strings.Contains(c.archive.mime(entry), "html") {
			content = htmlMarkdown(raw, c.dataset, entry.Namespace, entry.Path)
		}
	case "text":
		if strings.Contains(c.archive.mime(entry), "html") {
			content = nodeTextFromHTML(raw)
		}
	case "source":
	default:
		return model.Document{}, errors.New("format must be markdown, text, or source")
	}
	maximum := options.MaxChars
	if maximum <= 0 {
		maximum = knowledgeindex.DefaultReadCharacters
	}
	start := min(max(options.Offset, 0), len(content))
	end := min(start+maximum, len(content))
	document := model.Document{ID: zimDocumentID(entry.Namespace, entry.Path), Title: entry.Title, URL: record.URL, RequestedTitle: record.Title, Redirected: redirected, RedirectChain: chain, Format: format, Content: content[start:end], Offset: start, ReturnedChars: end - start, TotalChars: len(content), Truncated: end < len(content)}
	if document.Truncated {
		document.NextOffset = end
	}
	return document, nil
}

func recordForEntry(c *zimCorpus, entry zimEntry) provider.Record {
	return provider.Record{ID: zimDocumentID(entry.Namespace, entry.Path), Title: entry.Title, URL: strings.TrimRight(c.sourceURL, "/") + "/" + url.PathEscape(entry.Path), Locator: strconv.FormatUint(uint64(entry.Index), 10), Primary: !entry.isRedirect(), Metadata: map[string]string{"mime": c.archive.mime(entry), "path": entry.Path}}
}

func readableEntry(archive *zimArchive, entry zimEntry) bool {
	if entry.Namespace != 'C' && entry.Namespace != 'A' {
		return false
	}
	if entry.isRedirect() {
		return true
	}
	mime := strings.ToLower(archive.mime(entry))
	return strings.HasPrefix(mime, "text/") || mime == "application/xhtml+xml"
}

func (p *Kiwix) loadCatalog(ctx context.Context, refresh bool) ([]catalogEntry, error) {
	p.mu.Lock()
	if p.cached != nil && (!refresh || time.Since(p.cacheAt) < 15*time.Minute) && time.Since(p.cacheAt) < 24*time.Hour {
		result := append([]catalogEntry(nil), p.cached...)
		p.mu.Unlock()
		return result, nil
	}
	p.mu.Unlock()
	const pageSize = 500
	var entries []catalogEntry
	for start := 0; ; start += pageSize {
		requestURL := fmt.Sprintf("%s?count=%d&start=%d", p.catalogURL, pageSize, start)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
		if err != nil {
			return nil, err
		}
		resp, err := p.http.Do(req)
		if err != nil {
			return nil, fmt.Errorf("fetch Kiwix catalog: %w", err)
		}
		if resp.StatusCode != http.StatusOK {
			_ = resp.Body.Close()
			return nil, fmt.Errorf("fetch Kiwix catalog: %s", resp.Status)
		}
		var feed opdsFeed
		decodeErr := xml.NewDecoder(resp.Body).Decode(&feed)
		closeErr := resp.Body.Close()
		if err := errors.Join(decodeErr, closeErr); err != nil {
			return nil, fmt.Errorf("parse Kiwix catalog: %w", err)
		}
		for _, raw := range feed.Items {
			entries = append(entries, catalogFromOPDS(raw))
		}
		if len(feed.Items) == 0 || len(entries) >= feed.Total {
			break
		}
	}
	p.mu.Lock()
	p.cached, p.cacheAt = append([]catalogEntry(nil), entries...), time.Now()
	p.mu.Unlock()
	return entries, nil
}

func catalogFromOPDS(raw opdsEntry) catalogEntry {
	entry := catalogEntry{UUID: strings.TrimPrefix(raw.ID, "urn:uuid:"), Title: raw.Title, Updated: raw.Updated, Summary: raw.Summary, Language: raw.Language, Name: raw.Name, Flavour: raw.Flavour, Category: raw.Category, Tags: raw.Tags, ArticleCount: raw.ArticleCount, MediaCount: raw.MediaCount, Issued: raw.Issued, Author: raw.Author.Name, Publisher: raw.Publisher.Name}
	for _, link := range raw.Links {
		if link.Type == "text/html" {
			entry.BrowseURL = link.Href
		}
		if strings.Contains(link.Rel, "acquisition") && link.Type == "application/x-zim" {
			entry.MetalinkURL, entry.Size = link.Href, link.Length
		}
	}
	return entry
}

func (p *Kiwix) resolveMetalink(ctx context.Context, entry catalogEntry) (resolvedRelease, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, entry.MetalinkURL, nil)
	if err != nil {
		return resolvedRelease{}, err
	}
	resp, err := p.http.Do(req)
	if err != nil {
		return resolvedRelease{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return resolvedRelease{}, fmt.Errorf("fetch Kiwix metalink: %s", resp.Status)
	}
	var document metalink
	if err := xml.NewDecoder(resp.Body).Decode(&document); err != nil {
		return resolvedRelease{}, err
	}
	resolved := resolvedRelease{Entry: entry}
	resolved.Entry.Size = document.File.Size
	for _, hash := range document.File.Hashes {
		if strings.EqualFold(hash.Type, "sha-256") {
			resolved.SHA256 = strings.TrimSpace(hash.Value)
		}
	}
	sort.Slice(document.File.URLs, func(i, j int) bool { return document.File.URLs[i].Priority < document.File.URLs[j].Priority })
	for _, candidate := range document.File.URLs {
		if strings.HasPrefix(strings.TrimSpace(candidate.Value), "https://") {
			resolved.URL = strings.TrimSpace(candidate.Value)
			break
		}
	}
	if resolved.URL == "" || resolved.SHA256 == "" || resolved.Entry.Size <= 0 {
		return resolvedRelease{}, errors.New("Kiwix metalink lacks URL, SHA-256, or size")
	}
	return resolved, nil
}

func (p *Kiwix) download(ctx context.Context, release resolvedRelease, destination string, progress provider.Progress) error {
	if info, err := os.Stat(destination); err == nil && info.Size() == release.Entry.Size {
		hash, hashErr := hashFile(destination)
		if hashErr == nil && strings.EqualFold(hash, release.SHA256) {
			progress("verifying_archive", info.Size(), info.Size(), "bytes", 0, "Kiwix ZIM archive already downloaded and verified")
			return nil
		}
	}
	partial := destination + ".partial"
	var offset int64
	if info, err := os.Stat(partial); err == nil {
		offset = info.Size()
	}
	if offset > release.Entry.Size {
		if err := os.Truncate(partial, 0); err != nil {
			return err
		}
		offset = 0
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, release.URL, nil)
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
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("download Kiwix archive: %s", resp.Status)
	}
	flags := os.O_CREATE | os.O_WRONLY
	if offset > 0 && resp.StatusCode == http.StatusPartialContent {
		flags |= os.O_APPEND
	} else {
		flags |= os.O_TRUNC
		offset = 0
	}
	file, err := os.OpenFile(partial, flags, 0o644)
	if err != nil {
		return err
	}
	buffer := make([]byte, 1<<20)
	completed := offset
	markBytes, markTime := completed, time.Now()
	for {
		n, readErr := resp.Body.Read(buffer)
		if n > 0 {
			if _, err := file.Write(buffer[:n]); err != nil {
				_ = file.Close()
				return err
			}
			completed += int64(n)
			now := time.Now()
			if now.Sub(markTime) >= 500*time.Millisecond {
				progress("downloading_archive", completed, release.Entry.Size, "bytes", float64(completed-markBytes)/now.Sub(markTime).Seconds(), "downloading Kiwix ZIM archive")
				markBytes, markTime = completed, now
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			_ = file.Close()
			return readErr
		}
	}
	if err := file.Close(); err != nil {
		return err
	}
	progress("verifying_archive", completed, release.Entry.Size, "bytes", 0, "verifying Kiwix ZIM archive")
	if completed != release.Entry.Size {
		return fmt.Errorf("downloaded %d bytes, expected %d", completed, release.Entry.Size)
	}
	hash, err := hashFile(partial)
	if err != nil {
		return err
	}
	if !strings.EqualFold(hash, release.SHA256) {
		return errors.New("Kiwix archive SHA-256 mismatch")
	}
	return os.Rename(partial, destination)
}

func hashFile(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = file.Close() }()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
func collectionID(name string) string { return ProviderID + "-" + stableSlug(name) }
func variantID(entry catalogEntry) string {
	return firstNonempty(stableSlug(entry.Flavour), defaultVariantID)
}
func variantName(entry catalogEntry) string {
	if entry.Flavour == "" {
		return "Default"
	}
	return strings.ToUpper(entry.Flavour[:1]) + entry.Flavour[1:]
}
func variantDescription(entry catalogEntry) string {
	return fmt.Sprintf("%s; %s; %d articles; %s", firstNonempty(entry.Summary, entry.Title), entry.Tags, entry.ArticleCount, humanBytes(entry.Size))
}
func entryFingerprint(entry catalogEntry) string {
	digest := sha256.Sum256([]byte(strings.Join([]string{entry.UUID, entry.Updated, strconv.FormatInt(entry.Size, 10), entry.MetalinkURL}, "\x00")))
	return hex.EncodeToString(digest[:])
}
func releaseDate(entry catalogEntry) string {
	value := firstNonempty(entry.Issued, entry.Updated)
	if len(value) >= 10 {
		return strings.ReplaceAll(value[:10], "-", "")
	}
	return value
}
func stableSlug(value string) string {
	var output strings.Builder
	dash := false
	for _, r := range strings.ToLower(value) {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
			output.WriteRune(r)
			dash = false
		} else if !dash && output.Len() > 0 {
			output.WriteByte('-')
			dash = true
		}
	}
	result := strings.Trim(output.String(), "-")
	if len(result) > 120 {
		result = strings.TrimRight(result[:120], "-")
	}
	return result
}
func firstNonempty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
func languageMetadataList(value string) []model.Language {
	seen := make(map[string]struct{})
	result := make([]model.Language, 0, strings.Count(value, ",")+1)
	for _, raw := range strings.Split(value, ",") {
		code := strings.ToLower(strings.TrimSpace(raw))
		if code == "" {
			continue
		}
		name := code
		if base, err := language.ParseBase(code); err == nil {
			code = base.String()
			name = display.English.Languages().Name(base)
			if name == "" {
				name = code
			}
		}
		if _, exists := seen[code]; exists {
			continue
		}
		seen[code] = struct{}{}
		result = append(result, model.Language{Code: code, Name: name, LocalName: name})
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Name != result[j].Name {
			return result[i].Name < result[j].Name
		}
		return result[i].Code < result[j].Code
	})
	return result
}

func languageSummary(languages []model.Language) model.Language {
	if len(languages) == 0 {
		return model.Language{}
	}
	if len(languages) == 1 {
		return languages[0]
	}
	return model.Language{Code: "mul", Name: fmt.Sprintf("Multilingual (%d languages)", len(languages)), LocalName: "Multilingual"}
}

func kiwixProfile(entry catalogEntry, languages []model.Language) model.DatasetProfile {
	seen := make(map[string]struct{})
	var topics []string
	for _, value := range append([]string{entry.Category, entry.Author}, strings.Split(entry.Tags, ";")...) {
		value = strings.TrimSpace(value)
		if value == "" || strings.HasPrefix(value, "_") {
			continue
		}
		key := strings.ToLower(value)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		topics = append(topics, value)
	}
	timeCoverage := "Source-defined coverage; archive snapshot " + releaseDate(entry)
	audience := "multilingual audience"
	if len(languages) == 1 {
		audience = languages[0].Name + "-language audience"
	}
	return model.DatasetProfile{Topics: topics, GeographicScope: []string{"source-defined", audience}, TimeCoverage: timeCoverage, DocumentTypes: []string{"offline web pages", "articles"}, UpdateCadence: "Varies by Kiwix content producer", CoverageNotes: "Scope and completeness follow the selected Kiwix archive and flavour.", SourceFeatures: []string{"HTML to Markdown conversion", "tables", "internal links", "redirect resolution", "offline source assets"}}
}
func humanBytes(value int64) string {
	const unit = 1024
	if value < unit {
		return fmt.Sprintf("%d B", value)
	}
	div, exp := int64(unit), 0
	for n := value / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(value)/float64(div), "KMGTPE"[exp])
}
