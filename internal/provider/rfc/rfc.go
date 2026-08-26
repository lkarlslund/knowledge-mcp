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
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/lkarlslund/knowledge-mcp/internal/knowledgeindex"
	"github.com/lkarlslund/knowledge-mcp/internal/model"
	"github.com/lkarlslund/knowledge-mcp/internal/provider"
)

const (
	RFCProviderID  = "rfc"
	rfcCollection  = "rfc"
	rfcVariant     = "text"
	rfcIndexURL    = "https://www.rfc-editor.org/rfc-index.xml"
	rfcDocumentURL = "https://www.rfc-editor.org/rfc/rfc%s.txt"
)

var (
	rfcReferenceRE          = regexp.MustCompile(`(?i)\bRFC[ -]?(\d{1,5})\b`)
	rfcNumberedHeadingRE    = regexp.MustCompile(`^(\d+(?:\.\d+)*\.)\s+(\S.*)$`)
	rfcAppendixHeadingRE    = regexp.MustCompile(`^Appendix\s+([A-Z])\.?\s+(\S.*)$`)
	rfcAppendixSubheadingRE = regexp.MustCompile(`^([A-Z](?:\.\d+)+\.)\s+(\S.*)$`)
)

var rfcNamedHeadings = map[string]struct{}{
	"Abstract": {}, "Status of This Memo": {}, "Copyright Notice": {}, "Table of Contents": {},
	"Acknowledgements": {}, "Acknowledgments": {}, "Contributors": {}, "References": {},
	"Security Considerations": {}, "IANA Considerations": {}, "Index": {}, "Authors' Addresses": {},
}

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
	ID          string   `json:"id"`
	Title       string   `json:"title"`
	Authors     []string `json:"authors,omitempty"`
	Date        string   `json:"date,omitempty"`
	Status      string   `json:"status,omitempty"`
	Stream      string   `json:"stream,omitempty"`
	Keywords    []string `json:"keywords,omitempty"`
	Obsoletes   []string `json:"obsoletes,omitempty"`
	ObsoletedBy []string `json:"obsoleted_by,omitempty"`
	Updates     []string `json:"updates,omitempty"`
	UpdatedBy   []string `json:"updated_by,omitempty"`
	Also        []string `json:"also,omitempty"`
	SeeAlso     []string `json:"see_also,omitempty"`
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
		Formats     []string `xml:"format>file-format"`
		Status      string   `xml:"current-status"`
		Stream      string   `xml:"stream"`
		Keywords    []string `xml:"keywords>kw"`
		Obsoletes   []string `xml:"obsoletes>doc-id"`
		ObsoletedBy []string `xml:"obsoleted-by>doc-id"`
		Updates     []string `xml:"updates>doc-id"`
		UpdatedBy   []string `xml:"updated-by>doc-id"`
		Also        []string `xml:"is-also>doc-id"`
		SeeAlso     []string `xml:"see-also>doc-id"`
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

func (p *RFC) OpenCorpus(path string, _ model.Manifest) (provider.Corpus, error) {
	entries, err := readRFCEntries(path)
	if err != nil {
		return nil, err
	}
	return &rfcCorpus{path: path, baseURL: p.baseURL, entries: entries}, nil
}

type rfcCorpus struct {
	path, baseURL string
	entries       []rfcEntry
}

func (c *rfcCorpus) ScanTitles(ctx context.Context, after string, _ provider.ScanOptions, sink provider.RecordSink) error {
	return c.scan(ctx, after, false, sink)
}

func (c *rfcCorpus) ScanBodies(ctx context.Context, after string, _ provider.ScanOptions, sink provider.RecordSink) error {
	return c.scan(ctx, after, true, sink)
}

func (c *rfcCorpus) scan(ctx context.Context, after string, body bool, sink provider.RecordSink) error {
	start := 0
	if after != "" {
		parsed, err := strconv.Atoi(after)
		if err != nil || parsed < 0 || parsed > len(c.entries) {
			return fmt.Errorf("invalid RFC scan cursor %q", after)
		}
		start = parsed
	}
	for index := start; index < len(c.entries); index++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		entry := c.entries[index]
		rankWeight := 1.0
		if len(entry.ObsoletedBy) > 0 {
			rankWeight = 0.65
		} else if strings.EqualFold(entry.Status, "HISTORIC") {
			rankWeight = 0.8
		}
		record := provider.Record{ID: entry.ID, Title: entry.Title, URL: fmt.Sprintf(c.baseURL+"/info/rfc%s", entry.ID), Locator: entry.ID, Primary: true, Identifiers: append([]string{"RFC " + entry.ID}, entry.Also...), Aliases: []string{"RFC " + entry.ID, "RFC" + entry.ID}, Keywords: append(append([]string(nil), entry.Keywords...), entry.Stream, entry.Status), Status: rfcLifecycleStatus(entry), RankWeight: rankWeight, Metadata: map[string]string{"status": entry.Status, "authors": strings.Join(entry.Authors, " "), "date": entry.Date, "stream": entry.Stream}}
		record.Temporal.PublishedAt, record.Temporal.PublishedPrecision = rfcPublishedDate(entry.Date)
		if body {
			data, err := os.ReadFile(filepath.Join(c.path, "raw", entry.ID+".txt"))
			if err != nil {
				return err
			}
			record.Body = string(data)
		}
		if err := sink(record, provider.ScanPosition{Cursor: strconv.Itoa(index + 1), Completed: int64(index + 1), Total: int64(len(c.entries)), Units: "documents", Boundary: true}); err != nil {
			return err
		}
	}
	return nil
}

func (c *rfcCorpus) Read(_ context.Context, record provider.Record, options model.ReadOptions) (model.Document, error) {
	raw, err := os.ReadFile(filepath.Join(c.path, "raw", record.ID+".txt"))
	if errors.Is(err, os.ErrNotExist) {
		return model.Document{}, provider.ErrDocumentNotFound
	}
	if err != nil {
		return model.Document{}, err
	}
	entry, _ := findRFCEntry(c.path, record.ID)
	content := string(raw)
	format := options.Format
	switch format {
	case "", "markdown":
		format = "markdown"
		content = rfcMarkdown(record.ID, entry, content)
	case "source":
		content = string(raw)
	}
	outline := rfcSectionOutline(content)
	section := strings.TrimSpace(options.Section)
	var sectionFound *bool
	if section != "" && format == "markdown" {
		var found bool
		content, found = rfcExtractSection(content, section)
		sectionFound = &found
	}
	maximum := options.MaxChars
	if maximum <= 0 {
		maximum = knowledgeindex.DefaultReadCharacters
	}
	runes := []rune(content)
	start := min(max(options.Offset, 0), len(runes))
	end := min(start+maximum, len(runes))
	if options.AlignBoundaries && end < len(runes) {
		end = rfcReadableChunkEnd(runes, start, end)
	}
	document := model.Document{TemporalMetadata: record.Temporal, ID: record.ID, Title: entry.Title, URL: record.URL, Section: section, SectionFound: sectionFound, Format: format, Content: string(runes[start:end]), Offset: start, ReturnedChars: end - start, TotalChars: len(runes), Truncated: end < len(runes)}
	if options.IncludeOutline || sectionFound != nil && !*sectionFound {
		document.Sections = outline
		if len(document.Sections) > 200 {
			document.Sections = document.Sections[:200]
			document.OutlineTruncated = true
		}
	}
	document.Relationships = rfcRelationships(entry, c.baseURL)
	if document.Title == "" {
		document.Title = record.Title
	}
	if document.Truncated {
		document.NextOffset = end
	}
	return document, nil
}

func rfcReadableChunkEnd(content []rune, start, hardEnd int) int {
	minimum := start + (hardEnd-start)/2
	for index := hardEnd; index > minimum; index-- {
		if content[index-1] == '\n' {
			return index
		}
	}
	return hardEnd
}

func (*rfcCorpus) Close() error { return nil }

func rfcPublishedDate(value string) (*time.Time, string) {
	for _, candidate := range []struct {
		layout, precision string
	}{{"January 2006", "month"}, {"Jan 2006", "month"}, {"2006", "year"}} {
		parsed, err := time.Parse(candidate.layout, strings.TrimSpace(value))
		if err == nil {
			parsed = parsed.UTC()
			return &parsed, candidate.precision
		}
	}
	return nil, ""
}

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
		Profile:         rfcProfile(),
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
			Profile:         rfcProfile(),
			Language:        model.Language{Code: "en", Name: "English", LocalName: "English", Direction: "ltr"},
			OnlineSourceURL: p.baseURL + "/", SourceDocuments: uint64(len(resolved.Entries)),
			MetadataUpdatedAt: time.Now().UTC(),
		},
	}, nil
}

func rfcProfile() model.DatasetProfile {
	return model.DatasetProfile{Topics: []string{"Internet protocols", "networking", "security", "standards", "operations"}, GeographicScope: []string{"global Internet"}, TimeCoverage: "1969 to present", DocumentTypes: []string{"standards", "protocol specifications", "best current practices", "informational RFCs", "experimental RFCs", "historic RFCs"}, UpdateCadence: "Continuous as RFCs are published", CoverageNotes: "Complete RFC Series entries with canonical text where the RFC Editor supplies it.", SourceFeatures: []string{"canonical text", "RFC cross-references", "status metadata", "author metadata"}}
}

func (*RFC) Backfill(_ context.Context, path string, manifest *model.Manifest) bool {
	changed := false
	if manifest.Provider == "" && manifest.Dataset == rfcCollection {
		manifest.Provider, manifest.Variant, changed = RFCProviderID, rfcVariant, true
	}
	if manifest.Provider == RFCProviderID && len(manifest.Site.Profile.Topics) == 0 {
		manifest.Site.Profile, changed = rfcProfile(), true
	}
	if manifest.Provider == RFCProviderID {
		if catalog, err := os.ReadFile(filepath.Join(path, "rfc-index.xml")); err == nil {
			if entries, parseErr := parseRFCIndex(catalog); parseErr == nil {
				if metadata, marshalErr := json.MarshalIndent(entries, "", "  "); marshalErr == nil {
					metadata = append(metadata, '\n')
					existing, _ := os.ReadFile(filepath.Join(path, "documents.json"))
					if string(existing) != string(metadata) {
						writeErr := os.WriteFile(filepath.Join(path, "documents.json"), metadata, 0o644)
						if writeErr == nil {
							changed = true
						}
					}
				}
			}
		}
	}
	return changed
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
		entries = append(entries, rfcEntry{ID: id, Title: strings.TrimSpace(raw.Title), Authors: authors, Date: strings.TrimSpace(raw.Date.Month + " " + raw.Date.Year), Status: strings.TrimSpace(raw.Status), Stream: strings.TrimSpace(raw.Stream), Keywords: cleanValues(raw.Keywords), Obsoletes: normalizeDocumentRefs(raw.Obsoletes), ObsoletedBy: normalizeDocumentRefs(raw.ObsoletedBy), Updates: normalizeDocumentRefs(raw.Updates), UpdatedBy: normalizeDocumentRefs(raw.UpdatedBy), Also: normalizeDocumentRefs(raw.Also), SeeAlso: normalizeDocumentRefs(raw.SeeAlso)})
	}
	sort.Slice(entries, func(i, j int) bool {
		a, _ := strconv.Atoi(entries[i].ID)
		b, _ := strconv.Atoi(entries[j].ID)
		return a < b
	})
	return entries, nil
}

func cleanValues(values []string) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			result = append(result, value)
		}
	}
	return result
}

func normalizeDocumentRefs(values []string) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.ToUpper(strings.TrimSpace(value))
		for _, prefix := range []string{"RFC", "STD", "BCP", "FYI"} {
			if strings.HasPrefix(value, prefix) {
				number := strings.TrimLeft(strings.TrimSpace(strings.TrimPrefix(value, prefix)), "0")
				if number == "" {
					number = "0"
				}
				value = prefix + " " + number
				break
			}
		}
		if value != "" {
			result = append(result, value)
		}
	}
	return result
}

func rfcLifecycleStatus(entry rfcEntry) string {
	if len(entry.ObsoletedBy) > 0 {
		return "obsoleted"
	}
	if entry.Status != "" {
		return strings.ToLower(entry.Status)
	}
	return "current"
}

func rfcRelationships(entry rfcEntry, baseURL string) []model.DocumentRelationship {
	types := []struct {
		name   string
		values []string
	}{{"Obsoletes", entry.Obsoletes}, {"Obsoleted by", entry.ObsoletedBy}, {"Updates", entry.Updates}, {"Updated by", entry.UpdatedBy}, {"Also published as", entry.Also}, {"See also", entry.SeeAlso}}
	var relationships []model.DocumentRelationship
	for _, group := range types {
		for _, label := range group.values {
			relationship := model.DocumentRelationship{Type: group.name, Label: label}
			upper := strings.ToUpper(label)
			if strings.HasPrefix(upper, "RFC ") {
				relationship.ID = strings.TrimSpace(strings.TrimPrefix(upper, "RFC "))
				relationship.URL = strings.TrimRight(baseURL, "/") + "/info/rfc" + relationship.ID
			} else {
				relationship.URL = strings.TrimRight(baseURL, "/") + "/search/rfc_search_detail.php?title=" + url.QueryEscape(label)
			}
			relationships = append(relationships, relationship)
		}
	}
	return relationships
}

func relationshipLink(relationship model.DocumentRelationship) string {
	if relationship.ID != "" {
		return "knowledge-read://read?dataset=rfc&id=" + relationship.ID + ` "Call knowledge_read with dataset=rfc and id=` + relationship.ID + `"`
	}
	return relationship.URL
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
	if lifecycle := rfcLifecycleStatus(entry); lifecycle != "current" {
		fmt.Fprintf(&output, "**Lifecycle:** %s  \n", lifecycle)
	}
	for _, relationship := range rfcRelationships(entry, "https://www.rfc-editor.org") {
		fmt.Fprintf(&output, "**%s:** [%s](%s)  \n", relationship.Type, relationship.Label, relationshipLink(relationship))
	}
	output.WriteString("\n")
	scanner := bufio.NewScanner(strings.NewReader(raw))
	scanner.Buffer(make([]byte, 64<<10), 2<<20)
	for scanner.Scan() {
		line := strings.TrimRightFunc(scanner.Text(), unicode.IsSpace)
		if heading, level, ok := rfcMarkdownHeading(line); ok {
			output.WriteString(strings.Repeat("#", level) + " " + heading + "\n")
			continue
		}
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

func rfcMarkdownHeading(line string) (string, int, bool) {
	if strings.TrimSpace(line) != line || line == "" {
		return "", 0, false
	}
	if match := rfcNumberedHeadingRE.FindStringSubmatch(line); match != nil {
		anchor := strings.TrimSuffix(match[1], ".")
		return anchor + ". " + strings.TrimSpace(match[2]), min(2+strings.Count(anchor, "."), 6), true
	}
	if match := rfcAppendixSubheadingRE.FindStringSubmatch(line); match != nil {
		anchor := strings.TrimSuffix(match[1], ".")
		return anchor + ". " + strings.TrimSpace(match[2]), min(2+strings.Count(anchor, "."), 6), true
	}
	if match := rfcAppendixHeadingRE.FindStringSubmatch(line); match != nil {
		return "Appendix " + match[1] + ". " + strings.TrimSpace(match[2]), 2, true
	}
	if _, ok := rfcNamedHeadings[line]; ok {
		return line, 2, true
	}
	return "", 0, false
}

type rfcSectionMarker struct {
	model.DocumentSection
	start int
}

func rfcSectionOutline(content string) []model.DocumentSection {
	markers := rfcSectionMarkers(content)
	sections := make([]model.DocumentSection, len(markers))
	for index := range markers {
		sections[index] = markers[index].DocumentSection
	}
	return sections
}

func rfcExtractSection(content, requested string) (string, bool) {
	markers := rfcSectionMarkers(content)
	target := normalizeRFCSection(requested)
	for index, marker := range markers {
		if target != normalizeRFCSection(marker.Anchor) && target != normalizeRFCSection(marker.Heading) {
			continue
		}
		end := len(content)
		for next := index + 1; next < len(markers); next++ {
			if markers[next].Level <= marker.Level {
				end = markers[next].start
				break
			}
		}
		return strings.TrimSpace(content[marker.start:end]) + "\n", true
	}
	return "", false
}

func rfcSectionMarkers(content string) []rfcSectionMarker {
	lines := strings.SplitAfter(content, "\n")
	markers := make([]rfcSectionMarker, 0)
	offset := 0
	for _, line := range lines {
		trimmed := strings.TrimSuffix(line, "\n")
		trimmed = strings.TrimSuffix(trimmed, "\r")
		level := 0
		for level < len(trimmed) && trimmed[level] == '#' {
			level++
		}
		if level >= 2 && level <= 6 && len(trimmed) > level && trimmed[level] == ' ' {
			heading := strings.TrimSpace(trimmed[level+1:])
			markers = append(markers, rfcSectionMarker{DocumentSection: model.DocumentSection{Heading: heading, Anchor: rfcSectionAnchor(heading), Level: level}, start: offset})
		}
		offset += len(line)
	}
	return markers
}

func rfcSectionAnchor(heading string) string {
	if match := rfcNumberedHeadingRE.FindStringSubmatch(heading); match != nil {
		return strings.TrimSuffix(match[1], ".")
	}
	if match := rfcAppendixSubheadingRE.FindStringSubmatch(heading); match != nil {
		return strings.TrimSuffix(match[1], ".")
	}
	if match := rfcAppendixHeadingRE.FindStringSubmatch(heading); match != nil {
		return "Appendix_" + match[1]
	}
	return strings.ReplaceAll(strings.Join(strings.Fields(heading), " "), " ", "_")
}

func normalizeRFCSection(value string) string {
	value = strings.TrimSpace(strings.TrimPrefix(value, "#"))
	value = strings.NewReplacer("_", " ", "-", " ").Replace(value)
	value = strings.TrimSuffix(value, ".")
	return strings.ToLower(strings.Join(strings.Fields(value), " "))
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
