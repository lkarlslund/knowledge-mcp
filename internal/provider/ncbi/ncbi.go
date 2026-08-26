// Package ncbi provides local PubMed knowledge datasets from NCBI's official
// annual baseline. It keeps the provider concerned with acquisition and
// document formatting; the shared index remains provider-independent.
package ncbi

import (
	"context"
	"crypto/md5"
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

	"github.com/klauspost/compress/gzip"
	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/knowledgeindex"
	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/model"
	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/provider"
)

const (
	ProviderID           = "ncbi"
	datasetID            = "pubmed"
	variantID            = "baseline"
	defaultURL           = "https://ftp.ncbi.nlm.nih.gov/pubmed/baseline"
	defaultUpdatesURL    = "https://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles"
	pubmedRecordsPerPart = 30_000
)

var partRE = regexp.MustCompile(`href="(pubmed(\d{2})n\d{4}\.xml\.gz)"[^>]*>[^<]*</a>\s+([0-9-]+\s+[0-9:]+)\s+([0-9.]+[KMGT]?)`) // NCBI Apache listing.

type NCBI struct {
	baseURL, updatesURL string
	http                *http.Client
}

type release struct {
	Parts []part `json:"parts"`
}

type part struct {
	Name     string `json:"name"`
	Size     int64  `json:"size"`
	MD5      string `json:"md5"`
	Kind     string `json:"kind,omitempty"`
	Modified string `json:"modified,omitempty"`
}

func New() *NCBI { return NewWithURLs(defaultURL, defaultUpdatesURL) }

func NewWithBaseURL(baseURL string) *NCBI {
	return NewWithURLs(baseURL, "")
}

func NewWithURLs(baseURL, updatesURL string) *NCBI {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ResponseHeaderTimeout = 30 * time.Second
	return &NCBI{baseURL: strings.TrimRight(baseURL, "/"), updatesURL: strings.TrimRight(updatesURL, "/"), http: &http.Client{Transport: transport}}
}

func (*NCBI) ID() string                  { return ProviderID }
func (*NCBI) Owns(collection string) bool { return collection == datasetID }

func (p *NCBI) Discover(ctx context.Context, filter string, _ bool) ([]model.AvailableDataset, error) {
	resolved, err := p.resolve(ctx)
	if err != nil {
		return nil, err
	}
	if filter != "" && !strings.Contains("pubmed ncbi biomedical medicine life sciences citations abstracts research", strings.ToLower(strings.TrimSpace(filter))) {
		return nil, nil
	}
	var size int64
	for _, item := range resolved.Parts {
		size += item.Size
	}
	year := pubmedReleaseDate(resolved.Parts)
	return []model.AvailableDataset{{
		Provider: ProviderID, Variant: variantID, ID: datasetID, DisplayName: "PubMed",
		Description: "NCBI PubMed biomedical and life-sciences citations, bibliographic metadata, abstracts, publication types, and MeSH terms from the annual baseline plus ordered daily updates.",
		Project:     "NCBI PubMed", ContentType: "Biomedical citations and abstracts", Profile: profile(),
		Language:        model.Language{Code: "mul", Name: "Multiple languages", LocalName: "Multiple languages"},
		OnlineSourceURL: "https://pubmed.ncbi.nlm.nih.gov/", ReleaseDate: year, Available: true,
		RawSize: size, PartCount: len(resolved.Parts), Fingerprint: fingerprint(resolved),
		Variants: []model.Variant{{ID: variantID, Name: "Current PubMed", Description: "Complete annual baseline with subsequent daily revisions and deletions applied in order", Format: "application/xml+gzip"}},
	}}, nil
}

func (p *NCBI) Latest(ctx context.Context, collection, variant string) (provider.Release, error) {
	if !p.Owns(collection) || variant != "" && variant != variantID {
		return provider.Release{}, fmt.Errorf("unknown NCBI dataset or variant %q/%q", collection, variant)
	}
	resolved, err := p.resolve(ctx)
	if err != nil {
		return provider.Release{}, err
	}
	return provider.Release{Fingerprint: fingerprint(resolved), Date: pubmedReleaseDate(resolved.Parts), Value: resolved}, nil
}

func (p *NCBI) resolve(ctx context.Context) (*release, error) {
	baseline, err := p.resolveListing(ctx, p.baseURL, "baseline")
	if err != nil {
		return nil, err
	}
	resolved := &release{Parts: baseline}
	if p.updatesURL != "" {
		updates, updateErr := p.resolveListing(ctx, p.updatesURL, "update")
		if updateErr != nil {
			return nil, updateErr
		}
		resolved.Parts = append(resolved.Parts, updates...)
	}
	sort.SliceStable(resolved.Parts, func(i, j int) bool { return resolved.Parts[i].Name < resolved.Parts[j].Name })
	return resolved, nil
}

func (p *NCBI) resolveListing(ctx context.Context, baseURL, kind string) ([]part, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/", nil)
	if err != nil {
		return nil, err
	}
	response, err := p.http.Do(request)
	if err != nil {
		return nil, err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("NCBI %s catalog: %s", kind, response.Status)
	}
	listing, err := io.ReadAll(io.LimitReader(response.Body, 8<<20))
	if err != nil {
		return nil, err
	}
	matches := partRE.FindAllSubmatch(listing, -1)
	if len(matches) == 0 {
		if kind == "update" {
			return nil, nil
		}
		return nil, fmt.Errorf("NCBI %s catalog contained no PubMed parts", kind)
	}
	resolved := make([]part, 0, len(matches))
	for _, match := range matches {
		resolved = append(resolved, part{Name: string(match[1]), Size: parseSize(string(match[4])), Kind: kind, Modified: strings.Join(strings.Fields(string(match[3])), " ")})
	}
	sort.Slice(resolved, func(i, j int) bool { return resolved[i].Name < resolved[j].Name })
	return resolved, nil
}

func (p *NCBI) fetchText(ctx context.Context, address string, limit int64) (string, error) {
	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, address, nil)
	response, err := p.http.Do(request)
	if err != nil {
		return "", err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("download %s: %s", address, response.Status)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, limit))
	return string(data), err
}

func (p *NCBI) Acquire(ctx context.Context, collection, _ string, value provider.Release, stage, current string, progress provider.Progress) (model.Manifest, error) {
	resolved, ok := value.Value.(*release)
	if !ok {
		return model.Manifest{}, errors.New("invalid NCBI release metadata")
	}
	rawDir := filepath.Join(stage, "raw")
	if err := os.MkdirAll(rawDir, 0o755); err != nil {
		return model.Manifest{}, err
	}
	metadata, _ := json.MarshalIndent(resolved, "", "  ")
	if err := os.WriteFile(filepath.Join(stage, "documents.json"), append(metadata, '\n'), 0o644); err != nil {
		return model.Manifest{}, err
	}
	var total int64
	for _, item := range resolved.Parts {
		total += item.Size
	}
	var completed int64
	var progressMu sync.Mutex
	tasks := make(chan part)
	errCh := make(chan error, 1)
	var workers sync.WaitGroup
	for range min(6, len(resolved.Parts)) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for item := range tasks {
				destination := filepath.Join(rawDir, item.Name)
				if validFile(destination, item) {
					progressMu.Lock()
					completed += item.Size
					progress("downloading_parts", completed, total, "bytes", 0, "acquiring PubMed baseline and daily updates")
					progressMu.Unlock()
					continue
				}
				if current != "" && validFile(filepath.Join(current, "raw", item.Name), item) {
					if err := linkOrCopy(filepath.Join(current, "raw", item.Name), destination); err != nil {
						select {
						case errCh <- err:
						default:
						}
						return
					}
				} else if err := p.downloadPart(ctx, item, destination); err != nil {
					select {
					case errCh <- err:
					default:
					}
					return
				}
				progressMu.Lock()
				completed += item.Size
				progress("downloading_parts", completed, total, "bytes", 0, "acquiring PubMed baseline and daily updates")
				progressMu.Unlock()
			}
		}()
	}
	var acquireErr error
send:
	for _, item := range resolved.Parts {
		select {
		case tasks <- item:
		case acquireErr = <-errCh:
			break send
		case <-ctx.Done():
			acquireErr = ctx.Err()
			break send
		}
	}
	close(tasks)
	workers.Wait()
	select {
	case err := <-errCh:
		if acquireErr == nil {
			acquireErr = err
		}
	default:
	}
	if acquireErr != nil {
		return model.Manifest{}, acquireErr
	}
	var actualBytes int64
	for _, item := range resolved.Parts {
		if info, statErr := os.Stat(filepath.Join(rawDir, item.Name)); statErr == nil {
			actualBytes += info.Size()
		}
	}
	return model.Manifest{Provider: ProviderID, Variant: variantID, Dataset: collection, ReleaseDate: value.Date, Fingerprint: value.Fingerprint, PartCount: len(resolved.Parts), RawSize: actualBytes, ProviderMetadataSize: int64(len(metadata)), PublishedAt: time.Now().UTC(), Site: model.DatasetMetadata{Name: "PubMed", Description: "NCBI PubMed biomedical and life-sciences citations, bibliographic metadata, abstracts, publication types, and MeSH terms from the annual baseline plus ordered daily updates.", Project: "NCBI PubMed", ContentType: "Biomedical citations and abstracts", Profile: profile(), Language: model.Language{Code: "mul", Name: "Multiple languages", LocalName: "Multiple languages"}, OnlineSourceURL: "https://pubmed.ncbi.nlm.nih.gov/", MetadataUpdatedAt: time.Now().UTC()}}, nil
}

func (p *NCBI) downloadPart(ctx context.Context, item part, destination string) error {
	baseURL := p.baseURL
	if item.Kind == "update" {
		baseURL = p.updatesURL
	}
	checksum, err := p.fetchText(ctx, baseURL+"/"+item.Name+".md5", 1024)
	if err != nil {
		return err
	}
	fields := strings.Fields(checksum)
	if len(fields) == 0 || len(fields[len(fields)-1]) != md5.Size*2 {
		return fmt.Errorf("invalid checksum for %s", item.Name)
	}
	item.MD5 = strings.ToLower(fields[len(fields)-1])
	partial := destination + ".part"
	var offset int64
	if info, err := os.Stat(partial); err == nil {
		offset = info.Size()
	}
	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/"+item.Name, nil)
	if offset > 0 {
		request.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
	}
	response, err := p.http.Do(request)
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	flags := os.O_CREATE | os.O_WRONLY
	if response.StatusCode == http.StatusPartialContent && offset > 0 {
		flags |= os.O_APPEND
	} else if response.StatusCode == http.StatusOK {
		flags |= os.O_TRUNC
	} else {
		return fmt.Errorf("download %s: %s", item.Name, response.Status)
	}
	file, err := os.OpenFile(partial, flags, 0o644)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(file, response.Body)
	closeErr := file.Close()
	if copyErr != nil {
		return copyErr
	}
	if closeErr != nil {
		return closeErr
	}
	if !validFile(partial, item) {
		return fmt.Errorf("checksum verification failed for %s", item.Name)
	}
	return os.Rename(partial, destination)
}

func validFile(path string, item part) bool {
	info, err := os.Stat(path)
	if err != nil || info.Size() == 0 {
		return false
	}
	if item.MD5 == "" {
		return true
	}
	file, err := os.Open(path)
	if err != nil {
		return false
	}
	defer func() { _ = file.Close() }()
	hash := md5.New() // NCBI publishes MD5 sidecars; this is integrity checking, not security.
	_, err = io.Copy(hash, file)
	return err == nil && hex.EncodeToString(hash.Sum(nil)) == item.MD5
}

func linkOrCopy(source, destination string) error {
	if err := os.Link(source, destination); err == nil {
		return nil
	}
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	out, err := os.Create(destination)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(out, in)
	closeErr := out.Close()
	if copyErr != nil {
		return copyErr
	}
	return closeErr
}

func (p *NCBI) OpenCorpus(path string, _ model.Manifest) (provider.Corpus, error) {
	data, err := os.ReadFile(filepath.Join(path, "documents.json"))
	if err != nil {
		return nil, err
	}
	var resolved release
	if err := json.Unmarshal(data, &resolved); err != nil {
		return nil, err
	}
	return &corpus{path: path, parts: resolved.Parts}, nil
}

type corpus struct {
	path  string
	parts []part
}

func (c *corpus) ScanTitles(ctx context.Context, after string, _ provider.ScanOptions, sink provider.RecordSink) error {
	return c.scanSAX(ctx, after, false, sink)
}
func (c *corpus) ScanBodies(ctx context.Context, after string, _ provider.ScanOptions, sink provider.RecordSink) error {
	return c.scanSAX(ctx, after, true, sink)
}

func (c *corpus) scanSAX(ctx context.Context, after string, bodies bool, sink provider.RecordSink) error {
	partIndex, recordIndex, documents, err := parseCursor(after)
	if err != nil {
		return err
	}
	estimatedDocuments := int64(len(c.parts) * pubmedRecordsPerPart)
	for pi := partIndex; pi < len(c.parts); pi++ {
		file, err := os.Open(filepath.Join(c.path, "raw", c.parts[pi].Name))
		if err != nil {
			return err
		}
		gz, err := gzip.NewReader(file)
		if err != nil {
			_ = file.Close()
			return err
		}
		current := 0
		err = pipelinePubMedArticles(ctx, gz, bodies, func(article pubmedTitle) error {
			if pi == partIndex && current < recordIndex {
				current++
				return nil
			}
			record := article.record(c.parts[pi].Name, bodies)
			if record.ID == "" {
				current++
				return nil
			}
			documents++
			current++
			cursor := fmt.Sprintf("%d:%d:%d", pi, current, documents)
			if err := sink(record, provider.ScanPosition{Cursor: cursor, Completed: documents, Total: estimatedDocuments, Units: "documents", Boundary: true}); err != nil {
				return err
			}
			return nil
		})
		_ = gz.Close()
		_ = file.Close()
		if err != nil {
			return err
		}
		recordIndex = 0
	}
	return nil
}

func pipelinePubMedArticles(ctx context.Context, source io.Reader, bodies bool, consume func(pubmedTitle) error) error {
	parseCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	articles := make(chan pubmedTitle, 1024)
	parsed := make(chan error, 1)
	go func() {
		defer close(articles)
		parsed <- scanPubMedArticles(source, bodies, func(article pubmedTitle) error {
			select {
			case articles <- article:
				return nil
			case <-parseCtx.Done():
				return parseCtx.Err()
			}
		})
	}()
	for article := range articles {
		if err := consume(article); err != nil {
			cancel()
			<-parsed
			return err
		}
	}
	return <-parsed
}

func parseCursor(value string) (int, int, int64, error) {
	if value == "" {
		return 0, 0, 0, nil
	}
	parts := strings.Split(value, ":")
	if len(parts) != 3 {
		return 0, 0, 0, fmt.Errorf("invalid PubMed cursor %q", value)
	}
	a, err1 := strconv.Atoi(parts[0])
	b, err2 := strconv.Atoi(parts[1])
	c, err3 := strconv.ParseInt(parts[2], 10, 64)
	if err1 != nil || err2 != nil || err3 != nil || a < 0 || b < 0 || c < 0 {
		return 0, 0, 0, fmt.Errorf("invalid PubMed cursor %q", value)
	}
	return a, b, c, nil
}

type pubmedArticle struct {
	Citation struct {
		PMID    string `xml:"PMID"`
		Article struct {
			Title    richText       `xml:"ArticleTitle"`
			Abstract []abstractText `xml:"Abstract>AbstractText"`
			Journal  struct {
				Title string `xml:"Title"`
				Issue struct {
					Date struct {
						Year    string `xml:"Year"`
						Medline string `xml:"MedlineDate"`
					} `xml:"PubDate"`
				} `xml:"JournalIssue"`
			} `xml:"Journal"`
			Authors []struct {
				Last       string `xml:"LastName"`
				Fore       string `xml:"ForeName"`
				Collective string `xml:"CollectiveName"`
			} `xml:"AuthorList>Author"`
			Types    []string `xml:"PublicationTypeList>PublicationType"`
			Language []string `xml:"Language"`
		} `xml:"Article"`
		Mesh []struct {
			Descriptor string   `xml:"DescriptorName"`
			Qualifiers []string `xml:"QualifierName"`
		} `xml:"MeshHeadingList>MeshHeading"`
	} `xml:"MedlineCitation"`
	Data struct {
		IDs []struct {
			Type  string `xml:"IdType,attr"`
			Value string `xml:",chardata"`
		} `xml:"ArticleIdList>ArticleId"`
	} `xml:"PubmedData"`
}

type richText string

type abstractText struct {
	Label string
	Text  richText
}

func (value *abstractText) UnmarshalXML(decoder *xml.Decoder, start xml.StartElement) error {
	for _, attribute := range start.Attr {
		if attribute.Name.Local == "Label" {
			value.Label = attribute.Value
		}
	}
	return value.Text.UnmarshalXML(decoder, start)
}

func (value *richText) UnmarshalXML(decoder *xml.Decoder, start xml.StartElement) error {
	var text strings.Builder
	depth := 1
	for depth > 0 {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		switch item := token.(type) {
		case xml.StartElement:
			depth++
		case xml.EndElement:
			depth--
		case xml.CharData:
			text.Write(item)
		}
	}
	*value = richText(strings.Join(strings.Fields(text.String()), " "))
	return nil
}

func (a pubmedArticle) markdown() string {
	var out strings.Builder
	out.WriteString("# " + strings.TrimSpace(string(a.Citation.Article.Title)) + "\n\n")
	var authors []string
	for _, author := range a.Citation.Article.Authors {
		name := strings.TrimSpace(strings.TrimSpace(author.Fore + " " + author.Last))
		if name == "" {
			name = author.Collective
		}
		if name != "" {
			authors = append(authors, name)
		}
	}
	if len(authors) > 0 {
		out.WriteString("**Authors:** " + strings.Join(authors, ", ") + "\n\n")
	}
	if journal := strings.TrimSpace(a.Citation.Article.Journal.Title); journal != "" {
		out.WriteString("**Journal:** " + journal + "\n\n")
	}
	for _, abstract := range a.Citation.Article.Abstract {
		if abstract.Label != "" {
			out.WriteString("## " + abstract.Label + "\n\n")
		}
		out.WriteString(strings.TrimSpace(string(abstract.Text)) + "\n\n")
	}
	if len(a.Citation.Mesh) > 0 {
		var terms []string
		for _, mesh := range a.Citation.Mesh {
			if mesh.Descriptor != "" {
				terms = append(terms, mesh.Descriptor)
			}
		}
		out.WriteString("**MeSH terms:** " + strings.Join(terms, "; ") + "\n")
	}
	return strings.TrimSpace(out.String())
}

func (c *corpus) Read(ctx context.Context, record provider.Record, options model.ReadOptions) (model.Document, error) {
	if options.Format != "" && options.Format != "markdown" {
		return model.Document{}, errors.New("PubMed documents are available as markdown")
	}
	file, err := os.Open(filepath.Join(c.path, "raw", filepath.Base(record.Locator)))
	if err != nil {
		return model.Document{}, err
	}
	defer func() { _ = file.Close() }()
	gz, err := gzip.NewReader(file)
	if err != nil {
		return model.Document{}, err
	}
	defer func() { _ = gz.Close() }()
	decoder := xml.NewDecoder(gz)
	for {
		token, tokenErr := decoder.Token()
		if errors.Is(tokenErr, io.EOF) {
			break
		}
		if tokenErr != nil {
			return model.Document{}, tokenErr
		}
		start, ok := token.(xml.StartElement)
		if !ok || start.Name.Local != "PubmedArticle" {
			continue
		}
		var article pubmedArticle
		if err := decoder.DecodeElement(&article, &start); err != nil {
			return model.Document{}, err
		}
		if strings.TrimSpace(article.Citation.PMID) == record.ID {
			return document(record, article.markdown(), options), nil
		}
		if err := ctx.Err(); err != nil {
			return model.Document{}, err
		}
	}
	return model.Document{}, provider.ErrDocumentNotFound
}

func document(record provider.Record, content string, options model.ReadOptions) model.Document {
	runes := []rune(content)
	maximum := options.MaxChars
	if maximum <= 0 {
		maximum = knowledgeindex.DefaultReadCharacters
	}
	start := min(max(options.Offset, 0), len(runes))
	end := min(start+maximum, len(runes))
	result := model.Document{TemporalMetadata: record.Temporal, ID: record.ID, Title: record.Title, URL: record.URL, Format: "markdown", Content: string(runes[start:end]), Offset: start, ReturnedChars: end - start, TotalChars: len(runes), Truncated: end < len(runes)}
	if result.Truncated {
		result.NextOffset = end
	}
	return result
}

func (*corpus) Close() error                                         { return nil }
func (*NCBI) Backfill(context.Context, string, *model.Manifest) bool { return false }

func fingerprint(value *release) string {
	data, _ := json.Marshal(value)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
func baselineYear(parts []part) string {
	if len(parts) > 0 && len(parts[0].Name) >= 8 {
		year, _ := strconv.Atoi(parts[0].Name[6:8])
		if year >= 70 {
			return fmt.Sprintf("19%02d", year)
		}
		return fmt.Sprintf("20%02d", year)
	}
	return time.Now().UTC().Format("2006")
}

func pubmedReleaseDate(parts []part) string {
	for index := len(parts) - 1; index >= 0; index-- {
		if parts[index].Kind == "update" && len(parts[index].Modified) >= len("2006-01-02") {
			return strings.ReplaceAll(parts[index].Modified[:len("2006-01-02")], "-", "")
		}
	}
	return baselineYear(parts)
}
func parseSize(value string) int64 {
	value = strings.TrimSpace(strings.ToUpper(value))
	multiplier := float64(1)
	if len(value) > 0 {
		switch value[len(value)-1] {
		case 'K':
			multiplier, value = 1<<10, value[:len(value)-1]
		case 'M':
			multiplier, value = 1<<20, value[:len(value)-1]
		case 'G':
			multiplier, value = 1<<30, value[:len(value)-1]
		case 'T':
			multiplier, value = 1<<40, value[:len(value)-1]
		}
	}
	number, _ := strconv.ParseFloat(value, 64)
	return int64(number * multiplier)
}
func profile() model.DatasetProfile {
	return model.DatasetProfile{Topics: []string{"medicine", "biomedicine", "life sciences", "health research"}, GeographicScope: []string{"global research literature"}, TimeCoverage: "1946 to present", DocumentTypes: []string{"journal citations", "abstracts", "clinical studies", "reviews"}, UpdateCadence: "Daily updates after annual baseline", CoverageNotes: "PubMed annual baseline with ordered daily additions, revisions, and deletions; citations may have abstracts and MeSH indexing depending on the source record.", SourceFeatures: []string{"PMID", "DOI", "abstracts", "MeSH terms", "publication types", "authors", "publication and revision dates"}}
}
