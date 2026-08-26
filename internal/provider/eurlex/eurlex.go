// Package eurlex provides EU law datasets from the Publications Office CELLAR
// API. Provider-specific discovery and XHTML rendering stay behind the common
// provider.Corpus contract.
package eurlex

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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
	"golang.org/x/net/html"
)

const (
	ProviderID      = "eurlex"
	datasetID       = "eurlex-in-force"
	defaultSPARQL   = "https://publications.europa.eu/webapi/rdf/sparql"
	defaultResource = "https://publications.europa.eu/resource/cellar"
)

type language struct{ Code, Name, Local, Cellar string }

var languages = []language{
	{"bg", "Bulgarian", "Български", "BUL"}, {"es", "Spanish", "Español", "SPA"}, {"cs", "Czech", "Čeština", "CES"}, {"da", "Danish", "Dansk", "DAN"},
	{"de", "German", "Deutsch", "DEU"}, {"et", "Estonian", "Eesti", "EST"}, {"el", "Greek", "Ελληνικά", "ELL"}, {"en", "English", "English", "ENG"},
	{"fr", "French", "Français", "FRA"}, {"ga", "Irish", "Gaeilge", "GLE"}, {"hr", "Croatian", "Hrvatski", "HRV"}, {"it", "Italian", "Italiano", "ITA"},
	{"lv", "Latvian", "Latviešu", "LAV"}, {"lt", "Lithuanian", "Lietuvių", "LIT"}, {"hu", "Hungarian", "Magyar", "HUN"}, {"mt", "Maltese", "Malti", "MLT"},
	{"nl", "Dutch", "Nederlands", "NLD"}, {"pl", "Polish", "Polski", "POL"}, {"pt", "Portuguese", "Português", "POR"}, {"ro", "Romanian", "Română", "RON"},
	{"sk", "Slovak", "Slovenčina", "SLK"}, {"sl", "Slovenian", "Slovenščina", "SLV"}, {"fi", "Finnish", "Suomi", "FIN"}, {"sv", "Swedish", "Svenska", "SWE"},
}

type EURLEX struct {
	sparqlURL, resourceURL string
	http                   *http.Client
}
type release struct {
	Language language `json:"language"`
	Entries  []entry  `json:"entries"`
}
type entry struct {
	CELEX      string `json:"celex"`
	Expression string `json:"expression"`
	Title      string `json:"title"`
	Date       string `json:"date,omitempty"`
}

func New() *EURLEX { return NewWithURLs(defaultSPARQL, defaultResource) }
func NewWithURLs(sparql, resource string) *EURLEX {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ResponseHeaderTimeout = 45 * time.Second
	return &EURLEX{sparqlURL: sparql, resourceURL: strings.TrimRight(resource, "/"), http: &http.Client{Transport: transport}}
}
func (*EURLEX) ID() string                  { return ProviderID }
func (*EURLEX) Owns(collection string) bool { return collection == datasetID }

func (p *EURLEX) Discover(ctx context.Context, filter string, _ bool) ([]model.AvailableDataset, error) {
	if filter != "" && !strings.Contains("eur lex eurlex european union eu law legislation regulations directives decisions legal acts", strings.ToLower(strings.TrimSpace(filter))) {
		return nil, nil
	}
	variants := make([]model.Variant, 0, len(languages))
	availableLanguages := make([]model.Language, 0, len(languages))
	for _, item := range languages {
		variants = append(variants, model.Variant{ID: item.Code, Name: item.Name, Description: "EU legal acts in force in " + item.Name, Format: "application/xhtml+xml"})
		availableLanguages = append(availableLanguages, model.Language{Code: item.Code, Name: item.Name, LocalName: item.Local, Direction: "ltr"})
	}
	return []model.AvailableDataset{{Provider: ProviderID, ID: datasetID, DisplayName: "EUR-Lex — EU law in force", Description: "European Union regulations, directives, decisions, and other sector 3 legal acts currently marked in force by EUR-Lex, with official titles and full text.", Project: "EUR-Lex", ContentType: "European Union law", Profile: profile(), Language: model.Language{Code: "mul", Name: "Multiple languages", LocalName: "Multiple languages"}, Languages: availableLanguages, OnlineSourceURL: "https://eur-lex.europa.eu/", Available: true, Variants: variants}}, nil
}

func (p *EURLEX) Latest(ctx context.Context, collection, variant string) (provider.Release, error) {
	if !p.Owns(collection) {
		return provider.Release{}, fmt.Errorf("unknown EUR-Lex dataset %q", collection)
	}
	lang, ok := findLanguage(variant)
	if !ok {
		return provider.Release{}, fmt.Errorf("unknown EUR-Lex language %q", variant)
	}
	resolved, err := p.resolve(ctx, lang)
	if err != nil {
		return provider.Release{}, err
	}
	return provider.Release{Fingerprint: fingerprint(resolved), Date: time.Now().UTC().Format("20060102"), Value: resolved}, nil
}

func findLanguage(code string) (language, bool) {
	if code == "" {
		code = "en"
	}
	for _, item := range languages {
		if item.Code == code {
			return item, true
		}
	}
	return language{}, false
}

func (p *EURLEX) resolve(ctx context.Context, lang language) (*release, error) {
	resolved := &release{Language: lang}
	const pageSize = 5000
	lastCELEX := ""
	for {
		cursorFilter := ""
		if lastCELEX != "" {
			cursorFilter = fmt.Sprintf("\n FILTER(STR(?celex) > %q)", lastCELEX)
		}
		query := fmt.Sprintf(`PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
SELECT DISTINCT ?celex ?expr ?title ?date WHERE {
 ?work cdm:resource_legal_in-force "true"^^xsd:boolean ; owl:sameAs ?celex .
 FILTER(STRSTARTS(STR(?celex), "http://publications.europa.eu/resource/celex/3"))%s
 ?expr cdm:expression_belongs_to_work ?work ; cdm:expression_uses_language <http://publications.europa.eu/resource/authority/language/%s> ; cdm:expression_title ?title .
 OPTIONAL { ?work cdm:work_date_document ?date }
} ORDER BY ?celex LIMIT %d`, cursorFilter, lang.Cellar, pageSize)
		var result struct {
			Results struct {
				Bindings []map[string]struct {
					Value string `json:"value"`
				} `json:"bindings"`
			} `json:"results"`
		}
		if err := p.querySPARQL(ctx, query, &result); err != nil {
			return nil, err
		}
		for _, binding := range result.Results.Bindings {
			celex := strings.TrimPrefix(binding["celex"].Value, "http://publications.europa.eu/resource/celex/")
			expression, expressionErr := cellarID(binding["expr"].Value)
			if expressionErr != nil {
				return nil, expressionErr
			}
			if celex != "" {
				resolved.Entries = append(resolved.Entries, entry{CELEX: celex, Expression: expression, Title: binding["title"].Value, Date: binding["date"].Value})
			}
		}
		if len(result.Results.Bindings) < pageSize {
			break
		}
		nextCELEX := result.Results.Bindings[len(result.Results.Bindings)-1]["celex"].Value
		if nextCELEX <= lastCELEX {
			return nil, errors.New("EUR-Lex catalog pagination did not advance")
		}
		lastCELEX = nextCELEX
	}
	if len(resolved.Entries) == 0 {
		return nil, errors.New("EUR-Lex catalog contained no in-force legal acts")
	}
	sort.Slice(resolved.Entries, func(i, j int) bool { return resolved.Entries[i].CELEX < resolved.Entries[j].CELEX })
	unique := resolved.Entries[:0]
	for _, item := range resolved.Entries {
		if len(unique) > 0 && unique[len(unique)-1].CELEX == item.CELEX {
			continue
		}
		unique = append(unique, item)
	}
	resolved.Entries = unique
	return resolved, nil
}

func cellarID(value string) (string, error) {
	parsed, err := url.Parse(value)
	if err != nil {
		return "", fmt.Errorf("invalid EUR-Lex expression URI %q: %w", value, err)
	}
	id := strings.TrimPrefix(parsed.Path, "/resource/cellar/")
	if id == "" || id == parsed.Path || strings.Contains(id, "/") {
		return "", fmt.Errorf("invalid EUR-Lex expression URI %q", value)
	}
	return id, nil
}

func (p *EURLEX) querySPARQL(ctx context.Context, query string, target any) error {
	form := url.Values{"query": {query}, "format": {"application/sparql-results+json"}}.Encode()
	const attempts = 5
	for attempt := range attempts {
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, p.sparqlURL, strings.NewReader(form))
		if err != nil {
			return err
		}
		request.Header.Set("Accept", "application/sparql-results+json")
		request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		response, err := p.http.Do(request)
		if err != nil {
			if attempt+1 == attempts {
				return fmt.Errorf("EUR-Lex catalog request: %w", err)
			}
			if err := waitForRetry(ctx, "", attempt); err != nil {
				return err
			}
			continue
		}
		if response.StatusCode == http.StatusOK {
			decodeErr := json.NewDecoder(response.Body).Decode(target)
			_ = response.Body.Close()
			if decodeErr != nil {
				return fmt.Errorf("decode EUR-Lex catalog: %w", decodeErr)
			}
			return nil
		}
		message, _ := io.ReadAll(io.LimitReader(response.Body, 4<<10))
		_ = response.Body.Close()
		retryable := response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= http.StatusInternalServerError
		if !retryable || attempt+1 == attempts {
			return fmt.Errorf("EUR-Lex catalog: %s: %s", response.Status, strings.TrimSpace(string(message)))
		}
		if err := waitForRetry(ctx, response.Header.Get("Retry-After"), attempt); err != nil {
			return err
		}
	}
	return errors.New("EUR-Lex catalog retries exhausted")
}

func waitForRetry(ctx context.Context, retryAfter string, attempt int) error {
	delay := time.Duration(1<<attempt) * 500 * time.Millisecond
	if seconds, err := strconv.Atoi(retryAfter); err == nil && seconds >= 0 {
		delay = time.Duration(seconds) * time.Second
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (p *EURLEX) Acquire(ctx context.Context, collection, variant string, value provider.Release, stage, current string, progress provider.Progress) (model.Manifest, error) {
	resolved, ok := value.Value.(*release)
	if !ok {
		return model.Manifest{}, errors.New("invalid EUR-Lex release metadata")
	}
	rawDir := filepath.Join(stage, "raw")
	if err := os.MkdirAll(rawDir, 0o755); err != nil {
		return model.Manifest{}, err
	}
	metadata, _ := json.MarshalIndent(resolved, "", "  ")
	if err := os.WriteFile(filepath.Join(stage, "documents.json"), append(metadata, '\n'), 0o644); err != nil {
		return model.Manifest{}, err
	}
	old := map[string]entry{}
	if current != "" {
		if data, err := os.ReadFile(filepath.Join(current, "documents.json")); err == nil {
			var previous release
			if json.Unmarshal(data, &previous) == nil {
				for _, item := range previous.Entries {
					old[item.CELEX] = item
				}
			}
		}
	}
	tasks := make(chan entry)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var completed, bytes int64
	for range min(8, len(resolved.Entries)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range tasks {
				destination := filepath.Join(rawDir, item.CELEX+".xhtml")
				previous, unchanged := old[item.CELEX]
				if unchanged && previous.Expression == item.Expression && previous.Title == item.Title && previous.Date == item.Date {
					if err := linkOrCopy(filepath.Join(current, "raw", item.CELEX+".xhtml"), destination); err != nil && !errors.Is(err, os.ErrNotExist) {
						select {
						case errCh <- err:
						default:
						}
						return
					}
				}
				if _, err := os.Stat(destination); errors.Is(err, os.ErrNotExist) {
					if err := p.download(ctx, item, destination); err != nil {
						select {
						case errCh <- err:
						default:
						}
						return
					}
				}
				info, err := os.Stat(destination)
				if err != nil {
					select {
					case errCh <- err:
					default:
					}
					return
				}
				mu.Lock()
				completed++
				bytes += info.Size()
				progress("downloading_documents", completed, int64(len(resolved.Entries)), "documents", 0, "acquiring EUR-Lex legal acts")
				mu.Unlock()
			}
		}()
	}
	var acquireErr error
send:
	for _, item := range resolved.Entries {
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
	wg.Wait()
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
	lang, _ := findLanguage(variant)
	return model.Manifest{Provider: ProviderID, Variant: lang.Code, Dataset: collection, ReleaseDate: value.Date, Fingerprint: value.Fingerprint, PartCount: len(resolved.Entries), RawSize: bytes, ProviderMetadataSize: int64(len(metadata)), DocumentCount: uint64(len(resolved.Entries)), PublishedAt: time.Now().UTC(), Site: model.DatasetMetadata{Name: "EUR-Lex — EU law in force (" + lang.Name + ")", Description: "European Union regulations, directives, decisions, and other sector 3 legal acts currently marked in force by EUR-Lex, with official titles and full text.", Project: "EUR-Lex", ContentType: "European Union law", Profile: profile(), Language: model.Language{Code: lang.Code, Name: lang.Name, LocalName: lang.Local, Direction: "ltr"}, OnlineSourceURL: "https://eur-lex.europa.eu/", SourceDocuments: uint64(len(resolved.Entries)), MetadataUpdatedAt: time.Now().UTC()}}, nil
}

func (p *EURLEX) download(ctx context.Context, item entry, destination string) error {
	partial := destination + ".part"
	var offset int64
	if info, err := os.Stat(partial); err == nil {
		offset = info.Size()
	}
	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, p.resourceURL+"/"+url.PathEscape(item.Expression), nil)
	request.Header.Set("Accept", "text/html")
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
		return fmt.Errorf("download CELEX %s: %s", item.CELEX, response.Status)
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
	return os.Rename(partial, destination)
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

func (p *EURLEX) OpenCorpus(path string, _ model.Manifest) (provider.Corpus, error) {
	data, err := os.ReadFile(filepath.Join(path, "documents.json"))
	if err != nil {
		return nil, err
	}
	var resolved release
	if err := json.Unmarshal(data, &resolved); err != nil {
		return nil, err
	}
	return &corpus{path: path, entries: resolved.Entries}, nil
}

type corpus struct {
	path    string
	entries []entry
}

func (c *corpus) ScanTitles(ctx context.Context, after string, _ provider.ScanOptions, sink provider.RecordSink) error {
	return c.scan(ctx, after, false, sink)
}
func (c *corpus) ScanBodies(ctx context.Context, after string, _ provider.ScanOptions, sink provider.RecordSink) error {
	return c.scan(ctx, after, true, sink)
}
func (c *corpus) scan(ctx context.Context, after string, bodies bool, sink provider.RecordSink) error {
	start := 0
	if after != "" {
		parsed, err := strconv.Atoi(after)
		if err != nil || parsed < 0 || parsed > len(c.entries) {
			return fmt.Errorf("invalid EUR-Lex cursor %q", after)
		}
		start = parsed
	}
	for i := start; i < len(c.entries); i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		item := c.entries[i]
		record := provider.Record{ID: item.CELEX, Title: item.Title, URL: "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:" + item.CELEX, Locator: item.CELEX, Primary: true, Identifiers: []string{"CELEX " + item.CELEX}, Keywords: []string{"European Union law"}, RankWeight: 1, Metadata: map[string]string{"date": item.Date}}
		if bodies {
			data, err := os.ReadFile(filepath.Join(c.path, "raw", item.CELEX+".xhtml"))
			if err != nil {
				return err
			}
			record.Body = xhtmlMarkdown(data)
		}
		if err := sink(record, provider.ScanPosition{Cursor: strconv.Itoa(i + 1), Completed: int64(i + 1), Total: int64(len(c.entries)), Units: "documents", Boundary: true}); err != nil {
			return err
		}
	}
	return nil
}
func (c *corpus) Read(_ context.Context, record provider.Record, options model.ReadOptions) (model.Document, error) {
	if options.Format != "" && options.Format != "markdown" {
		return model.Document{}, errors.New("EUR-Lex documents are available as markdown")
	}
	data, err := os.ReadFile(filepath.Join(c.path, "raw", record.ID+".xhtml"))
	if errors.Is(err, os.ErrNotExist) {
		return model.Document{}, provider.ErrDocumentNotFound
	}
	if err != nil {
		return model.Document{}, err
	}
	return makeDocument(record, xhtmlMarkdown(data), options), nil
}
func (*corpus) Close() error                                           { return nil }
func (*EURLEX) Backfill(context.Context, string, *model.Manifest) bool { return false }

func fingerprint(value *release) string {
	data, _ := json.Marshal(value)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
func profile() model.DatasetProfile {
	return model.DatasetProfile{Topics: []string{"European Union law", "regulation", "directives", "legal compliance"}, GeographicScope: []string{"European Union"}, TimeCoverage: "Founding treaties to present", DocumentTypes: []string{"regulations", "directives", "decisions", "recommendations", "opinions"}, UpdateCadence: "Daily upstream changes", CoverageNotes: "Sector 3 EU legal acts currently marked in force by EUR-Lex, in the selected official language.", SourceFeatures: []string{"CELEX identifiers", "official titles", "full legal text", "tables", "cross-references"}}
}

func makeDocument(record provider.Record, content string, options model.ReadOptions) model.Document {
	runes := []rune(content)
	maximum := options.MaxChars
	if maximum <= 0 {
		maximum = knowledgeindex.DefaultReadCharacters
	}
	start := min(max(options.Offset, 0), len(runes))
	end := min(start+maximum, len(runes))
	result := model.Document{ID: record.ID, Title: record.Title, URL: record.URL, Format: "markdown", Content: string(runes[start:end]), Offset: start, ReturnedChars: end - start, TotalChars: len(runes), Truncated: end < len(runes)}
	if result.Truncated {
		result.NextOffset = end
	}
	return result
}

func xhtmlMarkdown(source []byte) string {
	document, err := html.Parse(bytes.NewReader(source))
	if err != nil {
		return strings.TrimSpace(string(source))
	}
	var out strings.Builder
	render(&out, document, 0)
	result := strings.ReplaceAll(out.String(), "\u00a0", " ")
	for strings.Contains(result, "\n\n\n") {
		result = strings.ReplaceAll(result, "\n\n\n", "\n\n")
	}
	return strings.TrimSpace(result)
}
func render(out *strings.Builder, node *html.Node, depth int) {
	if node.Type == html.TextNode {
		out.WriteString(strings.Join(strings.Fields(node.Data), " "))
		return
	}
	if node.Type == html.ElementNode && (node.Data == "script" || node.Data == "style" || node.Data == "svg") {
		return
	}
	if node.Type != html.ElementNode {
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			render(out, child, depth)
		}
		return
	}
	name := strings.ToLower(node.Data)
	switch name {
	case "h1", "h2", "h3", "h4", "h5", "h6":
		blank(out)
		level := int(name[1] - '0')
		out.WriteString(strings.Repeat("#", level) + " ")
	case "p", "div", "section", "article":
		blank(out)
	case "br":
		out.WriteString("  \n")
	case "strong", "b":
		out.WriteString("**")
	case "em", "i":
		out.WriteString("*")
	case "ul", "ol":
		newline(out)
		depth++
	case "li":
		newline(out)
		out.WriteString(strings.Repeat("  ", max(0, depth-1)) + "- ")
	case "table":
		renderTable(out, node)
		blank(out)
		return
	case "a":
		label := nodeText(node)
		href := attribute(node, "href")
		if label != "" {
			out.WriteString("[" + escape(label) + "](" + eurlexLink(href) + ")")
		}
		return
	}
	for child := node.FirstChild; child != nil; child = child.NextSibling {
		render(out, child, depth)
	}
	switch name {
	case "strong", "b":
		out.WriteString("**")
	case "em", "i":
		out.WriteString("*")
	case "p", "div", "section", "article", "h1", "h2", "h3", "h4", "h5", "h6", "ul", "ol":
		blank(out)
	case "li":
		newline(out)
	}
}
func renderTable(out *strings.Builder, table *html.Node) {
	var rows [][]string
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.ElementNode && strings.EqualFold(n.Data, "tr") {
			var cells []string
			for c := n.FirstChild; c != nil; c = c.NextSibling {
				if c.Type == html.ElementNode && (strings.EqualFold(c.Data, "th") || strings.EqualFold(c.Data, "td")) {
					cells = append(cells, strings.ReplaceAll(nodeText(c), "|", "\\|"))
				}
			}
			if len(cells) > 0 {
				rows = append(rows, cells)
			}
			return
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(table)
	if len(rows) == 0 {
		return
	}
	columns := 0
	for _, row := range rows {
		columns = max(columns, len(row))
	}
	write := func(row []string) {
		out.WriteString("| ")
		for i := range columns {
			if i < len(row) {
				out.WriteString(row[i])
			}
			out.WriteString(" | ")
		}
		out.WriteByte('\n')
	}
	write(rows[0])
	separator := make([]string, columns)
	for i := range separator {
		separator[i] = "---"
	}
	write(separator)
	for _, row := range rows[1:] {
		write(row)
	}
}
func nodeText(node *html.Node) string {
	var out strings.Builder
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.TextNode {
			out.WriteString(n.Data)
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(node)
	return strings.Join(strings.Fields(out.String()), " ")
}
func attribute(node *html.Node, name string) string {
	for _, item := range node.Attr {
		if strings.EqualFold(item.Key, name) {
			return item.Val
		}
	}
	return ""
}
func eurlexLink(href string) string {
	parsed, err := url.Parse(strings.TrimSpace(href))
	if err != nil {
		return href
	}
	candidate := parsed.Query().Get("uri")
	candidate = strings.TrimPrefix(strings.ToUpper(candidate), "CELEX:")
	if candidate == "" {
		parts := strings.Split(strings.Trim(parsed.Path, "/"), "/")
		for i := range parts {
			if strings.EqualFold(parts[i], "celex") && i+1 < len(parts) {
				candidate = parts[i+1]
			}
		}
	}
	if candidate != "" {
		return "knowledge-read://read?dataset=" + datasetID + "&id=" + url.QueryEscape(candidate)
	}
	return href
}
func escape(value string) string {
	return strings.NewReplacer("\\", "\\\\", "[", "\\[", "]", "\\]").Replace(value)
}
func newline(out *strings.Builder) {
	value := out.String()
	if len(value) > 0 && value[len(value)-1] != '\n' {
		out.WriteByte('\n')
	}
}
func blank(out *strings.Builder) {
	value := out.String()
	if value == "" || strings.HasSuffix(value, "\n\n") {
		return
	}
	if strings.HasSuffix(value, "\n") {
		out.WriteByte('\n')
	} else {
		out.WriteString("\n\n")
	}
}
