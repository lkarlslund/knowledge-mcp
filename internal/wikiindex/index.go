package wikiindex

import (
	"bufio"
	"compress/bzip2"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"html"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"unicode"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/analysis/analyzer/keyword"
	"github.com/blevesearch/bleve/v2/mapping"
	blevequery "github.com/blevesearch/bleve/v2/search/query"
	"github.com/cosnicolaou/pbzip2"
	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/model"
)

const (
	TitleIndexDir = "titles.bleve"
	BodyIndexDir  = "bodies.bleve"
)

type titleDocument struct {
	Title      string `json:"title"`
	TitleExact string `json:"title_exact"`
	PageID     uint64 `json:"page_id"`
	Offset     int64  `json:"offset"`
}

type bodyDocument struct {
	Title  string `json:"title"`
	Body   string `json:"body"`
	PageID uint64 `json:"page_id"`
}

type stream struct {
	Offset int64
	End    int64
}

type xmlPage struct {
	Title    string `xml:"title"`
	ID       uint64 `xml:"id"`
	Revision struct {
		ID        uint64 `xml:"id"`
		Timestamp string `xml:"timestamp"`
		Text      string `xml:"text"`
	} `xml:"revision"`
}

type BuildProgress func(done, total int64)

func BuildTitle(ctx context.Context, compressedIndex, destination string, progress BuildProgress) (uint64, error) {
	if err := os.RemoveAll(destination); err != nil {
		return 0, err
	}
	idx, err := bleve.New(destination, titleMapping())
	if err != nil {
		return 0, fmt.Errorf("create title index: %w", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = idx.Close()
		}
	}()
	f, err := os.Open(compressedIndex)
	if err != nil {
		return 0, err
	}
	defer func() { _ = f.Close() }()
	scanner := bufio.NewScanner(pbzip2.NewReader(ctx, f))
	scanner.Buffer(make([]byte, 64<<10), 4<<20)
	batch := idx.NewBatch()
	var count uint64
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
		}
		offset, pageID, title, err := parseIndexLine(scanner.Text())
		if err != nil {
			return 0, err
		}
		doc := titleDocument{Title: title, TitleExact: normalizeTitle(title), PageID: pageID, Offset: offset}
		if err := batch.Index(strconv.FormatUint(pageID, 10), doc); err != nil {
			return 0, err
		}
		count++
		if count%5000 == 0 {
			if err := idx.Batch(batch); err != nil {
				return 0, fmt.Errorf("write title index: %w", err)
			}
			batch = idx.NewBatch()
			progress(int64(count), 0)
		}
	}
	if err := scanner.Err(); err != nil {
		return 0, fmt.Errorf("read multistream index: %w", err)
	}
	if err := idx.Batch(batch); err != nil {
		return 0, fmt.Errorf("write final title batch: %w", err)
	}
	progress(int64(count), int64(count))
	if err := idx.Close(); err != nil {
		return 0, fmt.Errorf("close title index: %w", err)
	}
	closed = true
	return count, nil
}

func BuildBody(ctx context.Context, dumpPath, compressedIndex, destination string, progress BuildProgress) error {
	if err := os.RemoveAll(destination); err != nil {
		return err
	}
	streams, err := readStreams(ctx, compressedIndex, dumpPath)
	if err != nil {
		return err
	}
	idx, err := bleve.New(destination, bodyMapping())
	if err != nil {
		return fmt.Errorf("create body index: %w", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = idx.Close()
		}
	}()
	dump, err := os.Open(dumpPath)
	if err != nil {
		return err
	}
	defer func() { _ = dump.Close() }()
	type decoded struct {
		part  stream
		pages []xmlPage
		err   error
	}
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	workerCount := min(runtime.GOMAXPROCS(0), 16)
	jobs := make(chan stream)
	results := make(chan decoded, workerCount)
	var workers sync.WaitGroup
	for range workerCount {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for part := range jobs {
				pages, decodeErr := decodeStream(io.NewSectionReader(dump, part.Offset, part.End-part.Offset))
				select {
				case results <- decoded{part: part, pages: pages, err: decodeErr}:
				case <-workerCtx.Done():
					return
				}
			}
		}()
	}
	go func() {
		defer close(jobs)
		for _, part := range streams {
			select {
			case jobs <- part:
			case <-workerCtx.Done():
				return
			}
		}
	}()
	go func() {
		workers.Wait()
		close(results)
	}()

	batch := idx.NewBatch()
	batchCount, completedStreams := 0, int64(0)
	for result := range results {
		if result.err != nil {
			cancel()
			return fmt.Errorf("decode stream at %d: %w", result.part.Offset, result.err)
		}
		for _, page := range result.pages {
			doc := bodyDocument{Title: page.Title, Body: PlainText(page.Revision.Text), PageID: page.ID}
			if err := batch.Index(strconv.FormatUint(page.ID, 10), doc); err != nil {
				return err
			}
			batchCount++
			if batchCount >= 1000 {
				if err := idx.Batch(batch); err != nil {
					return fmt.Errorf("write body index: %w", err)
				}
				batch, batchCount = idx.NewBatch(), 0
			}
		}
		completedStreams++
		progress(completedStreams, int64(len(streams)))
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if batchCount > 0 {
		if err := idx.Batch(batch); err != nil {
			return fmt.Errorf("write final body batch: %w", err)
		}
	}
	if err := idx.Close(); err != nil {
		return fmt.Errorf("close body index: %w", err)
	}
	closed = true
	return nil
}

func Search(generationPath, query string, offset, limit int, fullText bool) (model.SearchResult, error) {
	if strings.TrimSpace(query) == "" {
		return model.SearchResult{}, errors.New("query cannot be empty")
	}
	if offset < 0 {
		offset = 0
	}
	if limit <= 0 || limit > 50 {
		limit = 10
	}
	path := filepath.Join(generationPath, TitleIndexDir)
	mode := "title"
	if fullText {
		path = filepath.Join(generationPath, BodyIndexDir)
		mode = "full_text"
	}
	idx, err := bleve.Open(path)
	if err != nil {
		return model.SearchResult{}, fmt.Errorf("open %s index: %w", mode, err)
	}
	defer func() { _ = idx.Close() }()
	titleQuery := bleve.NewMatchQuery(query)
	titleQuery.SetField("title")
	titleQuery.SetBoost(3)
	var q blevequery.Query = titleQuery
	if fullText {
		bodyQuery := bleve.NewMatchQuery(query)
		bodyQuery.SetField("body")
		q = bleve.NewDisjunctionQuery(titleQuery, bodyQuery)
	}
	req := bleve.NewSearchRequestOptions(q, limit, offset, false)
	req.Fields = []string{"title", "page_id"}
	response, err := idx.Search(req)
	if err != nil {
		return model.SearchResult{}, fmt.Errorf("search: %w", err)
	}
	result := model.SearchResult{SearchMode: mode, Total: response.Total, Offset: offset, Hits: make([]model.SearchHit, 0, len(response.Hits))}
	for _, hit := range response.Hits {
		pageID, _ := strconv.ParseUint(hit.ID, 10, 64)
		title, _ := hit.Fields["title"].(string)
		result.Hits = append(result.Hits, model.SearchHit{PageID: pageID, Title: title, Score: hit.Score})
	}
	return result, nil
}

func ReadPage(generationPath, title string, pageID uint64, format string, start, maxChars int) (model.Page, error) {
	if title == "" && pageID == 0 || title != "" && pageID != 0 {
		return model.Page{}, errors.New("provide exactly one of title or page_id")
	}
	idx, err := bleve.Open(filepath.Join(generationPath, TitleIndexDir))
	if err != nil {
		return model.Page{}, fmt.Errorf("open title index: %w", err)
	}
	var q blevequery.Query
	if pageID != 0 {
		q = bleve.NewDocIDQuery([]string{strconv.FormatUint(pageID, 10)})
	} else {
		term := bleve.NewTermQuery(normalizeTitle(title))
		term.SetField("title_exact")
		q = term
	}
	req := bleve.NewSearchRequestOptions(q, 1, 0, false)
	req.Fields = []string{"title", "page_id", "offset"}
	res, err := idx.Search(req)
	closeErr := idx.Close()
	if err != nil {
		return model.Page{}, err
	}
	if closeErr != nil {
		return model.Page{}, closeErr
	}
	if len(res.Hits) == 0 {
		return model.Page{}, errors.New("page not found")
	}
	hit := res.Hits[0]
	wantedID, _ := strconv.ParseUint(hit.ID, 10, 64)
	streamOffset := int64(hit.Fields["offset"].(float64))
	dump, err := os.Open(filepath.Join(generationPath, "dump.xml.bz2"))
	if err != nil {
		return model.Page{}, err
	}
	defer func() { _ = dump.Close() }()
	pages, err := decodeStream(io.NewSectionReader(dump, streamOffset, 1<<63-1-streamOffset))
	if err != nil {
		return model.Page{}, fmt.Errorf("decode page stream: %w", err)
	}
	for _, source := range pages {
		if source.ID != wantedID {
			continue
		}
		content := source.Revision.Text
		if format == "" || format == "text" {
			format, content = "text", PlainText(content)
		} else if format != "wikitext" {
			return model.Page{}, errors.New("format must be text or wikitext")
		}
		if start < 0 {
			start = 0
		}
		runes := []rune(content)
		if start > len(runes) {
			start = len(runes)
		}
		if maxChars <= 0 || maxChars > 100_000 {
			maxChars = 20_000
		}
		end := min(start+maxChars, len(runes))
		return model.Page{PageID: source.ID, RevisionID: source.Revision.ID, Title: source.Title, Timestamp: source.Revision.Timestamp, Format: format, Content: string(runes[start:end]), Truncated: end < len(runes), NextOffset: end}, nil
	}
	return model.Page{}, errors.New("page was absent from its indexed stream")
}

func parseIndexLine(line string) (int64, uint64, string, error) {
	parts := strings.SplitN(line, ":", 3)
	if len(parts) != 3 {
		return 0, 0, "", fmt.Errorf("invalid multistream index line %q", line)
	}
	offset, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || offset < 0 {
		return 0, 0, "", fmt.Errorf("invalid stream offset in %q", line)
	}
	pageID, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil || pageID == 0 {
		return 0, 0, "", fmt.Errorf("invalid page id in %q", line)
	}
	return offset, pageID, parts[2], nil
}

func readStreams(ctx context.Context, indexPath, dumpPath string) ([]stream, error) {
	f, err := os.Open(indexPath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	info, err := os.Stat(dumpPath)
	if err != nil {
		return nil, err
	}
	scanner := bufio.NewScanner(pbzip2.NewReader(ctx, f))
	scanner.Buffer(make([]byte, 64<<10), 4<<20)
	var offsets []int64
	last := int64(-1)
	for scanner.Scan() {
		offset, _, _, err := parseIndexLine(scanner.Text())
		if err != nil {
			return nil, err
		}
		if offset != last {
			offsets = append(offsets, offset)
			last = offset
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	streams := make([]stream, len(offsets))
	for i, offset := range offsets {
		end := info.Size()
		if i+1 < len(offsets) {
			end = offsets[i+1]
		}
		streams[i] = stream{Offset: offset, End: end}
	}
	return streams, nil
}

func decodeStream(r io.Reader) ([]xmlPage, error) {
	decoder := xml.NewDecoder(bzip2.NewReader(r))
	// Individual multistream blocks are XML fragments: only the first has the
	// opening mediawiki element and only the last has its closing element.
	decoder.Strict = false
	var pages []xmlPage
	for {
		token, err := decoder.Token()
		if errors.Is(err, io.EOF) {
			return pages, nil
		}
		if err != nil {
			var syntaxErr *xml.SyntaxError
			if len(pages) > 0 && errors.As(err, &syntaxErr) && (strings.Contains(syntaxErr.Msg, "unexpected end element") || strings.Contains(syntaxErr.Msg, "unexpected EOF")) {
				return pages, nil
			}
			return nil, err
		}
		start, ok := token.(xml.StartElement)
		if !ok || start.Name.Local != "page" {
			continue
		}
		var page xmlPage
		if err := decoder.DecodeElement(&page, &start); err != nil {
			return nil, err
		}
		pages = append(pages, page)
	}
}

func normalizeTitle(title string) string {
	return strings.ToLower(strings.TrimSpace(strings.ReplaceAll(title, "_", " ")))
}

func titleMapping() mapping.IndexMapping {
	m := bleve.NewIndexMapping()
	doc := bleve.NewDocumentMapping()
	storedText := bleve.NewTextFieldMapping()
	storedText.Store = true
	exact := bleve.NewTextFieldMapping()
	exact.Analyzer = keyword.Name
	exact.Store = false
	number := bleve.NewNumericFieldMapping()
	number.Store = true
	doc.AddFieldMappingsAt("title", storedText)
	doc.AddFieldMappingsAt("title_exact", exact)
	doc.AddFieldMappingsAt("page_id", number)
	doc.AddFieldMappingsAt("offset", number)
	m.DefaultMapping = doc
	return m
}

func bodyMapping() mapping.IndexMapping {
	m := bleve.NewIndexMapping()
	doc := bleve.NewDocumentMapping()
	title := bleve.NewTextFieldMapping()
	title.Store = true
	body := bleve.NewTextFieldMapping()
	body.Store = false
	number := bleve.NewNumericFieldMapping()
	number.Store = true
	doc.AddFieldMappingsAt("title", title)
	doc.AddFieldMappingsAt("body", body)
	doc.AddFieldMappingsAt("page_id", number)
	m.DefaultMapping = doc
	return m
}

var (
	commentRE  = regexp.MustCompile(`(?s)<!--.*?-->`)
	refRE      = regexp.MustCompile(`(?is)<ref\b[^>]*>.*?</ref\s*>|<ref\b[^>]*/\s*>`)
	tagRE      = regexp.MustCompile(`(?s)<[^>]+>`)
	linkRE     = regexp.MustCompile(`\[\[([^\]|]+)\|([^\]]+)\]\]|\[\[([^\]]+)\]\]`)
	externalRE = regexp.MustCompile(`\[(?:https?|ftp)://[^\s\]]+\s+([^\]]+)\]`)
)

func PlainText(source string) string {
	source = commentRE.ReplaceAllString(source, " ")
	source = refRE.ReplaceAllString(source, " ")
	source = stripBalanced(source, "{{", "}}")
	source = stripBalanced(source, "{|", "|}")
	source = linkRE.ReplaceAllStringFunc(source, func(value string) string {
		match := linkRE.FindStringSubmatch(value)
		if match[2] != "" {
			return match[2]
		}
		return match[3]
	})
	source = externalRE.ReplaceAllString(source, "$1")
	source = tagRE.ReplaceAllString(source, " ")
	source = strings.NewReplacer("'''", "", "''", "", "==", " ", "__TOC__", " ").Replace(source)
	source = html.UnescapeString(source)
	var out strings.Builder
	space := true
	for _, r := range source {
		if unicode.IsSpace(r) {
			if !space {
				out.WriteByte(' ')
				space = true
			}
			continue
		}
		out.WriteRune(r)
		space = false
	}
	return strings.TrimSpace(out.String())
}

func stripBalanced(source, open, closing string) string {
	var out strings.Builder
	depth := 0
	for i := 0; i < len(source); {
		if strings.HasPrefix(source[i:], open) {
			depth++
			i += len(open)
			continue
		}
		if depth > 0 && strings.HasPrefix(source[i:], closing) {
			depth--
			i += len(closing)
			continue
		}
		if depth == 0 {
			out.WriteByte(source[i])
		}
		i++
	}
	return out.String()
}
