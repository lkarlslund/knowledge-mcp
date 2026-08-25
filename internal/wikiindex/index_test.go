package wikiindex

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/blevesearch/bleve/v2/mapping"
	dsbzip2 "github.com/dsnet/compress/bzip2"
	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/model"
)

func TestTitleAndBodyIndexes(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	partsDir := filepath.Join(dir, "parts")
	if err := os.MkdirAll(partsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	first := compressForTest(t, []byte(`<mediawiki><page><title>Alpha Page</title><ns>0</ns><id>1</id><revision><id>11</id><timestamp>2026-01-01T00:00:00Z</timestamp><text>Alpha has a [[Useful link|useful label]], [[Beta: Details#Overview|known page]], and {{template|noise}}.&lt;ref name="proof"&gt;A cited source.&lt;/ref&gt;
== Details ==
The useful redirected section.</text></revision></page><page><title>Alpha alias</title><ns>0</ns><id>3</id><redirect title="Alpha Page"/><revision><id>33</id><timestamp>2026-01-03T00:00:00Z</timestamp><text>#REDIRECT [[Alpha Page#Details]]
unique_redirect_stub_noise</text></revision></page><page><title>Alpha double alias</title><ns>0</ns><id>4</id><redirect title="Alpha alias"/><revision><id>44</id><timestamp>2026-01-04T00:00:00Z</timestamp><text>#REDIRECT [[Alpha alias]]</text></revision></page>`))
	second := compressForTest(t, []byte(`<page><title>Beta: Details</title><ns>0</ns><id>2</id><revision><id>22</id><timestamp>2026-01-02T00:00:00Z</timestamp><text>Beta contains a distinctive platypus phrase.</text></revision></page><page><title>Mette Frederiksen</title><ns>0</ns><id>5</id><revision><id>55</id><text>The Danish prime minister discussed a Trump phone call about Greenland with foreign leaders.</text></revision></page><page><title>Phone call</title><ns>0</ns><id>6</id><revision><id>66</id><text>A phone call is a connection using a telephone.</text></revision></page><page><title>Wikipedia:Greenland phone call</title><ns>4</ns><id>7</id><revision><id>77</id><text>Trump Frederiksen phone call Greenland project coordination.</text></revision></page></mediawiki>`))
	firstDumpPath := filepath.Join(partsDir, "000.dump.bz2")
	firstIndexPath := filepath.Join(partsDir, "000.index.bz2")
	secondDumpPath := filepath.Join(partsDir, "001.dump.bz2")
	secondIndexPath := filepath.Join(partsDir, "001.index.bz2")
	if err := os.WriteFile(firstDumpPath, first, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(firstIndexPath, compressForTest(t, []byte("0:1:Alpha Page\n0:3:Alpha alias\n0:4:Alpha double alias\n")), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(secondDumpPath, second, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(secondIndexPath, compressForTest(t, []byte("0:2:Beta: Details\n0:5:Mette Frederiksen\n0:6:Phone call\n0:7:Wikipedia:Greenland phone call\n")), 0o644); err != nil {
		t.Fatal(err)
	}

	parts := []Part{
		{Number: 0, DumpPath: firstDumpPath, IndexPath: firstIndexPath},
		{Number: 1, DumpPath: secondDumpPath, IndexPath: secondIndexPath},
	}
	count, err := BuildTitle(context.Background(), parts, filepath.Join(dir, TitleIndexDir), func(uint64, int64, int64) {})
	if err != nil {
		t.Fatalf("BuildTitle: %v", err)
	}
	if count != 7 {
		t.Fatalf("page count = %d, want 7", count)
	}
	titleResult, err := Search(dir, "Beta", 0, 10, false)
	if err != nil {
		t.Fatalf("title Search: %v", err)
	}
	if len(titleResult.Hits) != 1 || titleResult.Hits[0].Title != "Beta: Details" {
		t.Fatalf("unexpected title hits: %#v", titleResult.Hits)
	}
	if titleResult.SnippetsAvailable || titleResult.SnippetsComplete {
		t.Fatalf("title-only snippet capability = %#v", titleResult)
	}
	page, err := ReadPage(dir, "Beta: Details", 0, "text", 0, 1000)
	if err != nil {
		t.Fatalf("ReadPage: %v", err)
	}
	if page.PageID != 2 || page.Content != "Beta contains a distinctive platypus phrase." {
		t.Fatalf("unexpected page: %#v", page)
	}
	markdownPage, err := ReadPage(dir, "Alpha Page", 0, "", 0, 1000)
	if err != nil || markdownPage.Format != "markdown" || !strings.Contains(markdownPage.Content, "[useful label](wiki:Useful_link)") || !strings.Contains(markdownPage.Content, "**template:** noise") || len(markdownPage.References) != 1 || markdownPage.References[0].Content != "A cited source." {
		t.Fatalf("default Markdown page = %#v, %v", markdownPage, err)
	}
	pageByID, err := ReadPage(dir, "", 2, "wikitext", 0, 1000)
	if err != nil || pageByID.Title != "Beta: Details" {
		t.Fatalf("ReadPage by document ID = %#v, %v", pageByID, err)
	}
	if _, err := ReadPage(dir, "Missing page", 0, "text", 0, 1000); !errors.Is(err, ErrPageNotFound) {
		t.Fatalf("missing ReadPage error = %v, want ErrPageNotFound", err)
	}
	redirectedPage, err := ReadPage(dir, "Alpha double alias", 0, "markdown", 0, 1000)
	if err != nil {
		t.Fatalf("ReadPage redirect: %v", err)
	}
	if redirectedPage.Title != "Alpha Page" || redirectedPage.RequestedTitle != "Alpha double alias" || len(redirectedPage.RedirectChain) != 2 || redirectedPage.Section != "Details" || redirectedPage.SectionFound == nil || !*redirectedPage.SectionFound || !strings.Contains(redirectedPage.Content, "## Details") || strings.Contains(redirectedPage.Content, "Alpha has") {
		t.Fatalf("redirected page = %#v", redirectedPage)
	}
	reader, err := OpenReader(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	redirectStub, err := reader.ReadPage(context.Background(), "Alpha alias", 0, model.ReadOptions{Format: "wikitext", MaxChars: 1000}, "")
	_ = reader.Close()
	if err != nil || redirectStub.Title != "Alpha alias" || !redirectStub.Redirected || redirectStub.Section != "" || !strings.HasPrefix(redirectStub.Content, "#REDIRECT") {
		t.Fatalf("unfollowed redirect = %#v, %v", redirectStub, err)
	}

	if err := BuildBody(context.Background(), parts, filepath.Join(dir, BodyIndexDir), func(int64, int64) {}); err != nil {
		t.Fatalf("BuildBody: %v", err)
	}
	bodyResult, err := Search(dir, "platypus", 0, 10, true)
	if err != nil {
		t.Fatalf("body Search: %v", err)
	}
	if len(bodyResult.Hits) != 1 || bodyResult.Hits[0].PageID != 2 {
		t.Fatalf("unexpected body hits: %#v", bodyResult.Hits)
	}
	exactResult, err := Search(dir, "Mette Frederiksen", 0, 10, true)
	if err != nil {
		t.Fatalf("exact-title Search: %v", err)
	}
	if len(exactResult.Hits) == 0 || exactResult.Hits[0].PageID != 5 {
		t.Fatalf("exact-title hits: %#v", exactResult.Hits)
	}
	for _, hit := range exactResult.Hits {
		if math.IsNaN(hit.Score) || math.IsInf(hit.Score, 0) {
			t.Fatalf("exact-title hit has non-finite score: %#v", hit)
		}
	}
	redirectResult, err := Search(dir, "Alpha alias", 0, 10, true)
	if err != nil || len(redirectResult.Hits) == 0 || redirectResult.Hits[0].PageID != 1 || redirectResult.Hits[0].Title != "Alpha Page" || redirectResult.Hits[0].MatchedTitle != "Alpha alias" || redirectResult.Hits[0].MatchMode != "exact_redirect" {
		t.Fatalf("exact redirect search = %#v, %v", redirectResult.Hits, err)
	}
	if math.IsNaN(redirectResult.Hits[0].Score) || math.IsInf(redirectResult.Hits[0].Score, 0) || redirectResult.Hits[0].Score <= 0 || redirectResult.Hits[0].Score >= 1_000 {
		t.Fatalf("exact redirect score is not a finite hard-tier score: %#v", redirectResult.Hits)
	}
	for _, hit := range redirectResult.Hits[1:] {
		if hit.PageID == redirectResult.Hits[0].PageID {
			t.Fatalf("canonical result was not deduplicated: %#v", redirectResult.Hits)
		}
	}
	redirectNoise, err := Search(dir, "unique_redirect_stub_noise", 0, 10, true)
	if err != nil || len(redirectNoise.Hits) != 0 {
		t.Fatalf("redirect stub text was indexed: %#v, %v", redirectNoise.Hits, err)
	}
	relevance, err := Search(dir, "Trump Frederiksen phone call Greenland", 0, 10, true)
	if err != nil || len(relevance.Hits) < 2 || relevance.Hits[0].PageID != 5 || relevance.Hits[0].MatchMode != "bm25" || relevance.Hits[1].PageID != 6 || relevance.Hits[1].MatchMode != "bm25" {
		t.Fatalf("all-term relevance ordering = %#v, %v", relevance.Hits, err)
	}
	if relevance.Hits[0].Namespace != 0 || !strings.Contains(relevance.Hits[0].Snippet, "Trump phone call") {
		t.Fatalf("relevant hit metadata = %#v", relevance.Hits[0])
	}
	if !relevance.SnippetsAvailable || !relevance.SnippetsComplete {
		t.Fatalf("full-text snippet capability = %#v", relevance)
	}
	fullReader, err := OpenReader(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	paged, err := fullReader.Search(context.Background(), "phone call", model.SearchOptions{Limit: 1}, true)
	if err != nil || paged.Total != 2 || len(paged.Hits) != 1 || paged.NextOffset != 1 {
		t.Fatalf("bounded first search page = %#v, %v", paged, err)
	}
	pagedNext, err := fullReader.Search(context.Background(), "phone call", model.SearchOptions{Offset: paged.NextOffset, Limit: 1}, true)
	if err != nil || pagedNext.Total != paged.Total || len(pagedNext.Hits) != 1 || pagedNext.Hits[0].PageID == paged.Hits[0].PageID || pagedNext.NextOffset != 0 {
		t.Fatalf("bounded second search page = %#v, %v", pagedNext, err)
	}
	withProjects, err := fullReader.Search(context.Background(), "Trump Frederiksen phone call Greenland", model.SearchOptions{Limit: 10, IncludeNonArticles: true}, true)
	_ = fullReader.Close()
	if err != nil || len(withProjects.Hits) < 2 || withProjects.Hits[0].Namespace != 4 {
		t.Fatalf("non-article namespace search = %#v, %v", withProjects.Hits, err)
	}

	readReader, err := OpenReader(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = readReader.Close() }()
	linkedPage, err := readReader.ReadPage(context.Background(), "Alpha Page", 0, model.ReadOptions{Format: "markdown", LinkWiki: "enwiki", MaxChars: 1000, FollowRedirects: true}, "https://en.wikipedia.org")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`[known page](wiki-read://read?wiki=enwiki&page_id=2&section=Overview "Call wiki_read with wiki=enwiki and page_id=2 and section=Overview")`,
		`[useful label](wiki-read://read?wiki=enwiki&title=Useful+link "Call wiki_read with wiki=enwiki and title=Useful link")`,
	} {
		if !strings.Contains(linkedPage.Content, want) {
			t.Errorf("linked Markdown does not contain %q:\n%s", want, linkedPage.Content)
		}
	}
	if strings.Contains(linkedPage.Content, "https://en.wikipedia.org/wiki/") {
		t.Errorf("linked Markdown retained an online internal URL:\n%s", linkedPage.Content)
	}
	sectionPage, err := readReader.ReadPage(context.Background(), "Alpha Page", 0, model.ReadOptions{Format: "markdown", Section: "Details", MaxChars: 1000, FollowRedirects: true, IncludeOutline: true}, "https://en.wikipedia.org")
	if err != nil || sectionPage.SectionFound == nil || !*sectionPage.SectionFound || len(sectionPage.Sections) != 1 || sectionPage.Sections[0].Anchor != "Details" || strings.Contains(sectionPage.Content, "Alpha has") {
		t.Fatalf("explicit section page = %#v, %v", sectionPage, err)
	}
	missingSection, err := readReader.ReadPage(context.Background(), "Alpha Page", 0, model.ReadOptions{Format: "markdown", Section: "Missing", MaxChars: 1000, FollowRedirects: true}, "")
	if err != nil || missingSection.SectionFound == nil || *missingSection.SectionFound || missingSection.Content != "" || len(missingSection.Sections) != 1 {
		t.Fatalf("missing section page = %#v, %v", missingSection, err)
	}
	complete, err := readReader.ReadPage(context.Background(), "Alpha Page", 0, model.ReadOptions{Format: "markdown", MaxChars: 1000, FollowRedirects: true, ReferenceBudgetChars: 5, ReferenceMaxChars: 5}, "")
	if err != nil || complete.Truncated || complete.NextOffset != 0 || complete.ReturnedChars != complete.TotalChars || !complete.ReferencesTruncated || len(complete.References) != 1 || !complete.References[0].Truncated {
		t.Fatalf("complete bounded-reference page = %#v, %v", complete, err)
	}
	var reconstructed strings.Builder
	next := 0
	for {
		chunk, chunkErr := readReader.ReadPage(context.Background(), "Alpha Page", 0, model.ReadOptions{Format: "text", Offset: next, MaxChars: 24, FollowRedirects: true, AlignBoundaries: true}, "")
		if chunkErr != nil {
			t.Fatal(chunkErr)
		}
		reconstructed.WriteString(chunk.Content)
		if !chunk.Truncated {
			if chunk.NextOffset != 0 {
				t.Fatalf("complete chunk next_offset = %d", chunk.NextOffset)
			}
			break
		}
		if chunk.NextOffset <= next {
			t.Fatalf("pagination did not progress: %#v", chunk)
		}
		next = chunk.NextOffset
	}
	whole, err := readReader.ReadPage(context.Background(), "Alpha Page", 0, model.ReadOptions{Format: "text", MaxChars: 1000, FollowRedirects: true}, "")
	if err != nil || reconstructed.String() != whole.Content {
		t.Fatalf("paginated reconstruction differs: got %q want %q, %v", reconstructed.String(), whole.Content, err)
	}
}

func TestSectionExtractionFallsBackWhenFragmentIsMissing(t *testing.T) {
	t.Parallel()
	content := "Lead\n\n## Existing\n\nBody"
	got, found := extractMarkdownSection(content, "Missing")
	if found || got != content {
		t.Fatalf("extractMarkdownSection missing = %q, %t", got, found)
	}
	got, found = extractMarkdownSection(content, "Existing")
	if !found || got != "## Existing\n\nBody" {
		t.Fatalf("extractMarkdownSection existing = %q, %t", got, found)
	}
}

func TestLeanMappings(t *testing.T) {
	t.Parallel()
	for name, indexMapping := range map[string]mapping.IndexMapping{"title": titleMapping(), "body": bodyMapping()} {
		impl, ok := indexMapping.(*mapping.IndexMappingImpl)
		if !ok {
			t.Fatalf("%s mapping has type %T", name, indexMapping)
		}
		if impl.IndexDynamic || impl.StoreDynamic || impl.DocValuesDynamic {
			t.Errorf("%s mapping retains dynamic fields", name)
		}
		all, ok := impl.DefaultMapping.Properties["_all"]
		if !ok || all.Enabled {
			t.Errorf("%s mapping does not explicitly disable _all", name)
		}
		for property, document := range impl.DefaultMapping.Properties {
			for _, field := range document.Fields {
				if field.IncludeTermVectors || field.IncludeInAll || field.DocValues {
					t.Errorf("%s.%s retains redundant index data: %#v", name, property, field)
				}
				if property == "title_exact" && field.SkipFreqNorm {
					t.Errorf("%s.%s skips frequency normalization required for scoring", name, property)
				}
			}
		}
	}
}

func TestPlainText(t *testing.T) {
	t.Parallel()
	tests := map[string]string{
		`== Heading == <!-- hidden --> Text [[Target|label]] <ref>citation</ref> {{Infobox|x}} &amp; [https://example.test external]`: "Heading Text label & external",
		`Before {{outer|{{inner}}}} after {| class="wikitable" | hidden |} done`:                                                      "Before after done",
		`[[Target]] and [[Target|shown]] with ''italics'' and '''bold'''`:                                                             "Target and shown with italics and bold",
		`A<REF name="x">hidden</REF>B<br/>C &lt; D __TOC__`:                                                                           "A B C < D",
	}
	for input, want := range tests {
		if got := PlainText(input); got != want {
			t.Errorf("PlainText(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestMarkdownPreservesAgentUsefulStructure(t *testing.T) {
	t.Parallel()
	source := `== History ==
Text with [[Target page|a linked item]] and [https://example.test external source].<ref name="source">{{Cite web|title=Primary source|url=https://source.test/report|website=Example|date=2026}}</ref>

* First item
* Second item

{| class="wikitable"
|+ Population
! Year !! People
|-
| 2020 || 10
|-
| 2021 || 12
|}

{{Infobox place
| name = Test City
| country = [[Denmark]]
}}

== References ==
{{reflist}}`

	got := Markdown(source, "https://en.wikipedia.org")
	for _, want := range []string{
		"## History",
		"[a linked item](https://en.wikipedia.org/wiki/Target_page)",
		"[external source](https://example.test)",
		"[^1]",
		"- First item",
		"**Population**",
		"| **Year** | **People** |",
		"| 2020 | 10 |",
		"| Infobox place | |",
		"[Denmark](https://en.wikipedia.org/wiki/Denmark)",
		"## References",
		"[^1]: [Primary source](https://source.test/report). Example. 2026",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("Markdown output does not contain %q:\n%s", want, got)
		}
	}
	for _, unwanted := range []string{"{{", "[[", "<ref"} {
		if strings.Contains(got, unwanted) {
			t.Errorf("Markdown output retains %q:\n%s", unwanted, got)
		}
	}
}

func TestMarkdownReferencesSurvivePagination(t *testing.T) {
	t.Parallel()
	document := RenderMarkdown(`Lead sentence.<ref name="source">{{Cite web|title=Primary source|url=https://source.test/report}}</ref>`+strings.Repeat(" More text.", 100), "https://en.wikipedia.org")
	if len(document.References) != 1 || document.References[0].Name != "source" || !strings.Contains(document.References[0].Content, "[Primary source](https://source.test/report)") {
		t.Fatalf("references = %#v", document.References)
	}
	references := referencedMarkdownDefinitions(document.Content[:50], document.References)
	if len(references) != 1 || references[0].ID != 1 {
		t.Fatalf("paginated references = %#v", references)
	}
}

func TestQuerySnippetPrefersPassageContainingMoreQueryTerms(t *testing.T) {
	t.Parallel()
	content := "Trump appears alone near the beginning. " + strings.Repeat("Unrelated background material. ", 30) + "Frederiksen discussed a Trump phone call concerning Greenland with allies. " + strings.Repeat("Trailing material. ", 30)
	snippet := querySnippet(content, "Trump Frederiksen phone call Greenland", 220)
	for _, term := range []string{"Trump", "Frederiksen", "phone call", "Greenland"} {
		if !strings.Contains(snippet, term) {
			t.Fatalf("snippet %q does not contain %q", snippet, term)
		}
	}
}

func TestMarkdownMediaLinkUsesCaption(t *testing.T) {
	t.Parallel()
	tests := map[string]string{
		`[[File:Example.svg|thumb|20px|A useful diagram]]`: `[Image: A useful diagram](https://en.wikipedia.org/wiki/File:Example.svg)`,
		`[[File:Square.jpg|thumb|[[Kultorvet]] in 2016]]`:  `[Image: Kultorvet in 2016](https://en.wikipedia.org/wiki/File:Square.jpg)`,
	}
	for source, want := range tests {
		if got := Markdown(source, "https://en.wikipedia.org"); got != want {
			t.Errorf("Markdown media link = %q, want %q", got, want)
		}
	}
}

func BenchmarkPlainText(b *testing.B) {
	source := strings.Repeat(`== Heading == Some [[Target|linked text]] with {{template|nested={{value}}}}, <ref>citation</ref> and [https://example.test an external link]. &amp; `, 200)
	b.ReportAllocs()
	for b.Loop() {
		_ = PlainText(source)
	}
}

func TestReadPageStopsAtNextMultistreamOffset(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	partsDir := filepath.Join(dir, "parts")
	if err := os.MkdirAll(partsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	first := compressForTest(t, []byte(`<page><title>First</title><id>1</id><revision><id>11</id><text>bounded stream</text></revision></page>`))
	dumpPath := filepath.Join(partsDir, "000.dump.bz2")
	if err := os.WriteFile(dumpPath, append(first, []byte("BZnot-a-valid-second-stream")...), 0o644); err != nil {
		t.Fatal(err)
	}
	indexPath := filepath.Join(partsDir, "000.index.bz2")
	indexSource := fmt.Sprintf("0:1:First\n%d:2:Broken second stream\n", len(first))
	if err := os.WriteFile(indexPath, compressForTest(t, []byte(indexSource)), 0o644); err != nil {
		t.Fatal(err)
	}
	parts := []Part{{Number: 0, DumpPath: dumpPath, IndexPath: indexPath}}
	if _, err := BuildTitle(context.Background(), parts, filepath.Join(dir, TitleIndexDir), func(uint64, int64, int64) {}); err != nil {
		t.Fatal(err)
	}
	page, err := ReadPage(dir, "", 1, "text", 0, 100)
	if err != nil || page.Content != "bounded stream" {
		t.Fatalf("bounded ReadPage = %#v, %v", page, err)
	}
}

func TestBodyIndexResumesFromStreamCheckpoint(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dumpPath := filepath.Join(dir, "dump.bz2")
	indexPath := filepath.Join(dir, "index.bz2")
	var dump, sourceIndex bytes.Buffer
	const streamCount = bodyBatchStreams + 16
	for pageID := 1; pageID <= streamCount; pageID++ {
		offset := dump.Len()
		page := fmt.Sprintf(`<page><title>Page %d</title><id>%d</id><revision><id>%d</id><text>resume token %d</text></revision></page>`, pageID, pageID, pageID+1000, pageID)
		dump.Write(compressForTest(t, []byte(page)))
		fmt.Fprintf(&sourceIndex, "%d:%d:Page %d\n", offset, pageID, pageID)
	}
	if err := os.WriteFile(dumpPath, dump.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(indexPath, compressForTest(t, sourceIndex.Bytes()), 0o644); err != nil {
		t.Fatal(err)
	}

	destination := filepath.Join(dir, BodyIndexDir)
	ctx, cancel := context.WithCancel(context.Background())
	err := BuildBody(ctx, []Part{{DumpPath: dumpPath, IndexPath: indexPath}}, destination, func(done, _ int64) {
		var saved bodyCheckpoint
		data, readErr := os.ReadFile(destination + ".checkpoint.json")
		if readErr == nil && json.Unmarshal(data, &saved) == nil && saved.Done >= bodyShardStreams {
			cancel()
		}
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("first BuildBody error = %v, want context.Canceled", err)
	}
	checkpoint, ok := loadBodyCheckpoint(destination+".checkpoint.json", destination, mustReadStreams(t, Part{DumpPath: dumpPath, IndexPath: indexPath}))
	if !ok || checkpoint.Done < bodyShardStreams || checkpoint.Done >= streamCount {
		t.Fatalf("checkpoint = %#v, valid %v", checkpoint, ok)
	}

	firstProgress := int64(-1)
	if err := BuildBody(context.Background(), []Part{{DumpPath: dumpPath, IndexPath: indexPath}}, destination, func(done, _ int64) {
		if firstProgress < 0 {
			firstProgress = done
		}
	}); err != nil {
		t.Fatalf("resumed BuildBody: %v", err)
	}
	if firstProgress != checkpoint.Done {
		t.Fatalf("resumed progress = %d, want saved checkpoint %d", firstProgress, checkpoint.Done)
	}
	if _, err := os.Stat(destination + ".checkpoint.json"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("completed checkpoint still exists: %v", err)
	}
	for shard := range bodyShardCount {
		if info, err := os.Stat(bodyShardPath(destination, shard)); err != nil || !info.IsDir() {
			t.Fatalf("body shard %d missing: %v", shard, err)
		}
	}

	for _, pageID := range []int{1, streamCount} {
		result, err := Search(dir, fmt.Sprintf("resume token %d", pageID), 0, 10, true)
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Hits) == 0 || result.Hits[0].PageID != uint64(pageID) {
			t.Fatalf("resumed index missing page %d: %#v", pageID, result.Hits)
		}
	}
}

func TestTitleIndexResumesFromLineCheckpoint(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	indexPath := filepath.Join(dir, "index.bz2")
	var sourceIndex bytes.Buffer
	const pageCount = titleBatchDocs + 100
	for pageID := 1; pageID <= pageCount; pageID++ {
		fmt.Fprintf(&sourceIndex, "%d:%d:Checkpoint Page %d\n", pageID, pageID, pageID)
	}
	if err := os.WriteFile(indexPath, compressForTest(t, sourceIndex.Bytes()), 0o644); err != nil {
		t.Fatal(err)
	}
	parts := []Part{{IndexPath: indexPath}}
	destination := filepath.Join(dir, TitleIndexDir)

	ctx, cancel := context.WithCancel(context.Background())
	_, err := BuildTitle(ctx, parts, destination, func(pages uint64, _, _ int64) {
		if pages >= titleBatchDocs {
			cancel()
		}
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("first BuildTitle error = %v, want context.Canceled", err)
	}
	checkpoint, _, ok := loadTitleCheckpoint(destination+".checkpoint.json", destination, parts)
	if !ok || checkpoint.Pages != titleBatchDocs || checkpoint.Parts[0].Lines != titleBatchDocs || checkpoint.Parts[0].Complete {
		t.Fatalf("checkpoint = %#v, valid %v", checkpoint, ok)
	}

	firstProgress := uint64(0)
	count, err := BuildTitle(context.Background(), parts, destination, func(pages uint64, _, _ int64) {
		if firstProgress == 0 {
			firstProgress = pages
		}
	})
	if err != nil {
		t.Fatalf("resumed BuildTitle: %v", err)
	}
	if firstProgress != checkpoint.Pages || count != pageCount {
		t.Fatalf("resumed pages = %d, final count = %d; want %d and %d", firstProgress, count, checkpoint.Pages, pageCount)
	}
	if _, err := os.Stat(destination + ".checkpoint.json"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("completed checkpoint still exists: %v", err)
	}
	result, err := Search(dir, fmt.Sprintf("Checkpoint Page %d", pageCount), 0, 10, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) == 0 || result.Hits[0].PageID != pageCount {
		t.Fatalf("resumed title index missing final page: %#v", result.Hits)
	}
}

func TestParseIndexLineWithColonInTitle(t *testing.T) {
	t.Parallel()
	offset, id, title, err := parseIndexLine("123:456:Category:Example")
	if err != nil {
		t.Fatal(err)
	}
	if offset != 123 || id != 456 || title != "Category:Example" {
		t.Fatalf("got %d %d %q", offset, id, title)
	}
}

func compressForTest(t *testing.T, data []byte) []byte {
	t.Helper()
	var out bytes.Buffer
	w, err := dsbzip2.NewWriter(&out, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func mustReadStreams(t *testing.T, part Part) []stream {
	t.Helper()
	streams, err := readStreams(context.Background(), part)
	if err != nil {
		t.Fatal(err)
	}
	for i := range streams {
		streams[i].Index = i
	}
	return streams
}
