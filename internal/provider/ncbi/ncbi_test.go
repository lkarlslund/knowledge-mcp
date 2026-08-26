package ncbi

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/knowledgeindex"
	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/model"
	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/provider"
)

func TestPubMedLifecycle(t *testing.T) {
	t.Parallel()
	xml := `<PubmedArticleSet><PubmedArticle><MedlineCitation><PMID>123</PMID><DateRevised><Year>2026</Year><Month>08</Month><Day>20</Day></DateRevised><Article><ArticleTitle><i>Useful</i> medicine</ArticleTitle><ArticleDate><Year>2024</Year><Month>03</Month><Day>12</Day></ArticleDate><Abstract><AbstractText Label="BACKGROUND"><b>Evidence</b> text.</AbstractText></Abstract><Journal><Title>Medical Journal</Title></Journal><AuthorList><Author><LastName>Doe</LastName><ForeName>Jane</ForeName></Author></AuthorList><PublicationTypeList><PublicationType>Review</PublicationType></PublicationTypeList><Language>eng</Language></Article><MeshHeadingList><MeshHeading><DescriptorName>Evidence-Based Medicine</DescriptorName></MeshHeading></MeshHeadingList></MedlineCitation><PubmedData><History><PubMedPubDate PubStatus="entrez"><Year>2024</Year><Month>04</Month><Day>01</Day></PubMedPubDate></History><ArticleIdList><ArticleId IdType="doi">10.1/example</ArticleId></ArticleIdList></PubmedData></PubmedArticle></PubmedArticleSet>`
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	_, _ = writer.Write([]byte(xml))
	_ = writer.Close()
	sum := md5.Sum(compressed.Bytes())
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/":
			_, _ = fmt.Fprintf(response, `<a href="pubmed26n0001.xml.gz">pubmed26n0001.xml.gz</a> 2026-01-29 14:48 %d`, compressed.Len())
		case "/pubmed26n0001.xml.gz.md5":
			_, _ = fmt.Fprintf(response, "MD5(pubmed26n0001.xml.gz)= %s", hex.EncodeToString(sum[:]))
		case "/pubmed26n0001.xml.gz":
			_, _ = response.Write(compressed.Bytes())
		default:
			http.NotFound(response, request)
		}
	}))
	defer server.Close()

	backend := NewWithBaseURL(server.URL)
	items, err := backend.Discover(context.Background(), "pubmed", false)
	if err != nil || len(items) != 1 {
		t.Fatalf("discover: items=%d err=%v", len(items), err)
	}
	release, err := backend.Latest(context.Background(), datasetID, variantID)
	if err != nil {
		t.Fatal(err)
	}
	stage := t.TempDir()
	manifest, err := backend.Acquire(context.Background(), datasetID, variantID, release, stage, "", func(string, int64, int64, string, float64, string) {})
	if err != nil {
		t.Fatal(err)
	}
	if manifest.RawSize != int64(compressed.Len()) {
		t.Fatalf("raw bytes=%d", manifest.RawSize)
	}
	corpus, err := backend.OpenCorpus(stage, manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = corpus.Close() }()
	var titleRecord provider.Record
	if err := corpus.ScanTitles(context.Background(), "", provider.ScanOptions{}, func(record provider.Record, _ provider.ScanPosition) error {
		titleRecord = record
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var indexed provider.Record
	var progress provider.ScanPosition
	if err := corpus.ScanBodies(context.Background(), "", provider.ScanOptions{}, func(record provider.Record, position provider.ScanPosition) error {
		indexed, progress = record, position
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if indexed.ID != "123" || !strings.Contains(indexed.Body, "Evidence text") || !strings.Contains(strings.Join(indexed.Keywords, " "), "Evidence-Based Medicine") {
		t.Fatalf("record=%+v", indexed)
	}
	if indexed.Temporal.PublishedAt == nil || indexed.Temporal.PublishedAt.Format("2006-01-02") != "2024-03-12" || indexed.Temporal.ModifiedAt == nil || indexed.Temporal.CreatedAt == nil {
		t.Fatalf("temporal metadata=%+v", indexed.Temporal)
	}
	if titleRecord.ID != indexed.ID || titleRecord.Title != indexed.Title || strings.Join(titleRecord.Identifiers, "|") != strings.Join(indexed.Identifiers, "|") || strings.Join(titleRecord.Keywords, "|") != strings.Join(indexed.Keywords, "|") {
		t.Fatalf("title record differs from body record\n title: %+v\n  body: %+v", titleRecord, indexed)
	}
	if progress.Completed != 1 || progress.Total != pubmedRecordsPerPart || progress.Units != "documents" {
		t.Fatalf("progress=%+v", progress)
	}
	document, err := corpus.Read(context.Background(), indexed, model.ReadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if document.Format != "markdown" || !strings.Contains(document.Content, "# Useful medicine") {
		t.Fatalf("document=%+v", document)
	}
	if indexed.Body != document.Content {
		t.Fatalf("SAX body differs from document decoder\n SAX: %q\n full: %q", indexed.Body, document.Content)
	}
	if _, err := os.Stat(filepath.Join(stage, "raw", "pubmed26n0001.xml.gz")); err != nil {
		t.Fatal(err)
	}
}

func TestPubMedDailyUpdatesAndDeletesFollowBaseline(t *testing.T) {
	t.Parallel()
	makePart := func(source string) ([]byte, string) {
		var compressed bytes.Buffer
		writer := gzip.NewWriter(&compressed)
		_, _ = writer.Write([]byte(source))
		_ = writer.Close()
		sum := md5.Sum(compressed.Bytes())
		return compressed.Bytes(), hex.EncodeToString(sum[:])
	}
	baseline, baselineMD5 := makePart(`<PubmedArticleSet><PubmedArticle><MedlineCitation><PMID>1</PMID><Article><ArticleTitle>Original record</ArticleTitle></Article></MedlineCitation></PubmedArticle></PubmedArticleSet>`)
	update, updateMD5 := makePart(`<PubmedArticleSet><PubmedArticle><MedlineCitation><PMID>1</PMID><Article><ArticleTitle>Revised record</ArticleTitle></Article></MedlineCitation></PubmedArticle><PubmedArticle><MedlineCitation><PMID>2</PMID><Article><ArticleTitle>Temporary record</ArticleTitle></Article></MedlineCitation></PubmedArticle><DeleteCitation><PMID>2</PMID></DeleteCitation></PubmedArticleSet>`)
	var updateAvailable atomic.Bool
	var baselineDownloads atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/baseline/":
			_, _ = fmt.Fprintf(response, `<a href="pubmed26n0001.xml.gz">pubmed26n0001.xml.gz</a> 2026-01-29 14:48 %d`, len(baseline))
		case "/updates/":
			if updateAvailable.Load() {
				_, _ = fmt.Fprintf(response, `<a href="pubmed26n1335.xml.gz">pubmed26n1335.xml.gz</a> 2026-01-30 14:02 %d`, len(update))
			}
		case "/baseline/pubmed26n0001.xml.gz.md5":
			_, _ = fmt.Fprintf(response, "MD5= %s", baselineMD5)
		case "/baseline/pubmed26n0001.xml.gz":
			baselineDownloads.Add(1)
			_, _ = response.Write(baseline)
		case "/updates/pubmed26n1335.xml.gz.md5":
			_, _ = fmt.Fprintf(response, "MD5= %s", updateMD5)
		case "/updates/pubmed26n1335.xml.gz":
			_, _ = response.Write(update)
		default:
			http.NotFound(response, request)
		}
	}))
	defer server.Close()
	backend := NewWithURLs(server.URL+"/baseline", server.URL+"/updates")
	initial, err := backend.Latest(context.Background(), datasetID, variantID)
	if err != nil {
		t.Fatal(err)
	}
	current := t.TempDir()
	if _, err := backend.Acquire(context.Background(), datasetID, variantID, initial, current, "", func(string, int64, int64, string, float64, string) {}); err != nil {
		t.Fatal(err)
	}
	updateAvailable.Store(true)
	latest, err := backend.Latest(context.Background(), datasetID, variantID)
	if err != nil {
		t.Fatal(err)
	}
	stage := t.TempDir()
	manifest, err := backend.Acquire(context.Background(), datasetID, variantID, latest, stage, current, func(string, int64, int64, string, float64, string) {})
	if err != nil {
		t.Fatal(err)
	}
	if baselineDownloads.Load() != 1 {
		t.Fatalf("baseline downloaded %d times, want one", baselineDownloads.Load())
	}
	corpus, err := backend.OpenCorpus(stage, manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = corpus.Close() }()
	var records []provider.Record
	if err := corpus.ScanTitles(context.Background(), "", provider.ScanOptions{}, func(record provider.Record, _ provider.ScanPosition) error {
		records = append(records, record)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(records) != 4 || records[0].Title != "Original record" || records[1].Title != "Revised record" || records[3].ID != "2" || !records[3].Deleted {
		t.Fatalf("ordered update records=%+v", records)
	}
	documents, err := knowledgeindex.BuildTitle(context.Background(), t.TempDir(), latest.Fingerprint, corpus, provider.ScanOptions{}, func(uint64, int64, int64) {})
	if err != nil {
		t.Fatal(err)
	}
	if documents != 1 {
		t.Fatalf("documents after revision and deletion=%d, want 1", documents)
	}
}
