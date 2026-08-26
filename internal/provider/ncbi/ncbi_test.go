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
	"testing"

	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/model"
	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/provider"
)

func TestPubMedLifecycle(t *testing.T) {
	t.Parallel()
	xml := `<PubmedArticleSet><PubmedArticle><MedlineCitation><PMID>123</PMID><Article><ArticleTitle><i>Useful</i> medicine</ArticleTitle><Abstract><AbstractText Label="BACKGROUND"><b>Evidence</b> text.</AbstractText></Abstract><Journal><Title>Medical Journal</Title></Journal><AuthorList><Author><LastName>Doe</LastName><ForeName>Jane</ForeName></Author></AuthorList><PublicationTypeList><PublicationType>Review</PublicationType></PublicationTypeList><Language>eng</Language></Article><MeshHeadingList><MeshHeading><DescriptorName>Evidence-Based Medicine</DescriptorName></MeshHeading></MeshHeadingList></MedlineCitation><PubmedData><ArticleIdList><ArticleId IdType="doi">10.1/example</ArticleId></ArticleIdList></PubmedData></PubmedArticle></PubmedArticleSet>`
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
