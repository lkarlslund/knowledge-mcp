package kiwix

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/model"
	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/provider"
)

func TestNativeZIMCorpusAndMarkdownLinks(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "archive.zim")
	writeTestZIM(t, path, "article.html", "Example", `<h1>Example</h1><p>Hello <a href="next.html">next page</a>.</p>`)
	archive, err := openZIM(path)
	if err != nil {
		t.Fatal(err)
	}
	corpus := &zimCorpus{archive: archive, dataset: "kiwix-test", sourceURL: "https://example.test/content/test"}
	defer func() { _ = corpus.Close() }()
	var record provider.Record
	if err := corpus.ScanBodies(context.Background(), "", provider.ScanOptions{}, func(candidate provider.Record, position provider.ScanPosition) error {
		record = candidate
		if position.Cursor != "1" || position.Completed != 1 || position.Total != 1 || !position.Boundary {
			t.Fatalf("position = %#v", position)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if record.Title != "Example" || !strings.Contains(record.Body, "# Example") || !strings.Contains(record.Body, "knowledge-read://read?dataset=kiwix-test&id=") {
		t.Fatalf("record = %#v", record)
	}
	document, err := corpus.Read(context.Background(), record, model.ReadOptions{Format: "markdown", MaxChars: 10_000})
	if err != nil {
		t.Fatal(err)
	}
	if document.ID != record.ID || document.Title != "Example" || document.Truncated {
		t.Fatalf("document = %#v", document)
	}
}

func TestLanguageMetadataIsCanonicalAndCompact(t *testing.T) {
	t.Parallel()
	languages := languageMetadataList("eng,en,fra,deu,fr")
	want := []model.Language{
		{Code: "en", Name: "English", LocalName: "English"},
		{Code: "fr", Name: "French", LocalName: "French"},
		{Code: "de", Name: "German", LocalName: "German"},
	}
	if len(languages) != len(want) {
		t.Fatalf("languages = %#v, want %#v", languages, want)
	}
	for index := range want {
		if languages[index] != want[index] {
			t.Fatalf("languages[%d] = %#v, want %#v", index, languages[index], want[index])
		}
	}
	summary := languageSummary(languages)
	if summary.Code != "mul" || summary.Name != "Multilingual (3 languages)" {
		t.Fatalf("summary = %#v", summary)
	}
}

func TestOfficialSmallKiwixArchive(t *testing.T) {
	if os.Getenv("KIWIX_INTEGRATION") == "" {
		t.Skip("set KIWIX_INTEGRATION=1 to exercise the official catalog and a small archive")
	}
	ctx := context.Background()
	backend := New()
	entries, err := backend.loadCatalog(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := provider.NewRegistry(backend)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Discover(ctx, "", false); err != nil {
		t.Fatalf("official catalog violates provider contract: %v", err)
	}
	var selected catalogEntry
	for _, entry := range entries {
		if entry.Name == "devdocs_en_lit" {
			selected = entry
			break
		}
	}
	if selected.Name == "" {
		t.Fatal("small DevDocs Lit archive is absent from the official catalog")
	}
	collection := collectionID(selected.Name)
	release, err := backend.Latest(ctx, collection, variantID(selected))
	if err != nil {
		t.Fatal(err)
	}
	stage := t.TempDir()
	manifest, err := backend.Acquire(ctx, collection, variantID(selected), release, stage, "", func(string, int64, int64, string, float64, string) {})
	if err != nil {
		t.Fatal(err)
	}
	corpus, err := backend.OpenCorpus(stage, manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = corpus.Close() }()
	var documents int
	err = corpus.ScanBodies(ctx, "", provider.ScanOptions{}, func(record provider.Record, _ provider.ScanPosition) error {
		if record.Body != "" {
			documents++
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if documents == 0 {
		t.Fatal("official archive exposed no readable documents")
	}
}

func writeTestZIM(t *testing.T, destination, entryPath, title, content string) {
	t.Helper()
	mimes := []byte("text/html\x00\x00")
	pathPointerPosition := uint64(zimHeaderBytes + len(mimes))
	titlePointerPosition := pathPointerPosition + 8
	clusterPointerPosition := titlePointerPosition + 4
	direntPosition := clusterPointerPosition + 8
	dirent := make([]byte, 16)
	binary.LittleEndian.PutUint16(dirent, 0)
	dirent[3] = 'C'
	dirent = append(dirent, entryPath...)
	dirent = append(dirent, 0)
	dirent = append(dirent, title...)
	dirent = append(dirent, 0)
	clusterPosition := direntPosition + uint64(len(dirent))
	cluster := []byte{1}
	offsets := make([]byte, 8)
	binary.LittleEndian.PutUint32(offsets, 8)
	binary.LittleEndian.PutUint32(offsets[4:], uint32(8+len(content)))
	cluster = append(cluster, offsets...)
	cluster = append(cluster, content...)
	checksumPosition := clusterPosition + uint64(len(cluster))
	header := make([]byte, zimHeaderBytes)
	binary.LittleEndian.PutUint32(header, zimMagic)
	binary.LittleEndian.PutUint16(header[4:], 6)
	binary.LittleEndian.PutUint16(header[6:], 1)
	binary.LittleEndian.PutUint32(header[24:], 1)
	binary.LittleEndian.PutUint32(header[28:], 1)
	binary.LittleEndian.PutUint64(header[32:], pathPointerPosition)
	binary.LittleEndian.PutUint64(header[40:], titlePointerPosition)
	binary.LittleEndian.PutUint64(header[48:], clusterPointerPosition)
	binary.LittleEndian.PutUint64(header[56:], zimHeaderBytes)
	binary.LittleEndian.PutUint32(header[64:], ^uint32(0))
	binary.LittleEndian.PutUint32(header[68:], ^uint32(0))
	binary.LittleEndian.PutUint64(header[72:], checksumPosition)
	data := append(header, mimes...)
	pointer := make([]byte, 8)
	binary.LittleEndian.PutUint64(pointer, direntPosition)
	data = append(data, pointer...)
	data = append(data, 0, 0, 0, 0)
	binary.LittleEndian.PutUint64(pointer, clusterPosition)
	data = append(data, pointer...)
	data = append(data, dirent...)
	data = append(data, cluster...)
	data = append(data, make([]byte, 16)...)
	if err := os.WriteFile(destination, data, 0o644); err != nil {
		t.Fatal(err)
	}
}
