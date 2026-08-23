package wikimedia

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestCatalogMetadataAndDownload(t *testing.T) {
	t.Parallel()
	payload := []byte("compressed fixture")
	hash := sha1.Sum(payload)
	sha := hex.EncodeToString(hash[:])
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/backup-index-bydb.html":
			_, _ = fmt.Fprint(w, `<li>2026-08-04 00:00:00 <a href="testwiki/20260801">testwiki</a>: <span class='done'>Dump complete</span></li>`)
		case "/testwiki/20260801/dumpstatus.json":
			_, _ = fmt.Fprintf(w, `{"jobs":{"articlesmultistreamdump":{"status":"done","files":{"testwiki-20260801-pages-articles-multistream.xml.bz2":{"size":%d,"url":"/files/dump","sha1":"%s"},"testwiki-20260801-pages-articles-multistream-index.txt.bz2":{"size":%d,"url":"/files/index","sha1":"%s"}}}}}`, len(payload), sha, len(payload), sha)
		case "/files/dump", "/files/index":
			if r.Header.Get("Range") == "bytes=4-" {
				w.Header().Set("Content-Range", fmt.Sprintf("bytes 4-%d/%d", len(payload)-1, len(payload)))
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write(payload[4:])
				return
			}
			_, _ = w.Write(payload)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := NewClientWithBaseURL(server.URL)
	result, err := client.ListAvailable(context.Background(), "test", 0, 20, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Wikis) != 1 || !result.Wikis[0].Available || result.Wikis[0].DumpSHA1 != sha {
		t.Fatalf("unexpected catalog: %#v", result)
	}
	destination := filepath.Join(t.TempDir(), "partial")
	if err := os.WriteFile(destination, payload[:4], 0o644); err != nil {
		t.Fatal(err)
	}
	metadata, err := client.LatestMetadata(context.Background(), "testwiki")
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Download(context.Background(), metadata.Parts[0].Dump, destination, func(int64, int64, float64) {}); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(destination)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(payload) {
		t.Fatalf("download = %q, want %q", got, payload)
	}
}

func TestValidWikiName(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"enwiki", "enwiktionary", "commonswiki", "be_x_oldwiki"} {
		if !ValidWikiName(name) {
			t.Errorf("ValidWikiName(%q) = false", name)
		}
	}
	for _, name := range []string{"../enwiki", "ENWIKI", "example.com", "wiki"} {
		if ValidWikiName(name) {
			t.Errorf("ValidWikiName(%q) = true", name)
		}
	}
}

func TestMultipartMetadataIsOrderedAndSummed(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/backup-index-bydb.html":
			_, _ = fmt.Fprint(w, `<li>2026-08-04 00:00:00 <a href="enwiki/20260801">enwiki</a>: <span class='done'>Dump complete</span></li>`)
		case "/enwiki/20260801/dumpstatus.json":
			_, _ = fmt.Fprint(w, `{"jobs":{"articlesmultistreamdump":{"status":"done","files":{`+
				`"enwiki-20260801-pages-articles-multistream2.xml-p20p29.bz2":{"size":200,"url":"/part2","sha1":"dump2"},`+
				`"enwiki-20260801-pages-articles-multistream-index2.txt-p20p29.bz2":{"size":20,"url":"/index2","sha1":"index2"},`+
				`"enwiki-20260801-pages-articles-multistream1.xml-p1p19.bz2":{"size":100,"url":"/part1","sha1":"dump1"},`+
				`"enwiki-20260801-pages-articles-multistream-index1.txt-p1p19.bz2":{"size":10,"url":"/index1","sha1":"index1"}`+
				`}}}}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := NewClientWithBaseURL(server.URL)
	result, err := client.ListAvailable(context.Background(), "enwiki", 0, 20, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Wikis) != 1 {
		t.Fatalf("wikis = %#v", result.Wikis)
	}
	wiki := result.Wikis[0]
	if !wiki.Available || wiki.PartCount != 2 || wiki.DumpSize != 300 || wiki.IndexSize != 30 || wiki.Fingerprint == "" {
		t.Fatalf("unexpected multipart wiki: %#v", wiki)
	}
	metadata, err := client.LatestMetadata(context.Background(), "enwiki")
	if err != nil {
		t.Fatal(err)
	}
	if metadata.Parts[0].Key != "1/p1p19" || metadata.Parts[1].Key != "2/p20p29" {
		t.Fatalf("parts are not ordered: %#v", metadata.Parts)
	}
}
