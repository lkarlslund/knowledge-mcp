package store

import (
	"strings"
	"testing"

	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/model"
	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/provider"
	wikimediaprovider "github.com/lkarlslund/wikipedia-multistream-mcp/internal/provider/wikimedia"
	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/wikimedia"
)

func TestRewriteKnowledgeLinksHidesRouting(t *testing.T) {
	t.Parallel()
	registry, err := provider.NewRegistry(wikimediaprovider.New(wikimedia.NewClient(1)))
	if err != nil {
		t.Fatal(err)
	}
	backend, err := Open(t.TempDir(), registry)
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	rewritten, err := backend.rewriteKnowledgeLinks(`[Target](knowledge-read://read?dataset=enwiki&id=42 "old routing")`)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(rewritten, "dataset=") || strings.Contains(rewritten, "id=42") || !strings.Contains(rewritten, "knowledge-read://document/r_") || !strings.Contains(rewritten, "knowledge_read with ref=r_") {
		t.Fatalf("unexpected rewritten link: %s", rewritten)
	}
	incomplete := `tail knowledge-read://read?d`
	unchanged, err := backend.rewriteKnowledgeLinks(incomplete)
	if err != nil || unchanged != incomplete {
		t.Fatalf("incomplete chunk-edge link = %q, %v", unchanged, err)
	}
	page := model.Document{ID: "1", Title: "Source", Format: "markdown", Content: `[Body](knowledge-read://read?dataset=enwiki&id=2)`, References: []model.DocumentReference{{Content: `[Citation](knowledge-read://read?dataset=enwiki&id=3)`}}}
	if err := backend.decorateDocument(&page, "wikimedia", "enwiki"); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(page.Content, "dataset=") || strings.Contains(page.References[0].Content, "dataset=") {
		t.Fatalf("document routing leaked after decoration: %#v", page)
	}
}
