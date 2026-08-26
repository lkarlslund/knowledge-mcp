package store

import (
	"context"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"unicode"

	"github.com/lkarlslund/knowledge-mcp/internal/model"
	documentrefs "github.com/lkarlslund/knowledge-mcp/internal/references"
)

var legacyKnowledgeLinkPattern = regexp.MustCompile(`knowledge-read://read\?[^)\s"]+(?:\s+"[^"]*")?`)

func normalizeReferenceTitle(value string) string {
	var result strings.Builder
	space := true
	for _, character := range strings.ToLower(value) {
		if unicode.IsLetter(character) || unicode.IsNumber(character) {
			result.WriteRune(character)
			space = false
		} else if !space {
			result.WriteByte(' ')
			space = true
		}
	}
	return strings.TrimSpace(result.String())
}

func (s *Store) decorateSearchHit(hit *model.SearchHit) error {
	ref, err := s.references.Mint(documentrefs.Route{Provider: hit.Provider, Dataset: hit.Dataset, ID: hit.ID, Title: hit.Title})
	if err != nil {
		return err
	}
	hit.Ref = ref
	return nil
}

func (s *Store) decorateDocument(page *model.Document, providerID, dataset string) error {
	route := documentrefs.Route{Provider: providerID, Dataset: dataset, ID: page.ID}
	if route.ID == "" {
		route.Title = page.Title
	}
	ref, err := s.references.Mint(route)
	if err != nil {
		return err
	}
	page.Ref = ref
	for index := range page.Relationships {
		if page.Relationships[index].ID == "" {
			continue
		}
		relationRef, mintErr := s.references.Mint(documentrefs.Route{Provider: providerID, Dataset: dataset, ID: page.Relationships[index].ID})
		if mintErr != nil {
			return mintErr
		}
		page.Relationships[index].Ref = relationRef
	}
	for index := range page.RedirectChain {
		if page.RedirectChain[index].FromID == "" {
			continue
		}
		hopRef, mintErr := s.references.Mint(documentrefs.Route{Provider: providerID, Dataset: dataset, ID: page.RedirectChain[index].FromID})
		if mintErr != nil {
			return mintErr
		}
		page.RedirectChain[index].Ref = hopRef
	}
	if page.Format == "markdown" {
		content, rewriteErr := s.rewriteKnowledgeLinks(page.Content)
		if rewriteErr != nil {
			return rewriteErr
		}
		page.Content = content
		for index := range page.References {
			referenceContent, rewriteErr := s.rewriteKnowledgeLinks(page.References[index].Content)
			if rewriteErr != nil {
				return rewriteErr
			}
			page.References[index].Content = referenceContent
		}
	}
	return nil
}

func (s *Store) rewriteKnowledgeLinks(content string) (string, error) {
	matches := legacyKnowledgeLinkPattern.FindAllStringIndex(content, -1)
	if len(matches) == 0 {
		return content, nil
	}
	var rewritten strings.Builder
	rewritten.Grow(len(content))
	previous := 0
	for _, bounds := range matches {
		rewritten.WriteString(content[previous:bounds[0]])
		replacement, err := s.rewriteKnowledgeLink(content[bounds[0]:bounds[1]])
		if err != nil {
			return "", err
		}
		rewritten.WriteString(replacement)
		previous = bounds[1]
	}
	rewritten.WriteString(content[previous:])
	return rewritten.String(), nil
}

func (s *Store) rewriteKnowledgeLink(match string) (string, error) {
	rawURL := strings.SplitN(match, " ", 2)[0]
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("parse embedded knowledge link: %w", err)
	}
	query := parsed.Query()
	dataset, id, title := query.Get("dataset"), query.Get("id"), query.Get("title")
	// A provider may paginate immediately after the URI prefix or partway through
	// its query. Leave that incomplete tail untouched; the next chunk resumes it.
	if dataset == "" || id == "" && title == "" {
		return match, nil
	}
	owner, err := s.providers.ForCollection(dataset)
	if err != nil {
		return "", fmt.Errorf("resolve embedded knowledge link dataset %q: %w", dataset, err)
	}
	ref, err := s.references.Mint(documentrefs.Route{Provider: owner.ID(), Dataset: dataset, ID: id, Title: title, Section: query.Get("section")})
	if err != nil {
		return "", err
	}
	return `knowledge-read://document/` + ref + ` "Call knowledge_read with ref=` + ref + `"`, nil
}

func (s *Store) ReadReference(ctx context.Context, reference string, options model.ReadOptions) (model.Document, error) {
	route, err := s.references.Resolve(reference)
	if err != nil {
		return model.Document{}, err
	}
	owner, err := s.providers.ForCollection(route.Dataset)
	if err != nil {
		return model.Document{}, err
	}
	if owner.ID() != route.Provider {
		return model.Document{}, fmt.Errorf("document reference provider no longer owns dataset %s", route.Dataset)
	}
	if options.Section == "" {
		options.Section = route.Section
	}
	var page model.Document
	if route.ID != "" {
		page, err = s.Read(ctx, route.Dataset, "", route.ID, options)
	} else {
		err = fmt.Errorf("document reference has no provider document ID")
	}
	if err != nil && route.Title != "" {
		if fallback, fallbackErr := s.Read(ctx, route.Dataset, route.Title, "", options); fallbackErr == nil {
			page, err = fallback, nil
		}
	}
	if err != nil && route.Title != "" {
		result, searchErr := s.Search(ctx, route.Dataset, route.Title, model.SearchOptions{Mode: "title", Limit: 10})
		if searchErr == nil {
			wanted := normalizeReferenceTitle(route.Title)
			for _, candidate := range result.Hits {
				if candidate.ID == route.ID || normalizeReferenceTitle(candidate.Title) != wanted {
					continue
				}
				fallback, fallbackErr := s.Read(ctx, route.Dataset, "", candidate.ID, options)
				if fallbackErr == nil {
					page, err = fallback, nil
					break
				}
			}
		}
	}
	if err == nil {
		page.Ref = reference
	}
	return page, err
}
