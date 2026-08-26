package ncbi

import (
	"bytes"
	"fmt"
	"io"
	"strings"

	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/provider"
	"github.com/orisano/gosax"
)

type titleField uint8

const (
	titleFieldNone titleField = iota
	titleFieldPMID
	titleFieldTitle
	titleFieldArticleID
	titleFieldPublicationType
	titleFieldDescriptor
	titleFieldQualifier
	titleFieldJournal
	titleFieldLanguage
	titleFieldAbstract
	titleFieldAuthorLast
	titleFieldAuthorFore
	titleFieldAuthorCollective
)

type pubmedTitle struct {
	id          string
	title       string
	journal     string
	identifiers []string
	keywords    []string
	mesh        []string
	languages   []string
	authors     []string
	abstracts   []abstractText
}

func (article pubmedTitle) record(locator string, body bool) provider.Record {
	identifiers := make([]string, 0, len(article.identifiers)+1)
	identifiers = append(identifiers, "PMID "+article.id)
	identifiers = append(identifiers, article.identifiers...)
	record := provider.Record{
		ID: article.id, Title: article.title,
		URL: "https://pubmed.ncbi.nlm.nih.gov/" + article.id + "/", Locator: locator,
		Primary: true, Identifiers: identifiers, Keywords: article.keywords, RankWeight: 1,
		Metadata: map[string]string{"journal": article.journal, "language": strings.Join(article.languages, " ")},
	}
	if body {
		record.Body = article.markdown()
	}
	return record
}

func (article pubmedTitle) markdown() string {
	var out strings.Builder
	out.WriteString("# " + article.title + "\n\n")
	if len(article.authors) > 0 {
		out.WriteString("**Authors:** " + strings.Join(article.authors, ", ") + "\n\n")
	}
	if article.journal != "" {
		out.WriteString("**Journal:** " + article.journal + "\n\n")
	}
	for _, abstract := range article.abstracts {
		if abstract.Label != "" {
			out.WriteString("## " + abstract.Label + "\n\n")
		}
		out.WriteString(strings.TrimSpace(string(abstract.Text)) + "\n\n")
	}
	if len(article.mesh) > 0 {
		out.WriteString("**MeSH terms:** " + strings.Join(article.mesh, "; ") + "\n")
	}
	return strings.TrimSpace(out.String())
}

func scanPubMedArticles(source io.Reader, bodies bool, emit func(pubmedTitle) error) error {
	reader := gosax.NewReaderSize(source, 64<<10)
	reader.EmitSelfClosingTag = true
	var article pubmedTitle
	var field titleField
	var fieldDepth int
	var articleIDType string
	var abstractLabel string
	var authorLast, authorFore, authorCollective string
	var value strings.Builder
	depth := 0
	inArticle := false
	journalDepth, abstractDepth, authorDepth := 0, 0, 0

	for {
		event, err := reader.Event()
		if err != nil {
			return fmt.Errorf("parse PubMed XML: %w", err)
		}
		switch event.Type() {
		case gosax.EventEOF:
			return nil
		case gosax.EventStart:
			name := pubmedElementName(event.Bytes)
			if !inArticle {
				if bytes.Equal(name, []byte("PubmedArticle")) || bytes.Equal(name, []byte("PubmedBookArticle")) {
					article = pubmedTitle{}
					depth = 1
					inArticle = true
				}
				continue
			}
			depth++
			if !bodies && (bytes.Equal(name, []byte("Abstract")) || bytes.Equal(name, []byte("AuthorList"))) {
				if err := gosax.Skip(reader); err != nil {
					return fmt.Errorf("skip PubMed body field: %w", err)
				}
				depth--
				continue
			}
			if bodies {
				switch {
				case bytes.Equal(name, []byte("Journal")):
					journalDepth = depth
				case bytes.Equal(name, []byte("Abstract")):
					abstractDepth = depth
				case bytes.Equal(name, []byte("Author")):
					authorDepth = depth
					authorLast, authorFore, authorCollective = "", "", ""
				}
			}
			switch {
			case article.id == "" && bytes.Equal(name, []byte("PMID")):
				field = titleFieldPMID
			case bytes.Equal(name, []byte("ArticleTitle")) || bytes.Equal(name, []byte("BookTitle")):
				field = titleFieldTitle
			case bytes.Equal(name, []byte("ArticleId")):
				field = titleFieldArticleID
				articleIDType, err = pubmedAttribute(event.Bytes, "IdType")
				if err != nil {
					return err
				}
			case bytes.Equal(name, []byte("PublicationType")):
				field = titleFieldPublicationType
			case bytes.Equal(name, []byte("DescriptorName")):
				field = titleFieldDescriptor
			case bytes.Equal(name, []byte("QualifierName")):
				field = titleFieldQualifier
			case bodies && journalDepth > 0 && bytes.Equal(name, []byte("Title")):
				field = titleFieldJournal
			case bodies && bytes.Equal(name, []byte("Language")):
				field = titleFieldLanguage
			case bodies && abstractDepth > 0 && bytes.Equal(name, []byte("AbstractText")):
				field = titleFieldAbstract
				abstractLabel, err = pubmedAttribute(event.Bytes, "Label")
				if err != nil {
					return err
				}
			case bodies && authorDepth > 0 && bytes.Equal(name, []byte("LastName")):
				field = titleFieldAuthorLast
			case bodies && authorDepth > 0 && bytes.Equal(name, []byte("ForeName")):
				field = titleFieldAuthorFore
			case bodies && authorDepth > 0 && bytes.Equal(name, []byte("CollectiveName")):
				field = titleFieldAuthorCollective
			default:
				continue
			}
			fieldDepth = depth
			value.Reset()
		case gosax.EventText:
			if field == titleFieldNone {
				continue
			}
			text, err := gosax.Unescape(event.Bytes)
			if err != nil {
				return fmt.Errorf("decode PubMed XML text: %w", err)
			}
			_, _ = value.Write(text)
		case gosax.EventCData:
			if field != titleFieldNone {
				_, _ = value.Write(bytes.TrimSuffix(bytes.TrimPrefix(event.Bytes, []byte("<![CDATA[")), []byte("]]>")))
			}
		case gosax.EventEnd:
			if !inArticle {
				continue
			}
			if field != titleFieldNone && depth == fieldDepth {
				assignPubMedTitleField(&article, field, articleIDType, abstractLabel, value.String(), &authorLast, &authorFore, &authorCollective)
				field, fieldDepth, articleIDType, abstractLabel = titleFieldNone, 0, "", ""
				value.Reset()
			}
			if bodies && depth == authorDepth {
				name := strings.TrimSpace(authorFore + " " + authorLast)
				if name == "" {
					name = authorCollective
				}
				if name != "" {
					article.authors = append(article.authors, name)
				}
				authorDepth = 0
			}
			if depth == abstractDepth {
				abstractDepth = 0
			}
			if depth == journalDepth {
				journalDepth = 0
			}
			if depth == 1 {
				if err := emit(article); err != nil {
					return err
				}
				inArticle = false
				depth = 0
				continue
			}
			depth--
		}
	}
}

func assignPubMedTitleField(article *pubmedTitle, field titleField, idType, abstractLabel, raw string, authorLast, authorFore, authorCollective *string) {
	value := strings.Join(strings.Fields(raw), " ")
	if value == "" {
		return
	}
	switch field {
	case titleFieldPMID:
		article.id = value
	case titleFieldTitle:
		article.title = value
	case titleFieldArticleID:
		article.identifiers = append(article.identifiers, strings.ToUpper(idType)+" "+value)
	case titleFieldPublicationType, titleFieldQualifier:
		article.keywords = append(article.keywords, value)
	case titleFieldDescriptor:
		article.keywords = append(article.keywords, value)
		article.mesh = append(article.mesh, value)
	case titleFieldJournal:
		article.journal = value
	case titleFieldLanguage:
		article.languages = append(article.languages, value)
	case titleFieldAbstract:
		article.abstracts = append(article.abstracts, abstractText{Label: abstractLabel, Text: richText(value)})
	case titleFieldAuthorLast:
		*authorLast = value
	case titleFieldAuthorFore:
		*authorFore = value
	case titleFieldAuthorCollective:
		*authorCollective = value
	case titleFieldNone:
	}
}

func pubmedElementName(element []byte) []byte {
	name, _ := gosax.Name(element)
	if separator := bytes.LastIndexByte(name, ':'); separator >= 0 {
		return name[separator+1:]
	}
	return name
}

func pubmedAttribute(element []byte, wanted string) (string, error) {
	_, attributes := gosax.Name(element)
	for len(attributes) > 0 {
		attribute, remaining, err := gosax.NextAttribute(attributes)
		if err != nil {
			return "", fmt.Errorf("parse PubMed XML attribute: %w", err)
		}
		attributes = remaining
		if !bytes.EqualFold(attribute.Key, []byte(wanted)) {
			continue
		}
		if len(attribute.Value) < 2 {
			return "", fmt.Errorf("PubMed XML attribute %q has an invalid value", wanted)
		}
		value, err := gosax.Unescape(attribute.Value[1 : len(attribute.Value)-1])
		if err != nil {
			return "", fmt.Errorf("decode PubMed XML attribute %q: %w", wanted, err)
		}
		return string(value), nil
	}
	return "", nil
}
