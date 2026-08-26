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
)

type pubmedTitle struct {
	id          string
	title       string
	identifiers []string
	keywords    []string
}

func (article pubmedTitle) record(locator string) provider.Record {
	identifiers := make([]string, 0, len(article.identifiers)+1)
	identifiers = append(identifiers, "PMID "+article.id)
	identifiers = append(identifiers, article.identifiers...)
	return provider.Record{
		ID: article.id, Title: article.title,
		URL: "https://pubmed.ncbi.nlm.nih.gov/" + article.id + "/", Locator: locator,
		Primary: true, Identifiers: identifiers, Keywords: article.keywords, RankWeight: 1,
	}
}

func scanPubMedTitles(source io.Reader, emit func(pubmedTitle) error) error {
	reader := gosax.NewReaderSize(source, 64<<10)
	reader.EmitSelfClosingTag = true
	var article pubmedTitle
	var field titleField
	var fieldDepth int
	var articleIDType string
	var value strings.Builder
	depth := 0
	inArticle := false

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
			if bytes.Equal(name, []byte("Abstract")) || bytes.Equal(name, []byte("AuthorList")) {
				if err := gosax.Skip(reader); err != nil {
					return fmt.Errorf("skip PubMed body field: %w", err)
				}
				depth--
				continue
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
				assignPubMedTitleField(&article, field, articleIDType, value.String())
				field, fieldDepth, articleIDType = titleFieldNone, 0, ""
				value.Reset()
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

func assignPubMedTitleField(article *pubmedTitle, field titleField, idType, raw string) {
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
	case titleFieldPublicationType, titleFieldDescriptor, titleFieldQualifier:
		article.keywords = append(article.keywords, value)
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
