package ncbi

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/lkarlslund/knowledge-mcp/internal/model"
	"github.com/lkarlslund/knowledge-mcp/internal/provider"
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
	titleFieldDateYear
	titleFieldDateMonth
	titleFieldDateDay
	titleFieldMedlineDate
)

type pubmedTitle struct {
	id                string
	title             string
	journal           string
	identifiers       []string
	keywords          []string
	mesh              []string
	languages         []string
	authors           []string
	abstracts         []abstractText
	temporal          model.TemporalMetadata
	publishedPriority int
	deleted           bool
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
		Temporal: article.temporal, Deleted: article.deleted,
	}
	if body {
		record.Body = article.markdown()
	}
	return record
}

type pubmedDateParts struct {
	year, month, day, medline string
}

type pubmedDateCapture struct {
	depth, priority int
	target          string
	parts           pubmedDateParts
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
	inDelete := false
	journalDepth, abstractDepth, authorDepth := 0, 0, 0
	var dateCapture pubmedDateCapture

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
				if inDelete {
					depth++
					if bytes.Equal(name, []byte("PMID")) {
						field, fieldDepth = titleFieldPMID, depth
						value.Reset()
					}
					continue
				}
				if bytes.Equal(name, []byte("DeleteCitation")) {
					inDelete, depth = true, 1
					continue
				}
				if bytes.Equal(name, []byte("PubmedArticle")) || bytes.Equal(name, []byte("PubmedBookArticle")) {
					article = pubmedTitle{}
					depth = 1
					inArticle = true
				}
				continue
			}
			depth++
			switch {
			case bytes.Equal(name, []byte("DateRevised")):
				dateCapture = pubmedDateCapture{depth: depth, target: "modified", priority: 1}
			case bytes.Equal(name, []byte("ArticleDate")):
				dateCapture = pubmedDateCapture{depth: depth, target: "published", priority: 2}
			case bytes.Equal(name, []byte("PubDate")):
				dateCapture = pubmedDateCapture{depth: depth, target: "published", priority: 1}
			case bytes.Equal(name, []byte("PubMedPubDate")):
				status, attributeErr := pubmedAttribute(event.Bytes, "PubStatus")
				if attributeErr != nil {
					return attributeErr
				}
				switch strings.ToLower(status) {
				case "entrez":
					dateCapture = pubmedDateCapture{depth: depth, target: "created", priority: 1}
				case "pubmed":
					dateCapture = pubmedDateCapture{depth: depth, target: "published", priority: 3}
				}
			}
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
			case dateCapture.depth > 0 && bytes.Equal(name, []byte("Year")):
				field = titleFieldDateYear
			case dateCapture.depth > 0 && bytes.Equal(name, []byte("Month")):
				field = titleFieldDateMonth
			case dateCapture.depth > 0 && bytes.Equal(name, []byte("Day")):
				field = titleFieldDateDay
			case dateCapture.depth > 0 && bytes.Equal(name, []byte("MedlineDate")):
				field = titleFieldMedlineDate
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
			if inDelete {
				if field == titleFieldPMID && depth == fieldDepth {
					id := strings.Join(strings.Fields(value.String()), " ")
					if id != "" {
						if err := emit(pubmedTitle{id: id, deleted: true}); err != nil {
							return err
						}
					}
					field, fieldDepth = titleFieldNone, 0
					value.Reset()
				}
				if depth == 1 {
					inDelete, depth = false, 0
					continue
				}
				depth--
				continue
			}
			if !inArticle {
				continue
			}
			if field != titleFieldNone && depth == fieldDepth {
				if field >= titleFieldDateYear {
					assignPubMedDateField(&dateCapture.parts, field, value.String())
				} else {
					assignPubMedTitleField(&article, field, articleIDType, abstractLabel, value.String(), &authorLast, &authorFore, &authorCollective)
				}
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
			if depth == dateCapture.depth {
				article.applyDate(dateCapture)
				dateCapture = pubmedDateCapture{}
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

func assignPubMedDateField(parts *pubmedDateParts, field titleField, raw string) {
	value := strings.Join(strings.Fields(raw), " ")
	switch field {
	case titleFieldDateYear:
		parts.year = value
	case titleFieldDateMonth:
		parts.month = value
	case titleFieldDateDay:
		parts.day = value
	case titleFieldMedlineDate:
		parts.medline = value
	case titleFieldNone, titleFieldPMID, titleFieldTitle, titleFieldArticleID, titleFieldPublicationType, titleFieldDescriptor, titleFieldQualifier, titleFieldJournal, titleFieldLanguage, titleFieldAbstract, titleFieldAuthorLast, titleFieldAuthorFore, titleFieldAuthorCollective:
	}
}

func (article *pubmedTitle) applyDate(capture pubmedDateCapture) {
	value, precision := parsePubMedDate(capture.parts)
	if value == nil {
		return
	}
	switch capture.target {
	case "created":
		article.temporal.CreatedAt = value
	case "modified":
		article.temporal.ModifiedAt = value
	case "published":
		if capture.priority >= article.publishedPriority {
			article.temporal.PublishedAt = value
			article.temporal.PublishedPrecision = precision
			article.publishedPriority = capture.priority
		}
	}
}

func parsePubMedDate(parts pubmedDateParts) (*time.Time, string) {
	year, err := strconv.Atoi(parts.year)
	if err != nil && len(parts.medline) >= 4 {
		year, err = strconv.Atoi(parts.medline[:4])
		if err == nil && parts.month == "" {
			fields := strings.Fields(parts.medline)
			if len(fields) > 1 {
				parts.month = strings.Trim(fields[1], "-.,")
			}
		}
	}
	if err != nil || year < 1000 || year > 9999 {
		return nil, ""
	}
	month, precision := time.January, "year"
	if parts.month != "" {
		if numeric, numericErr := strconv.Atoi(parts.month); numericErr == nil && numeric >= 1 && numeric <= 12 {
			month, precision = time.Month(numeric), "month"
		} else {
			for candidate := time.January; candidate <= time.December; candidate++ {
				if strings.HasPrefix(strings.ToLower(candidate.String()), strings.ToLower(parts.month)) {
					month, precision = candidate, "month"
					break
				}
			}
		}
	}
	day := 1
	if parsed, dayErr := strconv.Atoi(parts.day); dayErr == nil && parsed >= 1 && parsed <= 31 {
		day, precision = parsed, "day"
	}
	value := time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
	return &value, precision
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
	case titleFieldNone, titleFieldDateYear, titleFieldDateMonth, titleFieldDateDay, titleFieldMedlineDate:
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
