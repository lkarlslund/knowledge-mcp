package wikiindex

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/orisano/gosax"
)

type saxPageField uint8

const (
	saxPageFieldNone saxPageField = iota
	saxPageFieldTitle
	saxPageFieldNamespace
	saxPageFieldID
	saxPageFieldRevisionID
	saxPageFieldRevisionTimestamp
	saxPageFieldRevisionText
)

func decodeSelectedXMLPagesSAX(source io.Reader, wanted map[uint64]struct{}) ([]xmlPage, error) {
	reader := gosax.NewReaderSize(source, 64<<10)
	reader.EmitSelfClosingTag = true
	return decodeSelectedXMLPagesSAXReader(reader, wanted)
}

func (r *Reader) borrowSAXReader(source io.Reader) *gosax.Reader {
	reader, _ := r.saxReaders.Get().(*gosax.Reader)
	if reader == nil {
		reader = gosax.NewReaderSize(source, 64<<10)
	} else {
		reader.Reset(source)
	}
	reader.EmitSelfClosingTag = true
	return reader
}

func (r *Reader) releaseSAXReader(reader *gosax.Reader) {
	r.saxReaders.Put(reader)
}

func decodeSelectedXMLPagesSAXReader(reader *gosax.Reader, wanted map[uint64]struct{}) ([]xmlPage, error) {
	pages := make([]xmlPage, 0, len(wanted))
	selectAll := wanted == nil
	var page xmlPage
	var field saxPageField
	var fieldDepth int
	var value strings.Builder
	depth := 0
	inPage := false
	selected := false

	for {
		event, err := reader.Event()
		if err != nil {
			return nil, fmt.Errorf("parse XML stream: %w", err)
		}
		switch event.Type() {
		case gosax.EventEOF:
			return pages, nil
		case gosax.EventStart:
			name := saxLocalName(event.Bytes)
			if !inPage {
				if bytes.Equal(name, []byte("page")) {
					page = xmlPage{}
					depth = 1
					inPage = true
					selected = false
				}
				continue
			}

			depth++
			switch {
			case depth == 2 && bytes.Equal(name, []byte("title")):
				field = saxPageFieldTitle
			case depth == 2 && bytes.Equal(name, []byte("ns")):
				field = saxPageFieldNamespace
			case depth == 2 && bytes.Equal(name, []byte("id")):
				field = saxPageFieldID
			case depth == 2 && bytes.Equal(name, []byte("redirect")):
				title, attrErr := saxAttribute(event.Bytes, "title")
				if attrErr != nil {
					return nil, fmt.Errorf("parse redirect: %w", attrErr)
				}
				page.Redirect = &struct {
					Title string `xml:"title,attr"`
				}{Title: title}
			case depth == 2 && bytes.Equal(name, []byte("revision")) && !selected:
				if err := gosax.Skip(reader); err != nil {
					return nil, fmt.Errorf("skip unselected revision: %w", err)
				}
				depth--
			case depth == 3 && selected && bytes.Equal(name, []byte("id")):
				field = saxPageFieldRevisionID
			case depth == 3 && selected && bytes.Equal(name, []byte("timestamp")):
				field = saxPageFieldRevisionTimestamp
			case depth == 3 && selected && bytes.Equal(name, []byte("text")):
				field = saxPageFieldRevisionText
			default:
				continue
			}
			if field != saxPageFieldNone {
				fieldDepth = depth
				value.Reset()
			}
		case gosax.EventText:
			if field == saxPageFieldNone {
				continue
			}
			text, err := gosax.Unescape(event.Bytes)
			if err != nil {
				return nil, fmt.Errorf("decode XML character data: %w", err)
			}
			_, _ = value.Write(text)
		case gosax.EventCData:
			if field != saxPageFieldNone {
				_, _ = value.Write(bytes.TrimSuffix(bytes.TrimPrefix(event.Bytes, []byte("<![CDATA[")), []byte("]]>")))
			}
		case gosax.EventEnd:
			if !inPage {
				continue
			}
			if field != saxPageFieldNone && depth == fieldDepth {
				if err := assignSAXPageField(&page, field, value.String()); err != nil {
					return nil, err
				}
				if field == saxPageFieldID {
					if selectAll {
						selected = true
					} else {
						_, selected = wanted[page.ID]
					}
				}
				field = saxPageFieldNone
				fieldDepth = 0
				value.Reset()
			}
			if depth == 1 {
				if selected {
					pages = append(pages, page)
				}
				inPage = false
				depth = 0
				continue
			}
			depth--
		}
	}
}

func saxLocalName(element []byte) []byte {
	name, _ := gosax.Name(element)
	if separator := bytes.LastIndexByte(name, ':'); separator >= 0 {
		return name[separator+1:]
	}
	return name
}

func saxAttribute(element []byte, key string) (string, error) {
	_, attributes := gosax.Name(element)
	for len(attributes) > 0 {
		attribute, remaining, err := gosax.NextAttribute(attributes)
		if err != nil {
			return "", err
		}
		attributes = remaining
		name := attribute.Key
		if separator := bytes.LastIndexByte(name, ':'); separator >= 0 {
			name = name[separator+1:]
		}
		if !bytes.Equal(name, []byte(key)) {
			continue
		}
		if len(attribute.Value) < 2 {
			return "", fmt.Errorf("attribute %q has an invalid value", key)
		}
		value, err := gosax.Unescape(attribute.Value[1 : len(attribute.Value)-1])
		if err != nil {
			return "", err
		}
		return string(value), nil
	}
	return "", nil
}

func assignSAXPageField(page *xmlPage, field saxPageField, value string) error {
	switch field {
	case saxPageFieldNone:
		return nil
	case saxPageFieldTitle:
		page.Title = value
	case saxPageFieldNamespace:
		namespace, err := strconv.Atoi(strings.TrimSpace(value))
		if err != nil {
			return fmt.Errorf("parse page namespace: %w", err)
		}
		page.Namespace = namespace
	case saxPageFieldID:
		id, err := strconv.ParseUint(strings.TrimSpace(value), 10, 64)
		if err != nil {
			return fmt.Errorf("parse page id: %w", err)
		}
		page.ID = id
	case saxPageFieldRevisionID:
		id, err := strconv.ParseUint(strings.TrimSpace(value), 10, 64)
		if err != nil {
			return fmt.Errorf("parse revision id: %w", err)
		}
		page.Revision.ID = id
	case saxPageFieldRevisionTimestamp:
		page.Revision.Timestamp = value
	case saxPageFieldRevisionText:
		page.Revision.Text = value
	}
	return nil
}
