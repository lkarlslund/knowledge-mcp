package wikiindex

import (
	"encoding/xml"
	"errors"
	"io"
	"strings"
)

func decodeXMLPages(decoder *xml.Decoder) ([]xmlPage, error) {
	decoder.Strict = false
	var pages []xmlPage
	for {
		token, err := decoder.Token()
		if errors.Is(err, io.EOF) {
			return pages, nil
		}
		if err != nil {
			var syntaxErr *xml.SyntaxError
			if len(pages) > 0 && errors.As(err, &syntaxErr) && (strings.Contains(syntaxErr.Msg, "unexpected end element") || strings.Contains(syntaxErr.Msg, "unexpected EOF")) {
				return pages, nil
			}
			return nil, err
		}
		start, ok := token.(xml.StartElement)
		if !ok || start.Name.Local != "page" {
			continue
		}
		var page xmlPage
		if err := decoder.DecodeElement(&page, &start); err != nil {
			return nil, err
		}
		pages = append(pages, page)
	}
}

func decodeSelectedXMLPages(decoder *xml.Decoder, wanted map[uint64]struct{}) ([]xmlPage, error) {
	decoder.Strict = false
	pages := make([]xmlPage, 0, len(wanted))
	for {
		token, err := decoder.Token()
		if errors.Is(err, io.EOF) {
			return pages, nil
		}
		if err != nil {
			var syntaxErr *xml.SyntaxError
			if len(pages) > 0 && errors.As(err, &syntaxErr) && (strings.Contains(syntaxErr.Msg, "unexpected end element") || strings.Contains(syntaxErr.Msg, "unexpected EOF")) {
				return pages, nil
			}
			return nil, err
		}
		start, ok := token.(xml.StartElement)
		if !ok || start.Name.Local != "page" {
			continue
		}
		page, selected, decodeErr := decodeSelectedXMLPage(decoder, start, wanted)
		if decodeErr != nil {
			return nil, decodeErr
		}
		if selected {
			pages = append(pages, page)
		}
	}
}

func decodeSelectedXMLPage(decoder *xml.Decoder, pageStart xml.StartElement, wanted map[uint64]struct{}) (xmlPage, bool, error) {
	var page xmlPage
	for {
		token, err := decoder.Token()
		if err != nil {
			return xmlPage{}, false, err
		}
		switch element := token.(type) {
		case xml.EndElement:
			if element.Name.Local == pageStart.Name.Local {
				_, selected := wanted[page.ID]
				return page, selected, nil
			}
		case xml.StartElement:
			switch element.Name.Local {
			case "title":
				if err := decoder.DecodeElement(&page.Title, &element); err != nil {
					return xmlPage{}, false, err
				}
			case "ns":
				if err := decoder.DecodeElement(&page.Namespace, &element); err != nil {
					return xmlPage{}, false, err
				}
			case "id":
				if page.ID == 0 {
					if err := decoder.DecodeElement(&page.ID, &element); err != nil {
						return xmlPage{}, false, err
					}
				} else if err := decoder.Skip(); err != nil {
					return xmlPage{}, false, err
				}
			case "redirect":
				var redirect struct {
					Title string `xml:"title,attr"`
				}
				if err := decoder.DecodeElement(&redirect, &element); err != nil {
					return xmlPage{}, false, err
				}
				page.Redirect = &redirect
			case "revision":
				if _, selected := wanted[page.ID]; selected {
					if err := decoder.DecodeElement(&page.Revision, &element); err != nil {
						return xmlPage{}, false, err
					}
				} else if err := decoder.Skip(); err != nil {
					return xmlPage{}, false, err
				}
			default:
				if err := decoder.Skip(); err != nil {
					return xmlPage{}, false, err
				}
			}
		}
	}
}
