package wikiindex

import (
	"compress/bzip2"
	"context"
	"errors"
	"os"

	"github.com/cosnicolaou/pbzip2"
	"github.com/orisano/gosax"
)

// SourcePage is a provider-neutral view of one current MediaWiki XML page.
type SourcePage struct {
	ID         uint64
	Title      string
	Namespace  int
	RevisionID uint64
	Timestamp  string
	Wikitext   string
	Redirect   string
}

// ScanBzip2File streams one independently compressed MediaWiki content file.
// With includeBody false revision text is skipped without materializing it.
func ScanBzip2File(ctx context.Context, path string, includeBody bool, emit func(SourcePage) error) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	reader := gosax.NewReaderSize(pbzip2.NewReader(ctx, file), 64<<10)
	reader.EmitSelfClosingTag = true
	return scanXMLPagesSAXReader(reader, nil, includeBody, func(page xmlPage) error {
		return emit(exportSourcePage(page))
	})
}

// ReadBzip2Page scans a bounded content file for one page ID. Export files are
// page-range partitioned, so this avoids retaining an uncompressed corpus.
func ReadBzip2Page(ctx context.Context, path string, id uint64) (SourcePage, error) {
	file, err := os.Open(path)
	if err != nil {
		return SourcePage{}, err
	}
	defer func() { _ = file.Close() }()
	reader := gosax.NewReaderSize(contextReader{ctx: ctx, reader: bzip2.NewReader(file)}, 64<<10)
	reader.EmitSelfClosingTag = true
	var result SourcePage
	err = scanXMLPagesSAXReader(reader, map[uint64]struct{}{id: {}}, true, func(page xmlPage) error {
		result = exportSourcePage(page)
		return errStopPageScan
	})
	if errors.Is(err, errStopPageScan) {
		return result, nil
	}
	if err != nil {
		return SourcePage{}, err
	}
	if result.ID == 0 {
		return SourcePage{}, ErrPageNotFound
	}
	return result, nil
}

func exportSourcePage(page xmlPage) SourcePage {
	redirect := ""
	if page.Redirect != nil {
		redirect = page.Redirect.Title
	}
	return SourcePage{ID: page.ID, Title: page.Title, Namespace: page.Namespace, RevisionID: page.Revision.ID, Timestamp: page.Revision.Timestamp, Wikitext: page.Revision.Text, Redirect: redirect}
}

func sourceXMLPage(page SourcePage) xmlPage {
	result := xmlPage{ID: page.ID, Title: page.Title, Namespace: page.Namespace}
	result.Revision.ID, result.Revision.Timestamp, result.Revision.Text = page.RevisionID, page.Timestamp, page.Wikitext
	if page.Redirect != "" {
		result.Redirect = &struct {
			Title string `xml:"title,attr"`
		}{Title: page.Redirect}
	}
	return result
}

// IsRedirect reports whether the page is a redirect in either XML metadata or
// its wikitext directive.
func (page SourcePage) IsRedirect() bool { return redirectTarget(sourceXMLPage(page)) != "" }

var errStopPageScan = errors.New("stop page scan")
