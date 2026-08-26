package wikimedia

import (
	"compress/bzip2"
	"context"
	"encoding/json"
	"encoding/xml"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/model"
)

type matrixSite struct {
	URL      string `json:"url"`
	DBName   string `json:"dbname"`
	Code     string `json:"code"`
	Language string `json:"lang"`
	SiteName string `json:"sitename"`
	Closed   bool   `json:"closed"`
}

type matrixLanguage struct {
	Code      string       `json:"code"`
	Name      string       `json:"name"`
	LocalName string       `json:"localname"`
	Direction string       `json:"dir"`
	Sites     []matrixSite `json:"site"`
}

func (c *Client) SiteMetadata(ctx context.Context, wiki string) model.DatasetMetadata {
	metadata := inferSiteMetadata(wiki)
	if sites, err := c.loadSiteMatrix(ctx); err == nil {
		if found, ok := sites[wiki]; ok {
			metadata = found
		}
	}
	if enriched, err := c.loadSiteInfo(ctx, metadata); err == nil {
		metadata = enriched
	}
	return metadata
}

func (c *Client) loadSiteMatrix(ctx context.Context) (map[string]model.DatasetMetadata, error) {
	c.mu.Lock()
	if len(c.sites) > 0 && time.Since(c.sitesCached) < 24*time.Hour {
		sites := cloneSites(c.sites)
		c.mu.Unlock()
		return sites, nil
	}
	c.mu.Unlock()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.siteMatrixURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", userAgent)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, &httpStatusError{status: resp.Status}
	}
	var response struct {
		SiteMatrix map[string]json.RawMessage `json:"sitematrix"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 8<<20)).Decode(&response); err != nil {
		return nil, err
	}
	languages := make(map[string]model.Language)
	var groups []matrixLanguage
	for key, raw := range response.SiteMatrix {
		if key == "count" || key == "specials" {
			continue
		}
		var group matrixLanguage
		if json.Unmarshal(raw, &group) != nil || group.Code == "" {
			continue
		}
		groups = append(groups, group)
		languages[group.Code] = model.Language{Code: group.Code, Name: group.LocalName, LocalName: group.Name, Direction: group.Direction}
	}
	sites := make(map[string]model.DatasetMetadata)
	for _, group := range groups {
		for _, site := range group.Sites {
			sites[site.DBName] = matrixMetadata(site, languages[group.Code])
		}
	}
	var specials []matrixSite
	if raw := response.SiteMatrix["specials"]; json.Unmarshal(raw, &specials) == nil {
		for _, site := range specials {
			language := languages[site.Language]
			if language.Code == "" {
				language.Code = site.Language
			}
			sites[site.DBName] = matrixMetadata(site, language)
		}
	}
	c.mu.Lock()
	c.sites, c.sitesCached = sites, time.Now()
	c.mu.Unlock()
	return cloneSites(sites), nil
}

type httpStatusError struct{ status string }

func (e *httpStatusError) Error() string { return e.status }

func cloneSites(source map[string]model.DatasetMetadata) map[string]model.DatasetMetadata {
	result := make(map[string]model.DatasetMetadata, len(source))
	for wiki, metadata := range source {
		result[wiki] = metadata
	}
	return result
}

func matrixMetadata(site matrixSite, language model.Language) model.DatasetMetadata {
	project := normalizeProject(site.Code)
	name := site.SiteName
	if language.Name != "" && isGenericProjectName(site.SiteName, project) {
		name = language.Name + " " + site.SiteName
	}
	return model.DatasetMetadata{Name: name, Project: project, ContentType: projectContentType(project), Language: language, OnlineSourceURL: site.URL, Closed: site.Closed}
}

func inferSiteMetadata(wiki string) model.DatasetMetadata {
	project, code := projectFromWiki(wiki)
	return model.DatasetMetadata{Project: project, ContentType: projectContentType(project), Language: model.Language{Code: code}}
}

func projectFromWiki(wiki string) (string, string) {
	for _, suffix := range []string{"wiktionary", "wikiversity", "wikisource", "wikispecies", "wikivoyage", "wikibooks", "wikinews", "wikiquote", "wiki"} {
		if strings.HasSuffix(wiki, suffix) {
			return normalizeProject(suffix), strings.TrimSuffix(wiki, suffix)
		}
	}
	return "wikimedia", ""
}

func normalizeProject(project string) string {
	if project == "wiki" {
		return "wikipedia"
	}
	return project
}

func projectContentType(project string) string {
	return map[string]string{
		"wikipedia":   "general-purpose encyclopedia",
		"wiktionary":  "dictionary and lexical reference",
		"wikibooks":   "textbooks and instructional books",
		"wikinews":    "current and historical news",
		"wikiquote":   "notable quotations",
		"wikisource":  "primary-source texts and historical documents",
		"wikispecies": "taxonomy and species reference",
		"wikiversity": "courses and learning resources",
		"wikivoyage":  "travel guide",
		"abstract":    "language-independent encyclopedia content",
		"wikidata":    "structured, multilingual knowledge base",
		"commons":     "freely licensed media repository",
		"meta":        "Wikimedia coordination and documentation",
	}[project]
}

func isGenericProjectName(name, project string) bool {
	return strings.EqualFold(name, project) || project == "wikipedia" && strings.EqualFold(name, "Wikipedia")
}

func (c *Client) loadSiteInfo(ctx context.Context, metadata model.DatasetMetadata) (model.DatasetMetadata, error) {
	parsed, err := url.Parse(metadata.OnlineSourceURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return metadata, err
	}
	endpoint := parsed.Scheme + "://" + parsed.Host + "/w/api.php?action=query&meta=siteinfo&siprop=general%7Cstatistics%7Crightsinfo&format=json&formatversion=2"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return metadata, err
	}
	req.Header.Set("User-Agent", userAgent)
	resp, err := c.http.Do(req)
	if err != nil {
		return metadata, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return metadata, &httpStatusError{status: resp.Status}
	}
	var response struct {
		Query struct {
			General struct {
				Language string `json:"lang"`
				RTL      bool   `json:"rtl"`
			} `json:"general"`
			Statistics struct {
				Articles uint64 `json:"articles"`
			} `json:"statistics"`
			Rights struct {
				Text string `json:"text"`
				URL  string `json:"url"`
			} `json:"rightsinfo"`
		} `json:"query"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 2<<20)).Decode(&response); err != nil {
		return metadata, err
	}
	if metadata.Language.Code == "" {
		metadata.Language.Code = response.Query.General.Language
	}
	if response.Query.General.RTL {
		metadata.Language.Direction = "rtl"
	} else if metadata.Language.Direction == "" {
		metadata.Language.Direction = "ltr"
	}
	metadata.SourceDocuments = response.Query.Statistics.Articles
	metadata.License, metadata.LicenseURL = response.Query.Rights.Text, response.Query.Rights.URL
	metadata.MetadataUpdatedAt = time.Now().UTC()
	return metadata, nil
}

func ReadDumpSiteMetadata(ctx context.Context, wiki, path string) model.DatasetMetadata {
	metadata := inferSiteMetadata(wiki)
	f, err := os.Open(path)
	if err != nil {
		return metadata
	}
	defer func() { _ = f.Close() }()
	decoder := xml.NewDecoder(bzip2.NewReader(f))
	for {
		if err := ctx.Err(); err != nil {
			return metadata
		}
		token, err := decoder.Token()
		if err != nil {
			return metadata
		}
		start, ok := token.(xml.StartElement)
		if !ok {
			continue
		}
		if start.Name.Local == "mediawiki" {
			for _, attr := range start.Attr {
				if attr.Name.Local == "lang" {
					metadata.Language.Code = attr.Value
				}
			}
		}
		if start.Name.Local != "siteinfo" {
			continue
		}
		var site struct {
			Name string `xml:"sitename"`
			Base string `xml:"base"`
		}
		if decoder.DecodeElement(&site, &start) != nil {
			return metadata
		}
		metadata.Name = site.Name
		if parsed, parseErr := url.Parse(site.Base); parseErr == nil && parsed.Scheme != "" && parsed.Host != "" {
			metadata.OnlineSourceURL = parsed.Scheme + "://" + parsed.Host
		}
		return metadata
	}
}
