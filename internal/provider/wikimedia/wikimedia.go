package wikimediaprovider

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/knowledgeindex"
	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/model"
	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/provider"
	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/wikiindex"
	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/wikimedia"
)

const WikimediaProviderID = "wikimedia"

type Wikimedia struct{ client *wikimedia.Client }

func New(client *wikimedia.Client) *Wikimedia    { return &Wikimedia{client: client} }
func (p *Wikimedia) ID() string                  { return WikimediaProviderID }
func (p *Wikimedia) Owns(collection string) bool { return wikimedia.ValidWikiName(collection) }

func (*Wikimedia) OpenCorpus(path string, manifest model.Manifest) (provider.Corpus, error) {
	return &wikimediaCorpus{path: path, manifest: manifest}, nil
}

type wikimediaCorpus struct {
	path     string
	manifest model.Manifest
	mu       sync.Mutex
	reader   *wikiindex.Reader
}

func (c *wikimediaCorpus) ScanTitles(ctx context.Context, after string, _ provider.ScanOptions, sink provider.RecordSink) error {
	return wikiindex.ScanSourceTitles(ctx, wikiParts(c.path, c.manifest.PartCount), after, func(source wikiindex.SourceTitle, cursor string, completed, total int64, boundary bool) error {
		return sink(provider.Record{
			ID: strconv.FormatUint(source.ID, 10), Title: source.Title,
			URL: wikiindex.URL(c.manifest.Site.OnlineSourceURL, source.Title), Part: source.Part, Offset: source.Offset, End: source.End,
			Primary: true,
		}, provider.ScanPosition{Cursor: cursor, Completed: completed, Total: total, Boundary: boundary})
	})
}

func (c *wikimediaCorpus) ScanBodies(ctx context.Context, after string, options provider.ScanOptions, sink provider.RecordSink) error {
	return wikiindex.ScanSourceBodies(ctx, wikiParts(c.path, c.manifest.PartCount), after, options.Parallelism, func(documents []wikiindex.SourceBody, cursor string, completed, total int64) error {
		for index, source := range documents {
			body := ""
			if !source.Redirect {
				body = wikiindex.PlainText(source.Wikitext)
			}
			if err := sink(provider.Record{
				ID: strconv.FormatUint(source.ID, 10), Title: source.Title, Body: body,
				URL: wikiindex.URL(c.manifest.Site.OnlineSourceURL, source.Title), Namespace: source.Namespace, Primary: source.Namespace == 0,
			}, provider.ScanPosition{Cursor: cursor, Completed: completed, Total: total, Boundary: index == len(documents)-1}); err != nil {
				return err
			}
		}
		return nil
	})
}

func (c *wikimediaCorpus) Read(ctx context.Context, record provider.Record, options model.ReadOptions) (model.Document, error) {
	id, err := strconv.ParseUint(record.ID, 10, 64)
	if err != nil {
		return model.Document{}, fmt.Errorf("invalid Wikimedia document ID %q", record.ID)
	}
	c.mu.Lock()
	if c.reader == nil {
		c.reader, err = wikiindex.OpenReader(c.path, false)
	}
	reader := c.reader
	c.mu.Unlock()
	if err != nil {
		return model.Document{}, err
	}
	sourceFormat := options.Format == "source"
	if sourceFormat {
		options.Format = "wikitext"
	}
	document, err := reader.ReadPage(ctx, "", id, options, c.manifest.Site.OnlineSourceURL)
	if sourceFormat {
		document.Format = "source"
	}
	document.ID = strconv.FormatUint(document.NumericID, 10)
	for index := range document.RedirectChain {
		document.RedirectChain[index].FromID = strconv.FormatUint(document.RedirectChain[index].FromNumericID, 10)
	}
	if errors.Is(err, wikiindex.ErrPageNotFound) {
		err = provider.ErrDocumentNotFound
	}
	return document, err
}

func (c *wikimediaCorpus) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.reader == nil {
		return nil
	}
	err := c.reader.Close()
	c.reader = nil
	return err
}

func (p *Wikimedia) Discover(ctx context.Context, filter string, refresh bool) ([]model.AvailableDataset, error) {
	result, err := p.client.ListAvailable(ctx, filter, 0, -1, refresh)
	for i := range result.Datasets {
		result.Datasets[i].Provider = p.ID()
		result.Datasets[i].Description = wikimediaDatasetDescription(result.Datasets[i].DisplayName, result.Datasets[i].ContentType, result.Datasets[i].Language)
		result.Datasets[i].Profile = wikimediaProfile(result.Datasets[i].ContentType, result.Datasets[i].Project, result.Datasets[i].Language, result.Datasets[i].ReleaseDate)
		result.Datasets[i].Variant = "multistream"
		result.Datasets[i].Variants = []model.Variant{{ID: "multistream", Name: "Articles", Description: "Wikimedia article multistream dump", Format: "XML/bzip2"}}
	}
	return result.Datasets, err
}

func (p *Wikimedia) Latest(ctx context.Context, collection, variant string) (provider.Release, error) {
	if variant != "" && variant != "multistream" {
		return provider.Release{}, fmt.Errorf("Wikimedia collection %s has no variant %q", collection, variant)
	}
	metadata, err := p.client.LatestMetadata(ctx, collection)
	return provider.Release{Fingerprint: metadata.Fingerprint, Date: metadata.ReleaseDate, Value: metadata}, err
}

func (p *Wikimedia) Acquire(ctx context.Context, collection, _ string, release provider.Release, stage, _ string, progress provider.Progress) (model.Manifest, error) {
	metadata, ok := release.Value.(model.ReleaseMetadata)
	if !ok {
		return model.Manifest{}, errors.New("invalid Wikimedia release metadata")
	}
	partsDir := filepath.Join(stage, "parts")
	if err := os.MkdirAll(partsDir, 0o755); err != nil {
		return model.Manifest{}, err
	}
	if len(metadata.Parts) == 1 {
		legacy := map[string]string{
			filepath.Join(stage, "multistream-index.txt.bz2"): filepath.Join(partsDir, "000.index.bz2"),
			filepath.Join(stage, "dump.xml.bz2"):              filepath.Join(partsDir, "000.dump.bz2"),
		}
		for oldPath, newPath := range legacy {
			if _, err := os.Stat(newPath); err == nil {
				continue
			}
			if _, err := os.Stat(oldPath); err == nil {
				if err := os.Rename(oldPath, newPath); err != nil {
					return model.Manifest{}, fmt.Errorf("migrate partial download: %w", err)
				}
			}
		}
	}
	var total, completed int64
	for _, part := range metadata.Parts {
		total += part.ProviderMetadata.Size + part.Raw.Size
	}
	download := func(label string, file model.FileMetadata, path string) error {
		progress("downloading_"+label, completed, total, "bytes", 0, "downloading "+label)
		return p.client.Download(ctx, file, path, func(done, _ int64, rate float64) {
			progress("downloading_"+label, completed+done, total, "bytes", rate, "downloading "+label)
		})
	}
	for i, part := range metadata.Parts {
		label := fmt.Sprintf("index part %d/%d", i+1, len(metadata.Parts))
		if err := download(label, part.ProviderMetadata, filepath.Join(partsDir, fmt.Sprintf("%03d.index.bz2", i))); err != nil {
			return model.Manifest{}, err
		}
		completed += part.ProviderMetadata.Size
		label = fmt.Sprintf("dump part %d/%d", i+1, len(metadata.Parts))
		if err := download(label, part.Raw, filepath.Join(partsDir, fmt.Sprintf("%03d.dump.bz2", i))); err != nil {
			return model.Manifest{}, err
		}
		completed += part.Raw.Size
	}
	dumpSite := wikimedia.ReadDumpSiteMetadata(ctx, collection, filepath.Join(partsDir, "000.dump.bz2"))
	site := mergeWikiSite(dumpSite, p.client.SiteMetadata(ctx, collection))
	site.Description = wikimediaDatasetDescription(site.Name, site.ContentType, site.Language)
	site.Profile = wikimediaProfile(site.ContentType, site.Project, site.Language, metadata.ReleaseDate)
	manifest := model.Manifest{Provider: p.ID(), Variant: "multistream", Dataset: collection, ReleaseDate: metadata.ReleaseDate, Fingerprint: metadata.Fingerprint, PartCount: len(metadata.Parts), PublishedAt: time.Now().UTC(), Site: site}
	for _, part := range metadata.Parts {
		manifest.RawSize += part.Raw.Size
		manifest.ProviderMetadataSize += part.ProviderMetadata.Size
	}
	if len(metadata.Parts) == 1 {
		manifest.RawHash, manifest.ProviderMetadataHash = metadata.Parts[0].Raw.SHA1, metadata.Parts[0].ProviderMetadata.SHA1
	}
	return manifest, nil
}

func (p *Wikimedia) Backfill(ctx context.Context, path string, manifest *model.Manifest) bool {
	changed := false
	if manifest.Provider == "" {
		manifest.Provider, manifest.Variant = p.ID(), "multistream"
		changed = true
	}
	if manifest.DocumentCount == 0 && manifest.TitleReady {
		if count, err := knowledgeindex.DocumentCount(filepath.Join(path, knowledgeindex.TitleDirectory)); err == nil {
			manifest.DocumentCount = count
			changed = true
		}
	}
	if manifest.RawSize == 0 || manifest.ProviderMetadataSize == 0 {
		var rawSize, metadataSize int64
		for i := range manifest.PartCount {
			if info, err := os.Stat(filepath.Join(path, "parts", fmt.Sprintf("%03d.dump.bz2", i))); err == nil {
				rawSize += info.Size()
			}
			if info, err := os.Stat(filepath.Join(path, "parts", fmt.Sprintf("%03d.index.bz2", i))); err == nil {
				metadataSize += info.Size()
			}
		}
		if rawSize > 0 {
			manifest.RawSize = rawSize
			changed = true
		}
		if metadataSize > 0 {
			manifest.ProviderMetadataSize = metadataSize
			changed = true
		}
	}
	if manifest.ReleaseDate == "" {
		if release, err := p.client.LatestMetadata(ctx, manifest.Dataset); err == nil && release.Fingerprint == manifest.Fingerprint {
			manifest.ReleaseDate = release.ReleaseDate
			changed = true
		}
	}
	if manifest.Site.Description != "" && len(manifest.Site.Profile.Topics) > 0 && !manifest.Site.MetadataUpdatedAt.IsZero() && time.Since(manifest.Site.MetadataUpdatedAt) < 24*time.Hour {
		return changed
	}
	dump := wikimedia.ReadDumpSiteMetadata(ctx, manifest.Dataset, filepath.Join(path, "parts", "000.dump.bz2"))
	manifest.Site = mergeWikiSite(dump, p.client.SiteMetadata(ctx, manifest.Dataset))
	manifest.Site.Description = wikimediaDatasetDescription(manifest.Site.Name, manifest.Site.ContentType, manifest.Site.Language)
	manifest.Site.Profile = wikimediaProfile(manifest.Site.ContentType, manifest.Site.Project, manifest.Site.Language, manifest.ReleaseDate)
	return true
}

func wikiParts(path string, count int) []wikiindex.Part {
	parts := make([]wikiindex.Part, 0, count)
	for i := range count {
		parts = append(parts, wikiindex.Part{Number: i, DumpPath: filepath.Join(path, "parts", fmt.Sprintf("%03d.dump.bz2", i)), IndexPath: filepath.Join(path, "parts", fmt.Sprintf("%03d.index.bz2", i))})
	}
	return parts
}

func mergeWikiSite(primary, fallback model.DatasetMetadata) model.DatasetMetadata {
	if primary.Name == "" {
		primary.Name = fallback.Name
	}
	if primary.Description == "" {
		primary.Description = fallback.Description
	}
	if primary.Project == "" {
		primary.Project = fallback.Project
	}
	if primary.ContentType == "" {
		primary.ContentType = fallback.ContentType
	}
	if len(primary.Profile.Topics) == 0 {
		primary.Profile = fallback.Profile
	}
	if primary.Language.Code == "" {
		primary.Language = fallback.Language
	}
	if primary.OnlineSourceURL == "" {
		primary.OnlineSourceURL = fallback.OnlineSourceURL
	}
	if primary.SourceDocuments == 0 {
		primary.SourceDocuments = fallback.SourceDocuments
	}
	if primary.License == "" {
		primary.License = fallback.License
	}
	if primary.LicenseURL == "" {
		primary.LicenseURL = fallback.LicenseURL
	}
	primary.Closed = primary.Closed || fallback.Closed
	primary.MetadataUpdatedAt = time.Now().UTC()
	return primary
}

func wikimediaDatasetDescription(name, contentType string, language model.Language) string {
	label := name
	if label == "" {
		label = "This Wikimedia dataset"
	}
	description := label + " contains " + contentType
	if contentType == "" {
		description = label + " contains Wikimedia knowledge content"
	}
	languageName := language.Name
	if languageName == "" {
		languageName = language.LocalName
	}
	if languageName == "" && language.Code != "" {
		languageName = "language code " + language.Code
	}
	if languageName != "" {
		description += " in " + languageName
	}
	return description + ". Use it for locally searchable article text and references from that project."
}

func wikimediaProfile(contentType, project string, language model.Language, releaseDate string) model.DatasetProfile {
	topics := []string{"Wikimedia"}
	if project != "" {
		topics = append(topics, project)
	}
	if contentType != "" {
		topics = append(topics, contentType)
	}
	scope := []string{"global"}
	if language.Name != "" {
		scope = append(scope, language.Name+"-language coverage")
	} else if language.Code != "" {
		scope = append(scope, language.Code+"-language coverage")
	}
	coverage := "Snapshot of the source project"
	if releaseDate != "" {
		coverage += " released " + releaseDate
	}
	return model.DatasetProfile{Topics: topics, GeographicScope: scope, TimeCoverage: coverage, DocumentTypes: []string{"wiki articles", "reference pages"}, UpdateCadence: "Wikimedia snapshot cadence, commonly monthly", CoverageNotes: "Coverage follows the selected Wikimedia project and includes its locally stored snapshot.", SourceFeatures: []string{"article text", "tables", "references", "internal links", "redirect resolution"}}
}
