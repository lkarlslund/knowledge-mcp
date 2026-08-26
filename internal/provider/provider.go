// Package provider defines the boundary between raw knowledge providers and the
// local lifecycle/search service. Providers discover collections and variants,
// acquire raw data, expose documents in the requested format, and feed the
// application's local indexes. Collection and document IDs never encode paths.
package provider

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/model"
)

var ErrDocumentNotFound = errors.New("document not found")

var stableIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,127}$`)

type Progress func(phase string, completed, total int64, units string, rate float64, message string)

type Record struct {
	ID        string
	Title     string
	Body      string
	URL       string
	Locator   string
	Part      int
	Offset    int64
	End       int64
	Namespace int
	Primary   bool
	Metadata  map[string]string
}

type ScanPosition struct {
	Cursor    string
	Completed int64
	Total     int64
}

type RecordSink func(Record, ScanPosition) error

type Corpus interface {
	ScanTitles(context.Context, string, RecordSink) error
	ScanBodies(context.Context, string, RecordSink) error
	Read(context.Context, Record, model.ReadOptions) (model.Document, error)
	Close() error
}

// Release is provider-private resolved metadata. Value must only be consumed by
// the Provider that returned it.
type Release struct {
	Fingerprint string
	Date        string
	Value       any
}

type Provider interface {
	ID() string
	Owns(collection string) bool
	Discover(context.Context, string, bool) ([]model.AvailableDataset, error)
	Latest(context.Context, string, string) (Release, error)
	// Acquire must resume valid work already present in stage. When current is
	// non-empty it must construct an update generation by reusing unchanged
	// provider data and acquiring only data needed for the supplied release.
	// It must never mutate current; the store publishes stage atomically later.
	Acquire(context.Context, string, string, Release, string, string, Progress) (model.Manifest, error)
	OpenCorpus(string, model.Manifest) (Corpus, error)
	Backfill(context.Context, string, *model.Manifest) bool
}

type Registry struct {
	providers []Provider
	byID      map[string]Provider
}

func NewRegistry(providers ...Provider) (*Registry, error) {
	registry := &Registry{byID: make(map[string]Provider, len(providers))}
	for _, provider := range providers {
		if provider == nil || !stableIDPattern.MatchString(provider.ID()) {
			return nil, errors.New("source provider must have an ID")
		}
		if _, exists := registry.byID[provider.ID()]; exists {
			return nil, fmt.Errorf("duplicate source provider %q", provider.ID())
		}
		registry.providers = append(registry.providers, provider)
		registry.byID[provider.ID()] = provider
	}
	return registry, nil
}

func (r *Registry) Providers() []Provider { return append([]Provider(nil), r.providers...) }

func (r *Registry) ByID(id string) (Provider, error) {
	provider := r.byID[id]
	if provider == nil {
		return nil, fmt.Errorf("unknown source provider %q", id)
	}
	return provider, nil
}

func (r *Registry) ForCollection(collection string) (Provider, error) {
	var match Provider
	for _, provider := range r.providers {
		if provider.Owns(collection) {
			if match != nil {
				return nil, fmt.Errorf("dataset ID %q is ambiguous between providers %q and %q", collection, match.ID(), provider.ID())
			}
			match = provider
		}
	}
	if match != nil {
		return match, nil
	}
	return nil, fmt.Errorf("no provider owns collection %q", collection)
}

func (r *Registry) Discover(ctx context.Context, filter string, refresh bool) ([]model.AvailableDataset, error) {
	type result struct {
		provider string
		items    []model.AvailableDataset
		err      error
	}
	results := make(chan result, len(r.providers))
	for _, provider := range r.providers {
		go func() {
			items, err := provider.Discover(ctx, filter, refresh)
			results <- result{provider: provider.ID(), items: items, err: err}
		}()
	}
	var items []model.AvailableDataset
	var errs []error
	for range r.providers {
		result := <-results
		for _, item := range result.items {
			if item.Provider != result.provider {
				return nil, fmt.Errorf("provider %q advertised dataset %q with owner %q", result.provider, item.ID, item.Provider)
			}
		}
		items = append(items, result.items...)
		if result.err != nil {
			errs = append(errs, result.err)
		}
	}
	owners := make(map[string]string, len(items))
	for _, item := range items {
		if !stableIDPattern.MatchString(item.ID) || item.Provider == "" || strings.TrimSpace(item.Description) == "" || len(item.Variants) == 0 {
			return nil, fmt.Errorf("provider %q returned incomplete dataset metadata for %q", item.Provider, item.ID)
		}
		seenVariants := make(map[string]struct{}, len(item.Variants))
		for _, variant := range item.Variants {
			if !stableIDPattern.MatchString(variant.ID) || strings.TrimSpace(variant.Name) == "" {
				return nil, fmt.Errorf("provider %q returned invalid variant for dataset %q", item.Provider, item.ID)
			}
			if _, exists := seenVariants[variant.ID]; exists {
				return nil, fmt.Errorf("provider %q returned duplicate variant %q for dataset %q", item.Provider, variant.ID, item.ID)
			}
			seenVariants[variant.ID] = struct{}{}
		}
		if owner, exists := owners[item.ID]; exists && owner != item.Provider {
			return nil, fmt.Errorf("dataset ID %q is advertised by both %q and %q", item.ID, owner, item.Provider)
		}
		owners[item.ID] = item.Provider
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Provider != items[j].Provider {
			return items[i].Provider < items[j].Provider
		}
		return items[i].ID < items[j].ID
	})
	if len(items) == 0 && len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return items, nil
}
