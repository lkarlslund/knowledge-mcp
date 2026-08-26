// Package provider defines the boundary between raw knowledge providers and the
// local lifecycle/search service. Providers discover collections and variants,
// acquire raw data, expose documents in the requested format, and feed the
// application's local indexes. Collection and document IDs never encode paths.
package provider

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/model"
)

var ErrDocumentNotFound = errors.New("document not found")

type Progress func(phase string, completed, total int64, units string, rate float64, message string)
type TitleProgress func(documents uint64, completed, total int64)
type BodyProgress func(completed, total int64)

// Release is provider-private resolved metadata. Value must only be consumed by
// the Provider that returned it.
type Release struct {
	Fingerprint string
	Date        string
	Value       any
}

type Reader interface {
	Retain() (func(), error)
	Close() error
	Search(context.Context, string, model.SearchOptions, bool) (model.SearchResult, error)
	Read(context.Context, string, string, model.ReadOptions) (model.Document, error)
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
	BuildTitle(context.Context, string, model.Manifest, TitleProgress) (uint64, error)
	BuildBody(context.Context, string, model.Manifest, BodyProgress) error
	Open(string, bool) (Reader, error)
	IndexCurrent(model.Manifest) (title, body bool)
	Backfill(context.Context, string, *model.Manifest) bool
}

type Registry struct {
	providers []Provider
	byID      map[string]Provider
}

func NewRegistry(providers ...Provider) (*Registry, error) {
	registry := &Registry{byID: make(map[string]Provider, len(providers))}
	for _, provider := range providers {
		if provider == nil || strings.TrimSpace(provider.ID()) == "" {
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
	for _, provider := range r.providers {
		if provider.Owns(collection) {
			return provider, nil
		}
	}
	return nil, fmt.Errorf("no provider owns collection %q", collection)
}

func (r *Registry) Discover(ctx context.Context, filter string, refresh bool) ([]model.AvailableDataset, error) {
	type result struct {
		items []model.AvailableDataset
		err   error
	}
	results := make(chan result, len(r.providers))
	for _, provider := range r.providers {
		go func() {
			items, err := provider.Discover(ctx, filter, refresh)
			results <- result{items: items, err: err}
		}()
	}
	var items []model.AvailableDataset
	var errs []error
	for range r.providers {
		result := <-results
		items = append(items, result.items...)
		if result.err != nil {
			errs = append(errs, result.err)
		}
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
