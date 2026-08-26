package provider

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/lkarlslund/knowledge-mcp/internal/model"
)

type contractProvider struct {
	id          string
	collections map[string]bool
	datasets    []model.AvailableDataset
	discoveries int
	discoverErr error
}

func (p *contractProvider) ID() string          { return p.id }
func (p *contractProvider) Owns(id string) bool { return p.collections[id] }
func (p *contractProvider) Latest(context.Context, string, string) (Release, error) {
	return Release{}, nil
}
func (p *contractProvider) Acquire(context.Context, string, string, Release, string, string, Progress) (model.Manifest, error) {
	return model.Manifest{}, nil
}
func (p *contractProvider) OpenCorpus(string, model.Manifest) (Corpus, error) {
	return &contractCorpus{}, nil
}
func (p *contractProvider) Backfill(context.Context, string, *model.Manifest) bool {
	return false
}
func (p *contractProvider) Discover(context.Context, string, bool) ([]model.AvailableDataset, error) {
	p.discoveries++
	return append([]model.AvailableDataset(nil), p.datasets...), p.discoverErr
}

func TestRegistryCatalogPersistsAcrossProcesses(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	first := &contractProvider{id: "test", datasets: []model.AvailableDataset{validDataset("test", "dataset")}}
	registry, err := NewRegistry(first)
	if err != nil {
		t.Fatal(err)
	}
	registry.ConfigureCatalogCache(directory, 24*time.Hour)
	if _, err := registry.Discover(context.Background(), "", false); err != nil {
		t.Fatal(err)
	}
	if first.discoveries != 1 {
		t.Fatalf("upstream discoveries = %d, want 1", first.discoveries)
	}
	if _, err := os.Stat(directory + "/test.json"); err != nil {
		t.Fatal(err)
	}

	second := &contractProvider{id: "test", discoverErr: errors.New("offline")}
	reopened, err := NewRegistry(second)
	if err != nil {
		t.Fatal(err)
	}
	reopened.ConfigureCatalogCache(directory, 24*time.Hour)
	items, reports, err := reopened.DiscoverReport(context.Background(), "data", false)
	if err != nil {
		t.Fatal(err)
	}
	if second.discoveries != 0 || len(items) != 1 || len(reports) != 1 || reports[0].Error != "" {
		t.Fatalf("disk cache result: calls=%d items=%d reports=%+v", second.discoveries, len(items), reports)
	}

	_, reports, err = reopened.DiscoverReport(context.Background(), "", true)
	if err != nil {
		t.Fatal(err)
	}
	if second.discoveries != 1 || len(reports) != 1 || !strings.Contains(reports[0].Error, "using stale disk cache") {
		t.Fatalf("forced stale fallback: calls=%d reports=%+v", second.discoveries, reports)
	}
}

type contractCorpus struct{}

func (*contractCorpus) ScanTitles(context.Context, string, ScanOptions, RecordSink) error { return nil }
func (*contractCorpus) ScanBodies(context.Context, string, ScanOptions, RecordSink) error { return nil }
func (*contractCorpus) Read(context.Context, Record, model.ReadOptions) (model.Document, error) {
	return model.Document{}, nil
}
func (*contractCorpus) Close() error { return nil }

func validDataset(owner, id string) model.AvailableDataset {
	return model.AvailableDataset{Provider: owner, ID: id, Description: "Useful test knowledge.", Variants: []model.Variant{{ID: "default", Name: "Default"}}}
}

func TestRegistryRejectsAmbiguousDatasetOwnership(t *testing.T) {
	t.Parallel()
	left := &contractProvider{id: "left", collections: map[string]bool{"shared": true}}
	right := &contractProvider{id: "right", collections: map[string]bool{"shared": true}}
	registry, err := NewRegistry(left, right)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.ForCollection("shared"); err == nil || !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("ambiguous ownership error = %v", err)
	}
}

func TestRegistryRejectsDiscoveryCollisions(t *testing.T) {
	t.Parallel()
	left := &contractProvider{id: "left", datasets: []model.AvailableDataset{validDataset("left", "shared")}}
	right := &contractProvider{id: "right", datasets: []model.AvailableDataset{validDataset("right", "shared")}}
	registry, err := NewRegistry(left, right)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Discover(context.Background(), "", false); err == nil || !strings.Contains(err.Error(), "both") {
		t.Fatalf("collision error = %v", err)
	}
}

func TestRegistryEnforcesDiscoveryMetadata(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		dataset model.AvailableDataset
	}{
		{name: "description", dataset: model.AvailableDataset{Provider: "test", ID: "dataset", Variants: []model.Variant{{ID: "default", Name: "Default"}}}},
		{name: "variant", dataset: model.AvailableDataset{Provider: "test", ID: "dataset", Description: "Useful"}},
		{name: "stable id", dataset: model.AvailableDataset{Provider: "test", ID: "../dataset", Description: "Useful", Variants: []model.Variant{{ID: "default", Name: "Default"}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			registry, err := NewRegistry(&contractProvider{id: "test", datasets: []model.AvailableDataset{test.dataset}})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := registry.Discover(context.Background(), "", false); err == nil {
				t.Fatal("invalid discovery metadata was accepted")
			}
		})
	}
}
