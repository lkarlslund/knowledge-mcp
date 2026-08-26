package store

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/model"
	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/provider"
)

const maximumConfiguredWorkers = 32

func loadSettings(root string, defaults model.Settings) (model.Settings, error) {
	path := filepath.Join(root, "settings.json")
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		defaults.UpdatedAt = time.Now().UTC()
		if err := validateSettings(defaults); err != nil {
			return model.Settings{}, err
		}
		if err := writeJSON(path, defaults); err != nil {
			return model.Settings{}, err
		}
		return defaults, nil
	}
	if err != nil {
		return model.Settings{}, err
	}
	var settings model.Settings
	if err := json.Unmarshal(data, &settings); err != nil {
		return model.Settings{}, err
	}
	if settings.IndexingParallelism == 0 {
		settings.IndexingParallelism = defaults.IndexingParallelism
	}
	if settings.UpdateCheckHours == 0 {
		settings.UpdateCheckHours = defaults.UpdateCheckHours
	}
	if err := validateSettings(settings); err != nil {
		return model.Settings{}, err
	}
	return settings, nil
}

func validateSettings(settings model.Settings) error {
	if settings.DownloadWorkers < 1 || settings.DownloadWorkers > maximumConfiguredWorkers {
		return errors.New("download_workers must be between 1 and 32")
	}
	if settings.IndexWorkers < 1 || settings.IndexWorkers > maximumConfiguredWorkers {
		return errors.New("index_workers must be between 1 and 32")
	}
	if settings.IndexingParallelism < 1 || settings.IndexingParallelism > maximumConfiguredWorkers {
		return errors.New("indexing_parallelism must be between 1 and 32")
	}
	if settings.UpdateCheckHours < 1 || settings.UpdateCheckHours > 24*30 {
		return errors.New("update_check_hours must be between 1 and 720")
	}
	return nil
}

func (s *Store) Settings() model.Settings {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.settings
}

func (s *Store) UpdateSettings(settings model.Settings) (model.Settings, error) {
	settings.UpdatedAt = time.Now().UTC()
	if err := validateSettings(settings); err != nil {
		return model.Settings{}, err
	}
	if err := writeJSON(filepath.Join(s.root, "settings.json"), settings); err != nil {
		return model.Settings{}, err
	}
	s.mu.Lock()
	s.settings = settings
	s.notifyLocked()
	s.mu.Unlock()
	s.providers.ConfigureCatalogCache(filepath.Join(s.root, "catalogs"), time.Duration(settings.UpdateCheckHours)*time.Hour)
	for _, changed := range []chan struct{}{s.downloadSettings, s.indexSettings, s.scheduleSettings} {
		select {
		case changed <- struct{}{}:
		default:
		}
	}
	return settings, nil
}

func (s *Store) OperationalStatus() model.OperationalStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()
	providers := make([]model.ProviderStatus, 0, len(s.providerState))
	for _, status := range s.providerState {
		providers = append(providers, status)
	}
	sort.Slice(providers, func(i, j int) bool { return providers[i].Provider < providers[j].Provider })
	next := time.Now().UTC().Add(time.Duration(s.settings.UpdateCheckHours) * time.Hour)
	if !s.lastUpdateCheck.IsZero() {
		next = s.lastUpdateCheck.Add(time.Duration(s.settings.UpdateCheckHours) * time.Hour)
	}
	return model.OperationalStatus{Settings: s.settings, Providers: providers, LastUpdateCheck: s.lastUpdateCheck, NextUpdateCheck: next}
}

func (s *Store) recordDiscovery(reports []provider.DiscoveryReport, refresh bool) {
	now := time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, report := range reports {
		status := s.providerState[report.Provider]
		status.Provider, status.LastAttempt, status.Datasets = report.Provider, now, report.Datasets
		if report.Error == "" {
			status.State, status.Error, status.LastSuccess = "healthy", "", now
			if refresh {
				status.CatalogState = "refreshed"
			} else {
				status.CatalogState = "cached_or_current"
			}
		} else {
			status.State, status.Error, status.CatalogState = "degraded", report.Error, "stale_or_unavailable"
		}
		s.providerState[report.Provider] = status
	}
	s.notifyLocked()
}

func (s *Store) scheduleUpdates(ctx context.Context) {
	defer s.wg.Done()
	timer := time.NewTimer(time.Duration(s.Settings().UpdateCheckHours) * time.Hour)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.scheduleSettings:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(time.Duration(s.Settings().UpdateCheckHours) * time.Hour)
		case <-timer.C:
			s.performUpdateCheck(ctx)
			timer.Reset(time.Duration(s.Settings().UpdateCheckHours) * time.Hour)
		}
	}
}

func (s *Store) performUpdateCheck(ctx context.Context) {
	_, reports, _ := s.providers.DiscoverReport(ctx, "", true)
	s.recordDiscovery(reports, true)
	upgrades, _ := s.ListUpgrades(ctx)
	s.mu.Lock()
	s.lastUpdateCheck = time.Now().UTC()
	automatic := s.settings.AutomaticallyUpdate
	s.notifyLocked()
	s.mu.Unlock()
	if !automatic {
		return
	}
	for _, upgrade := range upgrades {
		if upgrade.UpdateAvailable {
			_, _ = s.Submit(upgrade.ID, upgrade.Variant, "update")
		}
	}
}
