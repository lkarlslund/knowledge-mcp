// Package references stores temporary opaque document routes exposed to agents.
package references

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"time"

	"go.etcd.io/bbolt"
)

const (
	referencePrefix = "r_"
	defaultTTL      = 7 * 24 * time.Hour
)

var (
	routesBucket  = []byte("routes")
	reverseBucket = []byte("reverse")
	ErrExpired    = errors.New("document reference expired or does not exist; repeat knowledge_search")
)

// Route is the provider-owned document address hidden behind an agent-facing reference.
type Route struct {
	Provider string `json:"provider"`
	Dataset  string `json:"dataset"`
	ID       string `json:"id,omitempty"`
	Title    string `json:"title,omitempty"`
	Section  string `json:"section,omitempty"`
}

type entry struct {
	Route     Route     `json:"route"`
	ExpiresAt time.Time `json:"expires_at"`
}

// Store persists active references and reuses a route's reference during its lifetime.
type Store struct {
	db  *bbolt.DB
	ttl time.Duration
	now func() time.Time
}

func Open(path string) (*Store, error) {
	db, err := bbolt.Open(path, 0o600, &bbolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, fmt.Errorf("open document reference store: %w", err)
	}
	store := &Store{db: db, ttl: defaultTTL, now: time.Now}
	if err := db.Update(func(tx *bbolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(routesBucket); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(reverseBucket); err != nil {
			return err
		}
		return store.removeExpired(tx, store.now().UTC())
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("initialize document reference store: %w", err)
	}
	return store, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) Mint(route Route) (string, error) {
	if strings.TrimSpace(route.Provider) == "" || strings.TrimSpace(route.Dataset) == "" || route.ID == "" && strings.TrimSpace(route.Title) == "" {
		return "", errors.New("document reference route requires provider, dataset, and id or title")
	}
	now := s.now().UTC()
	expires := now.Add(s.ttl)
	reverseKey := routeKey(route)
	var reference string
	err := s.db.Update(func(tx *bbolt.Tx) error {
		routes, reverse := tx.Bucket(routesBucket), tx.Bucket(reverseBucket)
		if existing := reverse.Get(reverseKey); existing != nil {
			var current entry
			if data := routes.Get(existing); data != nil && json.Unmarshal(data, &current) == nil && current.ExpiresAt.After(now) && current.Route == route {
				current.ExpiresAt, reference = expires, string(existing)
				encoded, err := json.Marshal(current)
				if err != nil {
					return err
				}
				return routes.Put(existing, encoded)
			}
			_ = reverse.Delete(reverseKey)
		}
		encoded, err := json.Marshal(entry{Route: route, ExpiresAt: expires})
		if err != nil {
			return err
		}
		for {
			reference = randomReference()
			if routes.Get([]byte(reference)) != nil {
				continue
			}
			if err := routes.Put([]byte(reference), encoded); err != nil {
				return err
			}
			return reverse.Put(reverseKey, []byte(reference))
		}
	})
	if err != nil {
		return "", fmt.Errorf("mint document reference: %w", err)
	}
	return reference, nil
}

func (s *Store) Resolve(reference string) (Route, error) {
	if !validReference(reference) {
		return Route{}, ErrExpired
	}
	now := s.now().UTC()
	var route Route
	err := s.db.Update(func(tx *bbolt.Tx) error {
		routes, reverse := tx.Bucket(routesBucket), tx.Bucket(reverseBucket)
		data := routes.Get([]byte(reference))
		if data == nil {
			return ErrExpired
		}
		var current entry
		if err := json.Unmarshal(data, &current); err != nil {
			return err
		}
		if !current.ExpiresAt.After(now) {
			_ = routes.Delete([]byte(reference))
			if stored := reverse.Get(routeKey(current.Route)); string(stored) == reference {
				_ = reverse.Delete(routeKey(current.Route))
			}
			return ErrExpired
		}
		current.ExpiresAt, route = now.Add(s.ttl), current.Route
		encoded, err := json.Marshal(current)
		if err != nil {
			return err
		}
		return routes.Put([]byte(reference), encoded)
	})
	if err != nil {
		if errors.Is(err, ErrExpired) {
			return Route{}, ErrExpired
		}
		return Route{}, fmt.Errorf("resolve document reference: %w", err)
	}
	return route, nil
}

func (s *Store) removeExpired(tx *bbolt.Tx, now time.Time) error {
	routes, reverse := tx.Bucket(routesBucket), tx.Bucket(reverseBucket)
	cursor := routes.Cursor()
	for key, data := cursor.First(); key != nil; key, data = cursor.Next() {
		var current entry
		if json.Unmarshal(data, &current) != nil || !current.ExpiresAt.After(now) {
			if current.Route.Provider != "" {
				if stored := reverse.Get(routeKey(current.Route)); string(stored) == string(key) {
					_ = reverse.Delete(routeKey(current.Route))
				}
			}
			if err := cursor.Delete(); err != nil {
				return err
			}
		}
	}
	return nil
}

func routeKey(route Route) []byte {
	return []byte(strings.Join([]string{route.Provider, route.Dataset, route.ID, route.Title, route.Section}, "\x00"))
}

func randomReference() string {
	var raw [4]byte
	binary.BigEndian.PutUint32(raw[:], rand.Uint32())
	return referencePrefix + base64.RawURLEncoding.EncodeToString(raw[:])
}

func validReference(reference string) bool {
	if !strings.HasPrefix(reference, referencePrefix) {
		return false
	}
	decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(reference, referencePrefix))
	return err == nil && len(decoded) == 4
}
