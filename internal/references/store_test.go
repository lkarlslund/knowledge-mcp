package references

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestReferencePersistsReusesAndExpires(t *testing.T) {
	path := filepath.Join(t.TempDir(), "references.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	route := Route{Provider: "wikimedia", Dataset: "enwiki", ID: "123", Section: "History"}
	reference, err := store.Mint(route)
	if err != nil {
		t.Fatal(err)
	}
	if len(reference) != 8 || reference[:2] != "r_" {
		t.Fatalf("reference = %q, want an 8-character r_ reference", reference)
	}
	reused, err := store.Mint(route)
	if err != nil || reused != reference {
		t.Fatalf("reused reference = %q, %v; want %q", reused, err, reference)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	store.now = func() time.Time { return now }
	resolved, err := store.Resolve(reference)
	if err != nil || resolved != route {
		t.Fatalf("resolved route = %#v, %v; want %#v", resolved, err, route)
	}
	store.now = func() time.Time { return now.Add(defaultTTL + time.Second) }
	if _, err := store.Resolve(reference); !errors.Is(err, ErrExpired) {
		t.Fatalf("expired resolve error = %v", err)
	}
	if _, err := store.Resolve("r_misspelled"); !errors.Is(err, ErrExpired) {
		t.Fatalf("invalid resolve error = %v", err)
	}
}
