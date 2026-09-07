package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	delegationconfig "github.com/GhostFlying/delegation/internal/config"
)

func privateStoreTestDirectory(t *testing.T) string {
	t.Helper()
	directory := t.TempDir()
	if err := delegationconfig.PreparePrivateDirectory(directory); err != nil {
		t.Fatal(err)
	}
	return directory
}

func openTestStore(t *testing.T) *Store {
	t.Helper()
	store, err := Open(context.Background(), filepath.Join(t.TempDir(), "state", "broker.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})
	return store
}

func testTime() time.Time {
	return time.Unix(1_700_000_000, 0).UTC()
}
