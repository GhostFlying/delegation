package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestOpenCreatesAndReopensSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state with space", "状态.sqlite3")
	ctx := context.Background()
	first, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()

	for pragma, want := range map[string]int{
		"application_id": storeApplicationID,
		"foreign_keys":   1,
		"synchronous":    2,
		"user_version":   schemaVersion,
	} {
		var got int
		if err := second.db.QueryRow("PRAGMA " + pragma).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("PRAGMA %s = %d, want %d", pragma, got, want)
		}
	}
	var journalMode string
	if err := second.db.QueryRow("PRAGMA journal_mode").Scan(&journalMode); err != nil {
		t.Fatal(err)
	}
	if journalMode != "wal" {
		t.Fatalf("journal mode = %q, want wal", journalMode)
	}
}

func TestValidatePathRejectsDirectoryAndSymlink(t *testing.T) {
	root := t.TempDir()
	directoryPath := filepath.Join(root, "directory.sqlite3")
	if err := os.Mkdir(directoryPath, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := ValidatePath(directoryPath); err == nil {
		t.Fatal("ValidatePath accepted a directory as broker state")
	}
	target := filepath.Join(root, "target.sqlite3")
	if err := os.WriteFile(target, []byte("state"), 0o600); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(root, "alias.sqlite3")
	if err := os.Symlink(target, alias); err != nil {
		t.Skipf("creating a state symlink is unavailable: %v", err)
	}
	if err := ValidatePath(alias); err == nil {
		t.Fatal("ValidatePath accepted a symbolic link as broker state")
	}
}

func TestConcurrentOpenSerializesSchemaInitialization(t *testing.T) {
	const openers = 8
	path := filepath.Join(t.TempDir(), "state", "broker.sqlite3")
	start := make(chan struct{})
	results := make(chan error, openers)
	var workers sync.WaitGroup
	for range openers {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			store, err := Open(context.Background(), path)
			if err == nil {
				err = store.Close()
			}
			results <- err
		}()
	}
	close(start)
	workers.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatalf("concurrent Open: %v", err)
		}
	}
}

func TestOpenRejectsUnsupportedSchemaIdentityWithoutMutation(t *testing.T) {
	tests := []struct {
		name          string
		applicationID int
		version       int
	}{
		{name: "missing application ID", applicationID: 0, version: schemaVersion},
		{name: "different application ID", applicationID: 1, version: schemaVersion},
		{name: "different schema version", applicationID: storeApplicationID, version: schemaVersion + 1},
		{name: "nonempty unversioned database", applicationID: 0, version: 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state", "broker.sqlite3")
			store, err := Open(context.Background(), path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.db.Exec(`
INSERT INTO controller_registries(controller_id, revision) VALUES ('sentinel', 7)
`); err != nil {
				t.Fatal(err)
			}
			if _, err := store.db.Exec(fmt.Sprintf("PRAGMA application_id = %d", test.applicationID)); err != nil {
				t.Fatal(err)
			}
			if _, err := store.db.Exec(fmt.Sprintf("PRAGMA user_version = %d", test.version)); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}

			if _, err := Open(context.Background(), path); err == nil {
				t.Fatal("Open accepted an unsupported broker state format")
			}
			if _, err := OpenCurrent(context.Background(), path); err == nil {
				t.Fatal("OpenCurrent accepted an unsupported broker state format")
			}

			db, err := sql.Open("sqlite", dataSourceName(path))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			var applicationID, version, sentinelRevision int
			if err := db.QueryRow("PRAGMA application_id").Scan(&applicationID); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow(`
SELECT revision FROM controller_registries WHERE controller_id = 'sentinel'
`).Scan(&sentinelRevision); err != nil {
				t.Fatal(err)
			}
			if applicationID != test.applicationID || version != test.version || sentinelRevision != 7 {
				t.Fatalf(
					"rejected state changed to application ID %d, version %d, sentinel %d",
					applicationID,
					version,
					sentinelRevision,
				)
			}
		})
	}
}

func TestOpenCurrentDoesNotInitializeState(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing", "broker.sqlite3")
	if _, err := OpenCurrent(context.Background(), missing); err == nil {
		t.Fatal("OpenCurrent created missing broker state")
	}
	if _, err := os.Lstat(missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing broker state was created: %v", err)
	}

	directory := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	empty := filepath.Join(directory, "empty.sqlite3")
	if err := os.WriteFile(empty, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenCurrent(context.Background(), empty); err == nil {
		t.Fatal("OpenCurrent initialized an empty broker state file")
	}
	db, err := sql.Open("sqlite", dataSourceName(empty))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var objectCount int
	if err := db.QueryRow("SELECT count(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'").Scan(&objectCount); err != nil {
		t.Fatal(err)
	}
	if objectCount != 0 {
		t.Fatalf("OpenCurrent created %d schema objects", objectCount)
	}
}

func TestOpenRejectsRelativeSymlinkAndCorruptState(t *testing.T) {
	if _, err := Open(context.Background(), "relative.sqlite3"); err == nil {
		t.Fatal("Open accepted a relative path")
	}

	dir := t.TempDir()
	target := filepath.Join(dir, "target")
	if err := os.WriteFile(target, []byte("target"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(target, link); err == nil {
		if _, err := Open(context.Background(), link); err == nil {
			t.Fatal("Open accepted a symbolic link")
		}
	}

	corrupt := filepath.Join(dir, "corrupt.sqlite3")
	if err := os.WriteFile(corrupt, []byte("not a database"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(context.Background(), corrupt); err == nil {
		t.Fatal("Open accepted corrupt state")
	}
}

func TestOpenDoesNotChangeExistingDirectoryPermissions(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "existing")
	if err := createPrivateDirectory(directory); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(directory)
	if err != nil {
		t.Fatal(err)
	}
	store, err := Open(context.Background(), filepath.Join(directory, "broker.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(directory)
	if err != nil {
		t.Fatal(err)
	}
	if after.Mode().Perm() != before.Mode().Perm() {
		t.Fatalf("directory permissions changed from %o to %o", before.Mode().Perm(), after.Mode().Perm())
	}
}
