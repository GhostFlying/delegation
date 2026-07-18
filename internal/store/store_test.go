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

	"github.com/GhostFlying/delegation/internal/control"
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

	var version int
	if err := second.db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != schemaVersion {
		t.Fatalf("schema version = %d, want %d", version, schemaVersion)
	}
	for pragma, want := range map[string]int{
		"foreign_keys": 1,
		"synchronous":  2,
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

func TestConcurrentOpenSerializesInitialMigration(t *testing.T) {
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

func TestOpenRejectsFutureSchemaVersion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "broker.sqlite3")
	store, err := Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(fmt.Sprintf("PRAGMA user_version = %d", schemaVersion+1)); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(context.Background(), path); err == nil {
		t.Fatal("Open accepted a future schema version")
	}
}

func TestOpenRejectsNegativeSchemaVersion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "broker.sqlite3")
	store, err := Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec("PRAGMA user_version = -1"); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(context.Background(), path); err == nil {
		t.Fatal("Open accepted a negative schema version")
	}
}

func TestOpenMigratesVersionOneCredentialState(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "broker.sqlite3")
	db, err := sql.Open("sqlite", dataSourceName(path))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(schemaV1); err != nil {
		db.Close()
		t.Fatal(err)
	}
	activeMAC := CredentialMAC{1}
	disabledMAC := CredentialMAC{2}
	const (
		controllerID     = "123e4567-e89b-42d3-a456-426614174010"
		activeDeviceID   = "123e4567-e89b-42d3-a456-426614174011"
		disabledDeviceID = "123e4567-e89b-42d3-a456-426614174012"
	)
	if _, err := db.Exec(`
INSERT INTO credentials(controller_id, device_id, role, token_mac, disabled, issued_at)
VALUES (?, ?, ?, ?, 0, 1700000000), (?, ?, ?, ?, 1, 1700000001)
`,
		controllerID, activeDeviceID, control.DeviceRoleWorker, activeMAC[:],
		controllerID, disabledDeviceID, control.DeviceRoleController, disabledMAC[:],
	); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	store, err := Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var version int
	if err := store.db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != schemaVersion {
		t.Fatalf("migrated schema version = %d, want %d", version, schemaVersion)
	}
	rows, err := store.db.Query("SELECT pending FROM credentials")
	if err != nil {
		t.Fatalf("query migrated pending column: %v", err)
	}
	rows.Close()
	activeWant := Credential{
		ControllerID: controllerID,
		DeviceID:     activeDeviceID,
		Role:         control.DeviceRoleWorker,
		MAC:          activeMAC,
		IssuedAt:     1_700_000_000,
	}
	if got, err := store.AuthenticateCredential(context.Background(), activeMAC); err != nil || got != activeWant {
		t.Fatalf("migrated active credential = %#v, %v; want %#v", got, err, activeWant)
	}
	disabledWant := Credential{
		ControllerID: controllerID,
		DeviceID:     disabledDeviceID,
		Role:         control.DeviceRoleController,
		MAC:          disabledMAC,
		Disabled:     true,
		IssuedAt:     1_700_000_001,
	}
	if got, err := store.Credential(context.Background(), controllerID, disabledDeviceID); err != nil || got != disabledWant {
		t.Fatalf("migrated disabled credential = %#v, %v; want %#v", got, err, disabledWant)
	}
	if _, err := store.AuthenticateCredential(context.Background(), disabledMAC); !errors.Is(err, ErrCredentialDisabled) {
		t.Fatalf("migrated disabled authentication error = %v, want ErrCredentialDisabled", err)
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
	if err := os.Mkdir(directory, 0o700); err != nil {
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
