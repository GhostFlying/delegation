package localupgrade

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/store"
	"github.com/GhostFlying/delegation/internal/workerprofile"
	_ "modernc.org/sqlite"
)

func TestDefaultDatabasePreparationMigratesOnlyPeerShadowProfile(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	canonical := filepath.Join(directory, "peer.sqlite3")
	peer, err := store.OpenPeer(ctx, canonical)
	if err != nil {
		t.Fatal(err)
	}
	worker := store.WorkerReservation{
		WorkerKey: store.WorkerKey{
			ControllerID: "123e4567-e89b-42d3-a456-426614174800",
			TreeID:       "123e4567-e89b-42d3-a456-426614174801",
			AgentID:      "123e4567-e89b-42d3-a456-426614174802",
		},
		ParentAgentID:  "123e4567-e89b-42d3-a456-426614174803",
		DeviceID:       "123e4567-e89b-42d3-a456-426614174804",
		TaskName:       "retained worker",
		PromptDigest:   strings.Repeat("a", 64),
		WorkspacePath:  filepath.Join(directory, "workspace"),
		ProfileVersion: 6,
	}
	if _, err := peer.ReserveWorker(ctx, worker, 1, time.Unix(1, 0)); err != nil {
		peer.Close()
		t.Fatal(err)
	}
	if _, err := peer.FailWorker(ctx, worker.WorkerKey, "retained", time.Unix(2, 0)); err != nil {
		peer.Close()
		t.Fatal(err)
	}
	if err := peer.Close(); err != nil {
		t.Fatal(err)
	}
	identity, err := store.CurrentDatabaseIdentity(store.DatabasePeer)
	if err != nil {
		t.Fatal(err)
	}
	database := Database{
		Kind: store.DatabasePeer, CanonicalPath: canonical,
		ShadowPath:     filepath.Join(directory, "peer.shadow.sqlite3"),
		RollbackPath:   filepath.Join(directory, "peer.rollback.sqlite3"),
		SourceIdentity: identity, TargetIdentity: identity,
		ControllerID: worker.ControllerID, DeviceID: worker.DeviceID,
		TargetWorkerProfileVersion: workerprofile.CurrentVersion,
	}
	operations := DefaultActivationOperations(func(context.Context, Journal) error { return nil })
	prepared, err := operations.PrepareDatabase(ctx, database)
	if err != nil {
		t.Fatal(err)
	}
	assertDatabaseWorkerProfile(t, canonical, 6)
	assertDatabaseWorkerProfile(t, prepared.RollbackPath, 6)
	assertDatabaseWorkerProfile(t, prepared.ShadowPath, workerprofile.CurrentVersion)
	if prepared.SourceDigest == prepared.TargetDigest {
		t.Fatal("profile migration did not change the shadow database digest")
	}

	resumed, err := operations.PrepareDatabase(ctx, prepared)
	if err != nil || resumed.TargetDigest != prepared.TargetDigest {
		t.Fatalf("resume prepared database = %#v, %v", resumed, err)
	}
	if switched, err := ReconcileDatabaseSwitch(prepared); err != nil || !switched {
		t.Fatalf("switch migrated database = %v, %v", switched, err)
	}
	assertDatabaseWorkerProfile(t, canonical, workerprofile.CurrentVersion)
	assertDatabaseWorkerProfile(t, prepared.RollbackPath, 6)
}

func TestPrepareSwitchAndRollbackPeerDatabaseIncludesWAL(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	canonical := filepath.Join(directory, "peer.sqlite3")
	peer, err := store.OpenPeer(ctx, canonical)
	if err != nil {
		t.Fatal(err)
	}
	if err := peer.Close(); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(os.Args[0], "-test.run=TestWALCrashHelper")
	command.Env = append(os.Environ(), "DELEGATION_TEST_WAL_PATH="+canonical)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("WAL helper: %v: %s", err, output)
	}
	walInfo, err := os.Stat(canonical + "-wal")
	if err != nil || walInfo.Size() <= 0 {
		t.Fatalf("committed WAL fixture was not left on disk: %v, %#v", err, walInfo)
	}
	identity, err := store.CurrentDatabaseIdentity(store.DatabasePeer)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := PrepareDatabase(ctx, Database{
		Kind: store.DatabasePeer, CanonicalPath: canonical,
		ShadowPath:     filepath.Join(directory, "peer.shadow.sqlite3"),
		RollbackPath:   filepath.Join(directory, "peer.rollback.sqlite3"),
		SourceIdentity: identity, TargetIdentity: identity,
	}, func(_ context.Context, path string, _ store.DatabaseIdentity) error {
		db, err := sql.Open("sqlite", path)
		if err != nil {
			return err
		}
		defer db.Close()
		_, err = db.Exec(`INSERT INTO upgrade_fixture VALUES ('migrated')`)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if prepared.SourceDigest == "" || prepared.TargetDigest == "" || prepared.SourceDigest == prepared.TargetDigest {
		t.Fatalf("prepared digests = %#v", prepared)
	}
	if switched, err := ReconcileDatabaseSwitch(prepared); err != nil || !switched {
		t.Fatalf("switch = %v, %v", switched, err)
	}
	if switched, err := ReconcileDatabaseSwitch(prepared); err != nil || switched {
		t.Fatalf("idempotent switch = %v, %v", switched, err)
	}
	if count := fixtureRowCount(t, canonical); count != 2 {
		t.Fatalf("switched row count = %d", count)
	}
	if restored, err := ReconcileDatabaseRollback(prepared); err != nil || !restored {
		t.Fatalf("rollback = %v, %v", restored, err)
	}
	if count := fixtureRowCount(t, canonical); count != 1 {
		t.Fatalf("rolled-back row count = %d", count)
	}
}

func TestWALCrashHelper(t *testing.T) {
	path := os.Getenv("DELEGATION_TEST_WAL_PATH")
	if path == "" {
		return
	}
	database, err := sql.Open("sqlite", path)
	if err != nil {
		os.Exit(10)
	}
	if _, err := database.Exec(`PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE upgrade_fixture(value TEXT); INSERT INTO upgrade_fixture VALUES ('wal');`); err != nil {
		os.Exit(11)
	}
	// Deliberately bypass Close so committed WAL frames survive process loss.
	os.Exit(0)
}

func TestPrepareDatabaseCheckpointsCrashedMigrationWAL(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	canonical := filepath.Join(directory, "peer.sqlite3")
	peer, err := store.OpenPeer(ctx, canonical)
	if err != nil {
		t.Fatal(err)
	}
	if err := peer.Close(); err != nil {
		t.Fatal(err)
	}
	database, err := sql.Open("sqlite", canonical)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`CREATE TABLE upgrade_fixture(value TEXT); INSERT INTO upgrade_fixture VALUES ('source')`); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	identity, err := store.CurrentDatabaseIdentity(store.DatabasePeer)
	if err != nil {
		t.Fatal(err)
	}
	shadow := filepath.Join(directory, "peer.shadow.sqlite3")
	prepared, err := PrepareDatabase(ctx, Database{
		Kind: store.DatabasePeer, CanonicalPath: canonical, ShadowPath: shadow,
		RollbackPath:   filepath.Join(directory, "peer.rollback.sqlite3"),
		SourceIdentity: identity, TargetIdentity: identity,
	}, func(_ context.Context, path string, _ store.DatabaseIdentity) error {
		command := exec.Command(os.Args[0], "-test.run=TestMigrationWALCrashHelper")
		command.Env = append(os.Environ(), "DELEGATION_TEST_MIGRATION_WAL_PATH="+path)
		if output, err := command.CombinedOutput(); err != nil {
			return fmt.Errorf("migration WAL helper: %w: %s", err, output)
		}
		wal, err := os.Stat(path + "-wal")
		if err != nil || wal.Size() == 0 {
			return fmt.Errorf("migration helper left no WAL: %w", err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if _, err := os.Lstat(shadow + suffix); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("shadow sidecar %s remains after preparation: %v", suffix, err)
		}
	}
	if switched, err := ReconcileDatabaseSwitch(prepared); err != nil || !switched {
		t.Fatalf("switch = %v, %v", switched, err)
	}
	if count := fixtureRowCount(t, canonical); count != 2 {
		t.Fatalf("switched row count = %d, want 2", count)
	}
}

func TestMigrationWALCrashHelper(t *testing.T) {
	path := os.Getenv("DELEGATION_TEST_MIGRATION_WAL_PATH")
	if path == "" {
		return
	}
	database, err := sql.Open("sqlite", path)
	if err != nil {
		os.Exit(20)
	}
	if _, err := database.Exec(`PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; INSERT INTO upgrade_fixture VALUES ('target')`); err != nil {
		os.Exit(21)
	}
	// Deliberately bypass Close so the migration commit survives only in WAL.
	os.Exit(0)
}

func TestDifferentSchemaMigrationAndResumeAfterCanonicalSwitch(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	canonical := filepath.Join(directory, "peer.sqlite3")
	peer, err := store.OpenPeer(ctx, canonical)
	if err != nil {
		t.Fatal(err)
	}
	if err := peer.Close(); err != nil {
		t.Fatal(err)
	}
	source, _ := store.CurrentDatabaseIdentity(store.DatabasePeer)
	target := source
	target.SchemaVersion++
	definition := Database{
		Kind: store.DatabasePeer, CanonicalPath: canonical,
		ShadowPath:     filepath.Join(directory, "peer.shadow.sqlite3"),
		RollbackPath:   filepath.Join(directory, "peer.rollback.sqlite3"),
		SourceIdentity: source, TargetIdentity: target,
	}
	prepared, err := PrepareDatabase(ctx, definition, func(_ context.Context, path string, target store.DatabaseIdentity) error {
		database, err := sql.Open("sqlite", path)
		if err != nil {
			return err
		}
		defer database.Close()
		_, err = database.Exec(`PRAGMA user_version=` + fmt.Sprint(target.SchemaVersion))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if switched, err := ReconcileDatabaseSwitch(prepared); err != nil || !switched {
		t.Fatalf("switch = %v, %v", switched, err)
	}
	resumed, err := PrepareDatabase(ctx, prepared, nil)
	if err != nil || resumed.SourceDigest != prepared.SourceDigest || resumed.TargetDigest != prepared.TargetDigest {
		t.Fatalf("resume after lost switch progress = %#v, %v", resumed, err)
	}
	if restored, err := ReconcileDatabaseRollback(prepared); err != nil || !restored {
		t.Fatalf("rollback = %v, %v", restored, err)
	}
}

func TestPrepareDatabaseResumesAfterRollbackAndShadowCreation(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	canonical := filepath.Join(directory, "peer.sqlite3")
	peer, err := store.OpenPeer(ctx, canonical)
	if err != nil {
		t.Fatal(err)
	}
	if err := peer.Close(); err != nil {
		t.Fatal(err)
	}
	identity, _ := store.CurrentDatabaseIdentity(store.DatabasePeer)
	definition := Database{
		Kind: store.DatabasePeer, CanonicalPath: canonical,
		ShadowPath:     filepath.Join(directory, "peer.shadow.sqlite3"),
		RollbackPath:   filepath.Join(directory, "peer.rollback.sqlite3"),
		SourceIdentity: identity, TargetIdentity: identity,
	}
	root, err := store.OpenUpgradeDatabaseRoot(canonical)
	if err != nil {
		t.Fatal(err)
	}
	if err := copyDatabase(root, filepath.Base(canonical), filepath.Base(definition.RollbackPath)); err != nil {
		root.Close()
		t.Fatal(err)
	}
	if err := root.Close(); err != nil {
		t.Fatal(err)
	}
	prepared, err := PrepareDatabase(ctx, definition, nil)
	if err != nil {
		t.Fatalf("resume after rollback creation: %v", err)
	}
	rollbackDigest, err := FileDigest(definition.RollbackPath)
	if err != nil || rollbackDigest != prepared.SourceDigest {
		t.Fatalf("rollback digest after resume = %s, %v", rollbackDigest, err)
	}
	shadowInfo, err := os.Stat(definition.ShadowPath)
	if err != nil {
		t.Fatal(err)
	}
	resumed, err := PrepareDatabase(ctx, prepared, nil)
	if err != nil {
		t.Fatalf("resume after shadow creation: %v", err)
	}
	shadowAfter, err := os.Stat(definition.ShadowPath)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(shadowInfo, shadowAfter) || resumed.TargetDigest != prepared.TargetDigest {
		t.Fatal("verified shadow material was replaced during resume")
	}
}

func TestPrepareDatabaseRejectsMigrationFailureAndUnsupportedSchema(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	canonical := filepath.Join(directory, "peer.sqlite3")
	peer, err := store.OpenPeer(ctx, canonical)
	if err != nil {
		t.Fatal(err)
	}
	if err := peer.Close(); err != nil {
		t.Fatal(err)
	}
	identity, _ := store.CurrentDatabaseIdentity(store.DatabasePeer)
	definition := Database{
		Kind: store.DatabasePeer, CanonicalPath: canonical,
		ShadowPath:     filepath.Join(directory, "peer.shadow.sqlite3"),
		RollbackPath:   filepath.Join(directory, "peer.rollback.sqlite3"),
		SourceIdentity: identity, TargetIdentity: identity,
	}
	migrationFailure := errors.New("migration failed")
	if _, err := PrepareDatabase(ctx, definition, func(context.Context, string, store.DatabaseIdentity) error {
		return migrationFailure
	}); !errors.Is(err, migrationFailure) {
		t.Fatalf("migration error = %v", err)
	}
	if _, err := os.Stat(canonical); err != nil {
		t.Fatalf("migration failure changed canonical database: %v", err)
	}
	if err := os.Remove(definition.ShadowPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(definition.RollbackPath); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", canonical)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`PRAGMA user_version=999`); err != nil {
		t.Fatal(err)
	}
	db.Close()
	if _, err := PrepareDatabase(ctx, definition, nil); err == nil {
		t.Fatal("PrepareDatabase accepted unsupported source schema")
	}
}

func TestValidateUpgradeDatabaseRejectsCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "peer.sqlite3")
	if err := os.WriteFile(path, []byte("not sqlite"), 0o600); err != nil {
		t.Fatal(err)
	}
	identity, _ := store.CurrentDatabaseIdentity(store.DatabasePeer)
	if err := store.ValidateUpgradeDatabase(context.Background(), path, store.DatabasePeer, identity); err == nil {
		t.Fatal("ValidateUpgradeDatabase accepted corrupt database")
	}
}

func fixtureRowCount(t *testing.T, path string) int {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var count int
	if err := database.QueryRow(`SELECT count(*) FROM upgrade_fixture`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func assertDatabaseWorkerProfile(t *testing.T, path string, want int) {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var got int
	if err := database.QueryRow(`SELECT profile_version FROM worker_reservations`).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("worker profile = %d, want %d", got, want)
	}
}
