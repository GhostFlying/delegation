package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

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
	if err := createPrivateDirectory(directory); err != nil {
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
		controllerID, activeDeviceID, "device", activeMAC[:],
		controllerID, disabledDeviceID, "controller", disabledMAC[:],
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
	if _, err := store.AuthenticateCredential(context.Background(), activeMAC); !errors.Is(err, ErrNotFound) {
		t.Fatalf("legacy device credential error = %v, want ErrNotFound", err)
	}
	replacement := NewCredential(controllerID, activeDeviceID, CredentialMAC{3}, time.Unix(1_700_000_002, 0))
	if err := store.CreateCredential(context.Background(), replacement); err != nil {
		t.Fatalf("reissue legacy device identity: %v", err)
	}
	disabledWant := Credential{
		ControllerID: controllerID,
		DeviceID:     disabledDeviceID,
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

func TestOpenMigratesVersionThreePeerTopology(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "state")
	if err := createPrivateDirectory(directory); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "broker.sqlite3")
	db, err := sql.Open("sqlite", dataSourceName(path))
	if err != nil {
		t.Fatal(err)
	}
	for _, schema := range []string{schemaV1, schemaV2, schemaV3} {
		if _, err := db.Exec(schema); err != nil {
			db.Close()
			t.Fatal(err)
		}
	}
	const (
		controllerID         = "123e4567-e89b-42d3-a456-426614174020"
		controllerDeviceID   = "123e4567-e89b-42d3-a456-426614174021"
		disabledControllerID = "123e4567-e89b-42d3-a456-426614174022"
		pendingControllerID  = "123e4567-e89b-42d3-a456-426614174023"
		activeLegacyDeviceID = "123e4567-e89b-42d3-a456-426614174024"
		revokedLegacyID      = "123e4567-e89b-42d3-a456-426614174025"
		pendingLegacyID      = "123e4567-e89b-42d3-a456-426614174026"
		treeID               = "123e4567-e89b-42d3-a456-426614174027"
		threadID             = "123e4567-e89b-42d3-a456-426614174028"
		agentID              = "123e4567-e89b-42d3-a456-426614174029"
	)
	macs := []CredentialMAC{{1}, {2}, {3}, {4}, {5}, {6}}
	if _, err := db.Exec(`
INSERT INTO credentials(controller_id, device_id, role, token_mac, disabled, issued_at, pending)
VALUES
    (?, ?, 'controller', ?, 0, 10, 0),
    (?, ?, 'controller', ?, 1, 11, 0),
    (?, ?, 'controller', ?, 1, 12, 1),
    (?, ?, 'device', ?, 0, 13, 0),
    (?, ?, 'device', ?, 1, 14, 0),
    (?, ?, 'device', ?, 1, 15, 1)
`,
		controllerID, controllerDeviceID, macs[0][:],
		controllerID, disabledControllerID, macs[1][:],
		controllerID, pendingControllerID, macs[2][:],
		controllerID, activeLegacyDeviceID, macs[3][:],
		controllerID, revokedLegacyID, macs[4][:],
		controllerID, pendingLegacyID, macs[5][:],
	); err != nil {
		db.Close()
		t.Fatal(err)
	}
	for index, fixture := range []struct {
		deviceID string
		role     string
	}{
		{controllerDeviceID, "controller"},
		{activeLegacyDeviceID, "device"},
	} {
		if _, err := db.Exec(`
INSERT INTO devices(
    controller_id, device_id, name, role, os, arch, runtime_version,
    protocol_version, features_json, online, last_seen_at, revision
) VALUES (?, ?, ?, ?, 'linux', 'amd64', '0.1.0-alpha.0.m1.1', 1, '["deviceRegistryV1"]', 1, ?, ?)
`, controllerID, fixture.deviceID, "peer", fixture.role, index+20, index+1); err != nil {
			db.Close()
			t.Fatal(err)
		}
	}
	root := control.NewRootPrincipal(controllerID, treeID, agentID, controllerDeviceID)
	capabilities, err := json.Marshal(root.Capabilities)
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	if _, err := db.Exec(`
INSERT INTO trees(controller_id, external_thread_id, tree_id, root_agent_id, root_device_id, created_at)
VALUES (?, ?, ?, ?, ?, 30)
`, controllerID, threadID, treeID, agentID, controllerDeviceID); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if _, err := db.Exec(`
INSERT INTO principals(
    controller_id, tree_id, agent_id, parent_agent_id, device_id, capabilities_json, created_at
) VALUES (?, ?, ?, '', ?, ?, 30)
`,
		controllerID, treeID, agentID, controllerDeviceID, string(capabilities),
	); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if _, err := db.Exec(
		"INSERT OR REPLACE INTO controller_registries(controller_id, revision) VALUES (?, 2)", controllerID,
	); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	registry, err := Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	if got, err := registry.AuthenticateCredential(context.Background(), macs[0]); err != nil ||
		got.ControllerID != controllerID || got.DeviceID != controllerDeviceID {
		t.Fatalf("migrated controller credential = %#v, error %v", got, err)
	}
	for index, deviceID := range []string{disabledControllerID, pendingControllerID} {
		got, err := registry.Credential(context.Background(), controllerID, deviceID)
		if err != nil || !got.Disabled || got.Pending {
			t.Fatalf("migrated controller state %s = %#v, error %v", deviceID, got, err)
		}
		if err := registry.CreateCredential(
			context.Background(), NewCredential(
				controllerID, deviceID, CredentialMAC{byte(index + 20)}, time.Unix(41, 0),
			),
		); !errors.Is(err, ErrConflict) {
			t.Fatalf("controller tombstone reissue error = %v, want ErrConflict", err)
		}
	}
	if _, err := registry.Credential(context.Background(), controllerID, activeLegacyDeviceID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("active legacy device credential error = %v, want ErrNotFound", err)
	}
	replacement := NewCredential(controllerID, activeLegacyDeviceID, CredentialMAC{7}, time.Unix(40, 0))
	if err := registry.CreateCredential(context.Background(), replacement); err != nil {
		t.Fatalf("reissue active legacy device identity: %v", err)
	}
	for _, deviceID := range []string{revokedLegacyID, pendingLegacyID} {
		got, err := registry.Credential(context.Background(), controllerID, deviceID)
		if err != nil || !got.Disabled || got.Pending {
			t.Fatalf("legacy device tombstone %s = %#v, error %v", deviceID, got, err)
		}
		if err := registry.CreateCredential(
			context.Background(), NewCredential(controllerID, deviceID, CredentialMAC{8}, time.Unix(41, 0)),
		); !errors.Is(err, ErrConflict) {
			t.Fatalf("legacy device tombstone reissue error = %v, want ErrConflict", err)
		}
	}
	page, err := registry.ListDevices(context.Background(), controllerID, DevicePageRequest{Limit: 10})
	if err != nil || len(page.Devices) != 2 {
		t.Fatalf("migrated devices = %#v, error %v", page, err)
	}
	tree, principal, err := registry.EnsureRootTree(
		context.Background(), controllerID, threadID, controllerDeviceID, time.Unix(50, 0),
	)
	if err != nil || tree.TreeID != treeID || !reflect.DeepEqual(principal, root) {
		t.Fatalf("migrated root binding = %#v, %#v, error %v", tree, principal, err)
	}
	var roleColumns int
	if err := registry.db.QueryRow(`
SELECT count(*) FROM pragma_table_info('credentials') WHERE name = 'role'
`).Scan(&roleColumns); err != nil || roleColumns != 0 {
		t.Fatalf("credential role columns = %d, error %v", roleColumns, err)
	}
	if err := registry.db.QueryRow(`
SELECT count(*) FROM pragma_table_info('devices') WHERE name = 'role'
`).Scan(&roleColumns); err != nil || roleColumns != 0 {
		t.Fatalf("device role columns = %d, error %v", roleColumns, err)
	}
	rows, err := registry.db.Query("PRAGMA foreign_key_check")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if rows.Next() {
		t.Fatal("v4 migration left a foreign-key violation")
	}
}

func TestOpenCurrentDoesNotMigrateLegacyState(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "state")
	if err := createPrivateDirectory(directory); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "broker.sqlite3")
	db, err := sql.Open("sqlite", dataSourceName(path))
	if err != nil {
		t.Fatal(err)
	}
	for _, schema := range []string{schemaV1, schemaV2, schemaV3} {
		if _, err := db.Exec(schema); err != nil {
			db.Close()
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenCurrent(context.Background(), path); err == nil || !strings.Contains(err.Error(), "coordinated migration") {
		t.Fatalf("OpenCurrent legacy error = %v", err)
	}
	db, err = sql.Open("sqlite", dataSourceName(path))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var version int
	if err := db.QueryRow("PRAGMA user_version").Scan(&version); err != nil || version != 3 {
		t.Fatalf("legacy schema version = %d, error %v", version, err)
	}
}

func TestOpenMigratesVersionTwoControllerRevisions(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "state")
	if err := createPrivateDirectory(directory); err != nil {
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
	if _, err := db.Exec(schemaV2); err != nil {
		db.Close()
		t.Fatal(err)
	}
	const (
		secondControllerID     = "123e4567-e89b-42d3-a456-426614174020"
		credentialControllerID = "123e4567-e89b-42d3-a456-426614174022"
		treeControllerID       = "123e4567-e89b-42d3-a456-426614174023"
	)
	if _, err := db.Exec("UPDATE metadata SET integer_value = 9 WHERE key = 'registry_revision'"); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if _, err := db.Exec(`
INSERT INTO devices(
    controller_id, device_id, name, role, os, arch, runtime_version,
    protocol_version, features_json, online, last_seen_at, revision
) VALUES
    (?, ?, 'first', 'device', 'linux', 'amd64', 'test', 1, '[]', 1, 1, 7),
    (?, ?, 'second', 'device', 'windows', 'amd64', 'test', 1, '[]', 0, 2, 12)
`,
		testControllerID,
		testDeviceID,
		secondControllerID,
		"123e4567-e89b-42d3-a456-426614174021",
	); err != nil {
		db.Close()
		t.Fatal(err)
	}
	credentialMAC := CredentialMAC{3}
	if _, err := db.Exec(`
INSERT INTO credentials(
    controller_id, device_id, role, token_mac, disabled, issued_at, pending
) VALUES (?, ?, 'device', ?, 0, 3, 0)
`, credentialControllerID, "123e4567-e89b-42d3-a456-426614174024", credentialMAC[:]); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if _, err := db.Exec(`
INSERT INTO trees(
    controller_id, external_thread_id, tree_id, root_agent_id, root_device_id, created_at
) VALUES (?, ?, ?, ?, ?, 4)
`,
		treeControllerID,
		"123e4567-e89b-42d3-a456-426614174025",
		"123e4567-e89b-42d3-a456-426614174026",
		"123e4567-e89b-42d3-a456-426614174027",
		"123e4567-e89b-42d3-a456-426614174028",
	); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	registry, err := Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	rows, err := registry.db.Query(`
SELECT controller_id, revision FROM controller_registries ORDER BY controller_id
`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := map[string]int64{}
	for rows.Next() {
		var controllerID string
		var revision int64
		if err := rows.Scan(&controllerID, &revision); err != nil {
			t.Fatal(err)
		}
		got[controllerID] = revision
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	want := map[string]int64{
		testControllerID:       9,
		secondControllerID:     12,
		credentialControllerID: 9,
		treeControllerID:       9,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("controller revisions = %#v, want %#v", got, want)
	}
}

func TestOpenMigratesVersionTwoRevisionBoundaries(t *testing.T) {
	for _, testCase := range []struct {
		name           string
		globalRevision int64
		wantRevision   int64
	}{
		{name: "negative becomes zero", globalRevision: -1, wantRevision: 0},
		{name: "maximum is preserved", globalRevision: math.MaxInt64, wantRevision: math.MaxInt64},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			directory := filepath.Join(t.TempDir(), "state")
			if err := createPrivateDirectory(directory); err != nil {
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
			if _, err := db.Exec(schemaV2); err != nil {
				db.Close()
				t.Fatal(err)
			}
			mac := CredentialMAC{4}
			if _, err := db.Exec(`
INSERT INTO credentials(
    controller_id, device_id, role, token_mac, disabled, issued_at, pending
) VALUES (?, ?, 'device', ?, 0, 1, 0)
`, testControllerID, testDeviceID, mac[:]); err != nil {
				db.Close()
				t.Fatal(err)
			}
			if _, err := db.Exec(
				"UPDATE metadata SET integer_value = ? WHERE key = 'registry_revision'",
				testCase.globalRevision,
			); err != nil {
				db.Close()
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			registry, err := Open(context.Background(), path)
			if err != nil {
				t.Fatal(err)
			}
			defer registry.Close()
			var revision int64
			if err := registry.db.QueryRow(`
SELECT revision FROM controller_registries WHERE controller_id = ?
`, testControllerID).Scan(&revision); err != nil {
				t.Fatal(err)
			}
			if revision != testCase.wantRevision {
				t.Fatalf("migrated revision = %d, want %d", revision, testCase.wantRevision)
			}
		})
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
