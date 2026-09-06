package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/GhostFlying/delegation/internal/workerprofile"
)

const (
	upgradeProfileControllerID = "123e4567-e89b-42d3-a456-426614174800"
	upgradeProfileDeviceID     = "123e4567-e89b-42d3-a456-426614174801"
)

func TestMigrateExactAlpha4DatabasesPreservesExistingData(t *testing.T) {
	tests := []struct {
		name          string
		kind          DatabaseKind
		legacyVersion int
		newTable      string
		create        func(context.Context, string) error
		seed          string
		query         string
		want          any
	}{
		{
			name: "broker 19 to 20", kind: DatabaseBroker, legacyVersion: 19,
			newTable: "device_worker_readiness",
			create: func(ctx context.Context, path string) error {
				state, err := Open(ctx, path)
				if err != nil {
					return err
				}
				return state.Close()
			},
			seed:  `INSERT INTO broker_metadata(singleton, host_kind) VALUES (1, 'traex')`,
			query: `SELECT host_kind FROM broker_metadata WHERE singleton = 1`, want: "traex",
		},
		{
			name: "peer 15 to 16", kind: DatabasePeer, legacyVersion: 15,
			newTable: "worker_readiness",
			create: func(ctx context.Context, path string) error {
				state, err := OpenPeer(ctx, path)
				if err != nil {
					return err
				}
				return state.Close()
			},
			seed:  `UPDATE peer_metadata SET worker_revision = 47 WHERE singleton = 1`,
			query: `SELECT worker_revision FROM peer_metadata WHERE singleton = 1`, want: int64(47),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			directory := t.TempDir()
			if err := os.Chmod(directory, 0o700); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(directory, string(test.kind)+".sqlite3")
			if err := test.create(ctx, path); err != nil {
				t.Fatal(err)
			}
			database, err := sql.Open("sqlite", dataSourceName(path))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := database.ExecContext(ctx, test.seed); err != nil {
				database.Close()
				t.Fatal(err)
			}
			if _, err := database.ExecContext(ctx, fmt.Sprintf(
				"DROP TABLE %s; PRAGMA user_version = %d", test.newTable, test.legacyVersion,
			)); err != nil {
				database.Close()
				t.Fatal(err)
			}
			if err := database.Close(); err != nil {
				t.Fatal(err)
			}

			source := DatabaseIdentity{
				ApplicationID: currentDatabaseIdentity(t, test.kind).ApplicationID,
				SchemaVersion: test.legacyVersion,
			}
			if got, err := InspectUpgradeDatabase(ctx, path, test.kind); err != nil || got != source {
				t.Fatalf("legacy identity = %#v, %v; want %#v", got, err, source)
			}
			if blockers, err := ReadUpgradeBlockers(ctx, path, test.kind,
				"123e4567-e89b-42d3-a456-426614174800",
				"123e4567-e89b-42d3-a456-426614174801",
			); err != nil || !blockers.Empty() {
				t.Fatalf("legacy blockers = %#v, %v", blockers, err)
			}
			target := currentDatabaseIdentity(t, test.kind)
			var migrateErr error
			if test.kind == DatabasePeer {
				migrateErr = MigratePeerUpgradeDatabase(
					ctx, path, target, upgradeProfileControllerID, upgradeProfileDeviceID,
					workerprofile.CurrentVersion,
				)
			} else {
				migrateErr = MigrateBrokerUpgradeDatabase(ctx, path, target)
			}
			if migrateErr != nil {
				t.Fatal(migrateErr)
			}
			if err := ValidateUpgradeDatabase(ctx, path, test.kind, target); err != nil {
				t.Fatal(err)
			}

			database, err = sql.Open("sqlite", dataSourceName(path))
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			var got any
			switch test.want.(type) {
			case string:
				var value string
				if err := database.QueryRowContext(ctx, test.query).Scan(&value); err != nil {
					t.Fatal(err)
				}
				got = value
			default:
				var value int64
				if err := database.QueryRowContext(ctx, test.query).Scan(&value); err != nil {
					t.Fatal(err)
				}
				got = value
			}
			if got != test.want {
				t.Fatalf("preserved value = %#v, want %#v", got, test.want)
			}
			var tableSQL string
			if err := database.QueryRowContext(ctx,
				`SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = ?`,
				test.newTable,
			).Scan(&tableSQL); err != nil || !strings.Contains(tableSQL, "STRICT") {
				t.Fatalf("migrated table = %q, %v", tableSQL, err)
			}
			referencePath := filepath.Join(directory, "reference.sqlite3")
			if err := test.create(ctx, referencePath); err != nil {
				t.Fatal(err)
			}
			reference, err := sql.Open("sqlite", dataSourceName(referencePath))
			if err != nil {
				t.Fatal(err)
			}
			defer reference.Close()
			var wantTableSQL string
			if err := reference.QueryRowContext(ctx,
				`SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = ?`,
				test.newTable,
			).Scan(&wantTableSQL); err != nil {
				t.Fatal(err)
			}
			if strings.Join(strings.Fields(tableSQL), " ") !=
				strings.Join(strings.Fields(wantTableSQL), " ") {
				t.Fatalf("migrated table differs from current schema:\ngot  %s\nwant %s", tableSQL, wantTableSQL)
			}
		})
	}
}

func TestMigrateUpgradeDatabaseRejectsUnsupportedVersionGap(t *testing.T) {
	for _, kind := range []DatabaseKind{DatabaseBroker, DatabasePeer} {
		t.Run(string(kind), func(t *testing.T) {
			ctx := context.Background()
			directory := t.TempDir()
			if err := os.Chmod(directory, 0o700); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(directory, string(kind)+".sqlite3")
			var closeState func() error
			if kind == DatabaseBroker {
				state, err := Open(ctx, path)
				if err != nil {
					t.Fatal(err)
				}
				closeState = state.Close
			} else {
				state, err := OpenPeer(ctx, path)
				if err != nil {
					t.Fatal(err)
				}
				closeState = state.Close
			}
			if err := closeState(); err != nil {
				t.Fatal(err)
			}
			target := currentDatabaseIdentity(t, kind)
			database, err := sql.Open("sqlite", dataSourceName(path))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := database.ExecContext(ctx, fmt.Sprintf(
				"PRAGMA user_version = %d", target.SchemaVersion-2,
			)); err != nil {
				database.Close()
				t.Fatal(err)
			}
			if err := database.Close(); err != nil {
				t.Fatal(err)
			}
			var migrateErr error
			if kind == DatabasePeer {
				migrateErr = MigratePeerUpgradeDatabase(
					ctx, path, target, upgradeProfileControllerID, upgradeProfileDeviceID,
					workerprofile.CurrentVersion,
				)
			} else {
				migrateErr = MigrateBrokerUpgradeDatabase(ctx, path, target)
			}
			if migrateErr == nil ||
				!strings.Contains(migrateErr.Error(), "unsupported") {
				t.Fatalf("migration gap error = %v", migrateErr)
			}
			if got, err := InspectUpgradeDatabase(ctx, path, kind); err != nil ||
				got.SchemaVersion != target.SchemaVersion-2 {
				t.Fatalf("failed migration changed identity = %#v, %v", got, err)
			}
		})
	}
}

func TestMigratePeerUpgradeDatabaseRewritesHistoricalProfiles(t *testing.T) {
	for _, test := range []struct {
		name          string
		schemaVersion int
		profile       int
	}{
		{name: "alpha4 profile 5", schemaVersion: 15, profile: 5},
		{name: "alpha7 profile 6", schemaVersion: 16, profile: 6},
		{name: "current profile 7", schemaVersion: 16, profile: workerprofile.CurrentVersion},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			path := peerUpgradeProfileFixture(
				t, test.schemaVersion,
				[]profileWorker{{profile: test.profile, status: WorkerPending},
					{profile: test.profile, status: WorkerIdle},
					{profile: test.profile, status: WorkerInterrupted},
					{profile: test.profile, status: WorkerFailed}},
			)
			if err := ValidatePeerWorkerProfileUpgrade(
				ctx, path, upgradeProfileControllerID, upgradeProfileDeviceID,
				workerprofile.CurrentVersion,
			); err != nil {
				t.Fatalf("profile preflight: %v", err)
			}
			target := currentDatabaseIdentity(t, DatabasePeer)
			if err := MigratePeerUpgradeDatabase(
				ctx, path, target, upgradeProfileControllerID, upgradeProfileDeviceID,
				workerprofile.CurrentVersion,
			); err != nil {
				t.Fatal(err)
			}
			if err := ValidateUpgradeDatabase(ctx, path, DatabasePeer, target); err != nil {
				t.Fatal(err)
			}
			assertWorkerProfiles(t, path, workerprofile.CurrentVersion, 4)

			// Repeating the stopped-shadow migration is idempotent.
			if err := MigratePeerUpgradeDatabase(
				ctx, path, target, upgradeProfileControllerID, upgradeProfileDeviceID,
				workerprofile.CurrentVersion,
			); err != nil {
				t.Fatalf("repeat migration: %v", err)
			}
			assertWorkerProfiles(t, path, workerprofile.CurrentVersion, 4)
		})
	}
}

func TestPeerWorkerProfilePreflightAcceptsForwardTargetButMigrationRequiresTargetRuntime(t *testing.T) {
	ctx := context.Background()
	path := peerUpgradeProfileFixture(t, 16, []profileWorker{{
		profile: workerprofile.CurrentVersion, status: WorkerFailed,
	}})
	forward := workerprofile.CurrentVersion + 1
	if err := ValidatePeerWorkerProfileUpgrade(
		ctx, path, upgradeProfileControllerID, upgradeProfileDeviceID, forward,
	); err != nil {
		t.Fatalf("forward profile preflight: %v", err)
	}
	if err := MigratePeerUpgradeDatabase(
		ctx, path, currentDatabaseIdentity(t, DatabasePeer),
		upgradeProfileControllerID, upgradeProfileDeviceID, forward,
	); err == nil || !strings.Contains(err.Error(), "does not match this runtime") {
		t.Fatalf("forward profile migration error = %v", err)
	}
	assertWorkerProfiles(t, path, workerprofile.CurrentVersion, 1)
}

func TestMigratePeerUpgradeDatabaseRejectsUnsafeHistoryAtomically(t *testing.T) {
	tests := []struct {
		name    string
		workers []profileWorker
		seed    func(*testing.T, string, WorkerReservation)
		want    string
	}{
		{name: "mixed historical generations", workers: []profileWorker{
			{profile: 5, status: WorkerFailed}, {profile: 6, status: WorkerFailed},
		}, want: "mixed worker profile"},
		{name: "mixed old and target generations", workers: []profileWorker{
			{profile: 6, status: WorkerFailed},
			{profile: workerprofile.CurrentVersion, status: WorkerFailed},
		}, want: "mixed worker profile"},
		{name: "unknown generation", workers: []profileWorker{
			{profile: 4, status: WorkerFailed},
		}, want: "cannot be upgraded"},
		{name: "future generation", workers: []profileWorker{
			{profile: workerprofile.CurrentVersion + 1, status: WorkerFailed},
		}, want: "cannot be upgraded"},
		{name: "occupied worker", workers: []profileWorker{
			{profile: 6, status: WorkerReserved},
		}, want: "active durable work"},
		{name: "pending operation", workers: []profileWorker{
			{profile: 6, status: WorkerPending},
		}, seed: seedPendingUpgradeOperation, want: "active durable work"},
		{name: "unfinished result publication", workers: []profileWorker{
			{profile: 6, status: WorkerIdle},
		}, seed: seedUnfinishedUpgradeArtifact, want: "active durable work"},
		{name: "foreign authority", workers: []profileWorker{
			{profile: 6, status: WorkerFailed},
		}, seed: seedForeignUpgradeAuthority, want: "another controller or device"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			path := peerUpgradeProfileFixture(t, 15, test.workers)
			if test.seed != nil {
				test.seed(t, path, upgradeProfileWorker(t, path, 0, test.workers[0]))
			}
			target := currentDatabaseIdentity(t, DatabasePeer)
			err := MigratePeerUpgradeDatabase(
				ctx, path, target, upgradeProfileControllerID, upgradeProfileDeviceID,
				workerprofile.CurrentVersion,
			)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("migration error = %v, want %q", err, test.want)
			}
			identity, inspectErr := InspectUpgradeDatabase(ctx, path, DatabasePeer)
			if inspectErr != nil || identity.SchemaVersion != 15 {
				t.Fatalf("failed migration identity = %#v, %v", identity, inspectErr)
			}
			assertWorkerProfileSequence(t, path, test.workers)
		})
	}
}

type profileWorker struct {
	profile int
	status  WorkerStatus
}

func peerUpgradeProfileFixture(t *testing.T, schemaVersion int, workers []profileWorker) string {
	t.Helper()
	ctx := context.Background()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "peer.sqlite3")
	state, err := OpenPeer(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	for index, profile := range workers {
		insertPeerStatusWorker(t, state, upgradeProfileWorker(t, path, index, profile))
	}
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
	if schemaVersion != peerSchemaVersion {
		database, err := sql.Open("sqlite", dataSourceName(path))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := database.ExecContext(ctx, fmt.Sprintf(
			"DROP TABLE worker_readiness; PRAGMA user_version = %d", schemaVersion,
		)); err != nil {
			database.Close()
			t.Fatal(err)
		}
		if err := database.Close(); err != nil {
			t.Fatal(err)
		}
	}
	return path
}

func upgradeProfileWorker(
	t *testing.T, path string, index int, profile profileWorker,
) WorkerReservation {
	t.Helper()
	id := func(offset int) string {
		return fmt.Sprintf("123e4567-e89b-42d3-a456-%012d", 100+index*10+offset)
	}
	worker := WorkerReservation{
		WorkerKey:     WorkerKey{ControllerID: upgradeProfileControllerID, TreeID: id(1), AgentID: id(2)},
		ParentAgentID: id(3), DeviceID: upgradeProfileDeviceID, TaskName: "retained worker",
		PromptDigest: strings.Repeat("a", 64), WorkspacePath: filepath.Join(filepath.Dir(path), "workspace-"+id(2)),
		ProfileVersion: profile.profile, Status: profile.status, Revision: uint64(index + 1),
		CreatedAt: int64(index + 1), UpdatedAt: int64(index + 1),
	}
	switch profile.status {
	case WorkerIdle:
		worker.CodexThreadID = id(4)
	case WorkerInterrupted:
		worker.CodexThreadID = id(4)
		worker.FailureCode = "interrupted"
	case WorkerFailed:
		worker.FailureCode = "failed"
	}
	return worker
}

func seedPendingUpgradeOperation(t *testing.T, path string, worker WorkerReservation) {
	t.Helper()
	database, err := sql.Open("sqlite", dataSourceName(path))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	_, err = database.Exec(`
INSERT INTO worker_operation_receipts(
 controller_id, operation_id, tree_id, agent_id, action, payload_digest,
 status, outcome, failure_code, created_at, updated_at
) VALUES (?, ?, ?, ?, 'send', ?, 'pending', 'pending', '', 1, 1)
`, worker.ControllerID, "123e4567-e89b-42d3-a456-426614174950", worker.TreeID, worker.AgentID, strings.Repeat("b", 64))
	if err != nil {
		t.Fatal(err)
	}
}

func seedUnfinishedUpgradeArtifact(t *testing.T, path string, worker WorkerReservation) {
	t.Helper()
	database, err := sql.Open("sqlite", dataSourceName(path))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	insertPeerStatusArtifact(t, &PeerStore{db: database}, worker, 1, ChangesCapturePending)
}

func seedForeignUpgradeAuthority(t *testing.T, path string, _ WorkerReservation) {
	t.Helper()
	database, err := sql.Open("sqlite", dataSourceName(path))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if _, err := database.Exec(`
UPDATE worker_reservations
SET device_id = '123e4567-e89b-42d3-a456-426614174999'
`); err != nil {
		t.Fatal(err)
	}
}

func assertWorkerProfiles(t *testing.T, path string, profile, count int) {
	t.Helper()
	database, err := sql.Open("sqlite", dataSourceName(path))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var got int
	if err := database.QueryRow(
		"SELECT count(*) FROM worker_reservations WHERE profile_version = ?", profile,
	).Scan(&got); err != nil || got != count {
		t.Fatalf("profile %d count = %d, %v; want %d", profile, got, err, count)
	}
}

func assertWorkerProfileSequence(t *testing.T, path string, want []profileWorker) {
	t.Helper()
	database, err := sql.Open("sqlite", dataSourceName(path))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	rows, err := database.Query("SELECT profile_version FROM worker_reservations ORDER BY created_at")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []int
	for rows.Next() {
		var profile int
		if err := rows.Scan(&profile); err != nil {
			t.Fatal(err)
		}
		got = append(got, profile)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("worker profiles = %v, want %d rows", got, len(want))
	}
	for index := range want {
		if got[index] != want[index].profile {
			t.Fatalf("worker profiles = %v, want source sequence %#v", got, want)
		}
	}
}

func currentDatabaseIdentity(t *testing.T, kind DatabaseKind) DatabaseIdentity {
	t.Helper()
	identity, err := CurrentDatabaseIdentity(kind)
	if err != nil {
		t.Fatal(err)
	}
	return identity
}
