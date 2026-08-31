package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
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
			if err := MigrateUpgradeDatabase(ctx, path, test.kind, target); err != nil {
				t.Fatal(err)
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
			if err := MigrateUpgradeDatabase(ctx, path, kind, target); err == nil ||
				!strings.Contains(err.Error(), "unsupported") {
				t.Fatalf("migration gap error = %v", err)
			}
			if got, err := InspectUpgradeDatabase(ctx, path, kind); err != nil ||
				got.SchemaVersion != target.SchemaVersion-2 {
				t.Fatalf("failed migration changed identity = %#v, %v", got, err)
			}
		})
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
