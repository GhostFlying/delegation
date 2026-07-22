package store

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestOpenPeerCreatesDistinctPersistentSchema(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state", "peer.sqlite3")
	first, err := OpenPeer(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := OpenPeer(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()

	for pragma, want := range map[string]int{
		"application_id": peerStoreApplicationID,
		"foreign_keys":   1,
		"synchronous":    2,
		"user_version":   peerSchemaVersion,
	} {
		var got int
		if err := second.db.QueryRow("PRAGMA " + pragma).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("PRAGMA %s = %d, want %d", pragma, got, want)
		}
	}
}

func TestPeerStoreRejectsBrokerSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "shared.sqlite3")
	broker, err := Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if err := broker.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenPeer(context.Background(), path); err == nil {
		t.Fatal("OpenPeer accepted broker state")
	}
}

func TestPeerStoreRejectsEarlierPreReleaseSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "peer.sqlite3")
	peer, err := OpenPeer(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := peer.db.Exec("PRAGMA user_version = 1"); err != nil {
		t.Fatal(err)
	}
	if err := peer.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenPeer(context.Background(), path); err == nil {
		t.Fatal("OpenPeer accepted an earlier pre-release schema")
	}
}

func TestPeerLeaseIsExclusiveAndDoesNotOpenState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "peer.sqlite3")
	first, err := AcquirePeerLease(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("acquiring peer lease opened state: %v", err)
	}
	second, err := AcquirePeerLease(path)
	if second != nil {
		_ = second.Close()
		t.Fatal("second peer lease succeeded")
	}
	if !errors.Is(err, ErrPeerLeaseHeld) {
		t.Fatalf("second peer lease error = %v, want ErrPeerLeaseHeld", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	third, err := AcquirePeerLease(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := third.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path + ".peer.lock"); err != nil {
		t.Fatalf("persistent peer lease file is missing: %v", err)
	}
}
