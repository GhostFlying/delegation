package store

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/GhostFlying/delegation/internal/protocol"
)

func TestPeerWorkerReadinessEpochsPersistAndChangeOnlyForAuthorizedEvents(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "peer.sqlite3")
	state, err := OpenPeer(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	runtimeDigest := strings.Repeat("a", 64)
	configDigest := strings.Repeat("b", 64)
	first, err := state.EnsureWorkerReadinessEpoch(
		context.Background(), runtimeDigest, configDigest, 1_000,
	)
	if err != nil {
		t.Fatal(err)
	}
	if first.Epoch != 1 || first.EpochStartedAt != 1_000 || first.NextAttemptAt != 1_000 {
		t.Fatalf("first readiness epoch = %#v", first)
	}
	first.AttemptCount = 1
	first.LastAttemptAt = 1_001
	first.NextAttemptAt = 11_000
	first.UpdatedAt = 1_001
	first.FailureCode = "worker_probe_failed"
	if _, err := state.UpdateWorkerReadiness(context.Background(), first.Epoch, first); err != nil {
		t.Fatal(err)
	}
	unchanged, err := state.EnsureWorkerReadinessEpoch(
		context.Background(), runtimeDigest, configDigest, 50_000,
	)
	if err != nil {
		t.Fatal(err)
	}
	if unchanged != first {
		t.Fatalf("ordinary restart changed epoch: got %#v, want %#v", unchanged, first)
	}
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenPeer(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	persisted, err := reopened.WorkerReadiness(context.Background())
	if err != nil || persisted != first {
		t.Fatalf("reopened readiness = %#v, %v", persisted, err)
	}
	changed, err := reopened.EnsureWorkerReadinessEpoch(
		context.Background(), strings.Repeat("c", 64), configDigest, 60_000,
	)
	if err != nil {
		t.Fatal(err)
	}
	if changed.Epoch != 2 || changed.EpochStartedAt != 60_000 || changed.AttemptCount != 0 {
		t.Fatalf("digest-change epoch = %#v", changed)
	}
	rechecked, err := reopened.RecheckWorkerReadiness(
		context.Background(), changed.RuntimeDigest, changed.ConfigDigest, 70_000,
	)
	if err != nil {
		t.Fatal(err)
	}
	if rechecked.Epoch != 3 || rechecked.EpochStartedAt != 70_000 {
		t.Fatalf("explicit recheck epoch = %#v", rechecked)
	}
}

func TestPeerWorkerReadinessUpdateFencesOldEpochAndRevision(t *testing.T) {
	state := openPeerTestStore(t)
	current, err := state.EnsureWorkerReadinessEpoch(
		context.Background(), strings.Repeat("a", 64), strings.Repeat("b", 64), 1_000,
	)
	if err != nil {
		t.Fatal(err)
	}
	staleRevision := current
	staleRevision.UpdatedAt = current.UpdatedAt
	if _, err := state.UpdateWorkerReadiness(
		context.Background(), current.Epoch, staleRevision,
	); !errors.Is(err, ErrWorkerReadinessStale) {
		t.Fatalf("equal-revision update error = %v, want stale", err)
	}
	next, err := state.RecheckWorkerReadiness(
		context.Background(), current.RuntimeDigest, current.ConfigDigest, 2_000,
	)
	if err != nil {
		t.Fatal(err)
	}
	current.UpdatedAt = 3_000
	if _, err := state.UpdateWorkerReadiness(
		context.Background(), current.Epoch, current,
	); !errors.Is(err, ErrWorkerReadinessStale) {
		t.Fatalf("old-epoch update error = %v, want stale; current %#v", err, next)
	}
}

func TestWorkerReadinessAllowsConsumedAttemptToBeDurablyFinalized(t *testing.T) {
	readiness := protocol.NewPendingWorkerReadiness(
		strings.Repeat("a", 64), strings.Repeat("b", 64), 1_000,
	)
	readiness.AttemptCount = protocol.MaximumReadinessAttempts
	readiness.LastAttemptAt = 2_000
	readiness.NextAttemptAt = 0
	readiness.UpdatedAt = 2_000
	if err := readiness.Validate(); err != nil {
		t.Fatalf("in-flight final attempt is invalid: %v", err)
	}
}

func TestPeerWorkerReadinessPersistsRepairRollbackFailureWithoutNewEpoch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "peer.sqlite3")
	state, err := OpenPeer(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	initial, err := state.EnsureWorkerReadinessEpoch(
		context.Background(), strings.Repeat("a", 64), strings.Repeat("b", 64), 100,
	)
	if err != nil {
		t.Fatal(err)
	}
	failed, err := state.FailWorkerReadiness(
		context.Background(), protocol.WorkerRepairRollbackFailed, 200,
	)
	if err != nil {
		t.Fatal(err)
	}
	if failed.Epoch != initial.Epoch ||
		failed.State != protocol.WorkerReadinessInterventionRequired ||
		failed.FailureCode != protocol.WorkerRepairRollbackFailed || failed.UpdatedAt != 200 {
		t.Fatalf("failed readiness = %#v", failed)
	}
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenPeer(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	persisted, err := reopened.WorkerReadiness(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if persisted != failed {
		t.Fatalf("persisted readiness = %#v, want %#v", persisted, failed)
	}
}
