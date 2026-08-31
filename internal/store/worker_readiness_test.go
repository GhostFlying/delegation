package store

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/GhostFlying/delegation/internal/protocol"
)

func TestBrokerWorkerReadinessPersistsAndRejectsStaleSnapshots(t *testing.T) {
	registry := openTestStore(t)
	ctx := context.Background()
	if _, err := registry.RegisterTrustedDevice(
		ctx, deviceDescriptor(testControllerID, testDeviceID), testTime(),
	); err != nil {
		t.Fatal(err)
	}
	readiness := protocol.NewPendingWorkerReadiness(
		strings.Repeat("a", 64), strings.Repeat("b", 64), 1_000,
	)
	if got, err := registry.PutWorkerReadiness(
		ctx, testControllerID, testDeviceID, readiness,
	); err != nil || got != readiness {
		t.Fatalf("PutWorkerReadiness() = %#v, %v", got, err)
	}
	if got, err := registry.WorkerReadiness(
		ctx, testControllerID, testDeviceID,
	); err != nil || got != readiness {
		t.Fatalf("WorkerReadiness() = %#v, %v", got, err)
	}
	older := readiness
	older.UpdatedAt--
	older.EpochStartedAt--
	older.NextAttemptAt--
	if _, err := registry.PutWorkerReadiness(
		ctx, testControllerID, testDeviceID, older,
	); !errors.Is(err, ErrWorkerReadinessStale) {
		t.Fatalf("older readiness error = %v, want stale", err)
	}
	conflicting := readiness
	conflicting.State = protocol.WorkerReadinessInterventionRequired
	conflicting.NextAttemptAt = 0
	conflicting.FailureCode = protocol.WorkerManagedHomeInvalid
	if _, err := registry.PutWorkerReadiness(
		ctx, testControllerID, testDeviceID, conflicting,
	); !errors.Is(err, ErrWorkerReadinessStale) {
		t.Fatalf("same-revision conflict error = %v, want stale", err)
	}
}
