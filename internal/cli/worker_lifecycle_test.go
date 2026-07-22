package cli

import (
	"context"
	"testing"

	"github.com/GhostFlying/delegation/internal/store"
)

type lifecycleHostStub struct {
	workers  []store.WorkerReservation
	revision uint64
	changes  chan struct{}
}

func (h lifecycleHostStub) Changes() <-chan struct{} { return h.changes }

func (h lifecycleHostStub) ListWorkers(context.Context) ([]store.WorkerReservation, error) {
	return append([]store.WorkerReservation(nil), h.workers...), nil
}

func (h lifecycleHostStub) WorkerRevision() uint64 { return h.revision }

func TestManagedWorkerLifecycleSourceMapsPersistedState(t *testing.T) {
	statuses := []store.WorkerStatus{
		store.WorkerReserved,
		store.WorkerPending,
		store.WorkerStarting,
		store.WorkerPreflight,
		store.WorkerReady,
		store.WorkerRunning,
		store.WorkerIdle,
		store.WorkerInterrupted,
		store.WorkerFailed,
	}
	workers := make([]store.WorkerReservation, 0, len(statuses))
	for index, status := range statuses {
		failureCode := ""
		if status == store.WorkerFailed {
			failureCode = "turn_failed"
		}
		workers = append(workers, store.WorkerReservation{
			WorkerKey: store.WorkerKey{
				ControllerID: runtimeControllerID,
				TreeID:       runtimeThreadID,
				AgentID:      runtimeAgentIDFor(index),
			},
			DeviceID: runtimeDeviceID, Status: status,
			FailureCode: failureCode, Revision: uint64(index + 1),
		})
	}
	changes := make(chan struct{}, 1)
	source := managedWorkerLifecycleSource{
		host:         lifecycleHostStub{workers: workers, revision: uint64(len(workers)), changes: changes},
		controllerID: runtimeControllerID, deviceID: runtimeDeviceID,
	}
	if source.WorkerRevision() != uint64(len(workers)) || source.WorkerLifecycleChanges() != changes {
		t.Fatal("managed worker lifecycle source did not expose host revision signals")
	}
	snapshots, err := source.ListWorkerLifecycles(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshots) != len(statuses) {
		t.Fatalf("lifecycle snapshots = %#v", snapshots)
	}
	for index, snapshot := range snapshots {
		if string(snapshot.Phase) != string(statuses[index]) || snapshot.Revision != uint64(index+1) {
			t.Fatalf("lifecycle snapshot %d = %#v", index, snapshot)
		}
		if statuses[index] == store.WorkerFailed && snapshot.FailureCode != "turn_failed" {
			t.Fatalf("failed lifecycle snapshot = %#v", snapshot)
		}
		if err := snapshot.Validate(); err != nil {
			t.Fatalf("lifecycle snapshot %d: %v", index, err)
		}
	}
}

func TestManagedWorkerLifecycleSourceRejectsForeignPeerState(t *testing.T) {
	source := managedWorkerLifecycleSource{
		host: lifecycleHostStub{workers: []store.WorkerReservation{{
			WorkerKey: store.WorkerKey{
				ControllerID: runtimeControllerID, TreeID: runtimeThreadID, AgentID: runtimeAgentIDFor(0),
			},
			DeviceID: runtimeWorkerID, Status: store.WorkerIdle, Revision: 1,
		}}},
		controllerID: runtimeControllerID, deviceID: runtimeDeviceID,
	}
	if _, err := source.ListWorkerLifecycles(context.Background()); err == nil {
		t.Fatal("foreign peer worker state was accepted")
	}
	if _, err := (managedWorkerLifecycleSource{}).ListWorkerLifecycles(context.Background()); err == nil {
		t.Fatal("missing lifecycle host was accepted")
	}
}

func runtimeAgentIDFor(index int) string {
	return []string{
		"123e4567-e89b-42d3-a456-426614174210",
		"123e4567-e89b-42d3-a456-426614174211",
		"123e4567-e89b-42d3-a456-426614174212",
		"123e4567-e89b-42d3-a456-426614174213",
		"123e4567-e89b-42d3-a456-426614174214",
		"123e4567-e89b-42d3-a456-426614174215",
		"123e4567-e89b-42d3-a456-426614174216",
		"123e4567-e89b-42d3-a456-426614174217",
		"123e4567-e89b-42d3-a456-426614174218",
	}[index]
}
