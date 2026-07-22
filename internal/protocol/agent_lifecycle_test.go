package protocol

import (
	"fmt"
	"math"
	"testing"
)

const (
	lifecycleTreeID  = "123e4567-e89b-42d3-a456-426614175000"
	lifecycleAgentID = "123e4567-e89b-42d3-a456-426614175001"
)

func TestWorkerLifecyclePhaseValidatesFailureCode(t *testing.T) {
	for _, phase := range []WorkerLifecyclePhase{
		WorkerLifecycleReserved,
		WorkerLifecyclePending,
		WorkerLifecycleStarting,
		WorkerLifecyclePreflight,
		WorkerLifecycleReady,
		WorkerLifecycleRunning,
		WorkerLifecycleIdle,
		WorkerLifecycleInterrupted,
	} {
		if err := phase.Validate(""); err != nil {
			t.Fatalf("phase %q: %v", phase, err)
		}
		if err := phase.Validate("worker_failed"); err == nil {
			t.Fatalf("phase %q accepted failureCode", phase)
		}
	}
	if err := WorkerLifecycleFailed.Validate("worker_failed"); err != nil {
		t.Fatal(err)
	}
	if err := WorkerLifecycleFailed.Validate(""); err == nil {
		t.Fatal("failed phase accepted an empty failureCode")
	}
	if err := WorkerLifecyclePhase("unknown").Validate(""); err == nil {
		t.Fatal("unknown lifecycle phase was accepted")
	}
}

func TestSyncWorkerLifecyclePageValidation(t *testing.T) {
	workers := make([]WorkerLifecycleSnapshot, MaximumWorkerLifecyclePage)
	for index := range workers {
		workers[index] = lifecycleSnapshot(uint64(index + 1))
		workers[index].AgentID = lifecycleID(0x100 + index)
	}
	validIncomplete := SyncWorkerLifecycleParams{
		ThroughRevision: 40,
		Workers:         workers,
	}
	if err := validIncomplete.Validate(); err != nil {
		t.Fatal(err)
	}
	if got, err := validIncomplete.AppliedRevision(); err != nil || got != MaximumWorkerLifecyclePage {
		t.Fatalf("incomplete applied revision = %d, error %v", got, err)
	}
	validComplete := SyncWorkerLifecycleParams{
		BaseRevision: 32, ThroughRevision: 40, Complete: true,
		Workers: []WorkerLifecycleSnapshot{{
			TreeID: lifecycleTreeID, AgentID: lifecycleAgentID, Revision: 39,
			Phase: WorkerLifecycleIdle,
		}},
	}
	if err := validComplete.Validate(); err != nil {
		t.Fatal(err)
	}
	if got, err := validComplete.AppliedRevision(); err != nil || got != 40 {
		t.Fatalf("complete applied revision = %d, error %v", got, err)
	}
	empty := SyncWorkerLifecycleParams{BaseRevision: 40, ThroughRevision: 40, Complete: true}
	applied, err := empty.AppliedRevision()
	if err != nil || applied != 40 {
		t.Fatalf("empty complete page = %v, applied %d", err, applied)
	}
}

func TestSyncWorkerLifecycleRejectsMalformedPages(t *testing.T) {
	full := make([]WorkerLifecycleSnapshot, MaximumWorkerLifecyclePage)
	for index := range full {
		full[index] = lifecycleSnapshot(uint64(index + 1))
		full[index].AgentID = lifecycleID(0x200 + index)
	}
	tests := []struct {
		name   string
		params SyncWorkerLifecycleParams
	}{
		{name: "through before base", params: SyncWorkerLifecycleParams{BaseRevision: 2, ThroughRevision: 1, Complete: true}},
		{name: "revision overflow", params: SyncWorkerLifecycleParams{ThroughRevision: math.MaxInt64 + 1, Complete: true}},
		{name: "oversized page", params: SyncWorkerLifecycleParams{ThroughRevision: 40, Complete: true, Workers: append(append([]WorkerLifecycleSnapshot{}, full...), lifecycleSnapshot(33))}},
		{name: "short incomplete page", params: SyncWorkerLifecycleParams{ThroughRevision: 2, Workers: []WorkerLifecycleSnapshot{lifecycleSnapshot(1)}}},
		{name: "empty incomplete range", params: SyncWorkerLifecycleParams{BaseRevision: 2, ThroughRevision: 2, Workers: full}},
		{name: "at base", params: SyncWorkerLifecycleParams{BaseRevision: 1, ThroughRevision: 2, Complete: true, Workers: []WorkerLifecycleSnapshot{lifecycleSnapshot(1)}}},
		{name: "out of order", params: SyncWorkerLifecycleParams{ThroughRevision: 3, Complete: true, Workers: []WorkerLifecycleSnapshot{lifecycleSnapshot(2), lifecycleSnapshot(1)}}},
		{name: "beyond through", params: SyncWorkerLifecycleParams{ThroughRevision: 1, Complete: true, Workers: []WorkerLifecycleSnapshot{lifecycleSnapshot(2)}}},
		{name: "incomplete reaches through", params: SyncWorkerLifecycleParams{ThroughRevision: MaximumWorkerLifecyclePage, Workers: full}},
		{name: "duplicate agent", params: SyncWorkerLifecycleParams{ThroughRevision: 2, Complete: true, Workers: []WorkerLifecycleSnapshot{lifecycleSnapshot(1), lifecycleSnapshot(2)}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.params.Validate(); err == nil {
				t.Fatal("malformed worker lifecycle page was accepted")
			}
			if _, err := test.params.AppliedRevision(); err == nil {
				t.Fatal("malformed worker lifecycle page produced an applied revision")
			}
		})
	}
}

func TestHelloRejectsUnrepresentableWorkerRevision(t *testing.T) {
	hello := Hello{
		ControllerID: testControllerID, DeviceID: testDeviceID, DeviceName: "builder",
		OS: "linux", Arch: "amd64", RuntimeVersion: "0.1.0", Features: []string{},
		WorkerRevision: math.MaxInt64 + 1,
	}
	if err := hello.Validate(); err == nil {
		t.Fatal("hello accepted an unrepresentable worker revision")
	}
}

func lifecycleSnapshot(revision uint64) WorkerLifecycleSnapshot {
	return WorkerLifecycleSnapshot{
		TreeID: lifecycleTreeID, AgentID: lifecycleAgentID, Revision: revision,
		Phase: WorkerLifecycleRunning,
	}
}

func lifecycleID(value int) string {
	return fmt.Sprintf("123e4567-e89b-42d3-a456-%012x", value)
}
