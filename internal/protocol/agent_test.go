package protocol

import (
	"encoding/json"
	"math"
	"strings"
	"testing"

	"github.com/GhostFlying/delegation/internal/control"
)

const (
	protocolAgentSpawnID  = "123e4567-e89b-42d3-a456-426614174300"
	protocolAgentTargetID = "123e4567-e89b-42d3-a456-426614174301"
)

func TestSpawnAgentParamsValidateBoundaries(t *testing.T) {
	valid := SpawnAgentParams{
		SpawnID:        protocolAgentSpawnID,
		TargetDeviceID: protocolAgentTargetID,
		TaskName:       strings.Repeat("a", MaximumAgentTaskNameBytes),
		Message:        strings.Repeat("m", MaximumAgentPromptBytes),
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid spawn parameters: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*SpawnAgentParams)
	}{
		{name: "spawn ID", mutate: func(value *SpawnAgentParams) { value.SpawnID = "invalid" }},
		{name: "target ID", mutate: func(value *SpawnAgentParams) { value.TargetDeviceID = "INVALID" }},
		{name: "task alphabet", mutate: func(value *SpawnAgentParams) { value.TaskName = "remote-build" }},
		{name: "task length", mutate: func(value *SpawnAgentParams) { value.TaskName += "a" }},
		{name: "blank message", mutate: func(value *SpawnAgentParams) { value.Message = " \n\t" }},
		{name: "message length", mutate: func(value *SpawnAgentParams) { value.Message += "m" }},
		{name: "message NUL", mutate: func(value *SpawnAgentParams) { value.Message = "run\x00task" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := valid
			test.mutate(&input)
			if err := input.Validate(); err == nil {
				t.Fatal("invalid spawn parameters were accepted")
			}
		})
	}
}

func TestAgentSummaryValidateStatusAndManagedIdentity(t *testing.T) {
	root := control.NewRootPrincipal(
		"123e4567-e89b-42d3-a456-426614174302",
		"123e4567-e89b-42d3-a456-426614174303",
		"123e4567-e89b-42d3-a456-426614174304",
		protocolAgentTargetID,
	)
	worker := control.NewWorkerPrincipal(
		root.ControllerID,
		root.TreeID,
		"123e4567-e89b-42d3-a456-426614174305",
		root.AgentID,
		protocolAgentTargetID,
	)
	valid := AgentSummary{
		SpawnID:     protocolAgentSpawnID,
		Principal:   worker.Identity(),
		TaskName:    "remote_build",
		SpawnStatus: AgentSpawnStarted,
		Sequence:    1,
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid agent summary: %v", err)
	}

	failed := valid
	failed.SpawnStatus = AgentSpawnFailed
	failed.SpawnFailureCode = "mcp_injection_blocked"
	if err := failed.Validate(); err != nil {
		t.Fatalf("valid failed agent summary: %v", err)
	}

	invalid := []AgentSummary{valid, valid, valid, valid}
	invalid[0].Principal = root.Identity()
	invalid[1].SpawnStatus = AgentSpawnFailed
	invalid[2].SpawnFailureCode = "unexpected"
	invalid[3].Sequence = MaximumAgentsPerTree + 1
	for index, summary := range invalid {
		if err := summary.Validate(); err == nil {
			t.Fatalf("invalid agent summary %d was accepted", index)
		}
	}
}

func TestAgentSpawnResultsValidateAttemptOutcomeAndDurableStatus(t *testing.T) {
	root := control.NewRootPrincipal(
		"123e4567-e89b-42d3-a456-426614174302",
		"123e4567-e89b-42d3-a456-426614174303",
		"123e4567-e89b-42d3-a456-426614174304",
		protocolAgentTargetID,
	)
	worker := control.NewWorkerPrincipal(
		root.ControllerID,
		root.TreeID,
		"123e4567-e89b-42d3-a456-426614174305",
		root.AgentID,
		protocolAgentTargetID,
	).Identity()
	agent := AgentSummary{
		SpawnID: protocolAgentSpawnID, Principal: worker, TaskName: "remote_build",
		SpawnStatus: AgentSpawnPending, Sequence: 1,
	}
	tests := []struct {
		name        string
		outcome     AgentSpawnOutcome
		status      AgentSpawnStatus
		failureCode string
		valid       bool
	}{
		{name: "indeterminate", outcome: AgentSpawnOutcomeIndeterminate, status: AgentSpawnPending, valid: true},
		{name: "busy", outcome: AgentSpawnOutcomeBusy, status: AgentSpawnPending, valid: true},
		{name: "started", outcome: AgentSpawnOutcomeStarted, status: AgentSpawnStarted, valid: true},
		{name: "failed", outcome: AgentSpawnOutcomeFailed, status: AgentSpawnFailed, failureCode: "worker_failed", valid: true},
		{name: "busy started", outcome: AgentSpawnOutcomeBusy, status: AgentSpawnStarted},
		{name: "busy failure", outcome: AgentSpawnOutcomeBusy, status: AgentSpawnPending, failureCode: "worker_failed"},
		{name: "failed pending", outcome: AgentSpawnOutcomeFailed, status: AgentSpawnPending, failureCode: "worker_failed"},
		{name: "unknown", outcome: "unknown", status: AgentSpawnPending},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			current := agent
			current.SpawnStatus = test.status
			current.SpawnFailureCode = test.failureCode
			result := SpawnAgentResult{Agent: current, Outcome: test.outcome}
			if err := result.Validate(); (err == nil) != test.valid {
				t.Fatalf("spawn agent result validation = %v, want valid %v", err, test.valid)
			}
			workerResult := SpawnWorkerResult{
				SpawnID: protocolAgentSpawnID, Principal: worker,
				Outcome: test.outcome, FailureCode: test.failureCode,
			}
			workerValid := test.outcome == AgentSpawnOutcomeIndeterminate && test.failureCode == "" ||
				test.outcome == AgentSpawnOutcomeBusy && test.failureCode == "" ||
				test.outcome == AgentSpawnOutcomeStarted && test.failureCode == "" ||
				test.outcome == AgentSpawnOutcomeFailed && test.failureCode == "worker_failed"
			if err := workerResult.Validate(); (err == nil) != workerValid {
				t.Fatalf("spawn worker result validation = %v, want valid %v", err, workerValid)
			}
		})
	}
}

func TestProjectAgentEffectiveStateMapping(t *testing.T) {
	tests := []struct {
		name              string
		spawn             AgentSpawnStatus
		spawnFailure      string
		phase             WorkerLifecyclePhase
		lifecycleFailure  string
		freshness         AgentLifecycleFreshness
		wantStatus        AgentEffectiveStatus
		wantFailure       string
		wantFailureSource AgentFailureSource
	}{
		{name: "spawn failure wins missing", spawn: AgentSpawnFailed, spawnFailure: "dispatch_failed", freshness: AgentLifecycleMissing, wantStatus: AgentEffectiveFailed, wantFailure: "dispatch_failed", wantFailureSource: AgentFailureSourceSpawn},
		{name: "spawn failure wins lifecycle", spawn: AgentSpawnFailed, spawnFailure: "dispatch_failed", phase: WorkerLifecycleFailed, lifecycleFailure: "turn_failed", freshness: AgentLifecycleOffline, wantStatus: AgentEffectiveFailed, wantFailure: "dispatch_failed", wantFailureSource: AgentFailureSourceSpawn},
		{name: "lifecycle failure current", spawn: AgentSpawnPending, phase: WorkerLifecycleFailed, lifecycleFailure: "thread_start_failed", freshness: AgentLifecycleCurrent, wantStatus: AgentEffectiveFailed, wantFailure: "thread_start_failed", wantFailureSource: AgentFailureSourceLifecycle},
		{name: "lifecycle failure offline remains terminal", spawn: AgentSpawnStarted, phase: WorkerLifecycleFailed, lifecycleFailure: "turn_failed", freshness: AgentLifecycleOffline, wantStatus: AgentEffectiveFailed, wantFailure: "turn_failed", wantFailureSource: AgentFailureSourceLifecycle},
		{name: "reserved", spawn: AgentSpawnPending, phase: WorkerLifecycleReserved, freshness: AgentLifecycleCurrent, wantStatus: AgentEffectiveStarting},
		{name: "pending", spawn: AgentSpawnPending, phase: WorkerLifecyclePending, freshness: AgentLifecycleCurrent, wantStatus: AgentEffectiveStarting},
		{name: "starting", spawn: AgentSpawnStarted, phase: WorkerLifecycleStarting, freshness: AgentLifecycleCurrent, wantStatus: AgentEffectiveStarting},
		{name: "preflight", spawn: AgentSpawnStarted, phase: WorkerLifecyclePreflight, freshness: AgentLifecycleCurrent, wantStatus: AgentEffectiveStarting},
		{name: "ready", spawn: AgentSpawnStarted, phase: WorkerLifecycleReady, freshness: AgentLifecycleCurrent, wantStatus: AgentEffectiveStarting},
		{name: "running", spawn: AgentSpawnPending, phase: WorkerLifecycleRunning, freshness: AgentLifecycleCurrent, wantStatus: AgentEffectiveRunning},
		{name: "finalizing", spawn: AgentSpawnStarted, phase: WorkerLifecycleFinalizing, freshness: AgentLifecycleCurrent, wantStatus: AgentEffectiveFinalizing},
		{name: "idle", spawn: AgentSpawnPending, phase: WorkerLifecycleIdle, freshness: AgentLifecycleCurrent, wantStatus: AgentEffectiveIdle},
		{name: "interrupted", spawn: AgentSpawnStarted, phase: WorkerLifecycleInterrupted, freshness: AgentLifecycleCurrent, wantStatus: AgentEffectiveInterrupted},
		{name: "syncing running", spawn: AgentSpawnStarted, phase: WorkerLifecycleRunning, freshness: AgentLifecycleSyncing, wantStatus: AgentEffectiveIndeterminate},
		{name: "offline idle", spawn: AgentSpawnStarted, phase: WorkerLifecycleIdle, freshness: AgentLifecycleOffline, wantStatus: AgentEffectiveIndeterminate},
		{name: "missing", spawn: AgentSpawnPending, freshness: AgentLifecycleMissing, wantStatus: AgentEffectiveIndeterminate},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			status, failure, source := ProjectAgentEffectiveState(
				test.spawn, test.spawnFailure, test.phase, test.lifecycleFailure, test.freshness,
			)
			if status != test.wantStatus || failure != test.wantFailure || source != test.wantFailureSource {
				t.Fatalf("projection = (%q, %q, %q), want (%q, %q, %q)", status, failure, source, test.wantStatus, test.wantFailure, test.wantFailureSource)
			}
		})
	}
}

func TestAgentWireContractUsesExplicitSpawnAndProjectionFields(t *testing.T) {
	root := control.NewRootPrincipal(
		"123e4567-e89b-42d3-a456-426614174302",
		"123e4567-e89b-42d3-a456-426614174303",
		"123e4567-e89b-42d3-a456-426614174304",
		protocolAgentTargetID,
	)
	worker := control.NewWorkerPrincipal(
		root.ControllerID, root.TreeID, "123e4567-e89b-42d3-a456-426614174305",
		root.AgentID, protocolAgentTargetID,
	).Identity()
	receipt := AgentSummary{
		SpawnID: protocolAgentSpawnID, Principal: worker, TaskName: "wire_contract",
		SpawnStatus: AgentSpawnFailed, SpawnFailureCode: "dispatch_failed", Sequence: 1,
	}
	state := AgentState{
		SpawnID: receipt.SpawnID, Principal: receipt.Principal, TaskName: receipt.TaskName,
		SpawnStatus: receipt.SpawnStatus, SpawnFailureCode: receipt.SpawnFailureCode,
		Sequence: receipt.Sequence, LifecyclePhase: WorkerLifecycleFailed,
		LifecycleFailureCode: "turn_failed", LifecycleTargetRevision: 2,
		LifecycleObservedAt: 3, LifecycleFreshness: AgentLifecycleCurrent,
		EffectiveStatus: AgentEffectiveFailed, EffectiveFailureCode: "dispatch_failed",
		FailureSource: AgentFailureSourceSpawn, TargetDispatchable: false,
	}
	for name, value := range map[string]any{
		"spawn receipt": receipt,
		"agent state":   state,
	} {
		encoded, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(encoded, &fields); err != nil {
			t.Fatal(err)
		}
		for _, required := range []string{"spawnStatus", "spawnFailureCode"} {
			if _, found := fields[required]; !found {
				t.Fatalf("%s omitted %q: %s", name, required, encoded)
			}
		}
		for _, forbidden := range []string{"status", "failureCode"} {
			if _, found := fields[forbidden]; found {
				t.Fatalf("%s retained ambiguous %q alias: %s", name, forbidden, encoded)
			}
		}
	}

	encoded, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"lifecyclePhase", "lifecycleFailureCode", "lifecycleTargetRevision",
		"lifecycleObservedAt", "lifecycleFreshness", "effectiveStatus",
		"effectiveFailureCode", "failureSource", "targetDispatchable",
	} {
		if _, found := fields[required]; !found {
			t.Fatalf("agent state omitted %q: %s", required, encoded)
		}
	}
}

func TestAgentStateRejectsInvalidLifecycleProjection(t *testing.T) {
	root := control.NewRootPrincipal(
		"123e4567-e89b-42d3-a456-426614174302",
		"123e4567-e89b-42d3-a456-426614174303",
		"123e4567-e89b-42d3-a456-426614174304",
		protocolAgentTargetID,
	)
	worker := control.NewWorkerPrincipal(
		root.ControllerID, root.TreeID, "123e4567-e89b-42d3-a456-426614174305",
		root.AgentID, protocolAgentTargetID,
	).Identity()
	valid := AgentState{
		SpawnID: protocolAgentSpawnID, Principal: worker, TaskName: "projection_validation",
		SpawnStatus: AgentSpawnStarted, Sequence: 1, LifecyclePhase: WorkerLifecycleRunning,
		LifecycleTargetRevision: 1, LifecycleObservedAt: 1, LifecycleFreshness: AgentLifecycleCurrent,
		EffectiveStatus: AgentEffectiveRunning, TargetDispatchable: true,
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid agent state: %v", err)
	}
	invalid := []AgentState{valid, valid, valid, valid}
	invalid[0].LifecycleFreshness = AgentLifecycleOffline
	invalid[0].EffectiveStatus = AgentEffectiveIndeterminate
	invalid[1].LifecycleTargetRevision = math.MaxInt64 + 1
	invalid[2].EffectiveStatus = AgentEffectiveIdle
	invalid[3].LifecycleFreshness = AgentLifecycleMissing
	for index, state := range invalid {
		if err := state.Validate(); err == nil {
			t.Fatalf("invalid agent state %d was accepted", index)
		}
	}
}
