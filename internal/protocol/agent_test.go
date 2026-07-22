package protocol

import (
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
		SpawnID:   protocolAgentSpawnID,
		Principal: worker.Identity(),
		TaskName:  "remote_build",
		Status:    AgentSpawnStarted,
		Sequence:  1,
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid agent summary: %v", err)
	}

	failed := valid
	failed.Status = AgentSpawnFailed
	failed.FailureCode = "mcp_injection_blocked"
	if err := failed.Validate(); err != nil {
		t.Fatalf("valid failed agent summary: %v", err)
	}

	invalid := []AgentSummary{valid, valid, valid, valid}
	invalid[0].Principal = root.Identity()
	invalid[1].Status = AgentSpawnFailed
	invalid[2].FailureCode = "unexpected"
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
		Status: AgentSpawnPending, Sequence: 1,
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
			current.Status = test.status
			current.FailureCode = test.failureCode
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
