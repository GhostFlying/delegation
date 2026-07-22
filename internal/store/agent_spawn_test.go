package store

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/protocol"
)

const (
	agentSpawnThreadID = "123e4567-e89b-42d3-a456-426614174060"
	agentSpawnTargetID = "123e4567-e89b-42d3-a456-426614174061"
	agentSpawnID       = "123e4567-e89b-42d3-a456-426614174062"
	agentSpawnAgentID  = "123e4567-e89b-42d3-a456-426614174063"
)

func TestAgentSpawnReceiptIsAtomicIdempotentAndPromptFree(t *testing.T) {
	registry, root := prepareAgentSpawnStore(t)
	message := "validate a bounded remote build"
	intent := AgentSpawnIntent{
		Source: root.Identity(), SpawnID: agentSpawnID, AgentID: agentSpawnAgentID,
		TargetDeviceID: agentSpawnTargetID, TaskName: "remote_build",
		PromptDigest: sha256.Sum256([]byte(message)),
	}
	created, err := registry.BeginAgentSpawn(context.Background(), intent, time.Unix(10, 0))
	if err != nil {
		t.Fatal(err)
	}
	if created.Agent.SpawnID != intent.SpawnID || created.Agent.Principal.AgentID != intent.AgentID ||
		created.Agent.Principal.ParentAgentID != root.AgentID ||
		created.Agent.Principal.DeviceID != agentSpawnTargetID ||
		created.Agent.Status != protocol.AgentSpawnPending || created.Agent.Sequence != 1 {
		t.Fatalf("created agent receipt = %#v", created)
	}
	retry := intent
	retry.AgentID = "123e4567-e89b-42d3-a456-426614174064"
	repeated, err := registry.BeginAgentSpawn(context.Background(), retry, time.Unix(20, 0))
	if err != nil || !reflect.DeepEqual(repeated, created) {
		t.Fatalf("idempotent receipt = %#v, error %v", repeated, err)
	}
	if authorized, err := registry.AuthorizePrincipal(
		context.Background(), created.Agent.Principal, control.CapabilityMessageSendParent,
	); err != nil || authorized.Identity() != created.Agent.Principal {
		t.Fatalf("stored worker principal = %#v, error %v", authorized, err)
	}
	var messageColumns int
	if err := registry.db.QueryRowContext(context.Background(), `
SELECT count(*) FROM pragma_table_info('agent_spawn_receipts') WHERE name IN ('message', 'prompt')
`).Scan(&messageColumns); err != nil {
		t.Fatal(err)
	}
	if messageColumns != 0 {
		t.Fatalf("agent spawn schema stores %d prompt columns", messageColumns)
	}
}

func TestAgentSpawnRejectsSemanticConflictsAndUnauthorizedSource(t *testing.T) {
	registry, root := prepareAgentSpawnStore(t)
	intent := AgentSpawnIntent{
		Source: root.Identity(), SpawnID: agentSpawnID, AgentID: agentSpawnAgentID,
		TargetDeviceID: agentSpawnTargetID, TaskName: "remote_build",
		PromptDigest: sha256.Sum256([]byte("first message")),
	}
	created, err := registry.BeginAgentSpawn(context.Background(), intent, time.Unix(10, 0))
	if err != nil {
		t.Fatal(err)
	}
	mutations := []func(*AgentSpawnIntent){
		func(value *AgentSpawnIntent) { value.TargetDeviceID = testDeviceID },
		func(value *AgentSpawnIntent) { value.TaskName = "another_task" },
		func(value *AgentSpawnIntent) { value.PromptDigest = sha256.Sum256([]byte("changed message")) },
	}
	for _, mutate := range mutations {
		changed := intent
		changed.AgentID = "123e4567-e89b-42d3-a456-426614174064"
		mutate(&changed)
		if _, err := registry.BeginAgentSpawn(
			context.Background(), changed, time.Unix(20, 0),
		); !errors.Is(err, ErrConflict) {
			t.Fatalf("changed spawn error = %v, want ErrConflict", err)
		}
	}
	second := intent
	second.SpawnID = "123e4567-e89b-42d3-a456-426614174065"
	second.AgentID = "123e4567-e89b-42d3-a456-426614174066"
	if _, err := registry.BeginAgentSpawn(
		context.Background(), second, time.Unix(20, 0),
	); !errors.Is(err, ErrConflict) {
		t.Fatalf("duplicate taskName error = %v, want ErrConflict", err)
	}
	workerIntent := intent
	workerIntent.Source = created.Agent.Principal
	workerIntent.SpawnID = "123e4567-e89b-42d3-a456-426614174067"
	workerIntent.AgentID = "123e4567-e89b-42d3-a456-426614174068"
	workerIntent.TaskName = "recursive_task"
	if _, err := registry.BeginAgentSpawn(
		context.Background(), workerIntent, time.Unix(20, 0),
	); !errors.Is(err, ErrAuthorizationDenied) {
		t.Fatalf("worker spawn error = %v, want authorization denial", err)
	}
}

func TestAgentSpawnTerminalStateAndStablePagination(t *testing.T) {
	registry, root := prepareAgentSpawnStore(t)
	ctx := context.Background()
	var receipts []AgentSpawnReceipt
	for index := range 5 {
		intent := AgentSpawnIntent{
			Source:         root.Identity(),
			SpawnID:        fmt.Sprintf("123e4567-e89b-42d3-a456-%012x", 0x100+index),
			AgentID:        fmt.Sprintf("123e4567-e89b-42d3-a456-%012x", 0x200+index),
			TargetDeviceID: agentSpawnTargetID,
			TaskName:       fmt.Sprintf("task_%d", index),
			PromptDigest:   sha256.Sum256([]byte(fmt.Sprintf("message %d", index))),
		}
		receipt, err := registry.BeginAgentSpawn(ctx, intent, time.Unix(int64(10+index), 0))
		if err != nil {
			t.Fatal(err)
		}
		receipts = append(receipts, receipt)
	}
	key := keyForReceipt(receipts[0])
	started, err := registry.MarkAgentSpawnStarted(ctx, key, time.Unix(30, 0))
	if err != nil || started.Agent.Status != protocol.AgentSpawnStarted || started.UpdatedAt != 30 {
		t.Fatalf("started receipt = %#v, error %v", started, err)
	}
	repeated, err := registry.MarkAgentSpawnStarted(ctx, key, time.Unix(31, 0))
	if err != nil || !reflect.DeepEqual(repeated, started) {
		t.Fatalf("repeated started receipt = %#v, error %v", repeated, err)
	}
	if _, err := registry.MarkAgentSpawnFailed(
		ctx, key, "worker_failed", time.Unix(32, 0),
	); !errors.Is(err, ErrConflict) {
		t.Fatalf("opposing terminal state error = %v, want ErrConflict", err)
	}
	failed, err := registry.MarkAgentSpawnFailed(
		ctx, keyForReceipt(receipts[1]), "mcp_injection_blocked", time.Unix(31, 0),
	)
	if err != nil || failed.Agent.Status != protocol.AgentSpawnFailed ||
		failed.Agent.FailureCode != "mcp_injection_blocked" {
		t.Fatalf("failed receipt = %#v, error %v", failed, err)
	}
	first, err := registry.ListAgents(ctx, root.Identity(), AgentPageRequest{Limit: 2})
	if err != nil || len(first.Agents) != 2 || first.NextSequence != 2 {
		t.Fatalf("first agent page = %#v, error %v", first, err)
	}
	second, err := registry.ListAgents(
		ctx, root.Identity(), AgentPageRequest{AfterSequence: first.NextSequence, Limit: 2},
	)
	if err != nil || len(second.Agents) != 2 || second.NextSequence != 4 {
		t.Fatalf("second agent page = %#v, error %v", second, err)
	}
	last, err := registry.ListAgents(
		ctx, root.Identity(), AgentPageRequest{AfterSequence: second.NextSequence, Limit: 2},
	)
	if err != nil || len(last.Agents) != 1 || last.NextSequence != 0 {
		t.Fatalf("last agent page = %#v, error %v", last, err)
	}
	all := append(append(first.Agents, second.Agents...), last.Agents...)
	for index, agent := range all {
		if agent.Sequence != uint64(index+1) || agent.Principal.TreeID != root.TreeID {
			t.Fatalf("agent page entry %d = %#v", index, agent)
		}
	}
}

func TestAgentSpawnLimitPreservesExistingIdempotency(t *testing.T) {
	registry, root := prepareAgentSpawnStore(t)
	ctx := context.Background()
	var firstIntent AgentSpawnIntent
	var firstReceipt AgentSpawnReceipt
	for index := range protocol.MaximumAgentsPerTree {
		intent := AgentSpawnIntent{
			Source:         root.Identity(),
			SpawnID:        fmt.Sprintf("123e4567-e89b-42d3-a456-%012x", 0x1000+index),
			AgentID:        fmt.Sprintf("123e4567-e89b-42d3-a456-%012x", 0x2000+index),
			TargetDeviceID: agentSpawnTargetID,
			TaskName:       fmt.Sprintf("limited_task_%d", index),
			PromptDigest:   sha256.Sum256([]byte(fmt.Sprintf("limited message %d", index))),
		}
		receipt, err := registry.BeginAgentSpawn(
			ctx, intent, time.Unix(int64(10+index), 0),
		)
		if err != nil {
			t.Fatalf("create agent %d: %v", index, err)
		}
		if index == 0 {
			firstIntent = intent
			firstReceipt = receipt
		}
	}

	overflow := AgentSpawnIntent{
		Source:         root.Identity(),
		SpawnID:        "123e4567-e89b-42d3-a456-426614176000",
		AgentID:        "123e4567-e89b-42d3-a456-426614176001",
		TargetDeviceID: agentSpawnTargetID,
		TaskName:       "one_too_many",
		PromptDigest:   sha256.Sum256([]byte("overflow")),
	}
	if _, err := registry.BeginAgentSpawn(
		ctx, overflow, time.Unix(1000, 0),
	); !errors.Is(err, ErrAgentLimit) {
		t.Fatalf("overflow error = %v, want ErrAgentLimit", err)
	}

	firstIntent.AgentID = "123e4567-e89b-42d3-a456-426614176002"
	repeated, err := registry.BeginAgentSpawn(ctx, firstIntent, time.Unix(1001, 0))
	if err != nil || !reflect.DeepEqual(repeated, firstReceipt) {
		t.Fatalf("full-tree idempotent retry = %#v, error %v", repeated, err)
	}
}

func prepareAgentSpawnStore(t *testing.T) (*Store, control.Principal) {
	t.Helper()
	registry := openTestStore(t)
	ctx := context.Background()
	for _, deviceID := range []string{testDeviceID, agentSpawnTargetID} {
		if _, err := registry.RegisterTrustedDevice(
			ctx, deviceDescriptor(testControllerID, deviceID), time.Unix(1, 0),
		); err != nil {
			t.Fatal(err)
		}
	}
	_, root, err := registry.EnsureRootTree(
		ctx, testControllerID, agentSpawnThreadID, testDeviceID, time.Unix(2, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	return registry, root
}

func keyForReceipt(receipt AgentSpawnReceipt) AgentSpawnKey {
	return AgentSpawnKey{
		ControllerID:  receipt.Agent.Principal.ControllerID,
		TreeID:        receipt.Agent.Principal.TreeID,
		SourceAgentID: receipt.Agent.Principal.ParentAgentID,
		SpawnID:       receipt.Agent.SpawnID,
	}
}
