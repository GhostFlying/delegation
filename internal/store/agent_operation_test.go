package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/protocol"
)

const (
	agentOperationID       = "123e4567-e89b-42d3-a456-426614174400"
	agentOperationSecondID = "123e4567-e89b-42d3-a456-426614174401"
	agentOperationAgentID  = "123e4567-e89b-42d3-a456-426614174410"
	agentOperationOtherID  = "123e4567-e89b-42d3-a456-426614174411"
	secondOperationThread  = "123e4567-e89b-42d3-a456-426614174420"
)

func TestAgentOperationReceiptIsIdempotentQueryableAndPayloadFree(t *testing.T) {
	registry, root := prepareAgentSpawnStore(t)
	agent := startOperationAgent(t, registry, root, agentOperationAgentID, "operation_target")
	payload := []byte("private operation message that must not be stored")
	intent := AgentOperationIntent{
		Source:        root.Identity(),
		OperationID:   agentOperationID,
		AgentID:       agent.Agent.Principal.AgentID,
		Action:        protocol.AgentOperationSend,
		PayloadDigest: sha256.Sum256(payload),
	}
	created, err := registry.BeginAgentOperation(context.Background(), intent, time.Unix(20, 0))
	if err != nil {
		t.Fatal(err)
	}
	if created.Outcome != protocol.AgentOperationOutcomePending || created.CreatedAt != 20 ||
		created.TargetDeviceID != agentSpawnTargetID {
		t.Fatalf("created receipt = %#v", created)
	}
	repeated, err := registry.BeginAgentOperation(context.Background(), intent, time.Unix(30, 0))
	if err != nil || !reflect.DeepEqual(repeated, created) {
		t.Fatalf("idempotent receipt = %#v, error %v", repeated, err)
	}
	queried, err := registry.GetAgentOperation(context.Background(), root.Identity(), agentOperationID)
	if err != nil || !reflect.DeepEqual(queried, created) {
		t.Fatalf("queried receipt = %#v, error %v", queried, err)
	}

	var plaintextColumns int
	if err := registry.db.QueryRowContext(context.Background(), `
SELECT count(*)
FROM pragma_table_info('agent_operation_receipts')
WHERE name IN ('message', 'prompt', 'payload')
`).Scan(&plaintextColumns); err != nil {
		t.Fatal(err)
	}
	if plaintextColumns != 0 {
		t.Fatalf("agent operation schema stores %d plaintext columns", plaintextColumns)
	}
	var storedDigest []byte
	if err := registry.db.QueryRowContext(context.Background(), `
SELECT payload_digest
FROM agent_operation_receipts
WHERE controller_id = ? AND tree_id = ? AND source_agent_id = ? AND operation_id = ?
`, created.Key.ControllerID, created.Key.TreeID, created.Key.SourceAgentID,
		created.Key.OperationID).Scan(&storedDigest); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(storedDigest, intent.PayloadDigest[:]) || bytes.Contains(storedDigest, payload) {
		t.Fatalf("stored payload digest = %x", storedDigest)
	}
}

func TestAgentOperationRejectsChangedArgumentsAndUnauthorizedSources(t *testing.T) {
	registry, root := prepareAgentSpawnStore(t)
	first := startOperationAgent(t, registry, root, agentOperationAgentID, "first_operation_target")
	second := startOperationAgent(t, registry, root, agentOperationOtherID, "second_operation_target")
	intent := AgentOperationIntent{
		Source:        root.Identity(),
		OperationID:   agentOperationID,
		AgentID:       first.Agent.Principal.AgentID,
		Action:        protocol.AgentOperationFollowup,
		PayloadDigest: sha256.Sum256([]byte("first follow-up")),
	}
	if _, err := registry.BeginAgentOperation(context.Background(), intent, time.Unix(20, 0)); err != nil {
		t.Fatal(err)
	}
	mutations := []func(*AgentOperationIntent){
		func(value *AgentOperationIntent) { value.AgentID = second.Agent.Principal.AgentID },
		func(value *AgentOperationIntent) {
			value.AgentID = "123e4567-e89b-42d3-a456-426614174499"
		},
		func(value *AgentOperationIntent) { value.Action = protocol.AgentOperationInterrupt },
		func(value *AgentOperationIntent) {
			value.PayloadDigest = sha256.Sum256([]byte("changed follow-up"))
		},
	}
	for index, mutate := range mutations {
		changed := intent
		mutate(&changed)
		if _, err := registry.BeginAgentOperation(
			context.Background(), changed, time.Unix(30, 0),
		); !errors.Is(err, ErrConflict) {
			t.Fatalf("changed argument %d error = %v, want ErrConflict", index, err)
		}
	}

	workerIntent := intent
	workerIntent.Source = first.Agent.Principal
	workerIntent.OperationID = agentOperationSecondID
	workerIntent.AgentID = second.Agent.Principal.AgentID
	if _, err := registry.BeginAgentOperation(
		context.Background(), workerIntent, time.Unix(30, 0),
	); !errors.Is(err, ErrAuthorizationDenied) {
		t.Fatalf("worker source error = %v, want authorization denial", err)
	}
	if _, err := registry.GetAgentOperation(
		context.Background(), first.Agent.Principal, agentOperationID,
	); !errors.Is(err, ErrAuthorizationDenied) {
		t.Fatalf("worker lookup error = %v, want authorization denial", err)
	}

	_, otherRoot, err := registry.EnsureRootTree(
		context.Background(), testControllerID, secondOperationThread, testDeviceID, time.Unix(3, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	otherAgent := startOperationAgent(
		t, registry, otherRoot, "123e4567-e89b-42d3-a456-426614174412", "other_tree_target",
	)
	crossTree := intent
	crossTree.OperationID = agentOperationSecondID
	crossTree.AgentID = otherAgent.Agent.Principal.AgentID
	if _, err := registry.BeginAgentOperation(
		context.Background(), crossTree, time.Unix(30, 0),
	); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-tree target error = %v, want ErrNotFound", err)
	}
	crossTree.Source = otherRoot.Identity()
	crossTree.AgentID = first.Agent.Principal.AgentID
	if _, err := registry.BeginAgentOperation(
		context.Background(), crossTree, time.Unix(30, 0),
	); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-tree source error = %v, want ErrNotFound", err)
	}

	if _, err := registry.db.ExecContext(context.Background(), `
UPDATE principals
SET parent_agent_id = ?
WHERE controller_id = ? AND tree_id = ? AND agent_id = ?
`, second.Agent.Principal.AgentID, root.ControllerID, root.TreeID,
		first.Agent.Principal.AgentID); err != nil {
		t.Fatal(err)
	}
	nonDescendant := intent
	nonDescendant.OperationID = "123e4567-e89b-42d3-a456-426614174402"
	if _, err := registry.BeginAgentOperation(
		context.Background(), nonDescendant, time.Unix(30, 0),
	); !errors.Is(err, ErrAuthorizationDenied) {
		t.Fatalf("non-descendant error = %v, want authorization denial", err)
	}
}

func TestAgentOperationRequiresStartedAgent(t *testing.T) {
	registry, root := prepareAgentSpawnStore(t)
	pending := beginOperationAgent(
		t, registry, root, agentOperationAgentID, "pending_operation_target",
	)
	intent := AgentOperationIntent{
		Source:        root.Identity(),
		OperationID:   agentOperationID,
		AgentID:       pending.Agent.Principal.AgentID,
		Action:        protocol.AgentOperationInterrupt,
		PayloadDigest: sha256.Sum256(nil),
	}
	if _, err := registry.BeginAgentOperation(
		context.Background(), intent, time.Unix(20, 0),
	); !errors.Is(err, ErrConflict) {
		t.Fatalf("pending target error = %v, want ErrConflict", err)
	}
	failed := beginOperationAgent(
		t, registry, root, agentOperationOtherID, "failed_operation_target",
	)
	if _, err := registry.MarkAgentSpawnFailed(
		context.Background(), keyForReceipt(failed), "worker_failed", time.Unix(11, 0),
	); err != nil {
		t.Fatal(err)
	}
	intent.OperationID = agentOperationSecondID
	intent.AgentID = failed.Agent.Principal.AgentID
	if _, err := registry.BeginAgentOperation(
		context.Background(), intent, time.Unix(20, 0),
	); !errors.Is(err, ErrConflict) {
		t.Fatalf("failed target error = %v, want ErrConflict", err)
	}
}

func TestAgentOperationTerminalReplayIsStable(t *testing.T) {
	registry, root := prepareAgentSpawnStore(t)
	agent := startOperationAgent(t, registry, root, agentOperationAgentID, "terminal_target")
	intent := AgentOperationIntent{
		Source:        root.Identity(),
		OperationID:   agentOperationID,
		AgentID:       agent.Agent.Principal.AgentID,
		Action:        protocol.AgentOperationSend,
		PayloadDigest: sha256.Sum256([]byte("queued message")),
	}
	pending, err := registry.BeginAgentOperation(context.Background(), intent, time.Unix(20, 0))
	if err != nil {
		t.Fatal(err)
	}
	finished, err := registry.FinishAgentOperation(
		context.Background(), pending.Key, protocol.AgentOperationOutcomeQueued, "", time.Unix(30, 0),
	)
	if err != nil || finished.Outcome != protocol.AgentOperationOutcomeQueued || finished.UpdatedAt != 30 {
		t.Fatalf("finished receipt = %#v, error %v", finished, err)
	}
	repeated, err := registry.FinishAgentOperation(
		context.Background(), pending.Key, protocol.AgentOperationOutcomeQueued, "", time.Unix(40, 0),
	)
	if err != nil || !reflect.DeepEqual(repeated, finished) {
		t.Fatalf("terminal replay = %#v, error %v", repeated, err)
	}
	if _, err := registry.FinishAgentOperation(
		context.Background(), pending.Key, protocol.AgentOperationOutcomeSteered, "", time.Unix(40, 0),
	); !errors.Is(err, ErrConflict) {
		t.Fatalf("changed terminal outcome error = %v, want ErrConflict", err)
	}

	failedIntent := intent
	failedIntent.OperationID = agentOperationSecondID
	failedIntent.Action = protocol.AgentOperationFollowup
	failedIntent.PayloadDigest = sha256.Sum256([]byte("follow-up that failed"))
	failedPending, err := registry.BeginAgentOperation(
		context.Background(), failedIntent, time.Unix(50, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	failed, err := registry.FinishAgentOperation(
		context.Background(), failedPending.Key, protocol.AgentOperationOutcomeFailed,
		"app_server_unavailable", time.Unix(60, 0),
	)
	if err != nil || failed.FailureCode != "app_server_unavailable" {
		t.Fatalf("failed receipt = %#v, error %v", failed, err)
	}
	failedReplay, err := registry.FinishAgentOperation(
		context.Background(), failedPending.Key, protocol.AgentOperationOutcomeFailed,
		"app_server_unavailable", time.Unix(70, 0),
	)
	if err != nil || !reflect.DeepEqual(failedReplay, failed) {
		t.Fatalf("failed replay = %#v, error %v", failedReplay, err)
	}
	if _, err := registry.FinishAgentOperation(
		context.Background(), failedPending.Key, protocol.AgentOperationOutcomeFailed,
		"different_failure", time.Unix(70, 0),
	); !errors.Is(err, ErrConflict) {
		t.Fatalf("changed failure error = %v, want ErrConflict", err)
	}
	stored, err := registry.GetAgentOperation(
		context.Background(), root.Identity(), failedPending.Key.OperationID,
	)
	if err != nil || !reflect.DeepEqual(stored, failed) {
		t.Fatalf("stored terminal receipt = %#v, error %v", stored, err)
	}
}

func TestQueuedAgentMessageAndReceiptCommitAtomically(t *testing.T) {
	registry, root := prepareAgentSpawnStore(t)
	agent := startOperationAgent(t, registry, root, agentOperationAgentID, "queued_target")
	message := "queued message"
	pending, err := registry.BeginAgentOperation(context.Background(), AgentOperationIntent{
		Source:        root.Identity(),
		OperationID:   agentOperationID,
		AgentID:       agent.Agent.Principal.AgentID,
		Action:        protocol.AgentOperationSend,
		PayloadDigest: sha256.Sum256([]byte(message)),
	}, time.Unix(20, 0))
	if err != nil {
		t.Fatal(err)
	}
	queued, delivery, err := registry.QueueAgentMessageAndFinishOperation(
		context.Background(), pending.Key, message, time.Unix(30, 0),
	)
	if err != nil || queued.Outcome != protocol.AgentOperationOutcomeQueued ||
		delivery.RecipientAgentID != agent.Agent.Principal.AgentID || delivery.Sequence != 1 {
		t.Fatalf("queued receipt = %#v, delivery = %#v, error %v", queued, delivery, err)
	}
	worker := control.NewWorkerPrincipal(
		agent.Agent.Principal.ControllerID,
		agent.Agent.Principal.TreeID,
		agent.Agent.Principal.AgentID,
		agent.Agent.Principal.ParentAgentID,
		agent.Agent.Principal.DeviceID,
	)
	page, err := registry.ReadMailbox(context.Background(), worker, 0, 1)
	if err != nil || len(page.Messages) != 1 || page.Messages[0].MessageID != agentOperationID ||
		page.Messages[0].Message != message {
		t.Fatalf("queued mailbox page = %#v, error %v", page, err)
	}
	if _, err := registry.ReadMailbox(
		context.Background(), worker, delivery.Sequence, 1,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.db.ExecContext(
		context.Background(), "DELETE FROM mailbox_receipts WHERE message_id = ?", agentOperationID,
	); err != nil {
		t.Fatal(err)
	}
	replayed, replayDelivery, err := registry.QueueAgentMessageAndFinishOperation(
		context.Background(), pending.Key, message, time.Unix(40, 0),
	)
	if err != nil || !reflect.DeepEqual(replayed, queued) || replayDelivery != (MailboxDelivery{}) {
		t.Fatalf("queued replay = %#v, delivery = %#v, error %v", replayed, replayDelivery, err)
	}
	empty, err := registry.ReadMailbox(
		context.Background(), worker, delivery.Sequence, 1,
	)
	if err != nil || len(empty.Messages) != 0 || empty.NextCursor != delivery.Sequence {
		t.Fatalf("mailbox after receipt pruning = %#v, error %v", empty, err)
	}
}

func TestQueuedAgentMessageConflictRollsBackTerminalReceipt(t *testing.T) {
	registry, root := prepareAgentSpawnStore(t)
	agent := startOperationAgent(t, registry, root, agentOperationAgentID, "conflict_target")
	message := "intended queued message"
	pending, err := registry.BeginAgentOperation(context.Background(), AgentOperationIntent{
		Source:        root.Identity(),
		OperationID:   agentOperationID,
		AgentID:       agent.Agent.Principal.AgentID,
		Action:        protocol.AgentOperationSend,
		PayloadDigest: sha256.Sum256([]byte(message)),
	}, time.Unix(20, 0))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.SendMailboxMessage(
		context.Background(),
		root,
		protocol.MessageTarget{Kind: protocol.MessageTargetAgent, AgentID: agent.Agent.Principal.AgentID},
		agentOperationID,
		"conflicting mailbox payload",
		time.Unix(21, 0),
	); err != nil {
		t.Fatal(err)
	}
	if _, _, err := registry.QueueAgentMessageAndFinishOperation(
		context.Background(), pending.Key, message, time.Unix(30, 0),
	); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflicting queue error = %v, want ErrConflict", err)
	}
	stored, err := registry.GetAgentOperation(
		context.Background(), root.Identity(), pending.Key.OperationID,
	)
	if err != nil || stored.Outcome != protocol.AgentOperationOutcomePending {
		t.Fatalf("receipt after mailbox rollback = %#v, error %v", stored, err)
	}
}

func startOperationAgent(
	t *testing.T,
	registry *Store,
	root control.Principal,
	agentID, taskName string,
) AgentSpawnReceipt {
	t.Helper()
	receipt := beginOperationAgent(t, registry, root, agentID, taskName)
	started, err := registry.MarkAgentSpawnStarted(
		context.Background(), keyForReceipt(receipt), time.Unix(11, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	return started
}

func beginOperationAgent(
	t *testing.T,
	registry *Store,
	root control.Principal,
	agentID, taskName string,
) AgentSpawnReceipt {
	t.Helper()
	receipt, err := registry.BeginAgentSpawn(context.Background(), AgentSpawnIntent{
		Source:         root.Identity(),
		SpawnID:        agentID,
		AgentID:        agentID,
		TargetDeviceID: agentSpawnTargetID,
		TaskName:       taskName,
		PromptDigest:   sha256.Sum256([]byte(taskName)),
	}, time.Unix(10, 0))
	if err != nil {
		t.Fatal(err)
	}
	return receipt
}
