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
	lifecycleConnectionOne   = "123e4567-e89b-42d3-a456-426614175100"
	lifecycleConnectionTwo   = "123e4567-e89b-42d3-a456-426614175101"
	lifecycleConnectionThree = "123e4567-e89b-42d3-a456-426614175102"
	lifecycleAgentOne        = "123e4567-e89b-42d3-a456-426614175110"
	lifecycleAgentTwo        = "123e4567-e89b-42d3-a456-426614175111"
	lifecycleMissingAgent    = "123e4567-e89b-42d3-a456-426614175112"
	lifecycleWrongTree       = "123e4567-e89b-42d3-a456-426614175120"
	lifecycleSecondThread    = "123e4567-e89b-42d3-a456-426614175121"
)

func TestWorkerLifecycleSessionClaimFencesConnectionsAndRejectsPeerRollback(t *testing.T) {
	registry, root := prepareAgentSpawnStore(t)
	agent := beginLifecycleAgent(t, registry, root, lifecycleAgentOne, agentSpawnTargetID, "lifecycle_target")
	session := lifecycleSession(t, registry, lifecycleConnectionOne)

	if applied := claimLifecycleSession(t, registry, session, 2); applied != 0 {
		t.Fatalf("initial applied revision = %d", applied)
	}
	first := applyLifecyclePage(t, registry, session, 0, 1, lifecycleSnapshotFor(agent, 1, protocol.WorkerLifecycleRunning))
	if first.AppliedRevision != 1 {
		t.Fatalf("first applied revision = %d", first.AppliedRevision)
	}

	rejected := session
	rejected.ConnectionID = lifecycleConnectionTwo
	if _, err := registry.ClaimWorkerLifecycleSession(context.Background(), WorkerLifecycleSessionClaim{
		Session: rejected, WorkerRevision: 0,
	}); !errors.Is(err, ErrWorkerLifecyclePeerBehind) {
		t.Fatalf("peer rollback claim error = %v, want ErrWorkerLifecyclePeerBehind", err)
	}
	second := applyLifecyclePage(t, registry, session, 1, 2, lifecycleSnapshotFor(agent, 2, protocol.WorkerLifecycleIdle))
	if second.AppliedRevision != 2 {
		t.Fatalf("second applied revision = %d", second.AppliedRevision)
	}

	if applied := claimLifecycleSession(t, registry, rejected, 2); applied != 2 {
		t.Fatalf("replacement applied revision = %d", applied)
	}
	_, err := registry.ApplyWorkerLifecyclePage(context.Background(), WorkerLifecyclePageApply{
		Session: session,
		Page: protocol.SyncWorkerLifecycleParams{
			BaseRevision: 2, ThroughRevision: 3, Complete: true,
		},
		ObservedAt: time.Unix(30, 0),
	})
	if !errors.Is(err, ErrWorkerLifecycleSessionFenced) {
		t.Fatalf("old connection apply error = %v, want fenced", err)
	}

	device, err := registry.RegisterTrustedDevice(
		context.Background(), deviceDescriptor(testControllerID, agentSpawnTargetID), time.Unix(40, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = registry.ApplyWorkerLifecyclePage(context.Background(), WorkerLifecyclePageApply{
		Session: rejected,
		Page: protocol.SyncWorkerLifecycleParams{
			BaseRevision: 2, ThroughRevision: 3, Complete: true,
		},
		ObservedAt: time.Unix(41, 0),
	})
	if !errors.Is(err, ErrWorkerLifecycleSessionFenced) {
		t.Fatalf("stale lease apply error = %v, want fenced", err)
	}
	replacement := rejected
	replacement.ConnectionID = lifecycleConnectionThree
	replacement.LeaseRevision = device.Revision
	if applied := claimLifecycleSession(t, registry, replacement, 2); applied != 2 {
		t.Fatalf("new lease applied revision = %d", applied)
	}
}

func TestWorkerLifecyclePageIsAtomicAndRejectsStaleOrUnauthorizedTargets(t *testing.T) {
	registry, root := prepareAgentSpawnStore(t)
	valid := beginLifecycleAgent(t, registry, root, lifecycleAgentOne, agentSpawnTargetID, "valid_target")
	session := lifecycleSession(t, registry, lifecycleConnectionOne)
	claimLifecycleSession(t, registry, session, 10)

	invalid := protocol.WorkerLifecycleSnapshot{
		TreeID: root.TreeID, AgentID: lifecycleMissingAgent, Revision: 2,
		Phase: protocol.WorkerLifecycleRunning,
	}
	_, err := registry.ApplyWorkerLifecyclePage(context.Background(), WorkerLifecyclePageApply{
		Session: session,
		Page: protocol.SyncWorkerLifecycleParams{
			ThroughRevision: 2, Complete: true,
			Workers: []protocol.WorkerLifecycleSnapshot{
				lifecycleSnapshotFor(valid, 1, protocol.WorkerLifecycleRunning), invalid,
			},
		},
		ObservedAt: time.Unix(20, 0),
	})
	if !errors.Is(err, ErrAuthorizationDenied) {
		t.Fatalf("invalid page error = %v, want authorization denial", err)
	}
	assertLifecycleStorage(t, registry, root, 0, 0, 0)

	applyLifecyclePage(t, registry, session, 0, 1, lifecycleSnapshotFor(valid, 1, protocol.WorkerLifecycleRunning))
	_, err = registry.ApplyWorkerLifecyclePage(context.Background(), WorkerLifecyclePageApply{
		Session: session,
		Page: protocol.SyncWorkerLifecycleParams{
			BaseRevision: 0, ThroughRevision: 2, Complete: true,
			Workers: []protocol.WorkerLifecycleSnapshot{
				lifecycleSnapshotFor(valid, 2, protocol.WorkerLifecycleIdle),
			},
		},
		ObservedAt: time.Unix(21, 0),
	})
	if !errors.Is(err, ErrWorkerLifecycleStaleBase) {
		t.Fatalf("stale base error = %v, want ErrWorkerLifecycleStaleBase", err)
	}
	assertLifecycleStorage(t, registry, root, 1, 1, 1)

	wrongTree := lifecycleSnapshotFor(valid, 2, protocol.WorkerLifecycleIdle)
	wrongTree.TreeID = lifecycleWrongTree
	assertLifecycleApplyDenied(t, registry, session, 1, 2, wrongTree)

	wrongDevice := beginLifecycleAgent(
		t, registry, root, lifecycleAgentTwo, testDeviceID, "wrong_device_target",
	)
	assertLifecycleApplyDenied(
		t, registry, session, 1, 2,
		lifecycleSnapshotFor(wrongDevice, 2, protocol.WorkerLifecycleRunning),
	)

	principal, err := registry.CreateWorkerPrincipal(
		context.Background(), root.ControllerID, root.TreeID, lifecycleMissingAgent,
		root.AgentID, agentSpawnTargetID, time.Unix(22, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	assertLifecycleApplyDenied(t, registry, session, 1, 2, protocol.WorkerLifecycleSnapshot{
		TreeID: root.TreeID, AgentID: principal.AgentID, Revision: 2,
		Phase: protocol.WorkerLifecycleRunning,
	})
	assertLifecycleStorage(t, registry, root, 1, 1, 1)
}

func TestWorkerLifecycleActivityIsIndependentFromDispatchAndTreeScoped(t *testing.T) {
	registry, root := prepareAgentSpawnStore(t)
	first := beginLifecycleAgent(t, registry, root, lifecycleAgentOne, agentSpawnTargetID, "first_target")
	second := beginLifecycleAgent(t, registry, root, lifecycleAgentTwo, agentSpawnTargetID, "second_target")
	session := lifecycleSession(t, registry, lifecycleConnectionOne)
	claimLifecycleSession(t, registry, session, 5)

	applyLifecyclePage(
		t, registry, session, 0, 2,
		lifecycleSnapshotFor(first, 1, protocol.WorkerLifecycleRunning),
		lifecycleSnapshotFor(second, 2, protocol.WorkerLifecycleReady),
	)
	agents, err := registry.ListAgents(context.Background(), root.Identity(), AgentPageRequest{Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if agents.Agents[0].Status != protocol.AgentSpawnPending || agents.Agents[1].Status != protocol.AgentSpawnPending {
		t.Fatalf("lifecycle changed dispatch receipts: %#v", agents.Agents)
	}
	if _, err := registry.MarkAgentSpawnStarted(
		context.Background(), keyForReceipt(first), time.Unix(30, 0),
	); err != nil {
		t.Fatal(err)
	}

	page, err := registry.ListAgentLifecycleActivity(
		context.Background(), root.Identity(), AgentLifecyclePageRequest{Limit: 1},
	)
	if err != nil || len(page.Activities) != 1 || page.NextSequence != 1 || page.Highwater != 2 {
		t.Fatalf("first lifecycle page = %#v, error %v", page, err)
	}
	page, err = registry.ListAgentLifecycleActivity(context.Background(), root.Identity(), AgentLifecyclePageRequest{
		AfterSequence: page.NextSequence, Limit: 2,
	})
	if err != nil || len(page.Activities) != 1 || page.NextSequence != 2 || page.Highwater != 2 {
		t.Fatalf("second lifecycle page = %#v, error %v", page, err)
	}

	applyLifecyclePage(
		t, registry, session, 2, 3,
		lifecycleSnapshotFor(first, 3, protocol.WorkerLifecycleRunning),
	)
	unchanged, err := registry.ListAgentLifecycleActivity(
		context.Background(), root.Identity(), AgentLifecyclePageRequest{AfterSequence: 2, Limit: 2},
	)
	if err != nil || len(unchanged.Activities) != 0 || unchanged.NextSequence != 2 || unchanged.Highwater != 2 {
		t.Fatalf("same-phase lifecycle activity = %#v, error %v", unchanged, err)
	}
	var targetRevision uint64
	if err := registry.db.QueryRowContext(context.Background(), `
SELECT target_revision FROM agent_lifecycle_states
WHERE controller_id = ? AND tree_id = ? AND agent_id = ?
`, root.ControllerID, root.TreeID, first.Agent.Principal.AgentID).Scan(&targetRevision); err != nil {
		t.Fatal(err)
	}
	if targetRevision != 3 {
		t.Fatalf("same-phase target revision = %d", targetRevision)
	}

	applyLifecyclePage(
		t, registry, session, 3, 4,
		lifecycleSnapshotFor(first, 4, protocol.WorkerLifecycleIdle),
	)
	changed, err := registry.ListAgentLifecycleActivity(
		context.Background(), root.Identity(), AgentLifecyclePageRequest{AfterSequence: 2, Limit: 2},
	)
	if err != nil || len(changed.Activities) != 1 || changed.Activities[0].Sequence != 3 ||
		changed.Activities[0].Phase != protocol.WorkerLifecycleIdle {
		t.Fatalf("changed lifecycle activity = %#v, error %v", changed, err)
	}
	if _, err := registry.ListAgentLifecycleActivity(
		context.Background(), root.Identity(), AgentLifecyclePageRequest{AfterSequence: 4, Limit: 1},
	); !errors.Is(err, ErrAgentLifecycleCursorAhead) {
		t.Fatalf("cursor ahead error = %v, want ErrAgentLifecycleCursorAhead", err)
	}

	_, otherRoot, err := registry.EnsureRootTree(
		context.Background(), testControllerID, lifecycleSecondThread, testDeviceID, time.Unix(40, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	otherPage, err := registry.ListAgentLifecycleActivity(
		context.Background(), otherRoot.Identity(), AgentLifecyclePageRequest{Limit: 2},
	)
	if err != nil || len(otherPage.Activities) != 0 || otherPage.Highwater != 0 {
		t.Fatalf("other tree lifecycle page = %#v, error %v", otherPage, err)
	}
}

func TestWorkerLifecycleResponseLossResumesWithoutDuplicateTreeSequence(t *testing.T) {
	registry, root := prepareAgentSpawnStore(t)
	agent := beginLifecycleAgent(t, registry, root, lifecycleAgentOne, agentSpawnTargetID, "response_loss")
	session := lifecycleSession(t, registry, lifecycleConnectionOne)
	claimLifecycleSession(t, registry, session, 1)
	page := protocol.SyncWorkerLifecycleParams{
		ThroughRevision: 1, Complete: true,
		Workers: []protocol.WorkerLifecycleSnapshot{
			lifecycleSnapshotFor(agent, 1, protocol.WorkerLifecycleRunning),
		},
	}
	if _, err := registry.ApplyWorkerLifecyclePage(context.Background(), WorkerLifecyclePageApply{
		Session: session, Page: page, ObservedAt: time.Unix(20, 0),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.ApplyWorkerLifecyclePage(context.Background(), WorkerLifecyclePageApply{
		Session: session, Page: page, ObservedAt: time.Unix(21, 0),
	}); !errors.Is(err, ErrWorkerLifecycleStaleBase) {
		t.Fatalf("lost-response replay error = %v, want stale base", err)
	}
	assertLifecycleStorage(t, registry, root, 1, 1, 1)

	resumed := session
	resumed.ConnectionID = lifecycleConnectionTwo
	if applied := claimLifecycleSession(t, registry, resumed, 1); applied != 1 {
		t.Fatalf("resumed applied revision = %d", applied)
	}
	if _, err := registry.ApplyWorkerLifecyclePage(context.Background(), WorkerLifecyclePageApply{
		Session: resumed,
		Page: protocol.SyncWorkerLifecycleParams{
			BaseRevision: 1, ThroughRevision: 1, Complete: true,
		},
		ObservedAt: time.Unix(22, 0),
	}); err != nil {
		t.Fatal(err)
	}
	assertLifecycleStorage(t, registry, root, 1, 1, 1)
}

func TestWorkerLifecycleRejectsNonForwardStoredRevisionAtomically(t *testing.T) {
	registry, root := prepareAgentSpawnStore(t)
	agent := beginLifecycleAgent(t, registry, root, lifecycleAgentOne, agentSpawnTargetID, "forward_only")
	session := lifecycleSession(t, registry, lifecycleConnectionOne)
	claimLifecycleSession(t, registry, session, 2)
	applyLifecyclePage(t, registry, session, 0, 1, lifecycleSnapshotFor(agent, 1, protocol.WorkerLifecycleRunning))
	if _, err := registry.db.ExecContext(context.Background(), `
UPDATE peer_worker_sync_cursors SET applied_revision = 0
WHERE controller_id = ? AND device_id = ?
`, session.ControllerID, session.DeviceID); err != nil {
		t.Fatal(err)
	}
	_, err := registry.ApplyWorkerLifecyclePage(context.Background(), WorkerLifecyclePageApply{
		Session: session,
		Page: protocol.SyncWorkerLifecycleParams{
			ThroughRevision: 1, Complete: true,
			Workers: []protocol.WorkerLifecycleSnapshot{
				lifecycleSnapshotFor(agent, 1, protocol.WorkerLifecycleIdle),
			},
		},
		ObservedAt: time.Unix(30, 0),
	})
	if !errors.Is(err, ErrWorkerLifecycleRevisionNotForward) {
		t.Fatalf("non-forward revision error = %v, want forward-only rejection", err)
	}
	assertLifecycleStorage(t, registry, root, 1, 1, 0)
	activity, err := registry.ListAgentLifecycleActivity(
		context.Background(), root.Identity(), AgentLifecyclePageRequest{Limit: 1},
	)
	if err != nil || activity.Activities[0].Phase != protocol.WorkerLifecycleRunning {
		t.Fatalf("non-forward rollback activity = %#v, error %v", activity, err)
	}
}

func beginLifecycleAgent(
	t *testing.T,
	registry *Store,
	root control.Principal,
	agentID, targetDeviceID, taskName string,
) AgentSpawnReceipt {
	t.Helper()
	receipt, err := registry.BeginAgentSpawn(context.Background(), AgentSpawnIntent{
		Source: root.Identity(), SpawnID: agentID, AgentID: agentID,
		TargetDeviceID: targetDeviceID, TaskName: taskName,
		PromptDigest: sha256.Sum256([]byte(taskName)),
	}, time.Unix(10, 0))
	if err != nil {
		t.Fatal(err)
	}
	return receipt
}

func lifecycleSession(t *testing.T, registry *Store, connectionID string) WorkerLifecycleSession {
	t.Helper()
	record, err := registry.DescribeDevice(context.Background(), testControllerID, agentSpawnTargetID)
	if err != nil {
		t.Fatal(err)
	}
	return WorkerLifecycleSession{
		ControllerID: testControllerID, DeviceID: agentSpawnTargetID,
		ConnectionID: connectionID, LeaseRevision: record.Device.Revision,
	}
}

func claimLifecycleSession(
	t *testing.T,
	registry *Store,
	session WorkerLifecycleSession,
	workerRevision uint64,
) uint64 {
	t.Helper()
	applied, err := registry.ClaimWorkerLifecycleSession(context.Background(), WorkerLifecycleSessionClaim{
		Session: session, WorkerRevision: workerRevision,
	})
	if err != nil {
		t.Fatal(err)
	}
	return applied
}

func applyLifecyclePage(
	t *testing.T,
	registry *Store,
	session WorkerLifecycleSession,
	baseRevision, throughRevision uint64,
	workers ...protocol.WorkerLifecycleSnapshot,
) protocol.SyncWorkerLifecycleResult {
	t.Helper()
	result, err := registry.ApplyWorkerLifecyclePage(context.Background(), WorkerLifecyclePageApply{
		Session: session,
		Page: protocol.SyncWorkerLifecycleParams{
			BaseRevision: baseRevision, ThroughRevision: throughRevision,
			Complete: true, Workers: workers,
		},
		ObservedAt: time.Unix(20+int64(throughRevision), 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func lifecycleSnapshotFor(
	receipt AgentSpawnReceipt,
	revision uint64,
	phase protocol.WorkerLifecyclePhase,
) protocol.WorkerLifecycleSnapshot {
	return protocol.WorkerLifecycleSnapshot{
		TreeID: receipt.Agent.Principal.TreeID, AgentID: receipt.Agent.Principal.AgentID,
		Revision: revision, Phase: phase,
	}
}

func assertLifecycleApplyDenied(
	t *testing.T,
	registry *Store,
	session WorkerLifecycleSession,
	baseRevision, throughRevision uint64,
	snapshot protocol.WorkerLifecycleSnapshot,
) {
	t.Helper()
	_, err := registry.ApplyWorkerLifecyclePage(context.Background(), WorkerLifecyclePageApply{
		Session: session,
		Page: protocol.SyncWorkerLifecycleParams{
			BaseRevision: baseRevision, ThroughRevision: throughRevision,
			Complete: true, Workers: []protocol.WorkerLifecycleSnapshot{snapshot},
		},
		ObservedAt: time.Unix(30, 0),
	})
	if !errors.Is(err, ErrAuthorizationDenied) {
		t.Fatalf("unauthorized lifecycle target error = %v, want authorization denial", err)
	}
}

func assertLifecycleStorage(
	t *testing.T,
	registry *Store,
	root control.Principal,
	wantStates, wantTreeSequence int,
	wantApplied uint64,
) {
	t.Helper()
	var states, treeSequence int
	if err := registry.db.QueryRowContext(context.Background(), `
SELECT count(*) FROM agent_lifecycle_states
WHERE controller_id = ? AND tree_id = ?
`, root.ControllerID, root.TreeID).Scan(&states); err != nil {
		t.Fatal(err)
	}
	if err := registry.db.QueryRowContext(context.Background(), `
SELECT last_lifecycle_sequence FROM trees
WHERE controller_id = ? AND tree_id = ?
`, root.ControllerID, root.TreeID).Scan(&treeSequence); err != nil {
		t.Fatal(err)
	}
	var applied uint64
	if err := registry.db.QueryRowContext(context.Background(), `
SELECT applied_revision FROM peer_worker_sync_cursors
WHERE controller_id = ? AND device_id = ?
`, root.ControllerID, agentSpawnTargetID).Scan(&applied); err != nil {
		t.Fatal(err)
	}
	got := []any{states, treeSequence, applied}
	want := []any{wantStates, wantTreeSequence, wantApplied}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("lifecycle storage = %v, want %v", got, want)
	}
}

func TestWorkerLifecycleSessionValidation(t *testing.T) {
	valid := WorkerLifecycleSession{
		ControllerID: testControllerID, DeviceID: agentSpawnTargetID,
		ConnectionID: lifecycleConnectionOne, LeaseRevision: 1,
	}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	for index, mutate := range []func(*WorkerLifecycleSession){
		func(value *WorkerLifecycleSession) { value.ControllerID = "invalid" },
		func(value *WorkerLifecycleSession) { value.DeviceID = "invalid" },
		func(value *WorkerLifecycleSession) { value.ConnectionID = "invalid" },
		func(value *WorkerLifecycleSession) { value.LeaseRevision = 0 },
	} {
		changed := valid
		mutate(&changed)
		if err := changed.Validate(); err == nil {
			t.Fatalf("invalid session mutation %d was accepted: %s", index, fmt.Sprint(changed))
		}
	}
}
