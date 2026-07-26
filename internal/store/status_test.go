package store

import (
	"context"
	"crypto/sha256"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/protocol"
)

const (
	statusPendingAgentID = "123e4567-e89b-42d3-a456-426614176100"
	statusFailedAgentID  = "123e4567-e89b-42d3-a456-426614176101"
	statusFollowupID     = "123e4567-e89b-42d3-a456-426614176102"
	statusThirdArtifact  = "123e4567-e89b-42d3-a456-426614176103"
	statusThirdTurn      = "123e4567-e89b-42d3-a456-426614176104"
)

func TestStatusSnapshotCountsDurableStateAndLifetimeStarts(t *testing.T) {
	ctx := context.Background()
	registry, root, worker, manifest, manifestHash := prepareChangesArtifactStore(t, true, true)

	spawnKey := AgentSpawnKey{
		ControllerID:  root.ControllerID,
		TreeID:        root.TreeID,
		SourceAgentID: root.AgentID,
		SpawnID:       agentSpawnID,
	}
	if _, err := registry.MarkAgentSpawnStarted(ctx, spawnKey, time.Unix(8, 0)); err != nil {
		t.Fatal(err)
	}
	pending := beginLifecycleAgent(
		t, registry, root, statusPendingAgentID, agentSpawnTargetID, "status_pending",
	)
	failed := beginLifecycleAgent(
		t, registry, root, statusFailedAgentID, agentSpawnTargetID, "status_failed",
	)
	if _, err := registry.MarkAgentSpawnFailed(
		ctx, keyForReceipt(failed), "worker_failed", time.Unix(12, 0),
	); err != nil {
		t.Fatal(err)
	}

	followup, err := registry.BeginAgentOperation(ctx, AgentOperationIntent{
		Source:        root.Identity(),
		OperationID:   statusFollowupID,
		AgentID:       worker.AgentID,
		Action:        protocol.AgentOperationFollowup,
		PayloadDigest: sha256.Sum256([]byte("status followup")),
	}, time.Unix(13, 0))
	if err != nil {
		t.Fatal(err)
	}
	startedFollowup, err := registry.FinishAgentOperation(
		ctx, followup.Key, protocol.AgentOperationOutcomeStarted, "", time.Unix(14, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	replayedFollowup, err := registry.FinishAgentOperation(
		ctx, followup.Key, protocol.AgentOperationOutcomeStarted, "", time.Unix(15, 0),
	)
	if err != nil || !reflect.DeepEqual(replayedFollowup, startedFollowup) {
		t.Fatalf("replayed followup = %#v, want %#v, error %v", replayedFollowup, startedFollowup, err)
	}

	available := testChangesArtifactParams(manifest, manifestHash)
	if _, err := registry.PublishChangesArtifact(
		ctx, worker.DeviceID, worker, available, time.Unix(20, 0),
	); err != nil {
		t.Fatal(err)
	}
	unchanged := available
	unchanged.ArtifactID = changesSecondID
	unchanged.TurnID = changesSecondTurn
	unchanged.Status = protocol.ChangesArtifactUnchanged
	unchanged.ResultHeadOID = unchanged.BaseHeadOID
	unchanged.ResultSnapshotHash = unchanged.BaseSnapshotHash
	unchanged.ResultClean = true
	unchanged.Parts = []protocol.WorkspaceArtifactDescriptor{}
	if _, err := registry.PublishChangesArtifact(
		ctx, worker.DeviceID, worker, unchanged, time.Unix(21, 0),
	); err != nil {
		t.Fatal(err)
	}
	captureFailed := available
	captureFailed.ArtifactID = statusThirdArtifact
	captureFailed.TurnID = statusThirdTurn
	captureFailed.Status = protocol.ChangesArtifactCaptureFailed
	captureFailed.ResultHeadOID = ""
	captureFailed.ResultSnapshotHash = ""
	captureFailed.ResultClean = false
	captureFailed.Parts = []protocol.WorkspaceArtifactDescriptor{}
	captureFailed.ResultWarnings = []string{}
	captureFailed.FailureCode = "capture_failed"
	if _, err := registry.PublishChangesArtifact(
		ctx, worker.DeviceID, worker, captureFailed, time.Unix(22, 0),
	); err != nil {
		t.Fatal(err)
	}

	session := lifecycleSession(t, registry, lifecycleConnectionOne)
	claimLifecycleSession(t, registry, session, 3)
	applyLifecyclePage(t, registry, session, 0, 3,
		protocol.WorkerLifecycleSnapshot{
			TreeID: root.TreeID, AgentID: worker.AgentID, Revision: 1,
			Phase:         protocol.WorkerLifecycleRunning,
			CodexThreadID: lifecycleCodexThreadOne, ActiveTurnID: lifecycleTurnOne,
		},
		lifecycleSnapshotFor(pending, 2, protocol.WorkerLifecycleReady),
		protocol.WorkerLifecycleSnapshot{
			TreeID: root.TreeID, AgentID: failed.Agent.Principal.AgentID, Revision: 3,
			Phase: protocol.WorkerLifecycleFailed, FailureCode: "worker_failed",
		},
	)

	rootDevice, err := registry.DescribeDevice(ctx, root.ControllerID, root.DeviceID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.MarkDeviceOffline(
		ctx, root.ControllerID, root.DeviceID, rootDevice.Device.Revision, time.Unix(30, 0),
	); err != nil {
		t.Fatal(err)
	}

	got, err := registry.ReadStatusSnapshot(
		ctx, root.ControllerID, []string{worker.DeviceID, worker.DeviceID},
	)
	if err != nil {
		t.Fatal(err)
	}
	want := StatusSnapshot{
		Devices:    StatusDeviceCounts{Total: 2, Online: 1},
		Trees:      1,
		Dispatches: StatusDispatchCounts{Pending: 1, Started: 1, Failed: 1},
		Workers:    StatusWorkerCounts{Running: 1, Occupied: 2},
		Artifacts:  StatusArtifactCounts{Available: 1, Unchanged: 1, CaptureFailed: 1},
		Lifetime:   StatusLifetimeCounters{DispatchesStarted: 1, TurnsStarted: 2},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("status snapshot = %#v, want %#v", got, want)
	}
}

func TestStatusSnapshotExcludesWorkersOnUnsynchronizedDevices(t *testing.T) {
	ctx := context.Background()
	registry, root, worker, _, _ := prepareChangesArtifactStore(t, true, true)
	session := lifecycleSession(t, registry, lifecycleConnectionOne)
	claimLifecycleSession(t, registry, session, 1)
	applyLifecyclePage(t, registry, session, 0, 1,
		protocol.WorkerLifecycleSnapshot{
			TreeID: root.TreeID, AgentID: worker.AgentID, Revision: 1,
			Phase:         protocol.WorkerLifecycleRunning,
			CodexThreadID: lifecycleCodexThreadOne, ActiveTurnID: lifecycleTurnOne,
		},
	)

	withoutReadyPeer, err := registry.ReadStatusSnapshot(ctx, root.ControllerID, nil)
	if err != nil {
		t.Fatal(err)
	}
	withReadyPeer, err := registry.ReadStatusSnapshot(
		ctx, root.ControllerID, []string{worker.DeviceID},
	)
	if err != nil {
		t.Fatal(err)
	}
	if withoutReadyPeer.Workers != (StatusWorkerCounts{}) {
		t.Fatalf("workers without a sync-ready peer = %#v", withoutReadyPeer.Workers)
	}
	if withReadyPeer.Workers != (StatusWorkerCounts{Running: 1, Occupied: 1}) {
		t.Fatalf("workers with a sync-ready peer = %#v", withReadyPeer.Workers)
	}
}

func TestPeerStatusSnapshotUsesAdmissionStatesAndArtifactBacklogs(t *testing.T) {
	ctx := context.Background()
	state := openPeerTestStore(t)
	statuses := []WorkerStatus{
		WorkerReserved,
		WorkerPending,
		WorkerStarting,
		WorkerPreflight,
		WorkerReady,
		WorkerRunning,
		WorkerFinalizing,
		WorkerIdle,
		WorkerInterrupted,
		WorkerFailed,
	}
	workers := make([]WorkerReservation, 0, len(statuses))
	for index, status := range statuses {
		worker := workerReservation(t, changesTestID(50_000+index), fmt.Sprintf("status %d", index))
		worker.Status = status
		worker.Revision = uint64(index + 1)
		worker.CreatedAt = int64(index + 1)
		worker.UpdatedAt = worker.CreatedAt
		if status == WorkerFinalizing {
			worker.ActiveTurnID = changesTestID(60_000 + index)
			worker.LastBoundTurnID = worker.ActiveTurnID
			worker.FinalTarget = WorkerIdle
		}
		insertPeerStatusWorker(t, state, worker)
		workers = append(workers, worker)
	}
	if _, err := state.db.ExecContext(ctx, `
UPDATE peer_metadata SET worker_revision = 77 WHERE singleton = 1
`); err != nil {
		t.Fatal(err)
	}
	insertPeerStatusArtifact(t, state, workers[0], 1, ChangesCapturePending)
	insertPeerStatusArtifact(t, state, workers[0], 2, ChangesPublishPending)
	insertPeerStatusArtifact(t, state, workers[0], 3, ChangesPublished)

	got, err := state.ReadPeerStatusSnapshot(ctx, workerControllerID, workerDeviceID)
	if err != nil {
		t.Fatal(err)
	}
	want := PeerStatusSnapshot{
		WorkerRevision: 77,
		Workers: PeerStatusWorkerCounts{
			Total: 10, Reserved: 1, Pending: 1, Starting: 1, Preflight: 1,
			Ready: 1, Running: 1, Finalizing: 1, Idle: 1, Interrupted: 1,
			Failed: 1, Occupied: 6,
		},
		Artifacts: PeerStatusArtifactCounts{
			CaptureBacklog: 1, PublishBacklog: 1, Retained: 1, RetainedBytes: 30,
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("peer status snapshot = %#v, want %#v", got, want)
	}
}

func insertPeerStatusWorker(t *testing.T, state *PeerStore, worker WorkerReservation) {
	t.Helper()
	_, err := state.db.ExecContext(context.Background(), `
INSERT INTO worker_reservations(
	controller_id, tree_id, agent_id, parent_agent_id, device_id,
	task_name, prompt_digest, workspace_id, workspace_path, working_directory,
	codex_thread_id, profile_version, status, retry_target, active_turn_id, last_bound_turn_id,
	failure_code, final_target_status, final_failure_code, revision, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`, worker.ControllerID, worker.TreeID, worker.AgentID, worker.ParentAgentID, worker.DeviceID,
		worker.TaskName, worker.PromptDigest, worker.WorkspaceID, worker.WorkspacePath,
		worker.WorkingDirectory, worker.CodexThreadID, worker.ProfileVersion, worker.Status,
		worker.RetryTarget, worker.ActiveTurnID, worker.LastBoundTurnID, worker.FailureCode, worker.FinalTarget,
		worker.FinalFailureCode, worker.Revision, worker.CreatedAt, worker.UpdatedAt)
	if err != nil {
		t.Fatal(err)
	}
}

func insertPeerStatusArtifact(
	t *testing.T,
	state *PeerStore,
	worker WorkerReservation,
	index int,
	artifactState ChangesArtifactState,
) {
	t.Helper()
	captureStatus := ""
	resultHeadOID := ""
	resultSnapshotHash := ""
	resultClean := 0
	bundlePartName := ""
	bundleSizeBytes := 0
	bundleSHA256 := ""
	retentionReserved := 0
	reservedBytes := 0
	payloadBytes := 0
	brokerSequence := 0
	if artifactState == ChangesPublishPending {
		captureStatus = string(ChangesUnchanged)
		resultHeadOID = strings.Repeat("a", 40)
		resultSnapshotHash = strings.Repeat("b", 64)
		resultClean = 1
	}
	if artifactState == ChangesPublished {
		captureStatus = string(ChangesAvailable)
		resultHeadOID = strings.Repeat("c", 40)
		resultSnapshotHash = strings.Repeat("d", 64)
		bundlePartName = ChangesBundlePartName
		bundleSizeBytes = 30
		bundleSHA256 = strings.Repeat("e", 64)
		retentionReserved = 1
		reservedBytes = 30
		payloadBytes = 30
		brokerSequence = 1
	}
	_, err := state.db.ExecContext(context.Background(), `
INSERT INTO peer_changes_artifacts(
	controller_id, tree_id, agent_id, turn_id, artifact_id, workspace_id,
	workspace_source_device_id, workspace_target_device_id,
	completion_target_status, completion_failure_code, state, capture_status,
	object_format, base_head_oid, base_clean, base_manifest_hash, base_snapshot_hash,
	base_warnings_json, result_head_oid, result_snapshot_hash, result_clean,
	bundle_part_name, bundle_size_bytes, bundle_sha256,
	retention_reserved, reserved_bytes, payload_bytes, broker_sequence, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'idle', '', ?, ?, 'sha1', ?, 1, ?, ?, '[]', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`, worker.ControllerID, worker.TreeID, worker.AgentID, changesTestID(70_000+index),
		changesTestID(80_000+index), changesTestID(90_000+index), changesSourceDeviceID,
		worker.DeviceID, artifactState, captureStatus, strings.Repeat("a", 40),
		strings.Repeat("f", 64), strings.Repeat("b", 64), resultHeadOID,
		resultSnapshotHash, resultClean, bundlePartName, bundleSizeBytes, bundleSHA256,
		retentionReserved, reservedBytes, payloadBytes, brokerSequence, index, index)
	if err != nil {
		t.Fatal(err)
	}
}
