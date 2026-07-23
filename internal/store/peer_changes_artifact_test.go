package store

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/protocol"
)

const changesSourceDeviceID = "123e4567-e89b-42d3-a456-426614174399"

func TestChangesArtifactFinalizationPersistsThroughBrokerAcknowledgement(t *testing.T) {
	ctx := context.Background()
	state := openPeerTestStore(t)
	start := time.Unix(1_000, 0)
	worker, workspace, turnID := prepareRunningChangesWorker(t, state, 1, true, start)

	finalization, err := state.BeginWorkerFinalization(
		ctx, worker.WorkerKey, turnID, WorkerIdle, "", start.Add(time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	if finalization.Worker.Status != WorkerFinalizing ||
		finalization.Worker.ActiveTurnID != turnID ||
		finalization.Worker.FinalTarget != WorkerIdle ||
		finalization.Artifact.State != ChangesCapturePending ||
		finalization.Artifact.BaseHeadOID != workspace.HeadOID ||
		finalization.Artifact.BaseClean != workspace.Clean ||
		!slices.Equal(finalization.Artifact.BaseWarnings, workspace.Warnings) ||
		len(finalization.Artifact.ResultWarnings) != 0 {
		t.Fatalf("initial finalization = %#v", finalization)
	}
	artifactID := finalization.Artifact.ArtifactID
	replay, err := state.BeginWorkerFinalization(
		ctx, worker.WorkerKey, turnID, WorkerIdle, "", start.Add(2*time.Second),
	)
	if err != nil || replay.Artifact.ArtifactID != artifactID ||
		replay.Worker.Revision != finalization.Worker.Revision {
		t.Fatalf("replayed finalization = %#v, %v", replay, err)
	}
	if _, err := state.BeginWorkerFinalization(
		ctx, worker.WorkerKey, turnID, WorkerFailed, "turn_failed", start.Add(2*time.Second),
	); !errors.Is(err, ErrChangesArtifactConflict) {
		t.Fatalf("conflicting completion error = %v", err)
	}

	other := workerReservation(t, changesTestID(900), "slot blocked")
	if _, err := state.ReserveWorkerStart(ctx, other, 1, start.Add(2*time.Second)); !errors.Is(
		err,
		ErrWorkerBusy,
	) {
		t.Fatalf("finalizing worker did not retain slot: %v", err)
	}
	captures, err := state.ListPendingChangesCaptures(ctx, workerControllerID, workerDeviceID, 10)
	if err != nil || len(captures) != 1 || captures[0].ArtifactID != artifactID {
		t.Fatalf("pending captures = %#v, %v", captures, err)
	}
	foreign, err := state.ListPendingChangesCaptures(ctx, workerControllerID, changesSourceDeviceID, 10)
	if err != nil || len(foreign) != 0 {
		t.Fatalf("foreign pending captures = %#v, %v", foreign, err)
	}

	reserved, err := state.ReserveChangesArtifactPayload(
		ctx, worker.WorkerKey, artifactID, 64, start.Add(3*time.Second),
	)
	if err != nil || !reserved.RetentionReserved || reserved.ReservedBytes != 64 {
		t.Fatalf("reserved artifact = %#v, %v", reserved, err)
	}
	reservedReplay, err := state.ReserveChangesArtifactPayload(
		ctx, worker.WorkerKey, artifactID, 64, start.Add(4*time.Second),
	)
	if err != nil || reservedReplay.UpdatedAt != reserved.UpdatedAt {
		t.Fatalf("replayed reservation = %#v, %v", reservedReplay, err)
	}
	if _, err := state.ReserveChangesArtifactPayload(
		ctx, worker.WorkerKey, artifactID, 63, start.Add(4*time.Second),
	); !errors.Is(err, ErrChangesArtifactConflict) {
		t.Fatalf("changed reservation error = %v", err)
	}

	result := ChangesCaptureResult{
		Status: ChangesAvailable, ResultHeadOID: strings.Repeat("b", 40),
		ResultSnapshotHash: strings.Repeat("c", 64), ResultClean: false,
		Parts: []ChangesArtifactPart{
			{Kind: ChangesArtifactOverlay, Name: ChangesOverlayPartName, SizeBytes: 20, SHA256: strings.Repeat("d", 64)},
			{Kind: ChangesArtifactBundle, Name: ChangesBundlePartName, SizeBytes: 10, SHA256: strings.Repeat("e", 64)},
		},
		ResultWarnings: []string{"lfs_payload_not_transferred"},
	}
	ready, err := state.CompleteChangesArtifactCapture(
		ctx, worker.WorkerKey, artifactID, result, start.Add(5*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	if ready.State != ChangesPublishPending || ready.PayloadBytes != 30 ||
		ready.ReservedBytes != 30 || len(ready.Parts) != 2 ||
		!slices.Equal(ready.BaseWarnings, workspace.Warnings) ||
		!slices.Equal(ready.ResultWarnings, result.ResultWarnings) ||
		ready.Parts[0].Kind != ChangesArtifactBundle || ready.Parts[1].Kind != ChangesArtifactOverlay {
		t.Fatalf("publication-ready artifact = %#v", ready)
	}
	replayedReady, err := state.CompleteChangesArtifactCapture(
		ctx, worker.WorkerKey, artifactID, result, start.Add(6*time.Second),
	)
	if err != nil || replayedReady.UpdatedAt != ready.UpdatedAt {
		t.Fatalf("replayed capture = %#v, %v", replayedReady, err)
	}
	changedWarnings := result
	changedWarnings.ResultWarnings = []string{"submodule_payload_not_included"}
	if _, err := state.CompleteChangesArtifactCapture(
		ctx, worker.WorkerKey, artifactID, changedWarnings, start.Add(6*time.Second),
	); !errors.Is(err, ErrChangesArtifactConflict) {
		t.Fatalf("changed capture warnings replay error = %v", err)
	}
	changed := result
	changed.ResultSnapshotHash = strings.Repeat("f", 64)
	if _, err := state.CompleteChangesArtifactCapture(
		ctx, worker.WorkerKey, artifactID, changed, start.Add(6*time.Second),
	); !errors.Is(err, ErrChangesArtifactConflict) {
		t.Fatalf("changed capture replay error = %v", err)
	}
	publications, err := state.ListPendingChangesPublications(
		ctx, workerControllerID, workerDeviceID, 10,
	)
	if err != nil || len(publications) != 1 || publications[0].ArtifactID != artifactID {
		t.Fatalf("pending publications = %#v, %v", publications, err)
	}

	acknowledged, err := state.AcknowledgeChangesArtifact(
		ctx, worker.WorkerKey, artifactID, 41, start.Add(7*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	if acknowledged.Worker.Status != WorkerIdle || acknowledged.Worker.ActiveTurnID != "" ||
		acknowledged.Worker.FinalTarget != "" ||
		acknowledged.Artifact.State != ChangesPublished ||
		acknowledged.Artifact.BrokerSequence != 41 || !acknowledged.Artifact.RetentionReserved {
		t.Fatalf("acknowledged finalization = %#v", acknowledged)
	}
	ackReplay, err := state.AcknowledgeChangesArtifact(
		ctx, worker.WorkerKey, artifactID, 41, start.Add(8*time.Second),
	)
	if err != nil || ackReplay.Worker.Revision != acknowledged.Worker.Revision ||
		ackReplay.Artifact.UpdatedAt != acknowledged.Artifact.UpdatedAt {
		t.Fatalf("replayed acknowledgement = %#v, %v", ackReplay, err)
	}
	if _, err := state.AcknowledgeChangesArtifact(
		ctx, worker.WorkerKey, artifactID, 42, start.Add(8*time.Second),
	); !errors.Is(err, ErrChangesArtifactConflict) {
		t.Fatalf("changed acknowledgement error = %v", err)
	}
	completedReplay, err := state.BeginWorkerFinalization(
		ctx, worker.WorkerKey, turnID, WorkerIdle, "", start.Add(9*time.Second),
	)
	if err != nil || completedReplay.Artifact.ArtifactID != artifactID ||
		completedReplay.Worker.Status != WorkerIdle {
		t.Fatalf("completed finalization replay = %#v, %v", completedReplay, err)
	}
}

func TestChangesArtifactPreservesDirtyBaseAndCaptureFailureSemantics(t *testing.T) {
	t.Run("dirty unchanged", func(t *testing.T) {
		ctx := context.Background()
		state := openPeerTestStore(t)
		start := time.Unix(2_000, 0)
		worker, workspace, turnID := prepareRunningChangesWorker(t, state, 10, false, start)
		finalization, err := state.BeginWorkerFinalization(
			ctx, worker.WorkerKey, turnID, WorkerIdle, "", start.Add(time.Second),
		)
		if err != nil {
			t.Fatal(err)
		}
		ready, err := state.CompleteChangesArtifactCapture(ctx, worker.WorkerKey,
			finalization.Artifact.ArtifactID, ChangesCaptureResult{
				Status: ChangesUnchanged, ResultHeadOID: workspace.HeadOID,
				ResultSnapshotHash: workspace.SourceSnapshotHash, ResultClean: false,
			}, start.Add(2*time.Second))
		if err != nil || ready.Status != ChangesUnchanged || ready.ResultClean {
			t.Fatalf("dirty unchanged artifact = %#v, %v", ready, err)
		}
	})

	t.Run("dirty base cleaned without payload", func(t *testing.T) {
		ctx := context.Background()
		state := openPeerTestStore(t)
		start := time.Unix(3_000, 0)
		worker, workspace, turnID := prepareRunningChangesWorker(t, state, 11, false, start)
		finalization, err := state.BeginWorkerFinalization(
			ctx, worker.WorkerKey, turnID, WorkerIdle, "", start.Add(time.Second),
		)
		if err != nil {
			t.Fatal(err)
		}
		ready, err := state.CompleteChangesArtifactCapture(ctx, worker.WorkerKey,
			finalization.Artifact.ArtifactID, ChangesCaptureResult{
				Status: ChangesAvailable, ResultHeadOID: workspace.HeadOID,
				ResultSnapshotHash: strings.Repeat("9", 64), ResultClean: true,
			}, start.Add(2*time.Second))
		if err != nil || ready.Status != ChangesAvailable || ready.PayloadBytes != 0 ||
			ready.RetentionReserved {
			t.Fatalf("zero-payload available artifact = %#v, %v", ready, err)
		}
	})

	t.Run("capture failure overrides idle target", func(t *testing.T) {
		ctx := context.Background()
		state := openPeerTestStore(t)
		start := time.Unix(4_000, 0)
		worker, _, turnID := prepareRunningChangesWorker(t, state, 12, true, start)
		finalization, err := state.BeginWorkerFinalization(
			ctx, worker.WorkerKey, turnID, WorkerIdle, "", start.Add(time.Second),
		)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := state.ReserveChangesArtifactPayload(
			ctx, worker.WorkerKey, finalization.Artifact.ArtifactID, 128, start.Add(2*time.Second),
		); err != nil {
			t.Fatal(err)
		}
		if _, err := state.CompleteChangesArtifactCapture(ctx, worker.WorkerKey,
			finalization.Artifact.ArtifactID, ChangesCaptureResult{
				Status: ChangesCaptureFailed, FailureCode: "git_history_diverged",
				ResultWarnings: []string{"lfs_payload_not_transferred"},
			}, start.Add(3*time.Second)); err == nil {
			t.Fatal("failed capture accepted result warnings")
		}
		ready, err := state.CompleteChangesArtifactCapture(ctx, worker.WorkerKey,
			finalization.Artifact.ArtifactID, ChangesCaptureResult{
				Status: ChangesCaptureFailed, FailureCode: "git_history_diverged",
			}, start.Add(3*time.Second))
		if err != nil || ready.RetentionReserved || ready.ReservedBytes != 0 ||
			!slices.Equal(ready.BaseWarnings, finalization.Artifact.BaseWarnings) ||
			len(ready.ResultWarnings) != 0 {
			t.Fatalf("failed capture = %#v, %v", ready, err)
		}
		finalWorker, err := state.GetWorker(ctx, worker.WorkerKey)
		if err != nil || finalWorker.FinalTarget != WorkerFailed ||
			finalWorker.FinalFailureCode != changesCaptureFailureCode {
			t.Fatalf("failed capture worker = %#v, %v", finalWorker, err)
		}
		acknowledged, err := state.AcknowledgeChangesArtifact(
			ctx, worker.WorkerKey, finalization.Artifact.ArtifactID, 1, start.Add(4*time.Second),
		)
		if err != nil || acknowledged.Worker.Status != WorkerFailed ||
			acknowledged.Worker.FailureCode != changesCaptureFailureCode {
			t.Fatalf("failed capture acknowledgement = %#v, %v", acknowledged, err)
		}
	})
}

func TestChangesArtifactRejectsUnclaimedWorkspaceAndArbitraryPartNames(t *testing.T) {
	ctx := context.Background()
	state := openPeerTestStore(t)
	start := time.Unix(5_000, 0)
	worker, workspace, turnID := prepareRunningChangesWorker(t, state, 20, true, start)
	finalization, err := state.BeginWorkerFinalization(
		ctx, worker.WorkerKey, turnID, WorkerIdle, "", start.Add(time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.ReserveChangesArtifactPayload(
		ctx, worker.WorkerKey, finalization.Artifact.ArtifactID, 1, start.Add(2*time.Second),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := state.CompleteChangesArtifactCapture(ctx, worker.WorkerKey,
		finalization.Artifact.ArtifactID, ChangesCaptureResult{
			Status: ChangesAvailable, ResultHeadOID: strings.Repeat("b", 40),
			ResultSnapshotHash: strings.Repeat("c", 64), ResultClean: true,
			Parts: []ChangesArtifactPart{{
				Kind: ChangesArtifactBundle, Name: ChangesBundlePartName, SizeBytes: 1,
				SHA256: strings.Repeat("d", 64),
			}},
			ResultWarnings: []string{protocol.WorkspaceWarningFullHistoryFallback},
		}, start.Add(3*time.Second)); err == nil || !strings.Contains(err.Error(), "source workspace warnings") {
		t.Fatalf("capture result transfer-only warning error = %v", err)
	}
	pending, err := state.GetChangesArtifact(ctx, worker.WorkerKey, finalization.Artifact.ArtifactID)
	if err != nil || pending.State != ChangesCapturePending ||
		!slices.Equal(pending.BaseWarnings, workspace.Warnings) || len(pending.ResultWarnings) != 0 {
		t.Fatalf("rejected result changed pending warning authority = %#v, %v", pending, err)
	}
	for _, name := range []string{"../changes.bundle", "/tmp/changes.bundle", "bundle"} {
		_, err := state.CompleteChangesArtifactCapture(ctx, worker.WorkerKey,
			finalization.Artifact.ArtifactID, ChangesCaptureResult{
				Status: ChangesAvailable, ResultHeadOID: strings.Repeat("b", 40),
				ResultSnapshotHash: strings.Repeat("c", 64), ResultClean: true,
				Parts: []ChangesArtifactPart{{
					Kind: ChangesArtifactBundle, Name: name, SizeBytes: 1,
					SHA256: strings.Repeat("d", 64),
				}},
			}, start.Add(3*time.Second))
		if err == nil {
			t.Fatalf("capture accepted arbitrary part name %q", name)
		}
	}
	wrongKey := worker.WorkerKey
	wrongKey.AgentID = changesTestID(999)
	if _, err := state.GetChangesArtifact(ctx, wrongKey, finalization.Artifact.ArtifactID); !errors.Is(
		err,
		ErrNotFound,
	) {
		t.Fatalf("foreign artifact lookup error = %v", err)
	}

	unclaimed := workerReservation(t, changesTestID(21), "unclaimed")
	unclaimed.WorkspaceID = changesTestID(1_021)
	unclaimed.WorkingDirectory = ""
	unclaimedWorkspace := changesPreparedWorkspace(t, unclaimed, true)
	if _, err := state.RecordPreparedWorkspace(ctx, unclaimedWorkspace, start.Add(4*time.Second)); err != nil {
		t.Fatal(err)
	}
	unclaimed, err = state.ReserveWorkerStart(ctx, unclaimed, 3, start.Add(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	unclaimed, err = makeChangesWorkerRunning(
		ctx, state, unclaimed, changesTestID(2_021), changesTestID(3_021), start.Add(6*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.BeginWorkerFinalization(ctx, unclaimed.WorkerKey,
		unclaimed.ActiveTurnID, WorkerIdle, "", start.Add(10*time.Second)); !errors.Is(
		err,
		ErrChangesArtifactAuthority,
	) {
		t.Fatalf("unclaimed workspace finalization error = %v", err)
	}
}

func TestChangesArtifactQuotaReservationsAreConcurrentAndDurable(t *testing.T) {
	ctx := context.Background()
	state := openPeerTestStore(t)
	start := time.Unix(6_000, 0)
	finalizations := make([]WorkerFinalization, 5)
	for index := range finalizations {
		worker, _, turnID := prepareRunningChangesWorker(t, state, 100+index, true, start)
		var err error
		finalizations[index], err = state.BeginWorkerFinalization(
			ctx, worker.WorkerKey, turnID, WorkerIdle, "", start.Add(time.Second),
		)
		if err != nil {
			t.Fatal(err)
		}
	}

	var wait sync.WaitGroup
	errorsByIndex := make([]error, len(finalizations))
	for index, finalization := range finalizations {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, errorsByIndex[index] = state.ReserveChangesArtifactPayload(
				ctx,
				finalization.Worker.WorkerKey,
				finalization.Artifact.ArtifactID,
				MaximumChangesArtifactPayloadBytes,
				start.Add(2*time.Second),
			)
		}()
	}
	wait.Wait()
	var succeeded, quotaRejected int
	for _, err := range errorsByIndex {
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, ErrChangesArtifactQuota):
			quotaRejected++
		default:
			t.Fatalf("unexpected concurrent reservation error = %v", err)
		}
	}
	if succeeded != 4 || quotaRejected != 1 {
		t.Fatalf("concurrent reservations: succeeded=%d quota=%d", succeeded, quotaRejected)
	}
	var reservedBytes int64
	if err := state.db.QueryRow(`
SELECT COALESCE(sum(reserved_bytes), 0) FROM peer_changes_artifacts
WHERE retention_reserved = 1
`).Scan(&reservedBytes); err != nil {
		t.Fatal(err)
	}
	if reservedBytes != MaximumRetainedChangesPayloadBytes {
		t.Fatalf("reserved bytes = %d, want %d", reservedBytes, MaximumRetainedChangesPayloadBytes)
	}
}

func TestChangesArtifactRecordQuotaCountsAllOutcomes(t *testing.T) {
	ctx := context.Background()
	state := openPeerTestStore(t)
	start := time.Unix(7_000, 0)
	for index := range MaximumRetainedChangesArtifacts {
		worker, workspace, turnID := prepareRunningChangesWorker(t, state, 200+index, false, start)
		finalization, err := state.BeginWorkerFinalization(
			ctx, worker.WorkerKey, turnID, WorkerIdle, "", start.Add(time.Second),
		)
		if err != nil {
			t.Fatal(err)
		}
		if index%2 == 0 {
			_, err = state.CompleteChangesArtifactCapture(ctx, worker.WorkerKey,
				finalization.Artifact.ArtifactID, ChangesCaptureResult{
					Status: ChangesUnchanged, ResultHeadOID: workspace.HeadOID,
					ResultSnapshotHash: workspace.SourceSnapshotHash, ResultClean: false,
				}, start.Add(2*time.Second))
		} else {
			_, err = state.CompleteChangesArtifactCapture(ctx, worker.WorkerKey,
				finalization.Artifact.ArtifactID, ChangesCaptureResult{
					Status: ChangesCaptureFailed, FailureCode: "capture_failed",
				}, start.Add(2*time.Second))
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	overflow, _, turnID := prepareRunningChangesWorker(t, state, 500, true, start)
	if _, err := state.BeginWorkerFinalization(
		ctx, overflow.WorkerKey, turnID, WorkerIdle, "", start.Add(3*time.Second),
	); !errors.Is(err, ErrChangesArtifactQuota) {
		t.Fatalf("record quota error = %v", err)
	}
}

func TestRecoverWorkersRestoresFinalizingWorkspaceCapture(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state", "peer.sqlite3")
	state, err := OpenPeer(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Unix(8_000, 0)
	worker, workspace, turnID := prepareRunningChangesWorker(t, state, 600, true, start)
	recovered, err := state.RecoverWorkers(
		ctx, worker.ControllerID, worker.DeviceID, start.Add(time.Second),
	)
	if err != nil || len(recovered) != 1 || recovered[0].Status != WorkerFinalizing ||
		recovered[0].ActiveTurnID != turnID || recovered[0].FinalTarget != WorkerInterrupted ||
		recovered[0].FinalFailureCode != workerRunningTurnInterruptedFailure {
		t.Fatalf("recovered workers = %#v, %v", recovered, err)
	}
	captures, err := state.ListPendingChangesCaptures(ctx, worker.ControllerID, worker.DeviceID, 10)
	if err != nil || len(captures) != 1 || captures[0].TurnID != turnID {
		t.Fatalf("recovered captures = %#v, %v", captures, err)
	}
	if !slices.Equal(captures[0].BaseWarnings, workspace.Warnings) ||
		len(captures[0].ResultWarnings) != 0 {
		t.Fatalf("capture-pending warnings = %#v, want base %v", captures[0], workspace.Warnings)
	}
	artifactID := captures[0].ArtifactID
	if again, err := state.RecoverWorkers(
		ctx, worker.ControllerID, worker.DeviceID, start.Add(2*time.Second),
	); err != nil || len(again) != 0 {
		t.Fatalf("second recovery = %#v, %v", again, err)
	}
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
	state, err = OpenPeer(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	captures, err = state.ListPendingChangesCaptures(ctx, worker.ControllerID, worker.DeviceID, 10)
	if err != nil || len(captures) != 1 || captures[0].ArtifactID != artifactID ||
		!slices.Equal(captures[0].BaseWarnings, workspace.Warnings) ||
		len(captures[0].ResultWarnings) != 0 {
		t.Fatalf("reopened captures = %#v, %v", captures, err)
	}
}

func openPeerTestStore(t *testing.T) *PeerStore {
	t.Helper()
	state, err := OpenPeer(context.Background(), filepath.Join(t.TempDir(), "state", "peer.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := state.Close(); err != nil {
			t.Errorf("close peer store: %v", err)
		}
	})
	return state
}

func prepareRunningChangesWorker(
	t *testing.T,
	state *PeerStore,
	index int,
	clean bool,
	start time.Time,
) (WorkerReservation, PreparedWorkspace, string) {
	t.Helper()
	ctx := context.Background()
	worker := workerReservation(t, changesTestID(10_000+index), fmt.Sprintf("changes %d", index))
	worker.WorkspaceID = changesTestID(20_000 + index)
	worker.WorkingDirectory = ""
	workspace := changesPreparedWorkspace(t, worker, clean)
	workspace, err := state.RecordPreparedWorkspace(ctx, workspace, start)
	if err != nil {
		t.Fatal(err)
	}
	worker, err = state.ReserveWorkerStartWithWorkspace(ctx, worker, 1_000, start.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	threadID := changesTestID(30_000 + index)
	turnID := changesTestID(40_000 + index)
	worker, err = makeChangesWorkerRunning(
		ctx, state, worker, threadID, turnID, start.Add(2*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	return worker, workspace, turnID
}

func makeChangesWorkerRunning(
	ctx context.Context,
	state *PeerStore,
	worker WorkerReservation,
	threadID, turnID string,
	start time.Time,
) (WorkerReservation, error) {
	var err error
	worker, err = state.AttachWorkerThread(ctx, worker.WorkerKey, threadID, start)
	if err != nil {
		return WorkerReservation{}, err
	}
	worker, err = state.MarkWorkerReady(ctx, worker.WorkerKey, start.Add(time.Second))
	if err != nil {
		return WorkerReservation{}, err
	}
	return state.MarkWorkerRunning(ctx, worker.WorkerKey, turnID, start.Add(2*time.Second))
}

func changesPreparedWorkspace(t *testing.T, worker WorkerReservation, clean bool) PreparedWorkspace {
	t.Helper()
	workspace := PreparedWorkspace{
		PreparedWorkspaceKey: PreparedWorkspaceKey{
			ControllerID: worker.ControllerID,
			TreeID:       worker.TreeID,
			WorkspaceID:  worker.WorkspaceID,
		},
		SourceAgentID: worker.ParentAgentID, SourceDeviceID: changesSourceDeviceID,
		TargetDeviceID: worker.DeviceID, GitURL: "https://example.invalid/repository.git",
		HeadOID: strings.Repeat("a", 40), ObjectFormat: "sha1",
		WorkingDirectory: worker.WorkingDirectory, Clean: clean,
		SourceSnapshotHash: strings.Repeat("b", 64), WorkspacePath: worker.WorkspacePath,
		Strategy:       protocol.WorkspaceStrategyFull,
		SourceWarnings: []string{"lfs_payload_not_transferred"},
		Warnings: []string{
			"lfs_payload_not_transferred",
			protocol.WorkspaceWarningFullHistoryFallback,
		},
	}
	manifestHash, err := protocol.WorkspaceManifestHash(protocol.WorkspaceManifest{
		GitURL: workspace.GitURL, HeadOID: workspace.HeadOID,
		ObjectFormat: workspace.ObjectFormat, WorkingDirectory: workspace.WorkingDirectory,
		Clean: workspace.Clean, SourceSnapshotHash: workspace.SourceSnapshotHash,
		Warnings: workspace.SourceWarnings,
	})
	if err != nil {
		t.Fatal(err)
	}
	workspace.ManifestHash = manifestHash
	return workspace
}

func changesTestID(value int) string {
	return fmt.Sprintf("00000000-0000-4000-8000-%012x", value)
}
