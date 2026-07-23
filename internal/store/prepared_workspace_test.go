package store

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/protocol"
)

const preparedWorkspaceID = "123e4567-e89b-42d3-a456-426614174080"

func TestPreparedWorkspaceClaimIsAtomicIdempotentAndSingleUse(t *testing.T) {
	ctx := context.Background()
	state, err := OpenPeer(ctx, filepath.Join(t.TempDir(), "state", "peer.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	workspace := newPreparedWorkspace(t)
	stored, err := state.RecordPreparedWorkspace(ctx, workspace, time.Unix(30, 0))
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != PreparedWorkspaceReady {
		t.Fatalf("prepared workspace = %#v", stored)
	}
	reservation := workerReservation(t, "123e4567-e89b-42d3-a456-426614174481", "prepared")
	reservation.WorkspaceID = workspace.WorkspaceID
	reservation.WorkspacePath = workspace.WorkspacePath
	reservation.WorkingDirectory = workspace.WorkingDirectory
	started, err := state.ReserveWorkerStartWithWorkspace(ctx, reservation, 1, time.Unix(31, 0))
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := state.GetPreparedWorkspace(ctx, workspace.PreparedWorkspaceKey)
	if err != nil {
		t.Fatal(err)
	}
	if claimed.Status != PreparedWorkspaceClaimed || claimed.ClaimedAgentID != reservation.AgentID ||
		started.Status != WorkerStarting || started.WorkspaceID != workspace.WorkspaceID {
		t.Fatalf("claimed workspace = %#v; worker = %#v", claimed, started)
	}
	repeated, err := state.ReserveWorkerStartWithWorkspace(ctx, reservation, 1, time.Unix(32, 0))
	if err != nil || !reflect.DeepEqual(repeated, started) {
		t.Fatalf("idempotent claim = %#v, %v", repeated, err)
	}
	other := reservation
	other.AgentID = "123e4567-e89b-42d3-a456-426614174482"
	other.TaskName = "reuse"
	if _, err := state.ReserveWorkerStartWithWorkspace(ctx, other, 2, time.Unix(33, 0)); !errors.Is(
		err, ErrWorkerReservationConflict,
	) {
		t.Fatalf("second claim = %v, want ErrWorkerReservationConflict", err)
	}
	if _, err := state.GetWorker(ctx, other.WorkerKey); !errors.Is(err, ErrNotFound) {
		t.Fatalf("failed claim stored worker: %v", err)
	}
}

func TestBusyWorkerDoesNotConsumePreparedWorkspace(t *testing.T) {
	ctx := context.Background()
	state, err := OpenPeer(ctx, filepath.Join(t.TempDir(), "state", "peer.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	active := workerReservation(t, "123e4567-e89b-42d3-a456-426614174483", "active")
	if _, err := state.ReserveWorkerStart(ctx, active, 1, time.Unix(40, 0)); err != nil {
		t.Fatal(err)
	}
	workspace := newPreparedWorkspace(t)
	if _, err := state.RecordPreparedWorkspace(ctx, workspace, time.Unix(41, 0)); err != nil {
		t.Fatal(err)
	}
	waiting := workerReservation(t, "123e4567-e89b-42d3-a456-426614174484", "waiting")
	waiting.WorkspaceID = workspace.WorkspaceID
	waiting.WorkspacePath = workspace.WorkspacePath
	waiting.WorkingDirectory = workspace.WorkingDirectory
	if _, err := state.ReserveWorkerStartWithWorkspace(ctx, waiting, 1, time.Unix(42, 0)); !errors.Is(
		err, ErrWorkerBusy,
	) {
		t.Fatalf("busy reservation = %v, want ErrWorkerBusy", err)
	}
	stored, err := state.GetPreparedWorkspace(ctx, workspace.PreparedWorkspaceKey)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != PreparedWorkspaceReady || stored.ClaimedAgentID != "" {
		t.Fatalf("busy reservation consumed workspace = %#v", stored)
	}
}

func newPreparedWorkspace(t *testing.T) PreparedWorkspace {
	t.Helper()
	manifest := testWorkspaceManifest("ssh://git@example.invalid/repository.git")
	hash, err := protocol.WorkspaceManifestHash(manifest)
	if err != nil {
		t.Fatal(err)
	}
	return PreparedWorkspace{
		PreparedWorkspaceKey: PreparedWorkspaceKey{
			ControllerID: workerControllerID, TreeID: workerTreeID, WorkspaceID: preparedWorkspaceID,
		},
		SourceAgentID: workerParentID, SourceDeviceID: workerParentID,
		TargetDeviceID: workerDeviceID, GitURL: manifest.GitURL,
		HeadOID: manifest.HeadOID, ObjectFormat: manifest.ObjectFormat,
		WorkingDirectory: manifest.WorkingDirectory, Clean: manifest.Clean,
		SourceSnapshotHash: manifest.SourceSnapshotHash,
		WorkspacePath:      filepath.Join(t.TempDir(), "workspace"),
		Strategy:           protocol.WorkspaceStrategyDirect, ManifestHash: hash,
		Warnings: []string{},
	}
}
