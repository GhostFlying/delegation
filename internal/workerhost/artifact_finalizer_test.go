package workerhost

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/gitworkspace"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
)

func TestWorkspaceCompletionPublishesCommittedAndDirtyChangesBeforeTerminalState(t *testing.T) {
	application := newFakeApplication()
	workspaceID := newTestID()
	agentID := newTestID()
	var workspacePath string
	host, state, _ := newTestHostWithStateSetup(t, 1, "", func(state *store.PeerStore, root string) {
		workspacePath = filepath.Join(root, workspaceSyncName(testTreeID, workspaceID))
		initializeTestRepository(t, workspacePath)
		recordExactPreparedWorkspace(t, state, testTreeID, workspaceID, workspacePath)
	}, application)
	started, err := host.Spawn(context.Background(), SpawnRequest{
		TreeID: testTreeID, AgentID: agentID, ParentAgentID: testParentID,
		TaskName: "changes", Prompt: "commit and modify files", WorkspaceID: workspaceID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(workspacePath, "nested", "source.txt"), []byte("committed\n"), 0o600,
	); err != nil {
		t.Fatal(err)
	}
	runTestGit(t, workspacePath, "add", "nested/source.txt")
	runTestGit(
		t, workspacePath, "-c", "user.name=Delegation Test", "-c", "user.email=test@example.invalid",
		"commit", "-m", "worker commit",
	)
	if err := os.WriteFile(filepath.Join(workspacePath, "dirty.txt"), []byte("dirty\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	application.notifyCompletion(
		started.Worker.CodexThreadID, started.Worker.ActiveTurnID, "completed",
	)

	artifact := waitChangesPublication(t, host)
	worker := waitWorkerStatus(t, state, started.Worker.WorkerKey, store.WorkerFinalizing)
	if worker.FinalTarget != store.WorkerIdle || artifact.Status != store.ChangesAvailable ||
		len(artifact.Parts) != 2 || artifact.Parts[0].Kind != store.ChangesArtifactBundle ||
		artifact.Parts[1].Kind != store.ChangesArtifactOverlay {
		t.Fatalf("finalizing worker/artifact = %#v / %#v", worker, artifact)
	}
	artifactDirectory := filepath.Join(
		host.artifactRoot.Name(), changesArtifactPrefix+artifact.ArtifactID,
	)
	artifactRootRelative, err := filepath.Rel(host.workspaceRoot.Name(), host.artifactRoot.Name())
	if err != nil || artifactRootRelative != changesArtifactRootName {
		t.Fatalf("artifact root is not anchored below workspace root: %q, %v", artifactRootRelative, err)
	}
	for _, part := range artifact.Parts {
		info, err := os.Stat(filepath.Join(artifactDirectory, part.Name))
		if err != nil || !info.Mode().IsRegular() || info.Size() != part.SizeBytes {
			t.Fatalf("artifact part %q = %#v, %v", part.Name, info, err)
		}
	}
	finalization, err := host.AcknowledgeChangesArtifact(
		context.Background(), worker.WorkerKey, artifact.ArtifactID, 7,
	)
	if err != nil {
		t.Fatal(err)
	}
	if finalization.Worker.Status != store.WorkerIdle ||
		finalization.Artifact.State != store.ChangesPublished {
		t.Fatalf("acknowledged finalization = %#v", finalization)
	}
	if publications, err := host.ListPendingChangesPublications(context.Background()); err != nil ||
		len(publications) != 0 {
		t.Fatalf("pending publications after ACK = %#v, %v", publications, err)
	}
}

func TestStartupRecoversWorkspaceCapturePending(t *testing.T) {
	workspaceID := newTestID()
	agentID := newTestID()
	var key store.WorkerKey
	host, state, _ := newTestHostWithStateSetup(t, 1, "", func(state *store.PeerStore, root string) {
		workspacePath := filepath.Join(root, workspaceSyncName(testTreeID, workspaceID))
		initializeTestRepository(t, workspacePath)
		workspace := recordExactPreparedWorkspace(
			t, state, testTreeID, workspaceID, workspacePath,
		)
		worker := makeRunningWorkspaceWorker(t, state, workspace, agentID)
		key = worker.WorkerKey
	}, newFakeApplication())

	artifact := waitChangesPublication(t, host)
	worker := waitWorkerStatus(t, state, key, store.WorkerFinalizing)
	if artifact.Status != store.ChangesUnchanged || worker.FinalTarget != store.WorkerInterrupted {
		t.Fatalf("recovered finalization = %#v / %#v", worker, artifact)
	}
	finalization, err := host.AcknowledgeChangesArtifact(
		context.Background(), key, artifact.ArtifactID, 9,
	)
	if err != nil {
		t.Fatal(err)
	}
	if finalization.Worker.Status != store.WorkerInterrupted {
		t.Fatalf("recovered ACK worker = %#v", finalization.Worker)
	}
}

func TestStartupSignalsPublishPendingWithoutRecapturing(t *testing.T) {
	workspaceID := newTestID()
	agentID := newTestID()
	var expected store.WorkerFinalization
	host, _, _ := newTestHostWithStateSetup(t, 1, "", func(state *store.PeerStore, root string) {
		workspacePath := filepath.Join(root, workspaceSyncName(testTreeID, workspaceID))
		initializeTestRepository(t, workspacePath)
		workspace := recordExactPreparedWorkspace(
			t, state, testTreeID, workspaceID, workspacePath,
		)
		worker := makeRunningWorkspaceWorker(t, state, workspace, agentID)
		var err error
		expected, err = state.BeginWorkerFinalization(
			context.Background(), worker.WorkerKey, worker.ActiveTurnID,
			store.WorkerIdle, "", time.Unix(1_700_000_005, 0),
		)
		if err != nil {
			t.Fatal(err)
		}
		expected.Artifact, err = state.CompleteChangesArtifactCapture(
			context.Background(), worker.WorkerKey, expected.Artifact.ArtifactID,
			store.ChangesCaptureResult{
				Status: store.ChangesCaptureFailed, FailureCode: changesCaptureFailureCode,
			},
			time.Unix(1_700_000_006, 0),
		)
		if err != nil {
			t.Fatal(err)
		}
	}, newFakeApplication())

	select {
	case <-host.ArtifactChanges():
	case <-time.After(5 * time.Second):
		t.Fatal("startup did not signal the publish-pending artifact")
	}
	artifacts, err := host.ListPendingChangesPublications(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(artifacts) != 1 || artifacts[0].ArtifactID != expected.Artifact.ArtifactID ||
		artifacts[0].Status != store.ChangesCaptureFailed {
		t.Fatalf("startup publication = %#v, want %#v", artifacts, expected.Artifact)
	}
	if _, err := os.Stat(filepath.Join(
		host.artifactRoot.Name(), changesArtifactPrefix+expected.Artifact.ArtifactID,
	)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("publish-pending artifact was recaptured: %v", err)
	}
}

func TestCaptureFailureIsPublishedAndFailsOnlyAfterACK(t *testing.T) {
	application := newFakeApplication()
	workspaceID := newTestID()
	agentID := newTestID()
	var workspacePath string
	host, state, _ := newTestHostWithStateSetup(t, 1, "", func(state *store.PeerStore, root string) {
		workspacePath = filepath.Join(root, workspaceSyncName(testTreeID, workspaceID))
		initializeTestRepository(t, workspacePath)
		recordExactPreparedWorkspace(t, state, testTreeID, workspaceID, workspacePath)
	}, application)
	started, err := host.Spawn(context.Background(), SpawnRequest{
		TreeID: testTreeID, AgentID: agentID, ParentAgentID: testParentID,
		TaskName: "capture failure", Prompt: "finish after repository loss", WorkspaceID: workspaceID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(workspacePath, ".git")); err != nil {
		t.Fatal(err)
	}
	application.notifyCompletion(
		started.Worker.CodexThreadID, started.Worker.ActiveTurnID, "completed",
	)

	artifact := waitChangesPublication(t, host)
	worker := waitWorkerStatus(t, state, started.Worker.WorkerKey, store.WorkerFinalizing)
	if artifact.Status != store.ChangesCaptureFailed ||
		artifact.FailureCode != changesCaptureFailureCode || worker.FinalTarget != store.WorkerFailed {
		t.Fatalf("failed capture = %#v / %#v", worker, artifact)
	}
	finalization, err := host.AcknowledgeChangesArtifact(
		context.Background(), worker.WorkerKey, artifact.ArtifactID, 11,
	)
	if err != nil {
		t.Fatal(err)
	}
	if finalization.Worker.Status != store.WorkerFailed ||
		finalization.Worker.FailureCode != "changes_capture_failed" {
		t.Fatalf("failed capture ACK = %#v", finalization.Worker)
	}
}

func TestCompletionWithoutWorkspaceRetainsDirectTerminalLifecycle(t *testing.T) {
	application := newFakeApplication()
	host, state, _ := newTestHost(t, 1, application)
	started := spawnTestWorker(t, host, newTestID(), "no workspace")
	application.notifyCompletion(
		started.Worker.CodexThreadID, started.Worker.ActiveTurnID, "completed",
	)
	waitWorkerStatus(t, state, started.Worker.WorkerKey, store.WorkerIdle)
	if publications, err := host.ListPendingChangesPublications(context.Background()); err != nil ||
		len(publications) != 0 {
		t.Fatalf("no-workspace publications = %#v, %v", publications, err)
	}
}

func TestShutdownLeavesUnacknowledgedWorkspaceWorkerFinalizing(t *testing.T) {
	application := newFakeApplication()
	workspaceID := newTestID()
	agentID := newTestID()
	host, state, _ := newTestHostWithStateSetup(t, 1, "", func(state *store.PeerStore, root string) {
		workspacePath := filepath.Join(root, workspaceSyncName(testTreeID, workspaceID))
		initializeTestRepository(t, workspacePath)
		recordExactPreparedWorkspace(t, state, testTreeID, workspaceID, workspacePath)
	}, application)
	started, err := host.Spawn(context.Background(), SpawnRequest{
		TreeID: testTreeID, AgentID: agentID, ParentAgentID: testParentID,
		TaskName: "shutdown", Prompt: "complete before shutdown", WorkspaceID: workspaceID,
	})
	if err != nil {
		t.Fatal(err)
	}
	application.notifyCompletion(
		started.Worker.CodexThreadID, started.Worker.ActiveTurnID, "completed",
	)
	artifact := waitChangesPublication(t, host)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := host.Close(ctx); err != nil {
		cancel()
		t.Fatal(err)
	}
	cancel()
	worker, err := state.GetWorker(context.Background(), started.Worker.WorkerKey)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := state.GetChangesArtifact(
		context.Background(), started.Worker.WorkerKey, artifact.ArtifactID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if worker.Status != store.WorkerFinalizing || stored.State != store.ChangesPublishPending {
		t.Fatalf("shutdown finalized unacknowledged worker = %#v / %#v", worker, stored)
	}
}

func TestChangesArtifactRootRejectsSymbolicLink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("creating an unprivileged directory symlink is not generally available")
	}
	workspacePath := filepath.Join(t.TempDir(), "workspaces")
	if err := config.PreparePrivateDirectory(workspacePath); err != nil {
		t.Fatal(err)
	}
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(workspacePath, changesArtifactRootName)); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(workspacePath)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	artifactRoot, err := openChangesArtifactRoot(root)
	if artifactRoot != nil {
		_ = artifactRoot.Close()
	}
	if err == nil || err.Error() != "changes artifact root must be a directory, not a symbolic link" {
		t.Fatalf("open symlink changes artifact root = %v", err)
	}
}

func waitChangesPublication(t *testing.T, host *Host) store.ChangesArtifact {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		artifacts, err := host.ListPendingChangesPublications(context.Background())
		if err == nil && len(artifacts) == 1 {
			return artifacts[0]
		}
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	artifacts, err := host.ListPendingChangesPublications(context.Background())
	t.Fatalf("changes publication = %#v, %v", artifacts, err)
	return store.ChangesArtifact{}
}

func recordExactPreparedWorkspace(
	t *testing.T,
	state *store.PeerStore,
	treeID, workspaceID, workspacePath string,
) store.PreparedWorkspace {
	t.Helper()
	gitBinary, err := exec.LookPath("git")
	if err != nil {
		t.Skip("Git is unavailable")
	}
	gitBinary, err = filepath.Abs(gitBinary)
	if err != nil {
		t.Fatal(err)
	}
	runner, err := gitworkspace.NewRunner(gitBinary)
	if err != nil {
		t.Fatal(err)
	}
	const gitURL = "ssh://git@example.invalid/repository.git"
	repository, err := runner.Inspect(
		context.Background(), filepath.Join(workspacePath, "nested"), gitURL,
	)
	if err != nil {
		t.Fatal(err)
	}
	manifestHash, err := protocol.WorkspaceManifestHash(repository.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	workspace, err := state.RecordPreparedWorkspace(context.Background(), store.PreparedWorkspace{
		PreparedWorkspaceKey: store.PreparedWorkspaceKey{
			ControllerID: testControllerID, TreeID: treeID, WorkspaceID: workspaceID,
		},
		SourceAgentID: testParentID, SourceDeviceID: testParentID,
		TargetDeviceID: testDeviceID, GitURL: repository.Manifest.GitURL,
		HeadOID: repository.Manifest.HeadOID, ObjectFormat: repository.Manifest.ObjectFormat,
		WorkingDirectory: repository.Manifest.WorkingDirectory, Clean: repository.Manifest.Clean,
		SourceSnapshotHash: repository.Manifest.SourceSnapshotHash,
		WorkspacePath:      workspacePath, Strategy: protocol.WorkspaceStrategyDirect,
		ManifestHash: manifestHash, SourceWarnings: repository.Manifest.Warnings,
		Warnings: repository.Manifest.Warnings,
	}, time.Unix(1_700_000_000, 0))
	if err != nil {
		t.Fatal(err)
	}
	return workspace
}

func makeRunningWorkspaceWorker(
	t *testing.T,
	state *store.PeerStore,
	workspace store.PreparedWorkspace,
	agentID string,
) store.WorkerReservation {
	t.Helper()
	ctx := context.Background()
	worker, err := state.ReserveWorkerStartWithWorkspace(ctx, store.WorkerReservation{
		WorkerKey: store.WorkerKey{
			ControllerID: testControllerID, TreeID: workspace.TreeID, AgentID: agentID,
		},
		ParentAgentID: testParentID, DeviceID: testDeviceID, TaskName: "restart",
		PromptDigest: promptDigest("restart prompt"), WorkspaceID: workspace.WorkspaceID,
		WorkspacePath: workspace.WorkspacePath, WorkingDirectory: workspace.WorkingDirectory,
		ProfileVersion: workerProfileVersion,
	}, 1, time.Unix(1_700_000_001, 0))
	if err != nil {
		t.Fatal(err)
	}
	worker, err = state.AttachWorkerThread(ctx, worker.WorkerKey, newTestID(), time.Unix(1_700_000_002, 0))
	if err != nil {
		t.Fatal(err)
	}
	worker, err = state.MarkWorkerReady(ctx, worker.WorkerKey, time.Unix(1_700_000_003, 0))
	if err != nil {
		t.Fatal(err)
	}
	worker, err = state.MarkWorkerRunning(
		ctx, worker.WorkerKey, newTestID(), time.Unix(1_700_000_004, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	return worker
}
