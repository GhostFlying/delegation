package workerhost

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
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
	if err := os.WriteFile(
		filepath.Join(workspacePath, ".gitattributes"), []byte("*.bin filter=lfs\n"), 0o600,
	); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(workspacePath, "large.bin"),
		[]byte("version https://git-lfs.github.com/spec/v1\noid sha256:"+strings.Repeat("a", 64)+"\nsize 1\n"),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	baseHead := outputTestGit(t, workspacePath, "rev-parse", "HEAD^{commit}")
	runTestGit(t, workspacePath, "add", "nested/source.txt", ".gitattributes", "large.bin")
	runTestGit(t, workspacePath, "update-index", "--add", "--cacheinfo", "160000,"+baseHead+",vendor/module")
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
		artifact.Parts[1].Kind != store.ChangesArtifactOverlay || len(artifact.BaseWarnings) != 0 ||
		!slices.Equal(artifact.ResultWarnings, []string{
			protocol.WorkspaceWarningLFSPayloadNotTransferred,
			protocol.WorkspaceWarningSubmoduleRepositoryNotTransferred,
		}) {
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

func TestWorkspaceFailedTurnPublishesChangesBeforeExposingFailure(t *testing.T) {
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
		TaskName: "failed changes", Prompt: "modify the repository and fail", WorkspaceID: workspaceID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(workspacePath, "nested", "source.txt"), []byte("failed turn change\n"), 0o600,
	); err != nil {
		t.Fatal(err)
	}
	application.notifyCompletion(
		started.Worker.CodexThreadID, started.Worker.ActiveTurnID, "failed",
	)

	artifact := waitChangesPublication(t, host)
	worker := waitWorkerStatus(t, state, started.Worker.WorkerKey, store.WorkerFinalizing)
	if worker.FinalTarget != store.WorkerFailed || worker.FinalFailureCode != "turn_failed" ||
		worker.FailureCode != "" || worker.ActiveTurnID != started.Worker.ActiveTurnID {
		t.Fatalf("failed turn was exposed before artifact ACK: %#v", worker)
	}
	if artifact.State != store.ChangesPublishPending || artifact.Status != store.ChangesAvailable ||
		artifact.TurnID != started.Worker.ActiveTurnID || artifact.WorkspaceID != workspaceID ||
		artifact.CompletionTarget != store.WorkerFailed ||
		artifact.CompletionFailureCode != "turn_failed" || artifact.ResultClean ||
		len(artifact.Parts) != 1 || artifact.Parts[0].Kind != store.ChangesArtifactOverlay ||
		len(artifact.BaseWarnings) != 0 || len(artifact.ResultWarnings) != 0 ||
		artifact.FailureCode != "" {
		t.Fatalf("failed turn pending artifact = %#v", artifact)
	}

	finalization, err := host.AcknowledgeChangesArtifact(
		context.Background(), worker.WorkerKey, artifact.ArtifactID, 13,
	)
	if err != nil {
		t.Fatal(err)
	}
	if finalization.Worker.Status != store.WorkerFailed ||
		finalization.Worker.FailureCode != "turn_failed" ||
		finalization.Worker.FinalTarget != "" || finalization.Worker.FinalFailureCode != "" ||
		finalization.Worker.ActiveTurnID != "" ||
		finalization.Artifact.State != store.ChangesPublished ||
		finalization.Artifact.BrokerSequence != 13 {
		t.Fatalf("failed turn acknowledged finalization = %#v", finalization)
	}
	if publications, err := host.ListPendingChangesPublications(context.Background()); err != nil ||
		len(publications) != 0 {
		t.Fatalf("pending publications after failed-turn ACK = %#v, %v", publications, err)
	}
	if captures, err := state.ListPendingChangesCaptures(
		context.Background(), testControllerID, testDeviceID, 1,
	); err != nil || len(captures) != 0 {
		t.Fatalf("pending captures after failed-turn ACK = %#v, %v", captures, err)
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
		if err := os.WriteFile(
			filepath.Join(workspacePath, ".gitattributes"), []byte("*.bin filter=lfs\n"), 0o600,
		); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(
			filepath.Join(workspacePath, "base.bin"),
			[]byte("version https://git-lfs.github.com/spec/v1\noid sha256:"+strings.Repeat("b", 64)+"\nsize 1\n"),
			0o600,
		); err != nil {
			t.Fatal(err)
		}
		runTestGit(t, workspacePath, "add", ".gitattributes", "base.bin")
		runTestGit(
			t, workspacePath, "-c", "user.name=Delegation Test", "-c", "user.email=test@example.invalid",
			"commit", "-m", "add base LFS marker",
		)
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
		artifact.FailureCode != changesCaptureFailureCode || worker.FinalTarget != store.WorkerFailed ||
		!slices.Equal(artifact.BaseWarnings, []string{
			protocol.WorkspaceWarningLFSPayloadNotTransferred,
		}) || len(artifact.ResultWarnings) != 0 {
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

func TestPublishedChangesArtifactRetentionPrunesOldestPayload(t *testing.T) {
	var artifacts []store.ChangesArtifact
	host, state, _ := newTestHostWithStateSetup(t, 2, "", func(state *store.PeerStore, root string) {
		for index := range 2 {
			workspaceID := newTestID()
			workspacePath := filepath.Join(root, workspaceSyncName(testTreeID, workspaceID))
			initializeTestRepository(t, workspacePath)
			workspace := recordExactPreparedWorkspace(
				t, state, testTreeID, workspaceID, workspacePath,
			)
			worker := makeRunningWorkspaceWorker(t, state, workspace, newTestID())
			finalization, err := state.BeginWorkerFinalization(
				context.Background(), worker.WorkerKey, worker.ActiveTurnID,
				store.WorkerIdle, "", time.Unix(1_700_000_010+int64(index*10), 0),
			)
			if err != nil {
				t.Fatal(err)
			}
			artifact, err := state.CompleteChangesArtifactCapture(
				context.Background(), worker.WorkerKey, finalization.Artifact.ArtifactID,
				store.ChangesCaptureResult{
					Status: store.ChangesUnchanged, ResultHeadOID: workspace.HeadOID,
					ResultSnapshotHash: workspace.SourceSnapshotHash, ResultClean: workspace.Clean,
				},
				time.Unix(1_700_000_011+int64(index*10), 0),
			)
			if err != nil {
				t.Fatal(err)
			}
			acknowledged, err := state.AcknowledgeChangesArtifact(
				context.Background(), worker.WorkerKey, artifact.ArtifactID,
				uint64(index+1), time.Unix(1_700_000_012+int64(index*10), 0),
			)
			if err != nil {
				t.Fatal(err)
			}
			artifacts = append(artifacts, acknowledged.Artifact)
		}
	})
	for _, artifact := range artifacts {
		_, finalName, err := changesArtifactDirectoryNames(artifact.ArtifactID)
		if err != nil {
			t.Fatal(err)
		}
		if err := host.artifactRoot.Mkdir(finalName, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := host.artifactRoot.WriteFile(filepath.Join(finalName, "marker"), []byte("retained"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := syncDirectory(host.artifactRoot); err != nil {
		t.Fatal(err)
	}

	if err := host.prunePublishedChangesArtifacts(
		context.Background(), 1, store.MaximumRetainedChangesPayloadBytes,
	); err != nil {
		t.Fatal(err)
	}
	_, oldestName, err := changesArtifactDirectoryNames(artifacts[0].ArtifactID)
	if err != nil {
		t.Fatal(err)
	}
	_, newestName, err := changesArtifactDirectoryNames(artifacts[1].ArtifactID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.artifactRoot.Stat(oldestName); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("oldest retained artifact directory survived pruning: %v", err)
	}
	if _, err := host.artifactRoot.Stat(filepath.Join(newestName, "marker")); err != nil {
		t.Fatalf("newest retained artifact payload was pruned: %v", err)
	}
	if _, err := state.GetChangesArtifact(
		context.Background(), artifacts[0].WorkerKey, artifacts[0].ArtifactID,
	); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("oldest retained artifact metadata survived pruning: %v", err)
	}
	retained, err := state.ListPublishedChangesArtifacts(
		context.Background(), testControllerID, testDeviceID, 2,
	)
	if err != nil || len(retained) != 1 || retained[0].ArtifactID != artifacts[1].ArtifactID {
		t.Fatalf("retained changes artifacts = %#v, %v", retained, err)
	}
}

func TestArtifactRetentionFailureRetriesWithoutFailingHost(t *testing.T) {
	state, err := store.OpenPeer(
		context.Background(), filepath.Join(t.TempDir(), "state", "peer.sqlite3"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	reported := make(chan error, 1)
	retried := make(chan struct{})
	var attempts atomic.Int64
	host := &Host{
		controllerID:     testControllerID,
		deviceID:         testDeviceID,
		state:            state,
		artifactWake:     make(chan struct{}, 1),
		artifactChanges:  make(chan struct{}, 1),
		artifactRetryMin: time.Millisecond,
		artifactRetryMax: time.Millisecond,
		reportError: func(err error) {
			select {
			case reported <- err:
			default:
			}
		},
	}
	host.pruneChangesArtifacts = func(context.Context, int, int64) error {
		if attempts.Add(1) == 1 {
			return errors.New("transient retention failure")
		}
		close(retried)
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	host.background.Add(1)
	go host.processArtifactFinalizations(ctx)
	host.signalArtifactWork()

	select {
	case <-retried:
	case <-time.After(2 * time.Second):
		t.Fatal("artifact retention was not retried")
	}
	select {
	case err := <-reported:
		if !strings.Contains(err.Error(), "transient retention failure") {
			t.Fatalf("reported retention error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("transient artifact retention error was not reported")
	}
	cancel()
	host.background.Wait()
	if attempts.Load() != 2 {
		t.Fatalf("artifact retention attempts = %d, want 2", attempts.Load())
	}
}

func TestPublishedChangesArtifactRetentionReclaimsCaptureHeadroom(t *testing.T) {
	var (
		published     []store.ChangesArtifact
		workspaceRoot string
	)
	host, state, _ := newTestHostWithStateSetup(t, 5, "", func(state *store.PeerStore, root string) {
		workspaceRoot = root
		for index := range 4 {
			workspaceID := newTestID()
			workspacePath := filepath.Join(root, workspaceSyncName(testTreeID, workspaceID))
			initializeTestRepository(t, workspacePath)
			workspace := recordExactPreparedWorkspace(
				t, state, testTreeID, workspaceID, workspacePath,
			)
			worker := makeRunningWorkspaceWorker(t, state, workspace, newTestID())
			finalization, err := state.BeginWorkerFinalization(
				context.Background(), worker.WorkerKey, worker.ActiveTurnID,
				store.WorkerIdle, "", time.Unix(1_700_000_010+int64(index*10), 0),
			)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := state.ReserveChangesArtifactPayload(
				context.Background(), worker.WorkerKey, finalization.Artifact.ArtifactID,
				store.MaximumChangesArtifactPayloadBytes,
				time.Unix(1_700_000_011+int64(index*10), 0),
			); err != nil {
				t.Fatal(err)
			}
			artifact, err := state.CompleteChangesArtifactCapture(
				context.Background(), worker.WorkerKey, finalization.Artifact.ArtifactID,
				store.ChangesCaptureResult{
					Status: store.ChangesAvailable, ResultHeadOID: workspace.HeadOID,
					ResultSnapshotHash: workspace.SourceSnapshotHash, ResultClean: workspace.Clean,
					Parts: []store.ChangesArtifactPart{
						{Kind: store.ChangesArtifactBundle, Name: store.ChangesBundlePartName,
							SizeBytes: protocol.MaximumWorkspaceArtifactBytes, SHA256: strings.Repeat("a", 64)},
						{Kind: store.ChangesArtifactOverlay, Name: store.ChangesOverlayPartName,
							SizeBytes: protocol.MaximumWorkspaceArtifactBytes, SHA256: strings.Repeat("b", 64)},
					},
				},
				time.Unix(1_700_000_012+int64(index*10), 0),
			)
			if err != nil {
				t.Fatal(err)
			}
			acknowledged, err := state.AcknowledgeChangesArtifact(
				context.Background(), worker.WorkerKey, artifact.ArtifactID,
				uint64(index+1), time.Unix(1_700_000_013+int64(index*10), 0),
			)
			if err != nil {
				t.Fatal(err)
			}
			published = append(published, acknowledged.Artifact)
		}
	})

	for _, artifact := range published {
		_, finalName, err := changesArtifactDirectoryNames(artifact.ArtifactID)
		if err != nil {
			t.Fatal(err)
		}
		if err := host.artifactRoot.Mkdir(finalName, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := host.artifactRoot.WriteFile(filepath.Join(finalName, "marker"), []byte("retained"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := syncDirectory(host.artifactRoot); err != nil {
		t.Fatal(err)
	}

	workspaceID := newTestID()
	workspacePath := filepath.Join(workspaceRoot, workspaceSyncName(testTreeID, workspaceID))
	initializeTestRepository(t, workspacePath)
	workspace := recordExactPreparedWorkspace(t, state, testTreeID, workspaceID, workspacePath)
	if err := os.WriteFile(
		filepath.Join(workspacePath, "nested", "worker.txt"), []byte("worker change\n"), 0o600,
	); err != nil {
		t.Fatal(err)
	}
	worker := makeRunningWorkspaceWorker(t, state, workspace, newTestID())
	finalization, err := state.BeginWorkerFinalization(
		context.Background(), worker.WorkerKey, worker.ActiveTurnID,
		store.WorkerIdle, "", time.Unix(1_700_000_100, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.ReserveChangesArtifactPayload(
		context.Background(), worker.WorkerKey, finalization.Artifact.ArtifactID,
		store.MaximumChangesArtifactPayloadBytes, time.Unix(1_700_000_101, 0),
	); !errors.Is(err, store.ErrChangesArtifactQuota) {
		t.Fatalf("full retained payload quota reservation error = %v", err)
	}

	if err := host.captureChangesArtifact(context.Background(), finalization.Artifact); err != nil {
		t.Fatal(err)
	}
	ready, err := state.GetChangesArtifact(
		context.Background(), worker.WorkerKey, finalization.Artifact.ArtifactID,
	)
	if err != nil || ready.State != store.ChangesPublishPending ||
		ready.Status != store.ChangesAvailable || ready.FailureCode != "" || len(ready.Parts) != 1 ||
		ready.Parts[0].Kind != store.ChangesArtifactOverlay {
		t.Fatalf("capture after byte retention pruning = %#v, %v", ready, err)
	}
	retention, err := state.GetPublishedChangesArtifactRetention(
		context.Background(), testControllerID, testDeviceID,
	)
	if err != nil || retention.Count != 3 ||
		retention.ReservedBytes != store.MaximumRetainedChangesPayloadBytes-store.MaximumChangesArtifactPayloadBytes {
		t.Fatalf("published retention after headroom pruning = %#v, %v", retention, err)
	}
	_, oldestName, err := changesArtifactDirectoryNames(published[0].ArtifactID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.artifactRoot.Stat(oldestName); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("oldest byte-heavy artifact survived pruning: %v", err)
	}
}

func TestPendingChangesArtifactQuotaBackpressuresUntilPublication(t *testing.T) {
	const maxSlots = 5
	var (
		pending       []store.ChangesArtifact
		workspaceRoot string
	)
	host, state, _ := newTestHostWithStateSetup(t, maxSlots, "", func(state *store.PeerStore, root string) {
		workspaceRoot = root
		for index := range 4 {
			workspaceID := newTestID()
			workspacePath := filepath.Join(root, workspaceSyncName(testTreeID, workspaceID))
			initializeTestRepository(t, workspacePath)
			workspace := recordExactPreparedWorkspace(
				t, state, testTreeID, workspaceID, workspacePath,
			)
			worker := makeRunningWorkspaceWorkerWithSlots(
				t, state, workspace, newTestID(), maxSlots,
			)
			finalization, err := state.BeginWorkerFinalization(
				context.Background(), worker.WorkerKey, worker.ActiveTurnID,
				store.WorkerIdle, "", time.Unix(1_700_001_000+int64(index*10), 0),
			)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := state.ReserveChangesArtifactPayload(
				context.Background(), worker.WorkerKey, finalization.Artifact.ArtifactID,
				store.MaximumChangesArtifactPayloadBytes,
				time.Unix(1_700_001_001+int64(index*10), 0),
			); err != nil {
				t.Fatal(err)
			}
			artifact, err := state.CompleteChangesArtifactCapture(
				context.Background(), worker.WorkerKey, finalization.Artifact.ArtifactID,
				store.ChangesCaptureResult{
					Status: store.ChangesAvailable, ResultHeadOID: workspace.HeadOID,
					ResultSnapshotHash: workspace.SourceSnapshotHash, ResultClean: workspace.Clean,
					Parts: []store.ChangesArtifactPart{
						{Kind: store.ChangesArtifactBundle, Name: store.ChangesBundlePartName,
							SizeBytes: protocol.MaximumWorkspaceArtifactBytes, SHA256: strings.Repeat("a", 64)},
						{Kind: store.ChangesArtifactOverlay, Name: store.ChangesOverlayPartName,
							SizeBytes: protocol.MaximumWorkspaceArtifactBytes, SHA256: strings.Repeat("b", 64)},
					},
				},
				time.Unix(1_700_001_002+int64(index*10), 0),
			)
			if err != nil {
				t.Fatal(err)
			}
			pending = append(pending, artifact)
		}
	})

	workspaceID := newTestID()
	workspacePath := filepath.Join(workspaceRoot, workspaceSyncName(testTreeID, workspaceID))
	initializeTestRepository(t, workspacePath)
	workspace := recordExactPreparedWorkspace(t, state, testTreeID, workspaceID, workspacePath)
	if err := os.WriteFile(
		filepath.Join(workspacePath, "nested", "worker.txt"), []byte("worker change\n"), 0o600,
	); err != nil {
		t.Fatal(err)
	}
	worker := makeRunningWorkspaceWorkerWithSlots(t, state, workspace, newTestID(), maxSlots)
	finalization, err := state.BeginWorkerFinalization(
		context.Background(), worker.WorkerKey, worker.ActiveTurnID,
		store.WorkerIdle, "", time.Unix(1_700_002_000, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	err = host.captureChangesArtifact(context.Background(), finalization.Artifact)
	var retentionError *artifactRetentionError
	if !errors.As(err, &retentionError) || !errors.Is(err, store.ErrChangesArtifactQuota) {
		t.Fatalf("full pending artifact quota capture = %v, want retryable retention error", err)
	}
	blocked, err := state.GetChangesArtifact(
		context.Background(), worker.WorkerKey, finalization.Artifact.ArtifactID,
	)
	if err != nil || blocked.State != store.ChangesCapturePending || blocked.Status != "" ||
		blocked.FailureCode != "" {
		t.Fatalf("backpressured artifact = %#v, %v", blocked, err)
	}
	blockedWorker, err := state.GetWorker(context.Background(), worker.WorkerKey)
	if err != nil || blockedWorker.Status != store.WorkerFinalizing ||
		blockedWorker.FinalTarget != store.WorkerIdle || blockedWorker.FinalFailureCode != "" {
		t.Fatalf("backpressured worker = %#v, %v", blockedWorker, err)
	}
	if _, err := state.AcknowledgeChangesArtifact(
		context.Background(), pending[0].WorkerKey, pending[0].ArtifactID, 1,
		time.Unix(1_700_002_001, 0),
	); err != nil {
		t.Fatal(err)
	}
	if err := host.captureChangesArtifact(context.Background(), finalization.Artifact); err != nil {
		t.Fatal(err)
	}
	ready, err := state.GetChangesArtifact(
		context.Background(), worker.WorkerKey, finalization.Artifact.ArtifactID,
	)
	if err != nil || ready.State != store.ChangesPublishPending ||
		ready.Status != store.ChangesAvailable || ready.FailureCode != "" || len(ready.Parts) != 1 ||
		ready.Parts[0].Kind != store.ChangesArtifactOverlay {
		t.Fatalf("recovered changes artifact = %#v, %v", ready, err)
	}
	if _, err := state.GetChangesArtifact(
		context.Background(), pending[0].WorkerKey, pending[0].ArtifactID,
	); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("reclaimed published artifact lookup = %v, want not found", err)
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
	return makeRunningWorkspaceWorkerWithSlots(t, state, workspace, agentID, 1)
}

func makeRunningWorkspaceWorkerWithSlots(
	t *testing.T,
	state *store.PeerStore,
	workspace store.PreparedWorkspace,
	agentID string,
	maxSlots int,
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
	}, maxSlots, time.Unix(1_700_000_001, 0))
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
