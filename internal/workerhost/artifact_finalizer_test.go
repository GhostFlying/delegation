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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/gitworkspace"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/resultpackagefiles"
	"github.com/GhostFlying/delegation/internal/store"
)

func TestResultPackageAggregateBudgetDeterministicallyDegradesWorkspace(t *testing.T) {
	maximumPayloadBytes := protocol.MaximumResultPackageBytes - protocol.MaximumResultManifestBytes
	boundaryOverlayBytes := maximumPayloadBytes -
		protocol.MaximumResultRolloutBytes - protocol.MaximumResultChangesBundleBytes
	if boundaryOverlayBytes < 1 || boundaryOverlayBytes >= protocol.MaximumResultChangesOverlayBytes {
		t.Fatalf("invalid aggregate boundary overlay size %d", boundaryOverlayBytes)
	}
	tests := []struct {
		name         string
		overlayBytes int64
		wantDegraded bool
	}{
		{name: "exact aggregate boundary", overlayBytes: boundaryOverlayBytes},
		{name: "one byte over aggregate boundary", overlayBytes: boundaryOverlayBytes + 1, wantDegraded: true},
		{name: "advertised component maxima", overlayBytes: protocol.MaximumResultChangesOverlayBytes, wantDegraded: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			manifest, parts := aggregateBudgetResultFixture(test.overlayBytes)
			kept, degraded, err := enforceResultPackageBudget(&manifest, parts)
			if err != nil || degraded != test.wantDegraded {
				t.Fatalf("enforceResultPackageBudget() degraded %t, err %v", degraded, err)
			}
			if test.wantDegraded {
				if manifest.Workspace.Status != protocol.ResultWorkspaceCaptureFailed ||
					manifest.Workspace.FailureCode != workspaceResultTooLargeCode ||
					manifest.Workspace.ResultHeadOID != "" ||
					manifest.Workspace.ResultSnapshotHash != "" ||
					len(manifest.Parts) != 1 ||
					manifest.Parts[0].Kind != protocol.ResultPackagePartRollout ||
					len(kept) != 1 || kept[0].Kind != protocol.ResultPackagePartRollout {
					t.Fatalf("degraded result package = %#v, sources %#v", manifest, kept)
				}
			} else if manifest.Workspace.Status != protocol.ResultWorkspaceChanged ||
				len(manifest.Parts) != 3 || len(kept) != 3 {
				t.Fatalf("boundary result package = %#v, sources %#v", manifest, kept)
			}
			if _, _, err := protocol.EncodeResultManifest(manifest); err != nil {
				t.Fatalf("encode fitted result manifest: %v", err)
			}
		})
	}
}

func aggregateBudgetResultFixture(
	overlayBytes int64,
) (protocol.ResultManifest, []resultpackagefiles.ResultPackagePartSource) {
	digest := strings.Repeat("a", 64)
	manifest := protocol.ResultManifest{
		Version: protocol.ResultManifestVersion, PackageID: newTestID(),
		ControllerID: testControllerID, TreeID: testTreeID,
		SourceAgentID: newTestID(), SourceDeviceID: testDeviceID,
		ManagedThreadID: newTestID(), TurnID: newTestID(), LifecycleRevision: 1,
		Terminal:   protocol.ResultTerminal{Outcome: protocol.ResultTerminalCompleted},
		CapturedAt: time.Now().Unix(),
		Rollout: protocol.ResultRolloutComponent{
			Status: protocol.ResultRolloutAvailable, RawSize: 1, RawSHA256: digest,
		},
		Workspace: protocol.ResultWorkspaceComponent{
			Status: protocol.ResultWorkspaceChanged, WorkspaceID: newTestID(),
			SourceDeviceID: newTestID(), TargetDeviceID: testDeviceID, ObjectFormat: "sha1",
			BaseHeadOID: strings.Repeat("b", 40), BaseManifestHash: digest,
			BaseSnapshotHash: strings.Repeat("c", 64), BaseClean: true,
			ResultHeadOID: strings.Repeat("d", 40), ResultSnapshotHash: strings.Repeat("e", 64),
			ResultClean: false, BaseWarnings: []string{}, ResultWarnings: []string{},
		},
		Parts: []protocol.ResultPackagePartDescriptor{
			{Kind: protocol.ResultPackagePartChangesBundle, Size: protocol.MaximumResultChangesBundleBytes, SHA256: digest},
			{Kind: protocol.ResultPackagePartChangesOverlay, Size: overlayBytes, SHA256: digest},
			{Kind: protocol.ResultPackagePartRollout, Size: protocol.MaximumResultRolloutBytes, SHA256: digest},
		},
	}
	parts := make([]resultpackagefiles.ResultPackagePartSource, len(manifest.Parts))
	for index, descriptor := range manifest.Parts {
		parts[index] = resultpackagefiles.ResultPackagePartSource{
			Kind: descriptor.Kind, Path: filepath.Join("ignored", string(descriptor.Kind)),
		}
	}
	return manifest, parts
}

func TestRolloutRecoverySharesFlushBudgetAcrossMaximumWorkerSlots(t *testing.T) {
	for _, appendTerminals := range []bool{true, false} {
		name := "budget exhausted"
		if appendTerminals {
			name = "terminals flush on final retry"
		}
		t.Run(name, func(t *testing.T) {
			intents, terminalLines := incompleteRecoveryRollouts(
				t,
				config.MaximumWorkerSlots,
			)
			var delays []time.Duration
			host := &Host{
				waitForRolloutFlush: func(_ context.Context, delay time.Duration) error {
					delays = append(delays, delay)
					if appendTerminals && len(delays) == rolloutFlushAttempts-1 {
						for path, terminal := range terminalLines {
							if err := appendSyncedFile(path, terminal); err != nil {
								t.Fatal(err)
							}
						}
					}
					return nil
				},
			}
			targets, err := host.resolveResultTargetsAfterClientExit(
				context.Background(),
				intents,
			)
			if err != nil {
				t.Fatal(err)
			}
			wantDelays := []time.Duration{
				20 * time.Millisecond,
				40 * time.Millisecond,
				80 * time.Millisecond,
				160 * time.Millisecond,
				250 * time.Millisecond,
				250 * time.Millisecond,
			}
			if !slices.Equal(delays, wantDelays) {
				t.Fatalf("shared rollout recovery delays = %v, want %v", delays, wantDelays)
			}
			for index, target := range targets {
				want := recoveredTurnTarget{
					status: store.WorkerInterrupted, failureCode: "app_server_lost",
				}
				if appendTerminals {
					want = recoveredTurnTarget{status: store.WorkerIdle}
				}
				if target != want {
					t.Fatalf("target[%d] = %#v, want %#v", index, target, want)
				}
			}
		})
	}
}

func incompleteRecoveryRollouts(
	t *testing.T,
	count int,
) ([]store.WorkerTurnStartIntent, map[string]string) {
	t.Helper()
	codexHome := filepath.Join(t.TempDir(), "codex-home")
	directory := filepath.Join(codexHome, "sessions", "2026", "07", "26")
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	resolvedCodexHome, err := filepath.EvalSymlinks(codexHome)
	if err != nil {
		t.Fatal(err)
	}
	intents := make([]store.WorkerTurnStartIntent, 0, count)
	terminals := make(map[string]string, count)
	for range count {
		threadID := newTestID()
		turnID := newTestID()
		path := filepath.Join(
			directory,
			"rollout-2026-07-26T00-00-00-"+threadID+".jsonl",
		)
		prefix := "{}\n"
		if err := os.WriteFile(
			path,
			[]byte(prefix+testManagedRolloutLine("task_started", turnID)),
			0o600,
		); err != nil {
			t.Fatal(err)
		}
		resolvedPath, err := filepath.EvalSymlinks(path)
		if err != nil {
			t.Fatal(err)
		}
		intents = append(intents, store.WorkerTurnStartIntent{
			IntentID: newTestID(), ManagedThreadID: threadID, TurnID: turnID,
			Rollout: store.WorkerRolloutLocator{
				Status: store.WorkerRolloutAvailable, CodexHome: resolvedCodexHome,
				Path: resolvedPath, Offset: int64(len(prefix)),
			},
		})
		terminals[resolvedPath] = testManagedRolloutLine("task_complete", turnID)
	}
	return intents, terminals
}

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

	outbox := waitResultPublication(t, state, started.Worker.WorkerKey)
	worker := waitWorkerStatus(t, state, started.Worker.WorkerKey, store.WorkerFinalizing)
	if worker.FinalTarget != store.WorkerIdle ||
		outbox.Manifest.Workspace.Status != protocol.ResultWorkspaceChanged ||
		len(outbox.Manifest.Parts) != 2 ||
		outbox.Manifest.Parts[0].Kind != protocol.ResultPackagePartChangesBundle ||
		outbox.Manifest.Parts[1].Kind != protocol.ResultPackagePartChangesOverlay ||
		len(outbox.Manifest.Workspace.BaseWarnings) != 0 ||
		!slices.Equal(outbox.Manifest.Workspace.ResultWarnings, []string{
			protocol.WorkspaceWarningLFSPayloadNotTransferred,
			protocol.WorkspaceWarningSubmoduleRepositoryNotTransferred,
		}) {
		t.Fatalf("finalizing worker/result = %#v / %#v", worker, outbox)
	}
	finalization, err := resultManager(host).AcknowledgeResultPackageMetadata(
		context.Background(), outbox.ResultOutboxKey, outbox.Metadata,
	)
	if err != nil {
		t.Fatal(err)
	}
	if finalization.Worker.Status != store.WorkerIdle ||
		finalization.Outbox.State != store.ResultOutboxDeliveryPending {
		t.Fatalf("acknowledged finalization = %#v", finalization)
	}
	if publications, err := state.ListPendingResultPublications(
		context.Background(), testControllerID, testDeviceID, 10,
	); err != nil ||
		len(publications) != 0 {
		t.Fatalf("pending publications after ACK = %#v, %v", publications, err)
	}
}

func TestWorkspaceCompletionPublishesUnchangedResult(t *testing.T) {
	application := newFakeApplication()
	workspaceID := newTestID()
	var workspacePath string
	host, state, _ := newTestHostWithStateSetup(t, 1, "", func(state *store.PeerStore, root string) {
		workspacePath = filepath.Join(root, workspaceSyncName(testTreeID, workspaceID))
		initializeTestRepository(t, workspacePath)
		recordExactPreparedWorkspace(t, state, testTreeID, workspaceID, workspacePath)
	}, application)
	started, err := host.Spawn(context.Background(), SpawnRequest{
		TreeID: testTreeID, AgentID: newTestID(), ParentAgentID: testParentID,
		TaskName: "unchanged", Prompt: "inspect without changing files", WorkspaceID: workspaceID,
	})
	if err != nil {
		t.Fatal(err)
	}
	application.notifyCompletion(
		started.Worker.CodexThreadID, started.Worker.ActiveTurnID, "completed",
	)

	outbox := waitResultPublication(t, state, started.Worker.WorkerKey)
	if outbox.Manifest.Workspace.Status != protocol.ResultWorkspaceUnchanged ||
		outbox.Manifest.Workspace.ResultHeadOID != outbox.Manifest.Workspace.BaseHeadOID ||
		!outbox.Manifest.Workspace.ResultClean || len(outbox.Manifest.Parts) != 0 {
		t.Fatalf("unchanged result = %#v", outbox.Manifest)
	}
	if _, err := resultManager(host).AcknowledgeResultPackageMetadata(
		context.Background(), outbox.ResultOutboxKey, outbox.Metadata,
	); err != nil {
		t.Fatal(err)
	}
}

func TestCompletionPublishesExactManagedRolloutSegment(t *testing.T) {
	application := newFakeApplication()
	application.threadID = newTestID()
	host, state, paths := newTestHost(t, 1, application)
	rolloutPath := filepath.Join(
		paths.codexHome, "sessions", "2026", "07", "26",
		"rollout-2026-07-26T00-00-00-"+application.threadID+".jsonl",
	)
	if err := os.MkdirAll(filepath.Dir(rolloutPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		rolloutPath,
		[]byte("{\"type\":\"event_msg\",\"payload\":{\"type\":\"thread_settings_applied\"}}\n"),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	application.threadPath = rolloutPath
	started := spawnTestWorker(t, host, newTestID(), "rollout capture")
	segment := testManagedRolloutLine("task_started", started.Worker.ActiveTurnID) +
		"{\"type\":\"response_item\",\"payload\":{}}\n" +
		testManagedRolloutLine("task_complete", started.Worker.ActiveTurnID)
	rollout, err := os.OpenFile(rolloutPath, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rollout.WriteString(segment); err != nil {
		_ = rollout.Close()
		t.Fatal(err)
	}
	if err := errors.Join(rollout.Sync(), rollout.Close()); err != nil {
		t.Fatal(err)
	}
	application.notifyCompletion(
		started.Worker.CodexThreadID, started.Worker.ActiveTurnID, "completed",
	)

	outbox := waitResultPublication(t, state, started.Worker.WorkerKey)
	if outbox.Manifest.Rollout.Status != protocol.ResultRolloutAvailable ||
		outbox.Manifest.Rollout.RawSize != int64(len(segment)) ||
		outbox.Manifest.Rollout.RawSHA256 == "" || len(outbox.Manifest.Parts) != 1 ||
		outbox.Manifest.Parts[0].Kind != protocol.ResultPackagePartRollout {
		t.Fatalf("captured rollout result = %#v", outbox.Manifest)
	}
}

func TestCompletionWaitsForTerminalRolloutFlush(t *testing.T) {
	application := newFakeApplication()
	application.threadID = newTestID()
	host, state, paths := newTestHost(t, 1, application)
	rolloutPath := filepath.Join(
		paths.codexHome, "sessions", "2026", "07", "26",
		"rollout-2026-07-26T00-00-00-"+application.threadID+".jsonl",
	)
	if err := os.MkdirAll(filepath.Dir(rolloutPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(rolloutPath, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	application.threadPath = rolloutPath
	started := spawnTestWorker(t, host, newTestID(), "rollout flush")
	start := testManagedRolloutLine("task_started", started.Worker.ActiveTurnID)
	if err := appendSyncedFile(rolloutPath, start); err != nil {
		t.Fatal(err)
	}
	firstIncomplete := make(chan struct{})
	releaseRetry := make(chan struct{})
	var once sync.Once
	host.waitForRolloutFlush = func(ctx context.Context, _ time.Duration) error {
		once.Do(func() { close(firstIncomplete) })
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-releaseRetry:
			return nil
		}
	}
	application.notifyCompletion(
		started.Worker.CodexThreadID, started.Worker.ActiveTurnID, "completed",
	)
	select {
	case <-firstIncomplete:
	case <-time.After(5 * time.Second):
		t.Fatal("rollout capture did not observe the unflushed terminal")
	}
	terminal := testManagedRolloutLine("task_complete", started.Worker.ActiveTurnID)
	if err := appendSyncedFile(rolloutPath, terminal); err != nil {
		t.Fatal(err)
	}
	close(releaseRetry)
	outbox := waitResultPublication(t, state, started.Worker.WorkerKey)
	if outbox.Manifest.Rollout.Status != protocol.ResultRolloutAvailable ||
		outbox.Manifest.Rollout.RawSize != int64(len(start)+len(terminal)) {
		t.Fatalf("post-flush rollout result = %#v", outbox.Manifest.Rollout)
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

	outbox := waitResultPublication(t, state, started.Worker.WorkerKey)
	worker := waitWorkerStatus(t, state, started.Worker.WorkerKey, store.WorkerFinalizing)
	if worker.FinalTarget != store.WorkerFailed || worker.FinalFailureCode != "turn_failed" ||
		worker.FailureCode != "" || worker.ActiveTurnID != started.Worker.ActiveTurnID {
		t.Fatalf("failed turn was exposed before artifact ACK: %#v", worker)
	}
	if outbox.State != store.ResultOutboxPublishPending ||
		outbox.Manifest.Terminal != (protocol.ResultTerminal{
			Outcome: protocol.ResultTerminalFailed, FailureCode: "turn_failed",
		}) || outbox.Manifest.TurnID != started.Worker.ActiveTurnID ||
		outbox.Manifest.Workspace.WorkspaceID != workspaceID ||
		outbox.Manifest.Workspace.Status != protocol.ResultWorkspaceChanged ||
		outbox.Manifest.Workspace.ResultClean || len(outbox.Manifest.Parts) != 1 ||
		outbox.Manifest.Parts[0].Kind != protocol.ResultPackagePartChangesOverlay ||
		len(outbox.Manifest.Workspace.BaseWarnings) != 0 ||
		len(outbox.Manifest.Workspace.ResultWarnings) != 0 {
		t.Fatalf("failed turn pending result = %#v", outbox)
	}

	finalization, err := resultManager(host).AcknowledgeResultPackageMetadata(
		context.Background(), outbox.ResultOutboxKey, outbox.Metadata,
	)
	if err != nil {
		t.Fatal(err)
	}
	if finalization.Worker.Status != store.WorkerFailed ||
		finalization.Worker.FailureCode != "turn_failed" ||
		finalization.Worker.FinalTarget != "" || finalization.Worker.FinalFailureCode != "" ||
		finalization.Worker.ActiveTurnID != "" ||
		finalization.Outbox.State != store.ResultOutboxDeliveryPending {
		t.Fatalf("failed turn acknowledged finalization = %#v", finalization)
	}
	if publications, err := state.ListPendingResultPublications(
		context.Background(), testControllerID, testDeviceID, 10,
	); err != nil ||
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

func TestStartupReconcilesPreparedAndBoundTerminalTurnIntents(t *testing.T) {
	for _, test := range []struct {
		name  string
		bound bool
	}{
		{name: "prepared response loss", bound: false},
		{name: "bound before crash", bound: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			application := newFakeApplication()
			var key store.WorkerKey
			host, state, _ := newTestHostWithStateSetup(
				t, 1, "", func(state *store.PeerStore, root string) {
					worker, intent, turnID := makeStartupTurnIntent(t, state, root, test.bound)
					key = worker.WorkerKey
					application.threadTurns[worker.CodexThreadID] = []turn{{
						ID: turnID, Status: "completed",
					}}
					if test.bound && intent.TurnID != turnID {
						t.Fatalf("bound startup intent = %#v", intent)
					}
				},
				application,
			)

			outbox := waitResultPublication(t, state, key)
			worker := waitWorkerStatus(t, state, key, store.WorkerFinalizing)
			if worker.FinalTarget != store.WorkerIdle ||
				outbox.Manifest.Terminal.Outcome != protocol.ResultTerminalCompleted ||
				outbox.Manifest.TurnID != worker.ActiveTurnID {
				t.Fatalf("recovered terminal result = %#v / %#v", worker, outbox.Manifest)
			}
			record := application.snapshot()
			if len(record.resumes) != 1 || len(record.reads) != 1 ||
				!record.reads[0].IncludeTurns || len(record.turns) != 0 {
				t.Fatalf("startup reconciliation calls = %#v", record)
			}
			intent, err := state.GetWorkerTurnStartIntentByTurn(
				context.Background(), key, worker.ActiveTurnID,
			)
			if err != nil || intent.State != store.WorkerTurnStartBound {
				t.Fatalf("recovered bound intent = %#v, %v", intent, err)
			}
			if _, err := resultManager(host).AcknowledgeResultPackageMetadata(
				context.Background(), outbox.ResultOutboxKey, outbox.Metadata,
			); err != nil {
				t.Fatal(err)
			}
		})
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

	outbox := waitResultPublication(t, state, started.Worker.WorkerKey)
	worker := waitWorkerStatus(t, state, started.Worker.WorkerKey, store.WorkerFinalizing)
	if outbox.Manifest.Workspace.Status != protocol.ResultWorkspaceCaptureFailed ||
		outbox.Manifest.Workspace.FailureCode != workspaceCaptureFailureCode ||
		worker.FinalTarget != store.WorkerIdle ||
		!slices.Equal(outbox.Manifest.Workspace.BaseWarnings, []string{
			protocol.WorkspaceWarningLFSPayloadNotTransferred,
		}) || len(outbox.Manifest.Workspace.ResultWarnings) != 0 {
		t.Fatalf("failed capture = %#v / %#v", worker, outbox)
	}
	finalization, err := resultManager(host).AcknowledgeResultPackageMetadata(
		context.Background(), outbox.ResultOutboxKey, outbox.Metadata,
	)
	if err != nil {
		t.Fatal(err)
	}
	if finalization.Worker.Status != store.WorkerIdle || finalization.Worker.FailureCode != "" {
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
	outbox := waitResultPublication(t, state, started.Worker.WorkerKey)
	worker := waitWorkerStatus(t, state, started.Worker.WorkerKey, store.WorkerFinalizing)
	if outbox.Manifest.Workspace.Status != protocol.ResultWorkspaceNotManaged ||
		worker.FinalTarget != store.WorkerIdle {
		t.Fatalf("no-workspace result = %#v / %#v", worker, outbox)
	}
	for {
		select {
		case <-host.Changes():
		default:
			goto changesDrained
		}
	}

changesDrained:
	finalization, err := resultManager(host).AcknowledgeResultPackageMetadata(
		context.Background(), outbox.ResultOutboxKey, outbox.Metadata,
	)
	if err != nil || finalization.Worker.Status != store.WorkerIdle {
		t.Fatalf("no-workspace metadata ACK = %#v, %v", finalization, err)
	}
	if host.WorkerRevision() != finalization.Worker.Revision {
		t.Fatalf("worker revision after metadata ACK = %d, want %d", host.WorkerRevision(), finalization.Worker.Revision)
	}
	select {
	case <-host.Changes():
	case <-time.After(time.Second):
		t.Fatal("metadata ACK did not signal worker lifecycle change")
	}
}

func TestResultFinalizationRetriesTransientPublicationFailure(t *testing.T) {
	application := newFakeApplication()
	var publisher *transientResultPackagePublisher
	host, state, _ := newTestHostWithStateSetupAndResultPublisher(
		t, 1, "", nil,
		func(delegate *resultpackagefiles.Manager) resultPackagePublisher {
			publisher = &transientResultPackagePublisher{
				delegate: delegate,
				failed:   make(chan struct{}),
				retry:    make(chan struct{}),
			}
			return publisher
		},
		application,
	)
	started := spawnTestWorker(t, host, newTestID(), "transient result publication")
	application.notifyCompletion(
		started.Worker.CodexThreadID, started.Worker.ActiveTurnID, "completed",
	)
	select {
	case <-publisher.failed:
	case <-time.After(5 * time.Second):
		t.Fatal("transient publication failure was not exercised")
	}
	worker := waitWorkerStatus(t, state, started.Worker.WorkerKey, store.WorkerFinalizing)
	outboxes, err := state.ListPendingResultCaptures(
		context.Background(), testControllerID, testDeviceID, 10,
	)
	if err != nil || len(outboxes) != 1 || outboxes[0].State != store.ResultOutboxCapturePending {
		t.Fatalf("capture state after transient failure = %#v, %v", outboxes, err)
	}
	if worker.FinalTarget != store.WorkerIdle || host.Err() != nil {
		t.Fatalf("transient failure terminated host/worker = %#v, %v", worker, host.Err())
	}
	close(publisher.retry)
	waitResultPublication(t, state, started.Worker.WorkerKey)
	if attempts := publisher.attempts.Load(); attempts != 2 {
		t.Fatalf("publication attempts = %d, want 2", attempts)
	}
}

func TestResultFinalizationIntegrityErrorsRemainFatal(t *testing.T) {
	for _, err := range []error{
		store.ErrNotFound,
		store.ErrResultPackageAuthority,
		store.ErrResultPackageConflict,
		store.ErrResultPackageQuota,
		store.ErrResultPackageTransition,
		resultpackagefiles.ErrPublicationIntegrity,
		&resultFinalizationIntegrityError{err: errors.New("invalid worker binding")},
	} {
		classified := classifyResultFinalizationError(err)
		var retry *artifactRetentionError
		if errors.As(classified, &retry) {
			t.Fatalf("integrity error %v was classified retryable", err)
		}
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
	outbox := waitResultPublication(t, state, started.Worker.WorkerKey)
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
	stored, err := state.GetResultOutbox(context.Background(), outbox.ResultOutboxKey)
	if err != nil {
		t.Fatal(err)
	}
	if worker.Status != store.WorkerFinalizing || stored.State != store.ResultOutboxPublishPending {
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

func TestCleanupResultCaptureStagingRemovesValidDirectoryAndPreservesLegacyArtifacts(t *testing.T) {
	path := t.TempDir()
	validName := resultCapturePrefix + newTestID()
	legacyName := changesArtifactPrefix + newTestID()
	for _, name := range []string{validName, legacyName, "unrelated"} {
		if err := os.Mkdir(filepath.Join(path, name), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := cleanupResultCaptureStaging(root); err != nil {
		_ = root.Close()
		t.Fatal(err)
	}
	if err := root.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := os.OpenRoot(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := reopened.Stat(validName); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("valid result staging survived durable cleanup: %v", err)
	}
	for _, name := range []string{legacyName, "unrelated"} {
		if _, err := reopened.Stat(name); err != nil {
			t.Fatalf("unrelated artifact directory %q was removed: %v", name, err)
		}
	}
}

func TestCleanupResultCaptureStagingFailsClosedOnMalformedName(t *testing.T) {
	path := t.TempDir()
	malformedName := resultCapturePrefix + "not-an-id"
	legacyName := changesArtifactPrefix + newTestID()
	for _, name := range []string{malformedName, legacyName} {
		if err := os.Mkdir(filepath.Join(path, name), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err := cleanupResultCaptureStaging(root); err == nil ||
		!strings.Contains(err.Error(), "invalid result capture staging path") {
		t.Fatalf("malformed result staging cleanup error = %v", err)
	}
	for _, name := range []string{malformedName, legacyName} {
		if _, err := root.Stat(name); err != nil {
			t.Fatalf("fail-closed cleanup removed %q: %v", name, err)
		}
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

func resultManager(host *Host) *resultpackagefiles.Manager {
	return host.resultPackages.(*resultpackagefiles.Manager)
}

func waitResultPublication(
	t *testing.T,
	state *store.PeerStore,
	key store.WorkerKey,
) store.ResultOutbox {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		outboxes, err := state.ListPendingResultPublications(
			context.Background(), testControllerID, testDeviceID, 10,
		)
		if err != nil {
			t.Fatal(err)
		}
		for _, outbox := range outboxes {
			if outbox.WorkerKey == key {
				return outbox
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	outboxes, err := state.ListPendingResultPublications(
		context.Background(), testControllerID, testDeviceID, 10,
	)
	t.Fatalf("result publication = %#v, %v", outboxes, err)
	return store.ResultOutbox{}
}

type transientResultPackagePublisher struct {
	delegate *resultpackagefiles.Manager
	attempts atomic.Int64
	failed   chan struct{}
	retry    chan struct{}
}

func (p *transientResultPackagePublisher) PublishResultPackage(
	ctx context.Context,
	request resultpackagefiles.PublishResultPackageRequest,
) (store.ResultOutbox, error) {
	if p.attempts.Add(1) == 1 {
		close(p.failed)
		return store.ResultOutbox{}, errors.New("transient result package filesystem failure")
	}
	select {
	case <-ctx.Done():
		return store.ResultOutbox{}, ctx.Err()
	case <-p.retry:
	}
	return p.delegate.PublishResultPackage(ctx, request)
}

func testManagedRolloutLine(eventType, turnID string) string {
	return "{\"type\":\"event_msg\",\"payload\":{\"type\":\"" + eventType +
		"\",\"turn_id\":\"" + turnID + "\"}}\n"
}

func testManagedFailedRolloutLine(turnID string) string {
	return "{\"type\":\"event_msg\",\"payload\":{\"type\":\"task_complete\",\"turn_id\":\"" +
		turnID + "\",\"error\":{\"message\":\"managed turn failed\"}}}\n"
}

func appendSyncedFile(path, data string) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		return err
	}
	_, writeErr := file.WriteString(data)
	return errors.Join(writeErr, file.Sync(), file.Close())
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

func makeStartupTurnIntent(
	t *testing.T,
	state *store.PeerStore,
	root string,
	bound bool,
) (store.WorkerReservation, store.WorkerTurnStartIntent, string) {
	t.Helper()
	ctx := context.Background()
	key := store.WorkerKey{
		ControllerID: testControllerID, TreeID: testTreeID, AgentID: newTestID(),
	}
	workspacePath := filepath.Join(root, workspaceName(key))
	if err := os.Mkdir(workspacePath, 0o700); err != nil {
		t.Fatal(err)
	}
	worker, err := state.ReserveWorkerStart(ctx, store.WorkerReservation{
		WorkerKey: key, ParentAgentID: testParentID, DeviceID: testDeviceID,
		TaskName: "startup reconciliation", PromptDigest: promptDigest("startup prompt"),
		WorkspacePath: workspacePath, ProfileVersion: workerProfileVersion,
	}, 1, time.Unix(1_700_100_000, 0))
	if err != nil {
		t.Fatal(err)
	}
	threadID := newTestID()
	worker, err = state.AttachWorkerThread(ctx, key, threadID, time.Unix(1_700_100_001, 0))
	if err != nil {
		t.Fatal(err)
	}
	worker, err = state.MarkWorkerReady(ctx, key, time.Unix(1_700_100_002, 0))
	if err != nil {
		t.Fatal(err)
	}
	intent, _, err := state.PrepareWorkerTurnStartIntent(
		ctx,
		store.PrepareWorkerTurnStartIntentRequest{
			WorkerKey: key, IntentID: newTestID(), DeviceID: testDeviceID,
			ManagedThreadID: threadID, PackageID: newTestID(),
			Rollout: store.WorkerRolloutLocator{
				Status: store.WorkerRolloutUnavailable, FailureCode: rolloutLocatorFailureCode,
			},
			ReservationLimitBytes: int64(
				protocol.MaximumResultManifestBytes + protocol.MaximumResultRolloutBytes,
			),
		},
		time.Unix(1_700_100_003, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	turnID := newTestID()
	if bound {
		resolution, err := state.BindWorkerTurnStartIntent(
			ctx, key, intent.IntentID, turnID, time.Unix(1_700_100_004, 0),
		)
		if err != nil {
			t.Fatal(err)
		}
		worker = resolution.Worker
		intent = resolution.Intent
	}
	return worker, intent, turnID
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
