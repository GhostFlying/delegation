package rootapply

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/gitworkspace"
	"github.com/GhostFlying/delegation/internal/localbridge"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/resultpackagefiles"
)

const (
	applyTestControllerID = "123e4567-e89b-42d3-a456-426614174500"
	applyTestTreeID       = "123e4567-e89b-42d3-a456-426614174501"
	applyTestRootAgentID  = "123e4567-e89b-42d3-a456-426614174502"
	applyTestWorkerID     = "123e4567-e89b-42d3-a456-426614174503"
	applyTestRootDeviceID = "123e4567-e89b-42d3-a456-426614174504"
	applyTestPeerDeviceID = "123e4567-e89b-42d3-a456-426614174505"
	applyTestThreadID     = "123e4567-e89b-42d3-a456-426614174506"
	applyTestTurnID       = "123e4567-e89b-42d3-a456-426614174507"
	applyTestPackageID    = "123e4567-e89b-42d3-a456-426614174508"
	applyTestWorkspaceID  = "123e4567-e89b-42d3-a456-426614174509"
	applyTestApplyID      = "123e4567-e89b-42d3-a456-426614174510"
	applyTestOtherID      = "123e4567-e89b-42d3-a456-426614174511"
	applyTestGitURL       = "ssh://git@example.invalid/repository.git"
)

type applyPackageSource struct {
	manifest protocol.ResultManifest
	parts    map[protocol.ResultPackagePartKind]string
}

type unmanagedApplyPackageSource struct{}

func (unmanagedApplyPackageSource) LookupApplyManifest(
	_ context.Context,
	request resultpackagefiles.ApplyPackageRequest,
) (protocol.ResultManifest, error) {
	return protocol.ResultManifest{
		Version: protocol.ResultManifestVersion, PackageID: request.PackageID,
		ControllerID: request.Root.ControllerID, TreeID: request.Root.TreeID,
		SourceAgentID: applyTestWorkerID, SourceDeviceID: applyTestPeerDeviceID,
		ManagedThreadID: applyTestThreadID, TurnID: applyTestTurnID, LifecycleRevision: 1,
		Terminal: protocol.ResultTerminal{Outcome: protocol.ResultTerminalCompleted}, CapturedAt: 1,
		Rollout: protocol.ResultRolloutComponent{
			Status: protocol.ResultRolloutCaptureFailed, FailureCode: "rollout_unavailable",
		},
		Workspace: protocol.ResultWorkspaceComponent{
			Status: protocol.ResultWorkspaceNotManaged, BaseWarnings: []string{}, ResultWarnings: []string{},
		},
		Parts: []protocol.ResultPackagePartDescriptor{},
	}, nil
}

func (unmanagedApplyPackageSource) MaterializeApplyWorkspace(
	context.Context,
	resultpackagefiles.MaterializeApplyPackageRequest,
	*os.Root,
) (protocol.ResultManifest, error) {
	return protocol.ResultManifest{}, errors.New("unmanaged test package cannot be materialized")
}

func (s *applyPackageSource) LookupApplyManifest(
	_ context.Context,
	request resultpackagefiles.ApplyPackageRequest,
) (protocol.ResultManifest, error) {
	if request.PackageID != s.manifest.PackageID || request.Root.ControllerID != s.manifest.ControllerID ||
		request.Root.TreeID != s.manifest.TreeID || request.Root.ParentAgentID != "" {
		return protocol.ResultManifest{}, errors.New("test package authority mismatch")
	}
	return s.manifest, nil
}

func (s *applyPackageSource) MaterializeApplyWorkspace(
	ctx context.Context,
	request resultpackagefiles.MaterializeApplyPackageRequest,
	destination *os.Root,
) (protocol.ResultManifest, error) {
	if request.PackageID != s.manifest.PackageID ||
		request.Authorization.WorkspaceID != s.manifest.Workspace.WorkspaceID ||
		request.Authorization.BaseManifestHash != s.manifest.Workspace.BaseManifestHash {
		return protocol.ResultManifest{}, errors.New("test materialization authority mismatch")
	}
	for kind, sourcePath := range s.parts {
		if err := ctx.Err(); err != nil {
			return protocol.ResultManifest{}, err
		}
		name, err := kind.FileName()
		if err != nil {
			return protocol.ResultManifest{}, err
		}
		input, err := os.Open(sourcePath)
		if err != nil {
			return protocol.ResultManifest{}, err
		}
		output, err := destination.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			_ = input.Close()
			return protocol.ResultManifest{}, err
		}
		_, copyErr := io.Copy(output, input)
		closeErr := errors.Join(input.Close(), output.Sync(), output.Close())
		if copyErr != nil || closeErr != nil {
			return protocol.ResultManifest{}, errors.Join(copyErr, closeErr)
		}
	}
	return s.manifest, nil
}

type rootApplyFixture struct {
	runner        gitworkspace.Runner
	manager       *Manager
	journalRoot   string
	packages      *applyPackageSource
	rootPath      string
	sourcePath    string
	workerPath    string
	base          protocol.WorkspaceManifest
	expected      protocol.WorkspaceManifest
	request       localbridge.ResultApplyRequest
	authorization protocol.AuthorizeResultApplyResult
	initialHead   string
	initialRef    string
	initialRefOID string
}

func TestApplyAgentChangesPreservesExactGitStatesAndRootHead(t *testing.T) {
	tests := []struct {
		name         string
		rootMutation func(*testing.T, string, string)
		worker       func(*testing.T, string, string)
	}{
		{
			name: "commit only",
			worker: func(t *testing.T, git, root string) {
				writeApplyFile(t, filepath.Join(root, "committed.txt"), "worker commit\n")
				gitApplyRun(t, git, root, "add", "committed.txt")
				gitApplyCommit(t, git, root, "worker commit")
			},
		},
		{
			name: "dirty only",
			worker: func(t *testing.T, git, root string) {
				writeApplyFile(t, filepath.Join(root, "nested", "hello.txt"), "worker unstaged\n")
				writeApplyFile(t, filepath.Join(root, "staged.txt"), "worker staged\n")
				gitApplyRun(t, git, root, "add", "staged.txt")
				writeApplyFile(t, filepath.Join(root, "untracked.txt"), "worker untracked\n")
			},
		},
		{
			name: "commit and dirty",
			worker: func(t *testing.T, git, root string) {
				writeApplyFile(t, filepath.Join(root, "committed.txt"), "worker commit\n")
				gitApplyRun(t, git, root, "add", "committed.txt")
				gitApplyCommit(t, git, root, "worker commit")
				writeApplyFile(t, filepath.Join(root, "nested", "hello.txt"), "worker dirty\n")
				writeApplyFile(t, filepath.Join(root, "untracked.txt"), "worker untracked\n")
			},
		},
		{
			name: "dirty base plus staged unstaged and untracked result",
			rootMutation: func(t *testing.T, git, root string) {
				writeApplyFile(t, filepath.Join(root, "root-staged.txt"), "root staged\n")
				gitApplyRun(t, git, root, "add", "root-staged.txt")
				writeApplyFile(t, filepath.Join(root, "nested", "hello.txt"), "root unstaged\n")
				writeApplyFile(t, filepath.Join(root, "root-untracked.txt"), "root untracked\n")
			},
			worker: func(t *testing.T, git, root string) {
				writeApplyFile(t, filepath.Join(root, "worker-commit.txt"), "worker commit\n")
				gitApplyRun(t, git, root, "add", "worker-commit.txt")
				gitApplyCommitOnly(t, git, root, "worker commit", "worker-commit.txt")
				writeApplyFile(t, filepath.Join(root, "worker-staged.txt"), "worker staged\n")
				gitApplyRun(t, git, root, "add", "worker-staged.txt")
				writeApplyFile(t, filepath.Join(root, "nested", "hello.txt"), "worker unstaged\n")
				writeApplyFile(t, filepath.Join(root, "worker-untracked.txt"), "worker untracked\n")
			},
		},
		{
			name: "binary rename delete and intent to add",
			rootMutation: func(t *testing.T, git, root string) {
				writeApplyFile(t, filepath.Join(root, "delete.txt"), "delete me\n")
				gitApplyRun(t, git, root, "add", "delete.txt")
				gitApplyCommit(t, git, root, "add deletion fixture")
			},
			worker: func(t *testing.T, git, root string) {
				if err := os.Rename(
					filepath.Join(root, "nested", "hello.txt"), filepath.Join(root, "renamed.bin"),
				); err != nil {
					t.Fatal(err)
				}
				writeApplyBytes(t, filepath.Join(root, "renamed.bin"), []byte{'s', 't', 'a', 'g', 'e', 'd', 0, '\n'})
				gitApplyRun(t, git, root, "add", "-A", "nested/hello.txt", "renamed.bin")
				writeApplyBytes(t, filepath.Join(root, "renamed.bin"), []byte{'u', 'n', 's', 't', 'a', 'g', 'e', 'd', 0, '\n'})
				writeApplyFile(t, filepath.Join(root, "intent.txt"), "intent initial\n")
				gitApplyRun(t, git, root, "add", "-N", "intent.txt")
				writeApplyFile(t, filepath.Join(root, "intent.txt"), "intent final\n")
				if err := os.Remove(filepath.Join(root, "delete.txt")); err != nil {
					t.Fatal(err)
				}
				writeApplyBytes(t, filepath.Join(root, "untracked.bin"), []byte{0, 1, 2, 3})
			},
		},
		{
			name: "worker removes root untracked state",
			rootMutation: func(t *testing.T, _, root string) {
				writeApplyFile(t, filepath.Join(root, "root-untracked.txt"), "remove me\n")
			},
			worker: func(t *testing.T, _, root string) {
				if err := os.Remove(filepath.Join(root, "root-untracked.txt")); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "worker retracks root cached deletion",
			rootMutation: func(t *testing.T, git, root string) {
				gitApplyRun(t, git, root, "rm", "--cached", "--", "nested/hello.txt")
			},
			worker: func(t *testing.T, git, root string) {
				gitApplyRun(t, git, root, "add", "--", "nested/hello.txt")
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newRootApplyFixture(t, test.rootMutation, test.worker, applyTestPeerDeviceID)
			result := fixture.apply(t)
			if result.Outcome != localbridge.ApplyAgentChangesApplied || result.FailureCode != "" {
				t.Fatalf("apply result = %#v", result)
			}
			fixture.assertDesired(t)
		})
	}
}

func TestApplyAgentChangesPreservesRootCWDWhenWorkerCommitsItsRemoval(t *testing.T) {
	tests := []struct {
		name       string
		worker     func(*testing.T, string, string)
		resultPath string
	}{
		{
			name: "delete",
			worker: func(t *testing.T, git, root string) {
				gitApplyRun(t, git, root, "rm", "-r", "--", "nested")
				gitApplyCommit(t, git, root, "delete original cwd")
			},
		},
		{
			name: "rename",
			worker: func(t *testing.T, git, root string) {
				gitApplyRun(t, git, root, "mv", "--", "nested", "renamed")
				gitApplyCommit(t, git, root, "rename original cwd")
			},
			resultPath: filepath.Join("renamed", "hello.txt"),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newRootApplyFixture(t, nil, test.worker, applyTestPeerDeviceID)
			result := fixture.apply(t)
			if result.Outcome != localbridge.ApplyAgentChangesApplied || result.FailureCode != "" {
				t.Fatalf("apply result = %#v", result)
			}
			fixture.assertDesired(t)
			entries, err := os.ReadDir(filepath.Join(fixture.rootPath, "nested"))
			if err != nil || len(entries) != 0 {
				t.Fatalf("preserved root cwd entries = %#v, %v", entries, err)
			}
			if test.resultPath != "" {
				contents, err := os.ReadFile(filepath.Join(fixture.rootPath, test.resultPath))
				if err != nil || string(contents) != "hello\n" {
					t.Fatalf("renamed result = %q, %v", contents, err)
				}
			}
			replay := fixture.prepare(t)
			if replay.Completed == nil || *replay.Completed != result || replay.Authorization != nil {
				t.Fatalf("completed replay = %#v", replay)
			}
			assertCompactJournal(t, filepath.Join(
				fixture.journalRoot, journalDirectoryName, fixture.request.Params.ApplyID,
			))
		})
	}
}

func TestApplyAgentChangesHasEquivalentSelfAndRemoteSemantics(t *testing.T) {
	for _, targetDeviceID := range []string{applyTestRootDeviceID, applyTestPeerDeviceID} {
		t.Run(targetDeviceID, func(t *testing.T) {
			fixture := newRootApplyFixture(t, nil, func(t *testing.T, _, root string) {
				writeApplyFile(t, filepath.Join(root, "self-or-remote.txt"), "same result\n")
			}, targetDeviceID)
			if result := fixture.apply(t); result.Outcome != localbridge.ApplyAgentChangesApplied {
				t.Fatalf("apply result = %#v", result)
			}
			fixture.assertDesired(t)
		})
	}
}

func TestApplyAgentChangesAcceptsFullHistoryTransportWarning(t *testing.T) {
	fixture := newRootApplyFixture(t, nil, func(t *testing.T, _, root string) {
		writeApplyFile(t, filepath.Join(root, "full-fallback.txt"), "worker result\n")
	}, applyTestPeerDeviceID)
	fixture.packages.manifest.Workspace.BaseWarnings = []string{
		protocol.WorkspaceWarningFullHistoryFallback,
	}
	_, descriptor, err := protocol.EncodeResultManifest(fixture.packages.manifest)
	if err != nil {
		t.Fatal(err)
	}
	fixture.authorization.ManifestSHA256 = descriptor.SHA256
	result := fixture.apply(t)
	if result.Outcome != localbridge.ApplyAgentChangesApplied || result.FailureCode != "" {
		t.Fatalf("full fallback apply = %#v", result)
	}
	fixture.assertDesired(t)
}

func TestApplyAgentChangesRejectsContentWarningMismatchWithFullHistoryWarning(t *testing.T) {
	tests := []struct {
		name     string
		warnings []string
	}{
		{
			name: "lfs",
			warnings: []string{
				protocol.WorkspaceWarningLFSPayloadNotTransferred,
				protocol.WorkspaceWarningFullHistoryFallback,
			},
		},
		{
			name: "submodule",
			warnings: []string{
				protocol.WorkspaceWarningFullHistoryFallback,
				protocol.WorkspaceWarningSubmoduleRepositoryNotTransferred,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newRootApplyFixture(t, nil, func(t *testing.T, _, root string) {
				writeApplyFile(t, filepath.Join(root, "warning-mismatch.txt"), "worker result\n")
			}, applyTestPeerDeviceID)
			fixture.packages.manifest.Workspace.BaseWarnings = test.warnings
			_, descriptor, err := protocol.EncodeResultManifest(fixture.packages.manifest)
			if err != nil {
				t.Fatal(err)
			}
			fixture.authorization.ManifestSHA256 = descriptor.SHA256
			result := fixture.apply(t)
			if result.Outcome != localbridge.ApplyAgentChangesNeedsResolution ||
				result.FailureCode != "root_workspace_conflict" {
				t.Fatalf("warning mismatch apply = %#v", result)
			}
		})
	}
}

func TestApplyAgentChangesSupportsLinkedWorktreeWithoutMovingHeadOrRef(t *testing.T) {
	fixture := newRootApplyFixtureMode(t, nil, func(t *testing.T, git, root string) {
		writeApplyFile(t, filepath.Join(root, "linked-commit.txt"), "linked commit\n")
		gitApplyRun(t, git, root, "add", "linked-commit.txt")
		gitApplyCommit(t, git, root, "linked worker commit")
		writeApplyFile(t, filepath.Join(root, "linked-dirty.txt"), "linked dirty\n")
	}, applyTestPeerDeviceID, true)
	if result := fixture.apply(t); result.Outcome != localbridge.ApplyAgentChangesApplied {
		t.Fatalf("linked worktree apply = %#v", result)
	}
	fixture.assertDesired(t)
}

func TestApplyAgentChangesDetectsDriftWithoutMutationAndReplays(t *testing.T) {
	fixture := newRootApplyFixture(t, nil, func(t *testing.T, _, root string) {
		writeApplyFile(t, filepath.Join(root, "worker.txt"), "worker\n")
	}, applyTestPeerDeviceID)
	preparation := fixture.prepare(t)
	writeApplyFile(t, filepath.Join(fixture.rootPath, "external.txt"), "external drift\n")
	before, err := fixture.runner.InspectApplySource(context.Background(), fixture.sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	beforeStatus := gitApplyOutput(t, fixture.runner.Binary, fixture.rootPath, "status", "--porcelain=v2", "-z", "--untracked-files=all")
	result, err := fixture.manager.ApplyAuthorizedResult(
		context.Background(), fixture.request, fixture.authorization,
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != localbridge.ApplyAgentChangesNeedsResolution ||
		result.FailureCode != "root_workspace_conflict" {
		t.Fatalf("drift apply result = %#v", result)
	}
	after, err := fixture.runner.InspectApplySource(context.Background(), fixture.sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	afterStatus := gitApplyOutput(t, fixture.runner.Binary, fixture.rootPath, "status", "--porcelain=v2", "-z", "--untracked-files=all")
	if !sameWorkspaceManifest(before.Manifest, after.Manifest) || beforeStatus != afterStatus {
		t.Fatal("conflicting apply mutated the root workspace")
	}
	replayed, err := fixture.manager.PrepareResultApply(context.Background(), fixture.request)
	if err != nil || replayed.Completed == nil || *replayed.Completed != result {
		t.Fatalf("completed replay = %#v, %v", replayed, err)
	}
	conflict := fixture.request
	conflict.Params.PackageID = applyTestOtherID
	if _, err := fixture.manager.PrepareResultApply(context.Background(), conflict); !errors.Is(err, localbridge.ErrApplyRequestConflict) {
		t.Fatalf("different-argument replay error = %v", err)
	}
	if preparation.Authorization == nil {
		t.Fatal("initial preparation did not require authorization")
	}
}

func TestCompletedApplyJournalCompactsPayloadsAndReplays(t *testing.T) {
	fixture := newRootApplyFixture(t, nil, func(t *testing.T, git, root string) {
		writeApplyFile(t, filepath.Join(root, "committed.txt"), "committed\n")
		gitApplyRun(t, git, root, "add", "committed.txt")
		gitApplyCommit(t, git, root, "worker commit")
		writeApplyFile(t, filepath.Join(root, "dirty.txt"), "dirty\n")
	}, applyTestPeerDeviceID)
	result := fixture.apply(t)
	journalPath := filepath.Join(fixture.journalRoot, journalDirectoryName, applyTestApplyID)
	assertCompactJournal(t, journalPath)
	writeApplyFile(t, filepath.Join(journalPath, desiredOverlayName), "crash-leftover\n")
	fixture.restartManager(t)
	assertCompactJournal(t, journalPath)
	replay := fixture.prepare(t)
	if replay.Completed == nil || *replay.Completed != result {
		t.Fatalf("compacted journal replay = %#v, want %#v", replay, result)
	}
	replayedResult, err := fixture.manager.ApplyAuthorizedResult(
		context.Background(), fixture.request, fixture.authorization,
	)
	if err != nil || replayedResult != result {
		t.Fatalf("completed authorized replay = %#v, %v", replayedResult, err)
	}
	assertCompactJournal(t, journalPath)
}

func TestStartupPrunesJournalCreationInterruptedBeforeFirstRecord(t *testing.T) {
	runner := rootApplyTestRunner(t)
	workspaceRoot := privateApplyDirectory(t, filepath.Join(t.TempDir(), "managed"))
	journalRoot := filepath.Join(workspaceRoot, journalDirectoryName)
	if err := os.Mkdir(journalRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	incompleteID := retentionTestID(900)
	incomplete := filepath.Join(journalRoot, incompleteID)
	if err := os.Mkdir(incomplete, 0o700); err != nil {
		t.Fatal(err)
	}
	writeApplyFile(t, filepath.Join(incomplete, ".journal-interrupted.tmp"), "partial\n")
	manager, err := New(Options{
		WorkspaceRoot: workspaceRoot, Runner: runner, Packages: unmanagedApplyPackageSource{},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	if _, err := os.Lstat(incomplete); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("interrupted journal creation was not pruned: %v", err)
	}
}

func TestActiveJournalRetentionBoundsCountBytesAndAge(t *testing.T) {
	t.Run("count and age", func(t *testing.T) {
		fixture := newRootApplyFixture(t, nil, func(t *testing.T, _, root string) {
			writeApplyFile(t, filepath.Join(root, "worker.txt"), "worker\n")
		}, applyTestPeerDeviceID)
		now := time.Now().Add(-defaultMaximumActiveAge - time.Hour)
		fixture.manager.now = func() time.Time { return now }
		fixture.manager.retention.maximumActiveJournals = 2
		requests := []localbridge.ResultApplyRequest{fixture.request, fixture.request, fixture.request}
		requests[1].Params.ApplyID = retentionTestID(1)
		requests[2].Params.ApplyID = retentionTestID(2)
		for _, request := range requests[:2] {
			preparation, err := fixture.manager.PrepareResultApply(context.Background(), request)
			if err != nil || preparation.Authorization == nil {
				t.Fatalf("active journal preparation = %#v, %v", preparation, err)
			}
		}
		if _, err := fixture.manager.PrepareResultApply(
			context.Background(), requests[2],
		); !errors.Is(err, localbridge.ErrApplyBacklog) {
			t.Fatalf("active journal count admission error = %v", err)
		}
		fixture.restartManager(t)
		for _, request := range requests[:2] {
			preparation, err := fixture.manager.PrepareResultApply(context.Background(), request)
			if err != nil || preparation.Completed == nil ||
				preparation.Completed.Outcome != localbridge.ApplyAgentChangesNeedsResolution ||
				preparation.Completed.FailureCode != "root_workspace_recovery_required" {
				t.Fatalf("expired active journal replay = %#v, %v", preparation, err)
			}
			assertCompactJournal(
				t, filepath.Join(fixture.journalRoot, journalDirectoryName, request.Params.ApplyID),
			)
		}
	})

	t.Run("bytes", func(t *testing.T) {
		fixture := newRootApplyFixture(t, nil, func(t *testing.T, _, root string) {
			writeApplyFile(t, filepath.Join(root, "worker.txt"), "worker\n")
		}, applyTestPeerDeviceID)
		fixture.prepare(t)
		lease, err := fixture.manager.openJournal(fixture.request.Params.ApplyID)
		if err != nil {
			t.Fatal(err)
		}
		size, sizeErr := journalDirectoryBytes(lease.root)
		closeErr := lease.close()
		if sizeErr != nil || closeErr != nil {
			t.Fatal(errors.Join(sizeErr, closeErr))
		}
		fixture.manager.retention.maximumActiveBytes = size + maximumJournalBytes - 1
		request := fixture.request
		request.Params.ApplyID = retentionTestID(3)
		if _, err := fixture.manager.PrepareResultApply(
			context.Background(), request,
		); !errors.Is(err, localbridge.ErrApplyBacklog) {
			t.Fatalf("active journal byte admission error = %v", err)
		}
	})
}

func TestTerminalJournalRetentionBoundsRepeatedAppliesByCountBytesAndAge(t *testing.T) {
	runner := rootApplyTestRunner(t)
	workspaceRoot := privateApplyDirectory(t, filepath.Join(t.TempDir(), "managed"))
	manager, err := New(Options{
		WorkspaceRoot: workspaceRoot, Runner: runner, Packages: unmanagedApplyPackageSource{},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	now := time.Unix(2_000_000_000, 0)
	manager.now = func() time.Time { return now }
	manager.retention.maximumTerminalJournals = 2
	requests := make([]localbridge.ResultApplyRequest, 3)
	for index := range requests {
		requests[index] = retentionTestRequest(index)
		preparation, err := manager.PrepareResultApply(context.Background(), requests[index])
		if err != nil || preparation.Completed == nil ||
			preparation.Completed.Outcome != localbridge.ApplyAgentChangesUnchanged {
			t.Fatalf("terminal apply %d = %#v, %v", index, preparation, err)
		}
		now = now.Add(time.Second)
	}
	names := journalDirectoryNames(t, filepath.Join(workspaceRoot, journalDirectoryName))
	wantNames := []string{requests[1].Params.ApplyID, requests[2].Params.ApplyID}
	if !reflect.DeepEqual(names, wantNames) {
		t.Fatalf("retained terminal journals = %#v, want %#v", names, wantNames)
	}
	for _, request := range requests[1:] {
		assertCompactJournal(
			t, filepath.Join(workspaceRoot, journalDirectoryName, request.Params.ApplyID),
		)
	}
	replay, err := manager.PrepareResultApply(context.Background(), requests[1])
	if err != nil || replay.Completed == nil || replay.Completed.Outcome != localbridge.ApplyAgentChangesUnchanged {
		t.Fatalf("retained terminal replay = %#v, %v", replay, err)
	}

	manager.retention.maximumTerminalJournals = defaultMaximumTerminalJournals
	journalRoot, err := manager.openJournal(requests[1].Params.ApplyID)
	if err != nil {
		t.Fatal(err)
	}
	receiptBytes, sizeErr := journalDirectoryBytes(journalRoot.root)
	closeErr := journalRoot.close()
	if sizeErr != nil || closeErr != nil {
		t.Fatal(errors.Join(sizeErr, closeErr))
	}
	manager.retention.maximumTerminalBytes = maximumJournalBytes + receiptBytes
	fourth := retentionTestRequest(4)
	preparation, err := manager.PrepareResultApply(context.Background(), fourth)
	if err != nil || preparation.Completed == nil {
		t.Fatalf("byte-bounded terminal apply = %#v, %v", preparation, err)
	}
	if got := len(journalDirectoryNames(t, filepath.Join(workspaceRoot, journalDirectoryName))); got != 2 {
		t.Fatalf("byte-bounded terminal journal count = %d, want 2", got)
	}

	now = now.Add(manager.retention.maximumTerminalAge)
	if _, err := manager.maintainJournals(false); err != nil {
		t.Fatal(err)
	}
	if names := journalDirectoryNames(t, filepath.Join(workspaceRoot, journalDirectoryName)); len(names) != 0 {
		t.Fatalf("expired terminal journals retained: %#v", names)
	}
}

func TestApplyAgentChangesRejectsMutationBoundaryDriftWithoutDelegationWrites(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, *rootApplyFixture)
	}{
		{
			name: "tracked worktree",
			mutate: func(t *testing.T, fixture *rootApplyFixture) {
				writeApplyFile(t, filepath.Join(fixture.rootPath, "nested", "hello.txt"), "external tracked\n")
			},
		},
		{
			name: "untracked worktree",
			mutate: func(t *testing.T, fixture *rootApplyFixture) {
				writeApplyFile(t, filepath.Join(fixture.rootPath, "external-untracked.txt"), "external untracked\n")
			},
		},
		{
			name: "index",
			mutate: func(t *testing.T, fixture *rootApplyFixture) {
				writeApplyFile(t, filepath.Join(fixture.rootPath, "external-staged.txt"), "external staged\n")
				gitApplyRun(t, fixture.runner.Binary, fixture.rootPath, "add", "external-staged.txt")
			},
		},
		{
			name: "current ref",
			mutate: func(t *testing.T, fixture *rootApplyFixture) {
				writeApplyFile(t, filepath.Join(fixture.rootPath, "external-commit.txt"), "external commit\n")
				gitApplyRun(t, fixture.runner.Binary, fixture.rootPath, "add", "external-commit.txt")
				gitApplyCommit(t, fixture.runner.Binary, fixture.rootPath, "external commit")
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newRootApplyFixture(t, nil, func(t *testing.T, git, root string) {
				writeApplyFile(t, filepath.Join(root, "worker.txt"), "worker\n")
				gitApplyRun(t, git, root, "add", "worker.txt")
			}, applyTestPeerDeviceID)
			workerOID := gitApplyOutput(t, fixture.runner.Binary, fixture.workerPath, "rev-parse", ":worker.txt")
			if gitApplyObjectExists(t, fixture.runner.Binary, fixture.rootPath, workerOID) {
				t.Fatal("worker index object unexpectedly existed in root before apply")
			}
			fixture.prepare(t)
			var wantManifest protocol.WorkspaceManifest
			var wantStatus, wantHead, wantRefOID string
			fixture.manager.fault = func(point string) error {
				if point != faultBeforeDestructiveWrite {
					return nil
				}
				test.mutate(t, fixture)
				current, err := fixture.runner.InspectApplySource(context.Background(), fixture.sourcePath)
				if err != nil {
					t.Fatal(err)
				}
				wantManifest = current.Manifest
				wantStatus = gitApplyOutput(t, fixture.runner.Binary, fixture.rootPath, "status", "--porcelain=v2", "-z", "--untracked-files=all")
				wantHead = gitApplyOutput(t, fixture.runner.Binary, fixture.rootPath, "rev-parse", "HEAD")
				wantRefOID = gitApplyOutput(t, fixture.runner.Binary, fixture.rootPath, "rev-parse", fixture.initialRef)
				return nil
			}
			result, err := fixture.manager.ApplyAuthorizedResult(
				context.Background(), fixture.request, fixture.authorization,
			)
			if err != nil || result.Outcome != localbridge.ApplyAgentChangesNeedsResolution ||
				result.FailureCode != "root_workspace_conflict" {
				t.Fatalf("mutation-boundary drift result = %#v, %v", result, err)
			}
			current, err := fixture.runner.InspectApplySource(context.Background(), fixture.sourcePath)
			if err != nil {
				t.Fatal(err)
			}
			gotStatus := gitApplyOutput(t, fixture.runner.Binary, fixture.rootPath, "status", "--porcelain=v2", "-z", "--untracked-files=all")
			gotHead := gitApplyOutput(t, fixture.runner.Binary, fixture.rootPath, "rev-parse", "HEAD")
			gotRefOID := gitApplyOutput(t, fixture.runner.Binary, fixture.rootPath, "rev-parse", fixture.initialRef)
			if !sameWorkspaceManifest(current.Manifest, wantManifest) || gotStatus != wantStatus ||
				gotHead != wantHead || gotRefOID != wantRefOID {
				t.Fatal("Delegation mutated root state after mutation-boundary drift")
			}
			if gitApplyObjectExists(t, fixture.runner.Binary, fixture.rootPath, workerOID) {
				t.Fatal("conflicting apply wrote the worker index object into the root repository")
			}
		})
	}
}

func TestApplyAgentChangesPreservesIgnoredPathsThatConflictWithWorkerTrackedResults(t *testing.T) {
	tests := []struct {
		name         string
		rootMutation func(*testing.T, string, string)
		worker       func(*testing.T, string, string)
		ignoredPath  string
		want         string
	}{
		{
			name: "force-added ignored file",
			rootMutation: func(t *testing.T, _, root string) {
				writeApplyFile(t, filepath.Join(root, "collision.cache"), "local cache\n")
			},
			worker: func(t *testing.T, git, root string) {
				writeApplyFile(t, filepath.Join(root, "collision.cache"), "worker result\n")
				gitApplyRun(t, git, root, "add", "-f", "collision.cache")
			},
			ignoredPath: "collision.cache",
			want:        "local cache\n",
		},
		{
			name: "case-folded ignored directory replaced by tracked file",
			rootMutation: func(t *testing.T, git, root string) {
				writeApplyFile(t, filepath.Join(root, ".gitignore"), "*.cache\n[Bb]uild/\n")
				gitApplyRun(t, git, root, "add", ".gitignore")
				gitApplyCommit(t, git, root, "ignore build output")
				writeApplyFile(t, filepath.Join(root, "Build", "local.dat"), "local build output\n")
			},
			worker: func(t *testing.T, git, root string) {
				writeApplyFile(t, filepath.Join(root, "build"), "worker tracked file\n")
				gitApplyRun(t, git, root, "add", "-f", "build")
			},
			ignoredPath: filepath.Join("Build", "local.dat"),
			want:        "local build output\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newRootApplyFixture(
				t, test.rootMutation, test.worker, applyTestPeerDeviceID,
			)
			before, err := fixture.runner.InspectApplySource(context.Background(), fixture.sourcePath)
			if err != nil {
				t.Fatal(err)
			}
			beforeStatus := gitApplyOutput(
				t, fixture.runner.Binary, fixture.rootPath,
				"status", "--porcelain=v2", "-z", "--untracked-files=all", "--ignored=matching",
			)
			result := fixture.apply(t)
			if result.Outcome != localbridge.ApplyAgentChangesNeedsResolution ||
				result.FailureCode != "root_workspace_conflict" {
				t.Fatalf("ignored collision result = %#v", result)
			}
			after, err := fixture.runner.InspectApplySource(context.Background(), fixture.sourcePath)
			if err != nil {
				t.Fatal(err)
			}
			afterStatus := gitApplyOutput(
				t, fixture.runner.Binary, fixture.rootPath,
				"status", "--porcelain=v2", "-z", "--untracked-files=all", "--ignored=matching",
			)
			contents, err := os.ReadFile(filepath.Join(fixture.rootPath, test.ignoredPath))
			if err != nil || string(contents) != test.want ||
				!sameWorkspaceManifest(before.Manifest, after.Manifest) || beforeStatus != afterStatus {
				t.Fatalf(
					"ignored root state changed: contents=%q err=%v before=%q after=%q",
					contents, err, beforeStatus, afterStatus,
				)
			}
		})
	}
}

func TestApplyAgentChangesIgnoresGlobalSmudgeFilter(t *testing.T) {
	fixture := newRootApplyFixture(t, func(t *testing.T, git, root string) {
		writeApplyFile(t, filepath.Join(root, ".gitattributes"), "nested/hello.txt filter=hostile\n")
		gitApplyRun(t, git, root, "add", ".gitattributes")
	}, func(t *testing.T, _, root string) {
		writeApplyFile(t, filepath.Join(root, "worker.txt"), "worker\n")
	}, applyTestPeerDeviceID)
	sentinel := filepath.Join(t.TempDir(), "executed")
	script := filepath.Join(t.TempDir(), "hostile.sh")
	content := "#!/bin/sh\nprintf executed > '" + strings.ReplaceAll(sentinel, "'", "'\\''") + "'\ncat\n"
	if err := os.WriteFile(script, []byte(content), 0o700); err != nil {
		t.Fatal(err)
	}
	home := t.TempDir()
	config := "[filter \"hostile\"]\n\tsmudge = " + script + "\n"
	if err := os.WriteFile(filepath.Join(home, ".gitconfig"), []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", home)
	if result := fixture.apply(t); result.Outcome != localbridge.ApplyAgentChangesApplied {
		t.Fatalf("apply result = %#v", result)
	}
	if _, err := os.Lstat(sentinel); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("global smudge filter executed: %v", err)
	}
	fixture.assertDesired(t)
}

func TestApplyAgentChangesRecoversAtMutationCrashBoundaries(t *testing.T) {
	for _, point := range []string{
		faultBeforeMutation, faultBeforeDestructiveWrite, faultAfterMutation,
	} {
		t.Run(point, func(t *testing.T) {
			fixture := newRootApplyFixture(t, nil, func(t *testing.T, git, root string) {
				writeApplyFile(t, filepath.Join(root, "committed.txt"), "commit\n")
				gitApplyRun(t, git, root, "add", "committed.txt")
				gitApplyCommit(t, git, root, "worker commit")
				writeApplyFile(t, filepath.Join(root, "dirty.txt"), "dirty\n")
			}, applyTestPeerDeviceID)
			fixture.manager.fault = func(current string) error {
				if current == point {
					return errSimulatedCrash
				}
				return nil
			}
			fixture.prepare(t)
			if _, err := fixture.manager.ApplyAuthorizedResult(
				context.Background(), fixture.request, fixture.authorization,
			); !errors.Is(err, errSimulatedCrash) {
				t.Fatalf("fault result = %v", err)
			}
			fixture.restartManager(t)
			preparation := fixture.prepare(t)
			if preparation.Authorization == nil {
				t.Fatalf("recovery preparation = %#v", preparation)
			}
			result, err := fixture.manager.ApplyAuthorizedResult(
				context.Background(), fixture.request, fixture.authorization,
			)
			if err != nil || result.Outcome != localbridge.ApplyAgentChangesApplied {
				t.Fatalf("recovered apply = %#v, %v", result, err)
			}
			fixture.assertDesired(t)
		})
	}
}

func TestApplyAgentChangesRetriesTransientInspectionAfterMutation(t *testing.T) {
	fixture := newRootApplyFixture(t, nil, func(t *testing.T, git, root string) {
		writeApplyFile(t, filepath.Join(root, "worker.txt"), "worker\n")
		gitApplyRun(t, git, root, "add", "worker.txt")
	}, applyTestPeerDeviceID)
	fixture.manager.fault = func(point string) error {
		if point == faultAfterMutation {
			return errSimulatedCrash
		}
		return nil
	}
	fixture.prepare(t)
	if _, err := fixture.manager.ApplyAuthorizedResult(
		context.Background(), fixture.request, fixture.authorization,
	); !errors.Is(err, errSimulatedCrash) {
		t.Fatalf("fault result = %v", err)
	}
	fixture.restartManager(t)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := fixture.manager.ApplyAuthorizedResult(
		canceled, fixture.request, fixture.authorization,
	); !errors.Is(err, context.Canceled) {
		t.Fatalf("transient inspection error = %v", err)
	}
	preparation := fixture.prepare(t)
	if preparation.Authorization == nil {
		t.Fatalf("retry preparation = %#v", preparation)
	}
	result, err := fixture.manager.ApplyAuthorizedResult(
		context.Background(), fixture.request, fixture.authorization,
	)
	if err != nil || result.Outcome != localbridge.ApplyAgentChangesApplied {
		t.Fatalf("retry after transient inspection = %#v, %v", result, err)
	}
	fixture.assertDesired(t)
}

func TestApplyAgentChangesPreservesAmbiguousRecoveryUntilRootResolves(t *testing.T) {
	fixture := newRootApplyFixture(t, nil, func(t *testing.T, _, root string) {
		writeApplyFile(t, filepath.Join(root, "worker.txt"), "worker\n")
	}, applyTestPeerDeviceID)
	fixture.manager.fault = func(point string) error {
		if point == faultBeforeMutation {
			return errSimulatedCrash
		}
		return nil
	}
	fixture.prepare(t)
	if _, err := fixture.manager.ApplyAuthorizedResult(
		context.Background(), fixture.request, fixture.authorization,
	); !errors.Is(err, errSimulatedCrash) {
		t.Fatalf("fault result = %v", err)
	}
	fixture.restartManager(t)
	writeApplyFile(t, filepath.Join(fixture.rootPath, "external-after-crash.txt"), "external\n")
	before := gitApplyOutput(t, fixture.runner.Binary, fixture.rootPath, "status", "--porcelain=v2", "-z", "--untracked-files=all")
	fixture.prepare(t)
	result, err := fixture.manager.ApplyAuthorizedResult(
		context.Background(), fixture.request, fixture.authorization,
	)
	if err != nil || result.Outcome != localbridge.ApplyAgentChangesNeedsResolution ||
		result.FailureCode != "root_workspace_recovery_required" {
		t.Fatalf("ambiguous recovery result = %#v, %v", result, err)
	}
	after := gitApplyOutput(t, fixture.runner.Binary, fixture.rootPath, "status", "--porcelain=v2", "-z", "--untracked-files=all")
	if before != after {
		t.Fatal("ambiguous recovery mutated the root workspace")
	}
	recoveryArtifact := filepath.Join(
		fixture.journalRoot, journalDirectoryName, applyTestApplyID, desiredOverlayName,
	)
	if _, err := os.Lstat(recoveryArtifact); err != nil {
		t.Fatalf("recovery artifact was not retained: %v", err)
	}
	fixture.manager.now = func() time.Time {
		return time.Now().Add(defaultMaximumActiveAge + time.Hour)
	}
	if _, err := fixture.manager.maintainJournals(false); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(recoveryArtifact); err != nil {
		t.Fatalf("expired recovery artifact was discarded: %v", err)
	}
	replayed, err := fixture.manager.PrepareResultApply(context.Background(), fixture.request)
	if err != nil || replayed.Authorization == nil || replayed.Completed != nil {
		t.Fatalf("durable recovery-required replay = %#v, %v", replayed, err)
	}
	if err := os.Remove(filepath.Join(fixture.rootPath, "external-after-crash.txt")); err != nil {
		t.Fatal(err)
	}
	recovered, err := fixture.manager.ApplyAuthorizedResult(
		context.Background(), fixture.request, fixture.authorization,
	)
	if err != nil || recovered.Outcome != localbridge.ApplyAgentChangesApplied {
		t.Fatalf("root-resolved recovery = %#v, %v", recovered, err)
	}
	fixture.assertDesired(t)
}

func TestApplyAgentChangesRejectsSymlinkedJournalArtifact(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink creation requires privileges on some Windows hosts")
	}
	fixture := newRootApplyFixture(t, nil, func(t *testing.T, _, root string) {
		writeApplyFile(t, filepath.Join(root, "worker.txt"), "worker\n")
	}, applyTestPeerDeviceID)
	fixture.manager.fault = func(point string) error {
		if point == faultBeforeMutation {
			return errSimulatedCrash
		}
		return nil
	}
	fixture.prepare(t)
	if _, err := fixture.manager.ApplyAuthorizedResult(
		context.Background(), fixture.request, fixture.authorization,
	); !errors.Is(err, errSimulatedCrash) {
		t.Fatalf("fault result = %v", err)
	}
	fixture.restartManager(t)
	artifact := filepath.Join(
		fixture.journalRoot, journalDirectoryName, applyTestApplyID, desiredOverlayName,
	)
	if err := os.Remove(artifact); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(fixture.rootPath, "ignored.cache"), artifact); err != nil {
		t.Fatal(err)
	}
	fixture.prepare(t)
	result, err := fixture.manager.ApplyAuthorizedResult(
		context.Background(), fixture.request, fixture.authorization,
	)
	if err != nil || result.Outcome != localbridge.ApplyAgentChangesNeedsResolution ||
		result.FailureCode != "root_workspace_recovery_required" {
		t.Fatalf("symlinked journal artifact result = %#v, %v", result, err)
	}
	current, err := fixture.runner.InspectApplySource(context.Background(), fixture.sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if !sameWorkspaceManifest(current.Manifest, fixture.base) {
		t.Fatal("symlinked journal artifact mutated the root workspace")
	}
}

func TestApplyAgentChangesPreflightRejectsUnsafeGitStateWithoutMutation(t *testing.T) {
	tests := []struct {
		name   string
		unsafe func(*testing.T, string, string)
	}{
		{
			name: "clean filter",
			unsafe: func(t *testing.T, git, root string) {
				gitApplyRun(t, git, root, "config", "filter.unsafe.clean", "cat")
			},
		},
		{
			name: "merge state",
			unsafe: func(t *testing.T, git, root string) {
				writeApplyFile(t, filepath.Join(root, ".git", "MERGE_HEAD"), gitApplyOutput(t, git, root, "rev-parse", "HEAD")+"\n")
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newRootApplyFixture(t, nil, func(t *testing.T, _, root string) {
				writeApplyFile(t, filepath.Join(root, "worker.txt"), "worker\n")
			}, applyTestPeerDeviceID)
			test.unsafe(t, fixture.runner.Binary, fixture.rootPath)
			before := gitApplyOutput(t, fixture.runner.Binary, fixture.rootPath, "status", "--porcelain=v2", "-z", "--untracked-files=all")
			preparation, err := fixture.manager.PrepareResultApply(context.Background(), fixture.request)
			if err != nil || preparation.Completed == nil ||
				preparation.Completed.Outcome != localbridge.ApplyAgentChangesNeedsResolution ||
				preparation.Completed.FailureCode != "root_workspace_conflict" {
				t.Fatalf("unsafe preparation = %#v, %v", preparation, err)
			}
			after := gitApplyOutput(t, fixture.runner.Binary, fixture.rootPath, "status", "--porcelain=v2", "-z", "--untracked-files=all")
			if before != after {
				t.Fatal("unsafe preflight mutated the root workspace")
			}
		})
	}
}

func TestApplyAgentChangesAllowsConfiguredHooksWithoutInvokingThem(t *testing.T) {
	fixture := newRootApplyFixture(t, nil, func(t *testing.T, _, root string) {
		writeApplyFile(t, filepath.Join(root, "worker.txt"), "worker\n")
	}, applyTestPeerDeviceID)
	hooks := privateApplyDirectory(t, filepath.Join(t.TempDir(), "hooks"))
	hook := filepath.Join(hooks, "pre-commit")
	writeApplyFile(t, hook, "#!/bin/sh\nexit 99\n")
	if err := os.Chmod(hook, 0o700); err != nil {
		t.Fatal(err)
	}
	gitApplyRun(t, fixture.runner.Binary, fixture.rootPath, "config", "core.hooksPath", hooks)
	result := fixture.apply(t)
	if result.Outcome != localbridge.ApplyAgentChangesApplied {
		t.Fatalf("apply with configured hooks = %#v", result)
	}
	fixture.assertDesired(t)
}

func TestApplyAgentChangesCompletesUnmanagedPackageWithoutBrokerAuthorization(t *testing.T) {
	runner := rootApplyTestRunner(t)
	manifest := protocol.ResultManifest{
		Version: protocol.ResultManifestVersion, PackageID: applyTestPackageID,
		ControllerID: applyTestControllerID, TreeID: applyTestTreeID,
		SourceAgentID: applyTestWorkerID, SourceDeviceID: applyTestPeerDeviceID,
		ManagedThreadID: applyTestThreadID, TurnID: applyTestTurnID, LifecycleRevision: 1,
		Terminal: protocol.ResultTerminal{Outcome: protocol.ResultTerminalCompleted}, CapturedAt: 1,
		Rollout: protocol.ResultRolloutComponent{
			Status: protocol.ResultRolloutCaptureFailed, FailureCode: "rollout_unavailable",
		},
		Workspace: protocol.ResultWorkspaceComponent{
			Status: protocol.ResultWorkspaceNotManaged, BaseWarnings: []string{}, ResultWarnings: []string{},
		},
		Parts: []protocol.ResultPackagePartDescriptor{},
	}
	packages := &applyPackageSource{manifest: manifest}
	workspaceRoot := privateApplyDirectory(t, filepath.Join(t.TempDir(), "managed"))
	manager, err := New(Options{WorkspaceRoot: workspaceRoot, Runner: runner, Packages: packages})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	request := localbridge.ResultApplyRequest{
		Root: control.NewRootPrincipal(
			applyTestControllerID, applyTestTreeID, applyTestRootAgentID, applyTestRootDeviceID,
		).Identity(),
		Params: localbridge.ApplyAgentChangesParams{
			ApplyID: applyTestApplyID, PackageID: applyTestPackageID, SourcePath: t.TempDir(),
		},
	}
	preparation, err := manager.PrepareResultApply(context.Background(), request)
	if err != nil || preparation.Authorization != nil || preparation.Completed == nil ||
		preparation.Completed.Outcome != localbridge.ApplyAgentChangesUnchanged {
		t.Fatalf("unmanaged preparation = %#v, %v", preparation, err)
	}
	replay, err := manager.PrepareResultApply(context.Background(), request)
	if err != nil || !reflect.DeepEqual(replay, preparation) {
		t.Fatalf("unmanaged replay = %#v, %v", replay, err)
	}
}

func newRootApplyFixture(
	t *testing.T,
	rootMutation func(*testing.T, string, string),
	workerMutation func(*testing.T, string, string),
	targetDeviceID string,
) *rootApplyFixture {
	return newRootApplyFixtureMode(t, rootMutation, workerMutation, targetDeviceID, false)
}

func newRootApplyFixtureMode(
	t *testing.T,
	rootMutation func(*testing.T, string, string),
	workerMutation func(*testing.T, string, string),
	targetDeviceID string,
	linkedWorktree bool,
) *rootApplyFixture {
	t.Helper()
	runner := rootApplyTestRunner(t)
	rootPath := privateApplyDirectory(t, filepath.Join(t.TempDir(), "root"))
	gitApplyRun(t, runner.Binary, rootPath, "init", "--template=", "--object-format=sha1")
	gitApplyRun(t, runner.Binary, rootPath, "config", "--local", "core.autocrlf", "false")
	gitApplyRun(t, runner.Binary, rootPath, "config", "--local", "core.attributesFile", "")
	gitApplyRun(t, runner.Binary, rootPath, "config", "--local", "core.excludesFile", "")
	writeApplyFile(t, filepath.Join(rootPath, ".gitignore"), "*.cache\n")
	writeApplyFile(t, filepath.Join(rootPath, "nested", "hello.txt"), "hello\n")
	gitApplyRun(t, runner.Binary, rootPath, "add", ".gitignore", "nested/hello.txt")
	gitApplyCommit(t, runner.Binary, rootPath, "initial")
	gitApplyRun(t, runner.Binary, rootPath, "remote", "add", "origin", applyTestGitURL)
	gitApplyRun(t, runner.Binary, rootPath, "config", "delegation.test", "preserve")
	if linkedWorktree {
		linkedPath := filepath.Join(t.TempDir(), "linked")
		gitApplyRun(t, runner.Binary, rootPath, "worktree", "add", "-b", "delegation-apply-test", linkedPath)
		rootPath = linkedPath
	}
	writeApplyFile(t, filepath.Join(rootPath, "ignored.cache"), "preserve ignored\n")
	if rootMutation != nil {
		rootMutation(t, runner.Binary, rootPath)
	}
	sourcePath := filepath.Join(rootPath, "nested")
	baseRepository, err := runner.InspectApplySource(context.Background(), sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	base := baseRepository.Manifest
	transport := t.TempDir()
	baseBundle := filepath.Join(transport, "base.bundle")
	if _, err := runner.CreateBundle(context.Background(), rootPath, baseBundle, base, nil); err != nil {
		t.Fatal(err)
	}
	workerPath := filepath.Join(t.TempDir(), "worker")
	if err := runner.ApplyBundle(context.Background(), workerPath, baseBundle, base); err != nil {
		t.Fatal(err)
	}
	if !base.Clean {
		baseOverlay := filepath.Join(transport, "base-overlay.tar.zst")
		if err := runner.CreateOverlay(context.Background(), rootPath, baseOverlay, base); err != nil {
			t.Fatal(err)
		}
		if err := runner.ApplyOverlay(context.Background(), workerPath, baseOverlay, base); err != nil {
			t.Fatal(err)
		}
	}
	workerMutation(t, runner.Binary, workerPath)
	artifactDirectory := filepath.Join(t.TempDir(), "result")
	capture, err := runner.CaptureResult(context.Background(), workerPath, artifactDirectory, base)
	if err != nil {
		t.Fatal(err)
	}
	if capture.Unchanged {
		t.Fatal("apply fixture worker did not change its workspace")
	}
	manifestHash, err := gitworkspace.ManifestHash(base)
	if err != nil {
		t.Fatal(err)
	}
	parts := make([]protocol.ResultPackagePartDescriptor, 0, 2)
	partPaths := make(map[protocol.ResultPackagePartKind]string)
	if capture.Bundle != nil {
		parts = append(parts, protocol.ResultPackagePartDescriptor{
			Kind: protocol.ResultPackagePartChangesBundle,
			Size: capture.Bundle.Size, SHA256: capture.Bundle.SHA256,
		})
		partPaths[protocol.ResultPackagePartChangesBundle] = capture.Bundle.Path
	}
	if capture.Overlay != nil {
		parts = append(parts, protocol.ResultPackagePartDescriptor{
			Kind: protocol.ResultPackagePartChangesOverlay,
			Size: capture.Overlay.Size, SHA256: capture.Overlay.SHA256,
		})
		partPaths[protocol.ResultPackagePartChangesOverlay] = capture.Overlay.Path
	}
	manifest := protocol.ResultManifest{
		Version: protocol.ResultManifestVersion, PackageID: applyTestPackageID,
		ControllerID: applyTestControllerID, TreeID: applyTestTreeID,
		SourceAgentID: applyTestWorkerID, SourceDeviceID: targetDeviceID,
		ManagedThreadID: applyTestThreadID, TurnID: applyTestTurnID, LifecycleRevision: 1,
		Terminal: protocol.ResultTerminal{Outcome: protocol.ResultTerminalCompleted}, CapturedAt: 1,
		Rollout: protocol.ResultRolloutComponent{
			Status: protocol.ResultRolloutCaptureFailed, FailureCode: "rollout_unavailable",
		},
		Workspace: protocol.ResultWorkspaceComponent{
			Status: protocol.ResultWorkspaceChanged, WorkspaceID: applyTestWorkspaceID,
			SourceDeviceID: applyTestRootDeviceID, TargetDeviceID: targetDeviceID,
			ObjectFormat: base.ObjectFormat, BaseHeadOID: base.HeadOID,
			BaseManifestHash: manifestHash, BaseSnapshotHash: base.SourceSnapshotHash,
			BaseClean: base.Clean, ResultHeadOID: capture.ResultHeadOID,
			ResultSnapshotHash: capture.ResultSnapshotHash, ResultClean: capture.ResultClean,
			BaseWarnings:   append([]string{}, base.Warnings...),
			ResultWarnings: append([]string{}, capture.ResultWarnings...),
		},
		Parts: parts,
	}
	_, manifestDescriptor, err := protocol.EncodeResultManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := runner.FlattenStagingResult(context.Background(), workerPath, base)
	if err != nil {
		t.Fatal(err)
	}
	packages := &applyPackageSource{manifest: manifest, parts: partPaths}
	journalRoot := privateApplyDirectory(t, filepath.Join(t.TempDir(), "managed"))
	manager, err := New(Options{WorkspaceRoot: journalRoot, Runner: runner, Packages: packages})
	if err != nil {
		t.Fatal(err)
	}
	fixture := &rootApplyFixture{
		runner: runner, manager: manager, journalRoot: journalRoot, packages: packages,
		rootPath: rootPath, sourcePath: sourcePath, workerPath: workerPath,
		base: base, expected: expected.Manifest,
		request: localbridge.ResultApplyRequest{
			Root: control.NewRootPrincipal(
				applyTestControllerID, applyTestTreeID, applyTestRootAgentID, applyTestRootDeviceID,
			).Identity(),
			Params: localbridge.ApplyAgentChangesParams{
				ApplyID: applyTestApplyID, PackageID: applyTestPackageID, SourcePath: sourcePath,
			},
		},
		authorization: protocol.AuthorizeResultApplyResult{
			ApplyID: applyTestApplyID, PackageID: applyTestPackageID,
			ManifestSHA256: manifestDescriptor.SHA256, WorkspaceID: applyTestWorkspaceID,
			BaseManifestHash: manifestHash,
		},
		initialHead: gitApplyOutput(t, runner.Binary, rootPath, "rev-parse", "HEAD"),
		initialRef:  gitApplyOutput(t, runner.Binary, rootPath, "symbolic-ref", "HEAD"),
	}
	fixture.initialRefOID = gitApplyOutput(t, runner.Binary, rootPath, "rev-parse", fixture.initialRef)
	t.Cleanup(func() {
		if fixture.manager != nil {
			_ = fixture.manager.Close()
		}
	})
	return fixture
}

func (f *rootApplyFixture) prepare(t *testing.T) localbridge.ResultApplyPreparation {
	t.Helper()
	preparation, err := f.manager.PrepareResultApply(context.Background(), f.request)
	if err != nil {
		t.Fatal(err)
	}
	if err := preparation.Validate(f.request); err != nil {
		t.Fatal(err)
	}
	return preparation
}

func (f *rootApplyFixture) apply(t *testing.T) localbridge.ApplyAgentChangesResult {
	t.Helper()
	preparation := f.prepare(t)
	if preparation.Authorization == nil || preparation.Authorization.ApplyID != f.request.Params.ApplyID ||
		preparation.Authorization.PackageID != f.request.Params.PackageID ||
		preparation.Authorization.GitURL != applyTestGitURL ||
		preparation.Authorization.SourcePathSHA256 != hashPath(f.sourcePath) {
		t.Fatalf("apply preparation = %#v", preparation)
	}
	result, err := f.manager.ApplyAuthorizedResult(context.Background(), f.request, f.authorization)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func (f *rootApplyFixture) assertDesired(t *testing.T) {
	t.Helper()
	actual, err := f.runner.InspectApplySource(context.Background(), f.sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if !sameWorkspaceManifest(actual.Manifest, f.expected) {
		t.Fatalf("applied manifest = %#v, want %#v", actual.Manifest, f.expected)
	}
	rootStatus := gitApplyOutput(t, f.runner.Binary, f.rootPath, "status", "--porcelain=v2", "-z", "--untracked-files=all")
	workerStatus := gitApplyOutput(t, f.runner.Binary, f.workerPath, "status", "--porcelain=v2", "-z", "--untracked-files=all")
	if rootStatus != workerStatus {
		t.Fatalf("applied status = %q, want %q", rootStatus, workerStatus)
	}
	if head := gitApplyOutput(t, f.runner.Binary, f.rootPath, "rev-parse", "HEAD"); head != f.initialHead {
		t.Fatalf("root HEAD moved to %s, want %s", head, f.initialHead)
	}
	if ref := gitApplyOutput(t, f.runner.Binary, f.rootPath, "symbolic-ref", "HEAD"); ref != f.initialRef {
		t.Fatalf("root ref changed to %s, want %s", ref, f.initialRef)
	}
	if refOID := gitApplyOutput(t, f.runner.Binary, f.rootPath, "rev-parse", f.initialRef); refOID != f.initialRefOID {
		t.Fatalf("root branch moved to %s, want %s", refOID, f.initialRefOID)
	}
	if value := gitApplyOutput(t, f.runner.Binary, f.rootPath, "config", "--local", "delegation.test"); value != "preserve" {
		t.Fatalf("root local config = %q", value)
	}
	ignored, err := os.ReadFile(filepath.Join(f.rootPath, "ignored.cache"))
	if err != nil || string(ignored) != "preserve ignored\n" {
		t.Fatalf("root ignored file = %q, %v", ignored, err)
	}
}

func (f *rootApplyFixture) restartManager(t *testing.T) {
	t.Helper()
	if err := f.manager.Close(); err != nil {
		t.Fatal(err)
	}
	f.manager = nil
	manager, err := New(Options{
		WorkspaceRoot: f.journalRoot, Runner: f.runner, Packages: f.packages,
	})
	if err != nil {
		t.Fatal(err)
	}
	f.manager = manager
}

func retentionTestID(sequence int) string {
	return fmt.Sprintf("123e4567-e89b-42d3-a456-%012x", 0x500000+sequence)
}

func retentionTestRequest(sequence int) localbridge.ResultApplyRequest {
	return localbridge.ResultApplyRequest{
		Root: control.NewRootPrincipal(
			applyTestControllerID, applyTestTreeID, applyTestRootAgentID, applyTestRootDeviceID,
		).Identity(),
		Params: localbridge.ApplyAgentChangesParams{
			ApplyID: retentionTestID(sequence), PackageID: retentionTestID(100 + sequence),
			SourcePath: filepath.Join(os.TempDir(), "delegation-retention-root"),
		},
	}
}

func assertCompactJournal(t *testing.T, path string) {
	t.Helper()
	entries, err := os.ReadDir(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != journalFileName || entries[0].IsDir() {
		t.Fatalf("terminal journal payloads were retained: %#v", entries)
	}
}

func journalDirectoryNames(t *testing.T, path string) []string {
	t.Helper()
	entries, err := os.ReadDir(path)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			t.Fatalf("journal authority contains non-directory %q", entry.Name())
		}
		names = append(names, entry.Name())
	}
	return names
}

func rootApplyTestRunner(t *testing.T) gitworkspace.Runner {
	t.Helper()
	binary, err := exec.LookPath("git")
	if err != nil {
		t.Skip("Git is unavailable")
	}
	binary, err = filepath.Abs(binary)
	if err != nil {
		t.Fatal(err)
	}
	binary, err = filepath.EvalSymlinks(binary)
	if err != nil {
		t.Fatal(err)
	}
	runner, err := gitworkspace.NewRunner(binary)
	if err != nil {
		t.Fatal(err)
	}
	return runner
}

func privateApplyDirectory(t *testing.T, path string) string {
	t.Helper()
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}

func writeApplyFile(t *testing.T, path, content string) {
	t.Helper()
	writeApplyBytes(t, path, []byte(content))
}

func writeApplyBytes(t *testing.T, path string, content []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}
}

func gitApplyRun(t *testing.T, binary, root string, args ...string) {
	t.Helper()
	command := exec.Command(binary, args...)
	command.Dir = root
	command.Env = gitApplyEnvironment()
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, output)
	}
}

func gitApplyOutput(t *testing.T, binary, root string, args ...string) string {
	t.Helper()
	command := exec.Command(binary, args...)
	command.Dir = root
	command.Env = gitApplyEnvironment()
	output, err := command.Output()
	if err != nil {
		t.Fatalf("git %v: %v", args, err)
	}
	return strings.TrimSpace(string(output))
}

func gitApplyObjectExists(t *testing.T, binary, root, objectID string) bool {
	t.Helper()
	expression := objectID + "^{blob}"
	command := exec.Command(binary, "cat-file", "--batch-check=%(objectname) %(objecttype)")
	command.Dir = root
	command.Env = gitApplyEnvironment()
	command.Stdin = strings.NewReader(expression + "\n")
	output, err := command.Output()
	if err != nil {
		t.Fatalf("inspect Git object %s: %v", objectID, err)
	}
	switch strings.TrimSpace(string(output)) {
	case objectID + " blob":
		return true
	case expression + " missing":
		return false
	default:
		t.Fatalf("unexpected Git object inspection for %s: %q", objectID, output)
		return false
	}
}

func gitApplyEnvironment() []string {
	environment := make([]string, 0, len(os.Environ())+4)
	for _, variable := range os.Environ() {
		name, _, _ := strings.Cut(variable, "=")
		if strings.HasPrefix(strings.ToUpper(name), "GIT_") {
			continue
		}
		environment = append(environment, variable)
	}
	return append(
		environment,
		"GIT_TERMINAL_PROMPT=0",
		"GIT_CONFIG_GLOBAL="+os.DevNull,
		"GIT_CONFIG_NOSYSTEM=1",
		"GIT_ATTR_NOSYSTEM=1",
	)
}

func gitApplyCommit(t *testing.T, binary, root, message string) {
	t.Helper()
	gitApplyRun(
		t, binary, root, "-c", "user.name=Delegation Test", "-c", "user.email=test@example.invalid",
		"commit", "-m", message,
	)
}

func gitApplyCommitOnly(t *testing.T, binary, root, message, path string) {
	t.Helper()
	gitApplyRun(
		t, binary, root, "-c", "user.name=Delegation Test", "-c", "user.email=test@example.invalid",
		"commit", "--only", "-m", message, "--", path,
	)
}
