//go:build integration && linux

package codex_peer_e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/identity"
	"github.com/GhostFlying/delegation/internal/localbridge"
	"github.com/GhostFlying/delegation/internal/protocol"
)

const (
	workspaceDirectSyncID  = "123e4567-e89b-42d3-a456-426614174930"
	workspaceDirectSpawnID = "123e4567-e89b-42d3-a456-426614174931"
	workspaceThinSyncID    = "123e4567-e89b-42d3-a456-426614174932"
	workspaceThinSpawnID   = "123e4567-e89b-42d3-a456-426614174933"
	workspaceFullSyncID    = "123e4567-e89b-42d3-a456-426614174934"
	workspaceFullSpawnID   = "123e4567-e89b-42d3-a456-426614174935"

	rootWorkspaceDirectSync  = "root-workspace-direct-sync"
	rootWorkspaceDirectSpawn = "root-workspace-direct-spawn"
	rootWorkspaceThinSync    = "root-workspace-thin-sync"
	rootWorkspaceThinSpawn   = "root-workspace-thin-spawn"
	rootWorkspaceFullSync    = "root-workspace-full-sync"
	rootWorkspaceFullSpawn   = "root-workspace-full-spawn"

	workerWorkspaceDirect = "worker-workspace-direct"
	workerWorkspaceThin   = "worker-workspace-thin"
	workerWorkspaceFull   = "worker-workspace-full"
)

type workspaceE2EScenario struct {
	name               string
	syncID             string
	spawnID            string
	taskName           string
	rootSyncCase       string
	rootSpawnCase      string
	workerCase         string
	gitURL             string
	sourceRoot         string
	nestedCWD          string
	head               string
	sourceMarker       string
	dirtyMarker        string
	workerCommitMarker string
	workerMarker       string
	strategy           protocol.WorkspaceStrategy
	baseWarnings       []string
}

func createTopologyGitRepositories(
	t *testing.T,
	root string,
	peers []peer,
) []workspaceE2EScenario {
	t.Helper()
	gitRoot := filepath.Join(root, "git")
	if err := os.MkdirAll(gitRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewTLSServer(http.FileServer(http.Dir(gitRoot)))
	t.Cleanup(server.Close)
	for _, current := range peers {
		if err := os.WriteFile(
			filepath.Join(current.home, ".gitconfig"),
			[]byte("[http]\n\tsslVerify = false\n"), 0o600,
		); err != nil {
			t.Fatal(err)
		}
	}

	scenarios := []workspaceE2EScenario{
		{
			name: "direct", syncID: workspaceDirectSyncID, spawnID: workspaceDirectSpawnID,
			taskName: "direct_workspace", rootSyncCase: rootWorkspaceDirectSync,
			rootSpawnCase: rootWorkspaceDirectSpawn, workerCase: workerWorkspaceDirect,
			sourceMarker:       "delegation-workspace-direct-source",
			dirtyMarker:        "delegation-workspace-direct-dirty-overlay",
			workerCommitMarker: "delegation-workspace-direct-worker-commit",
			workerMarker:       "delegation-workspace-direct-worker-write",
			strategy:           protocol.WorkspaceStrategyDirect,
		},
		{
			name: "thin", syncID: workspaceThinSyncID, spawnID: workspaceThinSpawnID,
			taskName: "thin_bundle_workspace", rootSyncCase: rootWorkspaceThinSync,
			rootSpawnCase: rootWorkspaceThinSpawn, workerCase: workerWorkspaceThin,
			sourceMarker:       "delegation-workspace-thin-unpublished-head",
			dirtyMarker:        "delegation-workspace-thin-dirty-overlay",
			workerCommitMarker: "delegation-workspace-thin-worker-commit",
			workerMarker:       "delegation-workspace-thin-worker-write",
			strategy:           protocol.WorkspaceStrategyThin,
		},
		{
			name: "full", syncID: workspaceFullSyncID, spawnID: workspaceFullSpawnID,
			taskName: "full_bundle_workspace", rootSyncCase: rootWorkspaceFullSync,
			rootSpawnCase: rootWorkspaceFullSpawn, workerCase: workerWorkspaceFull,
			sourceMarker:       "delegation-workspace-full-unreachable-remote",
			dirtyMarker:        "delegation-workspace-full-dirty-overlay",
			workerCommitMarker: "delegation-workspace-full-worker-commit",
			workerMarker:       "delegation-workspace-full-worker-write",
			strategy:           protocol.WorkspaceStrategyFull,
			baseWarnings:       []string{protocol.WorkspaceWarningFullHistoryFallback},
		},
	}
	for index := range scenarios {
		scenario := &scenarios[index]
		scenario.sourceRoot = filepath.Join(gitRoot, scenario.name+"-source")
		scenario.nestedCWD = filepath.Join(scenario.sourceRoot, "nested")
		if err := os.MkdirAll(scenario.nestedCWD, 0o700); err != nil {
			t.Fatal(err)
		}
		run(t, os.Environ(), "git", "init", scenario.sourceRoot)

		if scenario.strategy == protocol.WorkspaceStrategyThin {
			writeWorkspaceSourceMarker(t, scenario.nestedCWD, "delegation-workspace-thin-published-base")
		} else {
			writeWorkspaceSourceMarker(t, scenario.nestedCWD, scenario.sourceMarker)
		}
		commitWorkspaceSource(t, scenario.sourceRoot, "initial")

		if scenario.strategy != protocol.WorkspaceStrategyFull {
			remote := filepath.Join(gitRoot, scenario.name+"-remote.git")
			run(t, os.Environ(), "git", "init", "--bare", remote)
			run(t, os.Environ(), "git", "-C", scenario.sourceRoot, "remote", "add", "origin", remote)
			run(t, os.Environ(), "git", "-C", scenario.sourceRoot, "push", "origin", "HEAD:refs/heads/main")
			run(t, os.Environ(), "git", "--git-dir="+remote, "update-server-info")
			scenario.gitURL = server.URL + "/" + filepath.Base(remote)
			run(t, os.Environ(), "git", "-C", scenario.sourceRoot, "remote", "set-url", "origin", scenario.gitURL)
		} else {
			scenario.gitURL = server.URL + "/unavailable-full-remote.git"
			run(t, os.Environ(), "git", "-C", scenario.sourceRoot, "remote", "add", "origin", scenario.gitURL)
		}

		if scenario.strategy == protocol.WorkspaceStrategyThin {
			writeWorkspaceSourceMarker(t, scenario.nestedCWD, scenario.sourceMarker)
			commitWorkspaceSource(t, scenario.sourceRoot, "unpublished exact head")
		}
		head, _ := run(t, os.Environ(), "git", "-C", scenario.sourceRoot, "rev-parse", "HEAD^{commit}")
		scenario.head = strings.TrimSpace(head)
		if err := os.WriteFile(
			filepath.Join(scenario.nestedCWD, "dirty-source.txt"),
			[]byte(scenario.dirtyMarker+"\n"), 0o600,
		); err != nil {
			t.Fatal(err)
		}
	}
	return scenarios
}

func writeWorkspaceSourceMarker(t *testing.T, nestedCWD, marker string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(nestedCWD, "source.txt"), []byte(marker+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
}

func commitWorkspaceSource(t *testing.T, sourceRoot, message string) {
	t.Helper()
	run(t, os.Environ(), "git", "-C", sourceRoot, "add", "nested/source.txt")
	run(t, os.Environ(), "git", "-C", sourceRoot,
		"-c", "user.name=Delegation Test", "-c", "user.email=test@example.invalid",
		"commit", "-m", message)
}

func testWorkspaceDelegation(
	t *testing.T,
	source peer,
	target peer,
	codexBinary, delegationBinary, brokerStatePath string,
	scenario workspaceE2EScenario,
) {
	t.Helper()
	sourceStatus, _ := run(
		t, os.Environ(), "git", "-C", scenario.sourceRoot,
		"status", "--porcelain=v1", "--untracked-files=all",
	)
	sourceHead, _ := run(
		t, os.Environ(), "git", "-C", scenario.sourceRoot, "rev-parse", "HEAD^{commit}",
	)
	sourceData, err := os.ReadFile(filepath.Join(scenario.nestedCWD, "source.txt"))
	if err != nil {
		t.Fatal(err)
	}
	synchronized := runCodexAt(
		t, source, codexBinary, delegationBinary, scenario.nestedCWD,
		scenario.rootSyncCase, "",
	)
	spawned := runCodexAt(
		t, source, codexBinary, delegationBinary, scenario.nestedCWD,
		scenario.rootSpawnCase, synchronized.threadID,
	)
	if spawned.threadID != synchronized.threadID {
		t.Fatalf("workspace spawn resumed %q, want %q", spawned.threadID, synchronized.threadID)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	root := prepareManagedDispatchRoot(t, source, synchronized.threadID)
	agent := findManagedAgent(t, ctx, root, scenario.taskName)
	if agent.WorkspaceID != scenario.syncID || agent.Principal.DeviceID != deviceIDs[target.label] {
		t.Fatalf("workspace agent = %#v", agent)
	}
	targetConfig, err := delegationconfig.Read(target.configPath)
	if err != nil {
		t.Fatal(err)
	}
	waitForManagedWorkerIdle(
		t, targetConfig.Peer.StateFile,
		agent.Principal.TreeID, agent.Principal.AgentID, agent.Principal.ParentAgentID,
		deviceIDs[target.label], scenario.taskName,
	)

	database := openDatabase(t, targetConfig.Peer.StateFile)
	defer database.Close()
	var workspacePath, workspaceStatus, claimedAgentID, strategy string
	var sourceWarningsJSON, warningsJSON string
	var sourceClean bool
	var sourceSnapshotHash, manifestHash string
	if err := database.QueryRow(`
	SELECT workspace_path, status, claimed_agent_id, strategy,
	       source_warnings_json, warnings_json,
	       source_clean, source_snapshot_hash, manifest_hash
	FROM prepared_workspaces
	WHERE controller_id = ? AND tree_id = ? AND workspace_id = ?
	`, networkID, agent.Principal.TreeID, scenario.syncID).Scan(
		&workspacePath, &workspaceStatus, &claimedAgentID, &strategy,
		&sourceWarningsJSON, &warningsJSON,
		&sourceClean, &sourceSnapshotHash, &manifestHash,
	); err != nil {
		t.Fatal(err)
	}
	if workspaceStatus != "claimed" || claimedAgentID != agent.Principal.AgentID {
		t.Fatalf("prepared workspace status = %q, claimant %q", workspaceStatus, claimedAgentID)
	}
	assertWorkspaceStrategyAndWarnings(
		t, "target prepared workspace", strategy, sourceWarningsJSON, warningsJSON, scenario,
	)
	if sourceClean || len(sourceSnapshotHash) != 64 || len(manifestHash) != 64 {
		t.Fatalf(
			"target source snapshot = clean %t, snapshot %q, manifest %q",
			sourceClean, sourceSnapshotHash, manifestHash,
		)
	}
	if data, err := os.ReadFile(filepath.Join(workspacePath, "nested", "source.txt")); err != nil ||
		strings.TrimSpace(string(data)) != scenario.workerCommitMarker {
		t.Fatalf("target committed marker = %q, %v", data, err)
	}
	if data, err := os.ReadFile(filepath.Join(workspacePath, "nested", "worker-change.txt")); err != nil ||
		strings.TrimSpace(string(data)) != scenario.workerMarker {
		t.Fatalf("target worker marker = %q, %v", data, err)
	}
	if data, err := os.ReadFile(filepath.Join(workspacePath, "nested", "dirty-source.txt")); err != nil ||
		strings.TrimSpace(string(data)) != scenario.dirtyMarker {
		t.Fatalf("target dirty marker = %q, %v", data, err)
	}
	dirtyStatus, _ := run(
		t, os.Environ(), "git", "-C", workspacePath, "status", "--porcelain=v1",
		"--untracked-files=all", "--", "nested/dirty-source.txt", "nested/worker-change.txt",
	)
	if strings.TrimSpace(dirtyStatus) != "?? nested/dirty-source.txt\n?? nested/worker-change.txt" {
		t.Fatalf("target dirty marker status = %q", dirtyStatus)
	}
	checkedOutHead, _ := run(t, os.Environ(), "git", "-C", workspacePath, "rev-parse", "HEAD^{commit}")
	resultHead := strings.TrimSpace(checkedOutHead)
	if resultHead == scenario.head {
		t.Fatalf("target HEAD did not advance from %q", scenario.head)
	}
	run(t, os.Environ(), "git", "-C", workspacePath, "merge-base", "--is-ancestor", scenario.head, resultHead)
	commitCount, _ := run(
		t, os.Environ(), "git", "-C", workspacePath,
		"rev-list", "--count", scenario.head+".."+resultHead,
	)
	if strings.TrimSpace(commitCount) != "1" {
		t.Fatalf("target result commit count = %q", commitCount)
	}
	assertSourceWorkspaceUnchanged(t, scenario, sourceHead, sourceStatus, sourceData)

	resultPackage := waitForWorkspaceResultPackage(
		t, ctx, root, agent, scenario, resultHead, manifestHash, sourceSnapshotHash,
	)

	broker := openDatabase(t, brokerStatePath)
	defer broker.Close()
	var status, consumedSpawnID, brokerSnapshotHash, brokerManifestHash string
	var brokerSourceClean bool
	if err := broker.QueryRow(`
	SELECT status, consumed_spawn_id, strategy, source_warnings_json, warnings_json,
	       source_clean, source_snapshot_hash, manifest_hash
FROM workspace_sync_receipts
WHERE controller_id = ? AND sync_id = ?
	`, networkID, scenario.syncID).Scan(
		&status, &consumedSpawnID, &strategy, &sourceWarningsJSON, &warningsJSON,
		&brokerSourceClean, &brokerSnapshotHash, &brokerManifestHash,
	); err != nil {
		t.Fatal(err)
	}
	if status != "prepared" || consumedSpawnID != scenario.spawnID {
		t.Fatalf("broker workspace receipt = status %q, spawn %q", status, consumedSpawnID)
	}
	assertWorkspaceStrategyAndWarnings(
		t, "broker workspace receipt", strategy, sourceWarningsJSON, warningsJSON, scenario,
	)
	if brokerSourceClean || brokerSnapshotHash != sourceSnapshotHash || brokerManifestHash != manifestHash {
		t.Fatalf(
			"broker source snapshot = clean %t, snapshot %q, manifest %q; target snapshot %q, manifest %q",
			brokerSourceClean, brokerSnapshotHash, brokerManifestHash, sourceSnapshotHash, manifestHash,
		)
	}
	assertBrokerResultPackageContainsNoPeerPayload(
		t, brokerStatePath, workspacePath, scenario,
	)
	applyCtx, cancelApply := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancelApply()
	applyWorkspaceResultPackage(t, applyCtx, root, resultPackage, scenario, sourceHead)
}

func assertWorkspaceStrategyAndWarnings(
	t *testing.T,
	location, strategy, sourceWarningsJSON, warningsJSON string,
	scenario workspaceE2EScenario,
) {
	t.Helper()
	var sourceWarnings, warnings []string
	if err := json.Unmarshal([]byte(sourceWarningsJSON), &sourceWarnings); err != nil {
		t.Fatalf("%s source warnings %q are invalid: %v", location, sourceWarningsJSON, err)
	}
	if err := json.Unmarshal([]byte(warningsJSON), &warnings); err != nil {
		t.Fatalf("%s warnings %q are invalid: %v", location, warningsJSON, err)
	}
	if len(sourceWarnings) != 0 || strategy != string(scenario.strategy) ||
		!slices.Equal(warnings, scenario.baseWarnings) {
		t.Fatalf(
			"%s strategy/source/final warnings = %q/%v/%v, want %q/[]/%v",
			location, strategy, sourceWarnings, warnings, scenario.strategy, scenario.baseWarnings,
		)
	}
}

func assertSourceWorkspaceUnchanged(
	t *testing.T,
	scenario workspaceE2EScenario,
	head, status string,
	sourceData []byte,
) {
	t.Helper()
	currentHead, _ := run(
		t, os.Environ(), "git", "-C", scenario.sourceRoot, "rev-parse", "HEAD^{commit}",
	)
	currentStatus, _ := run(
		t, os.Environ(), "git", "-C", scenario.sourceRoot,
		"status", "--porcelain=v1", "--untracked-files=all",
	)
	currentSource, err := os.ReadFile(filepath.Join(scenario.nestedCWD, "source.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(currentHead) != strings.TrimSpace(head) || currentStatus != status ||
		!bytes.Equal(currentSource, sourceData) {
		t.Fatalf(
			"source workspace changed: head %q/%q, status %q/%q, source %q/%q",
			strings.TrimSpace(currentHead), strings.TrimSpace(head), currentStatus, status,
			currentSource, sourceData,
		)
	}
	if data, err := os.ReadFile(filepath.Join(scenario.nestedCWD, "dirty-source.txt")); err != nil ||
		strings.TrimSpace(string(data)) != scenario.dirtyMarker {
		t.Fatalf("source dirty marker = %q, %v", data, err)
	}
	if _, err := os.Stat(filepath.Join(scenario.nestedCWD, "worker-change.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("worker change was written back to source: %v", err)
	}
}

func waitForWorkspaceResultPackage(
	t *testing.T,
	ctx context.Context,
	root managedDispatchRoot,
	agent protocol.AgentSummary,
	scenario workspaceE2EScenario,
	resultHead, manifestHash, sourceSnapshotHash string,
) protocol.ResultPackageHandle {
	t.Helper()
	rootSource := root.root.Principal.Identity()
	var result protocol.WaitAgentResult
	if err := root.client.Call(
		ctx, protocol.MethodWaitAgent, root.root.Tree.TreeID, &rootSource,
		protocol.WaitAgentParams{
			TimeoutMillis: 10_000,
			MessageLimit:  protocol.MaximumAgentWaitMessages,
			ActivityLimit: protocol.MaximumAgentWaitActivities,
			ArtifactLimit: protocol.MaximumAgentWaitArtifacts,
			ResultLimit:   protocol.MaximumAgentWaitResults,
		},
		&result,
	); err != nil {
		t.Fatalf("wait for workspace result package: %v", err)
	}
	if len(result.Artifacts) != 0 || len(result.Results) != 1 || result.MoreResults ||
		result.NextResultCursor == 0 {
		t.Fatalf("workspace result package wait = %#v", result)
	}
	handle := result.Results[0]
	manifest := handle.Manifest
	if err := handle.Validate(); err != nil {
		t.Fatalf("workspace result package handle: %v", err)
	}
	if handle.Availability != protocol.ResultPackageAvailable ||
		handle.Sequence != result.NextResultCursor ||
		manifest.TreeID != agent.Principal.TreeID ||
		manifest.SourceAgentID != agent.Principal.AgentID ||
		manifest.SourceDeviceID != agent.Principal.DeviceID ||
		manifest.Terminal.Outcome != protocol.ResultTerminalCompleted ||
		manifest.Workspace.Status != protocol.ResultWorkspaceChanged ||
		manifest.Workspace.SourceDeviceID != rootSource.DeviceID ||
		manifest.Workspace.TargetDeviceID != agent.Principal.DeviceID ||
		manifest.Workspace.WorkspaceID != scenario.syncID ||
		manifest.Workspace.ObjectFormat != "sha1" ||
		manifest.Workspace.BaseHeadOID != scenario.head || manifest.Workspace.BaseClean ||
		manifest.Workspace.BaseManifestHash != manifestHash ||
		manifest.Workspace.BaseSnapshotHash != sourceSnapshotHash ||
		manifest.Workspace.ResultHeadOID != resultHead || manifest.Workspace.ResultClean ||
		len(manifest.Workspace.ResultSnapshotHash) != 64 ||
		!slices.Equal(manifest.Workspace.BaseWarnings, scenario.baseWarnings) ||
		len(manifest.Workspace.ResultWarnings) != 0 {
		t.Fatalf("workspace result package = %#v", handle)
	}
	wantKinds := []protocol.ResultPackagePartKind{
		protocol.ResultPackagePartChangesBundle,
		protocol.ResultPackagePartChangesOverlay,
	}
	gotKinds := make([]protocol.ResultPackagePartKind, 0, len(manifest.Parts))
	for _, part := range manifest.Parts {
		if part.Kind == protocol.ResultPackagePartRollout {
			continue
		}
		gotKinds = append(gotKinds, part.Kind)
		if part.Size < 1 || len(part.SHA256) != 64 {
			t.Fatalf("workspace result package part = %#v", part)
		}
	}
	if !slices.Equal(gotKinds, wantKinds) {
		t.Fatalf("workspace result package Git parts = %#v, want %#v", gotKinds, wantKinds)
	}
	return handle
}

func applyWorkspaceResultPackage(
	t *testing.T,
	ctx context.Context,
	root managedDispatchRoot,
	handle protocol.ResultPackageHandle,
	scenario workspaceE2EScenario,
	rootHead string,
) {
	t.Helper()
	rootSource := root.root.Principal.Identity()
	applyID, err := identity.NewID()
	if err != nil {
		t.Fatal(err)
	}
	params := localbridge.ApplyAgentChangesParams{
		ApplyID: applyID, PackageID: handle.Manifest.PackageID,
		SourcePath: scenario.nestedCWD,
	}
	var result localbridge.ApplyAgentChangesResult
	if err := root.client.Call(
		ctx,
		localbridge.MethodApplyAgentChanges,
		root.root.Tree.TreeID,
		&rootSource,
		params,
		&result,
	); err != nil {
		t.Fatalf("apply workspace result package: %v", err)
	}
	if result.ApplyID != params.ApplyID || result.PackageID != params.PackageID ||
		result.Outcome != localbridge.ApplyAgentChangesApplied || result.FailureCode != "" {
		t.Fatalf("workspace result apply = %#v", result)
	}
	currentHead, _ := run(
		t, os.Environ(), "git", "-C", scenario.sourceRoot, "rev-parse", "HEAD^{commit}",
	)
	if strings.TrimSpace(currentHead) != strings.TrimSpace(rootHead) {
		t.Fatalf("root HEAD moved after apply: %q, want %q", currentHead, rootHead)
	}
	for name, want := range map[string]string{
		"source.txt":        scenario.workerCommitMarker,
		"dirty-source.txt":  scenario.dirtyMarker,
		"worker-change.txt": scenario.workerMarker,
	} {
		data, err := os.ReadFile(filepath.Join(scenario.nestedCWD, name))
		if err != nil || strings.TrimSpace(string(data)) != want {
			t.Fatalf("applied root file %s = %q, %v", name, data, err)
		}
	}
	status, _ := run(
		t, os.Environ(), "git", "-C", scenario.sourceRoot,
		"status", "--porcelain=v1", "--untracked-files=all",
	)
	wantStatus := "M  nested/source.txt\n?? nested/dirty-source.txt\n?? nested/worker-change.txt"
	if strings.TrimSpace(strings.ReplaceAll(status, "\r\n", "\n")) != wantStatus {
		t.Fatalf("applied root status = %q, want %q", status, wantStatus)
	}
}

func assertBrokerResultPackageContainsNoPeerPayload(
	t *testing.T,
	statePath, workspacePath string,
	scenario workspaceE2EScenario,
) {
	t.Helper()
	for _, path := range []string{statePath, statePath + "-wal"} {
		data, err := os.ReadFile(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		for _, forbidden := range []string{workspacePath, scenario.workerCommitMarker, scenario.workerMarker} {
			if bytes.Contains(data, []byte(forbidden)) {
				t.Fatalf("broker state %s contains peer-local artifact data %q", path, forbidden)
			}
		}
	}
}
