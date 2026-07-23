//go:build integration && linux

package codex_peer_e2e

import (
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
	name          string
	syncID        string
	spawnID       string
	taskName      string
	rootSyncCase  string
	rootSpawnCase string
	workerCase    string
	gitURL        string
	sourceRoot    string
	nestedCWD     string
	head          string
	sourceMarker  string
	workerMarker  string
	strategy      protocol.WorkspaceStrategy
	warnings      []string
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
			sourceMarker: "delegation-workspace-direct-source",
			workerMarker: "delegation-workspace-direct-worker-write",
			strategy:     protocol.WorkspaceStrategyDirect,
		},
		{
			name: "thin", syncID: workspaceThinSyncID, spawnID: workspaceThinSpawnID,
			taskName: "thin_bundle_workspace", rootSyncCase: rootWorkspaceThinSync,
			rootSpawnCase: rootWorkspaceThinSpawn, workerCase: workerWorkspaceThin,
			sourceMarker: "delegation-workspace-thin-unpublished-head",
			workerMarker: "delegation-workspace-thin-worker-write",
			strategy:     protocol.WorkspaceStrategyThin,
		},
		{
			name: "full", syncID: workspaceFullSyncID, spawnID: workspaceFullSpawnID,
			taskName: "full_bundle_workspace", rootSyncCase: rootWorkspaceFullSync,
			rootSpawnCase: rootWorkspaceFullSpawn, workerCase: workerWorkspaceFull,
			sourceMarker: "delegation-workspace-full-unreachable-remote",
			workerMarker: "delegation-workspace-full-worker-write",
			strategy:     protocol.WorkspaceStrategyFull,
			warnings:     []string{protocol.WorkspaceWarningFullHistoryFallback},
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
		} else {
			scenario.gitURL = server.URL + "/unavailable-full-remote.git"
		}

		if scenario.strategy == protocol.WorkspaceStrategyThin {
			writeWorkspaceSourceMarker(t, scenario.nestedCWD, scenario.sourceMarker)
			commitWorkspaceSource(t, scenario.sourceRoot, "unpublished exact head")
		}
		head, _ := run(t, os.Environ(), "git", "-C", scenario.sourceRoot, "rev-parse", "HEAD^{commit}")
		scenario.head = strings.TrimSpace(head)
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
	var workspacePath, workspaceStatus, claimedAgentID, strategy, warningsJSON string
	if err := database.QueryRow(`
SELECT workspace_path, status, claimed_agent_id, strategy, warnings_json
FROM prepared_workspaces
WHERE controller_id = ? AND tree_id = ? AND workspace_id = ?
`, networkID, agent.Principal.TreeID, scenario.syncID).Scan(
		&workspacePath, &workspaceStatus, &claimedAgentID, &strategy, &warningsJSON,
	); err != nil {
		t.Fatal(err)
	}
	if workspaceStatus != "claimed" || claimedAgentID != agent.Principal.AgentID {
		t.Fatalf("prepared workspace status = %q, claimant %q", workspaceStatus, claimedAgentID)
	}
	assertWorkspaceStrategyAndWarnings(t, "target prepared workspace", strategy, warningsJSON, scenario)
	if data, err := os.ReadFile(filepath.Join(workspacePath, "nested", "source.txt")); err != nil ||
		strings.TrimSpace(string(data)) != scenario.sourceMarker {
		t.Fatalf("target source marker = %q, %v", data, err)
	}
	if data, err := os.ReadFile(filepath.Join(workspacePath, "nested", "worker-change.txt")); err != nil ||
		strings.TrimSpace(string(data)) != scenario.workerMarker {
		t.Fatalf("target worker marker = %q, %v", data, err)
	}
	if _, err := os.Stat(filepath.Join(scenario.nestedCWD, "worker-change.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("worker change was written back to source: %v", err)
	}
	checkedOutHead, _ := run(t, os.Environ(), "git", "-C", workspacePath, "rev-parse", "HEAD^{commit}")
	if strings.TrimSpace(checkedOutHead) != scenario.head {
		t.Fatalf("target HEAD = %q, want %q", strings.TrimSpace(checkedOutHead), scenario.head)
	}

	broker := openDatabase(t, brokerStatePath)
	defer broker.Close()
	var status, consumedSpawnID string
	if err := broker.QueryRow(`
SELECT status, consumed_spawn_id, strategy, warnings_json
FROM workspace_sync_receipts
WHERE controller_id = ? AND sync_id = ?
`, networkID, scenario.syncID).Scan(&status, &consumedSpawnID, &strategy, &warningsJSON); err != nil {
		t.Fatal(err)
	}
	if status != "prepared" || consumedSpawnID != scenario.spawnID {
		t.Fatalf("broker workspace receipt = status %q, spawn %q", status, consumedSpawnID)
	}
	assertWorkspaceStrategyAndWarnings(t, "broker workspace receipt", strategy, warningsJSON, scenario)
}

func assertWorkspaceStrategyAndWarnings(
	t *testing.T,
	location, strategy, warningsJSON string,
	scenario workspaceE2EScenario,
) {
	t.Helper()
	var warnings []string
	if err := json.Unmarshal([]byte(warningsJSON), &warnings); err != nil {
		t.Fatalf("%s warnings %q are invalid: %v", location, warningsJSON, err)
	}
	if strategy != string(scenario.strategy) || !slices.Equal(warnings, scenario.warnings) {
		t.Fatalf(
			"%s strategy/warnings = %q/%v, want %q/%v",
			location, strategy, warnings, scenario.strategy, scenario.warnings,
		)
	}
}
