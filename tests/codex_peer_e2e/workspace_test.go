//go:build integration && linux

package codex_peer_e2e

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	delegationconfig "github.com/GhostFlying/delegation/internal/config"
)

const (
	workspaceE2ESyncID    = "123e4567-e89b-42d3-a456-426614174930"
	workspaceE2ESpawnID   = "123e4567-e89b-42d3-a456-426614174931"
	workspaceE2ETask      = "direct_workspace"
	rootWorkspaceSync     = "root-workspace-sync"
	rootWorkspaceSpawn    = "root-workspace-spawn"
	workerWorkspaceDirect = "worker-workspace-direct"
	workspaceSourceMarker = "delegation-workspace-source"
	workspaceWorkerMarker = "delegation-workspace-worker-write"
)

type topologyGitRepository struct {
	gitURL     string
	sourceRoot string
	nestedCWD  string
	head       string
}

func createTopologyGitRepository(
	t *testing.T,
	root string,
	peers []peer,
) topologyGitRepository {
	t.Helper()
	gitRoot := filepath.Join(root, "git")
	remote := filepath.Join(gitRoot, "remote.git")
	source := filepath.Join(gitRoot, "source")
	nested := filepath.Join(source, "nested")
	if err := os.MkdirAll(nested, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(nested, "source.txt"), []byte(workspaceSourceMarker+"\n"), 0o600,
	); err != nil {
		t.Fatal(err)
	}
	run(t, os.Environ(), "git", "init", "--bare", remote)
	run(t, os.Environ(), "git", "init", source)
	run(t, os.Environ(), "git", "-C", source, "add", "nested/source.txt")
	run(t, os.Environ(), "git", "-C", source,
		"-c", "user.name=Delegation Test", "-c", "user.email=test@example.invalid",
		"commit", "-m", "initial")
	run(t, os.Environ(), "git", "-C", source, "remote", "add", "origin", remote)
	run(t, os.Environ(), "git", "-C", source, "push", "origin", "HEAD:refs/heads/main")
	run(t, os.Environ(), "git", "--git-dir="+remote, "update-server-info")
	head, _ := run(t, os.Environ(), "git", "-C", source, "rev-parse", "HEAD^{commit}")
	head = strings.TrimSpace(head)
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
	return topologyGitRepository{
		gitURL:     server.URL + "/" + filepath.Base(remote),
		sourceRoot: source, nestedCWD: nested, head: head,
	}
}

func testDirectWorkspaceDelegation(
	t *testing.T,
	source peer,
	target peer,
	codexBinary, delegationBinary, brokerStatePath string,
	repository topologyGitRepository,
) {
	t.Helper()
	synchronized := runCodexAt(
		t, source, codexBinary, delegationBinary, repository.nestedCWD,
		rootWorkspaceSync, "",
	)
	spawned := runCodexAt(
		t, source, codexBinary, delegationBinary, repository.nestedCWD,
		rootWorkspaceSpawn, synchronized.threadID,
	)
	if spawned.threadID != synchronized.threadID {
		t.Fatalf("workspace spawn resumed %q, want %q", spawned.threadID, synchronized.threadID)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	root := prepareManagedDispatchRoot(t, source, synchronized.threadID)
	agent := findManagedAgent(t, ctx, root, workspaceE2ETask)
	if agent.WorkspaceID != workspaceE2ESyncID || agent.Principal.DeviceID != deviceIDs[target.label] {
		t.Fatalf("workspace agent = %#v", agent)
	}
	targetConfig, err := delegationconfig.Read(target.configPath)
	if err != nil {
		t.Fatal(err)
	}
	waitForManagedWorkerIdle(
		t, targetConfig.Peer.StateFile,
		agent.Principal.TreeID, agent.Principal.AgentID, agent.Principal.ParentAgentID,
		deviceIDs[target.label], workspaceE2ETask,
	)

	database := openDatabase(t, targetConfig.Peer.StateFile)
	defer database.Close()
	var workspacePath, workspaceStatus, claimedAgentID string
	if err := database.QueryRow(`
SELECT workspace_path, status, claimed_agent_id
FROM prepared_workspaces
WHERE controller_id = ? AND tree_id = ? AND workspace_id = ?
`, networkID, agent.Principal.TreeID, workspaceE2ESyncID).Scan(
		&workspacePath, &workspaceStatus, &claimedAgentID,
	); err != nil {
		t.Fatal(err)
	}
	if workspaceStatus != "claimed" || claimedAgentID != agent.Principal.AgentID {
		t.Fatalf("prepared workspace status = %q, claimant %q", workspaceStatus, claimedAgentID)
	}
	if data, err := os.ReadFile(filepath.Join(workspacePath, "nested", "source.txt")); err != nil ||
		strings.TrimSpace(string(data)) != workspaceSourceMarker {
		t.Fatalf("target source marker = %q, %v", data, err)
	}
	if data, err := os.ReadFile(filepath.Join(workspacePath, "nested", "worker-change.txt")); err != nil ||
		strings.TrimSpace(string(data)) != workspaceWorkerMarker {
		t.Fatalf("target worker marker = %q, %v", data, err)
	}
	if _, err := os.Stat(filepath.Join(repository.nestedCWD, "worker-change.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("worker change was written back to source: %v", err)
	}
	checkedOutHead, _ := run(t, os.Environ(), "git", "-C", workspacePath, "rev-parse", "HEAD^{commit}")
	if strings.TrimSpace(checkedOutHead) != repository.head {
		t.Fatalf("target HEAD = %q, want %q", strings.TrimSpace(checkedOutHead), repository.head)
	}
	broker := openDatabase(t, brokerStatePath)
	defer broker.Close()
	var status, consumedSpawnID string
	if err := broker.QueryRow(`
SELECT status, consumed_spawn_id
FROM workspace_sync_receipts
WHERE controller_id = ? AND sync_id = ?
`, networkID, workspaceE2ESyncID).Scan(&status, &consumedSpawnID); err != nil {
		t.Fatal(err)
	}
	if status != "prepared" || consumedSpawnID != workspaceE2ESpawnID {
		t.Fatalf("broker workspace receipt = status %q, spawn %q", status, consumedSpawnID)
	}
}
