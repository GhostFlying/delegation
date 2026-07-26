//go:build integration && linux

package codex_peer_e2e

import (
	"bytes"
	"context"
	"database/sql"
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
		} else {
			scenario.gitURL = server.URL + "/unavailable-full-remote.git"
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

	artifact := waitForWorkspaceArtifact(t, ctx, root, agent, scenario, resultHead, manifestHash, sourceSnapshotHash)
	assertPeerChangesArtifact(t, database, artifact)

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
	assertBrokerChangesArtifact(t, broker, brokerStatePath, workspacePath, scenario, artifact)
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

func waitForWorkspaceArtifact(
	t *testing.T,
	ctx context.Context,
	root managedDispatchRoot,
	agent protocol.AgentSummary,
	scenario workspaceE2EScenario,
	resultHead, manifestHash, sourceSnapshotHash string,
) protocol.ChangesArtifactMetadata {
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
		t.Fatalf("wait for workspace artifact: %v", err)
	}
	if len(result.Artifacts) != 1 || result.MoreArtifacts || result.NextArtifactCursor == 0 {
		t.Fatalf("workspace artifact wait = %#v", result)
	}
	artifact := result.Artifacts[0]
	if artifact.Sequence != result.NextArtifactCursor || artifact.TreeID != agent.Principal.TreeID ||
		artifact.SourceAgentID != agent.Principal.AgentID ||
		artifact.SourceDeviceID != agent.Principal.DeviceID ||
		artifact.WorkspaceSourceDeviceID != rootSource.DeviceID ||
		artifact.WorkspaceTargetDeviceID != agent.Principal.DeviceID ||
		artifact.WorkspaceID != scenario.syncID || artifact.Status != protocol.ChangesArtifactAvailable ||
		artifact.ObjectFormat != "sha1" || artifact.BaseHeadOID != scenario.head || artifact.BaseClean ||
		artifact.BaseManifestHash != manifestHash || artifact.BaseSnapshotHash != sourceSnapshotHash ||
		artifact.ResultHeadOID != resultHead || artifact.ResultClean ||
		len(artifact.ResultSnapshotHash) != 64 || artifact.FailureCode != "" ||
		!slices.Equal(artifact.BaseWarnings, scenario.baseWarnings) ||
		len(artifact.ResultWarnings) != 0 {
		t.Fatalf("workspace artifact metadata = %#v", artifact)
	}
	if len(artifact.Parts) != 2 || artifact.Parts[0].Kind != protocol.WorkspaceArtifactBundle ||
		artifact.Parts[1].Kind != protocol.WorkspaceArtifactOverlay {
		t.Fatalf("workspace artifact parts = %#v", artifact.Parts)
	}
	for _, part := range artifact.Parts {
		if part.Size < 1 || len(part.SHA256) != 64 {
			t.Fatalf("workspace artifact part = %#v", part)
		}
	}
	return artifact
}

func assertPeerChangesArtifact(
	t *testing.T,
	database *sql.DB,
	artifact protocol.ChangesArtifactMetadata,
) {
	t.Helper()
	var (
		state, status, baseHead, baseManifest, baseSnapshot   string
		workspaceSourceDeviceID, workspaceTargetDeviceID      string
		resultHead, resultSnapshot, bundleName, bundleSHA     string
		overlayName, overlaySHA, baseWarningsJSON             string
		resultWarningsJSON                                    string
		resultClean, baseClean                                bool
		bundleSize, overlaySize, payloadBytes, brokerSequence int64
	)
	if err := database.QueryRow(`
SELECT state, capture_status, workspace_source_device_id, workspace_target_device_id,
       base_head_oid, base_manifest_hash, base_snapshot_hash,
       base_clean, result_head_oid, result_snapshot_hash, result_clean,
       bundle_part_name, bundle_size_bytes, bundle_sha256,
       overlay_part_name, overlay_size_bytes, overlay_sha256,
       base_warnings_json, result_warnings_json, payload_bytes, broker_sequence
FROM peer_changes_artifacts
WHERE controller_id = ? AND tree_id = ? AND agent_id = ? AND artifact_id = ?
`, networkID, artifact.TreeID, artifact.SourceAgentID, artifact.ArtifactID).Scan(
		&state, &status, &workspaceSourceDeviceID, &workspaceTargetDeviceID,
		&baseHead, &baseManifest, &baseSnapshot, &baseClean,
		&resultHead, &resultSnapshot, &resultClean,
		&bundleName, &bundleSize, &bundleSHA, &overlayName, &overlaySize, &overlaySHA,
		&baseWarningsJSON, &resultWarningsJSON, &payloadBytes, &brokerSequence,
	); err != nil {
		t.Fatal(err)
	}
	var baseWarnings, resultWarnings []string
	if err := json.Unmarshal([]byte(baseWarningsJSON), &baseWarnings); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(resultWarningsJSON), &resultWarnings); err != nil {
		t.Fatal(err)
	}
	if state != "published" || status != string(artifact.Status) ||
		workspaceSourceDeviceID != artifact.WorkspaceSourceDeviceID ||
		workspaceTargetDeviceID != artifact.WorkspaceTargetDeviceID ||
		baseHead != artifact.BaseHeadOID || baseManifest != artifact.BaseManifestHash ||
		baseSnapshot != artifact.BaseSnapshotHash || baseClean != artifact.BaseClean ||
		resultHead != artifact.ResultHeadOID || resultSnapshot != artifact.ResultSnapshotHash ||
		resultClean != artifact.ResultClean || bundleName != "changes.bundle" ||
		bundleSize != artifact.Parts[0].Size || bundleSHA != artifact.Parts[0].SHA256 ||
		overlayName != "changes-overlay.tar.zst" || overlaySize != artifact.Parts[1].Size ||
		overlaySHA != artifact.Parts[1].SHA256 || payloadBytes != bundleSize+overlaySize ||
		uint64(brokerSequence) != artifact.Sequence ||
		!slices.Equal(baseWarnings, artifact.BaseWarnings) ||
		!slices.Equal(resultWarnings, artifact.ResultWarnings) {
		t.Fatalf("peer changes artifact does not match root metadata")
	}
}

func assertBrokerChangesArtifact(
	t *testing.T,
	database *sql.DB,
	statePath, workspacePath string,
	scenario workspaceE2EScenario,
	artifact protocol.ChangesArtifactMetadata,
) {
	t.Helper()
	var (
		status, workspaceID, sourceAgentID, sourceDeviceID, baseHead string
		workspaceSourceDeviceID, workspaceTargetDeviceID             string
		baseManifest, baseSnapshot, resultHead, resultSnapshot       string
		partsJSON, baseWarningsJSON, resultWarningsJSON              string
		failureCode                                                  string
		baseClean, resultClean                                       bool
		sequence                                                     uint64
	)
	if err := database.QueryRow(`
SELECT status, workspace_id, source_agent_id, source_device_id,
       workspace_source_device_id, workspace_target_device_id, base_head_oid,
       base_manifest_hash, base_snapshot_hash, base_clean, result_head_oid,
       result_snapshot_hash, result_clean, parts_json, base_warnings_json,
       result_warnings_json, failure_code, artifact_sequence
FROM changes_artifacts
WHERE controller_id = ? AND tree_id = ? AND artifact_id = ?
`, networkID, artifact.TreeID, artifact.ArtifactID).Scan(
		&status, &workspaceID, &sourceAgentID, &sourceDeviceID,
		&workspaceSourceDeviceID, &workspaceTargetDeviceID, &baseHead,
		&baseManifest, &baseSnapshot, &baseClean, &resultHead, &resultSnapshot,
		&resultClean, &partsJSON, &baseWarningsJSON, &resultWarningsJSON,
		&failureCode, &sequence,
	); err != nil {
		t.Fatal(err)
	}
	var parts []protocol.WorkspaceArtifactDescriptor
	var baseWarnings, resultWarnings []string
	if err := json.Unmarshal([]byte(partsJSON), &parts); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(baseWarningsJSON), &baseWarnings); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(resultWarningsJSON), &resultWarnings); err != nil {
		t.Fatal(err)
	}
	if status != string(artifact.Status) || workspaceID != artifact.WorkspaceID ||
		sourceAgentID != artifact.SourceAgentID || sourceDeviceID != artifact.SourceDeviceID ||
		workspaceSourceDeviceID != artifact.WorkspaceSourceDeviceID ||
		workspaceTargetDeviceID != artifact.WorkspaceTargetDeviceID ||
		baseHead != artifact.BaseHeadOID || baseManifest != artifact.BaseManifestHash ||
		baseSnapshot != artifact.BaseSnapshotHash || baseClean != artifact.BaseClean ||
		resultHead != artifact.ResultHeadOID || resultSnapshot != artifact.ResultSnapshotHash ||
		resultClean != artifact.ResultClean || !slices.Equal(parts, artifact.Parts) ||
		!slices.Equal(baseWarnings, artifact.BaseWarnings) ||
		!slices.Equal(resultWarnings, artifact.ResultWarnings) ||
		failureCode != artifact.FailureCode ||
		sequence != artifact.Sequence {
		t.Fatalf("broker changes artifact does not match root metadata")
	}
	if strings.Contains(partsJSON, "changes.bundle") || strings.Contains(partsJSON, "changes-overlay.tar.zst") {
		t.Fatalf("broker parts contain peer-local names: %s", partsJSON)
	}

	rows, err := database.Query(`PRAGMA table_info('changes_artifacts')`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, columnType string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			t.Fatal(err)
		}
		lowerName := strings.ToLower(name)
		if strings.Contains(lowerName, "path") || strings.Contains(lowerName, "payload") ||
			strings.EqualFold(columnType, "blob") {
			t.Fatalf("broker changes artifact contains payload-bearing column %q %q", name, columnType)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

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
