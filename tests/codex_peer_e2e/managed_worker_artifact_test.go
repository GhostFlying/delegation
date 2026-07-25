//go:build integration

package codex_peer_e2e

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/broker"
	"github.com/GhostFlying/delegation/internal/codexconfig"
	"github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/connector"
	"github.com/GhostFlying/delegation/internal/gitworkspace"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
	"github.com/GhostFlying/delegation/internal/workerhost"
)

const (
	artifactE2EControllerID = "123e4567-e89b-42d3-a456-426614174960"
	artifactE2EDeviceID     = "123e4567-e89b-42d3-a456-426614174961"
	artifactE2EThreadID     = "123e4567-e89b-42d3-a456-426614174962"
	artifactE2EWorkspaceID  = "123e4567-e89b-42d3-a456-426614174963"
	artifactE2ESpawnID      = "123e4567-e89b-42d3-a456-426614174964"
	artifactE2EAgentID      = "123e4567-e89b-42d3-a456-426614174965"
	artifactEETestCase      = "artifact-cross-platform"
	artifactEESuccessMarker = "CROSS_PLATFORM_ARTIFACT_OK"
)

func TestManagedWorkerPublishesChangesArtifactCrossPlatform(t *testing.T) {
	delegationBinary := requiredManagedExecutable(t, "DELEGATION_E2E_BINARY")
	codexBinary := requiredManagedExecutable(t, "CODEX_BINARY")
	gitBinary, err := exec.LookPath("git")
	if err != nil {
		t.Fatalf("resolve Git: %v", err)
	}
	gitBinary, err = filepath.Abs(gitBinary)
	if err != nil {
		t.Fatal(err)
	}

	userHome, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	root, err := os.MkdirTemp(userHome, ".dma-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	home := filepath.Join(root, "home")
	if err := os.Mkdir(home, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", home)
	if runtime.GOOS == "windows" {
		t.Setenv("USERPROFILE", home)
	}
	if err := os.WriteFile(
		filepath.Join(home, ".gitconfig"),
		[]byte("[http]\n\tsslVerify = false\n"),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	protectedRuntimeDirectory := filepath.Join(root, "protected-runtime")
	if err := os.Mkdir(protectedRuntimeDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	managedRuntimeName := "delegation"
	if runtime.GOOS == "windows" {
		managedRuntimeName += ".exe"
	}
	managedDelegationBinary := filepath.Join(protectedRuntimeDirectory, managedRuntimeName)
	if err := copyExecutable(delegationBinary, managedDelegationBinary); err != nil {
		t.Fatal(err)
	}

	sourceRoot, sourceCWD, gitURL, gitServer := createArtifactE2EGitRepository(t, root, gitBinary)
	defer gitServer.Close()
	runner, err := gitworkspace.NewRunner(gitBinary)
	if err != nil {
		t.Fatal(err)
	}
	repository, err := runner.Inspect(context.Background(), sourceCWD, gitURL)
	if err != nil {
		t.Fatal(err)
	}
	if !repository.Manifest.Clean || repository.Manifest.WorkingDirectory != "nested" {
		t.Fatalf("source manifest = %#v", repository.Manifest)
	}

	mock := &artifactWorkerMock{}
	modelServer := httptest.NewServer(mock)
	defer modelServer.Close()
	providerJSON := fmt.Sprintf(
		`{"model":"gpt-5.2","model_provider":"delegation_mock","model_providers.delegation_mock":{"name":"Delegation artifact mock","base_url":%q,"wire_api":"responses","requires_openai_auth":false}}`,
		modelServer.URL+"/v1",
	)
	delegationHome := filepath.Join(root, "delegation")
	peerConfigPath := filepath.Join(delegationHome, "peer.json")
	peerStatePath := filepath.Join(delegationHome, "state", "peer.sqlite3")
	codexHome := filepath.Join(root, "codex")
	workspaceRoot := filepath.Join(root, "workspaces")
	providerDirectory := filepath.Join(root, "provider")
	if err := config.PreparePrivateDirectory(providerDirectory); err != nil {
		t.Fatal(err)
	}
	providerEnvironmentPath := filepath.Join(providerDirectory, "peer.env")
	if err := os.WriteFile(
		providerEnvironmentPath,
		[]byte(codexconfig.EnvironmentVariable+"="+providerJSON+"\n"),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	runManagedCommand(t, os.Environ(), delegationBinary,
		"setup", "peer", "--config", peerConfigPath,
		"--controller-id", artifactE2EControllerID,
		"--device-id", artifactE2EDeviceID,
		"--device-name", "artifact-native-e2e",
		"--broker-url", "ws://127.0.0.1:1",
		"--auth-mode", "none",
		"--codex-binary", codexBinary,
		"--codex-home", codexHome,
		"--workspace-root", workspaceRoot,
		"--state", peerStatePath,
		"--max-worker-slots", "1",
		"--json",
	)
	peerState, err := store.OpenPeer(context.Background(), peerStatePath)
	if err != nil {
		t.Fatal(err)
	}
	defer peerState.Close()
	host := openManagedTestHost(
		t, peerConfigPath, managedDelegationBinary, codexBinary, providerEnvironmentPath, peerState,
	)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := host.Close(ctx); err != nil {
			t.Errorf("close managed worker host: %v", err)
		}
	}()

	brokerState, err := store.Open(context.Background(), filepath.Join(root, "broker", "state.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	defer brokerState.Close()
	brokerServer, err := broker.New(broker.Options{
		ControllerID: artifactE2EControllerID,
		AuthMode:     config.AuthModeNone,
		Registry:     brokerState,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := brokerServer.Prepare(context.Background()); err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(brokerServer.Handler())
	defer func() {
		httpServer.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := brokerServer.Close(ctx); err != nil {
			t.Errorf("close broker: %v", err)
		}
	}()

	artifactSource := &artifactE2ESource{host: host, state: peerState}
	connectorErrors := make(chan error, 16)
	noopPeer := artifactE2ENoopPeer{}
	peerConnector, err := connector.New(connector.Options{
		BrokerURL:             strings.Replace(httpServer.URL, "http://", "ws://", 1) + broker.ConnectPath,
		ControllerID:          artifactE2EControllerID,
		DeviceID:              artifactE2EDeviceID,
		DeviceName:            "artifact-native-e2e",
		AuthMode:              config.AuthModeNone,
		RuntimeVersion:        "artifact-native-e2e",
		OperatingSystem:       runtime.GOOS,
		Architecture:          runtime.GOARCH,
		ReconnectMin:          5 * time.Millisecond,
		ReconnectMax:          20 * time.Millisecond,
		ReportError:           func(err error) { connectorErrors <- err },
		WorkerSpawner:         noopPeer,
		WorkerController:      noopPeer,
		WorkerLifecycleSource: noopPeer,
		ChangesArtifactSource: artifactSource,
		WorkspaceManager:      noopPeer,
	})
	if err != nil {
		t.Fatal(err)
	}
	connectorContext, cancelConnector := context.WithCancel(context.Background())
	connectorDone := make(chan error, 1)
	go func() { connectorDone <- peerConnector.Run(connectorContext) }()
	defer func() {
		cancelConnector()
		select {
		case err := <-connectorDone:
			if err != nil {
				t.Errorf("connector run: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("connector did not stop")
		}
	}()
	readyContext, cancelReady := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelReady()
	if err := peerConnector.WaitReady(readyContext); err != nil {
		t.Fatalf("connector ready: %v; errors: %v", err, drainArtifactE2EErrors(connectorErrors))
	}

	now := time.Now()
	_, rootPrincipal, err := brokerState.EnsureRootTree(
		context.Background(), artifactE2EControllerID, artifactE2EThreadID,
		artifactE2EDeviceID, now,
	)
	if err != nil {
		t.Fatal(err)
	}
	sourcePathHash := sha256.Sum256([]byte(sourceRoot))
	syncReceipt, err := brokerState.BeginWorkspaceSync(
		context.Background(),
		store.WorkspaceSyncIntent{
			Source: rootPrincipal.Identity(), SyncID: artifactE2EWorkspaceID,
			TargetDeviceID: artifactE2EDeviceID, GitURL: gitURL,
			SourcePathHash: sourcePathHash,
		},
		now.Add(time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	syncReceipt, err = brokerState.PinWorkspaceSyncManifest(
		context.Background(), syncReceipt.Key, repository.Manifest, now.Add(2*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := host.PrepareWorkspace(context.Background(), workerhost.WorkspacePrepareRequest{
		TreeID: rootPrincipal.TreeID,
		Source: rootPrincipal.Identity(),
		Params: protocol.PrepareWorkspaceParams{
			WorkspaceID:    artifactE2EWorkspaceID,
			SourceAgentID:  rootPrincipal.AgentID,
			SourceDeviceID: rootPrincipal.DeviceID,
			Manifest:       repository.Manifest,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if prepared.Outcome != protocol.WorkspacePrepareReady ||
		prepared.Strategy != protocol.WorkspaceStrategyDirect {
		t.Fatalf("prepared workspace = %#v", prepared)
	}
	if _, err := brokerState.FinishWorkspaceSync(
		context.Background(), syncReceipt.Key,
		protocol.WorkspaceSummary{
			WorkspaceID:      artifactE2EWorkspaceID,
			SourceDeviceID:   artifactE2EDeviceID,
			TargetDeviceID:   artifactE2EDeviceID,
			HeadOID:          repository.Manifest.HeadOID,
			ObjectFormat:     repository.Manifest.ObjectFormat,
			WorkingDirectory: repository.Manifest.WorkingDirectory,
			Strategy:         prepared.Strategy,
			ManifestHash:     prepared.ManifestHash,
			Warnings:         prepared.Warnings,
		},
		now.Add(3*time.Second),
	); err != nil {
		t.Fatal(err)
	}
	spawnReceipt, err := brokerState.BeginAgentSpawn(
		context.Background(),
		store.AgentSpawnIntent{
			Source: rootPrincipal.Identity(), SpawnID: artifactE2ESpawnID,
			AgentID: artifactE2EAgentID, TargetDeviceID: artifactE2EDeviceID,
			TaskName: "native_artifact", PromptDigest: sha256.Sum256([]byte(artifactEETestCase)),
			WorkspaceID: artifactE2EWorkspaceID,
		},
		now.Add(4*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := brokerState.MarkAgentSpawnStarted(
		context.Background(),
		store.AgentSpawnKey{
			ControllerID:  artifactE2EControllerID,
			TreeID:        rootPrincipal.TreeID,
			SourceAgentID: rootPrincipal.AgentID,
			SpawnID:       artifactE2ESpawnID,
		},
		now.Add(5*time.Second),
	); err != nil {
		t.Fatal(err)
	}

	started, err := host.Spawn(context.Background(), workerhost.SpawnRequest{
		TreeID:        rootPrincipal.TreeID,
		AgentID:       spawnReceipt.Agent.Principal.AgentID,
		ParentAgentID: rootPrincipal.AgentID,
		TaskName:      "native_artifact",
		Prompt: "managed-worker-case=" + artifactEETestCase +
			" Commit the tracked change and rename, then leave the requested dirty file.",
		WorkspaceID: artifactE2EWorkspaceID,
	})
	if err != nil {
		t.Fatal(err)
	}
	worker := waitForWorkerState(
		t, peerState, started.Worker.WorkerKey, store.WorkerIdle,
		mock.diagnostics,
	)
	if worker.ActiveTurnID != "" || worker.FailureCode != "" {
		t.Fatalf("acknowledged worker = %#v", worker)
	}

	page, err := brokerState.ListChangesArtifacts(
		context.Background(), rootPrincipal.Identity(), store.ChangesArtifactPageRequest{Limit: 1},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Artifacts) != 1 {
		t.Fatalf("root-visible changes artifacts = %#v", page)
	}
	artifact := page.Artifacts[0]
	if artifact.TreeID != rootPrincipal.TreeID ||
		artifact.SourceAgentID != artifactE2EAgentID ||
		artifact.SourceDeviceID != artifactE2EDeviceID ||
		artifact.WorkspaceSourceDeviceID != artifactE2EDeviceID ||
		artifact.WorkspaceTargetDeviceID != artifactE2EDeviceID ||
		artifact.WorkspaceID != artifactE2EWorkspaceID ||
		artifact.Status != protocol.ChangesArtifactAvailable ||
		artifact.BaseHeadOID != repository.Manifest.HeadOID ||
		!artifact.BaseClean || artifact.ResultClean ||
		artifact.ResultHeadOID == repository.Manifest.HeadOID ||
		artifact.Sequence == 0 || page.NextSequence != artifact.Sequence ||
		len(artifact.Parts) != 2 ||
		artifact.Parts[0].Kind != protocol.WorkspaceArtifactBundle ||
		artifact.Parts[1].Kind != protocol.WorkspaceArtifactOverlay {
		t.Fatalf("root-visible changes artifact = %#v", artifact)
	}
	storedArtifact, err := peerState.GetChangesArtifact(
		context.Background(), started.Worker.WorkerKey, artifact.ArtifactID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if storedArtifact.State != store.ChangesPublished ||
		storedArtifact.BrokerSequence != artifact.Sequence ||
		storedArtifact.WorkspaceSourceDeviceID != artifactE2EDeviceID ||
		storedArtifact.WorkspaceTargetDeviceID != artifactE2EDeviceID {
		t.Fatalf("peer ACK state = %#v", storedArtifact)
	}
	if pending, err := host.ListPendingChangesPublications(context.Background()); err != nil || len(pending) != 0 {
		t.Fatalf("pending changes publications = %#v, %v", pending, err)
	}

	assertArtifactE2EGitResult(t, gitBinary, sourceRoot, worker.WorkspacePath, repository.Manifest.HeadOID, artifact.ResultHeadOID)
	mock.verify(t)
	if reported := drainArtifactE2EErrors(connectorErrors); len(reported) != 0 {
		t.Fatalf("connector errors: %v", reported)
	}
}

func createArtifactE2EGitRepository(
	t *testing.T,
	root, gitBinary string,
) (string, string, string, *httptest.Server) {
	t.Helper()
	gitRoot := filepath.Join(root, "git")
	sourceRoot := filepath.Join(gitRoot, "source")
	sourceCWD := filepath.Join(sourceRoot, "nested")
	remoteRoot := filepath.Join(gitRoot, "remote.git")
	if err := os.MkdirAll(sourceCWD, 0o700); err != nil {
		t.Fatal(err)
	}
	runManagedCommand(t, os.Environ(), gitBinary, "init", sourceRoot)
	for name, content := range map[string]string{
		"tracked.txt":       "tracked-base\n",
		"rename-source.txt": "rename-base\n",
	} {
		if err := os.WriteFile(filepath.Join(sourceCWD, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	runManagedCommand(t, os.Environ(), gitBinary, "-C", sourceRoot, "add", "nested")
	runManagedCommand(
		t, os.Environ(), gitBinary, "-C", sourceRoot,
		"-c", "user.name=Delegation Test", "-c", "user.email=test@example.invalid",
		"commit", "-m", "artifact base",
	)
	runManagedCommand(t, os.Environ(), gitBinary, "init", "--bare", remoteRoot)
	runManagedCommand(t, os.Environ(), gitBinary, "-C", sourceRoot, "push", remoteRoot, "HEAD:refs/heads/main")
	runManagedCommand(t, os.Environ(), gitBinary, "--git-dir="+remoteRoot, "symbolic-ref", "HEAD", "refs/heads/main")
	runManagedCommand(t, os.Environ(), gitBinary, "--git-dir="+remoteRoot, "update-server-info")
	server := httptest.NewTLSServer(http.FileServer(http.Dir(gitRoot)))
	return sourceRoot, sourceCWD, server.URL + "/remote.git", server
}

func assertArtifactE2EGitResult(
	t *testing.T,
	gitBinary, sourceRoot, workerRoot, baseHead, resultHead string,
) {
	t.Helper()
	tracked, err := os.ReadFile(filepath.Join(workerRoot, "nested", "tracked.txt"))
	if err != nil || strings.TrimSpace(string(tracked)) != "tracked-worker" {
		t.Fatalf("worker tracked file = %q, %v", tracked, err)
	}
	renamed, err := os.ReadFile(filepath.Join(workerRoot, "nested", "renamed-worker.txt"))
	if err != nil || strings.TrimSpace(string(renamed)) != "rename-base" {
		t.Fatalf("worker renamed file = %q, %v", renamed, err)
	}
	if _, err := os.Stat(filepath.Join(workerRoot, "nested", "rename-source.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("worker retained pre-rename path: %v", err)
	}
	dirty, err := os.ReadFile(filepath.Join(workerRoot, "nested", "dirty-worker.txt"))
	if err != nil || strings.TrimSpace(string(dirty)) != "dirty-worker" {
		t.Fatalf("worker dirty file = %q, %v", dirty, err)
	}
	checkedHead, _ := runManagedCommand(t, os.Environ(), gitBinary, "-C", workerRoot, "rev-parse", "HEAD^{commit}")
	if strings.TrimSpace(checkedHead) != resultHead {
		t.Fatalf("worker HEAD = %q, want %q", strings.TrimSpace(checkedHead), resultHead)
	}
	runManagedCommand(t, os.Environ(), gitBinary, "-C", workerRoot, "merge-base", "--is-ancestor", baseHead, resultHead)
	diff, _ := runManagedCommand(t, os.Environ(), gitBinary, "-C", workerRoot, "diff", "--name-status", "-M", baseHead+".."+resultHead)
	for _, expected := range []string{"M\tnested/tracked.txt", "R100\tnested/rename-source.txt\tnested/renamed-worker.txt"} {
		if !strings.Contains(strings.ReplaceAll(diff, "\r\n", "\n"), expected) {
			t.Fatalf("worker committed diff = %q, missing %q", diff, expected)
		}
	}
	status, _ := runManagedCommand(t, os.Environ(), gitBinary, "-C", workerRoot, "status", "--porcelain=v1", "--untracked-files=all")
	if strings.TrimSpace(strings.ReplaceAll(status, "\r\n", "\n")) != "?? nested/dirty-worker.txt" {
		t.Fatalf("worker dirty status = %q", status)
	}
	sourceStatus, _ := runManagedCommand(t, os.Environ(), gitBinary, "-C", sourceRoot, "status", "--porcelain=v1", "--untracked-files=all")
	if strings.TrimSpace(sourceStatus) != "" {
		t.Fatalf("source workspace changed: %q", sourceStatus)
	}
	if _, err := os.Stat(filepath.Join(sourceRoot, "nested", "rename-source.txt")); err != nil {
		t.Fatalf("source rename path changed: %v", err)
	}
}
