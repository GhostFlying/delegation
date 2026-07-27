//go:build integration

package codex_peer_e2e

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/broker"
	"github.com/GhostFlying/delegation/internal/codexconfig"
	"github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/connector"
	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/gitworkspace"
	"github.com/GhostFlying/delegation/internal/localbridge"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/rolloutcapture"
	"github.com/GhostFlying/delegation/internal/store"
	"github.com/GhostFlying/delegation/internal/workerhost"
	"github.com/klauspost/compress/zstd"
)

const (
	artifactE2EControllerID   = "123e4567-e89b-42d3-a456-426614174960"
	artifactE2ERootDeviceID   = "123e4567-e89b-42d3-a456-426614174961"
	artifactE2EThreadID       = "123e4567-e89b-42d3-a456-426614174962"
	artifactE2EWorkspaceID    = "123e4567-e89b-42d3-a456-426614174963"
	artifactE2ESpawnID        = "123e4567-e89b-42d3-a456-426614174964"
	artifactE2EAgentID        = "123e4567-e89b-42d3-a456-426614174965"
	artifactE2EFollowupID     = "123e4567-e89b-42d3-a456-426614174966"
	artifactE2EWorkerDeviceID = "123e4567-e89b-42d3-a456-426614174967"
	artifactEEDirtyCase       = "artifact-dirty-commit"
	artifactEECleanCase       = "artifact-clean-commit"
	artifactEESuccessMarker   = "CROSS_PLATFORM_ARTIFACT_OK"
)

type artifactE2EScenario struct {
	name       string
	testCase   string
	leaveDirty bool
}

type artifactE2EPeer struct {
	deviceID string
	state    *store.PeerStore
	host     *managedTestHost
}

func openArtifactE2EPeer(
	t *testing.T,
	root, name, deviceID, setupBinary, managedBinary, codexBinary, providerEnvironmentPath string,
) artifactE2EPeer {
	t.Helper()
	peerRoot := filepath.Join(root, name+"-peer")
	configPath := filepath.Join(peerRoot, "delegation", "peer.json")
	statePath := filepath.Join(peerRoot, "delegation", "state", "peer.sqlite3")
	codexHome := filepath.Join(peerRoot, "codex")
	workspaceRoot := filepath.Join(peerRoot, "workspaces")
	runManagedCommand(t, os.Environ(), setupBinary,
		"setup", "peer", "--config", configPath,
		"--controller-id", artifactE2EControllerID,
		"--device-id", deviceID,
		"--device-name", "artifact-native-e2e-"+name,
		"--broker-url", "ws://127.0.0.1:1",
		"--auth-mode", "none",
		"--codex-binary", codexBinary,
		"--codex-home", codexHome,
		"--workspace-root", workspaceRoot,
		"--state", statePath,
		"--max-worker-slots", "1",
		"--json",
	)
	state, err := store.OpenPeer(context.Background(), statePath)
	if err != nil {
		t.Fatal(err)
	}
	host := openManagedTestHost(
		t, configPath, managedBinary, codexBinary, providerEnvironmentPath, state,
	)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := host.Close(ctx); err != nil {
			t.Errorf("close %s managed host: %v", name, err)
		}
		if err := state.Close(); err != nil {
			t.Errorf("close %s peer state: %v", name, err)
		}
	})
	return artifactE2EPeer{deviceID: deviceID, state: state, host: host}
}

func startArtifactE2EConnector(
	t *testing.T,
	brokerURL, name string,
	peer artifactE2EPeer,
) (*connector.Client, <-chan error, func()) {
	t.Helper()
	reported := make(chan error, 32)
	artifactSource := &artifactE2ESource{host: peer.host.Host, state: peer.state}
	noopPeer := artifactE2ENoopPeer{}
	client, err := connector.New(connector.Options{
		BrokerURL:             brokerURL,
		ControllerID:          artifactE2EControllerID,
		DeviceID:              peer.deviceID,
		DeviceName:            "artifact-native-e2e-" + name,
		AuthMode:              config.AuthModeNone,
		RuntimeVersion:        "artifact-native-e2e",
		OperatingSystem:       runtime.GOOS,
		Architecture:          runtime.GOARCH,
		ReconnectMin:          5 * time.Millisecond,
		ReconnectMax:          20 * time.Millisecond,
		ReportError:           func(err error) { reported <- err },
		WorkerSpawner:         noopPeer,
		WorkerController:      noopPeer,
		WorkerLifecycleSource: peer.host,
		ChangesArtifactSource: artifactSource,
		ResultPackageSource:   peer.host,
		WorkspaceManager:      noopPeer,
		ResultPackageManager:  peer.host,
	})
	if err != nil {
		t.Fatal(err)
	}
	connectorContext, cancelConnector := context.WithCancel(context.Background())
	connectorDone := make(chan error, 1)
	go func() { connectorDone <- client.Run(connectorContext) }()
	readyContext, cancelReady := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelReady()
	if err := client.WaitReady(readyContext); err != nil {
		cancelConnector()
		t.Fatalf(
			"%s connector ready: %v; errors: %v",
			name, err, drainArtifactE2EErrors(reported),
		)
	}
	stop := func() {
		cancelConnector()
		select {
		case err := <-connectorDone:
			if err != nil {
				t.Errorf("%s connector run: %v", name, err)
			}
		case <-time.After(5 * time.Second):
			t.Errorf("%s connector did not stop", name)
		}
	}
	return client, reported, stop
}

func TestManagedWorkerReturnsResultPackageCrossPlatform(t *testing.T) {
	for _, scenario := range []artifactE2EScenario{
		{name: "dirty_and_commit", testCase: artifactEEDirtyCase, leaveDirty: true},
		{name: "clean_commit_only", testCase: artifactEECleanCase},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			testManagedWorkerPublishesResultPackage(t, scenario)
		})
	}
}

func testManagedWorkerPublishesResultPackage(t *testing.T, scenario artifactE2EScenario) {
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

	mock := &artifactWorkerMock{testCase: scenario.testCase, leaveDirty: scenario.leaveDirty}
	modelServer := httptest.NewServer(mock)
	defer modelServer.Close()
	providerJSON := fmt.Sprintf(
		`{"model":"gpt-5.2","model_provider":"delegation_mock","model_providers.delegation_mock":{"name":"Delegation artifact mock","base_url":%q,"wire_api":"responses","requires_openai_auth":false}}`,
		modelServer.URL+"/v1",
	)
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
	rootPeer := openArtifactE2EPeer(
		t, root, "root", artifactE2ERootDeviceID, delegationBinary,
		managedDelegationBinary, codexBinary, providerEnvironmentPath,
	)
	workerPeer := openArtifactE2EPeer(
		t, root, "worker", artifactE2EWorkerDeviceID, delegationBinary,
		managedDelegationBinary, codexBinary, providerEnvironmentPath,
	)

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

	brokerURL := strings.Replace(httpServer.URL, "http://", "ws://", 1) + broker.ConnectPath
	rootConnector, rootConnectorErrors, stopRootConnector := startArtifactE2EConnector(
		t, brokerURL, "root", rootPeer,
	)
	defer stopRootConnector()
	_, workerConnectorErrors, stopWorkerConnector := startArtifactE2EConnector(
		t, brokerURL, "worker", workerPeer,
	)
	defer stopWorkerConnector()

	now := time.Now()
	_, rootPrincipal, err := brokerState.EnsureRootTree(
		context.Background(), artifactE2EControllerID, artifactE2EThreadID,
		artifactE2ERootDeviceID, now,
	)
	if err != nil {
		t.Fatal(err)
	}
	sourcePathHash := sha256.Sum256([]byte(sourceRoot))
	syncReceipt, err := brokerState.BeginWorkspaceSync(
		context.Background(),
		store.WorkspaceSyncIntent{
			Source: rootPrincipal.Identity(), SyncID: artifactE2EWorkspaceID,
			TargetDeviceID: artifactE2EWorkerDeviceID, GitURL: gitURL,
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
	prepared, err := workerPeer.host.PrepareWorkspace(context.Background(), workerhost.WorkspacePrepareRequest{
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
			SourceDeviceID:   artifactE2ERootDeviceID,
			TargetDeviceID:   artifactE2EWorkerDeviceID,
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
			AgentID: artifactE2EAgentID, TargetDeviceID: artifactE2EWorkerDeviceID,
			TaskName: "native_artifact", PromptDigest: sha256.Sum256([]byte(scenario.testCase)),
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

	started, err := workerPeer.host.Spawn(context.Background(), workerhost.SpawnRequest{
		TreeID:        rootPrincipal.TreeID,
		AgentID:       spawnReceipt.Agent.Principal.AgentID,
		ParentAgentID: rootPrincipal.AgentID,
		TaskName:      "native_artifact",
		Prompt: "managed-worker-case=" + scenario.testCase +
			" Commit the tracked change and rename, then leave the requested dirty file.",
		WorkspaceID: artifactE2EWorkspaceID,
	})
	if err != nil {
		t.Fatal(err)
	}
	initialTurnID := started.Worker.ActiveTurnID
	worker := waitForWorkerState(
		t, workerPeer.state, started.Worker.WorkerKey, store.WorkerIdle,
		mock.diagnostics,
	)
	if initialTurnID == "" || worker.LastBoundTurnID != initialTurnID ||
		worker.ActiveTurnID != "" || worker.FailureCode != "" {
		t.Fatalf("acknowledged initial worker = %#v; turnId = %q", worker, initialTurnID)
	}
	initialResultHandle := waitForArtifactE2EResult(
		t, rootConnector, rootPeer.host, rootPrincipal.Identity(), initialTurnID, scenario,
	)
	assertArtifactE2EResultPackage(
		t, runner, sourceRoot, worker.WorkspacePath, rootPeer.host.workspaceRoot,
		repository.Manifest, initialResultHandle, rootPrincipal.TreeID,
		worker.CodexThreadID, initialTurnID, scenario,
	)
	followup, err := workerPeer.host.Followup(context.Background(), workerhost.FollowupRequest{
		OperationID: artifactE2EFollowupID,
		Key:         started.Worker.WorkerKey,
		Message: "managed-worker-case=" + scenario.testCase +
			" Verify the workspace and return result-ready without changing files.",
	})
	if err != nil {
		t.Fatal(err)
	}
	resultTurnID := followup.Worker.ActiveTurnID
	worker = waitForWorkerState(
		t, workerPeer.state, followup.Worker.WorkerKey, store.WorkerIdle, mock.diagnostics,
	)
	if resultTurnID == "" || worker.LastBoundTurnID != resultTurnID ||
		worker.ActiveTurnID != "" || worker.FailureCode != "" {
		t.Fatalf("acknowledged follow-up worker = %#v; turnId = %q", worker, resultTurnID)
	}

	resultHandle := waitForArtifactE2EResult(
		t, rootConnector, rootPeer.host, rootPrincipal.Identity(), resultTurnID, scenario,
	)
	assertArtifactE2EResultPackage(
		t, runner, sourceRoot, worker.WorkspacePath, rootPeer.host.workspaceRoot,
		repository.Manifest, resultHandle, rootPrincipal.TreeID,
		worker.CodexThreadID, resultTurnID, scenario,
	)
	assertArtifactE2EGitResult(
		t, gitBinary, sourceRoot, worker.WorkspacePath,
		repository.Manifest.HeadOID, resultHandle.Manifest.Workspace.ResultHeadOID,
		scenario.leaveDirty,
	)
	mock.verify(t)
	if reported := drainArtifactE2EErrors(rootConnectorErrors); len(reported) != 0 {
		t.Fatalf("root connector errors: %v", reported)
	}
	if reported := drainArtifactE2EErrors(workerConnectorErrors); len(reported) != 0 {
		t.Fatalf("worker connector errors: %v", reported)
	}
}

func waitForArtifactE2EResult(
	t *testing.T,
	rootConnector *connector.Client,
	rootHost *managedTestHost,
	root control.PrincipalIdentity,
	turnID string,
	scenario artifactE2EScenario,
) protocol.ResultPackageHandle {
	t.Helper()
	endpoint, err := localbridge.Endpoint(artifactE2EControllerID, artifactE2ERootDeviceID)
	if err != nil {
		t.Fatal(err)
	}
	bridge, err := localbridge.ListenWithResultPackages(
		endpoint,
		localbridge.ServiceIdentity{
			ControllerID: artifactE2EControllerID,
			DeviceID:     artifactE2ERootDeviceID,
		},
		rootConnector,
		nil,
		nil,
		rootHost,
	)
	if err != nil {
		t.Fatal(err)
	}
	bridgeContext, cancelBridge := context.WithCancel(context.Background())
	bridgeDone := make(chan error, 1)
	go func() { bridgeDone <- bridge.Serve(bridgeContext) }()
	defer func() {
		cancelBridge()
		if err := bridge.Close(); err != nil {
			t.Errorf("close result-package local bridge: %v", err)
		}
		if err := <-bridgeDone; err != nil {
			t.Errorf("serve result-package local bridge: %v", err)
		}
	}()
	client, err := localbridge.NewClient(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	waitContext, cancelWait := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelWait()
	params := protocol.WaitAgentParams{
		TimeoutMillis: 2_000,
		MessageLimit:  protocol.MaximumAgentWaitMessages,
		ActivityLimit: protocol.MaximumAgentWaitActivities,
		ArtifactLimit: protocol.MaximumAgentWaitArtifacts,
		ResultLimit:   protocol.MaximumAgentWaitResults,
	}
	for {
		var result protocol.WaitAgentResult
		if err := client.Call(
			waitContext,
			protocol.MethodWaitAgent,
			root.TreeID,
			&root,
			params,
			&result,
		); err != nil {
			t.Fatalf("root wait_agent for %s result package: %v", scenario.name, err)
		}
		params.MailboxCursor = result.NextMailboxCursor
		params.LifecycleCursor = result.NextLifecycleCursor
		params.ArtifactCursor = result.NextArtifactCursor
		params.ResultCursor = result.NextResultCursor
		for _, handle := range result.Results {
			if handle.Manifest.SourceAgentID != artifactE2EAgentID ||
				handle.Manifest.TurnID != turnID {
				continue
			}
			if handle.Availability != protocol.ResultPackageAvailable {
				t.Fatalf("root wait_agent result availability = %q", handle.Availability)
			}
			if err := handle.Validate(); err != nil {
				t.Fatalf("root wait_agent result handle: %v", err)
			}
			return handle
		}
		if err := waitContext.Err(); err != nil {
			t.Fatalf("root wait_agent did not surface result package: %v", err)
		}
	}
}

func assertArtifactE2EResultPackage(
	t *testing.T,
	runner gitworkspace.Runner,
	sourceRoot, workerRoot, workspaceRoot string,
	base protocol.WorkspaceManifest,
	handle protocol.ResultPackageHandle,
	treeID, managedThreadID, turnID string,
	scenario artifactE2EScenario,
) {
	t.Helper()
	manifest := handle.Manifest
	baseManifestHash, err := gitworkspace.ManifestHash(base)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.ControllerID != artifactE2EControllerID ||
		manifest.TreeID != treeID ||
		manifest.SourceAgentID != artifactE2EAgentID ||
		manifest.SourceDeviceID != artifactE2EWorkerDeviceID ||
		manifest.ManagedThreadID != managedThreadID || manifest.TurnID != turnID ||
		manifest.Terminal.Outcome != protocol.ResultTerminalCompleted ||
		manifest.Rollout.Status != protocol.ResultRolloutAvailable ||
		manifest.Workspace.Status != protocol.ResultWorkspaceChanged ||
		manifest.Workspace.WorkspaceID != artifactE2EWorkspaceID ||
		manifest.Workspace.SourceDeviceID != artifactE2ERootDeviceID ||
		manifest.Workspace.TargetDeviceID != artifactE2EWorkerDeviceID ||
		manifest.Workspace.ObjectFormat != base.ObjectFormat ||
		manifest.Workspace.BaseHeadOID != base.HeadOID ||
		manifest.Workspace.BaseManifestHash != baseManifestHash ||
		manifest.Workspace.BaseSnapshotHash != base.SourceSnapshotHash ||
		manifest.Workspace.BaseClean != base.Clean ||
		manifest.Workspace.ResultHeadOID == base.HeadOID ||
		manifest.Workspace.ResultClean != !scenario.leaveDirty {
		t.Fatalf("root result package manifest = %#v", manifest)
	}
	wantKinds := []protocol.ResultPackagePartKind{protocol.ResultPackagePartChangesBundle}
	if scenario.leaveDirty {
		wantKinds = append(wantKinds, protocol.ResultPackagePartChangesOverlay)
	}
	wantKinds = append(wantKinds, protocol.ResultPackagePartRollout)
	gotKinds := make([]protocol.ResultPackagePartKind, 0, len(manifest.Parts))
	for _, part := range manifest.Parts {
		gotKinds = append(gotKinds, part.Kind)
	}
	if !slices.Equal(gotKinds, wantKinds) {
		t.Fatalf("root result package parts = %#v, want %#v", gotKinds, wantKinds)
	}

	packageDirectory := filepath.Join(
		workspaceRoot,
		".delegation-result-inbox-v2",
		manifest.PackageID,
	)
	manifestPath := filepath.Join(packageDirectory, protocol.ResultManifestFileName)
	manifestBytes, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	wantManifestBytes, _, err := protocol.EncodeResultManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(manifestBytes, wantManifestBytes) {
		t.Fatal("root inbox manifest bytes differ from wait_agent metadata")
	}
	for _, descriptor := range manifest.Parts {
		fileName, err := descriptor.Kind.FileName()
		if err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(filepath.Join(packageDirectory, fileName))
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(data)
		if int64(len(data)) != descriptor.Size || fmt.Sprintf("%x", digest) != descriptor.SHA256 {
			t.Fatalf("root inbox %s does not match descriptor %#v", descriptor.Kind, descriptor)
		}
	}

	rolloutPath := filepath.Join(packageDirectory, protocol.ResultRolloutFileName)
	rolloutFile, err := os.Open(rolloutPath)
	if err != nil {
		t.Fatal(err)
	}
	verifyErr := rolloutcapture.VerifyCompressedSegment(
		context.Background(), rolloutFile, manifest.Rollout.RawSize, manifest.Rollout.RawSHA256,
	)
	verifyErr = errors.Join(verifyErr, rolloutFile.Close())
	if verifyErr != nil {
		t.Fatalf("verify returned rollout: %v", verifyErr)
	}
	compressedRollout, err := os.ReadFile(rolloutPath)
	if err != nil {
		t.Fatal(err)
	}
	decoder, err := zstd.NewReader(bytes.NewReader(compressedRollout))
	if err != nil {
		t.Fatal(err)
	}
	rawRollout, readErr := io.ReadAll(io.LimitReader(decoder, manifest.Rollout.RawSize+1))
	decoder.Close()
	if readErr != nil || int64(len(rawRollout)) != manifest.Rollout.RawSize ||
		!bytes.Contains(rawRollout, []byte(manifest.TurnID)) ||
		!bytes.Contains(rawRollout, []byte(`"type":"task_started"`)) ||
		!bytes.Contains(rawRollout, []byte(`"type":"task_complete"`)) {
		t.Fatalf("returned rollout segment is incomplete: bytes=%d error=%v", len(rawRollout), readErr)
	}

	resultManifest := base
	resultManifest.HeadOID = manifest.Workspace.ResultHeadOID
	resultManifest.SourceSnapshotHash = manifest.Workspace.ResultSnapshotHash
	resultManifest.Clean = manifest.Workspace.ResultClean
	resultManifest.Warnings = slices.Clone(manifest.Workspace.ResultWarnings)
	if err := resultManifest.Validate(); err != nil {
		t.Fatal(err)
	}
	materializedRoot := filepath.Join(t.TempDir(), "returned-workspace")
	prepared, err := runner.PrepareBase(context.Background(), materializedRoot, resultManifest)
	if err != nil {
		t.Fatal(err)
	}
	if !prepared.BundleRequired || prepared.OverlayRequired != scenario.leaveDirty {
		t.Fatalf("returned Git payload preparation = %#v", prepared)
	}
	if err := runner.ApplyBundle(
		context.Background(),
		materializedRoot,
		filepath.Join(packageDirectory, protocol.ResultChangesBundleFileName),
		resultManifest,
	); err != nil {
		t.Fatalf("apply returned changes bundle: %v", err)
	}
	if scenario.leaveDirty {
		if err := runner.ApplyOverlay(
			context.Background(),
			materializedRoot,
			filepath.Join(packageDirectory, protocol.ResultChangesOverlayFileName),
			resultManifest,
		); err != nil {
			t.Fatalf("apply returned changes overlay: %v", err)
		}
	}
	assertArtifactE2EGitResult(
		t,
		runner.Binary,
		sourceRoot,
		materializedRoot,
		base.HeadOID,
		manifest.Workspace.ResultHeadOID,
		scenario.leaveDirty,
	)
	workerHead, _ := runManagedCommand(
		t, os.Environ(), runner.Binary, "-C", workerRoot, "rev-parse", "HEAD^{commit}",
	)
	if strings.TrimSpace(workerHead) != manifest.Workspace.ResultHeadOID {
		t.Fatalf("worker result HEAD = %q", strings.TrimSpace(workerHead))
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
	leaveDirty bool,
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
	if leaveDirty && (err != nil || strings.TrimSpace(string(dirty)) != "dirty-worker") {
		t.Fatalf("worker dirty file = %q, %v", dirty, err)
	}
	if !leaveDirty && !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("clean worker result contains dirty file: %v", err)
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
	wantStatus := ""
	if leaveDirty {
		wantStatus = "?? nested/dirty-worker.txt"
	}
	if strings.TrimSpace(strings.ReplaceAll(status, "\r\n", "\n")) != wantStatus {
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
