package connector

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/coder/websocket"
)

const (
	workspaceTransferTestWorkspaceID = "123e4567-e89b-42d3-a456-426614174210"
	workspaceTransferTestTransferID  = "123e4567-e89b-42d3-a456-426614174211"
	workspaceTransferTestOtherID     = "123e4567-e89b-42d3-a456-426614174212"
	workspaceTransferTestOtherDevice = "123e4567-e89b-42d3-a456-426614174213"
	workspaceTransferTestOtherTree   = "123e4567-e89b-42d3-a456-426614174214"
	workspaceTransferTestController  = "123e4567-e89b-42d3-a456-426614174215"
)

type workspaceTransferRPCFixture struct {
	manifest protocol.WorkspaceManifest
	transfer protocol.WorkspaceTransferManifest
	create   protocol.CreateWorkspaceTransferParams
	read     protocol.ReadWorkspaceArtifactParams
	begin    protocol.BeginWorkspaceTransferParams
	write    protocol.WriteWorkspaceArtifactParams
	control  protocol.WorkspaceTransferControlParams
}

type workspaceTransferRPCManager struct {
	testWorkerSpawner

	mu    sync.Mutex
	calls []string

	createResult protocol.CreateWorkspaceTransferResult
	readResult   protocol.ReadWorkspaceArtifactResult
	beginResult  protocol.BeginWorkspaceTransferResult
	writeResult  protocol.WriteWorkspaceArtifactResult
	finishResult protocol.FinishWorkspaceTransferResult
	cancelResult protocol.CancelWorkspaceTransferResult
}

type disconnectWorkspaceTransferManager struct {
	testWorkerSpawner
	started    chan struct{}
	finished   chan struct{}
	cleaned    chan struct{}
	startOnce  sync.Once
	finishOnce sync.Once
	cleanOnce  sync.Once
}

type failOnceCleanupManager struct {
	testWorkerSpawner
	cleanupCalls atomic.Int32
	firstFailed  chan struct{}
	retryStarted chan struct{}
	allowRetry   chan struct{}
	retryOnce    sync.Once
}

func (m *disconnectWorkspaceTransferManager) CreateWorkspaceTransfer(
	ctx context.Context,
	_ WorkspaceCreateTransferRequest,
) (protocol.CreateWorkspaceTransferResult, error) {
	m.startOnce.Do(func() { close(m.started) })
	<-ctx.Done()
	m.finishOnce.Do(func() { close(m.finished) })
	return protocol.CreateWorkspaceTransferResult{}, ctx.Err()
}

func (m *disconnectWorkspaceTransferManager) CleanupWorkspaceTransfers(context.Context) error {
	select {
	case <-m.finished:
	default:
		return errors.New("cleanup ran before active workspace RPC exited")
	}
	m.cleanOnce.Do(func() { close(m.cleaned) })
	return nil
}

func (m *failOnceCleanupManager) CleanupWorkspaceTransfers(ctx context.Context) error {
	switch m.cleanupCalls.Add(1) {
	case 1:
		close(m.firstFailed)
		return errors.New("transient workspace cleanup failure")
	default:
		m.retryOnce.Do(func() { close(m.retryStarted) })
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-m.allowRetry:
			return nil
		}
	}
}

func (m *workspaceTransferRPCManager) CreateWorkspaceTransfer(
	_ context.Context,
	_ WorkspaceCreateTransferRequest,
) (protocol.CreateWorkspaceTransferResult, error) {
	m.record("create")
	return m.createResult, nil
}

func (m *workspaceTransferRPCManager) ReadWorkspaceArtifact(
	_ context.Context,
	_ WorkspaceReadArtifactRequest,
) (protocol.ReadWorkspaceArtifactResult, error) {
	m.record("read")
	return m.readResult, nil
}

func (m *workspaceTransferRPCManager) BeginWorkspaceTransfer(
	_ context.Context,
	_ WorkspaceBeginTransferRequest,
) (protocol.BeginWorkspaceTransferResult, error) {
	m.record("begin")
	return m.beginResult, nil
}

func (m *workspaceTransferRPCManager) WriteWorkspaceArtifact(
	_ context.Context,
	_ WorkspaceWriteArtifactRequest,
) (protocol.WriteWorkspaceArtifactResult, error) {
	m.record("write")
	return m.writeResult, nil
}

func (m *workspaceTransferRPCManager) FinishWorkspaceTransfer(
	_ context.Context,
	_ WorkspaceTransferControlRequest,
) (protocol.FinishWorkspaceTransferResult, error) {
	m.record("finish")
	return m.finishResult, nil
}

func (m *workspaceTransferRPCManager) CancelWorkspaceTransfer(
	_ context.Context,
	_ WorkspaceTransferControlRequest,
) (protocol.CancelWorkspaceTransferResult, error) {
	m.record("cancel")
	return m.cancelResult, nil
}

func (m *workspaceTransferRPCManager) CleanupWorkspaceTransfers(context.Context) error { return nil }

func (m *workspaceTransferRPCManager) record(method string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, method)
}

func (m *workspaceTransferRPCManager) totalCalls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.calls)
}

func TestConnectorRejectsInvalidWorkspaceTransferAuthority(t *testing.T) {
	fixture := newWorkspaceTransferRPCFixture(t)
	remoteRoot := workerOperationRoot()
	remoteWorker := remoteRoot
	remoteWorker.ParentAgentID = connectorTestWorkerID
	workerBegin := fixture.begin
	workerBegin.SourceAgentID = remoteWorker.AgentID
	workerBegin.SourceDeviceID = remoteWorker.DeviceID
	workerControl := fixture.control
	workerControl.SourceAgentID = remoteWorker.AgentID
	workerControl.SourceDeviceID = remoteWorker.DeviceID
	mismatchedAgentControl := fixture.control
	mismatchedAgentControl.SourceAgentID = workspaceTransferTestOtherID
	mismatchedDeviceControl := fixture.control
	mismatchedDeviceControl.SourceDeviceID = workspaceTransferTestOtherDevice

	tests := []struct {
		name     string
		method   string
		source   control.PrincipalIdentity
		params   any
		wantCode int
	}{
		{
			name:   "create must originate from this peer",
			method: protocol.MethodCreateWorkspaceTransfer, source: remoteRoot, params: fixture.create,
			wantCode: protocol.ErrorInvalidRequest,
		},
		{
			name:   "read must originate from this peer",
			method: protocol.MethodReadWorkspaceArtifact, source: remoteRoot, params: fixture.read,
			wantCode: protocol.ErrorInvalidRequest,
		},
		{
			name:   "begin rejects a worker principal",
			method: protocol.MethodBeginWorkspaceTransfer, source: remoteWorker, params: workerBegin,
			wantCode: protocol.ErrorInvalidRequest,
		},
		{
			name:   "write rejects a worker principal",
			method: protocol.MethodWriteWorkspaceArtifact, source: remoteWorker, params: fixture.write,
			wantCode: protocol.ErrorInvalidRequest,
		},
		{
			name:   "finish binds the source agent",
			method: protocol.MethodFinishWorkspaceTransfer, source: remoteRoot, params: mismatchedAgentControl,
			wantCode: protocol.ErrorInvalidParams,
		},
		{
			name:   "cancel binds the source device",
			method: protocol.MethodCancelWorkspaceTransfer, source: remoteRoot, params: mismatchedDeviceControl,
			wantCode: protocol.ErrorInvalidParams,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			manager := &workspaceTransferRPCManager{}
			request := workerOperationEnvelope(t, test.method, test.source, test.params)
			response := runWorkspaceTransferRPC(t, manager, request)
			if response.Error == nil || response.Error.Code != test.wantCode {
				t.Fatalf("workspace transfer response = %#v, want code %d", response, test.wantCode)
			}
			if calls := manager.totalCalls(); calls != 0 {
				t.Fatalf("invalid request reached workspace transfer manager %d times", calls)
			}
		})
	}
}

func TestWorkspaceTransferRootEnvelopeAuthority(t *testing.T) {
	root := workerOperationRoot()
	request := protocol.Envelope{
		ControllerID: connectorTestControllerID,
		TreeID:       connectorTestThreadID,
		Source:       &root,
	}
	if err := validateBrokerWorkerRequest(request); err != nil {
		t.Fatal(err)
	}

	tests := map[string]func(*protocol.Envelope){
		"missing source": func(request *protocol.Envelope) {
			request.Source = nil
		},
		"worker source": func(request *protocol.Envelope) {
			request.Source.ParentAgentID = connectorTestWorkerID
		},
		"controller mismatch": func(request *protocol.Envelope) {
			request.Source.ControllerID = workspaceTransferTestController
		},
		"tree mismatch": func(request *protocol.Envelope) {
			request.Source.TreeID = workspaceTransferTestOtherTree
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			changed := request
			rootCopy := *request.Source
			changed.Source = &rootCopy
			mutate(&changed)
			if err := validateBrokerWorkerRequest(changed); err == nil {
				t.Fatal("invalid workspace transfer root envelope was accepted")
			}
		})
	}
}

func TestConnectorRejectsMismatchedWorkspaceTransferResults(t *testing.T) {
	fixture := newWorkspaceTransferRPCFixture(t)
	localRoot := control.NewRootPrincipal(
		connectorTestControllerID,
		connectorTestThreadID,
		connectorTestRootAgentID,
		connectorTestDeviceID,
	).Identity()
	remoteRoot := workerOperationRoot()
	ready := protocol.PrepareWorkspaceResult{
		WorkspaceID:  fixture.transfer.WorkspaceID,
		Outcome:      protocol.WorkspacePrepareReady,
		Strategy:     fixture.transfer.Strategy,
		ManifestHash: fixture.transfer.ManifestHash,
		Warnings:     fixture.transfer.Warnings,
	}

	tests := []struct {
		name      string
		method    string
		source    control.PrincipalIdentity
		params    any
		configure func(*workspaceTransferRPCManager)
	}{
		{
			name: "create transfer ID", method: protocol.MethodCreateWorkspaceTransfer,
			source: localRoot, params: fixture.create,
			configure: func(manager *workspaceTransferRPCManager) {
				transfer := fixture.transfer
				transfer.TransferID = workspaceTransferTestOtherID
				manager.createResult = protocol.CreateWorkspaceTransferResult{Transfer: transfer}
			},
		},
		{
			name: "create workspace ID", method: protocol.MethodCreateWorkspaceTransfer,
			source: localRoot, params: fixture.create,
			configure: func(manager *workspaceTransferRPCManager) {
				transfer := fixture.transfer
				transfer.WorkspaceID = workspaceTransferTestOtherID
				manager.createResult = protocol.CreateWorkspaceTransferResult{Transfer: transfer}
			},
		},
		{
			name: "read artifact kind", method: protocol.MethodReadWorkspaceArtifact,
			source: localRoot, params: fixture.read,
			configure: func(manager *workspaceTransferRPCManager) {
				manager.readResult = protocol.ReadWorkspaceArtifactResult{
					TransferID: fixture.read.TransferID, Kind: protocol.WorkspaceArtifactOverlay,
					Offset: fixture.read.Offset, Data: []byte{1, 2, 3}, NextOffset: 3,
				}
			},
		},
		{
			name: "read artifact offset", method: protocol.MethodReadWorkspaceArtifact,
			source: localRoot, params: fixture.read,
			configure: func(manager *workspaceTransferRPCManager) {
				manager.readResult = protocol.ReadWorkspaceArtifactResult{
					TransferID: fixture.read.TransferID, Kind: fixture.read.Kind,
					Offset: 1, Data: []byte{1, 2, 3}, NextOffset: 4,
				}
			},
		},
		{
			name: "read limit", method: protocol.MethodReadWorkspaceArtifact,
			source: localRoot,
			params: protocol.ReadWorkspaceArtifactParams{
				TransferID: fixture.read.TransferID, Kind: fixture.read.Kind, Limit: 2,
			},
			configure: func(manager *workspaceTransferRPCManager) {
				manager.readResult = protocol.ReadWorkspaceArtifactResult{
					TransferID: fixture.read.TransferID, Kind: fixture.read.Kind,
					Data: []byte{1, 2, 3}, NextOffset: 3,
				}
			},
		},
		{
			name: "begin transfer ID", method: protocol.MethodBeginWorkspaceTransfer,
			source: remoteRoot, params: fixture.begin,
			configure: func(manager *workspaceTransferRPCManager) {
				manager.beginResult = protocol.BeginWorkspaceTransferResult{TransferID: workspaceTransferTestOtherID}
			},
		},
		{
			name: "write transfer ID", method: protocol.MethodWriteWorkspaceArtifact,
			source: remoteRoot, params: fixture.write,
			configure: func(manager *workspaceTransferRPCManager) {
				manager.writeResult = protocol.WriteWorkspaceArtifactResult{
					TransferID: workspaceTransferTestOtherID,
					NextOffset: int64(len(fixture.write.Data)),
				}
			},
		},
		{
			name: "write next offset", method: protocol.MethodWriteWorkspaceArtifact,
			source: remoteRoot, params: fixture.write,
			configure: func(manager *workspaceTransferRPCManager) {
				manager.writeResult = protocol.WriteWorkspaceArtifactResult{
					TransferID: fixture.write.TransferID,
					NextOffset: int64(len(fixture.write.Data)) + 1,
				}
			},
		},
		{
			name: "finish workspace ID", method: protocol.MethodFinishWorkspaceTransfer,
			source: remoteRoot, params: fixture.control,
			configure: func(manager *workspaceTransferRPCManager) {
				mismatched := ready
				mismatched.WorkspaceID = workspaceTransferTestOtherID
				manager.finishResult = protocol.FinishWorkspaceTransferResult{Workspace: mismatched}
			},
		},
		{
			name: "cancel transfer ID", method: protocol.MethodCancelWorkspaceTransfer,
			source: remoteRoot, params: fixture.control,
			configure: func(manager *workspaceTransferRPCManager) {
				manager.cancelResult = protocol.CancelWorkspaceTransferResult{TransferID: workspaceTransferTestOtherID}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			manager := &workspaceTransferRPCManager{}
			test.configure(manager)
			request := workerOperationEnvelope(t, test.method, test.source, test.params)
			response := runWorkspaceTransferRPC(t, manager, request)
			if response.Error == nil || response.Error.Code != protocol.ErrorInternal {
				t.Fatalf("mismatched workspace transfer response = %#v", response)
			}
			if calls := manager.totalCalls(); calls != 1 {
				t.Fatalf("workspace transfer manager calls = %d, want 1", calls)
			}
		})
	}
}

type workspaceManagerWithoutTransfer struct{}

func (workspaceManagerWithoutTransfer) InspectWorkspace(
	context.Context,
	WorkspaceInspectRequest,
) (protocol.InspectWorkspaceResult, error) {
	return protocol.InspectWorkspaceResult{}, errors.New("not used")
}

func (workspaceManagerWithoutTransfer) PrepareWorkspace(
	context.Context,
	WorkspacePrepareRequest,
) (protocol.PrepareWorkspaceResult, error) {
	return protocol.PrepareWorkspaceResult{}, errors.New("not used")
}

func TestConnectorRequiresWorkspaceTransferImplementation(t *testing.T) {
	_, err := New(Options{
		BrokerURL: "ws://127.0.0.1:1", ControllerID: connectorTestControllerID,
		DeviceID: connectorTestDeviceID, DeviceName: "builder", AuthMode: config.AuthModeNone,
		RuntimeVersion: "test", OperatingSystem: "linux", Architecture: "amd64",
		WorkerSpawner: testWorkerSpawner{}, WorkerLifecycleSource: testWorkerSpawner{},
		WorkspaceManager: workspaceManagerWithoutTransfer{},
	})
	if err == nil || !strings.Contains(err.Error(), "workspace transfer manager is required") {
		t.Fatalf("connector without workspace transfer implementation error = %v", err)
	}
}

func TestConnectorDrainsWorkspaceRPCBeforeSessionCleanup(t *testing.T) {
	fixture := newWorkspaceTransferRPCFixture(t)
	localRoot := control.NewRootPrincipal(
		connectorTestControllerID, connectorTestThreadID, connectorTestRootAgentID, connectorTestDeviceID,
	).Identity()
	request := workerOperationEnvelope(t, protocol.MethodCreateWorkspaceTransfer, localRoot, fixture.create)
	manager := &disconnectWorkspaceTransferManager{
		started: make(chan struct{}), finished: make(chan struct{}), cleaned: make(chan struct{}),
	}
	server := newFakeBroker(t, func(connection *websocket.Conn) {
		writeTestEnvelope(t, connection, request)
		select {
		case <-manager.started:
		case <-time.After(2 * time.Second):
			t.Error("connector did not start workspace RPC")
		}
		_ = connection.CloseNow()
	})
	defer server.Close()
	client, err := New(Options{
		BrokerURL: websocketURL(server.URL), ControllerID: connectorTestControllerID,
		DeviceID: connectorTestDeviceID, DeviceName: "builder", AuthMode: config.AuthModeNone,
		RuntimeVersion: "test", OperatingSystem: "linux", Architecture: "amd64",
		WorkerSpawner: testWorkerSpawner{}, WorkerLifecycleSource: testWorkerSpawner{},
		WorkspaceManager: manager,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.runSession(context.Background()); err == nil {
		t.Fatal("disconnected connector session succeeded")
	}
	select {
	case <-manager.cleaned:
	default:
		t.Fatal("connector session did not clean workspace transfer state")
	}
}

func TestConnectorRequiresWorkspaceCleanupBeforeReconnecting(t *testing.T) {
	manager := &failOnceCleanupManager{
		firstFailed: make(chan struct{}), retryStarted: make(chan struct{}), allowRetry: make(chan struct{}),
	}
	connections := make(chan int32, 2)
	stopRecovered := make(chan struct{})
	var connectionCount atomic.Int32
	server := newFakeBroker(t, func(connection *websocket.Conn) {
		current := connectionCount.Add(1)
		connections <- current
		if current == 1 {
			_ = connection.CloseNow()
			return
		}
		<-stopRecovered
	})
	defer server.Close()
	client, err := New(Options{
		BrokerURL: websocketURL(server.URL), ControllerID: connectorTestControllerID,
		DeviceID: connectorTestDeviceID, DeviceName: "builder", AuthMode: config.AuthModeNone,
		RuntimeVersion: "test", OperatingSystem: "linux", Architecture: "amd64",
		ReconnectMin: 5 * time.Millisecond, ReconnectMax: 10 * time.Millisecond,
		WorkerSpawner: testWorkerSpawner{}, WorkerLifecycleSource: testWorkerSpawner{},
		WorkspaceManager: manager,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runClient(client, ctx)
	select {
	case first := <-connections:
		if first != 1 {
			t.Fatalf("first broker connection = %d", first)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("connector did not establish its first session")
	}
	select {
	case <-manager.firstFailed:
	case <-time.After(2 * time.Second):
		t.Fatal("connector did not attempt session cleanup")
	}
	select {
	case <-manager.retryStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("connector did not retry failed cleanup")
	}
	select {
	case reconnect := <-connections:
		t.Fatalf("connector reconnected before cleanup completed: connection %d", reconnect)
	case <-time.After(50 * time.Millisecond):
	}
	close(manager.allowRetry)
	select {
	case reconnect := <-connections:
		if reconnect != 2 {
			t.Fatalf("recovered broker connection = %d", reconnect)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("connector did not reconnect after cleanup completed")
	}
	waitReady(t, client)
	cancel()
	close(stopRecovered)
	if err := waitClient(done); err != nil {
		t.Fatal(err)
	}
}

func TestConnectorPreservesCleanupFenceAcrossRunCalls(t *testing.T) {
	manager := &failOnceCleanupManager{
		firstFailed: make(chan struct{}), retryStarted: make(chan struct{}), allowRetry: make(chan struct{}),
	}
	connections := make(chan int32, 2)
	stopRecovered := make(chan struct{})
	var connectionCount atomic.Int32
	server := newFakeBroker(t, func(connection *websocket.Conn) {
		current := connectionCount.Add(1)
		connections <- current
		if current == 1 {
			_ = connection.CloseNow()
			return
		}
		<-stopRecovered
	})
	defer server.Close()
	client, err := New(Options{
		BrokerURL: websocketURL(server.URL), ControllerID: connectorTestControllerID,
		DeviceID: connectorTestDeviceID, DeviceName: "builder", AuthMode: config.AuthModeNone,
		RuntimeVersion: "test", OperatingSystem: "linux", Architecture: "amd64",
		ReconnectMin: 100 * time.Millisecond, ReconnectMax: 100 * time.Millisecond,
		WorkerSpawner: testWorkerSpawner{}, WorkerLifecycleSource: testWorkerSpawner{},
		WorkspaceManager: manager,
	})
	if err != nil {
		t.Fatal(err)
	}
	firstContext, cancelFirst := context.WithCancel(context.Background())
	firstDone := runClient(client, firstContext)
	select {
	case first := <-connections:
		if first != 1 {
			t.Fatalf("first broker connection = %d", first)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("connector did not establish its first session")
	}
	select {
	case <-manager.firstFailed:
	case <-time.After(2 * time.Second):
		t.Fatal("connector did not record failed cleanup")
	}
	cancelFirst()
	if err := waitClient(firstDone); err != nil {
		t.Fatal(err)
	}
	secondContext, cancelSecond := context.WithCancel(context.Background())
	secondDone := runClient(client, secondContext)
	select {
	case <-manager.retryStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("second Run did not retry the retained cleanup fence")
	}
	select {
	case reconnect := <-connections:
		t.Fatalf("second Run connected before retained cleanup completed: connection %d", reconnect)
	case <-time.After(50 * time.Millisecond):
	}
	close(manager.allowRetry)
	select {
	case reconnect := <-connections:
		if reconnect != 2 {
			t.Fatalf("recovered broker connection = %d", reconnect)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("second Run did not connect after retained cleanup completed")
	}
	waitReady(t, client)
	cancelSecond()
	close(stopRecovered)
	if err := waitClient(secondDone); err != nil {
		t.Fatal(err)
	}
}

func newWorkspaceTransferRPCFixture(t *testing.T) workspaceTransferRPCFixture {
	t.Helper()
	manifest := protocol.WorkspaceManifest{
		GitURL: "ssh://example.invalid/repository", HeadOID: strings.Repeat("1", 40),
		ObjectFormat: "sha1", Clean: true, SourceSnapshotHash: strings.Repeat("2", 64),
		Warnings: []string{},
	}
	manifestHash, err := protocol.WorkspaceManifestHash(manifest)
	if err != nil {
		t.Fatal(err)
	}
	transfer := protocol.WorkspaceTransferManifest{
		TransferID: workspaceTransferTestTransferID, WorkspaceID: workspaceTransferTestWorkspaceID,
		Strategy: protocol.WorkspaceStrategyThin, ManifestHash: manifestHash,
		Artifacts: []protocol.WorkspaceArtifactDescriptor{{
			Kind: protocol.WorkspaceArtifactBundle, Size: 3, SHA256: strings.Repeat("a", 64),
		}},
		Warnings: []string{},
	}
	remoteRoot := workerOperationRoot()
	return workspaceTransferRPCFixture{
		manifest: manifest,
		transfer: transfer,
		create: protocol.CreateWorkspaceTransferParams{
			TransferID: transfer.TransferID, WorkspaceID: transfer.WorkspaceID,
			GitURL: manifest.GitURL, SourcePath: "/source", Manifest: manifest,
			BundleRequired: true,
		},
		read: protocol.ReadWorkspaceArtifactParams{
			TransferID: transfer.TransferID, Kind: protocol.WorkspaceArtifactBundle, Limit: 3,
		},
		begin: protocol.BeginWorkspaceTransferParams{
			SourceAgentID: remoteRoot.AgentID, SourceDeviceID: remoteRoot.DeviceID,
			Manifest: manifest, Transfer: transfer,
		},
		write: protocol.WriteWorkspaceArtifactParams{
			WorkspaceID: transfer.WorkspaceID, TransferID: transfer.TransferID,
			Kind: protocol.WorkspaceArtifactBundle, Data: []byte{1, 2, 3},
		},
		control: protocol.WorkspaceTransferControlParams{
			WorkspaceID: transfer.WorkspaceID, TransferID: transfer.TransferID,
			SourceAgentID: remoteRoot.AgentID, SourceDeviceID: remoteRoot.DeviceID,
		},
	}
}

func runWorkspaceTransferRPC(
	t *testing.T,
	manager *workspaceTransferRPCManager,
	request protocol.Envelope,
) protocol.Envelope {
	t.Helper()
	responses := make(chan protocol.Envelope, 1)
	stop := make(chan struct{})
	var stopOnce sync.Once
	stopServer := func() {
		stopOnce.Do(func() { close(stop) })
	}
	server := newFakeBroker(t, func(connection *websocket.Conn) {
		writeTestEnvelope(t, connection, request)
		responses <- readTestEnvelope(t, connection)
		<-stop
	})
	defer server.Close()
	defer stopServer()
	client, err := New(Options{
		BrokerURL: websocketURL(server.URL), ControllerID: connectorTestControllerID,
		DeviceID: connectorTestDeviceID, DeviceName: "builder", AuthMode: config.AuthModeNone,
		RuntimeVersion: "test", OperatingSystem: "linux", Architecture: "amd64",
		ReconnectMin: 5 * time.Millisecond, ReconnectMax: 10 * time.Millisecond,
		WorkerSpawner: testWorkerSpawner{}, WorkerLifecycleSource: testWorkerSpawner{},
		WorkspaceManager: manager,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runClient(client, ctx)
	waitReady(t, client)
	var response protocol.Envelope
	select {
	case response = <-responses:
	case <-time.After(2 * time.Second):
		t.Fatal("connector did not answer workspace transfer request")
	}
	cancel()
	stopServer()
	if err := waitClient(done); err != nil {
		t.Fatal(err)
	}
	return response
}
