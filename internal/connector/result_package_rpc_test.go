package connector

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/coder/websocket"
)

const (
	resultPackageTestPackageID = "123e4567-e89b-42d3-a456-426614174220"
	resultPackageTestThreadID  = "123e4567-e89b-42d3-a456-426614174221"
	resultPackageTestTurnID    = "123e4567-e89b-42d3-a456-426614174222"
	resultPackageTestAttemptID = "123e4567-e89b-42d3-a456-426614174223"
)

type resultPackageRPCFixture struct {
	metadata protocol.ResultPackageMetadata
	read     protocol.ReadResultPackagePartParams
	begin    protocol.BeginResultPackageParams
	write    protocol.WriteResultPackagePartParams
	finish   protocol.FinishResultPackageParams
	cancel   protocol.CancelResultPackageParams
	ack      protocol.AcknowledgeResultPackageParams
}

type resultPackageRPCManager struct {
	mu    sync.Mutex
	calls []string

	readResult   protocol.ReadResultPackagePartResult
	beginResult  protocol.BeginResultPackageResult
	writeResult  protocol.WriteResultPackagePartResult
	finishResult protocol.FinishResultPackageResult
	cancelResult protocol.CancelResultPackageResult
	ackResult    protocol.AcknowledgeResultPackageResult
}

func (m *resultPackageRPCManager) ReadResultPackagePart(
	_ context.Context,
	_ ResultPackageReadRequest,
) (protocol.ReadResultPackagePartResult, error) {
	m.record("read")
	return m.readResult, nil
}

func (m *resultPackageRPCManager) BeginResultPackage(
	_ context.Context,
	_ ResultPackageBeginRequest,
) (protocol.BeginResultPackageResult, error) {
	m.record("begin")
	return m.beginResult, nil
}

func (m *resultPackageRPCManager) WriteResultPackagePart(
	_ context.Context,
	_ ResultPackageWriteRequest,
) (protocol.WriteResultPackagePartResult, error) {
	m.record("write")
	return m.writeResult, nil
}

func (m *resultPackageRPCManager) FinishResultPackage(
	_ context.Context,
	_ ResultPackageFinishRequest,
) (protocol.FinishResultPackageResult, error) {
	m.record("finish")
	return m.finishResult, nil
}

func (m *resultPackageRPCManager) CancelResultPackage(
	_ context.Context,
	_ ResultPackageCancelRequest,
) (protocol.CancelResultPackageResult, error) {
	m.record("cancel")
	return m.cancelResult, nil
}

func (m *resultPackageRPCManager) AcknowledgeResultPackage(
	_ context.Context,
	_ ResultPackageAcknowledgeRequest,
) (protocol.AcknowledgeResultPackageResult, error) {
	m.record("ack")
	return m.ackResult, nil
}

func (m *resultPackageRPCManager) record(method string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, method)
}

func (m *resultPackageRPCManager) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.calls)
}

func TestConnectorDispatchesResultPackageRPCs(t *testing.T) {
	fixture := newResultPackageRPCFixture(t)
	root := workerOperationRoot()
	worker := resultPackageWorker()
	tests := []struct {
		name   string
		method string
		source control.PrincipalIdentity
		params any
	}{
		{name: "read", method: protocol.MethodReadResultPackagePart, source: worker, params: fixture.read},
		{name: "begin", method: protocol.MethodBeginResultPackage, source: root, params: fixture.begin},
		{name: "write", method: protocol.MethodWriteResultPackagePart, source: root, params: fixture.write},
		{name: "finish", method: protocol.MethodFinishResultPackage, source: root, params: fixture.finish},
		{name: "cancel", method: protocol.MethodCancelResultPackage, source: root, params: fixture.cancel},
		{name: "ack", method: protocol.MethodAcknowledgeResultPackage, source: worker, params: fixture.ack},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			manager := resultPackageManagerForFixture(fixture)
			request := workerOperationEnvelope(t, test.method, test.source, test.params)
			response := runResultPackageRPC(t, manager, request)
			if response.Error != nil {
				t.Fatalf("result package response = %#v", response)
			}
			if manager.callCount() != 1 {
				t.Fatalf("result package manager calls = %d, want 1", manager.callCount())
			}
		})
	}
}

func TestConnectorAcceptsDurableChunkReplayOffsetBeyondRequestedChunk(t *testing.T) {
	fixture := newResultPackageRPCFixture(t)
	result := resultPackageManagerForFixture(fixture).writeResult
	result.NextOffset += 3
	if !validResultPackageWriteResponse(fixture.write, result) {
		t.Fatalf("durable chunk replay response was rejected: %#v", result)
	}
}

func TestConnectorRejectsWrongResultPackagePrincipalRole(t *testing.T) {
	fixture := newResultPackageRPCFixture(t)
	root := workerOperationRoot()
	worker := resultPackageWorker()
	tests := []struct {
		name   string
		method string
		source control.PrincipalIdentity
		params any
	}{
		{name: "root reads worker outbox", method: protocol.MethodReadResultPackagePart, source: root, params: fixture.read},
		{name: "worker begins root inbox", method: protocol.MethodBeginResultPackage, source: worker, params: fixture.begin},
		{name: "root acknowledges worker outbox", method: protocol.MethodAcknowledgeResultPackage, source: root, params: fixture.ack},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			manager := resultPackageManagerForFixture(fixture)
			request := workerOperationEnvelope(t, test.method, test.source, test.params)
			response := runResultPackageRPC(t, manager, request)
			if response.Error == nil || response.Error.Code != protocol.ErrorInvalidRequest {
				t.Fatalf("result package response = %#v, want invalid request", response)
			}
			if manager.callCount() != 0 {
				t.Fatal("invalid result package request reached the manager")
			}
		})
	}
}

func TestValidateBrokerResultPackageRequestBindsLocalPeerAndTree(t *testing.T) {
	worker := resultPackageWorker()
	request := protocol.Envelope{
		ControllerID: connectorTestControllerID,
		TreeID:       connectorTestThreadID,
		Source:       &worker,
	}
	if err := validateBrokerResultPackageRequest(
		request, connectorTestDeviceID, resultPackageSourceWorker,
	); err != nil {
		t.Fatal(err)
	}
	wrongDevice := request
	changed := worker
	changed.DeviceID = workspaceTransferTestOtherDevice
	wrongDevice.Source = &changed
	if err := validateBrokerResultPackageRequest(
		wrongDevice, connectorTestDeviceID, resultPackageSourceWorker,
	); err == nil {
		t.Fatal("result package request accepted a principal from another peer")
	}
	wrongTree := request
	changed = worker
	changed.TreeID = workspaceTransferTestOtherTree
	wrongTree.Source = &changed
	if err := validateBrokerResultPackageRequest(
		wrongTree, connectorTestDeviceID, resultPackageSourceWorker,
	); err == nil {
		t.Fatal("result package request accepted a principal from another tree")
	}
}

func newResultPackageRPCFixture(t *testing.T) resultPackageRPCFixture {
	t.Helper()
	raw := []byte("rollout")
	rawDigest := sha256.Sum256(raw)
	compressed := []byte{1, 2, 3}
	compressedDigest := sha256.Sum256(compressed)
	manifest := protocol.ResultManifest{
		Version: protocol.ResultManifestVersion, PackageID: resultPackageTestPackageID,
		ControllerID: connectorTestControllerID, TreeID: connectorTestThreadID,
		SourceAgentID: connectorTestWorkerID, SourceDeviceID: connectorTestDeviceID,
		ManagedThreadID: resultPackageTestThreadID, TurnID: resultPackageTestTurnID,
		LifecycleRevision: 7,
		Terminal:          protocol.ResultTerminal{Outcome: protocol.ResultTerminalCompleted},
		CapturedAt:        1_700_000_000,
		Rollout: protocol.ResultRolloutComponent{
			Status: protocol.ResultRolloutAvailable, RawSize: int64(len(raw)),
			RawSHA256: hex.EncodeToString(rawDigest[:]),
		},
		Workspace: protocol.ResultWorkspaceComponent{
			Status: protocol.ResultWorkspaceNotManaged, BaseWarnings: []string{}, ResultWarnings: []string{},
		},
		Parts: []protocol.ResultPackagePartDescriptor{{
			Kind: protocol.ResultPackagePartRollout, Size: int64(len(compressed)),
			SHA256: hex.EncodeToString(compressedDigest[:]),
		}},
	}
	manifestBytes, manifestDescriptor, err := protocol.EncodeResultManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	metadata := protocol.ResultPackageMetadata{
		Manifest: manifestBytes, ManifestDescriptor: manifestDescriptor,
	}
	return resultPackageRPCFixture{
		metadata: metadata,
		read: protocol.ReadResultPackagePartParams{
			PackageID: resultPackageTestPackageID, Kind: protocol.ResultPackagePartRollout, Limit: 3,
		},
		begin: protocol.BeginResultPackageParams{
			AttemptID: resultPackageTestAttemptID, PackageID: resultPackageTestPackageID,
			LeaseExpiresAt: 1_700_000_060, Metadata: metadata,
		},
		write: protocol.WriteResultPackagePartParams{
			AttemptID: resultPackageTestAttemptID, PackageID: resultPackageTestPackageID,
			Kind: protocol.ResultPackagePartRollout, Data: compressed,
		},
		finish: protocol.FinishResultPackageParams{
			AttemptID: resultPackageTestAttemptID, PackageID: resultPackageTestPackageID,
		},
		cancel: protocol.CancelResultPackageParams{
			AttemptID: resultPackageTestAttemptID, PackageID: resultPackageTestPackageID,
		},
		ack: protocol.AcknowledgeResultPackageParams{PackageID: resultPackageTestPackageID, Sequence: 9},
	}
}

func resultPackageManagerForFixture(fixture resultPackageRPCFixture) *resultPackageRPCManager {
	return &resultPackageRPCManager{
		readResult: protocol.ReadResultPackagePartResult{
			PackageID: fixture.read.PackageID, Kind: fixture.read.Kind,
			Offset: fixture.read.Offset, Data: []byte{1, 2, 3}, NextOffset: 3,
		},
		beginResult: protocol.BeginResultPackageResult{
			AttemptID: fixture.begin.AttemptID, PackageID: fixture.begin.PackageID,
			Outcome: protocol.ResultPackageReceiving,
			Offsets: []protocol.ResultPackagePartOffset{{Kind: protocol.ResultPackagePartRollout}},
		},
		writeResult: protocol.WriteResultPackagePartResult{
			AttemptID: fixture.write.AttemptID, PackageID: fixture.write.PackageID,
			Kind: fixture.write.Kind, NextOffset: int64(len(fixture.write.Data)),
		},
		finishResult: protocol.FinishResultPackageResult(fixture.finish),
		cancelResult: protocol.CancelResultPackageResult(fixture.cancel),
		ackResult:    protocol.AcknowledgeResultPackageResult(fixture.ack),
	}
}

func resultPackageWorker() control.PrincipalIdentity {
	return control.NewWorkerPrincipal(
		connectorTestControllerID,
		connectorTestThreadID,
		connectorTestWorkerID,
		connectorTestRootAgentID,
		connectorTestDeviceID,
	).Identity()
}

func runResultPackageRPC(
	t *testing.T,
	manager *resultPackageRPCManager,
	request protocol.Envelope,
) protocol.Envelope {
	t.Helper()
	responses := make(chan protocol.Envelope, 1)
	stop := make(chan struct{})
	var stopOnce sync.Once
	stopServer := func() { stopOnce.Do(func() { close(stop) }) }
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
		ChangesArtifactSource: testWorkerSpawner{},
		WorkspaceManager:      &workspaceTransferRPCManager{},
		ResultPackageManager:  manager,
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
		t.Fatal("connector did not answer result package request")
	}
	cancel()
	stopServer()
	if err := waitClient(done); err != nil {
		t.Fatal(err)
	}
	return response
}
