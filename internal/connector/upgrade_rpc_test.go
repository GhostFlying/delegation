package connector

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/coder/websocket"
)

const (
	connectorUpgradeControllerTransactionID = "123e4567-e89b-42d3-a456-426614174291"
	connectorUpgradeTransactionID           = "123e4567-e89b-42d3-a456-426614174292"
)

type connectorUpgradeManager struct {
	mu       sync.Mutex
	calls    []string
	snapshot protocol.UpgradeSnapshot
	err      error
}

func (m *connectorUpgradeManager) PrepareCoordinatedUpgrade(
	_ context.Context, _ protocol.PrepareUpgradeParams,
) (protocol.UpgradeSnapshot, error) {
	return m.result("prepare")
}

func (m *connectorUpgradeManager) ArmCoordinatedUpgrade(
	_ context.Context, _ protocol.UpgradeTransactionParams,
) (protocol.UpgradeSnapshot, error) {
	return m.result("arm")
}

func (m *connectorUpgradeManager) ActivateCoordinatedUpgrade(
	_ context.Context, _ protocol.UpgradeTransactionParams,
) (protocol.UpgradeSnapshot, error) {
	return m.result("activate")
}

func (m *connectorUpgradeManager) CancelCoordinatedUpgrade(
	_ context.Context, _ protocol.UpgradeTransactionParams,
) (protocol.UpgradeSnapshot, error) {
	return m.result("cancel")
}

func (m *connectorUpgradeManager) CoordinatedUpgradeStatus(
	_ context.Context, _ protocol.UpgradeTransactionParams,
) (protocol.UpgradeSnapshot, error) {
	return m.result("status")
}

func (m *connectorUpgradeManager) result(operation string) (protocol.UpgradeSnapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, operation)
	return m.snapshot, m.err
}

func (m *connectorUpgradeManager) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.calls)
}

func TestConnectorDispatchesCoordinatedUpgradeRPCs(t *testing.T) {
	methods := []struct {
		method string
		params any
	}{
		{protocol.MethodPrepareUpgrade, protocol.PrepareUpgradeParams{
			ControllerTransactionID: connectorUpgradeControllerTransactionID, TargetVersion: "0.2.0",
		}},
		{protocol.MethodArmUpgrade, connectorUpgradeTransactionParams()},
		{protocol.MethodActivateUpgrade, connectorUpgradeTransactionParams()},
		{protocol.MethodCancelUpgrade, connectorUpgradeTransactionParams()},
		{protocol.MethodStatusUpgrade, connectorUpgradeTransactionParams()},
	}
	for _, test := range methods {
		t.Run(test.method, func(t *testing.T) {
			manager := &connectorUpgradeManager{snapshot: connectorUpgradeSnapshot()}
			response := runConnectorUpgradeRPC(t, manager, upgradeEnvelope(t, test.method, test.params))
			if response.Error != nil {
				t.Fatalf("upgrade response = %#v", response)
			}
			result, err := protocol.DecodePayload[protocol.UpgradeSnapshot](response.Payload)
			if err != nil || result.TransactionID != connectorUpgradeTransactionID {
				t.Fatalf("upgrade result = %#v, %v", result, err)
			}
			if manager.callCount() != 1 {
				t.Fatalf("upgrade manager calls = %d", manager.callCount())
			}
		})
	}
}

func TestConnectorRejectsUpgradeWorkerAuthorityAndMismatchedResult(t *testing.T) {
	manager := &connectorUpgradeManager{snapshot: connectorUpgradeSnapshot()}
	request := upgradeEnvelope(t, protocol.MethodPrepareUpgrade, protocol.PrepareUpgradeParams{
		ControllerTransactionID: connectorUpgradeControllerTransactionID, TargetVersion: "0.2.0",
	})
	request.TreeID = connectorTestThreadID
	source := workerOperationRoot()
	request.Source = &source
	response := runConnectorUpgradeRPC(t, manager, request)
	if response.Error == nil || response.Error.Code != protocol.ErrorInvalidRequest || manager.callCount() != 0 {
		t.Fatalf("authority response = %#v, calls = %d", response, manager.callCount())
	}

	manager.snapshot.ControllerTransactionID = "123e4567-e89b-42d3-a456-426614174293"
	response = runConnectorUpgradeRPC(
		t, manager, upgradeEnvelope(t, protocol.MethodStatusUpgrade, connectorUpgradeTransactionParams()),
	)
	if response.Error == nil || response.Error.Code != protocol.ErrorInternal {
		t.Fatalf("mismatched snapshot response = %#v", response)
	}
}

func TestConnectorUpgradeFailureIsBoundedAndDoesNotExposeManagerError(t *testing.T) {
	manager := &connectorUpgradeManager{snapshot: connectorUpgradeSnapshot(), err: errors.New("secret path /tmp/private")}
	response := runConnectorUpgradeRPC(
		t, manager, upgradeEnvelope(t, protocol.MethodArmUpgrade, connectorUpgradeTransactionParams()),
	)
	if response.Error == nil || response.Error.Code != protocol.ErrorUnavailable ||
		strings.Contains(response.Error.Message, "private") {
		t.Fatalf("upgrade failure response = %#v", response)
	}
}

func connectorUpgradeTransactionParams() protocol.UpgradeTransactionParams {
	return protocol.UpgradeTransactionParams{
		ControllerTransactionID: connectorUpgradeControllerTransactionID,
		TransactionID:           connectorUpgradeTransactionID,
	}
}

func connectorUpgradeSnapshot() protocol.UpgradeSnapshot {
	return protocol.UpgradeSnapshot{
		ControllerTransactionID: connectorUpgradeControllerTransactionID,
		TransactionID:           connectorUpgradeTransactionID, State: "prepared",
		SourceVersion: "0.1.0", TargetVersion: "0.2.0",
		TargetRuntimeDigest: strings.Repeat("a", 64), ConfigDigest: strings.Repeat("b", 64),
		SourceReadinessEpoch: 7, UpdatedAt: 1,
	}
}

func upgradeEnvelope(t *testing.T, method string, params any) protocol.Envelope {
	t.Helper()
	payload, err := json.Marshal(params)
	if err != nil {
		t.Fatal(err)
	}
	return protocol.Envelope{
		ProtocolVersion: protocol.Version, Kind: protocol.KindRequest,
		RequestID: testRequestID(t, protocol.DirectionBroker), Method: method,
		ControllerID: connectorTestControllerID, Payload: payload,
	}
}

func runConnectorUpgradeRPC(
	t *testing.T, manager UpgradeManager, request protocol.Envelope,
) protocol.Envelope {
	t.Helper()
	responses := make(chan protocol.Envelope, 1)
	stop := make(chan struct{})
	var once sync.Once
	stopServer := func() { once.Do(func() { close(stop) }) }
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
		RuntimeVersion: "0.1.0", OperatingSystem: "linux", Architecture: "amd64",
		ReconnectMin: 5 * time.Millisecond, ReconnectMax: 10 * time.Millisecond,
		WorkerSpawner: testWorkerSpawner{}, WorkerLifecycleSource: testWorkerSpawner{},
		WorkerReadinessSource: testWorkerSpawner{}, UpgradeManager: manager,
		ChangesArtifactSource: testWorkerSpawner{}, WorkspaceManager: testWorkerSpawner{},
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
		t.Fatal("connector did not answer coordinated upgrade request")
	}
	cancel()
	stopServer()
	if err := waitClient(done); err != nil {
		t.Fatal(err)
	}
	return response
}
