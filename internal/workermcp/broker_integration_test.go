package workermcp

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/broker"
	"github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/connector"
	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/identity"
	"github.com/GhostFlying/delegation/internal/localbridge"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type observedMailboxRegistry struct {
	*store.Store

	workerAgentID string
	emptyRead     chan struct{}
	emptyReadOnce sync.Once
}

func (r *observedMailboxRegistry) ReadMailbox(
	ctx context.Context,
	recipient control.Principal,
	cursor uint64,
	limit int,
) (protocol.WaitMailboxResult, error) {
	result, err := r.Store.ReadMailbox(ctx, recipient, cursor, limit)
	if err == nil && recipient.AgentID == r.workerAgentID && len(result.Messages) == 0 {
		r.emptyReadOnce.Do(func() { close(r.emptyRead) })
	}
	return result, err
}

type exactWorkerAuthorizer struct {
	expected control.PrincipalIdentity
}

func (a exactWorkerAuthorizer) ManagedWorkerThread(
	context.Context,
	string,
	string,
) (bool, error) {
	return false, nil
}

func (a exactWorkerAuthorizer) AuthorizeWorker(
	_ context.Context,
	claimed control.PrincipalIdentity,
) error {
	if claimed != a.expected {
		return errors.New("worker principal does not match the reservation")
	}
	return nil
}

type loseFirstSendResponseBackend struct {
	Backend
	mu   sync.Mutex
	lost bool
}

func (b *loseFirstSendResponseBackend) Call(
	ctx context.Context,
	method, treeID string,
	source *control.PrincipalIdentity,
	params, result any,
) error {
	b.mu.Lock()
	lose := method == protocol.MethodSendMessage && !b.lost
	if lose {
		b.lost = true
	}
	b.mu.Unlock()
	if err := b.Backend.Call(ctx, method, treeID, source, params, result); err != nil {
		return err
	}
	if lose {
		return connector.ErrUnavailable
	}
	return nil
}

type toolCallOutcome struct {
	result *mcp.CallToolResult
	err    error
}

type integrationWorkerSpawner struct{}

type integrationWorkerController struct{}

type integrationWorkerLifecycleSource struct{}

func (integrationWorkerLifecycleSource) WorkerRevision() uint64 { return 0 }

func (integrationWorkerLifecycleSource) WorkerLifecycleChanges() <-chan struct{} { return nil }

func (integrationWorkerLifecycleSource) ListWorkerLifecycles(
	context.Context,
) ([]protocol.WorkerLifecycleSnapshot, error) {
	return []protocol.WorkerLifecycleSnapshot{}, nil
}

func (integrationWorkerSpawner) SpawnWorker(
	context.Context,
	connector.WorkerSpawnRequest,
) (protocol.SpawnWorkerResult, error) {
	return protocol.SpawnWorkerResult{}, errors.New("worker spawning is outside this mailbox test")
}

func (integrationWorkerController) SendWorker(
	_ context.Context,
	request connector.WorkerSendRequest,
) (protocol.WorkerOperationResult, error) {
	return protocol.WorkerOperationResult{
		OperationID: request.Params.MessageID,
		AgentID:     request.Params.AgentID,
		Action:      protocol.AgentOperationSend,
		Outcome:     protocol.AgentOperationOutcomeQueued,
	}, nil
}

func (integrationWorkerController) FollowupWorker(
	_ context.Context,
	request connector.WorkerFollowupRequest,
) (protocol.WorkerOperationResult, error) {
	return protocol.WorkerOperationResult{
		OperationID: request.Params.OperationID,
		AgentID:     request.Params.AgentID,
		Action:      protocol.AgentOperationFollowup,
		Outcome:     protocol.AgentOperationOutcomeStarted,
	}, nil
}

func (integrationWorkerController) InterruptWorker(
	_ context.Context,
	request connector.WorkerInterruptRequest,
) (protocol.WorkerOperationResult, error) {
	return protocol.WorkerOperationResult{
		OperationID: request.Params.OperationID,
		AgentID:     request.Params.AgentID,
		Action:      protocol.AgentOperationInterrupt,
		Outcome:     protocol.AgentOperationOutcomeInterrupted,
	}, nil
}

func TestWorkerMCPMailboxThroughRealBrokerAndConnector(t *testing.T) {
	controllerID := newIntegrationID(t)
	deviceID := newIntegrationID(t)
	externalThreadID := newIntegrationID(t)
	workerAgentID := newIntegrationID(t)
	workerMessageID := newIntegrationID(t)

	registryStore, err := store.Open(
		context.Background(), filepath.Join(t.TempDir(), "broker", "state.sqlite3"),
	)
	if err != nil {
		t.Fatal(err)
	}
	registry := &observedMailboxRegistry{
		Store: registryStore, workerAgentID: workerAgentID, emptyRead: make(chan struct{}),
	}
	brokerServer, err := broker.New(broker.Options{
		ControllerID:      controllerID,
		AuthMode:          config.AuthModeNone,
		Registry:          registry,
		HeartbeatInterval: 100 * time.Millisecond,
	})
	if err != nil {
		registryStore.Close()
		t.Fatal(err)
	}
	if _, err := brokerServer.Prepare(context.Background()); err != nil {
		registryStore.Close()
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(brokerServer.Handler())

	runContext, cancelRun := context.WithCancel(context.Background())
	connectorClient, err := connector.New(connector.Options{
		BrokerURL:             "ws" + strings.TrimPrefix(httpServer.URL, "http"),
		ControllerID:          controllerID,
		DeviceID:              deviceID,
		DeviceName:            "mailbox-integration-peer",
		AuthMode:              config.AuthModeNone,
		RuntimeVersion:        "mailbox-integration-test",
		OperatingSystem:       "linux",
		Architecture:          "amd64",
		ReconnectMin:          5 * time.Millisecond,
		ReconnectMax:          10 * time.Millisecond,
		WorkerSpawner:         integrationWorkerSpawner{},
		WorkerController:      integrationWorkerController{},
		WorkerLifecycleSource: integrationWorkerLifecycleSource{},
	})
	if err != nil {
		cancelRun()
		httpServer.Close()
		closeBrokerIntegration(t, brokerServer, registryStore)
		t.Fatal(err)
	}
	connectorDone := make(chan error, 1)
	go func() { connectorDone <- connectorClient.Run(runContext) }()

	var bridge *localbridge.Server
	var bridgeDone chan error
	t.Cleanup(func() {
		cancelRun()
		if bridge != nil {
			if err := bridge.Close(); err != nil {
				t.Errorf("close local bridge: %v", err)
			}
		}
		if bridgeDone != nil {
			if err := <-bridgeDone; err != nil {
				t.Errorf("serve local bridge: %v", err)
			}
		}
		if err := <-connectorDone; err != nil {
			t.Errorf("run connector: %v", err)
		}
		httpServer.Close()
		closeBrokerIntegration(t, brokerServer, registryStore)
	})

	operationContext, cancelOperations := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelOperations()
	if err := connectorClient.WaitReady(operationContext); err != nil {
		t.Fatal(err)
	}
	var ensured protocol.EnsureRootTreeResult
	if err := connectorClient.Call(
		operationContext,
		protocol.MethodEnsureRootTree,
		"",
		nil,
		protocol.EnsureRootTreeParams{ExternalThreadID: externalThreadID},
		&ensured,
	); err != nil {
		t.Fatal(err)
	}
	worker, err := registryStore.CreateWorkerPrincipal(
		operationContext,
		controllerID,
		ensured.Tree.TreeID,
		workerAgentID,
		ensured.Principal.AgentID,
		deviceID,
		time.Now(),
	)
	if err != nil {
		t.Fatal(err)
	}

	endpoint, err := localbridge.Endpoint(controllerID, deviceID)
	if err != nil {
		t.Fatal(err)
	}
	bridge, err = localbridge.ListenWithAuthorization(
		endpoint,
		localbridge.ServiceIdentity{ControllerID: controllerID, DeviceID: deviceID},
		connectorClient,
		exactWorkerAuthorizer{expected: worker.Identity()},
	)
	if err != nil {
		t.Fatal(err)
	}
	bridgeDone = make(chan error, 1)
	go func() { bridgeDone <- bridge.Serve(runContext) }()
	bridgeClient, err := localbridge.NewClient(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	workerServer, err := NewServer(
		&loseFirstSendResponseBackend{Backend: bridgeClient},
		worker.Identity(),
	)
	if err != nil {
		t.Fatal(err)
	}
	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	serverSession, err := workerServer.Connect(operationContext, serverTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	client := mcp.NewClient(&mcp.Implementation{Name: "mailbox-integration", Version: "1"}, nil)
	clientSession, err := client.Connect(operationContext, clientTransport, nil)
	if err != nil {
		serverSession.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = clientSession.Close()
		_ = serverSession.Close()
	})

	waitDone := make(chan toolCallOutcome, 1)
	go func() {
		result, callErr := clientSession.CallTool(operationContext, &mcp.CallToolParams{
			Name: ToolWaitAgent,
			Arguments: map[string]any{
				"timeoutSeconds": 5,
			},
		})
		waitDone <- toolCallOutcome{result: result, err: callErr}
	}()
	select {
	case <-registry.emptyRead:
	case <-operationContext.Done():
		t.Fatal("worker mailbox wait did not reach the broker")
	}

	rootIdentity := ensured.Principal.Identity()
	var rootReceipt protocol.SendMessageResult
	if err := connectorClient.Call(
		operationContext,
		protocol.MethodSendMessage,
		ensured.Tree.TreeID,
		&rootIdentity,
		protocol.SendMessageParams{
			MessageID: "123e4567-e89b-42d3-a456-42661417470a",
			Target: protocol.MessageTarget{
				Kind:    protocol.MessageTargetAgent,
				AgentID: worker.AgentID,
			},
			Message: "root through shared connector",
		},
		&rootReceipt,
	); err != nil {
		t.Fatal(err)
	}
	if rootReceipt.Sequence != 1 {
		t.Fatalf("root send receipt = %#v", rootReceipt)
	}

	waitOutcome := <-waitDone
	if waitOutcome.err != nil {
		t.Fatal(waitOutcome.err)
	}
	if waitOutcome.result.IsError {
		t.Fatalf("worker wait tool error = %#v", waitOutcome.result.Content)
	}
	workerDelivery := decodeStructuredResult[WaitAgentOutput](t, waitOutcome.result)
	if len(workerDelivery.Messages) != 1 || workerDelivery.NextCursor != 1 ||
		workerDelivery.Messages[0].Source != rootIdentity ||
		workerDelivery.Messages[0].Message != "root through shared connector" {
		t.Fatalf("worker MCP mailbox delivery = %#v", workerDelivery)
	}

	sendArguments := map[string]any{
		"messageId": workerMessageID,
		"recipient": "parent",
		"message":   "worker through local bridge",
	}
	lostResult, err := clientSession.CallTool(operationContext, &mcp.CallToolParams{
		Name:      ToolSendMessage,
		Arguments: sendArguments,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !lostResult.IsError || !toolResultContains(lostResult, workerMessageID) {
		t.Fatalf("lost worker send response = %#v", lostResult)
	}
	sendResult, err := clientSession.CallTool(operationContext, &mcp.CallToolParams{
		Name:      ToolSendMessage,
		Arguments: sendArguments,
	})
	if err != nil {
		t.Fatal(err)
	}
	if sendResult.IsError {
		t.Fatalf("worker send tool error = %#v", sendResult.Content)
	}
	workerReceipt := decodeStructuredResult[SendMessageOutput](t, sendResult)
	if workerReceipt.MessageID != workerMessageID || workerReceipt.Sequence != 1 {
		t.Fatalf("worker send receipt = %#v", workerReceipt)
	}

	var rootDelivery protocol.WaitMailboxResult
	if err := connectorClient.Call(
		operationContext,
		protocol.MethodWaitMailbox,
		ensured.Tree.TreeID,
		&rootIdentity,
		protocol.WaitMailboxParams{TimeoutMillis: 0, Limit: 1},
		&rootDelivery,
	); err != nil {
		t.Fatal(err)
	}
	if len(rootDelivery.Messages) != 1 || rootDelivery.NextCursor != 1 ||
		rootDelivery.Messages[0].MessageID != workerMessageID ||
		rootDelivery.Messages[0].Source != worker.Identity() ||
		rootDelivery.Messages[0].Message != "worker through local bridge" {
		t.Fatalf("root mailbox delivery = %#v", rootDelivery)
	}
	var duplicate protocol.WaitMailboxResult
	if err := connectorClient.Call(
		operationContext,
		protocol.MethodWaitMailbox,
		ensured.Tree.TreeID,
		&rootIdentity,
		protocol.WaitMailboxParams{Cursor: rootDelivery.NextCursor, TimeoutMillis: 0, Limit: 1},
		&duplicate,
	); err != nil {
		t.Fatal(err)
	}
	if len(duplicate.Messages) != 0 || duplicate.NextCursor != rootDelivery.NextCursor {
		t.Fatalf("same messageId retry created another mailbox entry: %#v", duplicate)
	}
}

func newIntegrationID(t *testing.T) string {
	t.Helper()
	value, err := identity.NewID()
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func decodeStructuredResult[T any](t *testing.T, result *mcp.CallToolResult) T {
	t.Helper()
	var value T
	data, err := json.Marshal(result.StructuredContent)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &value); err != nil {
		t.Fatal(err)
	}
	return value
}

func toolResultContains(result *mcp.CallToolResult, value string) bool {
	data, err := json.Marshal(result.Content)
	return err == nil && strings.Contains(string(data), value)
}

func closeBrokerIntegration(t *testing.T, server *broker.Server, registry *store.Store) {
	t.Helper()
	closeContext, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := server.Close(closeContext); err != nil {
		t.Errorf("close broker: %v", err)
	}
	if err := registry.Close(); err != nil {
		t.Errorf("close broker store: %v", err)
	}
}
