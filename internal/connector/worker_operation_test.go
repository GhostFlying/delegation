package connector

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/coder/websocket"
)

const (
	connectorTestRootAgentID = "123e4567-e89b-42d3-a456-426614174206"
	connectorTestRootDevice  = "123e4567-e89b-42d3-a456-426614174207"
	connectorTestFollowupID  = "123e4567-e89b-42d3-a456-426614174208"
	connectorTestInterruptID = "123e4567-e89b-42d3-a456-426614174209"
)

type workerOperationManager struct {
	mu sync.Mutex

	sends      []WorkerSendRequest
	followups  []WorkerFollowupRequest
	interrupts []WorkerInterruptRequest

	sendResult      protocol.WorkerOperationResult
	sendErr         error
	followupResult  protocol.WorkerOperationResult
	followupErr     error
	interruptResult protocol.WorkerOperationResult
	interruptErr    error
}

type testWorkerManager interface {
	WorkerSpawner
	WorkerController
}

func (m *workerOperationManager) SpawnWorker(
	_ context.Context,
	request WorkerSpawnRequest,
) (protocol.SpawnWorkerResult, error) {
	return protocol.SpawnWorkerResult{
		SpawnID: request.Params.SpawnID,
		Principal: control.NewWorkerPrincipal(
			connectorTestControllerID,
			request.TreeID,
			request.Params.AgentID,
			request.Source.AgentID,
			connectorTestDeviceID,
		).Identity(),
		Status: protocol.AgentSpawnStarted,
	}, nil
}

func (m *workerOperationManager) SendWorker(
	_ context.Context,
	request WorkerSendRequest,
) (protocol.WorkerOperationResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sends = append(m.sends, request)
	return m.sendResult, m.sendErr
}

func (m *workerOperationManager) FollowupWorker(
	_ context.Context,
	request WorkerFollowupRequest,
) (protocol.WorkerOperationResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.followups = append(m.followups, request)
	return m.followupResult, m.followupErr
}

func (m *workerOperationManager) InterruptWorker(
	_ context.Context,
	request WorkerInterruptRequest,
) (protocol.WorkerOperationResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.interrupts = append(m.interrupts, request)
	return m.interruptResult, m.interruptErr
}

func (m *workerOperationManager) snapshot() (
	[]WorkerSendRequest,
	[]WorkerFollowupRequest,
	[]WorkerInterruptRequest,
) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]WorkerSendRequest(nil), m.sends...),
		append([]WorkerFollowupRequest(nil), m.followups...),
		append([]WorkerInterruptRequest(nil), m.interrupts...)
}

func TestConnectorRoutesWorkerOperationsWithRootAuthority(t *testing.T) {
	manager := &workerOperationManager{
		sendResult: protocol.WorkerOperationResult{
			OperationID: connectorTestMessageID,
			AgentID:     connectorTestWorkerID,
			Action:      protocol.AgentOperationSend,
			Outcome:     protocol.AgentOperationOutcomeQueued,
		},
		followupResult: protocol.WorkerOperationResult{
			OperationID: connectorTestFollowupID,
			AgentID:     connectorTestWorkerID,
			Action:      protocol.AgentOperationFollowup,
			Outcome:     protocol.AgentOperationOutcomeStarted,
		},
		interruptResult: protocol.WorkerOperationResult{
			OperationID: connectorTestInterruptID,
			AgentID:     connectorTestWorkerID,
			Action:      protocol.AgentOperationInterrupt,
			Outcome:     protocol.AgentOperationOutcomeInterrupted,
		},
	}
	root := workerOperationRoot()
	requests := []protocol.Envelope{
		workerOperationEnvelope(t, protocol.MethodSendWorker, root, protocol.SendWorkerParams{
			AgentID: connectorTestWorkerID, MessageID: connectorTestMessageID, Message: "send",
		}),
		workerOperationEnvelope(t, protocol.MethodFollowupWorker, root, protocol.FollowupWorkerParams{
			AgentID: connectorTestWorkerID, OperationID: connectorTestFollowupID, Message: "follow up",
		}),
		workerOperationEnvelope(t, protocol.MethodInterruptWorker, root, protocol.InterruptWorkerParams{
			AgentID: connectorTestWorkerID, OperationID: connectorTestInterruptID,
		}),
	}
	want := []protocol.WorkerOperationResult{
		manager.sendResult,
		manager.followupResult,
		manager.interruptResult,
	}
	completed := make(chan struct{})
	stop := make(chan struct{})
	server := newFakeBroker(t, func(connection *websocket.Conn) {
		for index, request := range requests {
			writeTestEnvelope(t, connection, request)
			response := readTestEnvelope(t, connection)
			if response.Error != nil {
				t.Errorf("%s response error = %#v", request.Method, response.Error)
				continue
			}
			result, err := protocol.DecodePayload[protocol.WorkerOperationResult](response.Payload)
			if err != nil || result != want[index] || response.ReplyTo != request.RequestID {
				t.Errorf("%s response = %#v, error %v", request.Method, response, err)
			}
		}
		close(completed)
		<-stop
	})
	defer server.Close()
	client := newTestClientWithWorkerManager(t, websocketURL(server.URL), manager, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := runClient(client, ctx)
	waitReady(t, client)
	select {
	case <-completed:
	case <-time.After(2 * time.Second):
		t.Fatal("connector did not handle worker operations")
	}
	sends, followups, interrupts := manager.snapshot()
	if !reflect.DeepEqual(sends, []WorkerSendRequest{{
		TreeID: connectorTestThreadID,
		Source: root,
		Params: protocol.SendWorkerParams{
			AgentID: connectorTestWorkerID, MessageID: connectorTestMessageID, Message: "send",
		},
	}}) || !reflect.DeepEqual(followups, []WorkerFollowupRequest{{
		TreeID: connectorTestThreadID,
		Source: root,
		Params: protocol.FollowupWorkerParams{
			AgentID: connectorTestWorkerID, OperationID: connectorTestFollowupID, Message: "follow up",
		},
	}}) || !reflect.DeepEqual(interrupts, []WorkerInterruptRequest{{
		TreeID: connectorTestThreadID,
		Source: root,
		Params: protocol.InterruptWorkerParams{
			AgentID: connectorTestWorkerID, OperationID: connectorTestInterruptID,
		},
	}}) {
		t.Fatalf("worker operation requests = %#v, %#v, %#v", sends, followups, interrupts)
	}
	cancel()
	close(stop)
	if err := waitClient(done); err != nil {
		t.Fatal(err)
	}
}

func TestConnectorRejectsInvalidWorkerOperationAuthorityAndPayload(t *testing.T) {
	manager := &workerOperationManager{}
	root := workerOperationRoot()
	nonRoot := root
	nonRoot.ParentAgentID = connectorTestWorkerID
	requests := []protocol.Envelope{
		workerOperationEnvelope(t, protocol.MethodSendWorker, nonRoot, protocol.SendWorkerParams{
			AgentID: connectorTestWorkerID, MessageID: connectorTestMessageID, Message: "blocked",
		}),
		workerOperationEnvelope(t, protocol.MethodFollowupWorker, root, map[string]any{
			"agentId": "invalid", "operationId": connectorTestFollowupID, "message": "blocked",
		}),
	}
	wantCodes := []int{protocol.ErrorInvalidRequest, protocol.ErrorInvalidParams}
	completed := make(chan struct{})
	stop := make(chan struct{})
	server := newFakeBroker(t, func(connection *websocket.Conn) {
		for index, request := range requests {
			writeTestEnvelope(t, connection, request)
			response := readTestEnvelope(t, connection)
			if response.Error == nil || response.Error.Code != wantCodes[index] {
				t.Errorf("%s response = %#v", request.Method, response)
			}
		}
		close(completed)
		<-stop
	})
	defer server.Close()
	client := newTestClientWithWorkerManager(t, websocketURL(server.URL), manager, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := runClient(client, ctx)
	waitReady(t, client)
	select {
	case <-completed:
	case <-time.After(2 * time.Second):
		t.Fatal("connector did not reject invalid worker operations")
	}
	sends, followups, interrupts := manager.snapshot()
	if len(sends) != 0 || len(followups) != 0 || len(interrupts) != 0 {
		t.Fatalf("invalid requests reached worker manager: %#v, %#v, %#v", sends, followups, interrupts)
	}
	cancel()
	close(stop)
	if err := waitClient(done); err != nil {
		t.Fatal(err)
	}
}

func TestConnectorReturnsTerminalReceiptDespiteHostError(t *testing.T) {
	hostFailure := errors.New("host recorded durable failure")
	manager := &workerOperationManager{
		sendResult: protocol.WorkerOperationResult{
			OperationID: connectorTestMessageID,
			AgentID:     connectorTestWorkerID,
			Action:      protocol.AgentOperationSend,
			Outcome:     protocol.AgentOperationOutcomeFailed,
			FailureCode: "app_server_unavailable",
		},
		sendErr: hostFailure,
		followupResult: protocol.WorkerOperationResult{
			OperationID: connectorTestFollowupID,
			AgentID:     connectorTestWorkerID,
			Action:      protocol.AgentOperationFollowup,
			Outcome:     protocol.AgentOperationOutcomePending,
		},
		followupErr: errors.New("follow-up result is ambiguous"),
		interruptResult: protocol.WorkerOperationResult{
			OperationID: connectorTestInterruptID,
			AgentID:     connectorTestWorkerID,
			Action:      protocol.AgentOperationInterrupt,
			Outcome:     protocol.AgentOperationOutcomeInterrupted,
		},
	}
	root := workerOperationRoot()
	requests := []protocol.Envelope{
		workerOperationEnvelope(t, protocol.MethodSendWorker, root, protocol.SendWorkerParams{
			AgentID: connectorTestWorkerID, MessageID: connectorTestMessageID, Message: "fail durably",
		}),
		workerOperationEnvelope(t, protocol.MethodFollowupWorker, root, protocol.FollowupWorkerParams{
			AgentID: connectorTestWorkerID, OperationID: connectorTestFollowupID, Message: "ambiguous",
		}),
		workerOperationEnvelope(t, protocol.MethodInterruptWorker, root, protocol.InterruptWorkerParams{
			AgentID: connectorTestWorkerID, OperationID: connectorTestInterruptID,
		}),
	}
	completed := make(chan struct{})
	stop := make(chan struct{})
	server := newFakeBroker(t, func(connection *websocket.Conn) {
		writeTestEnvelope(t, connection, requests[0])
		failed := readTestEnvelope(t, connection)
		result, err := protocol.DecodePayload[protocol.WorkerOperationResult](failed.Payload)
		if err != nil || failed.Error != nil || result != manager.sendResult {
			t.Errorf("durable failure response = %#v, result %#v, error %v", failed, result, err)
		}

		writeTestEnvelope(t, connection, requests[1])
		ambiguous := readTestEnvelope(t, connection)
		if ambiguous.Error == nil || ambiguous.Error.Code != protocol.ErrorUnavailable {
			t.Errorf("ambiguous response = %#v", ambiguous)
		}

		writeTestEnvelope(t, connection, requests[2])
		interrupted := readTestEnvelope(t, connection)
		result, err = protocol.DecodePayload[protocol.WorkerOperationResult](interrupted.Payload)
		if err != nil || interrupted.Error != nil || result != manager.interruptResult {
			t.Errorf("post-error interrupt response = %#v, result %#v, error %v", interrupted, result, err)
		}
		close(completed)
		<-stop
	})
	defer server.Close()
	reported := make(chan error, 4)
	client := newTestClientWithWorkerManager(t, websocketURL(server.URL), manager, func(err error) {
		reported <- err
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := runClient(client, ctx)
	waitReady(t, client)
	select {
	case <-completed:
	case <-time.After(2 * time.Second):
		t.Fatal("connector did not finish durable-error worker operations")
	}
	var messages []string
	for len(messages) < 2 {
		select {
		case err := <-reported:
			messages = append(messages, err.Error())
		case <-time.After(time.Second):
			t.Fatalf("reported worker operation errors = %v", messages)
		}
	}
	if !strings.Contains(messages[0], hostFailure.Error()) ||
		!strings.Contains(messages[1], "ambiguous") || !client.Status().Connected {
		t.Fatalf("reported errors = %v, status = %#v", messages, client.Status())
	}
	cancel()
	close(stop)
	if err := waitClient(done); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerOperationResultValidationBindsRequestIdentity(t *testing.T) {
	valid := protocol.WorkerOperationResult{
		OperationID: connectorTestMessageID,
		AgentID:     connectorTestWorkerID,
		Action:      protocol.AgentOperationSend,
		Outcome:     protocol.AgentOperationOutcomeFailed,
		FailureCode: "worker_failed",
	}
	if err := validateWorkerOperationResult(
		valid,
		connectorTestMessageID,
		connectorTestWorkerID,
		protocol.AgentOperationSend,
	); err != nil {
		t.Fatal(err)
	}
	for name, result := range map[string]protocol.WorkerOperationResult{
		"operation": func() protocol.WorkerOperationResult {
			changed := valid
			changed.OperationID = connectorTestFollowupID
			return changed
		}(),
		"agent": func() protocol.WorkerOperationResult {
			changed := valid
			changed.AgentID = connectorTestRootAgentID
			return changed
		}(),
		"action": func() protocol.WorkerOperationResult {
			changed := valid
			changed.Action = protocol.AgentOperationFollowup
			return changed
		}(),
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateWorkerOperationResult(
				result,
				connectorTestMessageID,
				connectorTestWorkerID,
				protocol.AgentOperationSend,
			); err == nil {
				t.Fatal("mismatched worker result was accepted")
			}
		})
	}
}

func workerOperationRoot() control.PrincipalIdentity {
	return control.NewRootPrincipal(
		connectorTestControllerID,
		connectorTestThreadID,
		connectorTestRootAgentID,
		connectorTestRootDevice,
	).Identity()
}

func workerOperationEnvelope(
	t *testing.T,
	method string,
	source control.PrincipalIdentity,
	params any,
) protocol.Envelope {
	t.Helper()
	payload, err := json.Marshal(params)
	if err != nil {
		t.Fatal(err)
	}
	return protocol.Envelope{
		ProtocolVersion: protocol.Version,
		Kind:            protocol.KindRequest,
		RequestID:       testRequestID(t, protocol.DirectionBroker),
		Method:          method,
		ControllerID:    connectorTestControllerID,
		TreeID:          connectorTestThreadID,
		Source:          &source,
		Payload:         payload,
	}
}

func newTestClientWithWorkerManager(
	t *testing.T,
	brokerURL string,
	manager testWorkerManager,
	reportError func(error),
) *Client {
	t.Helper()
	client, err := New(Options{
		BrokerURL: brokerURL, ControllerID: connectorTestControllerID, DeviceID: connectorTestDeviceID,
		DeviceName: "builder", AuthMode: config.AuthModeNone,
		RuntimeVersion: "0.1.0-alpha.0.m1.1", OperatingSystem: "linux", Architecture: "amd64",
		ReconnectMin: 5 * time.Millisecond, ReconnectMax: 10 * time.Millisecond,
		WorkerSpawner: manager, WorkerController: manager, ReportError: reportError,
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}
