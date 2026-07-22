package cli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/GhostFlying/delegation/internal/connector"
	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
	"github.com/GhostFlying/delegation/internal/workerhost"
)

const (
	operationTestTreeID      = "123e4567-e89b-42d3-a456-426614174250"
	operationTestAgentID     = "123e4567-e89b-42d3-a456-426614174251"
	operationTestParentID    = "123e4567-e89b-42d3-a456-426614174252"
	operationTestRootDevice  = "123e4567-e89b-42d3-a456-426614174253"
	operationTestMessageID   = "123e4567-e89b-42d3-a456-426614174254"
	operationTestFollowupID  = "123e4567-e89b-42d3-a456-426614174255"
	operationTestInterruptID = "123e4567-e89b-42d3-a456-426614174256"
	operationTestThreadID    = "123e4567-e89b-42d3-a456-426614174257"
)

type fakeManagedWorkerHost struct {
	mu sync.Mutex

	sendRequests      []workerhost.SendRequest
	followupRequests  []workerhost.FollowupRequest
	interruptRequests []workerhost.InterruptRequest

	sendResult      workerhost.OperationResult
	sendErr         error
	followupResult  workerhost.OperationResult
	followupErr     error
	interruptResult workerhost.OperationResult
	interruptErr    error
}

func (*fakeManagedWorkerHost) Spawn(
	context.Context,
	workerhost.SpawnRequest,
) (workerhost.StartedTurn, error) {
	return workerhost.StartedTurn{}, errors.New("unexpected spawn")
}

func (h *fakeManagedWorkerHost) Send(
	_ context.Context,
	request workerhost.SendRequest,
) (workerhost.OperationResult, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.sendRequests = append(h.sendRequests, request)
	return h.sendResult, h.sendErr
}

func (h *fakeManagedWorkerHost) Followup(
	_ context.Context,
	request workerhost.FollowupRequest,
) (workerhost.OperationResult, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.followupRequests = append(h.followupRequests, request)
	return h.followupResult, h.followupErr
}

func (h *fakeManagedWorkerHost) Interrupt(
	_ context.Context,
	request workerhost.InterruptRequest,
) (workerhost.OperationResult, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.interruptRequests = append(h.interruptRequests, request)
	return h.interruptResult, h.interruptErr
}

type staticManagedWorkerState struct {
	worker store.WorkerReservation
	err    error
}

func (s staticManagedWorkerState) GetWorker(
	_ context.Context,
	key store.WorkerKey,
) (store.WorkerReservation, error) {
	if s.err != nil {
		return store.WorkerReservation{}, s.err
	}
	if key != s.worker.WorkerKey {
		return store.WorkerReservation{}, store.ErrNotFound
	}
	return s.worker, nil
}

func TestManagedWorkerAdapterMapsDurableOperationReceipts(t *testing.T) {
	worker := operationTestWorker(t)
	host := &fakeManagedWorkerHost{
		sendResult: workerOperationHostResult(
			worker,
			operationTestMessageID,
			store.WorkerOperationSend,
			store.WorkerOutcomeQueued,
			"",
			[]byte("send"),
		),
		followupResult: workerOperationHostResult(
			worker,
			operationTestFollowupID,
			store.WorkerOperationFollowup,
			store.WorkerOutcomeStarted,
			"",
			[]byte("follow up"),
		),
		interruptResult: workerOperationHostResult(
			worker,
			operationTestInterruptID,
			store.WorkerOperationInterrupt,
			store.WorkerOutcomeInterrupted,
			"",
			nil,
		),
	}
	adapter := operationTestAdapter(host, worker)
	root := operationTestRoot()

	sent, err := adapter.SendWorker(context.Background(), connector.WorkerSendRequest{
		TreeID: operationTestTreeID,
		Source: root,
		Params: protocol.SendWorkerParams{
			AgentID: operationTestAgentID, MessageID: operationTestMessageID, Message: "send",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	followed, err := adapter.FollowupWorker(context.Background(), connector.WorkerFollowupRequest{
		TreeID: operationTestTreeID,
		Source: root,
		Params: protocol.FollowupWorkerParams{
			AgentID: operationTestAgentID, OperationID: operationTestFollowupID, Message: "follow up",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	interrupted, err := adapter.InterruptWorker(context.Background(), connector.WorkerInterruptRequest{
		TreeID: operationTestTreeID,
		Source: root,
		Params: protocol.InterruptWorkerParams{
			AgentID: operationTestAgentID, OperationID: operationTestInterruptID,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	wantSent := protocol.WorkerOperationResult{
		OperationID: operationTestMessageID,
		AgentID:     operationTestAgentID,
		Action:      protocol.AgentOperationSend,
		Outcome:     protocol.AgentOperationOutcomeQueued,
	}
	wantFollowed := protocol.WorkerOperationResult{
		OperationID: operationTestFollowupID,
		AgentID:     operationTestAgentID,
		Action:      protocol.AgentOperationFollowup,
		Outcome:     protocol.AgentOperationOutcomeStarted,
	}
	wantInterrupted := protocol.WorkerOperationResult{
		OperationID: operationTestInterruptID,
		AgentID:     operationTestAgentID,
		Action:      protocol.AgentOperationInterrupt,
		Outcome:     protocol.AgentOperationOutcomeInterrupted,
	}
	if sent != wantSent || followed != wantFollowed || interrupted != wantInterrupted {
		t.Fatalf("mapped results = %#v, %#v, %#v", sent, followed, interrupted)
	}
	host.mu.Lock()
	defer host.mu.Unlock()
	if !reflect.DeepEqual(host.sendRequests, []workerhost.SendRequest{{
		Key: worker.WorkerKey, MessageID: operationTestMessageID, Message: "send",
	}}) || !reflect.DeepEqual(host.followupRequests, []workerhost.FollowupRequest{{
		Key: worker.WorkerKey, OperationID: operationTestFollowupID, Message: "follow up",
	}}) || !reflect.DeepEqual(host.interruptRequests, []workerhost.InterruptRequest{{
		Key: worker.WorkerKey, OperationID: operationTestInterruptID,
	}}) {
		t.Fatalf(
			"worker host requests = %#v, %#v, %#v",
			host.sendRequests,
			host.followupRequests,
			host.interruptRequests,
		)
	}
}

func TestManagedWorkerAdapterPreservesTerminalFailureWithHostError(t *testing.T) {
	worker := operationTestWorker(t)
	hostFailure := errors.New("app-server rejected turn/steer")
	host := &fakeManagedWorkerHost{
		sendResult: workerOperationHostResult(
			worker,
			operationTestMessageID,
			store.WorkerOperationSend,
			store.WorkerOutcomeFailed,
			"app_server_rejected",
			[]byte("send"),
		),
		sendErr: hostFailure,
	}
	result, err := operationTestAdapter(host, worker).SendWorker(
		context.Background(),
		connector.WorkerSendRequest{
			TreeID: operationTestTreeID,
			Source: operationTestRoot(),
			Params: protocol.SendWorkerParams{
				AgentID: operationTestAgentID, MessageID: operationTestMessageID, Message: "send",
			},
		},
	)
	want := protocol.WorkerOperationResult{
		OperationID: operationTestMessageID,
		AgentID:     operationTestAgentID,
		Action:      protocol.AgentOperationSend,
		Outcome:     protocol.AgentOperationOutcomeFailed,
		FailureCode: "app_server_rejected",
	}
	if result != want || !errors.Is(err, hostFailure) {
		t.Fatalf("terminal failure = %#v, %v; want %#v, host error", result, err, want)
	}
}

func TestManagedWorkerAdapterRejectsMismatchedSourceBeforeHostCall(t *testing.T) {
	worker := operationTestWorker(t)
	host := &fakeManagedWorkerHost{}
	wrongRoot := operationTestRoot()
	wrongRoot.AgentID = operationTestFollowupID
	_, err := operationTestAdapter(host, worker).SendWorker(
		context.Background(),
		connector.WorkerSendRequest{
			TreeID: operationTestTreeID,
			Source: wrongRoot,
			Params: protocol.SendWorkerParams{
				AgentID: operationTestAgentID, MessageID: operationTestMessageID, Message: "blocked",
			},
		},
	)
	if err == nil || !strings.Contains(err.Error(), "reservation") {
		t.Fatalf("mismatched root error = %v", err)
	}
	host.mu.Lock()
	defer host.mu.Unlock()
	if len(host.sendRequests) != 0 {
		t.Fatalf("mismatched root reached host: %#v", host.sendRequests)
	}
}

func TestManagedWorkerAdapterRejectsMismatchedDurableReceipt(t *testing.T) {
	worker := operationTestWorker(t)
	hostFailure := errors.New("host failed after recording")
	host := &fakeManagedWorkerHost{
		sendResult: workerOperationHostResult(
			worker,
			operationTestMessageID,
			store.WorkerOperationSend,
			store.WorkerOutcomeFailed,
			"app_server_unavailable",
			[]byte("different payload"),
		),
		sendErr: hostFailure,
	}
	result, err := operationTestAdapter(host, worker).SendWorker(
		context.Background(),
		connector.WorkerSendRequest{
			TreeID: operationTestTreeID,
			Source: operationTestRoot(),
			Params: protocol.SendWorkerParams{
				AgentID: operationTestAgentID, MessageID: operationTestMessageID, Message: "send",
			},
		},
	)
	if result != (protocol.WorkerOperationResult{}) || !errors.Is(err, hostFailure) ||
		!strings.Contains(err.Error(), "payload digest") {
		t.Fatalf("mismatched receipt = %#v, %v", result, err)
	}
}

func operationTestAdapter(
	host managedWorkerHost,
	worker store.WorkerReservation,
) managedWorkerSpawner {
	return managedWorkerSpawner{
		host: host,
		state: staticManagedWorkerState{
			worker: worker,
		},
		controllerID: runtimeControllerID,
		deviceID:     runtimeDeviceID,
	}
}

func operationTestRoot() control.PrincipalIdentity {
	return control.NewRootPrincipal(
		runtimeControllerID,
		operationTestTreeID,
		operationTestParentID,
		operationTestRootDevice,
	).Identity()
}

func operationTestWorker(t *testing.T) store.WorkerReservation {
	t.Helper()
	return store.WorkerReservation{
		WorkerKey: store.WorkerKey{
			ControllerID: runtimeControllerID,
			TreeID:       operationTestTreeID,
			AgentID:      operationTestAgentID,
		},
		ParentAgentID:  operationTestParentID,
		DeviceID:       runtimeDeviceID,
		TaskName:       "operation adapter test",
		PromptDigest:   strings.Repeat("0", 64),
		WorkspacePath:  filepath.Join(t.TempDir(), "workspace"),
		ProfileVersion: 1,
		Status:         store.WorkerIdle,
		CodexThreadID:  operationTestThreadID,
		CreatedAt:      1,
		UpdatedAt:      2,
		Revision:       3,
	}
}

func workerOperationHostResult(
	worker store.WorkerReservation,
	operationID string,
	action store.WorkerOperationAction,
	outcome store.WorkerOperationOutcome,
	failureCode string,
	payload []byte,
) workerhost.OperationResult {
	status := store.WorkerOperationSucceeded
	if outcome == store.WorkerOutcomeFailed {
		status = store.WorkerOperationFailed
	}
	digest := sha256.Sum256(payload)
	return workerhost.OperationResult{
		Receipt: store.WorkerOperationReceipt{
			WorkerKey:     worker.WorkerKey,
			OperationID:   operationID,
			Action:        action,
			PayloadDigest: hex.EncodeToString(digest[:]),
			Status:        status,
			Outcome:       outcome,
			FailureCode:   failureCode,
			CreatedAt:     10,
			UpdatedAt:     11,
		},
		Worker: worker,
	}
}
