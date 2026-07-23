package localbridge

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"net"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/connector"
	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/identity"
	"github.com/GhostFlying/delegation/internal/protocol"
)

const (
	bridgeTestControllerID = "123e4567-e89b-42d3-a456-426614174300"
	bridgeTestDeviceID     = "123e4567-e89b-42d3-a456-426614174301"
	bridgeTestTreeID       = "123e4567-e89b-42d3-a456-426614174302"
	bridgeTestAgentID      = "123e4567-e89b-42d3-a456-426614174303"
)

type recordedCall struct {
	method  string
	treeID  string
	source  *control.PrincipalIdentity
	payload json.RawMessage
}

type fakeBackend struct {
	mu       sync.Mutex
	calls    []recordedCall
	result   json.RawMessage
	err      error
	block    bool
	started  chan struct{}
	finished chan struct{}
}

type fakeWorkerAuthorizer struct {
	mu          sync.Mutex
	sources     []control.PrincipalIdentity
	rootThreads []string
	managed     bool
	rootErr     error
	err         error
}

func (a *fakeWorkerAuthorizer) ManagedWorkerThread(
	_ context.Context,
	controllerID, externalThreadID string,
) (bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.rootThreads = append(a.rootThreads, controllerID+"/"+externalThreadID)
	return a.managed, a.rootErr
}

func (a *fakeWorkerAuthorizer) AuthorizeWorker(
	_ context.Context,
	source control.PrincipalIdentity,
) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.sources = append(a.sources, source)
	return a.err
}

func (b *fakeBackend) Call(
	ctx context.Context,
	method, treeID string,
	source *control.PrincipalIdentity,
	params, result any,
) error {
	payload := append(json.RawMessage(nil), params.(json.RawMessage)...)
	call := recordedCall{method: method, treeID: treeID, payload: payload}
	if source != nil {
		copy := *source
		call.source = &copy
	}
	b.mu.Lock()
	b.calls = append(b.calls, call)
	err := b.err
	response := append(json.RawMessage(nil), b.result...)
	block := b.block
	b.mu.Unlock()
	if block {
		close(b.started)
		<-ctx.Done()
		close(b.finished)
		return ctx.Err()
	}
	if err != nil {
		return err
	}
	*result.(*json.RawMessage) = response
	return nil
}

func (b *fakeBackend) snapshot() []recordedCall {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]recordedCall(nil), b.calls...)
}

func TestControllerBridgeForwardsAllowedCalls(t *testing.T) {
	backend := &fakeBackend{result: json.RawMessage(`{"ok":true}`)}
	client, stop := startTestBridge(t, backend)
	defer stop()
	type result struct {
		OK bool `json:"ok"`
	}
	var root result
	if err := client.Call(
		context.Background(),
		protocol.MethodEnsureRootTree,
		"",
		nil,
		protocol.EnsureRootTreeParams{ExternalThreadID: bridgeTestTreeID},
		&root,
	); err != nil {
		t.Fatal(err)
	}
	principal := control.NewRootPrincipal(
		bridgeTestControllerID, bridgeTestTreeID, bridgeTestAgentID, bridgeTestDeviceID,
	)
	source := principal.Identity()
	var list result
	if err := client.Call(
		context.Background(),
		protocol.MethodListDevices,
		bridgeTestTreeID,
		&source,
		protocol.ListDevicesParams{Limit: 10},
		&list,
	); err != nil {
		t.Fatal(err)
	}
	var spawned result
	if err := client.Call(
		context.Background(), protocol.MethodSpawnAgent, bridgeTestTreeID, &source,
		protocol.SpawnAgentParams{
			SpawnID:        bridgeTestTreeID,
			TargetDeviceID: bridgeTestDeviceID,
			TaskName:       "local_build",
			Message:        "run the local build",
		},
		&spawned,
	); err != nil {
		t.Fatal(err)
	}
	var agents result
	if err := client.Call(
		context.Background(), protocol.MethodListAgents, bridgeTestTreeID, &source,
		protocol.ListAgentsParams{Limit: 10}, &agents,
	); err != nil {
		t.Fatal(err)
	}
	if !root.OK || !list.OK || !spawned.OK || !agents.OK {
		t.Fatalf("bridge results = %#v, %#v, %#v, %#v", root, list, spawned, agents)
	}
	calls := backend.snapshot()
	if len(calls) != 4 || calls[0].method != protocol.MethodEnsureRootTree ||
		calls[0].treeID != "" || calls[0].source != nil ||
		calls[1].method != protocol.MethodListDevices || calls[1].treeID != bridgeTestTreeID ||
		calls[1].source == nil || *calls[1].source != source ||
		calls[2].method != protocol.MethodSpawnAgent || calls[3].method != protocol.MethodListAgents {
		t.Fatalf("forwarded calls = %#v", calls)
	}
}

func TestBridgeProbeRequiresExactServiceIdentity(t *testing.T) {
	backend := &fakeBackend{result: json.RawMessage(`{}`)}
	client, stop := startTestBridge(t, backend)
	defer stop()
	expected := testServiceIdentity()
	if err := Probe(context.Background(), client.endpoint, expected); err != nil {
		t.Fatal(err)
	}
	wrong := expected
	wrong.ControllerID = "123e4567-e89b-42d3-a456-426614174399"
	if err := Probe(context.Background(), client.endpoint, wrong); err == nil {
		t.Fatal("Probe accepted a bridge from another controller")
	}
	if calls := backend.snapshot(); len(calls) != 0 {
		t.Fatalf("identity probes reached broker backend: %#v", calls)
	}
}

func TestBridgeForwardsRootAgentControlsAndRejectsWorkers(t *testing.T) {
	backend := &fakeBackend{result: json.RawMessage(`{"ok":true}`)}
	client, stop := startTestBridge(t, backend)
	defer stop()
	root := control.NewRootPrincipal(
		bridgeTestControllerID, bridgeTestTreeID, bridgeTestAgentID, bridgeTestDeviceID,
	).Identity()
	tests := []struct {
		method string
		params any
	}{
		{
			method: protocol.MethodSendAgent,
			params: protocol.SendAgentParams{
				AgentID:   bridgeTestAgentID,
				MessageID: "123e4567-e89b-42d3-a456-426614174304",
				Message:   "status",
			},
		},
		{
			method: protocol.MethodFollowupAgent,
			params: protocol.FollowupAgentParams{
				OperationID: "123e4567-e89b-42d3-a456-426614174305",
				AgentID:     bridgeTestAgentID,
				Message:     "continue",
			},
		},
		{
			method: protocol.MethodInterruptAgent,
			params: protocol.InterruptAgentParams{
				OperationID: "123e4567-e89b-42d3-a456-426614174306",
				AgentID:     bridgeTestAgentID,
			},
		},
		{
			method: protocol.MethodWaitAgent,
			params: protocol.WaitAgentParams{
				TimeoutMillis: 1, MessageLimit: 1, ActivityLimit: 1,
			},
		},
	}
	for _, test := range tests {
		var result struct {
			OK bool `json:"ok"`
		}
		if err := client.Call(
			context.Background(), test.method, root.TreeID, &root, test.params, &result,
		); err != nil {
			t.Fatalf("root %s: %v", test.method, err)
		}
		if !result.OK {
			t.Fatalf("root %s result = %#v", test.method, result)
		}
	}
	calls := backend.snapshot()
	if len(calls) != len(tests) {
		t.Fatalf("root agent control calls = %#v", calls)
	}
	for index, test := range tests {
		if calls[index].method != test.method || calls[index].source == nil ||
			*calls[index].source != root {
			t.Fatalf("root agent control call %d = %#v", index, calls[index])
		}
	}

	worker := control.NewWorkerPrincipal(
		bridgeTestControllerID, bridgeTestTreeID, bridgeTestAgentID,
		"123e4567-e89b-42d3-a456-426614174307", bridgeTestDeviceID,
	).Identity()
	err := client.Call(
		context.Background(), protocol.MethodSendAgent, worker.TreeID, &worker,
		protocol.SendAgentParams{
			AgentID:   bridgeTestAgentID,
			MessageID: "123e4567-e89b-42d3-a456-426614174308",
			Message:   "not allowed",
		},
		nil,
	)
	assertRPCCode(t, err, protocol.ErrorForbidden)
	err = client.Call(
		context.Background(), protocol.MethodWaitAgent, worker.TreeID, &worker,
		protocol.WaitAgentParams{TimeoutMillis: 1, MessageLimit: 1, ActivityLimit: 1},
		nil,
	)
	assertRPCCode(t, err, protocol.ErrorForbidden)
	if calls := backend.snapshot(); len(calls) != len(tests) {
		t.Fatalf("worker agent control reached backend: %#v", calls)
	}
}

type deadlineBackend struct {
	remaining chan time.Duration
}

func (b *deadlineBackend) Call(
	ctx context.Context,
	_ string,
	_ string,
	_ *control.PrincipalIdentity,
	_ any,
	result any,
) error {
	deadline, ok := ctx.Deadline()
	if !ok {
		b.remaining <- 0
	} else {
		b.remaining <- time.Until(deadline)
	}
	*result.(*json.RawMessage) = json.RawMessage(`{"ok":true}`)
	return nil
}

func TestBridgeRootAgentControlHasLongCallDeadline(t *testing.T) {
	backend := &deadlineBackend{remaining: make(chan time.Duration, 1)}
	client, stop := startTestBridge(t, backend)
	defer stop()
	root := control.NewRootPrincipal(
		bridgeTestControllerID, bridgeTestTreeID, bridgeTestAgentID, bridgeTestDeviceID,
	).Identity()
	var result struct {
		OK bool `json:"ok"`
	}
	if err := client.Call(
		context.Background(), protocol.MethodSendAgent, root.TreeID, &root,
		protocol.SendAgentParams{
			AgentID:   bridgeTestAgentID,
			MessageID: "123e4567-e89b-42d3-a456-426614174304",
			Message:   "status",
		},
		&result,
	); err != nil {
		t.Fatal(err)
	}
	remaining := <-backend.remaining
	if remaining <= 2*time.Minute || remaining > localCallTimeout {
		t.Fatalf("local agent control deadline = %s", remaining)
	}
}

func TestBridgeWorkspaceCallHasExtendedDeadline(t *testing.T) {
	backend := &deadlineBackend{remaining: make(chan time.Duration, 1)}
	client, stop := startTestBridge(t, backend)
	defer stop()
	root := control.NewRootPrincipal(
		bridgeTestControllerID, bridgeTestTreeID, bridgeTestAgentID, bridgeTestDeviceID,
	).Identity()
	var result struct {
		OK bool `json:"ok"`
	}
	if err := client.Call(
		context.Background(), protocol.MethodSyncWorkspace, root.TreeID, &root,
		protocol.SyncWorkspaceParams{
			SyncID:         "123e4567-e89b-42d3-a456-426614174305",
			TargetDeviceID: bridgeTestDeviceID,
			GitURL:         "ssh://git@example.invalid/repository.git",
			SourcePath:     filepath.Join(t.TempDir(), "source"),
		},
		&result,
	); err != nil {
		t.Fatal(err)
	}
	remaining := <-backend.remaining
	if remaining <= 5*time.Minute || remaining > localWorkspaceCallTimeout {
		t.Fatalf("local workspace deadline = %s", remaining)
	}
}

func TestBridgeEnforcesRoleShapeAllowlistAndErrorMapping(t *testing.T) {
	backend := &fakeBackend{result: json.RawMessage(`{}`)}
	client, stop := startTestBridge(t, backend)
	defer stop()
	for _, test := range []struct {
		name   string
		method string
		err    error
		code   int
	}{
		{name: "broker error", method: protocol.MethodEnsureRootTree, err: &connector.RPCError{Code: protocol.ErrorNotFound, Message: "missing"}, code: protocol.ErrorNotFound},
		{name: "unavailable", method: protocol.MethodEnsureRootTree, err: connector.ErrUnavailable, code: protocol.ErrorUnavailable},
		{name: "internal", method: protocol.MethodEnsureRootTree, err: errors.New("failed"), code: protocol.ErrorInternal},
	} {
		t.Run(test.name, func(t *testing.T) {
			backend.mu.Lock()
			backend.err = test.err
			backend.mu.Unlock()
			err := client.Call(
				context.Background(),
				test.method,
				"",
				nil,
				protocol.EnsureRootTreeParams{ExternalThreadID: bridgeTestTreeID},
				nil,
			)
			assertRPCCode(t, err, test.code)
		})
	}
	backend.mu.Lock()
	backend.err = nil
	before := len(backend.calls)
	backend.mu.Unlock()
	assertRPCCode(t, client.Call(
		context.Background(), "future.call", "", nil, struct{}{}, nil,
	), protocol.ErrorMethodNotFound)
	assertRPCCode(t, client.Call(
		context.Background(), protocol.MethodListDevices, "", nil, protocol.ListDevicesParams{Limit: 10}, nil,
	), protocol.ErrorInvalidRequest)
	if calls := backend.snapshot(); len(calls) != before {
		t.Fatalf("rejected local methods reached backend: %d calls, want %d", len(calls), before)
	}
}

func TestBridgeRequiresLocalReservationAuthorizationForWorkerCalls(t *testing.T) {
	backend := &fakeBackend{result: json.RawMessage(`{
        "messageId":"123e4567-e89b-42d3-a456-426614174309",
        "sequence":1
    }`)}
	worker := control.NewWorkerPrincipal(
		bridgeTestControllerID,
		bridgeTestTreeID,
		bridgeTestAgentID,
		"123e4567-e89b-42d3-a456-426614174304",
		bridgeTestDeviceID,
	).Identity()

	unauthorizedClient, stopUnauthorized := startAuthorizedTestBridge(t, backend, nil)
	var result protocol.SendMessageResult
	err := unauthorizedClient.Call(
		context.Background(),
		protocol.MethodSendMessage,
		worker.TreeID,
		&worker,
		protocol.SendMessageParams{
			MessageID: "123e4567-e89b-42d3-a456-426614174311",
			Target:    protocol.MessageTarget{Kind: protocol.MessageTargetParent},
			Message:   "status",
		},
		&result,
	)
	assertRPCCode(t, err, protocol.ErrorForbidden)
	stopUnauthorized()

	authorizer := &fakeWorkerAuthorizer{}
	client, stop := startAuthorizedTestBridge(t, backend, authorizer)
	defer stop()
	err = client.Call(
		context.Background(),
		protocol.MethodListDevices,
		worker.TreeID,
		&worker,
		protocol.ListDevicesParams{Limit: 10},
		&protocol.ListDevicesResult{},
	)
	assertRPCCode(t, err, protocol.ErrorForbidden)
	err = client.Call(
		context.Background(), protocol.MethodSpawnAgent, worker.TreeID, &worker,
		protocol.SpawnAgentParams{
			SpawnID: bridgeTestTreeID, TargetDeviceID: bridgeTestDeviceID,
			TaskName: "recursive", Message: "do not allow this",
		},
		&protocol.SpawnAgentResult{},
	)
	assertRPCCode(t, err, protocol.ErrorForbidden)
	if calls := backend.snapshot(); len(calls) != 0 {
		t.Fatalf("worker device query reached broker backend: %#v", calls)
	}
	if err := client.Call(
		context.Background(),
		protocol.MethodSendMessage,
		worker.TreeID,
		&worker,
		protocol.SendMessageParams{
			MessageID: "123e4567-e89b-42d3-a456-426614174312",
			Target:    protocol.MessageTarget{Kind: protocol.MessageTargetParent},
			Message:   "status",
		},
		&result,
	); err != nil {
		t.Fatal(err)
	}
	if result.Sequence != 1 {
		t.Fatalf("send message result = %#v", result)
	}
	authorizer.mu.Lock()
	sources := append([]control.PrincipalIdentity(nil), authorizer.sources...)
	authorizer.mu.Unlock()
	if !reflect.DeepEqual(sources, []control.PrincipalIdentity{worker}) {
		t.Fatalf("authorized worker sources = %#v", sources)
	}

	authorizer.mu.Lock()
	authorizer.err = errors.New("reservation missing")
	authorizer.mu.Unlock()
	err = client.Call(
		context.Background(),
		protocol.MethodWaitMailbox,
		worker.TreeID,
		&worker,
		protocol.WaitMailboxParams{Limit: 1, TimeoutMillis: 1},
		&protocol.WaitMailboxResult{},
	)
	assertRPCCode(t, err, protocol.ErrorForbidden)

	crossDevice := worker
	crossDevice.DeviceID = "123e4567-e89b-42d3-a456-426614174398"
	err = client.Call(
		context.Background(),
		protocol.MethodSendMessage,
		crossDevice.TreeID,
		&crossDevice,
		protocol.SendMessageParams{},
		&protocol.SendMessageResult{},
	)
	assertRPCCode(t, err, protocol.ErrorForbidden)
}

func TestBridgeRejectsManagedWorkerThreadBeforeBrokerRootCreation(t *testing.T) {
	backend := &fakeBackend{result: json.RawMessage(`{"ok":true}`)}
	authorizer := &fakeWorkerAuthorizer{managed: true}
	client, stop := startAuthorizedTestBridge(t, backend, authorizer)
	defer stop()

	callRoot := func() error {
		return client.Call(
			context.Background(),
			protocol.MethodEnsureRootTree,
			"",
			nil,
			protocol.EnsureRootTreeParams{ExternalThreadID: bridgeTestTreeID},
			nil,
		)
	}
	assertRPCCode(t, callRoot(), protocol.ErrorForbidden)
	if calls := backend.snapshot(); len(calls) != 0 {
		t.Fatalf("managed worker root request reached broker backend: %#v", calls)
	}

	authorizer.mu.Lock()
	authorizer.managed = false
	authorizer.rootErr = errors.New("peer state unavailable")
	authorizer.mu.Unlock()
	assertRPCCode(t, callRoot(), protocol.ErrorUnavailable)
	if calls := backend.snapshot(); len(calls) != 0 {
		t.Fatalf("root request with failed local authorization reached broker backend: %#v", calls)
	}

	authorizer.mu.Lock()
	authorizer.rootErr = nil
	authorizer.mu.Unlock()
	if err := callRoot(); err != nil {
		t.Fatal(err)
	}
	if calls := backend.snapshot(); len(calls) != 1 || calls[0].method != protocol.MethodEnsureRootTree {
		t.Fatalf("ordinary root request calls = %#v", calls)
	}
	authorizer.mu.Lock()
	rootThreads := append([]string(nil), authorizer.rootThreads...)
	authorizer.mu.Unlock()
	wantThread := bridgeTestControllerID + "/" + bridgeTestTreeID
	if !reflect.DeepEqual(rootThreads, []string{wantThread, wantThread, wantThread}) {
		t.Fatalf("root authorization calls = %#v", rootThreads)
	}
}

type saturatedWaitBackend struct {
	started chan struct{}
	release chan struct{}
}

func (b *saturatedWaitBackend) Call(
	ctx context.Context,
	method, _ string,
	_ *control.PrincipalIdentity,
	_, result any,
) error {
	if method == protocol.MethodWaitMailbox {
		b.started <- struct{}{}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-b.release:
		}
	}
	*result.(*json.RawMessage) = json.RawMessage(`{"ok":true}`)
	return nil
}

func TestBridgeWorkerWaitCapacityPreservesControlHeadroom(t *testing.T) {
	backend := &saturatedWaitBackend{
		started: make(chan struct{}, config.MaximumWorkerSlots),
		release: make(chan struct{}),
	}
	client, stop := startTestBridge(t, backend)
	defer stop()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	waitDone := make(chan error, config.MaximumWorkerSlots)
	for range config.MaximumWorkerSlots {
		agentID, err := identity.NewID()
		if err != nil {
			t.Fatal(err)
		}
		worker := control.NewWorkerPrincipal(
			bridgeTestControllerID,
			bridgeTestTreeID,
			agentID,
			bridgeTestAgentID,
			bridgeTestDeviceID,
		).Identity()
		go func() {
			waitDone <- client.Call(
				ctx,
				protocol.MethodWaitMailbox,
				worker.TreeID,
				&worker,
				protocol.WaitMailboxParams{Limit: 1, TimeoutMillis: protocol.MaximumMailboxWaitMillis},
				nil,
			)
		}()
	}
	for range config.MaximumWorkerSlots {
		select {
		case <-backend.started:
		case <-ctx.Done():
			t.Fatal("all configured worker waits did not reach the backend")
		}
	}

	root := control.NewRootPrincipal(
		bridgeTestControllerID, bridgeTestTreeID, bridgeTestAgentID, bridgeTestDeviceID,
	).Identity()
	for _, call := range []struct {
		method string
		params any
	}{
		{method: protocol.MethodListDevices, params: protocol.ListDevicesParams{Limit: 1}},
		{method: protocol.MethodWaitAgent, params: protocol.WaitAgentParams{
			MessageLimit: 1, ActivityLimit: 1,
		}},
		{method: protocol.MethodSendMessage, params: protocol.SendMessageParams{
			MessageID: "123e4567-e89b-42d3-a456-426614174399",
			Target:    protocol.MessageTarget{Kind: protocol.MessageTargetRoot},
			Message:   "control remains available",
		}},
	} {
		var result struct {
			OK bool `json:"ok"`
		}
		if err := client.Call(ctx, call.method, root.TreeID, &root, call.params, &result); err != nil {
			t.Fatalf("%s while worker waits occupied = %v", call.method, err)
		}
		if !result.OK {
			t.Fatalf("%s result = %#v", call.method, result)
		}
	}

	close(backend.release)
	for range config.MaximumWorkerSlots {
		if err := <-waitDone; err != nil {
			t.Fatalf("worker wait release: %v", err)
		}
	}
}

func TestBridgeAgentWaitUsesWaitCapacityPool(t *testing.T) {
	server := &Server{
		waitSem: make(chan struct{}, 1), controlSem: make(chan struct{}, 1),
	}
	release, admitted := server.admitCall(protocol.MethodWaitAgent)
	if !admitted || len(server.waitSem) != 1 || len(server.controlSem) != 0 {
		t.Fatalf(
			"agent wait admission = admitted %v, wait %d, control %d",
			admitted, len(server.waitSem), len(server.controlSem),
		)
	}
	release()
}

func TestClientCancellationReachesBridgeBackend(t *testing.T) {
	backend := &fakeBackend{
		result: json.RawMessage(`{}`), block: true, started: make(chan struct{}), finished: make(chan struct{}),
	}
	client, stop := startTestBridge(t, backend)
	defer stop()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- client.Call(
			ctx,
			protocol.MethodEnsureRootTree,
			"",
			nil,
			protocol.EnsureRootTreeParams{ExternalThreadID: bridgeTestTreeID},
			nil,
		)
	}()
	select {
	case <-backend.started:
	case <-time.After(time.Second):
		t.Fatal("backend call did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled local call error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("local client did not stop after cancellation")
	}
	select {
	case <-backend.finished:
	case <-time.After(time.Second):
		t.Fatal("client disconnect did not cancel backend call")
	}
}

func TestFrameCodecHandlesPartialWritesAndRejectsOversize(t *testing.T) {
	writer := &oneByteWriter{}
	want := request{
		Version:   Version,
		RequestID: "l_123e4567-e89b-42d3-a456-426614174399",
		Method:    protocol.MethodEnsureRootTree,
		Payload:   json.RawMessage(`{}`),
	}
	if err := writeJSONFrame(writer, want); err != nil {
		t.Fatal(err)
	}
	got, err := readJSONFrame[request](bytes.NewReader(writer.Bytes()))
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("decoded partial-write frame = %#v, error %v", got, err)
	}
	var oversized bytes.Buffer
	if err := binary.Write(&oversized, binary.BigEndian, uint32(protocol.MaxMessageSize+1)); err != nil {
		t.Fatal(err)
	}
	if _, err := readJSONFrame[request](&oversized); err == nil {
		t.Fatal("frame codec accepted an oversized frame")
	}

	data := []byte(`{"version":1,"requestId":"l_123e4567-e89b-42d3-a456-426614174399","method":"tree.ensure_root","payload":{},"unknown":true}`)
	var unknown bytes.Buffer
	if err := binary.Write(&unknown, binary.BigEndian, uint32(len(data))); err != nil {
		t.Fatal(err)
	}
	unknown.Write(data)
	if _, err := readJSONFrame[request](&unknown); err == nil {
		t.Fatal("frame codec accepted an unknown field")
	}
}

func TestEndpointIsStableAndControllerDeviceScoped(t *testing.T) {
	first, err := Endpoint(bridgeTestControllerID, bridgeTestDeviceID)
	if err != nil {
		t.Fatal(err)
	}
	repeated, err := Endpoint(bridgeTestControllerID, bridgeTestDeviceID)
	if err != nil || first != repeated {
		t.Fatalf("repeated endpoint = %q, error %v; want %q", repeated, err, first)
	}
	otherController, err := Endpoint("123e4567-e89b-42d3-a456-426614174398", bridgeTestDeviceID)
	if err != nil || otherController == first {
		t.Fatalf("controller-scoped endpoint = %q, error %v", otherController, err)
	}
	otherDevice, err := Endpoint(bridgeTestControllerID, "123e4567-e89b-42d3-a456-426614174398")
	if err != nil || otherDevice == first {
		t.Fatalf("device-scoped endpoint = %q, error %v", otherDevice, err)
	}
}

func TestServerCloseReturnsListenerCleanupFailure(t *testing.T) {
	want := errors.New("cleanup failed")
	server := &Server{
		listener:    &failingListener{closeErr: want},
		connections: make(map[net.Conn]struct{}),
		serveDone:   make(chan struct{}),
	}
	if err := server.Close(); !errors.Is(err, want) {
		t.Fatalf("Close() error = %v, want %v", err, want)
	}
}

func TestUnsupportedLocalBridgeRequestFailsClosed(t *testing.T) {
	request := request{
		Version: Version + 1, RequestID: "l_123e4567-e89b-42d3-a456-426614174399",
		Method: protocol.MethodEnsureRootTree, Payload: json.RawMessage(`{}`),
	}
	if err := request.validate(); err == nil || !strings.Contains(err.Error(), "unsupported local bridge version 2") {
		t.Fatalf("unsupported local bridge validation error = %v", err)
	}
}

func startTestBridge(t *testing.T, backend Backend) (*Client, func()) {
	return startAuthorizedTestBridge(t, backend, &fakeWorkerAuthorizer{})
}

func startAuthorizedTestBridge(
	t *testing.T,
	backend Backend,
	authorizer Authorizer,
) (*Client, func()) {
	t.Helper()
	endpoint := testEndpoint(t)
	server, err := ListenWithAuthorization(
		endpoint,
		testServiceIdentity(),
		backend,
		authorizer,
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- server.Serve(ctx)
	}()
	client, err := NewClient(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			if err := server.Close(); err != nil {
				t.Errorf("close local bridge: %v", err)
			}
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("serve local bridge: %v", err)
				}
			case <-time.After(time.Second):
				t.Error("local bridge did not stop")
			}
		})
	}
	t.Cleanup(stop)
	return client, stop
}

func testEndpoint(t *testing.T) string {
	t.Helper()
	controllerID, err := identity.NewID()
	if err != nil {
		t.Fatal(err)
	}
	deviceID, err := identity.NewID()
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := Endpoint(controllerID, deviceID)
	if err != nil {
		t.Fatal(err)
	}
	return endpoint
}

func testServiceIdentity() ServiceIdentity {
	return ServiceIdentity{
		ControllerID: bridgeTestControllerID,
		DeviceID:     bridgeTestDeviceID,
	}
}

func assertRPCCode(t *testing.T, err error, code int) {
	t.Helper()
	var rpcError *RPCError
	if !errors.As(err, &rpcError) || rpcError.Code != code {
		t.Fatalf("RPC error = %v, want code %d", err, code)
	}
}

type oneByteWriter struct {
	bytes.Buffer
}

func (w *oneByteWriter) Write(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}
	return w.Buffer.Write(data[:1])
}

type failingListener struct {
	closeErr error
}

func (*failingListener) Accept() (net.Conn, error) { return nil, net.ErrClosed }
func (l *failingListener) Close() error            { return l.closeErr }
func (*failingListener) Addr() net.Addr            { return testAddr("failing") }

type testAddr string

func (testAddr) Network() string  { return "test" }
func (a testAddr) String() string { return string(a) }
