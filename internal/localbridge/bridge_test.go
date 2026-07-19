package localbridge

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

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
	if !root.OK || !list.OK {
		t.Fatalf("bridge results = %#v, %#v", root, list)
	}
	calls := backend.snapshot()
	if len(calls) != 2 || calls[0].method != protocol.MethodEnsureRootTree ||
		calls[0].treeID != "" || calls[0].source != nil ||
		calls[1].method != protocol.MethodListDevices || calls[1].treeID != bridgeTestTreeID ||
		calls[1].source == nil || *calls[1].source != source {
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

func TestLocalBridgeV1RequestFailsClosed(t *testing.T) {
	request := request{
		Version: 1, RequestID: "l_123e4567-e89b-42d3-a456-426614174399",
		Method: protocol.MethodEnsureRootTree, Payload: json.RawMessage(`{}`),
	}
	if err := request.validate(); err == nil || !strings.Contains(err.Error(), "unsupported local bridge version 1") {
		t.Fatalf("legacy local bridge validation error = %v", err)
	}
}

func startTestBridge(t *testing.T, backend Backend) (*Client, func()) {
	t.Helper()
	endpoint := testEndpoint(t)
	server, err := Listen(endpoint, testServiceIdentity(), backend)
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
