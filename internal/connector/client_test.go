package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/broker"
	"github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/credential"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
	"github.com/GhostFlying/delegation/internal/tokenfile"
	"github.com/coder/websocket"
)

const (
	connectorTestControllerID = "123e4567-e89b-42d3-a456-426614174200"
	connectorTestDeviceID     = "123e4567-e89b-42d3-a456-426614174201"
	connectorTestWorkerID     = "123e4567-e89b-42d3-a456-426614174202"
	connectorTestThreadID     = "123e4567-e89b-42d3-a456-426614174203"
	connectorTestConnectionID = "123e4567-e89b-42d3-a456-426614174204"
)

type brokerFixture struct {
	registry    *store.Store
	server      *broker.Server
	httpServer  *httptest.Server
	masterToken tokenfile.Token
	deviceToken tokenfile.Token
}

func TestTokenConnectorMaintainsPresenceAndCallsBroker(t *testing.T) {
	fixture := newBrokerFixture(t, config.AuthModeToken, 500*time.Millisecond)
	client := newTestClient(t, fixture.url(), config.AuthModeToken, &fixture.deviceToken)
	ctx, cancel := context.WithCancel(context.Background())
	done := runClient(client, ctx)
	waitReady(t, client)

	var root protocol.EnsureRootTreeResult
	if err := client.Call(
		context.Background(),
		protocol.MethodEnsureRootTree,
		"",
		nil,
		protocol.EnsureRootTreeParams{ExternalThreadID: connectorTestThreadID},
		&root,
	); err != nil {
		t.Fatal(err)
	}
	if root.Tree.RootDeviceID != connectorTestDeviceID || !root.Principal.Has(control.CapabilityDeviceRead) {
		t.Fatalf("root tree = %#v", root)
	}
	worker := protocol.Hello{
		ControllerID:   connectorTestControllerID,
		DeviceID:       connectorTestWorkerID,
		DeviceName:     "windows-builder",
		OS:             "windows",
		Arch:           "amd64",
		RuntimeVersion: "0.1.0-alpha.0.m1.1",
		Features: []string{
			protocol.FeatureDeviceRegistry,
			protocol.FeatureFullDuplexRPC,
			protocol.FeaturePeerRoot,
		},
	}
	if _, err := fixture.registry.RegisterTrustedDevice(
		context.Background(), worker.Descriptor(), time.Unix(10, 0),
	); err != nil {
		t.Fatal(err)
	}
	source := root.Principal.Identity()
	var devices protocol.ListDevicesResult
	if err := client.Call(
		context.Background(),
		protocol.MethodListDevices,
		root.Tree.TreeID,
		&source,
		protocol.ListDevicesParams{Limit: 10},
		&devices,
	); err != nil {
		t.Fatal(err)
	}
	if len(devices.Devices) != 2 ||
		devices.Devices[0].DeviceID != connectorTestDeviceID ||
		devices.Devices[1].DeviceID != connectorTestWorkerID {
		t.Fatalf("device registry = %#v", devices)
	}

	waitForDevice(t, fixture.registry, connectorTestDeviceID, func(device control.Device) bool {
		return device.Online && device.LastSeenAt > 1
	})
	cancel()
	if err := waitClient(done); err != nil {
		t.Fatal(err)
	}
	waitForDevice(t, fixture.registry, connectorTestDeviceID, func(device control.Device) bool {
		return !device.Online
	})
}

func TestNoneAuthPeerConnectorRegisters(t *testing.T) {
	fixture := newBrokerFixture(t, config.AuthModeNone, 20*time.Millisecond)
	client := newTestClient(t, fixture.url(), config.AuthModeNone, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := runClient(client, ctx)
	waitReady(t, client)
	waitForDevice(t, fixture.registry, connectorTestDeviceID, func(device control.Device) bool {
		return device.Online
	})
	cancel()
	if err := waitClient(done); err != nil {
		t.Fatal(err)
	}
}

func TestConnectorRequiresEveryBrokerFeatureBeforePublishingReadiness(t *testing.T) {
	required := []string{
		protocol.FeatureDeviceRegistry,
		protocol.FeatureFullDuplexRPC,
		protocol.FeaturePeerRoot,
	}
	for _, missing := range required {
		t.Run(missing, func(t *testing.T) {
			features := make([]string, 0, len(required)-1)
			for _, feature := range required {
				if feature != missing {
					features = append(features, feature)
				}
			}
			server := newFakeBrokerWithFeatures(t, features, func(*websocket.Conn) {})
			defer server.Close()
			client := newTestClient(t, websocketURL(server.URL), config.AuthModeNone, nil)
			heartbeat, err := client.runSession(context.Background())
			if err == nil || !strings.Contains(err.Error(), missing) || heartbeat {
				t.Fatalf("session without %s = heartbeat %v, error %v", missing, heartbeat, err)
			}
			if status := client.Status(); !reflect.DeepEqual(status, Status{}) {
				t.Fatalf("session without %s published readiness: %#v", missing, status)
			}
			readyContext, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			err = client.WaitReady(readyContext)
			cancel()
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("WaitReady() without %s = %v", missing, err)
			}
		})
	}
}

func TestConnectorCorrelatesConcurrentResponsesAndIgnoresCanceledLateResponse(t *testing.T) {
	requestSeen := make(chan struct{})
	releaseLate := make(chan struct{})
	brokerRequestHandled := make(chan protocol.Envelope, 1)
	server := newFakeBroker(t, func(connection *websocket.Conn) {
		first := readTestEnvelope(t, connection)
		second := readTestEnvelope(t, connection)
		writeTestResult(t, connection, second, map[string]string{"value": second.Method})
		writeTestResult(t, connection, first, map[string]string{"value": first.Method})

		late := readTestEnvelope(t, connection)
		close(requestSeen)
		<-releaseLate
		writeTestResult(t, connection, late, map[string]string{"value": late.Method})
		fast := readTestEnvelope(t, connection)
		writeTestResult(t, connection, fast, map[string]string{"value": fast.Method})

		brokerRequest := protocol.Envelope{
			ProtocolVersion: protocol.Version,
			Kind:            protocol.KindRequest,
			RequestID:       testRequestID(t, protocol.DirectionBroker),
			Method:          "future.call",
			ControllerID:    connectorTestControllerID,
			Payload:         json.RawMessage(`{}`),
		}
		writeTestEnvelope(t, connection, brokerRequest)
		brokerRequestHandled <- readTestEnvelope(t, connection)
	})
	defer server.Close()
	client := newTestClient(t, websocketURL(server.URL), config.AuthModeNone, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := runClient(client, ctx)
	waitReady(t, client)

	type result struct {
		Value string `json:"value"`
	}
	results := make(chan result, 2)
	callErrors := make(chan error, 2)
	for _, method := range []string{"test.first", "test.second"} {
		go func() {
			var response result
			err := client.Call(context.Background(), method, "", nil, struct{}{}, &response)
			results <- response
			callErrors <- err
		}()
	}
	seen := map[string]bool{}
	for range 2 {
		if err := <-callErrors; err != nil {
			t.Fatal(err)
		}
		seen[(<-results).Value] = true
	}
	if !reflect.DeepEqual(seen, map[string]bool{"test.first": true, "test.second": true}) {
		t.Fatalf("concurrent results = %v", seen)
	}

	lateContext, cancelLate := context.WithTimeout(context.Background(), 20*time.Millisecond)
	err := client.Call(lateContext, "test.late", "", nil, struct{}{}, nil)
	cancelLate()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("late call error = %v", err)
	}
	<-requestSeen
	close(releaseLate)
	var fast result
	if err := client.Call(context.Background(), "test.fast", "", nil, struct{}{}, &fast); err != nil {
		t.Fatal(err)
	}
	if fast.Value != "test.fast" || !client.Status().Connected {
		t.Fatalf("fast result = %#v, status = %#v", fast, client.Status())
	}
	select {
	case response := <-brokerRequestHandled:
		if response.Kind != protocol.KindResponse || response.Error == nil ||
			response.Error.Code != protocol.ErrorMethodNotFound ||
			!hasDirection(response.RequestID, protocol.DirectionConnector) {
			t.Fatalf("broker request response = %#v", response)
		}
	case <-time.After(time.Second):
		t.Fatal("connector did not answer broker request")
	}
	cancel()
	if err := waitClient(done); err != nil {
		t.Fatal(err)
	}
}

func TestConnectorReconnectsAfterForcedDisconnect(t *testing.T) {
	var connections atomic.Int64
	secondConnected := make(chan struct{})
	server := newFakeBroker(t, func(connection *websocket.Conn) {
		if connections.Add(1) == 1 {
			connection.CloseNow()
			return
		}
		close(secondConnected)
		_, _, _ = connection.Read(context.Background())
	})
	defer server.Close()
	client := newTestClient(t, websocketURL(server.URL), config.AuthModeNone, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := runClient(client, ctx)
	select {
	case <-secondConnected:
	case <-time.After(2 * time.Second):
		t.Fatal("connector did not reconnect")
	}
	waitReady(t, client)
	if connections.Load() < 2 {
		t.Fatalf("connection attempts = %d", connections.Load())
	}
	cancel()
	if err := waitClient(done); err != nil {
		t.Fatal(err)
	}
}

func TestLocalRequestFailuresDoNotDisconnectSharedSession(t *testing.T) {
	var connections atomic.Int64
	stop := make(chan struct{})
	server := newFakeBroker(t, func(connection *websocket.Conn) {
		connections.Add(1)
		request := readTestEnvelope(t, connection)
		writeTestResult(t, connection, request, map[string]string{"value": "connected"})
		<-stop
	})
	defer server.Close()
	client := newTestClient(t, websocketURL(server.URL), config.AuthModeNone, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := runClient(client, ctx)
	waitReady(t, client)

	oversized := strings.Repeat("x", protocol.MaxMessageSize)
	if err := client.Call(context.Background(), "test.oversized", "", nil, oversized, nil); err == nil {
		t.Fatal("oversized local request succeeded")
	}
	canceled, cancelCall := context.WithCancel(context.Background())
	cancelCall()
	if err := client.Call(canceled, "test.canceled", "", nil, struct{}{}, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled local request error = %v", err)
	}
	var result struct {
		Value string `json:"value"`
	}
	if err := client.Call(context.Background(), "test.connected", "", nil, struct{}{}, &result); err != nil {
		t.Fatal(err)
	}
	if result.Value != "connected" || connections.Load() != 1 || !client.Status().Connected {
		t.Fatalf("result = %#v, connections = %d, status = %#v", result, connections.Load(), client.Status())
	}
	cancel()
	close(stop)
	if err := waitClient(done); err != nil {
		t.Fatal(err)
	}
}

func TestBrokerResponseWinsImmediateSessionShutdown(t *testing.T) {
	var connections atomic.Int64
	server := newFakeBroker(t, func(connection *websocket.Conn) {
		sequence := connections.Add(1)
		request := readTestEnvelope(t, connection)
		writeTestResult(t, connection, request, map[string]int64{"sequence": sequence})
		_ = connection.CloseNow()
	})
	defer server.Close()
	client := newTestClient(t, websocketURL(server.URL), config.AuthModeNone, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := runClient(client, ctx)
	defer func() {
		cancel()
		if err := waitClient(done); err != nil {
			t.Error(err)
		}
	}()

	const callCount = 32
	for sequence := int64(1); sequence <= callCount; sequence++ {
		waitReady(t, client)
		var result struct {
			Sequence int64 `json:"sequence"`
		}
		callContext, cancelCall := context.WithTimeout(context.Background(), 2*time.Second)
		err := client.Call(callContext, "test.response_then_close", "", nil, struct{}{}, &result)
		cancelCall()
		if err != nil || result.Sequence != sequence {
			t.Fatalf("call %d result = %#v, error %v", sequence, result, err)
		}
		if sequence != callCount {
			deadline := time.Now().Add(2 * time.Second)
			for connections.Load() <= sequence && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if connections.Load() <= sequence {
				t.Fatalf("connector did not establish session %d", sequence+1)
			}
		}
	}
}

func TestMismatchedResponseClosesPendingCall(t *testing.T) {
	server := newFakeBroker(t, func(connection *websocket.Conn) {
		request := readTestEnvelope(t, connection)
		payload, err := json.Marshal(map[string]bool{"ok": true})
		if err != nil {
			t.Error(err)
			return
		}
		writeTestEnvelope(t, connection, protocol.Envelope{
			ProtocolVersion: protocol.Version,
			Kind:            protocol.KindResponse,
			RequestID:       testRequestID(t, protocol.DirectionBroker),
			ReplyTo:         request.RequestID,
			ControllerID:    connectorTestControllerID,
			TreeID:          connectorTestWorkerID,
			Payload:         payload,
		})
	})
	defer server.Close()
	client := newTestClient(t, websocketURL(server.URL), config.AuthModeNone, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := runClient(client, ctx)
	waitReady(t, client)
	callDone := make(chan error, 1)
	go func() {
		callDone <- client.Call(
			context.Background(), "test.mismatched", connectorTestThreadID, nil, struct{}{}, nil,
		)
	}()
	select {
	case err := <-callDone:
		if err == nil || !strings.Contains(err.Error(), "treeId") {
			t.Fatalf("mismatched response error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("mismatched response orphaned its pending call")
	}
	cancel()
	if err := waitClient(done); err != nil {
		t.Fatal(err)
	}
}

func TestWaitReadyBroadcastsAndHonorsCancellation(t *testing.T) {
	client := newTestClient(t, "ws://127.0.0.1:8787", config.AuthModeNone, nil)
	const waiterCount = 16
	started := make(chan struct{}, waiterCount)
	results := make(chan error, waiterCount)
	for range waiterCount {
		go func() {
			started <- struct{}{}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			results <- client.WaitReady(ctx)
		}()
	}
	for range waiterCount {
		<-started
	}
	time.Sleep(20 * time.Millisecond)
	client.publish(nil, protocol.HelloResult{
		ConnectionID:        connectorTestConnectionID,
		Features:            []string{protocol.FeatureDeviceRegistry},
		HeartbeatIntervalMS: time.Second.Milliseconds(),
		Revision:            1,
	})
	for range waiterCount {
		if err := <-results; err != nil {
			t.Fatalf("broadcast waiter failed: %v", err)
		}
	}

	offline := newTestClient(t, "ws://127.0.0.1:8787", config.AuthModeNone, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	err := offline.WaitReady(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("canceled WaitReady() error = %v", err)
	}
}

func TestPendingCallsDrainAtCapacityAndRecoverAfterReconnect(t *testing.T) {
	var connections atomic.Int64
	pendingFull := make(chan struct{})
	disconnect := make(chan struct{})
	reconnected := make(chan struct{})
	stopSecond := make(chan struct{})
	server := newFakeBroker(t, func(connection *websocket.Conn) {
		if connections.Add(1) == 1 {
			for range maximumPendingCalls {
				readTestEnvelope(t, connection)
			}
			close(pendingFull)
			<-disconnect
			_ = connection.CloseNow()
			return
		}
		close(reconnected)
		request := readTestEnvelope(t, connection)
		writeTestResult(t, connection, request, map[string]string{"value": "recovered"})
		<-stopSecond
	})
	defer server.Close()
	client := newTestClient(t, websocketURL(server.URL), config.AuthModeNone, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := runClient(client, ctx)
	waitReady(t, client)

	callErrors := make(chan error, maximumPendingCalls)
	var started sync.WaitGroup
	started.Add(maximumPendingCalls)
	for range maximumPendingCalls {
		go func() {
			started.Done()
			callErrors <- client.Call(context.Background(), "test.pending", "", nil, struct{}{}, nil)
		}()
	}
	started.Wait()
	select {
	case <-pendingFull:
	case <-time.After(2 * time.Second):
		t.Fatal("broker did not receive all pending calls")
	}
	if err := client.Call(context.Background(), "test.overflow", "", nil, struct{}{}, nil); !errors.Is(err, ErrBusy) {
		t.Fatalf("overflow call error = %v, want ErrBusy", err)
	}
	client.mu.RLock()
	disconnected := client.updates
	client.mu.RUnlock()
	close(disconnect)
	for range maximumPendingCalls {
		select {
		case err := <-callErrors:
			if err == nil {
				t.Fatal("disconnected pending call succeeded")
			}
		case <-time.After(2 * time.Second):
			t.Fatal("pending calls did not drain after disconnect")
		}
	}
	select {
	case <-disconnected:
	case <-time.After(2 * time.Second):
		t.Fatal("connector did not publish its disconnected state")
	}
	select {
	case <-reconnected:
	case <-time.After(2 * time.Second):
		t.Fatal("connector did not reconnect after draining pending calls")
	}
	waitReady(t, client)
	var recovered struct {
		Value string `json:"value"`
	}
	if err := client.Call(context.Background(), "test.recovered", "", nil, struct{}{}, &recovered); err != nil {
		t.Fatal(err)
	}
	if recovered.Value != "recovered" {
		t.Fatalf("recovered call = %#v", recovered)
	}
	cancel()
	close(stopSecond)
	if err := waitClient(done); err != nil {
		t.Fatal(err)
	}
}

func TestConnectorDoesNotFollowRedirectWithBearerToken(t *testing.T) {
	var redirected atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		redirected.Add(1)
	}))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Redirect(writer, request, target.URL, http.StatusFound)
	}))
	defer redirect.Close()
	token := tokenfile.Token{9, 8, 7}
	client := newTestClient(t, websocketURL(redirect.URL), config.AuthModeToken, &token)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	_, err := client.runSession(ctx)
	cancel()
	if err == nil || redirected.Load() != 0 {
		t.Fatalf("redirect result = %v, redirected requests = %d", err, redirected.Load())
	}
	if strings.Contains(err.Error(), tokenfile.Encode(token)) {
		t.Fatalf("connector error exposed token: %v", err)
	}
}

func TestConnectorValidatesStaticOptionsAndOfflineCalls(t *testing.T) {
	base := Options{
		BrokerURL:       "wss://broker.example.test/v1/connect",
		ControllerID:    connectorTestControllerID,
		DeviceID:        connectorTestDeviceID,
		DeviceName:      "builder",
		AuthMode:        config.AuthModeNone,
		RuntimeVersion:  "0.1.0-alpha.0.m1.1",
		OperatingSystem: "linux",
		Architecture:    "amd64",
	}
	client, err := New(base)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Call(context.Background(), "device.list", "", nil, struct{}{}, nil); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("offline call error = %v", err)
	}
	invalid := base
	invalid.BrokerURL = "wss://broker.example.test/other"
	if _, err := New(invalid); err == nil {
		t.Fatal("connector accepted an unrelated broker path")
	}
	invalid = base
	invalid.BrokerURL = "ws://broker.example.test"
	if _, err := New(invalid); err == nil {
		t.Fatal("connector accepted unacknowledged plaintext non-loopback transport")
	}
	invalid.AllowInsecureNonLoopback = true
	if _, err := New(invalid); err != nil {
		t.Fatalf("connector rejected acknowledged plaintext non-loopback transport: %v", err)
	}
	invalid = base
	invalid.AuthMode = config.AuthModeToken
	if _, err := New(invalid); err == nil {
		t.Fatal("connector accepted token auth without a token")
	}
}

func TestConnectorAmbientProxyPolicy(t *testing.T) {
	const helperEnvironment = "DELEGATION_TEST_CONNECTOR_PROXY_POLICY"
	const proxyURL = "http://127.0.0.1:32767"
	if os.Getenv(helperEnvironment) == "1" {
		base := Options{
			BrokerURL:                "ws://broker.example.test",
			AllowInsecureNonLoopback: true,
			ControllerID:             connectorTestControllerID,
			DeviceID:                 connectorTestDeviceID,
			DeviceName:               "builder",
			AuthMode:                 config.AuthModeNone,
			RuntimeVersion:           "0.1.0-alpha.0.m1.1",
			OperatingSystem:          "linux",
			Architecture:             "amd64",
		}
		plaintext, err := New(base)
		if err != nil {
			t.Fatal(err)
		}
		plaintextTransport, ok := plaintext.httpClient.Transport.(*http.Transport)
		if !ok {
			t.Fatalf("plaintext connector transport = %T", plaintext.httpClient.Transport)
		}
		if plaintextTransport.Proxy != nil {
			t.Fatal("plaintext connector retained ambient HTTP proxy routing")
		}

		base.BrokerURL = "wss://broker.example.test"
		base.AllowInsecureNonLoopback = false
		secure, err := New(base)
		if err != nil {
			t.Fatal(err)
		}
		secureTransport, ok := secure.httpClient.Transport.(*http.Transport)
		if !ok {
			t.Fatalf("secure connector transport = %T", secure.httpClient.Transport)
		}
		if secureTransport.Proxy == nil {
			t.Fatal("secure connector discarded standard HTTPS proxy routing")
		}
		request, err := http.NewRequest(http.MethodGet, "https://broker.example.test/v1/connect", nil)
		if err != nil {
			t.Fatal(err)
		}
		proxy, err := secureTransport.Proxy(request)
		if err != nil {
			t.Fatal(err)
		}
		if proxy == nil || proxy.String() != proxyURL {
			t.Fatalf("secure connector proxy = %v, want %s", proxy, proxyURL)
		}
		return
	}

	environment := make([]string, 0, len(os.Environ())+5)
	for _, variable := range os.Environ() {
		name, _, _ := strings.Cut(variable, "=")
		switch strings.ToUpper(name) {
		case "HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY":
			continue
		default:
			environment = append(environment, variable)
		}
	}
	environment = append(environment,
		helperEnvironment+"=1",
		"HTTP_PROXY="+proxyURL,
		"HTTPS_PROXY="+proxyURL,
		"NO_PROXY=",
	)
	command := exec.Command(os.Args[0], "-test.run=^TestConnectorAmbientProxyPolicy$", "-test.count=1")
	command.Env = environment
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("proxy policy helper failed: %v\n%s", err, output)
	}
}

func newBrokerFixture(
	t *testing.T,
	authMode config.AuthMode,
	heartbeat time.Duration,
) brokerFixture {
	t.Helper()
	registry, err := store.Open(context.Background(), t.TempDir()+"/state/broker.sqlite3")
	if err != nil {
		t.Fatal(err)
	}
	fixture := brokerFixture{
		registry: registry, masterToken: tokenfile.Token{1}, deviceToken: tokenfile.Token{2},
	}
	var master *tokenfile.Token
	if authMode == config.AuthModeToken {
		master = &fixture.masterToken
		mac := credential.MAC(fixture.masterToken, fixture.deviceToken)
		if err := registry.CreateCredential(context.Background(), store.NewCredential(
			connectorTestControllerID, connectorTestDeviceID, mac, time.Unix(1, 0),
		)); err != nil {
			t.Fatal(err)
		}
	}
	server, err := broker.New(broker.Options{
		ControllerID:      connectorTestControllerID,
		AuthMode:          authMode,
		MasterToken:       master,
		Registry:          registry,
		HeartbeatInterval: heartbeat,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.Prepare(context.Background()); err != nil {
		t.Fatal(err)
	}
	fixture.server = server
	fixture.httpServer = httptest.NewServer(server.Handler())
	t.Cleanup(func() {
		fixture.httpServer.Close()
		closeContext, cancel := context.WithTimeout(context.Background(), time.Second)
		if err := server.Close(closeContext); err != nil {
			t.Errorf("close broker: %v", err)
		}
		cancel()
		if err := registry.Close(); err != nil {
			t.Errorf("close broker store: %v", err)
		}
	})
	return fixture
}

func (f brokerFixture) url() string {
	return websocketURL(f.httpServer.URL)
}

func newTestClient(
	t *testing.T,
	brokerURL string,
	authMode config.AuthMode,
	token *tokenfile.Token,
) *Client {
	t.Helper()
	client, err := New(Options{
		BrokerURL: brokerURL, ControllerID: connectorTestControllerID, DeviceID: connectorTestDeviceID,
		DeviceName: "builder", AuthMode: authMode, Token: token,
		RuntimeVersion: "0.1.0-alpha.0.m1.1", OperatingSystem: "linux", Architecture: "amd64",
		ReconnectMin: 5 * time.Millisecond, ReconnectMax: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func runClient(client *Client, ctx context.Context) <-chan error {
	done := make(chan error, 1)
	go func() {
		done <- client.Run(ctx)
	}()
	return done
}

func waitClient(done <-chan error) error {
	select {
	case err := <-done:
		return err
	case <-time.After(2 * time.Second):
		return errors.New("connector did not stop")
	}
}

func waitReady(t *testing.T, client *Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
}

func waitForDevice(
	t *testing.T,
	registry *store.Store,
	deviceID string,
	ready func(control.Device) bool,
) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		record, err := registry.DescribeDevice(context.Background(), connectorTestControllerID, deviceID)
		if err == nil && ready(record.Device) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("device %s did not reach expected state", deviceID)
}

func newFakeBroker(t *testing.T, afterHello func(*websocket.Conn)) *httptest.Server {
	return newFakeBrokerWithFeatures(t, []string{
		protocol.FeatureDeviceRegistry,
		protocol.FeatureFullDuplexRPC,
		protocol.FeaturePeerRoot,
	}, afterHello)
}

func newFakeBrokerWithFeatures(
	t *testing.T,
	features []string,
	afterHello func(*websocket.Conn),
) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := websocket.Accept(writer, request, nil)
		if err != nil {
			t.Errorf("accept fake broker connection: %v", err)
			return
		}
		defer connection.CloseNow()
		hello := readTestEnvelope(t, connection)
		writeTestResult(t, connection, hello, protocol.HelloResult{
			ConnectionID:        connectorTestConnectionID,
			Features:            append([]string(nil), features...),
			HeartbeatIntervalMS: time.Hour.Milliseconds(),
			Revision:            1,
		})
		afterHello(connection)
	}))
}

func readTestEnvelope(t *testing.T, connection *websocket.Conn) protocol.Envelope {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	messageType, data, err := connection.Read(ctx)
	if err != nil {
		t.Errorf("read test envelope: %v", err)
		return protocol.Envelope{}
	}
	if messageType != websocket.MessageText {
		t.Errorf("test envelope message type = %v", messageType)
		return protocol.Envelope{}
	}
	envelope, err := protocol.Read(bytes.NewReader(data))
	if err != nil {
		t.Errorf("decode test envelope: %v", err)
	}
	return envelope
}

func writeTestResult(t *testing.T, connection *websocket.Conn, request protocol.Envelope, result any) {
	t.Helper()
	payload, err := json.Marshal(result)
	if err != nil {
		t.Error(err)
		return
	}
	writeTestEnvelope(t, connection, protocol.Envelope{
		ProtocolVersion: protocol.Version,
		Kind:            protocol.KindResponse,
		RequestID:       testRequestID(t, protocol.DirectionBroker),
		ReplyTo:         request.RequestID,
		ControllerID:    connectorTestControllerID,
		TreeID:          request.TreeID,
		Payload:         payload,
	})
}

func writeTestEnvelope(t *testing.T, connection *websocket.Conn, envelope protocol.Envelope) {
	t.Helper()
	data, err := protocol.Marshal(envelope)
	if err != nil {
		t.Error(err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := connection.Write(ctx, websocket.MessageText, data); err != nil {
		t.Errorf("write test envelope: %v", err)
	}
}

func testRequestID(t *testing.T, direction protocol.RequestDirection) string {
	t.Helper()
	requestID, err := protocol.NewRequestID(direction)
	if err != nil {
		t.Fatal(err)
	}
	return requestID
}

func websocketURL(httpURL string) string {
	return "ws" + strings.TrimPrefix(httpURL, "http")
}
