package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/credential"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
	"github.com/GhostFlying/delegation/internal/tokenfile"
	"github.com/coder/websocket"
)

const (
	brokerTestControllerID = "123e4567-e89b-42d3-a456-426614174100"
	brokerTestDeviceID     = "123e4567-e89b-42d3-a456-426614174101"
)

type brokerHarness struct {
	server      *Server
	registry    *store.Store
	httpServer  *httptest.Server
	masterToken tokenfile.Token
	deviceToken tokenfile.Token
	reported    chan error
}

type faultRegistry struct {
	*store.Store
	authenticateErr      error
	registerTrustedErr   error
	heartbeatErr         error
	markOfflineErr       error
	authenticateStarted  chan struct{}
	authenticateCanceled chan struct{}
}

type transientOfflineRegistry struct {
	*store.Store
	failures atomic.Int32
	attempts chan uint64
	failure  error
}

func (r *transientOfflineRegistry) MarkDeviceOffline(
	ctx context.Context,
	controllerID, deviceID string,
	expectedRevision uint64,
	observedAt time.Time,
) (control.Device, error) {
	r.attempts <- expectedRevision
	for {
		remaining := r.failures.Load()
		if remaining == 0 {
			break
		}
		if r.failures.CompareAndSwap(remaining, remaining-1) {
			return control.Device{}, r.failure
		}
	}
	return r.Store.MarkDeviceOffline(ctx, controllerID, deviceID, expectedRevision, observedAt)
}

func (f *faultRegistry) AuthenticateCredential(
	ctx context.Context,
	mac store.CredentialMAC,
) (store.Credential, error) {
	if f.authenticateStarted != nil {
		close(f.authenticateStarted)
		<-ctx.Done()
		close(f.authenticateCanceled)
		return store.Credential{}, ctx.Err()
	}
	if f.authenticateErr != nil {
		return store.Credential{}, f.authenticateErr
	}
	return f.Store.AuthenticateCredential(ctx, mac)
}

func (f *faultRegistry) RegisterTrustedDevice(
	ctx context.Context,
	descriptor control.DeviceDescriptor,
	observedAt time.Time,
) (control.Device, error) {
	if f.registerTrustedErr != nil {
		return control.Device{}, f.registerTrustedErr
	}
	return f.Store.RegisterTrustedDevice(ctx, descriptor, observedAt)
}

func (f *faultRegistry) HeartbeatDevice(
	ctx context.Context,
	controllerID, deviceID string,
	expectedRevision uint64,
	observedAt time.Time,
) (control.Device, error) {
	if f.heartbeatErr != nil {
		return control.Device{}, f.heartbeatErr
	}
	return f.Store.HeartbeatDevice(ctx, controllerID, deviceID, expectedRevision, observedAt)
}

func (f *faultRegistry) MarkDeviceOffline(
	ctx context.Context,
	controllerID, deviceID string,
	expectedRevision uint64,
	observedAt time.Time,
) (control.Device, error) {
	if f.markOfflineErr != nil {
		return control.Device{}, f.markOfflineErr
	}
	return f.Store.MarkDeviceOffline(ctx, controllerID, deviceID, expectedRevision, observedAt)
}

func TestTokenConnectionHeartbeatUnknownMethodsAndDisconnect(t *testing.T) {
	harness := newBrokerHarness(t, config.AuthModeToken, time.Second)
	connection, response, err := dialBroker(harness, &harness.deviceToken)
	if err != nil {
		t.Fatalf("dial broker: %v; response = %#v", err, response)
	}
	result := sendHello(t, connection, control.DeviceRoleWorker)
	if result.Revision != 1 || result.HeartbeatIntervalMS != 1000 {
		t.Fatalf("hello result = %#v", result)
	}
	record, err := harness.registry.DescribeDevice(
		context.Background(), brokerTestControllerID, brokerTestDeviceID,
	)
	if err != nil || !record.Device.Online || record.Device.Revision != 1 {
		t.Fatalf("registered device = %#v, error %v", record, err)
	}
	writeEnvelope(t, connection, protocol.Envelope{
		ProtocolVersion: protocol.Version,
		Kind:            protocol.KindNotification,
		RequestID:       newRequestID(t),
		Method:          "future.notice",
		ControllerID:    brokerTestControllerID,
		Payload:         json.RawMessage(`{"ignored":true}`),
	})
	heartbeat := request(t, protocol.MethodHeartbeat, protocol.Heartbeat{})
	heartbeatResponse := writeAndRead(t, connection, heartbeat)
	if heartbeatResponse.Error != nil {
		t.Fatalf("heartbeat response error = %#v", heartbeatResponse.Error)
	}
	heartbeatResult := decodeResult[protocol.HeartbeatResult](t, heartbeatResponse)
	if heartbeatResult.Revision != 1 || heartbeatResult.ServerTime != 2 {
		t.Fatalf("heartbeat result = %#v", heartbeatResult)
	}
	unknown := writeAndRead(t, connection, request(t, "future.request", struct{}{}))
	if unknown.Error == nil || unknown.Error.Code != protocol.ErrorMethodNotFound {
		t.Fatalf("unknown method response = %#v", unknown)
	}
	if err := connection.Close(websocket.StatusNormalClosure, "done"); err != nil {
		t.Fatal(err)
	}
	waitForDevice(t, harness.registry, false, 2)
}

func TestTokenAuthenticationAndCredentialRoleAreEnforced(t *testing.T) {
	harness := newBrokerHarness(t, config.AuthModeToken, time.Second)
	for name, token := range map[string]*tokenfile.Token{
		"missing": nil,
		"master":  &harness.masterToken,
		"unknown": ptr(tokenfile.Token{9}),
	} {
		t.Run(name, func(t *testing.T) {
			connection, response, err := dialBroker(harness, token)
			if err == nil {
				connection.CloseNow()
				t.Fatal("unauthorized WebSocket dial succeeded")
			}
			if response == nil || response.StatusCode != http.StatusUnauthorized {
				t.Fatalf("unauthorized response = %#v, error %v", response, err)
			}
		})
	}
	connection, _, err := dialBroker(harness, &harness.deviceToken)
	if err != nil {
		t.Fatal(err)
	}
	hello := helloRequest(t, control.DeviceRoleController)
	response := writeAndRead(t, connection, hello)
	if response.Error == nil || response.Error.Code != protocol.ErrorForbidden {
		t.Fatalf("role escalation response = %#v", response)
	}
	connection.CloseNow()
	if _, err := harness.registry.DescribeDevice(
		context.Background(), brokerTestControllerID, brokerTestDeviceID,
	); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("rejected hello created a device: %v", err)
	}
}

func TestPendingHelloAdmissionIsBoundedAndRecovers(t *testing.T) {
	for _, testCase := range []struct {
		name        string
		authMode    config.AuthMode
		globalLimit int
		deviceLimit int
	}{
		{name: "global none-auth limit", authMode: config.AuthModeNone, globalLimit: 1, deviceLimit: 2},
		{name: "per-device token limit", authMode: config.AuthModeToken, globalLimit: 2, deviceLimit: 1},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			harness := newBrokerHarness(t, testCase.authMode, time.Second)
			harness.server.mu.Lock()
			harness.server.helloLimit = testCase.globalLimit
			harness.server.deviceHelloLimit = testCase.deviceLimit
			harness.server.mu.Unlock()
			var token *tokenfile.Token
			if testCase.authMode == config.AuthModeToken {
				token = &harness.deviceToken
			}
			first, _, err := dialBroker(harness, token)
			if err != nil {
				t.Fatal(err)
			}
			second, response, err := dialBroker(harness, token)
			if second != nil {
				second.CloseNow()
			}
			if err == nil || response == nil || response.StatusCode != http.StatusServiceUnavailable {
				t.Fatalf("saturated dial response = %#v, error %v", response, err)
			}
			response.Body.Close()
			first.CloseNow()

			deadline := time.Now().Add(time.Second)
			for {
				recovered, response, err := dialBroker(harness, token)
				if err == nil {
					recovered.CloseNow()
					break
				}
				if response != nil {
					response.Body.Close()
				}
				if time.Now().After(deadline) {
					t.Fatalf("pending hello slot did not recover: %v", err)
				}
				time.Sleep(10 * time.Millisecond)
			}
		})
	}
}

func TestConnectionIDFailureDoesNotRegisterDevice(t *testing.T) {
	harness := newBrokerHarness(t, config.AuthModeNone, time.Second)
	failure := errors.New("connection ID generator failed")
	harness.server.newID = func() (string, error) { return "", failure }
	connection, _, err := dialBroker(harness, nil)
	if err != nil {
		t.Fatal(err)
	}
	response := writeAndRead(t, connection, helloRequest(t, control.DeviceRoleWorker))
	if response.Error == nil || response.Error.Code != protocol.ErrorInternal {
		t.Fatalf("connection ID failure response = %#v", response)
	}
	expectReported(t, harness.reported, failure)
	if _, err := harness.registry.DescribeDevice(
		context.Background(), brokerTestControllerID, brokerTestDeviceID,
	); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("connection ID failure registered a device: %v", err)
	}
}

func TestOfflineRetryCannotReleaseReplacementLease(t *testing.T) {
	harness := newBrokerHarness(t, config.AuthModeNone, time.Second)
	failure := errors.New("transient offline failure")
	retrying := &transientOfflineRegistry{
		Store: harness.registry, attempts: make(chan uint64, 4), failure: failure,
	}
	retrying.failures.Store(1)
	harness.server.registry = retrying
	harness.server.offlineRetryInterval = 500 * time.Millisecond

	first, _, err := dialBroker(harness, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result := sendHello(t, first, control.DeviceRoleWorker); result.Revision != 1 {
		t.Fatalf("first hello = %#v", result)
	}
	first.CloseNow()
	select {
	case revision := <-retrying.attempts:
		if revision != 1 {
			t.Fatalf("initial offline revision = %d", revision)
		}
	case <-time.After(time.Second):
		t.Fatal("initial offline transition did not run")
	}
	expectReported(t, harness.reported, failure)

	second, _, err := dialBroker(harness, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result := sendHello(t, second, control.DeviceRoleWorker); result.Revision != 2 {
		t.Fatalf("replacement hello = %#v", result)
	}
	select {
	case revision := <-retrying.attempts:
		if revision != 1 {
			t.Fatalf("retried offline revision = %d", revision)
		}
	case <-time.After(time.Second):
		t.Fatal("offline transition was not retried")
	}
	record, err := harness.registry.DescribeDevice(
		context.Background(), brokerTestControllerID, brokerTestDeviceID,
	)
	if err != nil || !record.Device.Online || record.Device.Revision != 2 {
		t.Fatalf("replacement after delayed retry = %#v, error %v", record, err)
	}
	if err := second.Close(websocket.StatusNormalClosure, "done"); err != nil {
		t.Fatal(err)
	}
	waitForDevice(t, harness.registry, false, 3)
}

func TestDuplicateConnectionCannotOfflineReplacement(t *testing.T) {
	harness := newBrokerHarness(t, config.AuthModeNone, time.Second)
	first, _, err := dialBroker(harness, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result := sendHello(t, first, control.DeviceRoleController); result.Revision != 1 {
		t.Fatalf("first hello = %#v", result)
	}
	second, _, err := dialBroker(harness, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result := sendHello(t, second, control.DeviceRoleController); result.Revision != 2 {
		t.Fatalf("replacement hello = %#v", result)
	}
	readContext, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, _, err := first.Read(readContext); err == nil {
		t.Fatal("replaced connection remained readable")
	}
	heartbeat := writeAndRead(t, second, request(t, protocol.MethodHeartbeat, protocol.Heartbeat{}))
	if result := decodeResult[protocol.HeartbeatResult](t, heartbeat); result.Revision != 2 {
		t.Fatalf("replacement heartbeat = %#v", result)
	}
	record, err := harness.registry.DescribeDevice(
		context.Background(), brokerTestControllerID, brokerTestDeviceID,
	)
	if err != nil || !record.Device.Online || record.Device.Revision != 2 {
		t.Fatalf("replacement device = %#v, error %v", record, err)
	}
	if err := second.Close(websocket.StatusNormalClosure, "done"); err != nil {
		t.Fatal(err)
	}
	waitForDevice(t, harness.registry, false, 3)
}

func TestActivationNeverReplacesNewerLease(t *testing.T) {
	harness := newBrokerHarness(t, config.AuthModeNone, time.Second)
	newer := &session{server: harness.server, deviceID: brokerTestDeviceID}
	newer.revision.Store(2)
	older := &session{server: harness.server, deviceID: brokerTestDeviceID}
	older.revision.Store(1)
	if _, active := harness.server.activate(newer); !active {
		t.Fatal("newer lease did not activate")
	}
	previous, active := harness.server.activate(older)
	if active || previous != newer || harness.server.connections[brokerTestDeviceID] != newer {
		t.Fatalf("older activation = previous %#v, active %v", previous, active)
	}
	harness.server.mu.Lock()
	delete(harness.server.connections, brokerTestDeviceID)
	harness.server.mu.Unlock()
}

func TestNonHeartbeatTrafficDoesNotExtendLease(t *testing.T) {
	harness := newBrokerHarness(t, config.AuthModeNone, 20*time.Millisecond)
	connection, _, err := dialBroker(harness, nil)
	if err != nil {
		t.Fatal(err)
	}
	sendHello(t, connection, control.DeviceRoleWorker)
	spamDeadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(spamDeadline) {
		notification := request(t, "future.notice", struct{}{})
		notification.Kind = protocol.KindNotification
		data, err := protocol.Marshal(notification)
		if err != nil {
			t.Fatal(err)
		}
		writeContext, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		err = connection.Write(writeContext, websocket.MessageText, data)
		cancel()
		if err != nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	waitForDevice(t, harness.registry, false, 2)
}

func TestPeerRequestDirectionsAreEnforced(t *testing.T) {
	t.Run("hello requestId", func(t *testing.T) {
		harness := newBrokerHarness(t, config.AuthModeNone, time.Second)
		connection, _, err := dialBroker(harness, nil)
		if err != nil {
			t.Fatal(err)
		}
		hello := helloRequest(t, control.DeviceRoleWorker)
		hello.RequestID = requestID(t, protocol.DirectionBroker)
		writeEnvelope(t, connection, hello)
		expectConnectionClosed(t, connection)
		if _, err := harness.registry.DescribeDevice(
			context.Background(), brokerTestControllerID, brokerTestDeviceID,
		); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("invalid hello created a device: %v", err)
		}
	})

	t.Run("active requestId", func(t *testing.T) {
		harness := newBrokerHarness(t, config.AuthModeNone, time.Second)
		connection, _, err := dialBroker(harness, nil)
		if err != nil {
			t.Fatal(err)
		}
		sendHello(t, connection, control.DeviceRoleWorker)
		heartbeat := request(t, protocol.MethodHeartbeat, protocol.Heartbeat{})
		heartbeat.RequestID = requestID(t, protocol.DirectionLocal)
		writeEnvelope(t, connection, heartbeat)
		expectConnectionClosed(t, connection)
		waitForDevice(t, harness.registry, false, 2)
	})

	t.Run("response replyTo", func(t *testing.T) {
		harness := newBrokerHarness(t, config.AuthModeNone, time.Second)
		connection, _, err := dialBroker(harness, nil)
		if err != nil {
			t.Fatal(err)
		}
		sendHello(t, connection, control.DeviceRoleWorker)
		response := protocol.Envelope{
			ProtocolVersion: protocol.Version,
			Kind:            protocol.KindResponse,
			RequestID:       requestID(t, protocol.DirectionConnector),
			ReplyTo:         requestID(t, protocol.DirectionConnector),
			ControllerID:    brokerTestControllerID,
			Payload:         json.RawMessage(`{}`),
		}
		writeEnvelope(t, connection, response)
		expectConnectionClosed(t, connection)
		waitForDevice(t, harness.registry, false, 2)
	})
}

func TestCloseForcesPeersAndPersistsOfflineState(t *testing.T) {
	harness := newBrokerHarness(t, config.AuthModeNone, time.Second)
	connection, _, err := dialBroker(harness, nil)
	if err != nil {
		t.Fatal(err)
	}
	sendHello(t, connection, control.DeviceRoleWorker)
	started := time.Now()
	closeContext, cancel := context.WithTimeout(context.Background(), time.Second)
	err = harness.server.Close(closeContext)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("broker close took %v", elapsed)
	}
	record, err := harness.registry.DescribeDevice(
		context.Background(), brokerTestControllerID, brokerTestDeviceID,
	)
	if err != nil || record.Device.Online {
		t.Fatalf("device after close = %#v, error %v", record, err)
	}
}

func TestCloseIgnoresConcurrentPeerClose(t *testing.T) {
	harness := newBrokerHarness(t, config.AuthModeNone, time.Second)
	connection, _, err := dialBroker(harness, nil)
	if err != nil {
		t.Fatal(err)
	}
	sendHello(t, connection, control.DeviceRoleWorker)
	harness.server.mu.Lock()
	var peer *websocket.Conn
	for current := range harness.server.peers {
		peer = current
	}
	harness.server.mu.Unlock()
	if peer == nil {
		t.Fatal("server did not track the connected peer")
	}
	peerClosed := make(chan error, 1)
	go func() {
		peerClosed <- peer.CloseNow()
	}()
	closeContext, cancel := context.WithTimeout(context.Background(), time.Second)
	err = harness.server.Close(closeContext)
	cancel()
	if err != nil {
		t.Fatalf("close raced with peer close: %v", err)
	}
	if err := <-peerClosed; err != nil && !errors.Is(err, net.ErrClosed) {
		t.Fatalf("peer close error = %v", err)
	}
}

func TestPeerCloseErrorNormalization(t *testing.T) {
	if err := normalizePeerCloseError(fmt.Errorf("wrapped close: %w", net.ErrClosed)); err != nil {
		t.Fatalf("wrapped net.ErrClosed was not ignored: %v", err)
	}
	failure := errors.New("peer transport failed")
	if err := normalizePeerCloseError(failure); !errors.Is(err, failure) {
		t.Fatalf("peer failure = %v, want %v", err, failure)
	}
}

func TestAuthenticationStopsWhenHTTPClientCancels(t *testing.T) {
	harness := newBrokerHarness(t, config.AuthModeToken, time.Second)
	started := make(chan struct{})
	canceled := make(chan struct{})
	harness.server.registry = &faultRegistry{
		Store:                harness.registry,
		authenticateStarted:  started,
		authenticateCanceled: canceled,
	}
	dialContext, cancelDial := context.WithCancel(context.Background())
	dialDone := make(chan error, 1)
	go func() {
		header := http.Header{}
		header.Set("Authorization", "Bearer "+tokenfile.Encode(harness.deviceToken))
		connection, _, err := websocket.Dial(
			dialContext,
			harness.httpServer.URL+ConnectPath,
			&websocket.DialOptions{HTTPHeader: header},
		)
		if connection != nil {
			connection.CloseNow()
		}
		dialDone <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("authentication did not start")
	}
	cancelDial()
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("HTTP cancellation did not stop authentication")
	}
	if err := <-dialDone; err == nil {
		t.Fatal("canceled dial succeeded")
	}
}

func TestBrokerCloseCancelsAuthenticationAndWaitsForHandler(t *testing.T) {
	harness := newBrokerHarness(t, config.AuthModeToken, time.Second)
	started := make(chan struct{})
	canceled := make(chan struct{})
	harness.server.registry = &faultRegistry{
		Store:                harness.registry,
		authenticateStarted:  started,
		authenticateCanceled: canceled,
	}
	dialDone := make(chan error, 1)
	go func() {
		header := http.Header{}
		header.Set("Authorization", "Bearer "+tokenfile.Encode(harness.deviceToken))
		connection, _, err := websocket.Dial(
			context.Background(),
			harness.httpServer.URL+ConnectPath,
			&websocket.DialOptions{HTTPHeader: header},
		)
		if connection != nil {
			connection.CloseNow()
		}
		dialDone <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("authentication did not start")
	}
	closeContext, cancel := context.WithTimeout(context.Background(), time.Second)
	if err := harness.server.Close(closeContext); err != nil {
		t.Fatalf("close broker with blocked authentication: %v", err)
	}
	cancel()
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("broker close did not cancel authentication")
	}
	select {
	case err := <-dialDone:
		if err == nil {
			t.Fatal("dial succeeded while broker closed")
		}
	case <-time.After(time.Second):
		t.Fatal("authentication handler remained blocked after broker close")
	}
}

func TestUnexpectedRegistryFailuresAreUnavailableAndReported(t *testing.T) {
	t.Run("authentication", func(t *testing.T) {
		harness := newBrokerHarness(t, config.AuthModeToken, time.Second)
		failure := errors.New("authentication database failed")
		harness.server.registry = &faultRegistry{Store: harness.registry, authenticateErr: failure}
		connection, response, err := dialBroker(harness, &harness.deviceToken)
		if err == nil {
			connection.CloseNow()
			t.Fatal("dial succeeded during authentication failure")
		}
		if response == nil || response.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("authentication failure response = %#v, error %v", response, err)
		}
		expectReported(t, harness.reported, failure)
	})

	t.Run("established session authentication", func(t *testing.T) {
		harness := newBrokerHarness(t, config.AuthModeToken, time.Second)
		connection, _, err := dialBroker(harness, &harness.deviceToken)
		if err != nil {
			t.Fatal(err)
		}
		sendHello(t, connection, control.DeviceRoleWorker)
		failure := errors.New("session authentication database failed")
		harness.server.registry = &faultRegistry{Store: harness.registry, authenticateErr: failure}
		response := writeAndRead(t, connection, request(t, protocol.MethodHeartbeat, protocol.Heartbeat{}))
		if response.Error == nil || response.Error.Code != protocol.ErrorUnavailable {
			t.Fatalf("session authentication failure response = %#v", response)
		}
		expectReported(t, harness.reported, failure)
	})

	t.Run("registration", func(t *testing.T) {
		harness := newBrokerHarness(t, config.AuthModeNone, time.Second)
		failure := errors.New("registration database failed")
		harness.server.registry = &faultRegistry{Store: harness.registry, registerTrustedErr: failure}
		connection, _, err := dialBroker(harness, nil)
		if err != nil {
			t.Fatal(err)
		}
		response := writeAndRead(t, connection, helloRequest(t, control.DeviceRoleWorker))
		if response.Error == nil || response.Error.Code != protocol.ErrorUnavailable {
			t.Fatalf("registration failure response = %#v", response)
		}
		expectReported(t, harness.reported, failure)
	})

	t.Run("heartbeat", func(t *testing.T) {
		harness := newBrokerHarness(t, config.AuthModeNone, time.Second)
		failure := errors.New("heartbeat database failed")
		harness.server.registry = &faultRegistry{Store: harness.registry, heartbeatErr: failure}
		connection, _, err := dialBroker(harness, nil)
		if err != nil {
			t.Fatal(err)
		}
		sendHello(t, connection, control.DeviceRoleWorker)
		response := writeAndRead(t, connection, request(t, protocol.MethodHeartbeat, protocol.Heartbeat{}))
		if response.Error == nil || response.Error.Code != protocol.ErrorUnavailable {
			t.Fatalf("heartbeat failure response = %#v", response)
		}
		expectReported(t, harness.reported, failure)
		waitForDevice(t, harness.registry, false, 2)
	})

	t.Run("offline cleanup", func(t *testing.T) {
		harness := newBrokerHarness(t, config.AuthModeNone, time.Second)
		failure := errors.New("offline database failed")
		harness.server.registry = &faultRegistry{Store: harness.registry, markOfflineErr: failure}
		connection, _, err := dialBroker(harness, nil)
		if err != nil {
			t.Fatal(err)
		}
		sendHello(t, connection, control.DeviceRoleWorker)
		connection.CloseNow()
		expectReported(t, harness.reported, failure)
	})
}

func TestHeartbeatTimeoutAndBrokerEpochMarkDevicesOffline(t *testing.T) {
	harness := newBrokerHarness(t, config.AuthModeNone, 20*time.Millisecond)
	connection, _, err := dialBroker(harness, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result := sendHello(t, connection, control.DeviceRoleWorker); result.Revision != 1 {
		t.Fatalf("hello = %#v", result)
	}
	waitForDevice(t, harness.registry, false, 2)
	connection.CloseNow()
	if _, err := harness.registry.RegisterTrustedDevice(
		context.Background(), hello(control.DeviceRoleWorker).Descriptor(), time.Unix(10, 0),
	); err != nil {
		t.Fatal(err)
	}
	transition, err := harness.server.Prepare(context.Background())
	if err != nil || transition.Count != 1 {
		t.Fatalf("broker epoch = %#v, error %v", transition, err)
	}
	record, err := harness.registry.DescribeDevice(
		context.Background(), brokerTestControllerID, brokerTestDeviceID,
	)
	if err != nil || record.Device.Online {
		t.Fatalf("epoch device = %#v, error %v", record, err)
	}
}

func newBrokerHarness(t *testing.T, authMode config.AuthMode, interval time.Duration) brokerHarness {
	return newBrokerHarnessForRole(t, authMode, interval, control.DeviceRoleWorker)
}

func newBrokerHarnessForRole(
	t *testing.T,
	authMode config.AuthMode,
	interval time.Duration,
	credentialRole control.DeviceRole,
) brokerHarness {
	t.Helper()
	registry, err := store.Open(
		context.Background(), filepath.Join(t.TempDir(), "state", "broker.sqlite3"),
	)
	if err != nil {
		t.Fatal(err)
	}
	harness := brokerHarness{
		registry: registry,
		reported: make(chan error, 16),
		masterToken: tokenfile.Token{
			1,
		},
		deviceToken: tokenfile.Token{
			2,
		},
	}
	var master *tokenfile.Token
	if authMode == config.AuthModeToken {
		master = &harness.masterToken
		mac := credential.MAC(harness.masterToken, harness.deviceToken)
		if err := registry.CreateCredential(context.Background(), store.NewCredential(
			brokerTestControllerID,
			brokerTestDeviceID,
			credentialRole,
			mac,
			time.Unix(1, 0),
		)); err != nil {
			t.Fatal(err)
		}
	}
	var tick atomic.Int64
	server, err := New(Options{
		ControllerID:      brokerTestControllerID,
		AuthMode:          authMode,
		MasterToken:       master,
		Registry:          registry,
		HeartbeatInterval: interval,
		Now: func() time.Time {
			return time.Unix(tick.Add(1), 0)
		},
		ReportError: func(err error) {
			harness.reported <- err
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	harness.server = server
	harness.httpServer = httptest.NewServer(server.Handler())
	t.Cleanup(func() {
		closeContext, cancel := context.WithTimeout(context.Background(), time.Second)
		if err := server.Close(closeContext); err != nil {
			t.Errorf("close broker: %v", err)
		}
		cancel()
		harness.httpServer.Close()
		if err := registry.Close(); err != nil {
			t.Errorf("close registry: %v", err)
		}
		select {
		case err := <-harness.reported:
			t.Errorf("unexpected broker error: %v", err)
		default:
		}
	})
	return harness
}

func dialBroker(harness brokerHarness, token *tokenfile.Token) (*websocket.Conn, *http.Response, error) {
	header := http.Header{}
	if token != nil {
		header.Set("Authorization", "Bearer "+tokenfile.Encode(*token))
	}
	return websocket.Dial(
		context.Background(), harness.httpServer.URL+ConnectPath, &websocket.DialOptions{HTTPHeader: header},
	)
}

func sendHello(t *testing.T, connection *websocket.Conn, role control.DeviceRole) protocol.HelloResult {
	t.Helper()
	response := writeAndRead(t, connection, helloRequest(t, role))
	if response.Error != nil {
		t.Fatalf("hello error = %#v", response.Error)
	}
	return decodeResult[protocol.HelloResult](t, response)
}

func helloRequest(t *testing.T, role control.DeviceRole) protocol.Envelope {
	t.Helper()
	return request(t, protocol.MethodHello, hello(role))
}

func hello(role control.DeviceRole) protocol.Hello {
	return protocol.Hello{
		ControllerID:   brokerTestControllerID,
		DeviceID:       brokerTestDeviceID,
		DeviceName:     "builder",
		Role:           role,
		OS:             "linux",
		Arch:           "amd64",
		RuntimeVersion: "0.1.0-alpha.0.m1",
		Features:       []string{protocol.FeatureDeviceRegistry},
	}
}

func request(t *testing.T, method string, payload any) protocol.Envelope {
	t.Helper()
	return protocol.Envelope{
		ProtocolVersion: protocol.Version,
		Kind:            protocol.KindRequest,
		RequestID:       newRequestID(t),
		Method:          method,
		ControllerID:    brokerTestControllerID,
		Payload:         marshalPayload(t, payload),
	}
}

func marshalPayload(t *testing.T, payload any) json.RawMessage {
	t.Helper()
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func newRequestID(t *testing.T) string {
	t.Helper()
	return requestID(t, protocol.DirectionConnector)
}

func requestID(t *testing.T, direction protocol.RequestDirection) string {
	t.Helper()
	requestID, err := protocol.NewRequestID(direction)
	if err != nil {
		t.Fatal(err)
	}
	return requestID
}

func expectConnectionClosed(t *testing.T, connection *websocket.Conn) {
	t.Helper()
	readContext, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, _, err := connection.Read(readContext); err == nil {
		t.Fatal("connection remained readable")
	}
}

func expectReported(t *testing.T, reported <-chan error, target error) {
	t.Helper()
	select {
	case err := <-reported:
		if !errors.Is(err, target) {
			t.Fatalf("reported error = %v, want %v", err, target)
		}
	case <-time.After(time.Second):
		t.Fatalf("broker did not report %v", target)
	}
}

func writeAndRead(
	t *testing.T,
	connection *websocket.Conn,
	envelope protocol.Envelope,
) protocol.Envelope {
	t.Helper()
	writeEnvelope(t, connection, envelope)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	messageType, data, err := connection.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if messageType != websocket.MessageText {
		t.Fatalf("response message type = %v, want text", messageType)
	}
	response, err := protocol.Read(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if response.ReplyTo != envelope.RequestID {
		t.Fatalf("response replyTo = %q, want %q", response.ReplyTo, envelope.RequestID)
	}
	return response
}

func writeEnvelope(t *testing.T, connection *websocket.Conn, envelope protocol.Envelope) {
	t.Helper()
	data, err := protocol.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := connection.Write(ctx, websocket.MessageText, data); err != nil {
		t.Fatal(err)
	}
}

func decodeResult[T any](t *testing.T, envelope protocol.Envelope) T {
	t.Helper()
	value, err := protocol.DecodePayload[T](envelope.Payload)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func waitForDevice(t *testing.T, registry *store.Store, online bool, revision uint64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		record, err := registry.DescribeDevice(
			context.Background(), brokerTestControllerID, brokerTestDeviceID,
		)
		if err == nil && record.Device.Online == online && record.Device.Revision == revision {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	record, err := registry.DescribeDevice(context.Background(), brokerTestControllerID, brokerTestDeviceID)
	t.Fatalf("device did not reach online=%v revision=%d: %#v, %v", online, revision, record, err)
}

func ptr[T any](value T) *T {
	return &value
}
