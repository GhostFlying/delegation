package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/broker"
	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
	"github.com/GhostFlying/delegation/internal/tokenfile"
	"github.com/coder/websocket"
)

const (
	runtimeControllerID = "123e4567-e89b-42d3-a456-426614174200"
	runtimeDeviceID     = "123e4567-e89b-42d3-a456-426614174201"
)

func TestBrokerServiceRunsConfiguredAuthModes(t *testing.T) {
	for _, authMode := range []string{"none", "token"} {
		t.Run(authMode, func(t *testing.T) {
			configPath, cfg := setupBrokerRuntimeTest(t, authMode)
			ctx, cancel := context.WithCancel(context.Background())
			ready := make(chan string, 1)
			var stderr bytes.Buffer
			done := make(chan error, 1)
			go func() {
				done <- runBrokerService(ctx, configPath, cfg, &stderr, testBrokerListen(ready))
			}()

			address := waitForBrokerAddress(t, ready, done)
			waitForBrokerHealth(t, address)
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("runBrokerService() error = %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("broker service did not stop after cancellation")
			}
			if !strings.Contains(stderr.String(), "broker listening on "+cfg.Broker.Listen) {
				t.Fatalf("stderr = %q", stderr.String())
			}
		})
	}
}

func TestBrokerServiceUsesConfiguredMasterToken(t *testing.T) {
	configPath, cfg := setupBrokerRuntimeTest(t, "token")
	registry, err := store.Open(context.Background(), cfg.Broker.StateFile)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}
	deviceTokenPath := privateTestPath(t, "device.token")
	var issueOutput bytes.Buffer
	var issueError bytes.Buffer
	if code := Run([]string{
		"credential", "issue",
		"--config", configPath,
		"--device-id", runtimeDeviceID,
		"--out", deviceTokenPath,
	}, &issueOutput, &issueError); code != 0 {
		t.Fatalf("credential issue code = %d, stderr = %q", code, issueError.String())
	}
	deviceToken, err := tokenfile.Read(deviceTokenPath)
	if err != nil {
		t.Fatal(err)
	}
	unrelatedToken, err := tokenfile.Generate()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan string, 1)
	done := make(chan error, 1)
	go func() {
		done <- runBrokerService(ctx, configPath, cfg, io.Discard, testBrokerListen(ready))
	}()
	address := waitForBrokerAddress(t, ready, done)
	waitForBrokerHealth(t, address)

	for _, test := range []struct {
		name  string
		token string
	}{
		{name: "missing"},
		{name: "unrelated", token: tokenfile.Encode(unrelatedToken)},
	} {
		t.Run(test.name, func(t *testing.T) {
			header := http.Header{}
			if test.token != "" {
				header.Set("Authorization", "Bearer "+test.token)
			}
			connection, response, err := websocket.Dial(
				context.Background(), "ws://"+address+"/v2/connect", &websocket.DialOptions{HTTPHeader: header},
			)
			if connection != nil {
				connection.CloseNow()
			}
			if err == nil || response == nil || response.StatusCode != http.StatusUnauthorized {
				t.Fatalf("unauthorized dial response = %#v, error = %v", response, err)
			}
			response.Body.Close()
		})
	}
	header := http.Header{"Authorization": []string{"Bearer " + tokenfile.Encode(deviceToken)}}
	connection, _, err := websocket.Dial(
		context.Background(), "ws://"+address+"/v2/connect", &websocket.DialOptions{HTTPHeader: header},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.CloseNow()
	sendRuntimeHello(t, connection, runtimeDeviceID)
	cancel()
	waitForBrokerStop(t, done)
}

func TestBrokerServiceClosesManagedWebSocketAndMarksDeviceOffline(t *testing.T) {
	configPath, cfg := setupBrokerRuntimeTest(t, "none")
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan string, 1)
	done := make(chan error, 1)
	go func() {
		done <- runBrokerService(ctx, configPath, cfg, io.Discard, testBrokerListen(ready))
	}()
	address := waitForBrokerAddress(t, ready, done)
	waitForBrokerHealth(t, address)
	connection, _, err := websocket.Dial(context.Background(), "ws://"+address+"/v2/connect", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.CloseNow()
	sendRuntimeHello(t, connection, runtimeDeviceID)

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runBrokerService() error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("broker service did not close a managed WebSocket")
	}
	readContext, stopRead := context.WithTimeout(context.Background(), time.Second)
	defer stopRead()
	if _, _, err := connection.Read(readContext); err == nil {
		t.Fatal("managed WebSocket remained open after broker shutdown")
	}
	registry, err := store.Open(context.Background(), cfg.Broker.StateFile)
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	record, err := registry.DescribeDevice(context.Background(), runtimeControllerID, runtimeDeviceID)
	if err != nil {
		t.Fatal(err)
	}
	if record.Device.Online {
		t.Fatalf("device remained online after shutdown: %#v", record.Device)
	}
}

func TestBrokerServiceRejectsSecondProcessWithoutInvalidatingLiveLease(t *testing.T) {
	const helperEnvironment = "DELEGATION_TEST_SECOND_BROKER"
	if os.Getenv(helperEnvironment) == "1" {
		configPath := os.Getenv("DELEGATION_TEST_BROKER_CONFIG")
		cfg, err := delegationconfig.Read(configPath)
		if err != nil {
			t.Fatal(err)
		}
		err = runBrokerService(context.Background(), configPath, cfg, io.Discard, brokerRuntimeOptions{
			listen: func(ctx context.Context, _, _ string) (net.Listener, error) {
				var listenConfig net.ListenConfig
				return listenConfig.Listen(ctx, "tcp", "127.0.0.1:0")
			},
		})
		if !errors.Is(err, store.ErrBrokerLeaseHeld) {
			t.Fatalf("second broker error = %v, want ErrBrokerLeaseHeld", err)
		}
		return
	}

	configPath, cfg := setupBrokerRuntimeTest(t, "none")
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan string, 1)
	done := make(chan error, 1)
	go func() {
		done <- runBrokerService(ctx, configPath, cfg, io.Discard, testBrokerListen(ready))
	}()
	address := waitForBrokerAddress(t, ready, done)
	waitForBrokerHealth(t, address)
	connection, _, err := websocket.Dial(context.Background(), "ws://"+address+"/v2/connect", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.CloseNow()
	sendRuntimeHello(t, connection, runtimeDeviceID)

	command := exec.Command(os.Args[0],
		"-test.run=^TestBrokerServiceRejectsSecondProcessWithoutInvalidatingLiveLease$", "-test.count=1",
	)
	command.Env = append(os.Environ(),
		helperEnvironment+"=1",
		"DELEGATION_TEST_BROKER_CONFIG="+configPath,
	)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("second broker helper failed: %v\n%s", err, output)
	}
	sendRuntimeHeartbeat(t, connection)

	cancel()
	waitForBrokerStop(t, done)
}

func TestBrokerServiceBindFailurePreservesPresence(t *testing.T) {
	configPath, cfg := setupBrokerRuntimeTest(t, "none")
	registry, err := store.Open(context.Background(), cfg.Broker.StateFile)
	if err != nil {
		t.Fatal(err)
	}
	want, err := registry.RegisterTrustedDevice(context.Background(), control.DeviceDescriptor{
		ControllerID:    runtimeControllerID,
		DeviceID:        runtimeDeviceID,
		Name:            "existing-worker",
		OS:              "linux",
		Arch:            "amd64",
		RuntimeVersion:  "0.1.0-alpha.0.m1.1",
		ProtocolVersion: protocol.Version,
	}, time.Unix(10, 0))
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("address already in use")
	err = runBrokerService(context.Background(), configPath, cfg, io.Discard, brokerRuntimeOptions{
		listen: func(context.Context, string, string) (net.Listener, error) {
			return nil, injected
		},
	})
	if !errors.Is(err, injected) {
		t.Fatalf("runBrokerService() error = %v", err)
	}
	registry, err = store.Open(context.Background(), cfg.Broker.StateFile)
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	record, err := registry.DescribeDevice(context.Background(), runtimeControllerID, runtimeDeviceID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(record.Device, want) {
		t.Fatalf("device after bind failure = %#v, want %#v", record.Device, want)
	}
}

func TestBrokerServiceLeaseFailurePrecedesStoreOpen(t *testing.T) {
	configPath, cfg := setupBrokerRuntimeTest(t, "none")
	injected := errors.New("broker lease held")
	storeOpened := false
	err := runBrokerService(context.Background(), configPath, cfg, io.Discard, brokerRuntimeOptions{
		listen: func(ctx context.Context, _, _ string) (net.Listener, error) {
			var listenConfig net.ListenConfig
			return listenConfig.Listen(ctx, "tcp", "127.0.0.1:0")
		},
		lease: func(string) (io.Closer, error) {
			return nil, injected
		},
		openStore: func(context.Context, string) (*store.Store, error) {
			storeOpened = true
			return nil, errors.New("store must not open")
		},
	})
	if !errors.Is(err, injected) {
		t.Fatalf("runBrokerService() error = %v", err)
	}
	if storeOpened {
		t.Fatal("broker opened its state store before acquiring the instance lease")
	}
}

func TestBrokerServiceReadsMasterTokenBeforeListening(t *testing.T) {
	configPath, cfg := setupBrokerRuntimeTest(t, "token")
	if err := os.Remove(cfg.Broker.Auth.TokenFile); err != nil {
		t.Fatal(err)
	}
	listenCalled := false
	err := runBrokerService(context.Background(), configPath, cfg, io.Discard, brokerRuntimeOptions{
		listen: func(context.Context, string, string) (net.Listener, error) {
			listenCalled = true
			return nil, errors.New("listener should not be called")
		},
	})
	if err == nil || !strings.Contains(err.Error(), "read broker master token") {
		t.Fatalf("runBrokerService() error = %v", err)
	}
	if listenCalled {
		t.Fatal("broker attempted to listen before validating its master token")
	}
}

func TestBrokerServiceCancellationStopsAtStartupBoundaries(t *testing.T) {
	for _, test := range []struct {
		name        string
		preCanceled bool
		wantListen  bool
		listen      func(context.CancelFunc, *bool) brokerListenFunc
	}{
		{
			name:        "before listen",
			preCanceled: true,
			listen: func(_ context.CancelFunc, called *bool) brokerListenFunc {
				return func(context.Context, string, string) (net.Listener, error) {
					*called = true
					return nil, errors.New("listener should not be called")
				}
			},
		},
		{
			name:       "after bind",
			wantListen: true,
			listen: func(cancel context.CancelFunc, called *bool) brokerListenFunc {
				return func(ctx context.Context, _, _ string) (net.Listener, error) {
					*called = true
					var listenConfig net.ListenConfig
					listener, err := listenConfig.Listen(ctx, "tcp", "127.0.0.1:0")
					cancel()
					return listener, err
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			configPath, cfg := setupBrokerRuntimeTest(t, "none")
			ctx, cancel := context.WithCancel(context.Background())
			called := false
			if test.preCanceled {
				cancel()
			}
			err := runBrokerService(ctx, configPath, cfg, io.Discard, brokerRuntimeOptions{
				listen: test.listen(cancel, &called),
			})
			if err != nil {
				t.Fatalf("runBrokerService() error = %v", err)
			}
			if called != test.wantListen {
				t.Fatalf("listen called = %v", called)
			}
			if _, err := os.Stat(cfg.Broker.StateFile); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("broker state was created after startup cancellation: %v", err)
			}
		})
	}
}

func TestBrokerServiceTreatsPrepareCancellationAsCleanStop(t *testing.T) {
	configPath, cfg := setupBrokerRuntimeTest(t, "none")
	ctx, cancel := context.WithCancel(context.Background())
	err := runBrokerService(ctx, configPath, cfg, io.Discard, brokerRuntimeOptions{
		listen: func(ctx context.Context, _, _ string) (net.Listener, error) {
			var listenConfig net.ListenConfig
			return listenConfig.Listen(ctx, "tcp", "127.0.0.1:0")
		},
		prepare: func(context.Context, *broker.Server) (store.PresenceTransition, error) {
			cancel()
			return store.PresenceTransition{}, context.Canceled
		},
	})
	if err != nil {
		t.Fatalf("runBrokerService() error = %v", err)
	}
}

func TestBrokerServiceWarnsBeforeInsecureListen(t *testing.T) {
	configPath := privateTestPath(t, "config.json")
	var setupOutput bytes.Buffer
	var setupError bytes.Buffer
	if code := Run([]string{
		"setup", "broker",
		"--config", configPath,
		"--controller-id", runtimeControllerID,
		"--listen", "0.0.0.0:8787",
		"--auth-mode", "none",
		"--allow-insecure-nonloopback",
	}, &setupOutput, &setupError); code != 0 {
		t.Fatalf("setup code = %d, stderr = %q", code, setupError.String())
	}
	cfg, err := delegationconfig.Read(configPath)
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	injected := errors.New("stop after warning")
	err = runBrokerService(context.Background(), configPath, cfg, &stderr, brokerRuntimeOptions{
		listen: func(context.Context, string, string) (net.Listener, error) {
			if !strings.Contains(stderr.String(), "plaintext non-loopback") {
				return nil, errors.New("listener called before security warning")
			}
			return nil, injected
		},
	})
	if !errors.Is(err, injected) {
		t.Fatalf("runBrokerService() error = %v; stderr = %q", err, stderr.String())
	}
}

func setupBrokerRuntimeTest(t *testing.T, authMode string) (string, delegationconfig.Config) {
	t.Helper()
	configPath := privateTestPath(t, "config.json")
	args := []string{
		"setup", "broker",
		"--config", configPath,
		"--controller-id", runtimeControllerID,
		"--listen", "127.0.0.1:8787",
		"--auth-mode", authMode,
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if code := Run(args, &stdout, &stderr); code != 0 {
		t.Fatalf("setup code = %d, stderr = %q", code, stderr.String())
	}
	cfg, err := delegationconfig.Read(configPath)
	if err != nil {
		t.Fatal(err)
	}
	return configPath, cfg
}

func testBrokerListen(ready chan<- string) brokerRuntimeOptions {
	return brokerRuntimeOptions{
		listen: func(ctx context.Context, network, address string) (net.Listener, error) {
			if network != "tcp" || address != "127.0.0.1:8787" {
				return nil, fmt.Errorf("listen network/address = %q/%q", network, address)
			}
			var listenConfig net.ListenConfig
			listener, err := listenConfig.Listen(ctx, "tcp", "127.0.0.1:0")
			if err == nil {
				ready <- listener.Addr().String()
			}
			return listener, err
		},
	}
}

func waitForBrokerHealth(t *testing.T, address string) {
	t.Helper()
	client := http.Client{Timeout: 200 * time.Millisecond}
	deadline := time.Now().Add(5 * time.Second)
	for {
		response, err := client.Get("http://" + address + "/healthz")
		if err == nil {
			body, readErr := io.ReadAll(response.Body)
			response.Body.Close()
			if readErr == nil && response.StatusCode == http.StatusOK && string(body) == "ok\n" {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("broker health endpoint did not become ready: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func waitForBrokerAddress(t *testing.T, ready <-chan string, done <-chan error) string {
	t.Helper()
	select {
	case address := <-ready:
		return address
	case err := <-done:
		t.Fatalf("broker stopped before listening: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("broker did not create a listener")
	}
	panic("unreachable broker address wait")
}

func waitForBrokerStop(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runBrokerService() error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("broker service did not stop")
	}
}

func sendRuntimeHello(t *testing.T, connection *websocket.Conn, deviceID string) {
	t.Helper()
	payload, err := json.Marshal(protocol.Hello{
		ControllerID:   runtimeControllerID,
		DeviceID:       deviceID,
		DeviceName:     "runtime-worker",
		OS:             "linux",
		Arch:           "amd64",
		RuntimeVersion: "0.1.0-alpha.0.m1.1",
		Features: []string{
			protocol.FeatureDeviceRegistry,
			protocol.FeatureFullDuplexRPC,
			protocol.FeaturePeerRoot,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	requestID, err := protocol.NewRequestID(protocol.DirectionConnector)
	if err != nil {
		t.Fatal(err)
	}
	message, err := protocol.Marshal(protocol.Envelope{
		ProtocolVersion: protocol.Version,
		Kind:            protocol.KindRequest,
		RequestID:       requestID,
		Method:          protocol.MethodHello,
		ControllerID:    runtimeControllerID,
		Payload:         payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	helloContext, stopHello := context.WithTimeout(context.Background(), time.Second)
	defer stopHello()
	if err := connection.Write(helloContext, websocket.MessageText, message); err != nil {
		t.Fatal(err)
	}
	messageType, data, err := connection.Read(helloContext)
	if err != nil {
		t.Fatalf("read hello response: %v", err)
	}
	if messageType != websocket.MessageText {
		t.Fatalf("hello response type = %v", messageType)
	}
	response, err := protocol.Read(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if response.Error != nil || response.ReplyTo != requestID {
		t.Fatalf("hello response = %#v", response)
	}
}

func sendRuntimeHeartbeat(t *testing.T, connection *websocket.Conn) {
	t.Helper()
	payload, err := json.Marshal(protocol.Heartbeat{})
	if err != nil {
		t.Fatal(err)
	}
	requestID, err := protocol.NewRequestID(protocol.DirectionConnector)
	if err != nil {
		t.Fatal(err)
	}
	message, err := protocol.Marshal(protocol.Envelope{
		ProtocolVersion: protocol.Version,
		Kind:            protocol.KindRequest,
		RequestID:       requestID,
		Method:          protocol.MethodHeartbeat,
		ControllerID:    runtimeControllerID,
		Payload:         payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	heartbeatContext, stopHeartbeat := context.WithTimeout(context.Background(), time.Second)
	defer stopHeartbeat()
	if err := connection.Write(heartbeatContext, websocket.MessageText, message); err != nil {
		t.Fatal(err)
	}
	messageType, data, err := connection.Read(heartbeatContext)
	if err != nil {
		t.Fatalf("read heartbeat response: %v", err)
	}
	if messageType != websocket.MessageText {
		t.Fatalf("heartbeat response type = %v", messageType)
	}
	response, err := protocol.Read(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if response.Error != nil || response.ReplyTo != requestID {
		t.Fatalf("heartbeat response = %#v", response)
	}
}
