package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/broker"
	"github.com/GhostFlying/delegation/internal/clilaunch"
	"github.com/GhostFlying/delegation/internal/codexconfig"
	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/connector"
	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/hostkind"
	"github.com/GhostFlying/delegation/internal/identity"
	"github.com/GhostFlying/delegation/internal/localbridge"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/rootmcp"
	"github.com/GhostFlying/delegation/internal/serviceenv"
	"github.com/GhostFlying/delegation/internal/store"
	"github.com/GhostFlying/delegation/internal/tokenfile"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestReportConnectorErrorDistinguishesTerminalStateRecovery(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		want      string
		doNotWant string
	}{
		{
			name:      "terminal state recovery",
			err:       fmt.Errorf("%w: broker cursor is ahead", connector.ErrStateRecoveryRequired),
			want:      "connector halted; state recovery required",
			doNotWant: "reconnecting",
		},
		{
			name:      "retryable disconnect",
			err:       context.DeadlineExceeded,
			want:      "connector reconnecting",
			doNotWant: "connector halted",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			write := func(format string, args ...any) error {
				_, err := fmt.Fprintf(&output, format, args...)
				return err
			}
			if err := reportConnectorError(write, test.err); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(output.String(), test.want) ||
				strings.Contains(output.String(), test.doNotWant) {
				t.Fatalf("connector error output = %q", output.String())
			}
		})
	}
}

func TestResolveConfiguredCLILaunchPreservesArgumentBoundaries(t *testing.T) {
	root := t.TempDir()
	runtimeExecutable := filepath.Join(root, "runtime")
	launcherExecutable := filepath.Join(root, "launcher")
	for _, path := range []string{runtimeExecutable, launcherExecutable} {
		if err := os.WriteFile(path, []byte("test"), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	resolvedRuntime, err := filepath.EvalSymlinks(runtimeExecutable)
	if err != nil {
		t.Fatal(err)
	}
	resolvedLauncher, err := filepath.EvalSymlinks(launcherExecutable)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name       string
		configured delegationconfig.CLIConfig
		want       clilaunch.Spec
	}{
		{
			name: "direct",
			configured: delegationconfig.CLIConfig{
				Arguments: []string{"-p", "profile with spaces", "semi;colon"},
			},
			want: clilaunch.Spec{
				Executable:      resolvedRuntime,
				PrefixArguments: []string{"-p", "profile with spaces", "semi;colon"},
			},
		},
		{
			name: "wrapped",
			configured: delegationconfig.CLIConfig{
				Arguments: []string{"-p", "profile with spaces", "semi;colon"},
				Launcher: &clilaunch.Spec{
					Executable:      launcherExecutable,
					PrefixArguments: []string{"run", "--"},
				},
			},
			want: clilaunch.Spec{
				Executable: resolvedLauncher,
				PrefixArguments: []string{
					"run", "--", runtimeExecutable,
					"-p", "profile with spaces", "semi;colon",
				},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := resolveConfiguredCLILaunch(test.configured, runtimeExecutable)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("resolved CLI launch = %#v, want %#v", got, test.want)
			}
			got.PrefixArguments[0] = "changed"
			if test.configured.Arguments[0] != "-p" {
				t.Fatalf("resolved CLI launch retained caller arguments: %#v", test.configured)
			}
			if test.configured.Launcher != nil &&
				test.configured.Launcher.PrefixArguments[0] != "run" {
				t.Fatalf("resolved CLI launch retained launcher arguments: %#v", test.configured)
			}
		})
	}
}

func TestResolveConfiguredCLILaunchFollowsUpdatedLauncherSymlink(t *testing.T) {
	root := t.TempDir()
	firstTarget := filepath.Join(root, "warmpool-first")
	secondTarget := filepath.Join(root, "warmpool-second")
	launcherName := "warmpool"
	if os.PathSeparator == '\\' {
		launcherName += ".exe"
	}
	launcher := filepath.Join(root, launcherName)
	runtimeExecutable := filepath.Join(root, "traex")
	for _, path := range []string{firstTarget, secondTarget, runtimeExecutable} {
		if err := os.WriteFile(path, []byte("test"), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(firstTarget, launcher); err != nil {
		t.Skipf("symbolic links are unavailable: %v", err)
	}
	configuredLauncher, err := resolveCLILauncher(launcher, []string{"run", "--"})
	if err != nil {
		t.Fatal(err)
	}
	if configuredLauncher.Executable != launcher {
		t.Fatalf(
			"configured launcher = %q, want lexical path %q",
			configuredLauncher.Executable,
			launcher,
		)
	}
	resolve := func() clilaunch.Spec {
		t.Helper()
		resolved, resolveErr := resolveConfiguredCLILaunch(
			delegationconfig.CLIConfig{Launcher: &configuredLauncher},
			runtimeExecutable,
		)
		if resolveErr != nil {
			t.Fatal(resolveErr)
		}
		return resolved
	}
	first := resolve()
	resolvedFirstTarget, err := filepath.EvalSymlinks(firstTarget)
	if err != nil {
		t.Fatal(err)
	}
	if first.Executable != resolvedFirstTarget {
		t.Fatalf(
			"first resolved launcher = %q, want %q",
			first.Executable,
			resolvedFirstTarget,
		)
	}
	if err := os.Remove(launcher); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(secondTarget, launcher); err != nil {
		t.Fatal(err)
	}
	second := resolve()
	resolvedSecondTarget, err := filepath.EvalSymlinks(secondTarget)
	if err != nil {
		t.Fatal(err)
	}
	if second.Executable != resolvedSecondTarget {
		t.Fatalf(
			"updated resolved launcher = %q, want %q",
			second.Executable,
			resolvedSecondTarget,
		)
	}
}

const (
	runtimeWorkerID        = "123e4567-e89b-42d3-a456-426614174202"
	runtimeThreadID        = "123e4567-e89b-42d3-a456-426614174203"
	runtimeManagedThreadID = "123e4567-e89b-42d3-a456-426614174204"
)

type observedRootRegistry struct {
	*store.Store
	mu      sync.Mutex
	threads []string
}

func (r *observedRootRegistry) EnsureRootTree(
	ctx context.Context,
	controllerID, externalThreadID, rootDeviceID string,
	createdAt time.Time,
) (control.Tree, control.Principal, error) {
	r.mu.Lock()
	r.threads = append(r.threads, externalThreadID)
	r.mu.Unlock()
	return r.Store.EnsureRootTree(ctx, controllerID, externalThreadID, rootDeviceID, createdAt)
}

func (r *observedRootRegistry) rootThreads() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.threads...)
}

func TestCloseWorkerHostWaitsForTerminalCleanupAfterTimeout(t *testing.T) {
	terminalErr := errors.New("terminal worker cleanup failed")
	host := &blockingWorkerHostCloser{
		terminalStarted: make(chan struct{}),
		terminalGate:    make(chan struct{}),
		terminalErr:     terminalErr,
	}
	done := make(chan error, 1)
	go func() {
		done <- closeWorkerHost(host, 10*time.Millisecond)
	}()

	select {
	case <-host.terminalStarted:
	case <-time.After(time.Second):
		t.Fatal("terminal Host.Close did not start after the bounded timeout")
	}
	select {
	case err := <-done:
		t.Fatalf("closeWorkerHost returned before terminal cleanup completed: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(host.terminalGate)
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, terminalErr) {
			t.Fatalf("closeWorkerHost error = %v, want timeout and terminal error", err)
		}
	case <-time.After(time.Second):
		t.Fatal("closeWorkerHost did not return after terminal cleanup completed")
	}
}

type blockingWorkerHostCloser struct {
	mu              sync.Mutex
	calls           int
	terminalStarted chan struct{}
	terminalGate    chan struct{}
	terminalErr     error
}

func (h *blockingWorkerHostCloser) Close(ctx context.Context) error {
	h.mu.Lock()
	h.calls++
	call := h.calls
	h.mu.Unlock()
	if call == 1 {
		<-ctx.Done()
		return ctx.Err()
	}
	if call == 2 {
		close(h.terminalStarted)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-h.terminalGate:
		return h.terminalErr
	}
}

func TestPeerServicesRegisterDevicesAndServeRootBridge(t *testing.T) {
	t.Setenv(codexconfig.EnvironmentVariable, `{
		"model":"mock-model",
		"model_provider":"mock",
		"model_providers.mock":{
			"name":"Mock Responses provider",
			"base_url":"http://127.0.0.1:1/v1",
			"wire_api":"responses",
			"requires_openai_auth":false
		}
	}`)
	registryStore, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "state", "broker.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	registry := &observedRootRegistry{Store: registryStore}
	brokerServer, err := broker.New(broker.Options{
		ControllerID:      runtimeControllerID,
		Transport:         delegationconfig.TransportStatus{Transport: "tcp"},
		AuthMode:          delegationconfig.AuthModeNone,
		Registry:          registry,
		HeartbeatInterval: 500 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := brokerServer.Prepare(context.Background()); err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(brokerServer.Handler())
	t.Cleanup(func() {
		httpServer.Close()
		closeContext, cancel := context.WithTimeout(context.Background(), time.Second)
		if err := brokerServer.Close(closeContext); err != nil {
			t.Errorf("close broker: %v", err)
		}
		cancel()
		if err := registryStore.Close(); err != nil {
			t.Errorf("close registry: %v", err)
		}
	})
	brokerURL := "ws" + strings.TrimPrefix(httpServer.URL, "http")
	firstPath, firstConfig := setupConnectorRuntimeTest(
		t, runtimeDeviceID, "peer-a", brokerURL,
	)
	secondPath, secondConfig := setupConnectorRuntimeTest(
		t, runtimeWorkerID, "peer-c", brokerURL,
	)

	controllerContext, cancelController := context.WithCancel(context.Background())
	deviceContext, cancelDevice := context.WithCancel(context.Background())
	controllerDone := make(chan error, 1)
	deviceDone := make(chan error, 1)
	var controllerLog bytes.Buffer
	var deviceLog bytes.Buffer
	go func() {
		controllerDone <- runConnectorService(
			controllerContext, firstPath, firstConfig, &controllerLog,
		)
	}()
	go func() {
		deviceDone <- runConnectorService(deviceContext, secondPath, secondConfig, &deviceLog)
	}()
	stopped := false
	t.Cleanup(func() {
		if stopped {
			return
		}
		cancelController()
		cancelDevice()
		<-controllerDone
		<-deviceDone
	})
	waitForRuntimeDevice(t, registryStore, runtimeDeviceID, true)
	waitForRuntimeDevice(t, registryStore, runtimeWorkerID, true)

	endpoint, err := localbridge.Endpoint(runtimeControllerID, runtimeDeviceID)
	if err != nil {
		t.Fatal(err)
	}
	waitForReadyRuntimeStatus(
		t,
		endpoint,
		runtimeDeviceID,
		"peer-a",
		firstConfig.Transport.Status(),
		firstConfig.Peer.MaxWorkerSlots,
	)
	bridgeClient, err := localbridge.NewClient(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	peerState, err := store.OpenPeer(context.Background(), firstConfig.Peer.StateFile)
	if err != nil {
		t.Fatal(err)
	}
	managedPrincipal := control.NewWorkerPrincipal(
		runtimeControllerID,
		"123e4567-e89b-42d3-a456-426614174205",
		"123e4567-e89b-42d3-a456-426614174206",
		"123e4567-e89b-42d3-a456-426614174207",
		runtimeDeviceID,
	).Identity()
	reservation, err := peerState.ReserveWorker(context.Background(), store.WorkerReservation{
		WorkerKey: store.WorkerKey{
			ControllerID: managedPrincipal.ControllerID,
			TreeID:       managedPrincipal.TreeID,
			AgentID:      managedPrincipal.AgentID,
		},
		ParentAgentID:  managedPrincipal.ParentAgentID,
		DeviceID:       managedPrincipal.DeviceID,
		TaskName:       "managed root guard integration",
		PromptDigest:   strings.Repeat("2", 64),
		WorkspacePath:  filepath.Join(firstConfig.Peer.WorkspaceRoot, "root-guard"),
		ProfileVersion: 1,
	}, firstConfig.Peer.MaxWorkerSlots, time.Unix(300, 0))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := peerState.BeginWorkerStart(
		context.Background(), reservation.WorkerKey, firstConfig.Peer.MaxWorkerSlots, time.Unix(301, 0),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := peerState.AttachWorkerThread(
		context.Background(), reservation.WorkerKey, runtimeManagedThreadID, time.Unix(302, 0),
	); err != nil {
		t.Fatal(err)
	}
	if err := peerState.Close(); err != nil {
		t.Fatal(err)
	}
	callContext, cancelCall := context.WithTimeout(context.Background(), time.Second)
	err = bridgeClient.Call(
		callContext,
		protocol.MethodEnsureRootTree,
		"",
		nil,
		protocol.EnsureRootTreeParams{ExternalThreadID: runtimeManagedThreadID},
		&protocol.EnsureRootTreeResult{},
	)
	cancelCall()
	var bridgeError *localbridge.RPCError
	if !errors.As(err, &bridgeError) || bridgeError.Code != protocol.ErrorForbidden {
		t.Fatalf("managed thread root error = %v", err)
	}
	if threads := registry.rootThreads(); len(threads) != 0 {
		t.Fatalf("managed thread reached broker tree creation: %#v", threads)
	}
	callContext, cancelCall = context.WithTimeout(context.Background(), time.Second)
	var root protocol.EnsureRootTreeResult
	err = bridgeClient.Call(
		callContext,
		protocol.MethodEnsureRootTree,
		"",
		nil,
		protocol.EnsureRootTreeParams{ExternalThreadID: runtimeThreadID},
		&root,
	)
	cancelCall()
	if err != nil {
		t.Fatal(err)
	}
	if threads := registry.rootThreads(); !reflect.DeepEqual(threads, []string{runtimeThreadID}) {
		t.Fatalf("broker root tree calls = %#v", threads)
	}
	source := root.Principal.Identity()
	callContext, cancelCall = context.WithTimeout(context.Background(), time.Second)
	var devices protocol.ListDevicesResult
	err = bridgeClient.Call(
		callContext,
		protocol.MethodListDevices,
		root.Tree.TreeID,
		&source,
		protocol.ListDevicesParams{Limit: 10},
		&devices,
	)
	cancelCall()
	if err != nil {
		t.Fatal(err)
	}
	if len(devices.Devices) != 2 ||
		devices.Devices[0].DeviceID != runtimeDeviceID ||
		devices.Devices[1].DeviceID != runtimeWorkerID {
		t.Fatalf("devices through controller bridge = %#v", devices.Devices)
	}
	assertRootMCPDevices(t, firstPath)

	duplicateContext, cancelDuplicate := context.WithTimeout(context.Background(), time.Second)
	err = runConnectorService(duplicateContext, firstPath, firstConfig, &bytes.Buffer{})
	cancelDuplicate()
	if err == nil || !strings.Contains(err.Error(), "already running") {
		t.Fatalf("duplicate connector service error = %v", err)
	}

	cancelController()
	cancelDevice()
	if err := waitConnectorRuntime(controllerDone); err != nil {
		t.Fatal(err)
	}
	if err := waitConnectorRuntime(deviceDone); err != nil {
		t.Fatal(err)
	}
	stopped = true
	waitForRuntimeDevice(t, registryStore, runtimeDeviceID, false)
	waitForRuntimeDevice(t, registryStore, runtimeWorkerID, false)
	if !strings.Contains(controllerLog.String(), "peer connector service started") ||
		!strings.Contains(deviceLog.String(), "peer connector service started") {
		t.Fatalf("connector logs = %q / %q", controllerLog.String(), deviceLog.String())
	}
}

func TestPeerServiceUsesRoleLocalTailscaleRuntimeForConnectorDial(t *testing.T) {
	t.Setenv(codexconfig.EnvironmentVariable, `{
		"model":"mock-model",
		"model_provider":"mock",
		"model_providers.mock":{
			"name":"Mock Responses provider",
			"base_url":"http://127.0.0.1:1/v1",
			"wire_api":"responses",
			"requires_openai_auth":false
		}
	}`)
	registry, err := store.Open(
		context.Background(),
		filepath.Join(t.TempDir(), "state", "broker.sqlite3"),
	)
	if err != nil {
		t.Fatal(err)
	}
	brokerServer, err := broker.New(broker.Options{
		ControllerID:      runtimeControllerID,
		Transport:         delegationconfig.TransportStatus{Transport: "tcp"},
		AuthMode:          delegationconfig.AuthModeNone,
		Registry:          registry,
		HeartbeatInterval: 500 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := brokerServer.Prepare(context.Background()); err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(brokerServer.Handler())
	t.Cleanup(func() {
		httpServer.Close()
		closeContext, cancel := context.WithTimeout(context.Background(), time.Second)
		if err := brokerServer.Close(closeContext); err != nil {
			t.Errorf("close broker: %v", err)
		}
		cancel()
		if err := registry.Close(); err != nil {
			t.Errorf("close registry: %v", err)
		}
	})
	brokerAddress := strings.TrimPrefix(httpServer.URL, "http://")
	configPath, cfg := setupConnectorRuntimeTest(
		t,
		runtimeDeviceID,
		"tailscale-peer",
		"wss://setup-placeholder.example.test",
	)
	cfg.Transport = testTailscaleRuntimeConfig(t, "peer-node")
	cfg.Broker.URL = "ws://broker-node:8787/v1/connect"
	useDelegationTokenAuthentication(t, &cfg)
	var connectionClosed bool
	fakeRuntime := &fakeEmbeddedTailscaleRuntime{
		dial: func(ctx context.Context, network, address string) (net.Conn, error) {
			if network != "tcp" || address != "broker-node:8787" {
				return nil, fmt.Errorf("tailscale dial network/address = %q/%q", network, address)
			}
			var dialer net.Dialer
			connection, err := dialer.DialContext(ctx, "tcp", brokerAddress)
			if err != nil {
				return nil, err
			}
			return &observedConn{Conn: connection, closed: &connectionClosed}, nil
		},
		beforeClose: func() error {
			if !connectionClosed {
				return errors.New("tailscale runtime closed before connector connection drained")
			}
			return nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- runConnectorServiceWithProviderEnvironment(
			ctx,
			configPath,
			cfg,
			"",
			serviceenv.LoadInherited,
			io.Discard,
			connectorRuntimeOptions{
				newTailscale: func() embeddedTailscaleRuntime { return fakeRuntime },
			},
		)
	}()
	waitForRuntimeDevice(t, registry, runtimeDeviceID, true)
	cancel()
	if err := waitConnectorRuntime(done); err != nil {
		t.Fatal(err)
	}
	waitForRuntimeDevice(t, registry, runtimeDeviceID, false)
	if fakeRuntime.startCalls != 1 || fakeRuntime.dialCalls == 0 || fakeRuntime.closeCalls != 1 {
		t.Fatalf(
			"tailscale lifecycle = start %d, dial %d, close %d",
			fakeRuntime.startCalls,
			fakeRuntime.dialCalls,
			fakeRuntime.closeCalls,
		)
	}
	wantTailscale := cfg.Transport.Tailscale
	if fakeRuntime.startConfig.Dir != wantTailscale.StateDir ||
		fakeRuntime.startConfig.Hostname != wantTailscale.Hostname ||
		fakeRuntime.startConfig.AuthKeyFile != wantTailscale.AuthKeyFile {
		t.Fatalf("tailscale start config = %#v, want %#v", fakeRuntime.startConfig, wantTailscale)
	}
}

func TestPeerServiceReleasesResourcesWhenTailscaleStartFails(t *testing.T) {
	configPath, cfg := setupConnectorRuntimeTest(
		t,
		runtimeDeviceID,
		"tailscale-start-failure",
		"wss://setup-placeholder.example.test",
	)
	cfg.Transport = testTailscaleRuntimeConfig(t, "peer-node")
	cfg.Broker.URL = "ws://broker-node:8787/v1/connect"
	useDelegationTokenAuthentication(t, &cfg)
	startErr := errors.New("tailscale start failed")
	closeErr := errors.New("tailscale close failed")
	fakeRuntime := &fakeEmbeddedTailscaleRuntime{startErr: startErr, closeErr: closeErr}
	err := runConnectorServiceWithProviderEnvironment(
		context.Background(),
		configPath,
		cfg,
		"",
		func() (serviceenv.Resolved, error) { return serviceenv.Resolved{}, nil },
		io.Discard,
		connectorRuntimeOptions{
			newTailscale: func() embeddedTailscaleRuntime { return fakeRuntime },
		},
	)
	for _, want := range []error{startErr, closeErr} {
		if !errors.Is(err, want) {
			t.Fatalf("connector start error = %v, want joined %v", err, want)
		}
	}
	if fakeRuntime.startCalls != 1 || fakeRuntime.closeCalls != 1 {
		t.Fatalf(
			"failed tailscale lifecycle = start %d, close %d",
			fakeRuntime.startCalls,
			fakeRuntime.closeCalls,
		)
	}
	lease, err := store.AcquirePeerLease(cfg.Peer.StateFile)
	if err != nil {
		t.Fatalf("peer state lease was not released: %v", err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
}

func assertRootMCPDevices(t *testing.T, configPath string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server, err := loadRootMCPServer(configPath)
	if err != nil {
		t.Fatal(err)
	}
	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	serverSession, err := server.Connect(ctx, serverTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	client := mcp.NewClient(&mcp.Implementation{Name: "integration-test", Version: "1"}, nil)
	clientSession, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		serverSession.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := clientSession.Close(); err != nil {
			t.Errorf("close root MCP client: %v", err)
		}
		if err := serverSession.Close(); err != nil {
			t.Errorf("close root MCP server: %v", err)
		}
	})
	result, err := clientSession.CallTool(ctx, &mcp.CallToolParams{
		Meta:      mcp.Meta{"threadId": runtimeThreadID},
		Name:      rootmcp.ToolListDevices,
		Arguments: map[string]any{"limit": 4},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.IsError {
		t.Fatalf("list_devices through root MCP = %#v", result)
	}
	data, err := json.Marshal(result.StructuredContent)
	if err != nil {
		t.Fatal(err)
	}
	var output rootmcp.ListDevicesOutput
	if err := json.Unmarshal(data, &output); err != nil {
		t.Fatal(err)
	}
	if len(output.Devices) != 2 || output.Devices[0].DeviceID != runtimeDeviceID ||
		output.Devices[1].DeviceID != runtimeWorkerID {
		t.Fatalf("devices through root MCP = %#v", output.Devices)
	}
}

type observedConn struct {
	net.Conn
	closed *bool
}

func (c *observedConn) Close() error {
	*c.closed = true
	return c.Conn.Close()
}

func TestConnectorAuthorityIsReadBeforeRuntimeSideEffects(t *testing.T) {
	root := privateTestDirectory(t)
	tokenPath := filepath.Join(root, "device.token")
	if _, err := tokenfile.WriteNew(tokenPath, tokenfile.Token{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(root, "config.json")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if code := Run([]string{
		"setup", "peer",
		"--config", configPath,
		"--controller-id", runtimeControllerID,
		"--device-id", runtimeDeviceID,
		"--device-name", "controller",
		"--broker-url", "wss://broker.example.test",
		"--auth-mode", "token",
		"--token-file", tokenPath,
		"--codex-binary", testCodexBinary(t),
	}, &stdout, &stderr); code != 0 {
		t.Fatalf("setup code = %d, stderr = %q", code, stderr.String())
	}
	cfg, err := delegationconfig.Read(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := tokenfile.Validate(tokenPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(tokenPath); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = runConnectorService(ctx, configPath, cfg, &stderr)
	if err == nil || !strings.Contains(err.Error(), "read peer token") {
		t.Fatalf("missing connector authority error = %v", err)
	}
}

func TestConnectorAuthorityAppliesManagedHomePolicyByHostKind(t *testing.T) {
	configPath, cfg := setupConnectorRuntimeTest(
		t,
		runtimeDeviceID,
		"runtime-home-policy",
		"wss://broker.example.test",
	)
	if err := os.WriteFile(
		filepath.Join(cfg.Peer.CodexHome, "config.toml"),
		[]byte("model = \"managed\"\n"),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := loadConnectorAuthority(configPath, cfg); err == nil ||
		!strings.Contains(err.Error(), "config.toml") {
		t.Fatalf("Codex connector authority error = %v", err)
	}
	cfg.HostKind = hostkind.TraeX
	if _, err := loadConnectorAuthority(configPath, cfg); err == nil ||
		!strings.Contains(err.Error(), "config.toml") {
		t.Fatalf("TraeX connector authority error = %v", err)
	}
}

func TestPeerWorkerAuthorizerRequiresMatchingActiveReservation(t *testing.T) {
	ctx := context.Background()
	state, err := store.OpenPeer(ctx, filepath.Join(t.TempDir(), "state", "peer.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	principal := control.NewWorkerPrincipal(
		runtimeControllerID,
		"123e4567-e89b-42d3-a456-426614174210",
		"123e4567-e89b-42d3-a456-426614174211",
		"123e4567-e89b-42d3-a456-426614174212",
		runtimeDeviceID,
	).Identity()
	worker, err := state.ReserveWorker(ctx, store.WorkerReservation{
		WorkerKey: store.WorkerKey{
			ControllerID: principal.ControllerID,
			TreeID:       principal.TreeID,
			AgentID:      principal.AgentID,
		},
		ParentAgentID:  principal.ParentAgentID,
		DeviceID:       principal.DeviceID,
		TaskName:       "authorization test",
		PromptDigest:   strings.Repeat("0", 64),
		WorkspacePath:  filepath.Join(t.TempDir(), "workspace"),
		ProfileVersion: 1,
	}, 1, time.Unix(100, 0))
	if err != nil {
		t.Fatal(err)
	}
	authorizer := peerAuthorizer{
		state: state, controllerID: runtimeControllerID, deviceID: runtimeDeviceID,
	}
	if err := authorizer.AuthorizeWorker(ctx, principal); err == nil {
		t.Fatal("authorizer accepted a reserved worker before startup")
	}
	if _, err := state.BeginWorkerStart(ctx, worker.WorkerKey, 1, time.Unix(101, 0)); err != nil {
		t.Fatal(err)
	}
	if err := authorizer.AuthorizeWorker(ctx, principal); err == nil {
		t.Fatal("authorizer accepted a starting worker before MCP preflight")
	}
	if _, err := state.RestoreWorkerPendingAfterUnsent(
		ctx,
		worker.WorkerKey,
		time.Unix(102, 0),
	); err != nil {
		t.Fatal(err)
	}
	if err := authorizer.AuthorizeWorker(ctx, principal); err == nil {
		t.Fatal("authorizer accepted a pending worker")
	}
	if _, err := state.BeginWorkerStart(ctx, worker.WorkerKey, 1, time.Unix(103, 0)); err != nil {
		t.Fatal(err)
	}
	wrongParent := principal
	wrongParent.ParentAgentID = "123e4567-e89b-42d3-a456-426614174213"
	if err := authorizer.AuthorizeWorker(ctx, wrongParent); err == nil {
		t.Fatal("authorizer accepted a mismatched parent")
	}
	if _, err := state.AttachWorkerThread(
		ctx,
		worker.WorkerKey,
		"123e4567-e89b-42d3-a456-426614174214",
		time.Unix(104, 0),
	); err != nil {
		t.Fatal(err)
	}
	if err := authorizer.AuthorizeWorker(ctx, principal); err != nil {
		t.Fatal(err)
	}
	if _, err := state.MarkWorkerReady(ctx, worker.WorkerKey, time.Unix(104, 0)); err != nil {
		t.Fatal(err)
	}
	if _, err := state.MarkWorkerRunning(
		ctx,
		worker.WorkerKey,
		"123e4567-e89b-42d3-a456-426614174215",
		time.Unix(105, 0),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := state.RecoverWorkers(ctx, runtimeControllerID, runtimeDeviceID, time.Unix(106, 0)); err != nil {
		t.Fatal(err)
	}
	if err := authorizer.AuthorizeWorker(ctx, principal); err == nil {
		t.Fatal("authorizer accepted an interrupted worker")
	}
	if _, err := state.FailWorker(ctx, worker.WorkerKey, "startup_failed", time.Unix(107, 0)); err != nil {
		t.Fatal(err)
	}
	if err := authorizer.AuthorizeWorker(ctx, principal); err == nil {
		t.Fatal("authorizer accepted a failed worker")
	}
}

func TestPeerAuthorizerRejectsEveryManagedThreadStatusAndStoreFailure(t *testing.T) {
	tests := []struct {
		status     store.WorkerStatus
		transition func(context.Context, *store.PeerStore, store.WorkerReservation, time.Time) error
	}{
		{status: store.WorkerPreflight},
		{status: store.WorkerReady, transition: func(
			ctx context.Context, state *store.PeerStore, worker store.WorkerReservation, observedAt time.Time,
		) error {
			_, err := state.MarkWorkerReady(ctx, worker.WorkerKey, observedAt)
			return err
		}},
		{status: store.WorkerRunning, transition: func(
			ctx context.Context, state *store.PeerStore, worker store.WorkerReservation, observedAt time.Time,
		) error {
			if _, err := state.MarkWorkerReady(ctx, worker.WorkerKey, observedAt); err != nil {
				return err
			}
			turnID, err := identity.NewID()
			if err != nil {
				return err
			}
			_, err = state.MarkWorkerRunning(ctx, worker.WorkerKey, turnID, observedAt.Add(time.Second))
			return err
		}},
		{status: store.WorkerIdle, transition: func(
			ctx context.Context, state *store.PeerStore, worker store.WorkerReservation, observedAt time.Time,
		) error {
			if _, err := state.MarkWorkerReady(ctx, worker.WorkerKey, observedAt); err != nil {
				return err
			}
			turnID, err := identity.NewID()
			if err != nil {
				return err
			}
			if _, err := state.MarkWorkerRunning(
				ctx, worker.WorkerKey, turnID, observedAt.Add(time.Second),
			); err != nil {
				return err
			}
			_, err = state.MarkWorkerIdle(ctx, worker.WorkerKey, observedAt.Add(2*time.Second))
			return err
		}},
		{status: store.WorkerInterrupted, transition: func(
			ctx context.Context, state *store.PeerStore, worker store.WorkerReservation, observedAt time.Time,
		) error {
			if _, err := state.MarkWorkerReady(ctx, worker.WorkerKey, observedAt); err != nil {
				return err
			}
			_, err := state.RecoverWorkers(
				ctx, runtimeControllerID, runtimeDeviceID, observedAt.Add(time.Second),
			)
			return err
		}},
		{status: store.WorkerFailed, transition: func(
			ctx context.Context, state *store.PeerStore, worker store.WorkerReservation, observedAt time.Time,
		) error {
			_, err := state.FailWorker(ctx, worker.WorkerKey, "root_guard_test", observedAt)
			return err
		}},
	}
	for _, test := range tests {
		t.Run(string(test.status), func(t *testing.T) {
			ctx := context.Background()
			state, err := store.OpenPeer(ctx, filepath.Join(t.TempDir(), "state", "peer.sqlite3"))
			if err != nil {
				t.Fatal(err)
			}
			defer state.Close()
			authorizer := peerAuthorizer{
				state: state, controllerID: runtimeControllerID, deviceID: runtimeDeviceID,
			}
			ordinaryThreadID, err := identity.NewID()
			if err != nil {
				t.Fatal(err)
			}
			managed, err := authorizer.ManagedWorkerThread(ctx, runtimeControllerID, ordinaryThreadID)
			if err != nil || managed {
				t.Fatalf("ordinary thread managed = %v, error %v", managed, err)
			}
			managedThreadID, err := identity.NewID()
			if err != nil {
				t.Fatal(err)
			}
			worker := reserveManagedRootGuardWorker(t, ctx, state, managedThreadID)
			if test.transition != nil {
				if err := test.transition(ctx, state, worker, time.Unix(203, 0)); err != nil {
					t.Fatal(err)
				}
			}
			reservation, err := state.WorkerForThread(ctx, runtimeControllerID, managedThreadID)
			if err != nil || reservation.Status != test.status {
				t.Fatalf("managed worker = %#v, error %v", reservation, err)
			}
			managed, err = authorizer.ManagedWorkerThread(ctx, runtimeControllerID, managedThreadID)
			if err != nil || !managed {
				t.Fatalf("%s thread managed = %v, error %v", test.status, managed, err)
			}
		})
	}

	ctx := context.Background()
	state, err := store.OpenPeer(ctx, filepath.Join(t.TempDir(), "closed", "peer.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	authorizer := peerAuthorizer{
		state: state, controllerID: runtimeControllerID, deviceID: runtimeDeviceID,
	}
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
	managed, err := authorizer.ManagedWorkerThread(
		ctx, runtimeControllerID, "123e4567-e89b-42d3-a456-426614174220",
	)
	if err == nil || managed {
		t.Fatalf("closed peer state managed = %v, error %v", managed, err)
	}
}

func reserveManagedRootGuardWorker(
	t *testing.T,
	ctx context.Context,
	state *store.PeerStore,
	managedThreadID string,
) store.WorkerReservation {
	t.Helper()
	treeID, err := identity.NewID()
	if err != nil {
		t.Fatal(err)
	}
	agentID, err := identity.NewID()
	if err != nil {
		t.Fatal(err)
	}
	parentID, err := identity.NewID()
	if err != nil {
		t.Fatal(err)
	}
	worker, err := state.ReserveWorker(ctx, store.WorkerReservation{
		WorkerKey: store.WorkerKey{
			ControllerID: runtimeControllerID,
			TreeID:       treeID,
			AgentID:      agentID,
		},
		ParentAgentID:  parentID,
		DeviceID:       runtimeDeviceID,
		TaskName:       "root guard status test",
		PromptDigest:   strings.Repeat("1", 64),
		WorkspacePath:  filepath.Join(t.TempDir(), "workspace"),
		ProfileVersion: 1,
	}, 1, time.Unix(200, 0))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.BeginWorkerStart(ctx, worker.WorkerKey, 1, time.Unix(201, 0)); err != nil {
		t.Fatal(err)
	}
	worker, err = state.AttachWorkerThread(ctx, worker.WorkerKey, managedThreadID, time.Unix(202, 0))
	if err != nil {
		t.Fatal(err)
	}
	return worker
}

func setupConnectorRuntimeTest(
	t *testing.T,
	deviceID, deviceName, brokerURL string,
) (string, delegationconfig.Config) {
	t.Helper()
	configPath := privateTestPath(t, deviceName+".json")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if code := Run([]string{
		"setup", "peer",
		"--config", configPath,
		"--controller-id", runtimeControllerID,
		"--device-id", deviceID,
		"--device-name", deviceName,
		"--broker-url", brokerURL,
		"--auth-mode", "none",
		"--codex-binary", testCodexBinary(t),
	}, &stdout, &stderr); code != 0 {
		t.Fatalf("setup peer code = %d, stderr = %q", code, stderr.String())
	}
	cfg, err := delegationconfig.Read(configPath)
	if err != nil {
		t.Fatal(err)
	}
	return configPath, cfg
}

func waitForRuntimeDevice(t *testing.T, registry *store.Store, deviceID string, online bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		record, err := registry.DescribeDevice(context.Background(), runtimeControllerID, deviceID)
		if err == nil && record.Device.Online == online {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("device %s online state did not become %v", deviceID, online)
}

func waitForReadyRuntimeStatus(
	t *testing.T,
	endpoint, deviceID, deviceName string,
	transport delegationconfig.TransportStatus,
	maxWorkerSlots int,
) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var (
		lastStatus localbridge.StatusSnapshot
		lastErr    error
	)
	for time.Now().Before(deadline) {
		statusContext, cancelStatus := context.WithTimeout(context.Background(), 250*time.Millisecond)
		lastStatus, lastErr = localbridge.ReadStatus(statusContext, endpoint)
		cancelStatus()
		if lastErr == nil && lastStatus.Connected && lastStatus.WorkerSyncReady &&
			lastStatus.ControllerID == runtimeControllerID && lastStatus.DeviceID == deviceID &&
			lastStatus.DeviceName == deviceName && lastStatus.TransportStatus == transport &&
			lastStatus.MaxWorkerSlots == maxWorkerSlots {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("connector local status did not become ready: status=%#v, error=%v", lastStatus, lastErr)
}

func waitConnectorRuntime(done <-chan error) error {
	select {
	case err := <-done:
		return err
	case <-time.After(2 * time.Second):
		return errors.New("connector runtime did not stop")
	}
}
