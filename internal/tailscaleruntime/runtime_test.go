package tailscaleruntime

import (
	"context"
	"errors"
	"io"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"tailscale.com/ipn/ipnstate"
	"tailscale.com/types/key"
)

const runtimeTestAuthKey = "tskey-auth-runtime-test-value"

func TestRuntimeStartConfiguresNodeAndClearsAuthKey(t *testing.T) {
	t.Setenv("TS_CONTROL_URL", "https://headscale.example.test")
	authKeyFile := runtimeAuthKeyFile(t)
	fake := &fakeNode{}
	lease := &fakeCloser{}
	var got nodeConfig
	var leasedDir string
	runtime := newRuntime(
		func(cfg nodeConfig) (node, error) {
			info, err := os.Lstat(cfg.Dir)
			if err != nil {
				t.Fatalf("inspect prepared state directory: %v", err)
			}
			if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
				t.Fatalf("prepared state directory = %#v", info.Mode())
			}
			if runtime.GOOS != "windows" && info.Mode().Perm() != 0o700 {
				t.Fatalf("prepared state directory mode = %o, want 0700", info.Mode().Perm())
			}
			got = cfg
			fake.authKey = cfg.AuthKey
			return fake, nil
		},
		func(dir string) (io.Closer, error) {
			leasedDir = dir
			return lease, nil
		},
	)
	cfg := Config{
		Dir:         filepath.Join(t.TempDir(), "tailscale"),
		Hostname:    "runtime-test",
		AuthKeyFile: authKeyFile,
	}
	if err := runtime.Start(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if leasedDir != cfg.Dir {
		t.Fatalf("lease state directory = %q, want %q", leasedDir, cfg.Dir)
	}
	if got.Dir != cfg.Dir || got.Hostname != cfg.Hostname || got.AuthKey != runtimeTestAuthKey {
		t.Fatalf("node config = %#v", got)
	}
	if got.ControlURL != defaultControlURL {
		t.Fatalf("node control URL = %q, want %q", got.ControlURL, defaultControlURL)
	}
	if got.Ephemeral {
		t.Fatal("runtime configured an ephemeral node")
	}
	if got.Logf == nil || got.UserLogf == nil {
		t.Fatal("runtime did not explicitly suppress both logger surfaces")
	}
	got.Logf("discarded backend secret %s", runtimeTestAuthKey)
	got.UserLogf("discarded user secret %s", runtimeTestAuthKey)
	if !fake.started || !fake.upCalled {
		t.Fatalf("node lifecycle = start %v, up %v", fake.started, fake.upCalled)
	}
	if fake.authKey != "" {
		t.Fatal("runtime retained the auth key on the started node")
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	if !fake.closed || lease.closeCalls != 1 {
		t.Fatalf("cleanup = node closed %v, lease closes %d", fake.closed, lease.closeCalls)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	if lease.closeCalls != 1 {
		t.Fatalf("idempotent Close closed lease %d times", lease.closeCalls)
	}
}

func TestRuntimeRejectsForceLoginBeforeLeaseOrNode(t *testing.T) {
	for _, value := range []string{"1", "true", "not-a-boolean"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv(forceLoginEnvironment, value)
			leaseCalled := false
			nodeCalled := false
			runtime := newRuntime(
				func(nodeConfig) (node, error) {
					nodeCalled = true
					return &fakeNode{}, nil
				},
				func(string) (io.Closer, error) {
					leaseCalled = true
					return &fakeCloser{}, nil
				},
			)
			err := runtime.Start(context.Background(), Config{
				Dir:         filepath.Join(t.TempDir(), "tailscale"),
				Hostname:    "force-login",
				AuthKeyFile: runtimeAuthKeyFile(t),
			})
			if err == nil || !strings.Contains(err.Error(), forceLoginEnvironment) {
				t.Fatalf("Start error = %v, want forced-login rejection", err)
			}
			if leaseCalled || nodeCalled {
				t.Fatalf("forced-login rejection side effects = lease %v, node %v", leaseCalled, nodeCalled)
			}
		})
	}
}

func TestRuntimePreCanceledStartHasNoSideEffects(t *testing.T) {
	leaseCalled := false
	nodeCalled := false
	runtime := newRuntime(
		func(nodeConfig) (node, error) {
			nodeCalled = true
			return &fakeNode{}, nil
		},
		func(string) (io.Closer, error) {
			leaseCalled = true
			return &fakeCloser{}, nil
		},
	)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := runtime.Start(ctx, Config{
		Dir:         filepath.Join(t.TempDir(), "tailscale"),
		Hostname:    "pre-canceled",
		AuthKeyFile: filepath.Join(t.TempDir(), "missing-auth.key"),
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Start error = %v, want context cancellation", err)
	}
	if leaseCalled || nodeCalled {
		t.Fatalf("pre-canceled Start side effects = lease %v, node %v", leaseCalled, nodeCalled)
	}
}

func TestRuntimeSameDirRejectionPrecedesKeyReadAndNodeCreation(t *testing.T) {
	leaseErr := errors.New("state directory already leased")
	nodeCalled := false
	runtime := newRuntime(
		func(nodeConfig) (node, error) {
			nodeCalled = true
			return &fakeNode{}, nil
		},
		func(string) (io.Closer, error) { return nil, leaseErr },
	)
	err := runtime.Start(context.Background(), Config{
		Dir:         filepath.Join(t.TempDir(), "tailscale"),
		Hostname:    "same-dir",
		AuthKeyFile: filepath.Join(t.TempDir(), "missing-auth.key"),
	})
	if !errors.Is(err, leaseErr) {
		t.Fatalf("Start error = %v, want lease error", err)
	}
	if nodeCalled {
		t.Fatal("same-dir rejection constructed a node")
	}
}

func TestRuntimeAuthKeyReadFailureReleasesLeaseBeforeNodeCreation(t *testing.T) {
	lease := &fakeCloser{}
	nodeCalled := false
	runtime := newRuntime(
		func(nodeConfig) (node, error) {
			nodeCalled = true
			return &fakeNode{}, nil
		},
		func(string) (io.Closer, error) { return lease, nil },
	)
	err := runtime.Start(context.Background(), Config{
		Dir:         filepath.Join(t.TempDir(), "tailscale"),
		Hostname:    "missing-auth-key",
		AuthKeyFile: filepath.Join(t.TempDir(), "missing-auth.key"),
	})
	if err == nil {
		t.Fatal("Start accepted a missing auth key")
	}
	if nodeCalled {
		t.Fatal("auth-key read failure constructed a node")
	}
	if lease.closeCalls != 1 {
		t.Fatalf("auth-key read failure closed lease %d times", lease.closeCalls)
	}
}

func TestRuntimeNodeFactoryFailureReleasesLease(t *testing.T) {
	nodeErr := errors.New("node factory failed")
	lease := &fakeCloser{}
	runtime := newRuntime(
		func(nodeConfig) (node, error) { return nil, nodeErr },
		func(string) (io.Closer, error) { return lease, nil },
	)
	err := runtime.Start(context.Background(), Config{
		Dir:         filepath.Join(t.TempDir(), "tailscale"),
		Hostname:    "node-factory-failure",
		AuthKeyFile: runtimeAuthKeyFile(t),
	})
	if !errors.Is(err, nodeErr) {
		t.Fatalf("Start error = %v, want %v", err, nodeErr)
	}
	if lease.closeCalls != 1 {
		t.Fatalf("node factory failure closed lease %d times", lease.closeCalls)
	}
}

func TestRuntimePrivateDirectoryFailurePrecedesKeyReadAndNodeCreation(t *testing.T) {
	root := filepath.Join(t.TempDir(), "authority")
	if err := delegationconfig.PreparePrivateDirectory(root); err != nil {
		t.Fatal(err)
	}
	stateDir := filepath.Join(root, "tailscale")
	if err := os.WriteFile(stateDir, []byte("not a directory"), 0o600); err != nil {
		t.Fatal(err)
	}
	lease := &fakeCloser{}
	nodeCalled := false
	runtime := newRuntime(
		func(nodeConfig) (node, error) {
			nodeCalled = true
			return &fakeNode{}, nil
		},
		func(string) (io.Closer, error) { return lease, nil },
	)
	err := runtime.Start(context.Background(), Config{
		Dir:         stateDir,
		Hostname:    "invalid-state-dir",
		AuthKeyFile: filepath.Join(root, "missing-auth.key"),
	})
	if err == nil || !strings.Contains(err.Error(), "private tailscale state directory") {
		t.Fatalf("Start error = %v, want private-directory failure", err)
	}
	if nodeCalled {
		t.Fatal("private-directory failure constructed a node")
	}
	if lease.closeCalls != 1 {
		t.Fatalf("private-directory failure closed lease %d times", lease.closeCalls)
	}
	data, readErr := os.ReadFile(stateDir)
	if readErr != nil || string(data) != "not a directory" {
		t.Fatalf("existing state path changed: %q, %v", data, readErr)
	}
}

func TestRuntimeRepairsExistingStateDirectoryWithoutChangingState(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix mode repair assertion")
	}
	root := filepath.Join(t.TempDir(), "authority")
	if err := delegationconfig.PreparePrivateDirectory(root); err != nil {
		t.Fatal(err)
	}
	stateDir := filepath.Join(root, "tailscale")
	if err := os.Mkdir(stateDir, 0o755); err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(stateDir, "tailscaled.state")
	state := []byte("existing tailscale state")
	if err := os.WriteFile(statePath, state, 0o600); err != nil {
		t.Fatal(err)
	}
	fake := &fakeNode{}
	lease := &fakeCloser{}
	runtime := newRuntime(
		func(cfg nodeConfig) (node, error) {
			info, err := os.Stat(cfg.Dir)
			if err != nil {
				t.Fatal(err)
			}
			if info.Mode().Perm() != 0o700 {
				t.Fatalf("state directory mode before node creation = %o, want 0700", info.Mode().Perm())
			}
			return fake, nil
		},
		func(string) (io.Closer, error) { return lease, nil },
	)
	if err := runtime.Start(context.Background(), Config{
		Dir:         stateDir,
		Hostname:    "existing-state",
		AuthKeyFile: runtimeAuthKeyFile(t),
	}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(state) {
		t.Fatalf("existing state = %q, want %q", got, state)
	}
	if lease.closeCalls != 1 {
		t.Fatalf("lease close calls = %d, want 1", lease.closeCalls)
	}
}

func TestRuntimeCloseClosesNodeBeforeLease(t *testing.T) {
	var order []string
	fake := &fakeNode{closeFunc: func() { order = append(order, "node") }}
	lease := &fakeCloser{closeFunc: func() { order = append(order, "lease") }}
	runtime := newRuntime(
		func(cfg nodeConfig) (node, error) {
			fake.authKey = cfg.AuthKey
			return fake, nil
		},
		func(string) (io.Closer, error) { return lease, nil },
	)
	if err := runtime.Start(context.Background(), Config{
		Dir:         filepath.Join(t.TempDir(), "tailscale"),
		Hostname:    "close-order",
		AuthKeyFile: runtimeAuthKeyFile(t),
	}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	if strings.Join(order, ",") != "node,lease" {
		t.Fatalf("close order = %q, want node,lease", order)
	}
}

func TestRuntimeCloseWaitsForStart(t *testing.T) {
	started := make(chan struct{})
	releaseStart := make(chan struct{})
	fake := &fakeNode{
		startFunc: func() error {
			close(started)
			<-releaseStart
			return nil
		},
	}
	runtime := newRuntime(
		func(cfg nodeConfig) (node, error) {
			fake.authKey = cfg.AuthKey
			return fake, nil
		},
		func(string) (io.Closer, error) { return &fakeCloser{}, nil },
	)
	startDone := make(chan error, 1)
	go func() {
		startDone <- runtime.Start(context.Background(), Config{
			Dir:         filepath.Join(t.TempDir(), "tailscale"),
			Hostname:    "serialized-start",
			AuthKeyFile: runtimeAuthKeyFile(t),
		})
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("node Start was not called")
	}
	closeDone := make(chan error, 1)
	go func() {
		closeDone <- runtime.Close()
	}()
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned while Start was running: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if fake.closed {
		t.Fatal("node was closed concurrently with Start")
	}
	close(releaseStart)
	if err := <-startDone; err != nil {
		t.Fatal(err)
	}
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}
	if !fake.closed {
		t.Fatal("node was not closed after Start completed")
	}
}

func TestRuntimeStartFailureJoinsCleanupErrors(t *testing.T) {
	startErr := errors.New("start failed")
	nodeCloseErr := errors.New("node close failed")
	leaseCloseErr := errors.New("lease close failed")
	fake := &fakeNode{startErr: startErr, closeErr: nodeCloseErr}
	lease := &fakeCloser{err: leaseCloseErr}
	runtime := newRuntime(
		func(cfg nodeConfig) (node, error) {
			fake.authKey = cfg.AuthKey
			return fake, nil
		},
		func(string) (io.Closer, error) { return lease, nil },
	)
	err := runtime.Start(context.Background(), Config{
		Dir:         filepath.Join(t.TempDir(), "tailscale"),
		Hostname:    "failed-start",
		AuthKeyFile: runtimeAuthKeyFile(t),
	})
	for _, want := range []error{startErr, nodeCloseErr, leaseCloseErr} {
		if !errors.Is(err, want) {
			t.Fatalf("Start error = %v, want joined %v", err, want)
		}
	}
	if fake.authKey != "" {
		t.Fatal("failed node retained its auth key")
	}
	if err := runtime.Close(); err != nil {
		t.Fatalf("Close after failed Start = %v", err)
	}
	if lease.closeCalls != 1 {
		t.Fatalf("failed Start closed lease %d times", lease.closeCalls)
	}
}

func TestRuntimeReadinessFailureReleasesResources(t *testing.T) {
	upErr := errors.New("node not ready")
	fake := &fakeNode{upErr: upErr}
	lease := &fakeCloser{}
	runtime := newRuntime(
		func(cfg nodeConfig) (node, error) {
			fake.authKey = cfg.AuthKey
			return fake, nil
		},
		func(string) (io.Closer, error) { return lease, nil },
	)
	err := runtime.Start(context.Background(), Config{
		Dir:         filepath.Join(t.TempDir(), "tailscale"),
		Hostname:    "failed-readiness",
		AuthKeyFile: runtimeAuthKeyFile(t),
	})
	if !errors.Is(err, upErr) {
		t.Fatalf("Start error = %v, want %v", err, upErr)
	}
	if !fake.closed || lease.closeCalls != 1 {
		t.Fatalf("failed readiness cleanup = node %v, lease %d", fake.closed, lease.closeCalls)
	}
}

func TestRuntimeListenAndDialRequireStartedRuntime(t *testing.T) {
	runtime := newRuntime(
		func(nodeConfig) (node, error) { return &fakeNode{}, nil },
		func(string) (io.Closer, error) { return &fakeCloser{}, nil },
	)
	if _, err := runtime.Listen(context.Background(), "tcp", ":8787"); err == nil {
		t.Fatal("Listen accepted an unstarted runtime")
	}
	if _, err := runtime.Dial(context.Background(), "tcp", "broker.test:8787"); err == nil {
		t.Fatal("Dial accepted an unstarted runtime")
	}
}

func TestRuntimeDialRejectsSystemResolvableUnknownHostnameBeforeDial(t *testing.T) {
	fake := &fakeNode{status: runningPeerStatus(
		"broker.tailnet.example.",
		true,
		"100.64.0.10",
	)}
	runtime := &Runtime{node: fake}

	if _, err := runtime.Dial(context.Background(), "tcp", "localhost:8787"); err == nil ||
		!strings.Contains(err.Error(), "not an online peer") {
		t.Fatalf("Dial error = %v, want unknown peer rejection", err)
	}
	if fake.statusCalls != 1 {
		t.Fatalf("Status calls = %d, want 1", fake.statusCalls)
	}
	if fake.dialPeerCalls != 0 {
		t.Fatalf("unknown hostname reached peer dial %d times", fake.dialPeerCalls)
	}
}

func TestRuntimeDialAcceptsMagicDNSPeer(t *testing.T) {
	for _, test := range []struct {
		name    string
		network string
		address string
		want    string
	}{
		{
			name:    "short name prefers IPv4",
			network: "tcp",
			address: "broker:8787",
			want:    "100.64.0.10:8787",
		},
		{
			name:    "case insensitive FQDN",
			network: "tcp4",
			address: "BROKER.TAILNET.EXAMPLE.:443",
			want:    "100.64.0.10:443",
		},
		{
			name:    "IPv6 preserves port",
			network: "tcp6",
			address: "broker:65535",
			want:    "[fd7a:115c:a1e0::10]:65535",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fake := &fakeNode{status: runningPeerStatus(
				"broker.tailnet.example.",
				true,
				"100.64.0.10",
				"fd7a:115c:a1e0::10",
			)}
			fake.dialPeerFunc = successfulPeerDial(t)
			runtime := &Runtime{node: fake}

			connection, err := runtime.Dial(
				context.Background(),
				test.network,
				test.address,
			)
			if err != nil {
				t.Fatal(err)
			}
			connection.Close()
			if fake.statusCalls != 2 {
				t.Fatalf("Status calls = %d, want 2", fake.statusCalls)
			}
			if fake.dialPeerCalls != 1 || fake.dialPeerAddress.String() != test.want {
				t.Fatalf(
					"peer dial = %d calls to %q, want one call to %q",
					fake.dialPeerCalls,
					fake.dialPeerAddress,
					test.want,
				)
			}
		})
	}
}

func TestRuntimeDialAcceptsLiteralOnlinePeerIP(t *testing.T) {
	fake := &fakeNode{status: runningPeerStatus(
		"broker.tailnet.example.",
		true,
		"100.64.0.10",
	)}
	fake.dialPeerFunc = successfulPeerDial(t)
	runtime := &Runtime{node: fake}

	connection, err := runtime.Dial(
		context.Background(),
		"tcp4",
		"100.64.0.10:8787",
	)
	if err != nil {
		t.Fatal(err)
	}
	connection.Close()
	if fake.dialPeerAddress.String() != "100.64.0.10:8787" {
		t.Fatalf("peer dial address = %q", fake.dialPeerAddress)
	}
}

func TestRuntimeDialRejectsOfflinePeer(t *testing.T) {
	fake := &fakeNode{status: runningPeerStatus(
		"broker.tailnet.example.",
		false,
		"100.64.0.10",
	)}
	runtime := &Runtime{node: fake}

	if _, err := runtime.Dial(context.Background(), "tcp", "broker:8787"); err == nil ||
		!strings.Contains(err.Error(), "not an online peer") {
		t.Fatalf("Dial error = %v, want offline peer rejection", err)
	}
	if fake.dialPeerCalls != 0 {
		t.Fatalf("offline peer reached peer dial %d times", fake.dialPeerCalls)
	}
}

func TestRuntimeDialRejectsAmbiguousMagicDNSIdentity(t *testing.T) {
	status := runningPeerStatus(
		"broker.tailnet.example.",
		true,
		"100.64.0.10",
	)
	status.Peer[key.NewNode().Public()] = &ipnstate.PeerStatus{
		DNSName:      "broker.tailnet.example.",
		TailscaleIPs: []netip.Addr{netip.MustParseAddr("100.64.0.11")},
		Online:       true,
	}
	fake := &fakeNode{status: status}
	runtime := &Runtime{node: fake}

	if _, err := runtime.Dial(context.Background(), "tcp", "broker:8787"); err == nil ||
		!strings.Contains(err.Error(), "multiple online peers match") {
		t.Fatalf("Dial error = %v, want ambiguous peer rejection", err)
	}
	if fake.dialPeerCalls != 0 {
		t.Fatalf("ambiguous identity reached peer dial %d times", fake.dialPeerCalls)
	}
}

func TestRuntimeDialRejectsSubnetAndExitRoutes(t *testing.T) {
	status := runningPeerStatus(
		"router.tailnet.example.",
		true,
		"100.64.0.20",
	)
	status.ExitNodeStatus = &ipnstate.ExitNodeStatus{Online: true}
	fake := &fakeNode{status: status}
	runtime := &Runtime{node: fake}

	for _, address := range []string{"10.20.30.40:8787", "203.0.113.10:8787"} {
		if _, err := runtime.Dial(context.Background(), "tcp", address); err == nil ||
			!strings.Contains(err.Error(), "not assigned to an online peer") {
			t.Fatalf("Dial(%q) error = %v, want routed-address rejection", address, err)
		}
	}
	if fake.dialPeerCalls != 0 {
		t.Fatalf("routed address reached peer dial %d times", fake.dialPeerCalls)
	}
}

func TestRuntimeDialClassificationFailureFailsClosed(t *testing.T) {
	statusErr := errors.New("status unavailable")
	fake := &fakeNode{statusErr: statusErr}
	runtime := &Runtime{node: fake}

	if _, err := runtime.Dial(context.Background(), "tcp", "broker:8787"); !errors.Is(err, statusErr) {
		t.Fatalf("Dial error = %v, want %v", err, statusErr)
	}
	if fake.dialPeerCalls != 0 {
		t.Fatalf("classification failure reached peer dial %d times", fake.dialPeerCalls)
	}
}

func TestRuntimeDialRouteDisappearanceFailsWithoutSystemFallback(t *testing.T) {
	routeErr := errors.New("peer route disappeared")
	fake := &fakeNode{
		status: runningPeerStatus(
			"broker.tailnet.example.",
			true,
			"100.64.0.10",
		),
		dialPeerFunc: func(context.Context, netip.AddrPort) (net.Conn, error) {
			return nil, routeErr
		},
	}
	runtime := &Runtime{node: fake}

	if _, err := runtime.Dial(context.Background(), "tcp", "broker:8787"); !errors.Is(err, routeErr) {
		t.Fatalf("Dial error = %v, want %v", err, routeErr)
	}
	if fake.statusCalls != 1 || fake.dialPeerCalls != 1 {
		t.Fatalf(
			"route disappearance calls = status %d, peer dial %d",
			fake.statusCalls,
			fake.dialPeerCalls,
		)
	}
	if fake.dialPeerAddress.String() != "100.64.0.10:8787" {
		t.Fatalf("classified peer address = %q", fake.dialPeerAddress)
	}
}

func TestRuntimeDialClosesConnectionWhenPeerDisappearsAfterConnect(t *testing.T) {
	status := runningPeerStatus(
		"broker.tailnet.example.",
		true,
		"100.64.0.10",
	)
	var server net.Conn
	fake := &fakeNode{
		status: status,
		dialPeerFunc: func(context.Context, netip.AddrPort) (net.Conn, error) {
			clientConnection, serverConnection := net.Pipe()
			server = serverConnection
			for _, peer := range status.Peer {
				peer.Online = false
			}
			return clientConnection, nil
		},
	}
	runtime := &Runtime{node: fake}

	_, err := runtime.Dial(context.Background(), "tcp", "broker:8787")
	if err == nil || !strings.Contains(err.Error(), "no longer online") {
		t.Fatalf("Dial error = %v, want post-connect peer rejection", err)
	}
	if fake.statusCalls != 2 || fake.dialPeerCalls != 1 {
		t.Fatalf(
			"post-connect disappearance calls = status %d, peer dial %d",
			fake.statusCalls,
			fake.dialPeerCalls,
		)
	}
	if server == nil {
		t.Fatal("peer dial did not create a connection")
	}
	defer server.Close()
	if _, readErr := server.Read(make([]byte, 1)); !errors.Is(readErr, io.EOF) {
		t.Fatalf("rejected connection read error = %v, want EOF", readErr)
	}
}

func TestRuntimeDialPreservesLiteralIPv6AndPort(t *testing.T) {
	fake := &fakeNode{status: runningPeerStatus(
		"broker.tailnet.example.",
		true,
		"fd7a:115c:a1e0::10",
	)}
	fake.dialPeerFunc = successfulPeerDial(t)
	runtime := &Runtime{node: fake}

	connection, err := runtime.Dial(
		context.Background(),
		"tcp6",
		"[fd7a:115c:a1e0::10]:65535",
	)
	if err != nil {
		t.Fatal(err)
	}
	connection.Close()
	if fake.dialPeerAddress.String() != "[fd7a:115c:a1e0::10]:65535" {
		t.Fatalf("peer dial address = %q", fake.dialPeerAddress)
	}
}

func TestRuntimeDialRejectsNonTCPAndAddressFamilyMismatch(t *testing.T) {
	fake := &fakeNode{status: runningPeerStatus(
		"broker.tailnet.example.",
		true,
		"100.64.0.10",
	)}
	runtime := &Runtime{node: fake}

	for _, test := range []struct {
		network string
		address string
	}{
		{network: "udp", address: "broker:8787"},
		{network: "tcp6", address: "100.64.0.10:8787"},
	} {
		if _, err := runtime.Dial(
			context.Background(),
			test.network,
			test.address,
		); err == nil {
			t.Fatalf("Dial(%q, %q) succeeded", test.network, test.address)
		}
	}
	if fake.dialPeerCalls != 0 {
		t.Fatalf("invalid target reached peer dial %d times", fake.dialPeerCalls)
	}
}

type fakeNode struct {
	mu              sync.Mutex
	authKey         string
	started         bool
	upCalled        bool
	closed          bool
	startErr        error
	upErr           error
	closeErr        error
	status          *ipnstate.Status
	statusErr       error
	statusCalls     int
	dialPeerAddress netip.AddrPort
	dialPeerCalls   int
	startFunc       func() error
	closeFunc       func()
	dialPeerFunc    func(context.Context, netip.AddrPort) (net.Conn, error)
}

func (n *fakeNode) Start() error {
	n.mu.Lock()
	n.started = true
	startFunc := n.startFunc
	startErr := n.startErr
	n.mu.Unlock()
	if startFunc != nil {
		return startFunc()
	}
	return startErr
}

func (n *fakeNode) ClearAuthKey() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.authKey = ""
}

func (n *fakeNode) Up(context.Context) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.upCalled = true
	if n.authKey != "" {
		return errors.New("node auth key was not cleared before readiness")
	}
	return n.upErr
}

func (n *fakeNode) Listen(string, string) (net.Listener, error) {
	return nil, errors.New("unused fake Listen")
}

func (n *fakeNode) Status(context.Context) (*ipnstate.Status, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.statusCalls++
	return n.status, n.statusErr
}

func (n *fakeNode) DialPeerTCP(
	ctx context.Context,
	address netip.AddrPort,
) (net.Conn, error) {
	n.mu.Lock()
	n.dialPeerCalls++
	n.dialPeerAddress = address
	dialPeerFunc := n.dialPeerFunc
	n.mu.Unlock()
	if dialPeerFunc == nil {
		return nil, errors.New("unused fake peer Dial")
	}
	return dialPeerFunc(ctx, address)
}

func (n *fakeNode) Close() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.closed = true
	if n.closeFunc != nil {
		n.closeFunc()
	}
	return n.closeErr
}

type fakeCloser struct {
	closeCalls int
	err        error
	closeFunc  func()
}

func (c *fakeCloser) Close() error {
	c.closeCalls++
	if c.closeFunc != nil {
		c.closeFunc()
	}
	return c.err
}

func runtimeAuthKeyFile(t *testing.T) string {
	t.Helper()
	directory := filepath.Join(t.TempDir(), "authority")
	if err := delegationconfig.PreparePrivateDirectory(directory); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "auth.key")
	if err := os.WriteFile(path, []byte(runtimeTestAuthKey), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func runningPeerStatus(
	dnsName string,
	online bool,
	addresses ...string,
) *ipnstate.Status {
	tailscaleIPs := make([]netip.Addr, 0, len(addresses))
	for _, address := range addresses {
		tailscaleIPs = append(tailscaleIPs, netip.MustParseAddr(address))
	}
	return &ipnstate.Status{
		BackendState: "Running",
		CurrentTailnet: &ipnstate.TailnetStatus{
			MagicDNSSuffix:  "tailnet.example",
			MagicDNSEnabled: true,
		},
		Peer: map[key.NodePublic]*ipnstate.PeerStatus{
			key.NewNode().Public(): {
				DNSName:      dnsName,
				TailscaleIPs: tailscaleIPs,
				Online:       online,
			},
		},
	}
}

func successfulPeerDial(t *testing.T) func(
	context.Context,
	netip.AddrPort,
) (net.Conn, error) {
	t.Helper()
	return func(context.Context, netip.AddrPort) (net.Conn, error) {
		client, server := net.Pipe()
		t.Cleanup(func() {
			client.Close()
			server.Close()
		})
		return client, nil
	}
}
