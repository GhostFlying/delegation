package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/statuspage"
	"github.com/GhostFlying/delegation/internal/tailscaleruntime"
)

type fakeEmbeddedTailscaleRuntime struct {
	mu          sync.Mutex
	startConfig tailscaleruntime.Config
	startCalls  int
	listenCalls int
	dialCalls   int
	closeCalls  int
	startErr    error
	closeErr    error
	listen      func(context.Context, string, string) (net.Listener, error)
	dial        func(context.Context, string, string) (net.Conn, error)
	beforeClose func() error
}

func (r *fakeEmbeddedTailscaleRuntime) Start(
	_ context.Context,
	cfg tailscaleruntime.Config,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.startCalls++
	r.startConfig = cfg
	return r.startErr
}

func (r *fakeEmbeddedTailscaleRuntime) Listen(
	ctx context.Context,
	network, address string,
) (net.Listener, error) {
	r.mu.Lock()
	r.listenCalls++
	listen := r.listen
	r.mu.Unlock()
	if listen == nil {
		return nil, errors.New("unexpected fake tailscale Listen")
	}
	return listen(ctx, network, address)
}

func (r *fakeEmbeddedTailscaleRuntime) Dial(
	ctx context.Context,
	network, address string,
) (net.Conn, error) {
	r.mu.Lock()
	r.dialCalls++
	dial := r.dial
	r.mu.Unlock()
	if dial == nil {
		return nil, errors.New("unexpected fake tailscale Dial")
	}
	return dial(ctx, network, address)
}

func (r *fakeEmbeddedTailscaleRuntime) Close() error {
	r.mu.Lock()
	r.closeCalls++
	beforeClose := r.beforeClose
	closeErr := r.closeErr
	r.mu.Unlock()
	if beforeClose != nil {
		return errors.Join(beforeClose(), closeErr)
	}
	return closeErr
}

func testTailscaleRuntimeConfig(t *testing.T, hostname string) delegationconfig.TransportConfig {
	t.Helper()
	root := privateTestDirectory(t)
	if err := delegationconfig.PreparePrivateDirectory(root); err != nil {
		t.Fatal(err)
	}
	stateDir := filepath.Join(root, "tailscale")
	authKeyFile := filepath.Join(root, "tailscale-auth.key")
	if err := os.WriteFile(authKeyFile, []byte("tskey-auth-cli-runtime-test"), 0o600); err != nil {
		t.Fatal(err)
	}
	return delegationconfig.TransportConfig{
		Mode: delegationconfig.TransportModeTailscale,
		Tailscale: &delegationconfig.TailscaleConfig{
			StateDir:    stateDir,
			Hostname:    hostname,
			AuthKeyFile: authKeyFile,
		},
	}
}

func TestRuntimeAwareCommandsReadTailscaleWithoutStartingNode(t *testing.T) {
	t.Run("service install", func(t *testing.T) {
		configPath, cfg := setupBrokerRuntimeTest(t, "none")
		stateDir := writeTailscaleConfigFixture(t, configPath, &cfg, "broker-node")
		var stderr bytes.Buffer
		code := runServiceInstall(
			[]string{
				"--config", configPath,
				"--environment-file", filepath.Join(privateTestDirectory(t), "unexpected.env"),
			},
			io.Discard,
			&stderr,
		)
		if code == 0 || !strings.Contains(stderr.String(), "broker service must not use") {
			t.Fatalf("service install code = %d, stderr = %q", code, stderr.String())
		}
		assertNoTailscaleRuntimeSideEffects(t, stateDir)
	})

	t.Run("doctor", func(t *testing.T) {
		configPath, cfg := setupBrokerRuntimeTest(t, "none")
		stateDir := writeTailscaleConfigFixture(t, configPath, &cfg, "broker-node")
		var stderr bytes.Buffer
		if code := runDoctor(
			[]string{"--config", configPath, "--json"},
			io.Discard,
			&stderr,
		); code != 0 {
			t.Fatalf("doctor code = %d, stderr = %q", code, stderr.String())
		}
		assertNoTailscaleRuntimeSideEffects(t, stateDir)
	})

	t.Run("status", func(t *testing.T) {
		configPath, cfg := setupBrokerRuntimeTest(t, "none")
		stateDir := writeTailscaleConfigFixture(t, configPath, &cfg, "broker-node")
		readBroker := func(context.Context, string) (statuspage.Snapshot, error) {
			return statuspage.Snapshot{ControllerID: cfg.ControllerID}, nil
		}
		var stderr bytes.Buffer
		if code := runStatusWithReaders(
			[]string{"--config", configPath, "--json"},
			io.Discard,
			&stderr,
			nil,
			readBroker,
		); code != 0 {
			t.Fatalf("status code = %d, stderr = %q", code, stderr.String())
		}
		assertNoTailscaleRuntimeSideEffects(t, stateDir)
	})

	t.Run("mcp", func(t *testing.T) {
		configPath := writeRootMCPConfig(t, delegationconfig.RolePeer)
		cfg, err := delegationconfig.Read(configPath)
		if err != nil {
			t.Fatal(err)
		}
		stateDir := writeTailscaleConfigFixture(t, configPath, &cfg, "peer-node")
		if _, err := loadRootMCPServer(configPath); err != nil {
			t.Fatal(err)
		}
		assertNoTailscaleRuntimeSideEffects(t, stateDir)
	})

	t.Run("credential", func(t *testing.T) {
		environment := setupCredentialTestBroker(t, "token")
		cfg, err := delegationconfig.Read(environment.configPath)
		if err != nil {
			t.Fatal(err)
		}
		stateDir := writeTailscaleConfigFixture(
			t,
			environment.configPath,
			&cfg,
			"broker-node",
		)
		if _, _, err := loadBrokerCredentialAuthority(environment.configPath); err != nil {
			t.Fatal(err)
		}
		assertNoTailscaleRuntimeSideEffects(t, stateDir)
	})
}

func writeTailscaleConfigFixture(
	t *testing.T,
	configPath string,
	cfg *delegationconfig.Config,
	hostname string,
) string {
	t.Helper()
	cfg.Transport = testTailscaleRuntimeConfig(t, hostname)
	if cfg.Role == delegationconfig.RoleBroker {
		cfg.Broker.Listen = ":8787"
		cfg.Broker.StatusListen = "127.0.0.1:8788"
	} else {
		cfg.Broker.URL = "ws://broker-node:8787/v1/connect"
	}
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := cfg.ValidateForRuntime(
		delegationconfig.RuntimeCapabilities{EmbeddedTailscale: true},
	); err != nil {
		t.Fatal(err)
	}
	return cfg.Transport.Tailscale.StateDir
}

func assertNoTailscaleRuntimeSideEffects(t *testing.T, stateDir string) {
	t.Helper()
	for _, path := range []string{stateDir, stateDir + ".tailscale.lock"} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("non-runtime command changed tailscale path %q: %v", path, err)
		}
	}
}
