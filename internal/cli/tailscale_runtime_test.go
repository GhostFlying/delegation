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
	"github.com/GhostFlying/delegation/internal/localbridge"
	"github.com/GhostFlying/delegation/internal/statuspage"
	"github.com/GhostFlying/delegation/internal/tailscaleruntime"
	"github.com/GhostFlying/delegation/internal/tokenfile"
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
			return statuspage.Snapshot{
				TransportStatus: cfg.Transport.Status(),
				ControllerID:    cfg.ControllerID,
			}, nil
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

func TestStatusRendersSafeTailscaleHumanOutput(t *testing.T) {
	tests := []struct {
		name string
		role delegationconfig.Role
	}{
		{name: "broker", role: delegationconfig.RoleBroker},
		{name: "peer", role: delegationconfig.RolePeer},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			configPath, cfg := writeStatusTestConfig(t, test.role)
			stateDir := writeTailscaleConfigFixture(
				t,
				configPath,
				&cfg,
				test.name+"-node",
			)
			authKeyPath := cfg.Transport.Tailscale.AuthKeyFile
			authKey, err := os.ReadFile(authKeyPath)
			if err != nil {
				t.Fatal(err)
			}
			var stdout bytes.Buffer
			var stderr bytes.Buffer
			var code int
			if test.role == delegationconfig.RoleBroker {
				readBroker := func(context.Context, string) (statuspage.Snapshot, error) {
					return statuspage.Snapshot{
						TransportStatus: cfg.Transport.Status(),
						Version:         "0.2.0-test",
						ControllerID:    cfg.ControllerID,
					}, nil
				}
				code = runStatusWithReaders(
					[]string{"--config", configPath},
					&stdout,
					&stderr,
					nil,
					readBroker,
				)
			} else {
				readPeer := func(context.Context, string) (localbridge.StatusSnapshot, error) {
					status := statusTestSnapshot(cfg)
					status.TransportStatus = cfg.Transport.Status()
					return status, nil
				}
				code = runStatusWithReader(
					[]string{"--config", configPath},
					&stdout,
					&stderr,
					readPeer,
				)
			}
			if code != 0 || stderr.Len() != 0 {
				t.Fatalf("status code = %d, stderr = %q", code, stderr.String())
			}
			want := "transport: tailscale\n" +
				"tailscale hostname: " + test.name + "-node\n"
			if !strings.Contains(stdout.String(), want) {
				t.Fatalf("tailscale human status = %q, want %q", stdout.String(), want)
			}
			for _, secret := range []string{string(authKey), authKeyPath, stateDir} {
				if strings.Contains(stdout.String(), secret) ||
					strings.Contains(stderr.String(), secret) {
					t.Fatalf(
						"tailscale status disclosed protected value %q: stdout = %q, stderr = %q",
						secret,
						stdout.String(),
						stderr.String(),
					)
				}
			}
		})
	}
}

func TestDoctorPreflightsTailscaleAuthorityOffline(t *testing.T) {
	t.Run("broker", func(t *testing.T) {
		configPath, cfg := setupBrokerRuntimeTest(t, "none")
		stateDir := writeTailscaleConfigFixture(t, configPath, &cfg, "broker-node")
		var stdout bytes.Buffer
		var stderr bytes.Buffer
		if code := runDoctor(
			[]string{"--config", configPath, "--json"},
			&stdout,
			&stderr,
		); code != 0 {
			t.Fatalf("doctor code = %d, stderr = %q", code, stderr.String())
		}
		assertNoTailscaleRuntimeSideEffects(t, stateDir)
	})

	t.Run("peer", func(t *testing.T) {
		configPath, cfg := setupConnectorRuntimeTest(
			t,
			"123e4567-e89b-42d3-a456-426614174202",
			"tailscale-doctor",
			"wss://broker.example.test",
		)
		stateDir := writeTailscaleConfigFixture(t, configPath, &cfg, "peer-node")
		var stdout bytes.Buffer
		var stderr bytes.Buffer
		if code := runDoctor(
			[]string{"--config", configPath, "--json"},
			&stdout,
			&stderr,
		); code != 0 {
			t.Fatalf("doctor code = %d, stderr = %q", code, stderr.String())
		}
		assertNoTailscaleRuntimeSideEffects(t, stateDir)
	})
}

func TestDoctorRejectsInvalidTailscaleAuthorityOffline(t *testing.T) {
	t.Run("malformed enrollment key", func(t *testing.T) {
		configPath, cfg := setupBrokerRuntimeTest(t, "none")
		stateDir := writeTailscaleConfigFixture(t, configPath, &cfg, "broker-node")
		if err := os.WriteFile(
			cfg.Transport.Tailscale.AuthKeyFile,
			[]byte("invalid-enrollment-secret"),
			0o600,
		); err != nil {
			t.Fatal(err)
		}
		var stderr bytes.Buffer
		code := runDoctor(
			[]string{"--config", configPath, "--json"},
			io.Discard,
			&stderr,
		)
		if code == 0 || !strings.Contains(stderr.String(), "tskey-auth- prefix") {
			t.Fatalf("doctor code = %d, stderr = %q", code, stderr.String())
		}
		if strings.Contains(stderr.String(), "invalid-enrollment-secret") {
			t.Fatalf("doctor disclosed enrollment key: %q", stderr.String())
		}
		assertNoTailscaleRuntimeSideEffects(t, stateDir)
	})

	t.Run("missing enrollment key", func(t *testing.T) {
		configPath, cfg := setupConnectorRuntimeTest(
			t,
			"123e4567-e89b-42d3-a456-426614174205",
			"tailscale-missing-key",
			"wss://broker.example.test",
		)
		stateDir := writeTailscaleConfigFixture(t, configPath, &cfg, "peer-node")
		if err := os.Remove(cfg.Transport.Tailscale.AuthKeyFile); err != nil {
			t.Fatal(err)
		}
		var stderr bytes.Buffer
		code := runDoctor(
			[]string{"--config", configPath, "--json"},
			io.Discard,
			&stderr,
		)
		if code == 0 ||
			!strings.Contains(stderr.String(), "inspect protected Tailscale enrollment key file") {
			t.Fatalf("doctor code = %d, stderr = %q", code, stderr.String())
		}
		assertNoTailscaleRuntimeSideEffects(t, stateDir)
	})

	t.Run("state path is a file", func(t *testing.T) {
		configPath, cfg := setupConnectorRuntimeTest(
			t,
			"123e4567-e89b-42d3-a456-426614174203",
			"tailscale-state-file",
			"wss://broker.example.test",
		)
		stateDir := writeTailscaleConfigFixture(t, configPath, &cfg, "peer-node")
		stateMarker := []byte("operator-state-marker")
		if err := os.WriteFile(stateDir, stateMarker, 0o600); err != nil {
			t.Fatal(err)
		}
		var stderr bytes.Buffer
		code := runDoctor(
			[]string{"--config", configPath, "--json"},
			io.Discard,
			&stderr,
		)
		if code == 0 || !strings.Contains(stderr.String(), "must be a directory") {
			t.Fatalf("doctor code = %d, stderr = %q", code, stderr.String())
		}
		got, err := os.ReadFile(stateDir)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, stateMarker) {
			t.Fatalf("state marker = %q, want %q", got, stateMarker)
		}
		if _, err := os.Lstat(stateDir + ".tailscale.lock"); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("doctor changed tailscale lease path: %v", err)
		}
	})

	t.Run("state overlaps broker authority", func(t *testing.T) {
		configPath, cfg := setupBrokerRuntimeTest(t, "none")
		writeTailscaleConfigFixture(t, configPath, &cfg, "broker-node")
		cfg.Transport.Tailscale.StateDir = filepath.Dir(configPath)
		data, err := json.Marshal(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(configPath, data, 0o600); err != nil {
			t.Fatal(err)
		}
		var stderr bytes.Buffer
		code := runDoctor(
			[]string{"--config", configPath, "--json"},
			io.Discard,
			&stderr,
		)
		if code == 0 || !strings.Contains(stderr.String(), "Tailscale state directory") {
			t.Fatalf("doctor code = %d, stderr = %q", code, stderr.String())
		}
	})

	t.Run("derived lease overlaps broker configuration", func(t *testing.T) {
		configPath, cfg := setupBrokerRuntimeTest(t, "none")
		stateDir := writeTailscaleConfigFixture(t, configPath, &cfg, "broker-node")
		collidingConfigPath := stateDir + ".tailscale.lock"
		data, err := json.Marshal(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(collidingConfigPath, data, 0o600); err != nil {
			t.Fatal(err)
		}
		var stderr bytes.Buffer
		code := runDoctor(
			[]string{"--config", collidingConfigPath, "--json"},
			io.Discard,
			&stderr,
		)
		if code == 0 || !strings.Contains(
			stderr.String(),
			"Tailscale state directory lease path conflicts with broker configuration",
		) {
			t.Fatalf("doctor code = %d, stderr = %q", code, stderr.String())
		}
		got, err := os.ReadFile(collidingConfigPath)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, data) {
			t.Fatal("doctor changed colliding config/lease authority")
		}
		if _, err := os.Lstat(stateDir); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("doctor created Tailscale state directory: %v", err)
		}
	})
}

func TestRuntimeCommandsRejectTailscaleNoneAuthenticationOffline(t *testing.T) {
	tests := []struct {
		name string
		run  func(string, io.Writer) int
	}{
		{
			name: "doctor",
			run: func(configPath string, stderr io.Writer) int {
				return runDoctor([]string{"--config", configPath, "--json"}, io.Discard, stderr)
			},
		},
		{
			name: "service run",
			run: func(configPath string, stderr io.Writer) int {
				return runServiceRuntime([]string{"--config", configPath}, stderr)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			configPath, cfg := setupBrokerRuntimeTest(t, "none")
			cfg.Transport = testTailscaleRuntimeConfig(t, "broker-node")
			stateDir := cfg.Transport.Tailscale.StateDir
			cfg.Broker.Listen = ":8787"
			cfg.Broker.StatusListen = "127.0.0.1:8788"
			data, err := json.Marshal(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(configPath, data, 0o600); err != nil {
				t.Fatal(err)
			}

			var stderr bytes.Buffer
			code := test.run(configPath, &stderr)

			if code == 0 || !strings.Contains(stderr.String(), "auth mode none") {
				t.Fatalf("%s code = %d, stderr = %q", test.name, code, stderr.String())
			}
			assertNoTailscaleRuntimeSideEffects(t, stateDir)
		})
	}
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
	useDelegationTokenAuthentication(t, cfg)
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

func useDelegationTokenAuthentication(t *testing.T, cfg *delegationconfig.Config) {
	t.Helper()
	tokenPath := filepath.Join(privateTestDirectory(t), "delegation.token")
	if _, err := tokenfile.Ensure(tokenPath); err != nil {
		t.Fatal(err)
	}
	cfg.Broker.Auth = delegationconfig.AuthConfig{
		Mode:      delegationconfig.AuthModeToken,
		TokenFile: tokenPath,
	}
}

func assertNoTailscaleRuntimeSideEffects(t *testing.T, stateDir string) {
	t.Helper()
	for _, path := range []string{stateDir, stateDir + ".tailscale.lock"} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("non-runtime command changed tailscale path %q: %v", path, err)
		}
	}
}
