package config

import (
	"encoding/json"
	"fmt"
	"maps"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/GhostFlying/delegation/internal/clilaunch"
	"github.com/GhostFlying/delegation/internal/hostkind"
)

const testID = "123e4567-e89b-42d3-a456-426614174000"

func testStateFile(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "broker.sqlite3")
}

func TestConfigRoundTrip(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "device.token")
	cfg := Config{
		SchemaVersion: CurrentSchemaVersion,
		InstanceID:    DefaultInstanceID,
		HostKind:      hostkind.Codex,
		Role:          RolePeer,
		ControllerID:  testID,
		DeviceID:      "123e4567-e89b-42d3-a456-426614174001",
		DeviceName:    "windows-builder",
		Broker: BrokerConfig{
			URL: "wss://broker.example.test",
			Auth: AuthConfig{
				Mode:      AuthModeToken,
				TokenFile: tokenFile,
			},
		},
		Peer: testPeerRuntime(t),
	}
	path := filepath.Join(t.TempDir(), "private", "config.json")

	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	writeProtectedConfigFixture(t, path, data)
	got, err := Read(path)
	if err != nil {
		t.Fatal(err)
	}
	if got != cfg {
		t.Fatalf("Read() = %#v, want %#v", got, cfg)
	}
	if got.Peer.EffectiveCLI().Command != cfg.Peer.CodexBinary {
		t.Fatalf("legacy effective CLI = %#v", got.Peer.EffectiveCLI())
	}
}

func TestTransportConfigRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		cfg  func(*testing.T) Config
	}{
		{
			name: "tcp broker",
			cfg:  protectedTestConfig,
		},
		{
			name: "tcp peer",
			cfg:  testPeerConfig,
		},
		{
			name: "tailscale broker",
			cfg: func(t *testing.T) Config {
				cfg := protectedTestConfig(t)
				cfg.Transport = testTailscaleTransport(t)
				cfg.Broker.Listen = ":8787"
				cfg.Broker.StatusListen = "127.0.0.1:8788"
				return cfg
			},
		},
		{
			name: "tailscale peer",
			cfg: func(t *testing.T) Config {
				cfg := testPeerConfig(t)
				cfg.Transport = testTailscaleTransport(t)
				cfg.Broker.URL = "ws://broker.tailnet.test:8787/v1/connect"
				return cfg
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := test.cfg(t)
			data, err := json.Marshal(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(data), `"transport":{"mode":"`) {
				t.Fatalf("encoded config does not explicitly select transport: %s", data)
			}
			path := filepath.Join(t.TempDir(), "private", "config.json")
			writeProtectedConfigFixture(t, path, data)
			var got Config
			if cfg.Transport.Mode == TransportModeTailscale {
				got, err = ReadForRuntime(path, tailscaleRuntimeCapabilities())
			} else {
				got, err = Read(path)
			}
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, cfg) {
				t.Fatalf("Read() = %#v, want %#v", got, cfg)
			}
		})
	}
}

func TestTransportModeValidation(t *testing.T) {
	tests := []struct {
		name   string
		cfg    func(*testing.T) Config
		mutate func(*Config)
		want   string
	}{
		{
			name: "tcp rejects tailscale object",
			cfg:  protectedTestConfig,
			mutate: func(cfg *Config) {
				tailscale := testTailscaleTransport(t).Tailscale
				cfg.Transport.Tailscale = tailscale
			},
			want: "must be absent",
		},
		{
			name: "tailscale requires object",
			cfg:  protectedTestConfig,
			mutate: func(cfg *Config) {
				cfg.Transport.Mode = TransportModeTailscale
			},
			want: "is required",
		},
		{
			name: "unknown mode",
			cfg:  protectedTestConfig,
			mutate: func(cfg *Config) {
				cfg.Transport.Mode = TransportMode(2)
			},
			want: "unsupported transport mode",
		},
		{
			name: "tailscale broker rejects tcp listen",
			cfg:  protectedTestConfig,
			mutate: func(cfg *Config) {
				cfg.Transport = testTailscaleTransport(t)
				cfg.Broker.Listen = "127.0.0.1:8787"
			},
			want: "empty host",
		},
		{
			name: "tailscale peer rejects wss",
			cfg:  testPeerConfig,
			mutate: func(cfg *Config) {
				cfg.Transport = testTailscaleTransport(t)
				cfg.Broker.URL = "wss://broker.tailnet.test/v1/connect"
			},
			want: "must use ws://",
		},
		{
			name: "tailscale broker rejects insecure acknowledgement",
			cfg:  protectedTestConfig,
			mutate: func(cfg *Config) {
				cfg.Transport = testTailscaleTransport(t)
				cfg.Broker.Listen = ":8787"
				cfg.Broker.AllowInsecureNonLoopback = true
			},
			want: "must be false",
		},
		{
			name: "tailscale peer rejects insecure acknowledgement",
			cfg:  testPeerConfig,
			mutate: func(cfg *Config) {
				cfg.Transport = testTailscaleTransport(t)
				cfg.Broker.URL = "ws://broker.tailnet.test/v1/connect"
				cfg.Broker.AllowInsecureNonLoopback = true
			},
			want: "must be false",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := test.cfg(t)
			test.mutate(&cfg)
			err := cfg.ValidateForRuntime(tailscaleRuntimeCapabilities())
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Validate() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestTransportModeCanonicalRoundTrip(t *testing.T) {
	for _, mode := range []TransportMode{TransportModeTCP, TransportModeTailscale} {
		data, err := json.Marshal(mode)
		if err != nil {
			t.Fatal(err)
		}
		var got TransportMode
		if err := json.Unmarshal(data, &got); err != nil {
			t.Fatal(err)
		}
		if got != mode {
			t.Fatalf("transport mode round-trip = %v, want %v", got, mode)
		}
	}
}

func TestTailscaleRuntimeActivationGate(t *testing.T) {
	tests := []struct {
		name string
		cfg  func(*testing.T) Config
	}{
		{
			name: "broker port listener",
			cfg: func(t *testing.T) Config {
				cfg := protectedTestConfig(t)
				cfg.Transport = testTailscaleTransport(t)
				cfg.Broker.Listen = ":8787"
				cfg.Broker.StatusListen = "127.0.0.1:8788"
				return cfg
			},
		},
		{
			name: "peer broker endpoint",
			cfg:  testTailscalePeerConfig,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := test.cfg(t)
			err := cfg.Validate()
			if err == nil || !strings.Contains(err.Error(), "not supported by this runtime") {
				t.Fatalf("Validate() error = %v, want unsupported runtime", err)
			}
			if err := cfg.ValidateForRuntime(tailscaleRuntimeCapabilities()); err != nil {
				t.Fatalf("ValidateForRuntime() with capability error = %v", err)
			}

			data, err := json.Marshal(cfg)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "private", "config.json")
			writeProtectedConfigFixture(t, path, data)
			if _, err := Read(path); err == nil ||
				!strings.Contains(err.Error(), "not supported by this runtime") {
				t.Fatalf("Read() error = %v, want unsupported runtime", err)
			}
			if _, err := ReadForRuntime(path, tailscaleRuntimeCapabilities()); err != nil {
				t.Fatalf("ReadForRuntime() with capability error = %v", err)
			}
		})
	}
}

func TestReadRequiresExplicitTransportMode(t *testing.T) {
	valid := protectedTestConfig(t)
	data, err := json.Marshal(valid)
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(data, &document); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name      string
		transport any
		remove    bool
		want      string
	}{
		{name: "missing transport", remove: true, want: "transport configuration is required"},
		{name: "null transport", transport: nil, want: "transport configuration is required"},
		{name: "missing mode", transport: map[string]any{}, want: "transport mode is required"},
		{name: "empty mode", transport: map[string]any{"mode": ""}, want: "unsupported transport mode"},
		{name: "unknown mode", transport: map[string]any{"mode": "wireguard"}, want: "unsupported transport mode"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := maps.Clone(document)
			if test.remove {
				delete(candidate, "transport")
			} else {
				candidate["transport"] = test.transport
			}
			contents, err := json.Marshal(candidate)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "private", "config.json")
			writeProtectedConfigFixture(t, path, contents)
			_, err = Read(path)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Read() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestTailscaleConfigValidation(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*TailscaleConfig)
		want   string
	}{
		{
			name: "empty state directory",
			mutate: func(cfg *TailscaleConfig) {
				cfg.StateDir = ""
			},
			want: "stateDir",
		},
		{
			name: "relative state directory",
			mutate: func(cfg *TailscaleConfig) {
				cfg.StateDir = "tailscale-state"
			},
			want: "stateDir",
		},
		{
			name: "empty auth key file",
			mutate: func(cfg *TailscaleConfig) {
				cfg.AuthKeyFile = ""
			},
			want: "authKeyFile",
		},
		{
			name: "relative auth key file",
			mutate: func(cfg *TailscaleConfig) {
				cfg.AuthKeyFile = "auth.key"
			},
			want: "authKeyFile",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := testTailscalePeerConfig(t)
			test.mutate(cfg.Transport.Tailscale)
			err := cfg.ValidateForRuntime(tailscaleRuntimeCapabilities())
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Validate() error = %v, want %q", err, test.want)
			}
		})
	}

	for _, hostname := range []string{
		"",
		"-broker",
		"broker-",
		"Broker",
		"broker.example",
		"broker_name",
		"broker name",
		strings.Repeat("a", 64),
		"bröker",
	} {
		t.Run("invalid hostname "+hostname, func(t *testing.T) {
			cfg := testTailscalePeerConfig(t)
			cfg.Transport.Tailscale.Hostname = hostname
			err := cfg.ValidateForRuntime(tailscaleRuntimeCapabilities())
			if err == nil || !strings.Contains(err.Error(), "hostname") {
				t.Fatalf("Validate() error = %v, want hostname error", err)
			}
		})
	}
	for _, hostname := range []string{"a", "0", "broker", "broker-1", strings.Repeat("a", 63)} {
		t.Run("valid hostname "+hostname[:min(len(hostname), 16)], func(t *testing.T) {
			cfg := testTailscalePeerConfig(t)
			cfg.Transport.Tailscale.Hostname = hostname
			if err := cfg.ValidateForRuntime(tailscaleRuntimeCapabilities()); err != nil {
				t.Fatalf("Validate() error = %v", err)
			}
		})
	}
}

func TestTailscaleBrokerListenValidation(t *testing.T) {
	for _, listen := range []string{
		"",
		"8787",
		"127.0.0.1:8787",
		"0.0.0.0:8787",
		"localhost:8787",
		"::8787",
		":not-a-port",
		":0",
		":65536",
	} {
		t.Run(listen, func(t *testing.T) {
			cfg := protectedTestConfig(t)
			cfg.Transport = testTailscaleTransport(t)
			cfg.Broker.Listen = listen
			err := cfg.ValidateForRuntime(tailscaleRuntimeCapabilities())
			if err == nil || !strings.Contains(err.Error(), "tailscale broker listen") {
				t.Fatalf("Validate() error = %v, want tailscale listen error", err)
			}
		})
	}

	cfg := protectedTestConfig(t)
	cfg.Transport = testTailscaleTransport(t)
	cfg.Broker.Listen = ":8787"
	cfg.Broker.StatusListen = "127.0.0.1:8788"
	if err := cfg.ValidateForRuntime(tailscaleRuntimeCapabilities()); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	cfg.Broker.StatusListen = "0.0.0.0:8788"
	if err := cfg.ValidateForRuntime(tailscaleRuntimeCapabilities()); err == nil ||
		!strings.Contains(err.Error(), "loopback") {
		t.Fatalf("Validate() status listener error = %v", err)
	}
	cfg.Broker.StatusListen = "127.0.0.1:8787"
	if err := cfg.ValidateForRuntime(tailscaleRuntimeCapabilities()); err == nil ||
		!strings.Contains(err.Error(), "overlap") {
		t.Fatalf("Validate() overlapping listener error = %v", err)
	}
}

func TestTailscaleBrokerURLValidation(t *testing.T) {
	for _, brokerURL := range []string{
		"ws://broker.tailnet.test/v1/connect",
		"ws://broker.tailnet.test:8787/v1/connect",
	} {
		t.Run("valid "+brokerURL, func(t *testing.T) {
			got, err := NormalizeBrokerURLForTransport(brokerURL, TransportModeTailscale, false)
			if err != nil || got != brokerURL {
				t.Fatalf("NormalizeBrokerURLForTransport() = %q, %v", got, err)
			}
			cfg := testTailscalePeerConfig(t)
			cfg.Broker.URL = brokerURL
			if err := cfg.ValidateForRuntime(tailscaleRuntimeCapabilities()); err != nil {
				t.Fatalf("Validate() error = %v", err)
			}
		})
	}
	for _, brokerURL := range []string{
		"",
		"ws:///v1/connect",
		"wss://broker.tailnet.test/v1/connect",
		"http://broker.tailnet.test/v1/connect",
		"ws://token@broker.tailnet.test/v1/connect",
		"ws://broker.tailnet.test",
		"ws://broker.tailnet.test/",
		"ws://broker.tailnet.test/v2/connect",
		"ws://broker.tailnet.test/%76%31/connect",
		"ws://broker.tailnet.test/v1/connect?",
		"ws://broker.tailnet.test/v1/connect?query",
		"ws://broker.tailnet.test/v1/connect#fragment",
		"ws://broker.tailnet.test:0/v1/connect",
		"ws://broker.tailnet.test:65536/v1/connect",
		"ws://broker.tailnet.test:/v1/connect",
	} {
		t.Run(brokerURL, func(t *testing.T) {
			if _, err := NormalizeBrokerURLForTransport(
				brokerURL,
				TransportModeTailscale,
				false,
			); err == nil {
				t.Fatal("NormalizeBrokerURLForTransport() accepted invalid tailscale endpoint")
			}
		})
	}
}

func TestTransportAwareBrokerURLNormalization(t *testing.T) {
	const brokerURL = "ws://broker.example.test:8787"
	want, err := NormalizeBrokerURL(brokerURL, true)
	if err != nil {
		t.Fatal(err)
	}
	got, err := NormalizeBrokerURLForTransport(brokerURL, TransportModeTCP, true)
	if err != nil || got != want {
		t.Fatalf("tcp normalization = %q, %v, want %q", got, err, want)
	}
	if _, err := NormalizeBrokerURLForTransport(
		"ws://broker.tailnet.test/v1/connect",
		TransportModeTailscale,
		false,
	); err != nil {
		t.Fatalf("tailscale normalization required plaintext acknowledgement: %v", err)
	}
}

func TestUsesInsecureNonLoopbackTransportIgnoresTailscale(t *testing.T) {
	tcpBroker := protectedTestConfig(t)
	tcpBroker.Broker.Listen = "0.0.0.0:8787"
	tcpBroker.Broker.AllowInsecureNonLoopback = true
	if !tcpBroker.UsesInsecureNonLoopbackTransport() {
		t.Fatal("TCP broker did not report insecure non-loopback transport")
	}
	tcpPeer := testPeerConfig(t)
	tcpPeer.Broker.URL = "ws://broker.example.test:8787"
	tcpPeer.Broker.AllowInsecureNonLoopback = true
	if !tcpPeer.UsesInsecureNonLoopbackTransport() {
		t.Fatal("TCP peer did not report insecure non-loopback transport")
	}

	tailscaleBroker := protectedTestConfig(t)
	tailscaleBroker.Transport = testTailscaleTransport(t)
	tailscaleBroker.Broker.Listen = ":8787"
	if tailscaleBroker.UsesInsecureNonLoopbackTransport() {
		t.Fatal("tailscale broker reported insecure non-loopback transport")
	}
	tailscalePeer := testTailscalePeerConfig(t)
	if tailscalePeer.UsesInsecureNonLoopbackTransport() {
		t.Fatal("tailscale peer reported insecure non-loopback transport")
	}
}

func TestStructuredCLIConfigRoundTrip(t *testing.T) {
	cfg := testPeerConfig(t)
	command := cfg.Peer.CodexBinary
	cfg.Peer.CodexBinary = ""
	cfg.Peer.CLI = &CLIConfig{
		Command:   command,
		Arguments: []string{"-p", "profile with spaces"},
		Launcher: &clilaunch.Spec{
			Executable:      filepath.Join(filepath.Dir(command), "launcher"),
			PrefixArguments: []string{"run", "--"},
		},
	}
	path := filepath.Join(t.TempDir(), "private", "config.json")

	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	writeProtectedConfigFixture(t, path, data)
	got, err := Read(path)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, cfg) {
		t.Fatalf("Read() = %#v, want %#v", got, cfg)
	}
}

func TestPeerCLIConfigValidation(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*PeerConfig)
		want   string
	}{
		{
			name: "missing legacy and structured configuration",
			mutate: func(peer *PeerConfig) {
				peer.CodexBinary = ""
			},
			want: "exactly one",
		},
		{
			name: "legacy and structured configuration",
			mutate: func(peer *PeerConfig) {
				peer.CLI = &CLIConfig{Command: peer.CodexBinary}
			},
			want: "exactly one",
		},
		{
			name: "relative command",
			mutate: func(peer *PeerConfig) {
				peer.CodexBinary = ""
				peer.CLI = &CLIConfig{Command: "codex"}
			},
			want: "command must be an absolute path",
		},
		{
			name: "command with NUL",
			mutate: func(peer *PeerConfig) {
				peer.CodexBinary = ""
				peer.CLI = &CLIConfig{Command: filepath.Join(t.TempDir(), "codex") + "\x00"}
			},
			want: "must not contain NUL",
		},
		{
			name: "relative launcher",
			mutate: func(peer *PeerConfig) {
				command := peer.CodexBinary
				peer.CodexBinary = ""
				peer.CLI = &CLIConfig{
					Command:  command,
					Launcher: &clilaunch.Spec{Executable: "warmpool"},
				}
			},
			want: "launcher executable must be an absolute path",
		},
		{
			name: "NUL argument",
			mutate: func(peer *PeerConfig) {
				command := peer.CodexBinary
				peer.CodexBinary = ""
				peer.CLI = &CLIConfig{
					Command:   command,
					Arguments: []string{"profile\x00name"},
				}
			},
			want: "must not contain NUL",
		},
		{
			name: "combined argument count",
			mutate: func(peer *PeerConfig) {
				command := peer.CodexBinary
				peer.CodexBinary = ""
				peer.CLI = &CLIConfig{
					Command:   command,
					Arguments: make([]string, clilaunch.MaximumPrefixArguments-1),
					Launcher: &clilaunch.Spec{
						Executable:      filepath.Join(filepath.Dir(command), "launcher"),
						PrefixArguments: []string{"run"},
					},
				}
			},
			want: "at most",
		},
		{
			name: "combined argument bytes",
			mutate: func(peer *PeerConfig) {
				command := peer.CodexBinary
				peer.CodexBinary = ""
				peer.CLI = &CLIConfig{
					Command:   command,
					Arguments: []string{strings.Repeat("x", clilaunch.MaximumPrefixBytes)},
					Launcher: &clilaunch.Spec{
						Executable: filepath.Join(filepath.Dir(command), "launcher"),
					},
				}
			},
			want: "bytes",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := testPeerConfig(t)
			test.mutate(&cfg.Peer)
			if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Validate() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestLegacyConfigUsesDefaultCodexInstance(t *testing.T) {
	cfg := protectedTestConfig(t)
	cfg.InstanceID = ""
	cfg.HostKind = ""
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "private", "legacy.json")
	writeProtectedConfigFixture(t, path, data)

	got, err := Read(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.InstanceID != "" || got.HostKind != "" ||
		got.EffectiveInstanceID() != DefaultInstanceID ||
		got.EffectiveHostKind() != hostkind.Codex {
		t.Fatalf("legacy config = %#v", got)
	}
}

func TestInstanceAndHostKindValidation(t *testing.T) {
	for _, invalid := range []string{"Codex", "1codex", "codex_", "codex-", strings.Repeat("a", 33)} {
		cfg := protectedTestConfig(t)
		cfg.InstanceID = invalid
		if err := cfg.Validate(); err == nil {
			t.Fatalf("Validate accepted instanceId %q", invalid)
		}
	}
	cfg := protectedTestConfig(t)
	cfg.HostKind = "claude"
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate accepted unsupported host kind")
	}
}

func TestTraeXPeerConfigRequiresStructuredLauncher(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*PeerConfig)
		want   string
	}{
		{
			name:   "legacy command",
			mutate: func(*PeerConfig) {},
			want:   "requires structured cli configuration",
		},
		{
			name: "direct structured command",
			mutate: func(peer *PeerConfig) {
				command := peer.CodexBinary
				peer.CodexBinary = ""
				peer.CLI = &CLIConfig{Command: command}
			},
			want: "requires a CLI launcher",
		},
		{
			name: "structured launcher",
			mutate: func(peer *PeerConfig) {
				command := peer.CodexBinary
				peer.CodexBinary = ""
				peer.CLI = &CLIConfig{
					Command:   command,
					Arguments: []string{"-p", "ultra"},
					Launcher: &clilaunch.Spec{
						Executable:      filepath.Join(filepath.Dir(command), "warmpool"),
						PrefixArguments: []string{"run", "--"},
					},
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := testPeerConfig(t)
			cfg.HostKind = hostkind.TraeX
			test.mutate(&cfg.Peer)
			err := cfg.Validate()
			if test.want == "" {
				if err != nil {
					t.Fatalf("Validate() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Validate() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestRoleSpecificDefaultPathsAndExplicitOverride(t *testing.T) {
	home := t.TempDir()
	t.Setenv("DELEGATION_HOME", home)
	t.Setenv("DELEGATION_CONFIG", "")
	brokerPath, err := DefaultBrokerPath()
	if err != nil {
		t.Fatal(err)
	}
	peerPath, err := DefaultPeerPath()
	if err != nil {
		t.Fatal(err)
	}
	if brokerPath != filepath.Join(home, "broker.json") || peerPath != filepath.Join(home, "peer.json") ||
		brokerPath == peerPath {
		t.Fatalf("role-specific paths = %q / %q", brokerPath, peerPath)
	}
	override := filepath.Join(home, "explicit.json")
	t.Setenv("DELEGATION_CONFIG", override)
	brokerPath, err = DefaultBrokerPath()
	if err != nil {
		t.Fatal(err)
	}
	peerPath, err = DefaultPeerPath()
	if err != nil {
		t.Fatal(err)
	}
	if brokerPath != override || peerPath != override {
		t.Fatalf("explicit config override = %q / %q", brokerPath, peerPath)
	}
}

func TestNamedInstanceDefaultPathsUseIsolatedNamespace(t *testing.T) {
	home := t.TempDir()
	t.Setenv("DELEGATION_HOME", home)
	t.Setenv("DELEGATION_CONFIG", "")

	brokerPath, err := DefaultBrokerPathForInstance("traex-main")
	if err != nil {
		t.Fatal(err)
	}
	peerPath, err := DefaultPeerPathForInstance("traex-main")
	if err != nil {
		t.Fatal(err)
	}
	instanceHome := filepath.Join(home, "instances", "traex-main")
	if brokerPath != filepath.Join(instanceHome, "broker.json") ||
		peerPath != filepath.Join(instanceHome, "peer.json") {
		t.Fatalf("named instance paths = %q / %q", brokerPath, peerPath)
	}
	defaultBroker, err := DefaultBrokerPathForInstance(DefaultInstanceID)
	if err != nil {
		t.Fatal(err)
	}
	if defaultBroker != filepath.Join(home, "broker.json") {
		t.Fatalf("default broker path = %q", defaultBroker)
	}
	if _, err := DefaultPeerPathForInstance("Invalid"); err == nil {
		t.Fatal("named default path accepted invalid instance")
	}
}

func TestBrokerNonLoopbackRequiresAcknowledgement(t *testing.T) {
	for _, auth := range []AuthConfig{
		{Mode: AuthModeNone},
		{Mode: AuthModeToken, TokenFile: filepath.Join(t.TempDir(), "broker.token")},
	} {
		t.Run(string(auth.Mode), func(t *testing.T) {
			cfg := Config{
				SchemaVersion: CurrentSchemaVersion,
				Role:          RoleBroker,
				ControllerID:  testID,
				Broker: BrokerConfig{
					Listen:    "0.0.0.0:8787",
					StateFile: testStateFile(t),
					Auth:      auth,
				},
			}

			if err := cfg.Validate(); err == nil {
				t.Fatal("Validate() succeeded without non-loopback acknowledgement")
			}
			cfg.Broker.AllowInsecureNonLoopback = true
			if err := cfg.Validate(); err != nil {
				t.Fatalf("Validate() with acknowledgement: %v", err)
			}
		})
	}
}

func TestBrokerURLRejectsEmbeddedCredentials(t *testing.T) {
	cfg := Config{
		SchemaVersion: CurrentSchemaVersion,
		Role:          RolePeer,
		ControllerID:  testID,
		DeviceID:      "123e4567-e89b-42d3-a456-426614174001",
		DeviceName:    "controller",
		Broker: BrokerConfig{
			URL:  "wss://token@broker.example.test",
			Auth: AuthConfig{Mode: AuthModeNone},
		},
		Peer: testPeerRuntime(t),
	}

	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() accepted credentials in broker URL")
	}
}

func TestBrokerURLRejectsEmptyHostname(t *testing.T) {
	if _, err := NormalizeBrokerURL("wss://:8787", false); err == nil {
		t.Fatal("NormalizeBrokerURL accepted an empty hostname")
	}
}

func TestBrokerURLValidationMatchesConnectorEndpoint(t *testing.T) {
	valid := Config{
		SchemaVersion: CurrentSchemaVersion,
		Role:          RolePeer,
		ControllerID:  testID,
		DeviceID:      "123e4567-e89b-42d3-a456-426614174001",
		DeviceName:    "controller",
		Broker: BrokerConfig{
			URL:  "wss://broker.example.test",
			Auth: AuthConfig{Mode: AuthModeNone},
		},
		Peer: testPeerRuntime(t),
	}
	for _, brokerURL := range []string{
		"wss://broker.example.test/other",
		"wss://broker.example.test/v2/connect",
		"wss://broker.example.test/%76%31/connect",
		"wss://broker.example.test?",
		"wss://broker.example.test/v1/connect#fragment",
	} {
		t.Run(brokerURL, func(t *testing.T) {
			cfg := valid
			cfg.Broker.URL = brokerURL
			if err := cfg.Validate(); err == nil {
				t.Fatal("Validate() accepted a broker URL the connector cannot use")
			}
		})
	}
	for _, brokerURL := range []string{
		"wss://broker.example.test",
		"wss://broker.example.test/",
		"wss://broker.example.test/v1/connect",
	} {
		t.Run("valid "+brokerURL, func(t *testing.T) {
			cfg := valid
			cfg.Broker.URL = brokerURL
			if err := cfg.Validate(); err != nil {
				t.Fatal(err)
			}
			got, err := NormalizeBrokerURL(brokerURL, false)
			if err != nil || got != "wss://broker.example.test/v1/connect" {
				t.Fatalf("NormalizeBrokerURL() = %q, %v", got, err)
			}
		})
	}
}

func TestDeviceNameUsesRuntimeDescriptorRules(t *testing.T) {
	valid := Config{
		SchemaVersion: CurrentSchemaVersion,
		Role:          RolePeer,
		ControllerID:  testID,
		DeviceID:      "123e4567-e89b-42d3-a456-426614174001",
		DeviceName:    "builder",
		Broker: BrokerConfig{
			URL:  "wss://broker.example.test",
			Auth: AuthConfig{Mode: AuthModeNone},
		},
		Peer: testPeerRuntime(t),
	}
	for _, name := range []string{"line\nbreak", strings.Repeat("x", 129), string([]byte{0xff})} {
		t.Run(name[:min(len(name), 16)], func(t *testing.T) {
			cfg := valid
			cfg.DeviceName = name
			if err := cfg.Validate(); err == nil {
				t.Fatal("Validate() accepted a device name rejected by the connector")
			}
		})
	}
}

func TestTokenAuthPlaintextRequiresAcknowledgement(t *testing.T) {
	cfg := Config{
		SchemaVersion: CurrentSchemaVersion,
		Role:          RolePeer,
		ControllerID:  testID,
		DeviceID:      "123e4567-e89b-42d3-a456-426614174001",
		DeviceName:    "device",
		Broker: BrokerConfig{
			URL: "ws://broker.example.test",
			Auth: AuthConfig{
				Mode:      AuthModeToken,
				TokenFile: filepath.Join(t.TempDir(), "device.token"),
			},
		},
		Peer: testPeerRuntime(t),
	}

	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() accepted remote token authentication over ws:// without acknowledgement")
	}
	cfg.Broker.AllowInsecureNonLoopback = true
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() rejected acknowledged token authentication over ws://: %v", err)
	}
	cfg.Broker.AllowInsecureNonLoopback = false
	cfg.Broker.URL = "ws://127.0.0.1:8787"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() rejected loopback token authentication over ws://: %v", err)
	}
	cfg.Broker.URL = "wss://broker.example.test"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() rejected token authentication over wss://: %v", err)
	}
}

func TestPlaintextBrokerURLRequiresAcknowledgement(t *testing.T) {
	cfg := Config{
		SchemaVersion: CurrentSchemaVersion,
		Role:          RolePeer,
		ControllerID:  testID,
		DeviceID:      "123e4567-e89b-42d3-a456-426614174001",
		DeviceName:    "device",
		Broker: BrokerConfig{
			URL:  "ws://broker.example.test:8787",
			Auth: AuthConfig{Mode: AuthModeNone},
		},
		Peer: testPeerRuntime(t),
	}

	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() accepted remote plaintext broker URL")
	}
	cfg.Broker.AllowInsecureNonLoopback = true
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() rejected acknowledged remote plaintext broker URL: %v", err)
	}
	cfg.Broker.AllowInsecureNonLoopback = false
	cfg.Broker.URL = "ws://127.0.0.1:8787"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() rejected loopback plaintext broker URL: %v", err)
	}
}

func TestBrokerURLPortMustBeUsable(t *testing.T) {
	for _, brokerURL := range []string{"wss://broker.example.test:0", "wss://broker.example.test:65536"} {
		t.Run(brokerURL, func(t *testing.T) {
			cfg := Config{
				SchemaVersion: CurrentSchemaVersion,
				Role:          RolePeer,
				ControllerID:  testID,
				DeviceID:      "123e4567-e89b-42d3-a456-426614174001",
				DeviceName:    "device",
				Broker: BrokerConfig{
					URL:  brokerURL,
					Auth: AuthConfig{Mode: AuthModeNone},
				},
				Peer: testPeerRuntime(t),
			}
			if err := cfg.Validate(); err == nil {
				t.Fatal("Validate() accepted unusable broker URL port")
			}
		})
	}
}

func TestReadRejectsUnknownAndTrailingFields(t *testing.T) {
	valid := Config{
		SchemaVersion: CurrentSchemaVersion,
		Role:          RoleBroker,
		ControllerID:  testID,
		Broker: BrokerConfig{
			Listen:    "127.0.0.1:8787",
			StateFile: testStateFile(t),
			Auth:      AuthConfig{Mode: AuthModeNone},
		},
	}
	validData, err := json.Marshal(valid)
	if err != nil {
		t.Fatal(err)
	}
	var unknown map[string]any
	if err := json.Unmarshal(validData, &unknown); err != nil {
		t.Fatal(err)
	}
	unknown["token"] = "secret"
	unknownData, err := json.Marshal(unknown)
	if err != nil {
		t.Fatal(err)
	}
	tests := map[string][]byte{
		"unknown":  unknownData,
		"trailing": append(validData, []byte(" {}")...),
	}
	for name, contents := range tests {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "private", "config.json")
			writeProtectedConfigFixture(t, path, contents)
			if _, err := Read(path); err == nil {
				t.Fatal("Read() accepted invalid config")
			}
		})
	}
}

func TestListenPortMustBeUsable(t *testing.T) {
	for _, listen := range []string{"127.0.0.1:not-a-port", "127.0.0.1:0", "127.0.0.1:65536"} {
		t.Run(listen, func(t *testing.T) {
			cfg := Config{
				SchemaVersion: CurrentSchemaVersion,
				Role:          RoleBroker,
				ControllerID:  testID,
				Broker: BrokerConfig{
					Listen:    listen,
					StateFile: testStateFile(t),
					Auth:      AuthConfig{Mode: AuthModeNone},
				},
			}
			if err := cfg.Validate(); err == nil {
				t.Fatal("Validate() accepted unusable listen port")
			}
		})
	}
}

func TestBrokerStatusListenMustBeLoopbackAndDistinct(t *testing.T) {
	for _, statusListen := range []string{
		"0.0.0.0:8788",
		"broker.example.test:8788",
		"127.0.0.1:0",
		"127.0.0.1:not-a-port",
		"127.0.0.1:8787",
	} {
		t.Run(statusListen, func(t *testing.T) {
			cfg := Config{
				SchemaVersion: CurrentSchemaVersion,
				Role:          RoleBroker,
				ControllerID:  testID,
				Broker: BrokerConfig{
					Listen:       "127.0.0.1:8787",
					StatusListen: statusListen,
					StateFile:    testStateFile(t),
					Auth:         AuthConfig{Mode: AuthModeNone},
				},
			}
			if err := cfg.Validate(); err == nil {
				t.Fatal("Validate() accepted unsafe broker status listener")
			}
		})
	}

	cfg := Config{
		SchemaVersion: CurrentSchemaVersion,
		Role:          RoleBroker,
		ControllerID:  testID,
		Broker: BrokerConfig{
			Listen:                   "0.0.0.0:8787",
			StatusListen:             "[::1]:8788",
			StateFile:                testStateFile(t),
			Auth:                     AuthConfig{Mode: AuthModeNone},
			AllowInsecureNonLoopback: true,
		},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() rejected loopback status listener: %v", err)
	}
}

func TestNamedBrokerRequiresStatusListener(t *testing.T) {
	cfg := protectedTestConfig(t)
	cfg.InstanceID = "second"
	cfg.Role = RoleBroker
	cfg.Broker = BrokerConfig{
		Listen:    "127.0.0.1:18787",
		StateFile: testStateFile(t),
		Auth:      AuthConfig{Mode: AuthModeNone},
	}

	err := cfg.Validate()

	if err == nil || !strings.Contains(err.Error(), "require a status listener") {
		t.Fatalf("named broker validation error = %v", err)
	}
}

func TestBrokerStatusListenRejectsSameNumericPort(t *testing.T) {
	for _, primaryListen := range []string{
		"0.0.0.0:8788",
		"0.0.0.0:08788",
		"[::]:8788",
		"localhost:8788",
		"127.0.0.2:8788",
	} {
		t.Run(primaryListen, func(t *testing.T) {
			cfg := Config{
				SchemaVersion: CurrentSchemaVersion,
				Role:          RoleBroker,
				ControllerID:  testID,
				Broker: BrokerConfig{
					Listen: primaryListen, StatusListen: "127.0.0.1:8788",
					StateFile: testStateFile(t), Auth: AuthConfig{Mode: AuthModeNone},
					AllowInsecureNonLoopback: true,
				},
			}
			if err := cfg.Validate(); err == nil {
				t.Fatal("Validate() accepted overlapping broker listeners")
			}
		})
	}
}

func TestConfigRejectsUnsupportedSchemaVersions(t *testing.T) {
	for _, version := range []int{0, 3, CurrentSchemaVersion + 1} {
		cfg := protectedTestConfig(t)
		cfg.SchemaVersion = version
		err := cfg.Validate()
		if err == nil {
			t.Fatalf("Validate accepted schema version %d", version)
		}
		for _, text := range []string{"unsupported config schema version", "supports only version 4", "setup broker or setup peer"} {
			if !strings.Contains(err.Error(), text) {
				t.Fatalf("schema version %d error = %q, want %q", version, err, text)
			}
		}
	}
}

func TestReadReportsUnsupportedSchemaBeforeUnknownFields(t *testing.T) {
	for _, version := range []int{3, CurrentSchemaVersion + 1} {
		t.Run(strconv.Itoa(version), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "private", "unsupported.json")
			data, err := json.Marshal(map[string]any{
				"schemaVersion": version,
				"role":          "broker",
				"unknown":       true,
			})
			if err != nil {
				t.Fatal(err)
			}
			writeProtectedConfigFixture(t, path, data)
			_, err = Read(path)
			if err == nil ||
				!strings.Contains(err.Error(), fmt.Sprintf("unsupported config schema version %d", version)) ||
				!strings.Contains(err.Error(), "supports only version 4") ||
				strings.Contains(err.Error(), "unknown field") ||
				strings.Contains(err.Error(), "transport configuration") {
				t.Fatalf("unsupported config read error = %v", err)
			}
		})
	}
}

func testPeerRuntime(t *testing.T) PeerConfig {
	t.Helper()
	root := t.TempDir()
	return PeerConfig{
		CodexBinary:    filepath.Join(root, "codex"),
		GitBinary:      filepath.Join(root, "git"),
		CodexHome:      filepath.Join(root, "codex-home"),
		WorkspaceRoot:  filepath.Join(root, "workspaces"),
		StateFile:      filepath.Join(root, "peer.sqlite3"),
		MaxWorkerSlots: 4,
	}
}

func testPeerConfig(t *testing.T) Config {
	t.Helper()
	return Config{
		SchemaVersion: CurrentSchemaVersion,
		InstanceID:    DefaultInstanceID,
		HostKind:      hostkind.Codex,
		Role:          RolePeer,
		ControllerID:  testID,
		DeviceID:      "123e4567-e89b-42d3-a456-426614174001",
		DeviceName:    "builder",
		Broker: BrokerConfig{
			URL:  "wss://broker.example.test",
			Auth: AuthConfig{Mode: AuthModeNone},
		},
		Peer: testPeerRuntime(t),
	}
}

func testTailscaleTransport(t *testing.T) TransportConfig {
	t.Helper()
	root := t.TempDir()
	return TransportConfig{
		Mode: TransportModeTailscale,
		Tailscale: &TailscaleConfig{
			StateDir:    filepath.Join(root, "tailscale"),
			Hostname:    "delegation-node",
			AuthKeyFile: filepath.Join(root, "auth.key"),
		},
	}
}

func tailscaleRuntimeCapabilities() RuntimeCapabilities {
	return RuntimeCapabilities{EmbeddedTailscale: true}
}

func testTailscalePeerConfig(t *testing.T) Config {
	t.Helper()
	cfg := testPeerConfig(t)
	cfg.Transport = testTailscaleTransport(t)
	cfg.Broker.URL = "ws://broker.tailnet.test:8787/v1/connect"
	return cfg
}

func TestReadRejectsOversizedProtectedConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "private", "config.json")
	writeProtectedConfigFixture(t, path, make([]byte, maximumConfigSize+1))
	if _, err := Read(path); err == nil {
		t.Fatal("Read accepted an oversized config")
	}
}

func writeProtectedConfigFixture(t *testing.T, path string, data []byte) {
	t.Helper()
	directory := filepath.Dir(path)
	if err := createDirectoriesDurably(directory); err != nil {
		t.Fatal(err)
	}
	lease, err := holdConfigDirectory(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	tempName, temp, err := createConfigTemp(lease)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Remove(tempName)
	if _, err := temp.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := temp.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := lease.PublishNoReplace(tempName, filepath.Base(path)); err != nil {
		t.Fatal(err)
	}
}

func protectedTestConfig(t *testing.T) Config {
	t.Helper()
	return Config{
		SchemaVersion: CurrentSchemaVersion,
		InstanceID:    DefaultInstanceID,
		HostKind:      hostkind.Codex,
		Role:          RoleBroker,
		ControllerID:  testID,
		Broker: BrokerConfig{
			Listen:    "127.0.0.1:8787",
			StateFile: testStateFile(t),
			Auth:      AuthConfig{Mode: AuthModeNone},
		},
	}
}
