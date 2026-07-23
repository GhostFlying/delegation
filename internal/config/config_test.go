package config

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
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

func TestConfigRejectsUnsupportedSchemaVersions(t *testing.T) {
	for _, version := range []int{0, CurrentSchemaVersion + 1} {
		cfg := protectedTestConfig(t)
		cfg.SchemaVersion = version
		err := cfg.Validate()
		if err == nil {
			t.Fatalf("Validate accepted schema version %d", version)
		}
		for _, text := range []string{"unsupported config schema version", "supports only version 3", "setup broker or setup peer"} {
			if !strings.Contains(err.Error(), text) {
				t.Fatalf("schema version %d error = %q, want %q", version, err, text)
			}
		}
	}
}

func TestReadReportsUnsupportedSchemaBeforeUnknownFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "private", "unsupported.json")
	data, err := json.Marshal(map[string]any{
		"schemaVersion": CurrentSchemaVersion + 1,
		"role":          "broker",
		"unknown":       true,
	})
	if err != nil {
		t.Fatal(err)
	}
	writeProtectedConfigFixture(t, path, data)
	_, err = Read(path)
	if err == nil || !strings.Contains(err.Error(), "unsupported config schema version 4") ||
		strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("unsupported config read error = %v", err)
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
		Role:          RoleBroker,
		ControllerID:  testID,
		Broker: BrokerConfig{
			Listen:    "127.0.0.1:8787",
			StateFile: testStateFile(t),
			Auth:      AuthConfig{Mode: AuthModeNone},
		},
	}
}
