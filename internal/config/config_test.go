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
	}
	for _, brokerURL := range []string{
		"wss://broker.example.test/other",
		"wss://broker.example.test/%76%32/connect",
		"wss://broker.example.test?",
		"wss://broker.example.test/v2/connect#fragment",
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
		"wss://broker.example.test/v2/connect",
	} {
		t.Run("valid "+brokerURL, func(t *testing.T) {
			cfg := valid
			cfg.Broker.URL = brokerURL
			if err := cfg.Validate(); err != nil {
				t.Fatal(err)
			}
			got, err := NormalizeBrokerURL(brokerURL, false)
			if err != nil || got != "wss://broker.example.test/v2/connect" {
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

func TestSchemaOneRequiresVersionSpecificMigration(t *testing.T) {
	cfg := Config{
		SchemaVersion: 1,
		Role:          RoleBroker,
		ControllerID:  testID,
		Broker: BrokerConfig{
			Listen: "0.0.0.0:9876",
			Auth: AuthConfig{
				Mode:      AuthModeToken,
				TokenFile: filepath.Join(t.TempDir(), "broker.token"),
			},
		},
	}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("schema 1 config was accepted")
	}
	for _, text := range []string{
		"schema version 1", "version-specific secure migration", "schema version 3",
		"do not run delegation migrate config directly",
	} {
		if !strings.Contains(err.Error(), text) {
			t.Fatalf("schema 1 validation error = %q, want %q", err, text)
		}
	}
}

func TestSchemaThreeDeviceRequiresFreshPeerCredential(t *testing.T) {
	cfg := Config{
		SchemaVersion: LegacySchemaVersion,
		Role:          LegacyRoleDevice,
		ControllerID:  testID,
		DeviceID:      "123e4567-e89b-42d3-a456-426614174001",
		DeviceName:    "device",
		Broker: BrokerConfig{
			URL: "wss://broker.example.test",
			Auth: AuthConfig{
				Mode:      AuthModeToken,
				TokenFile: filepath.Join(t.TempDir(), "obsolete-target.token"),
			},
		},
	}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("schema 3 target config was accepted")
	}
	for _, text := range []string{"obsolete device role", "issue a fresh peer credential", "same deviceId", "--token-file"} {
		if !strings.Contains(err.Error(), text) {
			t.Fatalf("schema 3 target validation error = %q, want %q", err, text)
		}
	}
}

func TestReadSchemaTwoRequiresTransportAwareSetup(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		want []string
	}{
		{
			name: "token broker",
			cfg: Config{
				SchemaVersion: 2,
				Role:          RoleBroker,
				ControllerID:  testID,
				Broker: BrokerConfig{
					Listen:    "0.0.0.0:8787",
					StateFile: testStateFile(t),
					Auth: AuthConfig{
						Mode:      AuthModeToken,
						TokenFile: filepath.Join(t.TempDir(), "broker.token"),
					},
				},
			},
			want: []string{
				"schema version 2", "version-specific secure migration", "schema version 3",
				"do not run delegation migrate config directly",
			},
		},
		{
			name: "device",
			cfg: Config{
				SchemaVersion: 2,
				Role:          LegacyRoleDevice,
				ControllerID:  testID,
				DeviceID:      "123e4567-e89b-42d3-a456-426614174001",
				DeviceName:    "device",
				Broker: BrokerConfig{
					URL:  "wss://broker.example.test",
					Auth: AuthConfig{Mode: AuthModeNone},
				},
			},
			want: []string{
				"schema version 2", "version-specific secure migration", "schema version 3",
				"do not run delegation migrate config directly",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "private", "config.json")
			data, err := json.Marshal(test.cfg)
			if err != nil {
				t.Fatal(err)
			}
			writeProtectedConfigFixture(t, path, data)
			_, err = Read(path)
			if err == nil {
				t.Fatal("Read() accepted schema 2 config")
			}
			for _, text := range test.want {
				if !strings.Contains(err.Error(), text) {
					t.Fatalf("schema 2 read error = %q, want %q", err, text)
				}
			}
		})
	}
}

func TestFutureSchemaRequiresNewerRuntime(t *testing.T) {
	cfg := Config{SchemaVersion: CurrentSchemaVersion + 1}
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "newer delegation runtime") || strings.Contains(err.Error(), "rerun setup") {
		t.Fatalf("future schema validation error = %v", err)
	}
}

func TestReadFutureSchemaWithUnknownFieldsRequiresNewerRuntime(t *testing.T) {
	path := filepath.Join(t.TempDir(), "private", "future.json")
	data, err := json.Marshal(map[string]any{
		"schemaVersion": CurrentSchemaVersion + 1,
		"role":          "broker",
		"futureField":   true,
	})
	if err != nil {
		t.Fatal(err)
	}
	writeProtectedConfigFixture(t, path, data)
	_, err = Read(path)
	if err == nil || !strings.Contains(err.Error(), "newer delegation runtime") || strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("future config read error = %v", err)
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
