package config

import (
	"encoding/json"
	"os"
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
		Role:          RoleDevice,
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
	path := filepath.Join(t.TempDir(), "config.json")

	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := Read(path)
	if err != nil {
		t.Fatal(err)
	}
	if got != cfg {
		t.Fatalf("Read() = %#v, want %#v", got, cfg)
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
		Role:          RoleController,
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

func TestTokenAuthPlaintextRequiresAcknowledgement(t *testing.T) {
	cfg := Config{
		SchemaVersion: CurrentSchemaVersion,
		Role:          RoleDevice,
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
		Role:          RoleDevice,
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
				Role:          RoleDevice,
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
			path := filepath.Join(t.TempDir(), "config.json")
			if err := os.WriteFile(path, contents, 0o600); err != nil {
				t.Fatal(err)
			}
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

func TestSchemaOneRequiresSetupAgain(t *testing.T) {
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
	for _, text := range []string{"move the config aside", "--controller-id", "--listen", "--auth-mode", "--token-file", "--state", "--allow-insecure-nonloopback", "any non-loopback listener"} {
		if !strings.Contains(err.Error(), text) {
			t.Fatalf("schema 1 validation error = %q, want %q", err, text)
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
			want: []string{"schema version 2", "--token-file", "--state", "--allow-insecure-nonloopback", "any non-loopback listener"},
		},
		{
			name: "device",
			cfg: Config{
				SchemaVersion: 2,
				Role:          RoleDevice,
				ControllerID:  testID,
				DeviceID:      "123e4567-e89b-42d3-a456-426614174001",
				DeviceName:    "device",
				Broker: BrokerConfig{
					URL:  "wss://broker.example.test",
					Auth: AuthConfig{Mode: AuthModeNone},
				},
			},
			want: []string{"schema version 2", "--controller-id", "--device-id", "--device-name", "--broker-url", "--auth-mode", "--token-file", "--allow-insecure-nonloopback", "non-loopback ws://"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			data, err := json.Marshal(test.cfg)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, data, 0o600); err != nil {
				t.Fatal(err)
			}
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
	path := filepath.Join(t.TempDir(), "future.json")
	data, err := json.Marshal(map[string]any{
		"schemaVersion": CurrentSchemaVersion + 1,
		"role":          "broker",
		"futureField":   true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = Read(path)
	if err == nil || !strings.Contains(err.Error(), "newer delegation runtime") || strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("future config read error = %v", err)
	}
}
