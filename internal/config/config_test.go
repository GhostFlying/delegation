package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

const testID = "123e4567-e89b-42d3-a456-426614174000"

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

func TestUnauthenticatedNonLoopbackRequiresAcknowledgement(t *testing.T) {
	cfg := Config{
		SchemaVersion: CurrentSchemaVersion,
		Role:          RoleBroker,
		ControllerID:  testID,
		Broker: BrokerConfig{
			Listen: "0.0.0.0:8787",
			Auth:   AuthConfig{Mode: AuthModeNone},
		},
	}

	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() succeeded without non-loopback acknowledgement")
	}
	cfg.Broker.AllowInsecureNonLoopback = true
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() with acknowledgement: %v", err)
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

func TestTokenAuthRequiresTLS(t *testing.T) {
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
		t.Fatal("Validate() accepted token authentication over ws://")
	}
	cfg.Broker.URL = "wss://broker.example.test"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() rejected token authentication over wss://: %v", err)
	}
}

func TestPlaintextBrokerURLRequiresLoopback(t *testing.T) {
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
	tests := map[string]string{
		"unknown":  `{"schemaVersion":1,"role":"broker","controllerId":"123e4567-e89b-42d3-a456-426614174000","broker":{"listen":"127.0.0.1:8787","auth":{"mode":"none"}},"token":"secret"}`,
		"trailing": `{"schemaVersion":1,"role":"broker","controllerId":"123e4567-e89b-42d3-a456-426614174000","broker":{"listen":"127.0.0.1:8787","auth":{"mode":"none"}}} {}`,
	}
	for name, contents := range tests {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := Read(path); err == nil {
				t.Fatal("Read() accepted invalid config")
			}
		})
	}
}

func TestDefaultStatePathUsesDelegationHome(t *testing.T) {
	home := t.TempDir()
	t.Setenv("DELEGATION_HOME", home)
	path, err := DefaultStatePath()
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(home, "state", "broker.sqlite3")
	if path != want {
		t.Fatalf("DefaultStatePath() = %q, want %q", path, want)
	}
}

func TestTokenAuthRejectsInsecureAcknowledgement(t *testing.T) {
	cfg := Config{
		SchemaVersion: CurrentSchemaVersion,
		Role:          RoleBroker,
		ControllerID:  testID,
		Broker: BrokerConfig{
			Listen:                   "0.0.0.0:8787",
			Auth:                     AuthConfig{Mode: AuthModeToken, TokenFile: filepath.Join(t.TempDir(), "broker.token")},
			AllowInsecureNonLoopback: true,
		},
	}

	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() accepted insecure acknowledgement with token auth")
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
					Listen: listen,
					Auth:   AuthConfig{Mode: AuthModeNone},
				},
			}
			if err := cfg.Validate(); err == nil {
				t.Fatal("Validate() accepted unusable listen port")
			}
		})
	}
}
