package runtimeconfig

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	delegationconfig "github.com/GhostFlying/delegation/internal/config"
)

func TestEmbeddedTailscaleConfigRoundTripWithoutChangingPlainConfigContract(t *testing.T) {
	root := filepath.Join(t.TempDir(), "authority")
	if err := delegationconfig.PreparePrivateDirectory(root); err != nil {
		t.Fatal(err)
	}
	cfg := delegationconfig.Config{
		SchemaVersion: delegationconfig.CurrentSchemaVersion,
		Role:          delegationconfig.RoleBroker,
		ControllerID:  "123e4567-e89b-42d3-a456-426614174700",
		Transport: delegationconfig.TransportConfig{
			Mode: delegationconfig.TransportModeTailscale,
			Tailscale: &delegationconfig.TailscaleConfig{
				StateDir:    filepath.Join(root, "tailscale"),
				Hostname:    "broker-node",
				AuthKeyFile: filepath.Join(root, "tailscale-auth.key"),
			},
		},
		Broker: delegationconfig.BrokerConfig{
			Listen:       ":8787",
			StatusListen: "127.0.0.1:8788",
			StateFile:    filepath.Join(root, "broker.sqlite3"),
			Auth: delegationconfig.AuthConfig{
				Mode:      delegationconfig.AuthModeToken,
				TokenFile: filepath.Join(root, "broker.token"),
			},
		},
	}

	capabilities := Capabilities()
	if !capabilities.EmbeddedTailscale {
		t.Fatal("Capabilities() does not report embedded tailscale")
	}
	if err := Validate(cfg); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "not supported by this runtime") {
		t.Fatalf("config.Validate() error = %v, want unsupported runtime", err)
	}

	path := filepath.Join(root, "broker.json")
	if err := WriteNew(path, cfg); err != nil {
		t.Fatal(err)
	}
	plainWritePath := filepath.Join(root, "plain-broker.json")
	if err := delegationconfig.WriteNew(plainWritePath, cfg); err == nil ||
		!strings.Contains(err.Error(), "not supported by this runtime") {
		t.Fatalf("config.WriteNew() error = %v, want unsupported runtime", err)
	}

	got, err := Read(path)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, cfg) {
		t.Fatalf("Read() = %#v, want %#v", got, cfg)
	}
	if _, err := delegationconfig.Read(path); err == nil ||
		!strings.Contains(err.Error(), "not supported by this runtime") {
		t.Fatalf("config.Read() error = %v, want unsupported runtime", err)
	}
}

func TestEmbeddedTailscaleRejectsNoneAuthenticationBeforeWrite(t *testing.T) {
	root := filepath.Join(t.TempDir(), "authority")
	if err := delegationconfig.PreparePrivateDirectory(root); err != nil {
		t.Fatal(err)
	}
	cfg := delegationconfig.Config{
		SchemaVersion: delegationconfig.CurrentSchemaVersion,
		Role:          delegationconfig.RoleBroker,
		ControllerID:  "123e4567-e89b-42d3-a456-426614174700",
		Transport: delegationconfig.TransportConfig{
			Mode: delegationconfig.TransportModeTailscale,
			Tailscale: &delegationconfig.TailscaleConfig{
				StateDir:    filepath.Join(root, "tailscale"),
				Hostname:    "broker-node",
				AuthKeyFile: filepath.Join(root, "tailscale-auth.key"),
			},
		},
		Broker: delegationconfig.BrokerConfig{
			Listen:       ":8787",
			StatusListen: "127.0.0.1:8788",
			StateFile:    filepath.Join(root, "broker.sqlite3"),
			Auth:         delegationconfig.AuthConfig{Mode: delegationconfig.AuthModeNone},
		},
	}

	if err := Validate(cfg); err == nil || !strings.Contains(err.Error(), "auth mode none") {
		t.Fatalf("Validate() error = %v, want auth mode none rejection", err)
	}

	handWrittenPath := filepath.Join(root, "hand-written.json")
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(handWrittenPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Read(handWrittenPath); err == nil || !strings.Contains(err.Error(), "auth mode none") {
		t.Fatalf("Read() error = %v, want auth mode none rejection", err)
	}

	writePath := filepath.Join(t.TempDir(), "new", "broker.json")
	if err := WriteNew(writePath, cfg); err == nil || !strings.Contains(err.Error(), "auth mode none") {
		t.Fatalf("WriteNew() error = %v, want auth mode none rejection", err)
	}
	if _, err := os.Lstat(filepath.Dir(writePath)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("WriteNew() created config directory: %v", err)
	}
}
