package runtimeconfig

import (
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
			Auth:         delegationconfig.AuthConfig{Mode: delegationconfig.AuthModeNone},
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
