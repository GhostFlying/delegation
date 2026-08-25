package runtimeconfig

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	delegationconfig "github.com/GhostFlying/delegation/internal/config"
)

func TestReadAcceptsEmbeddedTailscaleWithoutChangingPlainReadContract(t *testing.T) {
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
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "broker.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := Read(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.Transport.Mode != delegationconfig.TransportModeTailscale {
		t.Fatalf("runtime transport mode = %v", got.Transport.Mode)
	}
	if _, err := delegationconfig.Read(path); err == nil {
		t.Fatal("plain config.Read accepted embedded tailscale")
	}
}
