package cli

import (
	"bytes"
	"encoding/json"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/GhostFlying/delegation/internal/buildinfo"
	delegationconfig "github.com/GhostFlying/delegation/internal/config"
)

func TestDoctorValidatesBrokerConfiguration(t *testing.T) {
	configPath := privateTestPath(t, "config.json")
	var setupOutput bytes.Buffer
	var setupError bytes.Buffer
	if code := Run([]string{"setup", "broker", "--config", configPath}, &setupOutput, &setupError); code != 0 {
		t.Fatalf("setup code = %d, want 0; stderr = %q", code, setupError.String())
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run([]string{"doctor", "--config", configPath, "--json"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("doctor code = %d, want 0; stderr = %q", code, stderr.String())
	}
	var got doctorResult
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	want := doctorResult{
		OK:         true,
		Version:    buildinfo.Version,
		ConfigPath: configPath,
		Role:       delegationconfig.RoleBroker,
		Checks: []string{
			"configuration schema and role are valid",
			"broker state and authority paths are safe",
			"token file exists and is protected",
		},
	}
	if got.OK != want.OK || got.Version != want.Version || got.ConfigPath != want.ConfigPath || got.Role != want.Role || !slices.Equal(got.Checks, want.Checks) {
		t.Fatalf("doctor result = %#v, want %#v", got, want)
	}
}

func TestDoctorRejectsMalformedToken(t *testing.T) {
	configPath := privateTestPath(t, "config.json")
	var setupOutput bytes.Buffer
	var setupError bytes.Buffer
	if code := Run([]string{"setup", "broker", "--config", configPath}, &setupOutput, &setupError); code != 0 {
		t.Fatalf("setup code = %d, want 0; stderr = %q", code, setupError.String())
	}
	cfg, err := delegationconfig.Read(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cfg.Broker.Auth.TokenFile, []byte("invalid\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run([]string{"doctor", "--config", configPath, "--json"}, &stdout, &stderr)
	if code == 0 {
		t.Fatal("doctor accepted malformed token material")
	}
	if !strings.Contains(stderr.String(), "256-bit base64url token") {
		t.Fatalf("stderr = %q", stderr.String())
	}
}
