package cli

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/GhostFlying/delegation/internal/buildinfo"
	"github.com/GhostFlying/delegation/internal/clilaunch"
	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/hostkind"
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

func TestDoctorRejectsCLILauncherInsideManagedWorkspace(t *testing.T) {
	configPath := privateTestPath(t, "peer.json")
	var setupOutput bytes.Buffer
	var setupError bytes.Buffer
	if code := Run([]string{
		"setup", "peer",
		"--config", configPath,
		"--controller-id", "123e4567-e89b-42d3-a456-426614174000",
		"--device-id", "123e4567-e89b-42d3-a456-426614174001",
		"--device-name", "builder",
		"--broker-url", "wss://broker.example.test",
		"--auth-mode", "none",
		"--codex-binary", testCodexBinary(t),
	}, &setupOutput, &setupError); code != 0 {
		t.Fatalf("setup code = %d, want 0; stderr = %q", code, setupError.String())
	}
	cfg, err := delegationconfig.Read(configPath)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Peer.CLI.Launcher = &clilaunch.Spec{
		Executable: filepath.Join(cfg.Peer.WorkspaceRoot, "launcher"),
	}
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, data, 0o600); err != nil {
		t.Fatal(err)
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := Run([]string{"doctor", "--config", configPath}, &stdout, &stderr)
	if code == 0 || !strings.Contains(stderr.String(), "CLI launcher must not be inside worker workspace root") {
		t.Fatalf("doctor code = %d, stderr = %q", code, stderr.String())
	}
}

func TestDoctorRejectsMissingCLILauncher(t *testing.T) {
	configPath := privateTestPath(t, "peer.json")
	var setupOutput bytes.Buffer
	var setupError bytes.Buffer
	if code := Run([]string{
		"setup", "peer",
		"--config", configPath,
		"--controller-id", "123e4567-e89b-42d3-a456-426614174000",
		"--device-id", "123e4567-e89b-42d3-a456-426614174001",
		"--device-name", "builder",
		"--broker-url", "wss://broker.example.test",
		"--auth-mode", "none",
		"--codex-binary", testCodexBinary(t),
	}, &setupOutput, &setupError); code != 0 {
		t.Fatalf("setup code = %d, want 0; stderr = %q", code, setupError.String())
	}
	cfg, err := delegationconfig.Read(configPath)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Peer.CLI.Launcher = &clilaunch.Spec{
		Executable: filepath.Join(t.TempDir(), "missing-launcher"),
	}
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, data, 0o600); err != nil {
		t.Fatal(err)
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := Run([]string{"doctor", "--config", configPath}, &stdout, &stderr)
	if code == 0 || !strings.Contains(stderr.String(), "resolve configured CLI launch") {
		t.Fatalf("doctor code = %d, stderr = %q", code, stderr.String())
	}
}

func TestDoctorRejectsPrepopulatedManagedTraeXHome(t *testing.T) {
	for _, test := range []struct {
		name     string
		relative string
		want     string
	}{
		{name: "instructions", relative: "AGENTS.md", want: "AGENTS.md"},
		{name: "CLI authentication", relative: filepath.Join("cli", "auth.json"), want: "auth.json"},
		{name: "CLI hooks", relative: filepath.Join("cli", "hooks.json"), want: "hooks.json"},
		{name: "CLI plugins", relative: filepath.Join("cli", "plugins"), want: "plugins"},
		{name: "CLI rules", relative: filepath.Join("cli", "rules"), want: "rules"},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := privateTestDirectory(t)
			configPath := filepath.Join(root, "peer.json")
			managedHome := filepath.Join(root, "managed-trae")
			executable := testCodexBinary(t)
			var stdout bytes.Buffer
			var stderr bytes.Buffer
			if code := Run([]string{
				"setup", "peer",
				"--config", configPath,
				"--instance", "traex-main",
				"--host-kind", "traex",
				"--controller-id", "123e4567-e89b-42d3-a456-426614174000",
				"--device-id", "123e4567-e89b-42d3-a456-426614174001",
				"--broker-url", "wss://broker.example.test",
				"--auth-mode", "none",
				"--cli-command", executable,
				"--cli-launcher", executable,
				"--codex-home", managedHome,
			}, &stdout, &stderr); code != 0 {
				t.Fatalf("setup code = %d, stderr = %q", code, stderr.String())
			}
			artifact := filepath.Join(managedHome, test.relative)
			if err := os.MkdirAll(filepath.Dir(artifact), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(artifact, []byte("ambient"), 0o600); err != nil {
				t.Fatal(err)
			}
			stdout.Reset()
			stderr.Reset()
			code := Run([]string{"doctor", "--config", configPath}, &stdout, &stderr)
			if code == 0 || !strings.Contains(stderr.String(), test.want) {
				t.Fatalf("doctor code = %d, stderr = %q", code, stderr.String())
			}
		})
	}
}

func TestDoctorRejectsSymlinkedTraeCLIHome(t *testing.T) {
	root := privateTestDirectory(t)
	configPath := filepath.Join(root, "peer.json")
	managedHome := filepath.Join(root, "managed-trae")
	executable := testCodexBinary(t)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if code := Run([]string{
		"setup", "peer",
		"--config", configPath,
		"--instance", "traex-main",
		"--host-kind", "traex",
		"--controller-id", "123e4567-e89b-42d3-a456-426614174000",
		"--device-id", "123e4567-e89b-42d3-a456-426614174001",
		"--broker-url", "wss://broker.example.test",
		"--auth-mode", "none",
		"--cli-command", executable,
		"--cli-launcher", executable,
		"--codex-home", managedHome,
	}, &stdout, &stderr); code != 0 {
		t.Fatalf("setup code = %d, stderr = %q", code, stderr.String())
	}
	cliHome := filepath.Join(managedHome, "cli")
	target := filepath.Join(root, "external-cli")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, cliHome); err != nil {
		t.Skipf("symbolic links are unavailable: %v", err)
	}
	stdout.Reset()
	stderr.Reset()
	code := Run([]string{"doctor", "--config", configPath}, &stdout, &stderr)
	if code == 0 || !strings.Contains(stderr.String(), "TRAECLI_HOME must not be a symbolic link") {
		t.Fatalf("doctor code = %d, stderr = %q", code, stderr.String())
	}
}

func TestDoctorRejectsMismatchedBrokerStateHostKind(t *testing.T) {
	environment := setupCredentialTestBroker(t, "token")
	setCredentialTestHostKind(t, environment.configPath, hostkind.TraeX)
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run(
		[]string{"doctor", "--config", environment.configPath, "--json"},
		&stdout,
		&stderr,
	)
	if code == 0 || !strings.Contains(stderr.String(), "does not match state host kind") {
		t.Fatalf("doctor code = %d, stderr = %q", code, stderr.String())
	}
}
