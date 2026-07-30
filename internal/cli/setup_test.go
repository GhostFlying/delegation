package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/GhostFlying/delegation/internal/clilaunch"
	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/hostkind"
	"github.com/GhostFlying/delegation/internal/identity"
	"github.com/GhostFlying/delegation/internal/tokenfile"
)

func testCodexBinary(t *testing.T) string {
	t.Helper()
	path, err := filepath.Abs(os.Args[0])
	if err != nil {
		t.Fatal(err)
	}
	return path
}

func TestSetupBroker(t *testing.T) {
	dir := privateTestDirectory(t)
	configPath := filepath.Join(dir, "config.json")
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run([]string{"setup", "broker", "--config", configPath, "--json"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("setup code = %d, want 0; stderr = %q", code, stderr.String())
	}
	var result setupResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	wantState := filepath.Join(dir, "state", "broker.sqlite3")
	if result.Role != delegationconfig.RoleBroker || result.ConfigPath != configPath || result.ControllerID == "" ||
		result.InstanceID != delegationconfig.DefaultInstanceID || result.HostKind != hostkind.Codex ||
		result.StatePath != wantState || result.TokenFile == "" || result.StatusListen != "127.0.0.1:8788" {
		t.Fatalf("setup result = %#v", result)
	}
	cfg, err := delegationconfig.Read(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Role != delegationconfig.RoleBroker || cfg.ControllerID != result.ControllerID ||
		cfg.InstanceID != result.InstanceID || cfg.HostKind != result.HostKind ||
		cfg.Broker.StateFile != result.StatePath || cfg.Broker.Auth.TokenFile != result.TokenFile ||
		cfg.Broker.StatusListen != result.StatusListen {
		t.Fatalf("config = %#v, setup result = %#v", cfg, result)
	}
	token, err := os.ReadFile(result.TokenFile)
	if err != nil {
		t.Fatal(err)
	}
	configData, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(configData, bytes.TrimSpace(token)) {
		t.Fatal("config contains token material")
	}
}

func TestSetupBrokerPersistsInstanceAndTraeXNetworkKind(t *testing.T) {
	configPath := filepath.Join(privateTestDirectory(t), "traex.json")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := Run([]string{
		"setup", "broker",
		"--config", configPath,
		"--instance", "traex-main",
		"--host-kind", "traex",
		"--listen", "127.0.0.1:18787",
		"--status-listen", "127.0.0.1:18788",
		"--auth-mode", "none",
		"--json",
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("setup code = %d, stderr = %q", code, stderr.String())
	}
	cfg, err := delegationconfig.Read(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.InstanceID != "traex-main" || cfg.HostKind != hostkind.TraeX {
		t.Fatalf("TraeX broker config = %#v", cfg)
	}
}

func TestSetupNamedBrokerRequiresExplicitListenersWithoutSideEffects(t *testing.T) {
	home := privateTestDirectory(t)
	t.Setenv("DELEGATION_HOME", home)
	t.Setenv("DELEGATION_CONFIG", "")
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run([]string{
		"setup", "broker",
		"--instance", "second",
	}, &stdout, &stderr)

	if code == 0 || !strings.Contains(stderr.String(), "require explicit --listen and --status-listen") {
		t.Fatalf("setup code = %d, stderr = %q", code, stderr.String())
	}
	configPath := filepath.Join(home, "instances", "second", "broker.json")
	if _, err := os.Lstat(configPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("setup created named broker config: %v", err)
	}
}

func TestSetupNamedInstanceRejectsImplicitDelegationConfig(t *testing.T) {
	for _, role := range []string{"broker", "peer"} {
		t.Run(role, func(t *testing.T) {
			override := privateTestPath(t, role+".json")
			t.Setenv("DELEGATION_CONFIG", override)
			args := []string{"setup", role, "--instance", "second"}
			if role == "broker" {
				args = append(args,
					"--listen", "127.0.0.1:18787",
					"--status-listen", "127.0.0.1:18788",
				)
			}
			var stdout bytes.Buffer
			var stderr bytes.Buffer

			code := Run(args, &stdout, &stderr)

			if code == 0 || !strings.Contains(stderr.String(), "requires an explicit --config path") {
				t.Fatalf("setup code = %d, stderr = %q", code, stderr.String())
			}
			if _, err := os.Lstat(override); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("setup created implicit override config: %v", err)
			}
		})
	}
}

func TestSetupNamedBrokerRejectsEmptyStatusListenerWithoutSideEffects(t *testing.T) {
	home := privateTestDirectory(t)
	t.Setenv("DELEGATION_HOME", home)
	t.Setenv("DELEGATION_CONFIG", "")
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run([]string{
		"setup", "broker",
		"--instance", "second",
		"--listen", "127.0.0.1:18787",
		"--status-listen=",
	}, &stdout, &stderr)

	if code == 0 || !strings.Contains(stderr.String(), "require a status listener") {
		t.Fatalf("setup code = %d, stderr = %q", code, stderr.String())
	}
	instanceHome := filepath.Join(home, "instances", "second")
	for _, path := range []string{
		filepath.Join(instanceHome, "broker.json"),
		filepath.Join(instanceHome, "secrets", "broker.token"),
	} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("setup created %s: %v", path, err)
		}
	}
}

func TestSetupNamedBrokerUsesIsolatedDefaults(t *testing.T) {
	home := privateTestDirectory(t)
	t.Setenv("DELEGATION_HOME", home)
	t.Setenv("DELEGATION_CONFIG", "")
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run([]string{
		"setup", "broker",
		"--instance", "second",
		"--listen", "127.0.0.1:18787",
		"--status-listen", "127.0.0.1:18788",
		"--json",
	}, &stdout, &stderr)

	if code != 0 {
		t.Fatalf("setup code = %d, stderr = %q", code, stderr.String())
	}
	var result setupResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	instanceHome := filepath.Join(home, "instances", "second")
	if result.ConfigPath != filepath.Join(instanceHome, "broker.json") ||
		result.StatePath != filepath.Join(instanceHome, "state", "broker.sqlite3") ||
		result.TokenFile != filepath.Join(instanceHome, "secrets", "broker.token") {
		t.Fatalf("named broker result = %#v", result)
	}
}

func TestSetupNamedBrokersWithExplicitConfigsIsolateImplicitResources(t *testing.T) {
	root := privateTestDirectory(t)
	type configured struct {
		instance string
		listen   string
		status   string
	}
	for _, setup := range []configured{
		{instance: "alpha", listen: "127.0.0.1:18787", status: "127.0.0.1:18788"},
		{instance: "beta", listen: "127.0.0.1:28787", status: "127.0.0.1:28788"},
	} {
		configPath := filepath.Join(root, setup.instance+".json")
		var stdout bytes.Buffer
		var stderr bytes.Buffer
		code := Run([]string{
			"setup", "broker",
			"--config", configPath,
			"--instance", setup.instance,
			"--listen", setup.listen,
			"--status-listen", setup.status,
			"--json",
		}, &stdout, &stderr)
		if code != 0 {
			t.Fatalf("setup %s code = %d, stderr = %q", setup.instance, code, stderr.String())
		}
		cfg, err := delegationconfig.Read(configPath)
		if err != nil {
			t.Fatal(err)
		}
		instanceRoot := filepath.Join(root, "instances", setup.instance)
		if cfg.Broker.StateFile != filepath.Join(instanceRoot, "state", "broker.sqlite3") ||
			cfg.Broker.Auth.TokenFile != filepath.Join(instanceRoot, "secrets", "broker.token") {
			t.Fatalf("%s resources = %#v", setup.instance, cfg.Broker)
		}
	}
}

func TestSetupPeerRejectsTraeXWithoutStructuredLaunchWithoutSideEffects(t *testing.T) {
	configPath := privateTestPath(t, "traex-peer.json")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := Run([]string{
		"setup", "peer",
		"--config", configPath,
		"--instance", "traex-main",
		"--host-kind", "traex",
		"--controller-id", "123e4567-e89b-42d3-a456-426614174000",
		"--device-id", "123e4567-e89b-42d3-a456-426614174001",
		"--broker-url", "wss://broker.example.test",
		"--auth-mode", "none",
	}, &stdout, &stderr)
	if code == 0 || !strings.Contains(stderr.String(), "requires --cli-command and --cli-launcher") {
		t.Fatalf("setup code = %d, stderr = %q", code, stderr.String())
	}
	if _, err := os.Lstat(configPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("setup created unsupported TraeX peer config: %v", err)
	}
}

func TestSetupTraeXPeerPersistsExactStructuredLaunchAndPassesDoctor(t *testing.T) {
	root := privateTestDirectory(t)
	configPath := filepath.Join(root, "traex-peer.json")
	managedHome := filepath.Join(root, "managed-home")
	workspaceRoot := filepath.Join(root, "workspaces")
	statePath := filepath.Join(root, "state", "peer.sqlite3")
	command := testCodexBinary(t)
	launcher := testCodexBinary(t)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := Run([]string{
		"setup", "peer",
		"--config", configPath,
		"--instance", "traex-main",
		"--host-kind", "traex",
		"--controller-id", "123e4567-e89b-42d3-a456-426614174000",
		"--device-id", "123e4567-e89b-42d3-a456-426614174001",
		"--device-name", "traex-builder",
		"--broker-url", "wss://broker.example.test",
		"--auth-mode", "none",
		"--cli-command", command,
		"--cli-argument=-p",
		"--cli-argument=profile with spaces",
		"--cli-argument=",
		"--cli-launcher", launcher,
		"--cli-launcher-prefix-argument=run",
		"--cli-launcher-prefix-argument=--",
		"--codex-home", managedHome,
		"--workspace-root", workspaceRoot,
		"--state", statePath,
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("setup code = %d, stderr = %q", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "managed TRAE_HOME: "+managedHome) {
		t.Fatalf("setup output = %q", stdout.String())
	}
	cfg, err := delegationconfig.Read(configPath)
	if err != nil {
		t.Fatal(err)
	}
	wantCLI := &delegationconfig.CLIConfig{
		Command:   command,
		Arguments: []string{"-p", "profile with spaces", ""},
		Launcher: &clilaunch.Spec{
			Executable:      launcher,
			PrefixArguments: []string{"run", "--"},
		},
	}
	if cfg.HostKind != hostkind.TraeX || !reflect.DeepEqual(cfg.Peer.CLI, wantCLI) ||
		cfg.Peer.CodexBinary != "" {
		t.Fatalf("TraeX peer config = %#v", cfg)
	}
	stdout.Reset()
	stderr.Reset()
	if code := Run(
		[]string{"doctor", "--config", configPath, "--json"},
		&stdout,
		&stderr,
	); code != 0 {
		t.Fatalf("doctor code = %d, stderr = %q", code, stderr.String())
	}
}

func TestSetupCodexPeerAcceptsDirectStructuredCommand(t *testing.T) {
	configPath := privateTestPath(t, "codex-structured.json")
	command := testCodexBinary(t)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := Run([]string{
		"setup", "peer",
		"--config", configPath,
		"--controller-id", "123e4567-e89b-42d3-a456-426614174000",
		"--device-id", "123e4567-e89b-42d3-a456-426614174001",
		"--device-name", "codex-builder",
		"--broker-url", "wss://broker.example.test",
		"--auth-mode", "none",
		"--cli-command", command,
		"--cli-argument=-p",
		"--cli-argument=ultra",
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("setup code = %d, stderr = %q", code, stderr.String())
	}
	cfg, err := delegationconfig.Read(configPath)
	if err != nil {
		t.Fatal(err)
	}
	want := &delegationconfig.CLIConfig{
		Command:   command,
		Arguments: []string{"-p", "ultra"},
	}
	if cfg.HostKind != hostkind.Codex || !reflect.DeepEqual(cfg.Peer.CLI, want) {
		t.Fatalf("Codex structured config = %#v", cfg)
	}
}

func TestSetupNamedTraeXPeerUsesIsolatedTraeHomeDefault(t *testing.T) {
	home := privateTestDirectory(t)
	t.Setenv("DELEGATION_HOME", home)
	t.Setenv("DELEGATION_CONFIG", "")
	executable := testCodexBinary(t)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := Run([]string{
		"setup", "peer",
		"--instance", "traex-main",
		"--host-kind", "traex",
		"--controller-id", "123e4567-e89b-42d3-a456-426614174000",
		"--device-id", "123e4567-e89b-42d3-a456-426614174001",
		"--device-name", "traex-builder",
		"--broker-url", "wss://broker.example.test",
		"--auth-mode", "none",
		"--cli-command", executable,
		"--cli-launcher", executable,
		"--json",
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("setup code = %d, stderr = %q", code, stderr.String())
	}
	var result setupResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	instanceHome := filepath.Join(home, "instances", "traex-main")
	if result.CodexHome != filepath.Join(instanceHome, "trae") {
		t.Fatalf("TraeX managed home = %q", result.CodexHome)
	}
}

func TestSetupPeerRejectsInvalidStructuredCLIFlagsWithoutSideEffects(t *testing.T) {
	for _, test := range []struct {
		name     string
		hostKind string
		extra    func(*testing.T) []string
		want     string
	}{
		{
			name:     "TraeX explicit Codex shorthand",
			hostKind: "traex",
			extra: func(t *testing.T) []string {
				return []string{"--codex-binary", testCodexBinary(t)}
			},
			want: "supported only for Codex peers",
		},
		{
			name:     "TraeX command without launcher",
			hostKind: "traex",
			extra: func(t *testing.T) []string {
				return []string{"--cli-command", testCodexBinary(t)}
			},
			want: "requires --cli-launcher",
		},
		{
			name:     "argument without command",
			hostKind: "codex",
			extra: func(*testing.T) []string {
				return []string{"--cli-argument=-p"}
			},
			want: "--cli-command is required",
		},
		{
			name:     "prefix without launcher",
			hostKind: "codex",
			extra: func(t *testing.T) []string {
				return []string{
					"--cli-command", testCodexBinary(t),
					"--cli-launcher-prefix-argument=run",
				}
			},
			want: "requires --cli-launcher",
		},
		{
			name:     "legacy and structured flags",
			hostKind: "codex",
			extra: func(t *testing.T) []string {
				return []string{
					"--codex-binary", testCodexBinary(t),
					"--cli-command", testCodexBinary(t),
				}
			},
			want: "cannot be combined",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			configPath := filepath.Join(root, "peer.json")
			args := []string{
				"setup", "peer",
				"--config", configPath,
				"--host-kind", test.hostKind,
				"--controller-id", "123e4567-e89b-42d3-a456-426614174000",
				"--device-id", "123e4567-e89b-42d3-a456-426614174001",
				"--broker-url", "wss://broker.example.test",
				"--auth-mode", "none",
			}
			args = append(args, test.extra(t)...)
			var stdout bytes.Buffer
			var stderr bytes.Buffer
			code := Run(args, &stdout, &stderr)
			if code == 0 || !strings.Contains(stderr.String(), test.want) {
				t.Fatalf("setup code = %d, stderr = %q, want %q", code, stderr.String(), test.want)
			}
			if _, err := os.Lstat(configPath); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("setup created invalid structured config: %v", err)
			}
		})
	}
}

func TestSetupPeerRejectsUnsupportedHostKindBeforeSideEffects(t *testing.T) {
	configPath := privateTestPath(t, "unsupported-peer.json")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := Run([]string{
		"setup", "peer",
		"--config", configPath,
		"--host-kind", "claude",
		"--controller-id", "123e4567-e89b-42d3-a456-426614174000",
		"--device-id", "123e4567-e89b-42d3-a456-426614174001",
		"--broker-url", "wss://broker.example.test",
		"--auth-mode", "none",
	}, &stdout, &stderr)
	if code == 0 || !strings.Contains(stderr.String(), `unsupported host kind "claude"`) {
		t.Fatalf("setup code = %d, stderr = %q", code, stderr.String())
	}
	if _, err := os.Lstat(configPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("setup created unsupported peer config: %v", err)
	}
}

func TestSetupBrokerRejectsNonLoopbackStatusListener(t *testing.T) {
	configPath := filepath.Join(privateTestDirectory(t), "config.json")
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run([]string{
		"setup", "broker", "--config", configPath,
		"--status-listen", "0.0.0.0:8788",
	}, &stdout, &stderr)

	if code == 0 || !strings.Contains(stderr.String(), "status listener must use a loopback address") {
		t.Fatalf("setup code = %d, stderr = %q", code, stderr.String())
	}
	if _, err := os.Lstat(configPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("setup wrote rejected broker config: %v", err)
	}
}

func TestSetupBrokerPersistsExplicitStatePath(t *testing.T) {
	dir := privateTestDirectory(t)
	configPath := filepath.Join(dir, "config.json")
	statePath := privateTestPath(t, filepath.Join("registry", "broker.sqlite3"))
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := Run([]string{
		"setup", "broker",
		"--config", configPath,
		"--state", statePath,
		"--auth-mode", "none",
		"--json",
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("setup code = %d, stderr = %q", code, stderr.String())
	}
	cfg, err := delegationconfig.Read(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Broker.StateFile != statePath {
		t.Fatalf("broker stateFile = %q, want %q", cfg.Broker.StateFile, statePath)
	}
}

func TestSetupBrokerRejectsConfigStatePathCollision(t *testing.T) {
	path := filepath.Join(t.TempDir(), "authority")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := Run([]string{
		"setup", "broker",
		"--config", path,
		"--state", path,
		"--auth-mode", "none",
	}, &stdout, &stderr)
	if code == 0 || !strings.Contains(stderr.String(), "broker configuration") {
		t.Fatalf("setup code = %d, stderr = %q", code, stderr.String())
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("setup created colliding authority path: %v", err)
	}
}

func TestSetupBrokerRejectsUnusableStateWithoutSideEffects(t *testing.T) {
	for _, test := range []struct {
		name  string
		state func(*testing.T) string
	}{
		{
			name: "directory",
			state: func(t *testing.T) string {
				return t.TempDir()
			},
		},
		{
			name: "symlink",
			state: func(t *testing.T) string {
				target := filepath.Join(t.TempDir(), "target.sqlite3")
				if err := os.WriteFile(target, []byte("state"), 0o600); err != nil {
					t.Fatal(err)
				}
				alias := filepath.Join(t.TempDir(), "alias.sqlite3")
				if err := os.Symlink(target, alias); err != nil {
					t.Skipf("creating a state symlink is unavailable: %v", err)
				}
				return alias
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			configPath := filepath.Join(dir, "config.json")
			tokenPath := filepath.Join(dir, "broker.token")
			var stdout bytes.Buffer
			var stderr bytes.Buffer
			code := Run([]string{
				"setup", "broker",
				"--config", configPath,
				"--state", test.state(t),
				"--token-file", tokenPath,
			}, &stdout, &stderr)
			if code == 0 {
				t.Fatalf("setup accepted unusable state path; stderr = %q", stderr.String())
			}
			for _, path := range []string{configPath, tokenPath} {
				if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("setup created %s after state preflight failure: %v", path, err)
				}
			}
		})
	}
}

func TestSetupPeerWithoutAuthentication(t *testing.T) {
	configPath := privateTestPath(t, "peer.json")
	// Keep the managed roots outside the protected config hierarchy so this
	// exercises their independent platform security setup.
	codexHome := filepath.Join(t.TempDir(), "worker-codex")
	workspaceRoot := filepath.Join(t.TempDir(), "worker-workspaces")
	statePath := filepath.Join(filepath.Dir(configPath), "state", "peer.sqlite3")
	codexBinary := testCodexBinary(t)
	gitBinary, err := resolveGitExecutable("git")
	if err != nil {
		t.Fatal(err)
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	args := []string{
		"setup", "peer",
		"--config", configPath,
		"--controller-id", "123e4567-e89b-42d3-a456-426614174000",
		"--device-id", "123e4567-e89b-42d3-a456-426614174001",
		"--device-name", "windows-builder",
		"--broker-url", "wss://broker.example.test",
		"--auth-mode", "none",
		"--codex-binary", codexBinary,
		"--codex-home", codexHome,
		"--workspace-root", workspaceRoot,
		"--state", statePath,
	}

	code := Run(args, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("setup code = %d, want 0; stderr = %q", code, stderr.String())
	}
	cfg, err := delegationconfig.Read(configPath)
	if err != nil {
		t.Fatal(err)
	}
	want := delegationconfig.Config{
		SchemaVersion: delegationconfig.CurrentSchemaVersion,
		InstanceID:    delegationconfig.DefaultInstanceID,
		HostKind:      hostkind.Codex,
		Role:          delegationconfig.RolePeer,
		ControllerID:  "123e4567-e89b-42d3-a456-426614174000",
		DeviceID:      "123e4567-e89b-42d3-a456-426614174001",
		DeviceName:    "windows-builder",
		Broker: delegationconfig.BrokerConfig{
			URL:  "wss://broker.example.test",
			Auth: delegationconfig.AuthConfig{Mode: delegationconfig.AuthModeNone},
		},
		Peer: delegationconfig.PeerConfig{
			CLI: &delegationconfig.CLIConfig{
				Command: codexBinary,
			},
			GitBinary:      gitBinary,
			CodexHome:      codexHome,
			WorkspaceRoot:  workspaceRoot,
			StateFile:      statePath,
			MaxWorkerSlots: 4,
		},
	}
	if !reflect.DeepEqual(cfg, want) {
		t.Fatalf("config = %#v, want %#v", cfg, want)
	}
	if _, err := os.Stat(filepath.Join(codexHome, "config.toml")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("setup created a managed Codex config file: %v", err)
	}
	for description, path := range map[string]string{
		"managed CODEX_HOME": codexHome,
		"workspace root":     workspaceRoot,
	} {
		if err := delegationconfig.PreparePrivateDirectory(path); err != nil {
			t.Fatalf("%s is not protected independently: %v", description, err)
		}
	}
}

func TestSetupNamedPeerUsesIsolatedDefaults(t *testing.T) {
	home := privateTestDirectory(t)
	t.Setenv("DELEGATION_HOME", home)
	t.Setenv("DELEGATION_CONFIG", "")
	codexBinary := testCodexBinary(t)
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run([]string{
		"setup", "peer",
		"--instance", "second",
		"--controller-id", "123e4567-e89b-42d3-a456-426614174000",
		"--device-id", "123e4567-e89b-42d3-a456-426614174001",
		"--device-name", "second-peer",
		"--broker-url", "wss://broker.example.test",
		"--auth-mode", "none",
		"--codex-binary", codexBinary,
		"--json",
	}, &stdout, &stderr)

	if code != 0 {
		t.Fatalf("setup code = %d, stderr = %q", code, stderr.String())
	}
	var result setupResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	instanceHome := filepath.Join(home, "instances", "second")
	if result.ConfigPath != filepath.Join(instanceHome, "peer.json") ||
		result.StatePath != filepath.Join(instanceHome, "state", "peer.sqlite3") ||
		result.CodexHome != filepath.Join(instanceHome, "codex") ||
		result.WorkspaceRoot != filepath.Join(instanceHome, "workspaces") {
		t.Fatalf("named peer result = %#v", result)
	}
}

func TestSetupPeerRejectsMissingCodexBinary(t *testing.T) {
	configPath := privateTestPath(t, "peer.json")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := Run([]string{
		"setup", "peer",
		"--config", configPath,
		"--controller-id", "123e4567-e89b-42d3-a456-426614174000",
		"--device-id", "123e4567-e89b-42d3-a456-426614174001",
		"--device-name", "missing-codex",
		"--broker-url", "wss://broker.example.test",
		"--auth-mode", "none",
		"--codex-binary", filepath.Join(t.TempDir(), "missing-codex"),
	}, &stdout, &stderr)
	if code == 0 || !strings.Contains(stderr.String(), "resolve Codex executable") {
		t.Fatalf("setup code = %d, stderr = %q", code, stderr.String())
	}
	if _, err := os.Lstat(configPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("setup wrote config for a missing Codex binary: %v", err)
	}
}

func TestSetupPeerRejectsPrepopulatedManagedCodexHome(t *testing.T) {
	configPath := privateTestPath(t, "peer.json")
	codexHome := filepath.Join(t.TempDir(), "managed-codex")
	if err := os.Mkdir(codexHome, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(codexHome, "auth.json"), []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := Run([]string{
		"setup", "peer",
		"--config", configPath,
		"--controller-id", "123e4567-e89b-42d3-a456-426614174000",
		"--device-id", "123e4567-e89b-42d3-a456-426614174001",
		"--device-name", "prepopulated-home",
		"--broker-url", "wss://broker.example.test",
		"--auth-mode", "none",
		"--codex-binary", testCodexBinary(t),
		"--codex-home", codexHome,
	}, &stdout, &stderr)
	if code == 0 || !strings.Contains(stderr.String(), "auth.json") {
		t.Fatalf("setup code = %d, stderr = %q", code, stderr.String())
	}
	if _, err := os.Lstat(configPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("setup wrote config for prepopulated managed home: %v", err)
	}
}

func TestSetupPeerRollsBackNewManagedHomeWhenWorkspacePreparationFails(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "config", "peer.json")
	codexHome := filepath.Join(root, "worker-codex")
	blockedParent := filepath.Join(root, "blocked")
	if err := os.WriteFile(blockedParent, []byte("not a directory"), 0o600); err != nil {
		t.Fatal(err)
	}
	workspaceRoot := filepath.Join(blockedParent, "worker-workspaces")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := Run([]string{
		"setup", "peer",
		"--config", configPath,
		"--controller-id", "123e4567-e89b-42d3-a456-426614174000",
		"--device-id", "123e4567-e89b-42d3-a456-426614174001",
		"--device-name", "rollback-test",
		"--broker-url", "wss://broker.example.test",
		"--auth-mode", "none",
		"--codex-binary", testCodexBinary(t),
		"--codex-home", codexHome,
		"--workspace-root", workspaceRoot,
	}, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("setup accepted an unusable workspace root; stderr = %q", stderr.String())
	}
	for _, path := range []string{configPath, codexHome} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("setup left %s after rollback: %v", path, err)
		}
	}
}

func TestSetupPeerRejectsManagedDirectoryAndStateCollisions(t *testing.T) {
	codexBinary := testCodexBinary(t)
	for _, test := range []struct {
		name                    string
		state, codex, workspace string
	}{
		{name: "state is CODEX_HOME", state: "collision", codex: "collision", workspace: "workspaces"},
		{name: "state is workspace", state: "collision", codex: "codex", workspace: "collision"},
		{name: "managed directories match", state: "state/peer.sqlite3", codex: "collision", workspace: "collision"},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			configPath := filepath.Join(root, "peer.json")
			var stdout bytes.Buffer
			var stderr bytes.Buffer
			code := Run([]string{
				"setup", "peer",
				"--config", configPath,
				"--controller-id", "123e4567-e89b-42d3-a456-426614174000",
				"--device-id", "123e4567-e89b-42d3-a456-426614174001",
				"--device-name", "collision-test",
				"--broker-url", "wss://broker.example.test",
				"--auth-mode", "none",
				"--codex-binary", codexBinary,
				"--state", filepath.Join(root, test.state),
				"--codex-home", filepath.Join(root, test.codex),
				"--workspace-root", filepath.Join(root, test.workspace),
			}, &stdout, &stderr)
			if code == 0 {
				t.Fatalf("setup accepted colliding paths; stderr = %q", stderr.String())
			}
			if _, err := os.Lstat(configPath); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("setup wrote config after collision: %v", err)
			}
		})
	}
}

func TestSetupPeerNoneAuthExplainsTrustDomain(t *testing.T) {
	configPath := privateTestPath(t, "peer.json")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := Run([]string{
		"setup", "peer",
		"--config", configPath,
		"--controller-id", "123e4567-e89b-42d3-a456-426614174000",
		"--device-id", "123e4567-e89b-42d3-a456-426614174001",
		"--device-name", "peer",
		"--broker-url", "wss://broker.example.test",
		"--auth-mode", "none",
		"--codex-binary", testCodexBinary(t),
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("setup code = %d, stderr = %q", code, stderr.String())
	}
	for _, text := range []string{"join", "enumerate", "dispatch", "impersonate", "fence", "same deviceId", "entire tailnet"} {
		if !strings.Contains(stderr.String(), text) {
			t.Fatalf("none-auth warning = %q, want %q", stderr.String(), text)
		}
	}
	if strings.Contains(stderr.String(), "plaintext non-loopback") {
		t.Fatalf("WSS peer emitted a plaintext warning: %q", stderr.String())
	}
}

func TestSetupPeerWithoutAuthenticationGeneratesDeviceID(t *testing.T) {
	configPath := privateTestPath(t, "peer.json")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := Run([]string{
		"setup", "peer",
		"--config", configPath,
		"--controller-id", "123e4567-e89b-42d3-a456-426614174000",
		"--device-name", "local-worker",
		"--broker-url", "wss://broker.example.test",
		"--auth-mode", "none",
		"--codex-binary", testCodexBinary(t),
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("setup code = %d, want 0; stderr = %q", code, stderr.String())
	}
	cfg, err := delegationconfig.Read(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := identity.ValidateID(cfg.DeviceID); err != nil {
		t.Fatalf("generated deviceId = %q: %v", cfg.DeviceID, err)
	}
}

func TestSetupTokenAuthenticationRequiresDeviceID(t *testing.T) {
	for _, role := range []string{"peer"} {
		t.Run(role, func(t *testing.T) {
			dir := privateTestDirectory(t)
			configPath := filepath.Join(dir, role+".json")
			tokenPath := filepath.Join(dir, role+".token")
			if _, err := tokenfile.Ensure(tokenPath); err != nil {
				t.Fatal(err)
			}
			var stdout bytes.Buffer
			var stderr bytes.Buffer
			code := Run([]string{
				"setup", role,
				"--config", configPath,
				"--controller-id", "123e4567-e89b-42d3-a456-426614174000",
				"--device-name", "managed-device",
				"--broker-url", "wss://broker.example.test",
				"--token-file", tokenPath,
				"--codex-binary", testCodexBinary(t),
			}, &stdout, &stderr)
			if code == 0 || !strings.Contains(stderr.String(), "--device-id is required in token mode") {
				t.Fatalf("setup code = %d, stderr = %q", code, stderr.String())
			}
			if _, err := os.Stat(configPath); !os.IsNotExist(err) {
				t.Fatalf("config was created without a bound deviceId: %v", err)
			}
		})
	}
}

func TestSetupPeerWithTokenAuthentication(t *testing.T) {
	dir := privateTestDirectory(t)
	configPath := filepath.Join(dir, "peer.json")
	tokenPath := filepath.Join(dir, "peer.token")
	if _, err := tokenfile.Ensure(tokenPath); err != nil {
		t.Fatal(err)
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	args := []string{
		"setup", "peer",
		"--config", configPath,
		"--controller-id", "123e4567-e89b-42d3-a456-426614174000",
		"--device-id", "123e4567-e89b-42d3-a456-426614174001",
		"--device-name", "macos-builder",
		"--broker-url", "wss://broker.example.test",
		"--token-file", tokenPath,
		"--codex-binary", testCodexBinary(t),
	}

	if code := Run(args, &stdout, &stderr); code != 0 {
		t.Fatalf("setup code = %d, want 0; stderr = %q", code, stderr.String())
	}
	cfg, err := delegationconfig.Read(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Role != delegationconfig.RolePeer || cfg.Broker.Auth.TokenFile != tokenPath {
		t.Fatalf("peer config = %#v", cfg)
	}
}

func TestSetupClientRequiresAcknowledgementForRemotePlaintext(t *testing.T) {
	for _, role := range []string{"peer"} {
		for _, authMode := range []string{"none", "token"} {
			t.Run(role+"/"+authMode, func(t *testing.T) {
				dir := privateTestDirectory(t)
				configPath := filepath.Join(dir, role+".json")
				args := []string{
					"setup", role,
					"--config", configPath,
					"--controller-id", "123e4567-e89b-42d3-a456-426614174000",
					"--device-id", "123e4567-e89b-42d3-a456-426614174001",
					"--device-name", "managed-device",
					"--broker-url", "ws://broker.example.test:8787",
					"--auth-mode", authMode,
					"--codex-binary", testCodexBinary(t),
				}
				if authMode == "token" {
					tokenPath := filepath.Join(dir, role+".token")
					if _, err := tokenfile.Ensure(tokenPath); err != nil {
						t.Fatal(err)
					}
					args = append(args, "--token-file", tokenPath)
				}

				var stdout bytes.Buffer
				var stderr bytes.Buffer
				if code := Run(args, &stdout, &stderr); code == 0 {
					t.Fatal("setup accepted remote plaintext transport without acknowledgement")
				}
				if !strings.Contains(stderr.String(), "requires explicit acknowledgement") {
					t.Fatalf("stderr = %q", stderr.String())
				}
				if _, err := os.Stat(configPath); !os.IsNotExist(err) {
					t.Fatalf("config was created after failed setup: %v", err)
				}

				stdout.Reset()
				stderr.Reset()
				args = append(args, "--allow-insecure-nonloopback")
				if code := Run(args, &stdout, &stderr); code != 0 {
					t.Fatalf("setup code = %d, stderr = %q", code, stderr.String())
				}
				for _, text := range []string{"warning", "ws://broker.example.test:8787", "plaintext non-loopback", "Tailscale"} {
					if !strings.Contains(stderr.String(), text) {
						t.Fatalf("stderr = %q, want %q", stderr.String(), text)
					}
				}
				cfg, err := delegationconfig.Read(configPath)
				if err != nil {
					t.Fatal(err)
				}
				if cfg.Role != delegationconfig.Role(role) || !cfg.Broker.AllowInsecureNonLoopback {
					t.Fatalf("config = %#v", cfg)
				}
			})
		}
	}
}

func TestSetupClientWarningFailureDoesNotCreateConfig(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "device.json")
	var stdout bytes.Buffer
	code := Run([]string{
		"setup", "peer",
		"--config", configPath,
		"--controller-id", "123e4567-e89b-42d3-a456-426614174000",
		"--device-id", "123e4567-e89b-42d3-a456-426614174001",
		"--device-name", "managed-device",
		"--broker-url", "ws://broker.example.test:8787",
		"--auth-mode", "none",
		"--allow-insecure-nonloopback",
		"--codex-binary", testCodexBinary(t),
	}, &stdout, setupFailingWriter{})

	if code == 0 {
		t.Fatal("setup ignored a security warning output failure")
	}
	if _, err := os.Stat(configPath); !os.IsNotExist(err) {
		t.Fatalf("config was created without a delivered security warning: %v", err)
	}
}

func TestSetupBrokerRejectsUnacknowledgedNonLoopback(t *testing.T) {
	for _, authMode := range []string{"none", "token"} {
		t.Run(authMode, func(t *testing.T) {
			dir := t.TempDir()
			configPath := filepath.Join(dir, "config.json")
			var stdout bytes.Buffer
			var stderr bytes.Buffer

			code := Run([]string{
				"setup", "broker",
				"--config", configPath,
				"--listen", "0.0.0.0:8787",
				"--auth-mode", authMode,
			}, &stdout, &stderr)

			if code == 0 {
				t.Fatal("setup accepted unacknowledged non-loopback listener")
			}
			if !strings.Contains(stderr.String(), "requires explicit acknowledgement") {
				t.Fatalf("stderr = %q", stderr.String())
			}
			for _, path := range []string{configPath, filepath.Join(dir, "secrets", "broker.token")} {
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Fatalf("setup created %s after failed setup: %v", path, err)
				}
			}
		})
	}
}

func TestSetupBrokerWarnsForAcknowledgedNonLoopback(t *testing.T) {
	for _, authMode := range []string{"none", "token"} {
		for _, listen := range []string{"0.0.0.0:8787", "[::]:8787", "broker.example.test:8787"} {
			t.Run(authMode+"/"+listen, func(t *testing.T) {
				configPath := privateTestPath(t, "config.json")
				var stdout bytes.Buffer
				var stderr bytes.Buffer

				code := Run([]string{
					"setup", "broker",
					"--config", configPath,
					"--listen", listen,
					"--auth-mode", authMode,
					"--allow-insecure-nonloopback",
					"--json",
				}, &stdout, &stderr)

				if code != 0 {
					t.Fatalf("setup code = %d, want 0; stderr = %q", code, stderr.String())
				}
				for _, text := range []string{"warning", listen, "plaintext non-loopback", "Tailscale"} {
					if !strings.Contains(stderr.String(), text) {
						t.Fatalf("stderr = %q, want %q", stderr.String(), text)
					}
				}
				var result setupResult
				if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
					t.Fatal(err)
				}
				if result.Role != delegationconfig.RoleBroker || result.ConfigPath != configPath {
					t.Fatalf("setup result = %#v", result)
				}
				cfg, err := delegationconfig.Read(configPath)
				if err != nil {
					t.Fatal(err)
				}
				if !cfg.Broker.AllowInsecureNonLoopback || cfg.Broker.Auth.Mode != delegationconfig.AuthMode(authMode) {
					t.Fatalf("broker config = %#v", cfg.Broker)
				}
			})
		}
	}
}

func TestSetupBrokerLoopbackWarningDependsOnAuthentication(t *testing.T) {
	tests := []struct {
		name        string
		listen      string
		authMode    string
		allow       bool
		wantWarning bool
	}{
		{name: "IPv4 loopback none", listen: "127.0.0.1:8787", authMode: "none", allow: true, wantWarning: true},
		{name: "IPv6 loopback none", listen: "[::1]:8787", authMode: "none", allow: true, wantWarning: true},
		{name: "localhost none", listen: "LOCALHOST:8787", authMode: "none", allow: true, wantWarning: true},
		{name: "token IPv4 loopback", listen: "127.0.0.1:8787", authMode: "token", allow: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			configPath := privateTestPath(t, "config.json")
			var stdout bytes.Buffer
			var stderr bytes.Buffer
			args := []string{
				"setup", "broker",
				"--config", configPath,
				"--listen", test.listen,
				"--auth-mode", test.authMode,
			}
			if test.allow {
				args = append(args, "--allow-insecure-nonloopback")
			}

			code := Run(args, &stdout, &stderr)

			if code != 0 {
				t.Fatalf("setup code = %d, want 0; stderr = %q", code, stderr.String())
			}
			if test.wantWarning != strings.Contains(stderr.String(), "authentication is disabled") {
				t.Fatalf("authentication warning = %q, want %v", stderr.String(), test.wantWarning)
			}
			if strings.Contains(stderr.String(), "plaintext non-loopback") {
				t.Fatalf("loopback setup emitted transport warning: %q", stderr.String())
			}
		})
	}
}

func TestSetupBrokerWarningFailureDoesNotCreateConfig(t *testing.T) {
	dir := privateTestDirectory(t)
	configPath := filepath.Join(dir, "config.json")
	var stdout bytes.Buffer

	code := Run([]string{
		"setup", "broker",
		"--config", configPath,
		"--listen", "0.0.0.0:8787",
		"--allow-insecure-nonloopback",
	}, &stdout, setupFailingWriter{})

	if code == 0 {
		t.Fatal("setup ignored a security warning output failure")
	}
	if _, err := os.Stat(configPath); !os.IsNotExist(err) {
		t.Fatalf("config was created without a delivered security warning: %v", err)
	}
	tokenPath := filepath.Join(dir, "secrets", "broker.token")
	if _, err := os.Stat(tokenPath); !os.IsNotExist(err) {
		t.Fatalf("token was created without a delivered security warning: %v", err)
	}
}

func TestSetupBrokerValidatesBeforeCreatingToken(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.json")
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run([]string{
		"setup", "broker",
		"--config", configPath,
		"--listen", "invalid-listener",
	}, &stdout, &stderr)

	if code == 0 {
		t.Fatal("setup accepted invalid broker configuration")
	}
	tokenPath := filepath.Join(dir, "secrets", "broker.token")
	if _, err := os.Stat(tokenPath); !os.IsNotExist(err) {
		t.Fatalf("token was created after failed validation: %v", err)
	}
}

func TestSetupBrokerChecksConfigBeforeCreatingToken(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.json")
	if err := os.WriteFile(configPath, []byte("existing"), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run([]string{"setup", "broker", "--config", configPath}, &stdout, &stderr)

	if code == 0 {
		t.Fatal("setup overwrote an existing config")
	}
	tokenPath := filepath.Join(dir, "secrets", "broker.token")
	if _, err := os.Stat(tokenPath); !os.IsNotExist(err) {
		t.Fatalf("token was created when config already existed: %v", err)
	}
}

func TestSetupBrokerChecksConfigAuthorityBeforeCreatingToken(t *testing.T) {
	dir := unsafeTestDirectory(t)
	configPath := filepath.Join(dir, "config.json")
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run([]string{"setup", "broker", "--config", configPath}, &stdout, &stderr)

	if code == 0 {
		t.Fatal("setup accepted an unsafe config authority")
	}
	tokenPath := filepath.Join(dir, "secrets", "broker.token")
	if _, err := os.Stat(tokenPath); !os.IsNotExist(err) {
		t.Fatalf("token was created before config authority validation: %v", err)
	}
}

func TestSetupBrokerRejectsConfigTokenPathCollision(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shared")
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run([]string{
		"setup", "broker",
		"--config", path,
		"--token-file", path,
	}, &stdout, &stderr)

	if code == 0 {
		t.Fatal("setup accepted the same config and token path")
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatalf("setup created the colliding path: %v", err)
	}
}

func TestSetupBrokerRejectsConfigTokenParentAlias(t *testing.T) {
	realDir := t.TempDir()
	aliasDir := filepath.Join(t.TempDir(), "alias")
	if err := os.Symlink(realDir, aliasDir); err != nil {
		t.Skipf("creating directory symlink is unavailable: %v", err)
	}
	configPath := filepath.Join(realDir, "shared")
	tokenPath := filepath.Join(aliasDir, "shared")
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run([]string{
		"setup", "broker",
		"--config", configPath,
		"--token-file", tokenPath,
	}, &stdout, &stderr)

	if code == 0 {
		t.Fatal("setup accepted aliased config and token paths")
	}
	if _, err := os.Lstat(configPath); !os.IsNotExist(err) {
		t.Fatalf("setup created the aliased path: %v", err)
	}
}

func TestSetupBrokerRejectsDanglingConfigTokenParentAlias(t *testing.T) {
	root := t.TempDir()
	targetDir := filepath.Join(root, "future-target")
	aliasDir := filepath.Join(root, "alias")
	if err := os.Symlink(targetDir, aliasDir); err != nil {
		t.Skipf("creating directory symlink is unavailable: %v", err)
	}
	configPath := filepath.Join(aliasDir, "shared")
	tokenPath := filepath.Join(targetDir, "shared")
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run([]string{
		"setup", "broker",
		"--config", configPath,
		"--token-file", tokenPath,
	}, &stdout, &stderr)

	if code == 0 {
		t.Fatal("setup accepted a dangling parent alias collision")
	}
	if _, err := os.Lstat(tokenPath); !os.IsNotExist(err) {
		t.Fatalf("setup created the dangling aliased path: %v", err)
	}
}

func TestConcurrentBrokerSetupKeepsWinningToken(t *testing.T) {
	dir := privateTestDirectory(t)
	configPath := filepath.Join(dir, "config.json")
	args := []string{"setup", "broker", "--config", configPath, "--json"}
	type outcome struct {
		code int
	}
	outcomes := make(chan outcome, 2)
	var start sync.WaitGroup
	start.Add(1)
	for range 2 {
		go func() {
			start.Wait()
			var stdout bytes.Buffer
			var stderr bytes.Buffer
			outcomes <- outcome{code: Run(args, &stdout, &stderr)}
		}()
	}
	start.Done()

	successes := 0
	for range 2 {
		result := <-outcomes
		if result.code == 0 {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("successful setup calls = %d, want 1", successes)
	}
	cfg, err := delegationconfig.Read(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := tokenfile.Validate(cfg.Broker.Auth.TokenFile); err != nil {
		t.Fatalf("winning config token is missing or invalid: %v", err)
	}
}

type setupFailingWriter struct{}

func (setupFailingWriter) Write([]byte) (int, error) {
	return 0, errors.New("closed warning output")
}
