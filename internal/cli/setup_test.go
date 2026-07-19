package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/identity"
	"github.com/GhostFlying/delegation/internal/tokenfile"
)

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
		result.StatePath != wantState || result.TokenFile == "" {
		t.Fatalf("setup result = %#v", result)
	}
	cfg, err := delegationconfig.Read(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Role != delegationconfig.RoleBroker || cfg.ControllerID != result.ControllerID ||
		cfg.Broker.StateFile != result.StatePath || cfg.Broker.Auth.TokenFile != result.TokenFile {
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
		Role:          delegationconfig.RolePeer,
		ControllerID:  "123e4567-e89b-42d3-a456-426614174000",
		DeviceID:      "123e4567-e89b-42d3-a456-426614174001",
		DeviceName:    "windows-builder",
		Broker: delegationconfig.BrokerConfig{
			URL:  "wss://broker.example.test",
			Auth: delegationconfig.AuthConfig{Mode: delegationconfig.AuthModeNone},
		},
	}
	if cfg != want {
		t.Fatalf("config = %#v, want %#v", cfg, want)
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

func TestLegacySetupRolesReturnMigrationGuidanceWithoutWriting(t *testing.T) {
	for _, role := range []string{"controller", "device"} {
		t.Run(role, func(t *testing.T) {
			configPath := privateTestPath(t, role+".json")
			var stdout bytes.Buffer
			var stderr bytes.Buffer
			code := Run([]string{"setup", role, "--config", configPath}, &stdout, &stderr)
			if code == 0 || !strings.Contains(stderr.String(), "migrate config") {
				t.Fatalf("legacy setup code = %d, stderr = %q", code, stderr.String())
			}
			if role == "device" && !strings.Contains(stderr.String(), "fresh peer credential") {
				t.Fatalf("device migration guidance = %q", stderr.String())
			}
			if _, err := os.Stat(configPath); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("legacy setup wrote config: %v", err)
			}
		})
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
