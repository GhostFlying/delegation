package cli

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/tokenfile"
)

func TestSetupBroker(t *testing.T) {
	dir := t.TempDir()
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
	if result.Role != delegationconfig.RoleBroker || result.ConfigPath != configPath || result.ControllerID == "" || result.TokenFile == "" {
		t.Fatalf("setup result = %#v", result)
	}
	cfg, err := delegationconfig.Read(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Role != delegationconfig.RoleBroker || cfg.ControllerID != result.ControllerID || cfg.Broker.Auth.TokenFile != result.TokenFile {
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

func TestSetupDeviceWithoutAuthentication(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "device.json")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	args := []string{
		"setup", "device",
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
		Role:          delegationconfig.RoleDevice,
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

func TestSetupControllerWithTokenAuthentication(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "controller.json")
	tokenPath := filepath.Join(dir, "controller.token")
	if _, err := tokenfile.Ensure(tokenPath); err != nil {
		t.Fatal(err)
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	args := []string{
		"setup", "controller",
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
	if cfg.Role != delegationconfig.RoleController || cfg.Broker.Auth.TokenFile != tokenPath {
		t.Fatalf("controller config = %#v", cfg)
	}
}

func TestSetupBrokerRejectsUnauthenticatedNonLoopback(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.json")
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run([]string{
		"setup", "broker",
		"--config", configPath,
		"--listen", "0.0.0.0:8787",
		"--auth-mode", "none",
	}, &stdout, &stderr)

	if code == 0 {
		t.Fatal("setup accepted unauthenticated non-loopback listener")
	}
	if !strings.Contains(stderr.String(), "requires explicit acknowledgement") {
		t.Fatalf("stderr = %q", stderr.String())
	}
	if _, err := os.Stat(configPath); !os.IsNotExist(err) {
		t.Fatalf("config was created after failed setup: %v", err)
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

func TestPathsEquivalentConservativelyFoldsCase(t *testing.T) {
	root := t.TempDir()
	equivalent, err := pathsEquivalent(filepath.Join(root, "Config"), filepath.Join(root, "config"))
	if err != nil {
		t.Fatal(err)
	}
	if !equivalent {
		t.Fatal("pathsEquivalent() did not conservatively fold case")
	}
}

func TestConcurrentBrokerSetupKeepsWinningToken(t *testing.T) {
	dir := t.TempDir()
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

func TestNewUUID(t *testing.T) {
	first, err := newUUID()
	if err != nil {
		t.Fatal(err)
	}
	second, err := newUUID()
	if err != nil {
		t.Fatal(err)
	}
	if first == second || len(first) != 36 || first[14] != '4' {
		t.Fatalf("generated UUIDs = %q, %q", first, second)
	}
}
