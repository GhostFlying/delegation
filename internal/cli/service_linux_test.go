//go:build linux

package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/userservice"
)

func TestServiceInstallPreparesInactiveSystemdUnit(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "config.json")
	configHome := filepath.Join(root, "xdg")
	t.Setenv("XDG_CONFIG_HOME", configHome)
	var setupOutput bytes.Buffer
	var setupError bytes.Buffer
	if code := Run([]string{"setup", "broker", "--config", configPath}, &setupOutput, &setupError); code != 0 {
		t.Fatalf("setup code = %d, want 0; stderr = %q", code, setupError.String())
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run([]string{"service", "install", "--config", configPath, "--json"}, &stdout, &stderr)

	if code != 0 {
		t.Fatalf("service install code = %d, want 0; stderr = %q", code, stderr.String())
	}
	var result serviceInstallResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	wantArtifact := filepath.Join(configHome, "systemd", "user", userservice.SystemdUnitName)
	if result.State != userservice.StatePrepared || result.Kind != userservice.KindSystemd ||
		result.Artifact != wantArtifact || result.ConfigPath != configPath {
		t.Fatalf("service install result = %#v", result)
	}
	if _, err := os.Stat(wantArtifact); err != nil {
		t.Fatalf("prepared unit is missing: %v", err)
	}
}

func TestServiceInstallValidatesBeforeWritingArtifact(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "invalid.json")
	configHome := filepath.Join(root, "xdg")
	t.Setenv("XDG_CONFIG_HOME", configHome)
	if err := os.WriteFile(configPath, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run([]string{"service", "install", "--config", configPath}, &stdout, &stderr)

	if code == 0 {
		t.Fatal("service install accepted invalid configuration")
	}
	artifact := filepath.Join(configHome, "systemd", "user", userservice.SystemdUnitName)
	if _, err := os.Lstat(artifact); !os.IsNotExist(err) {
		t.Fatalf("service artifact exists after failed validation: %v", err)
	}
}

func TestServiceInstallPreflightsBrokerAuthorityBeforeWritingArtifact(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "config.json")
	configHome := filepath.Join(root, "xdg")
	t.Setenv("XDG_CONFIG_HOME", configHome)
	var setupOutput bytes.Buffer
	var setupError bytes.Buffer
	if code := Run([]string{"setup", "broker", "--config", configPath}, &setupOutput, &setupError); code != 0 {
		t.Fatalf("setup code = %d, stderr = %q", code, setupError.String())
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
	if code := Run([]string{"service", "install", "--config", configPath}, &stdout, &stderr); code == 0 {
		t.Fatal("service install accepted an invalid broker authority")
	}
	artifact := filepath.Join(configHome, "systemd", "user", userservice.SystemdUnitName)
	if _, err := os.Lstat(artifact); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("service artifact exists after authority preflight failure: %v", err)
	}
}

func TestServiceInstallReportsForeignConflictAsJSON(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "config.json")
	configHome := filepath.Join(root, "xdg")
	t.Setenv("XDG_CONFIG_HOME", configHome)
	var setupOutput bytes.Buffer
	var setupError bytes.Buffer
	if code := Run([]string{"setup", "broker", "--config", configPath}, &setupOutput, &setupError); code != 0 {
		t.Fatalf("setup code = %d, want 0; stderr = %q", code, setupError.String())
	}
	artifact := filepath.Join(configHome, "systemd", "user", userservice.SystemdUnitName)
	if err := os.MkdirAll(filepath.Dir(artifact), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(artifact, []byte("foreign\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run([]string{"service", "install", "--config", configPath, "--json"}, &stdout, &stderr)

	if code == 0 {
		t.Fatal("service install replaced a foreign definition")
	}
	var result serviceInstallResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.State != userservice.StateForeignConflict || result.Kind != userservice.KindSystemd ||
		result.Artifact != artifact || result.ConfigPath != configPath {
		t.Fatalf("service install result = %#v", result)
	}
	if !bytes.Contains(stderr.Bytes(), []byte(artifact)) {
		t.Fatalf("service install stderr omits artifact: %q", stderr.String())
	}
}

func TestServiceInstallReportsManagedDrift(t *testing.T) {
	root := t.TempDir()
	configHome := filepath.Join(root, "xdg")
	t.Setenv("XDG_CONFIG_HOME", configHome)
	firstConfig := filepath.Join(root, "first.json")
	secondConfig := filepath.Join(root, "second.json")
	for _, configPath := range []string{firstConfig, secondConfig} {
		var setupOutput bytes.Buffer
		var setupError bytes.Buffer
		if code := Run([]string{"setup", "broker", "--config", configPath}, &setupOutput, &setupError); code != 0 {
			t.Fatalf("setup %s code = %d, want 0; stderr = %q", configPath, code, setupError.String())
		}
	}
	var firstOutput bytes.Buffer
	var firstError bytes.Buffer
	if code := Run([]string{"service", "install", "--config", firstConfig}, &firstOutput, &firstError); code != 0 {
		t.Fatalf("first service install code = %d, want 0; stderr = %q", code, firstError.String())
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run([]string{"service", "install", "--config", secondConfig}, &stdout, &stderr)

	if code == 0 {
		t.Fatal("service install replaced a managed definition with drift")
	}
	if !bytes.Contains(stdout.Bytes(), []byte("service state: prepared")) ||
		!bytes.Contains(stderr.Bytes(), []byte("remove it explicitly")) {
		t.Fatalf("service install output = %q; stderr = %q", stdout.String(), stderr.String())
	}
}

func TestServiceInstallReportsCommittedStateWhenOutputFails(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "config.json")
	configHome := filepath.Join(root, "xdg")
	t.Setenv("XDG_CONFIG_HOME", configHome)
	var setupOutput bytes.Buffer
	var setupError bytes.Buffer
	if code := Run([]string{"setup", "broker", "--config", configPath}, &setupOutput, &setupError); code != 0 {
		t.Fatalf("setup code = %d, want 0; stderr = %q", code, setupError.String())
	}
	var stderr bytes.Buffer

	code := Run([]string{"service", "install", "--config", configPath}, failingWriter{}, &stderr)

	if code == 0 {
		t.Fatal("service install ignored an output failure")
	}
	artifact := filepath.Join(configHome, "systemd", "user", userservice.SystemdUnitName)
	for _, expected := range []string{"state prepared", artifact, configPath, "write service installation"} {
		if !bytes.Contains(stderr.Bytes(), []byte(expected)) {
			t.Fatalf("service install stderr = %q, want %q", stderr.String(), expected)
		}
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) {
	return 0, errors.New("closed output")
}
