//go:build linux

package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/GhostFlying/delegation/internal/broker"
	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/userservice"
)

const serviceTestControllerID = "123e4567-e89b-42d3-a456-426614174720"

func TestServiceInstallActivatesSystemdUnit(t *testing.T) {
	root := t.TempDir()
	installNoopSystemctl(t, root)
	listen := startTestBrokerReadiness(t)
	configPath := filepath.Join(root, "config.json")
	configHome := filepath.Join(root, "xdg")
	t.Setenv("XDG_CONFIG_HOME", configHome)
	var setupOutput bytes.Buffer
	var setupError bytes.Buffer
	if code := Run([]string{
		"setup", "broker", "--config", configPath,
		"--controller-id", serviceTestControllerID, "--listen", listen,
	}, &setupOutput, &setupError); code != 0 {
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
	if result.State != userservice.StateActive || result.Kind != userservice.KindSystemd ||
		result.Artifact != wantArtifact || result.ConfigPath != configPath {
		t.Fatalf("service install result = %#v", result)
	}
	if _, err := os.Stat(wantArtifact); err != nil {
		t.Fatalf("prepared unit is missing: %v", err)
	}
}

func TestServiceInstallActivatesSystemdUnitByDefault(t *testing.T) {
	root := t.TempDir()
	listen := startTestBrokerReadiness(t)
	configPath := filepath.Join(root, "config.json")
	configHome := filepath.Join(root, "xdg")
	commandLog := filepath.Join(root, "systemctl.log")
	bin := filepath.Join(root, "bin")
	if err := os.Mkdir(bin, 0o700); err != nil {
		t.Fatal(err)
	}
	systemctl := filepath.Join(bin, "systemctl")
	if err := os.WriteFile(systemctl, []byte(`#!/bin/sh
printf '%s\n' "$*" >>"$DELEGATION_TEST_SYSTEMCTL_LOG"
case " $* " in
  *" show "*)
    printf 'FragmentPath=%s/systemd/user/delegation.service\nDropInPaths=\n' "$XDG_CONFIG_HOME"
    ;;
esac
`), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("XDG_CONFIG_HOME", configHome)
	t.Setenv("DELEGATION_TEST_SYSTEMCTL_LOG", commandLog)
	var setupOutput bytes.Buffer
	var setupError bytes.Buffer
	if code := Run([]string{
		"setup", "broker", "--config", configPath,
		"--controller-id", serviceTestControllerID, "--listen", listen,
	}, &setupOutput, &setupError); code != 0 {
		t.Fatalf("setup code = %d, stderr = %q", code, setupError.String())
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := Run([]string{"service", "install", "--config", configPath, "--json"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("service install code = %d, stderr = %q", code, stderr.String())
	}
	var result serviceInstallResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.State != userservice.StateActive {
		t.Fatalf("service install result = %#v", result)
	}
	log, err := os.ReadFile(commandLog)
	if err != nil {
		t.Fatal(err)
	}
	want := strings.Join([]string{
		"--user --no-ask-password daemon-reload",
		"--user --no-ask-password show " + userservice.SystemdUnitName + " --property=FragmentPath --property=DropInPaths",
		"--user --no-ask-password enable --now " + userservice.SystemdUnitName,
		"--user --no-ask-password is-enabled --quiet " + userservice.SystemdUnitName,
		"--user --no-ask-password is-active --quiet " + userservice.SystemdUnitName,
		"--user --no-ask-password show " + userservice.SystemdUnitName + " --property=FragmentPath --property=DropInPaths",
		"",
	}, "\n")
	if string(log) != want {
		t.Fatalf("systemctl log = %q, want %q", log, want)
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
	installNoopSystemctl(t, root)
	listen := startTestBrokerReadiness(t)
	configHome := filepath.Join(root, "xdg")
	t.Setenv("XDG_CONFIG_HOME", configHome)
	firstConfig := filepath.Join(root, "first.json")
	secondConfig := filepath.Join(root, "second.json")
	for _, configPath := range []string{firstConfig, secondConfig} {
		var setupOutput bytes.Buffer
		var setupError bytes.Buffer
		if code := Run([]string{
			"setup", "broker", "--config", configPath,
			"--controller-id", serviceTestControllerID, "--listen", listen,
		}, &setupOutput, &setupError); code != 0 {
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
	installNoopSystemctl(t, root)
	listen := startTestBrokerReadiness(t)
	configPath := filepath.Join(root, "config.json")
	configHome := filepath.Join(root, "xdg")
	t.Setenv("XDG_CONFIG_HOME", configHome)
	var setupOutput bytes.Buffer
	var setupError bytes.Buffer
	if code := Run([]string{
		"setup", "broker", "--config", configPath,
		"--controller-id", serviceTestControllerID, "--listen", listen,
	}, &setupOutput, &setupError); code != 0 {
		t.Fatalf("setup code = %d, want 0; stderr = %q", code, setupError.String())
	}
	var stderr bytes.Buffer

	code := Run([]string{"service", "install", "--config", configPath}, failingWriter{}, &stderr)

	if code == 0 {
		t.Fatal("service install ignored an output failure")
	}
	artifact := filepath.Join(configHome, "systemd", "user", userservice.SystemdUnitName)
	for _, expected := range []string{"state active", artifact, configPath, "write service installation"} {
		if !bytes.Contains(stderr.Bytes(), []byte(expected)) {
			t.Fatalf("service install stderr = %q, want %q", stderr.String(), expected)
		}
	}
}

type failingWriter struct{}

func installNoopSystemctl(t *testing.T, root string) {
	t.Helper()
	bin := filepath.Join(root, "fake-systemctl")
	if err := os.Mkdir(bin, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(bin, "systemctl")
	if err := os.WriteFile(path, []byte(`#!/bin/sh
case " $* " in
  *" show "*)
    printf 'FragmentPath=%s/systemd/user/delegation.service\nDropInPaths=\n' "$XDG_CONFIG_HOME"
    ;;
esac
exit 0
`), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
}

func startTestBrokerReadiness(t *testing.T) string {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set(broker.HealthServiceHeader, "broker")
		writer.Header().Set(broker.HealthControllerHeader, serviceTestControllerID)
		_, _ = writer.Write([]byte("ok\n"))
	}))
	t.Cleanup(server.Close)
	return strings.TrimPrefix(server.URL, "http://")
}

func (failingWriter) Write([]byte) (int, error) {
	return 0, errors.New("closed output")
}
