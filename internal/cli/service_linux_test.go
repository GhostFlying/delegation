//go:build linux

package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/broker"
	"github.com/GhostFlying/delegation/internal/clilaunch"
	"github.com/GhostFlying/delegation/internal/codexconfig"
	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/hostkind"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/runtimeconfig"
	"github.com/GhostFlying/delegation/internal/store"
	"github.com/GhostFlying/delegation/internal/userservice"
)

const serviceTestControllerID = "123e4567-e89b-42d3-a456-426614174720"

const serviceTestProviderConfig = `{"model":"mock-model","model_provider":"mock","model_providers.mock":{"name":"Mock provider","base_url":"https://gateway.example.test/v1","wire_api":"responses","requires_openai_auth":false,"env_key":"GATEWAY_KEY"}}`

func TestServiceRepairRequiresStoppedPeerLease(t *testing.T) {
	configPath, cfg := setupConnectorRuntimeTest(
		t, "123e4567-e89b-42d3-a456-426614174722", "repair-running",
		"wss://broker.example.test/v1/connect",
	)
	cfg.HostKind = hostkind.TraeX
	cfg.Peer.CLI = &delegationconfig.CLIConfig{
		Command: testCodexBinary(t), Arguments: []string{"--profile", "legacy"},
		Launcher: &clilaunch.Spec{Executable: testCodexBinary(t)},
	}
	cfg.Peer.CodexBinary = ""
	rewriteRepairConfig(t, configPath, cfg)
	environmentPath := filepath.Join(filepath.Dir(configPath), "peer.env")
	writePeerServiceEnvironment(t, environmentPath, strings.Join([]string{
		codexconfig.EnvironmentVariable + "=" + serviceTestProviderConfig,
		"GATEWAY_KEY=secret",
		"",
	}, "\n"))
	writeFileForServiceRepair(t, filepath.Join(cfg.Peer.CodexHome, "AGENTS.md"), []byte("legacy"))
	lease, err := store.AcquirePeerLease(cfg.Peer.StateFile)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	before, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	code := Run([]string{
		"service", "repair", "--config", configPath,
		"--environment-file", environmentPath,
	}, &stdout, &stderr)
	if code == 0 || !strings.Contains(stderr.String(), store.ErrPeerLeaseHeld.Error()) {
		t.Fatalf("service repair = %d, stdout %q, stderr %q", code, stdout.String(), stderr.String())
	}
	after, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, before) {
		t.Fatal("service repair changed config while the peer lease was held")
	}
	if _, err := os.Stat(filepath.Join(cfg.Peer.CodexHome, "AGENTS.md")); err != nil {
		t.Fatalf("service repair moved managed-home state while the peer lease was held: %v", err)
	}
}

func TestServiceRepairCLICommitsInjectedFreshThreadQualification(t *testing.T) {
	configPath, cfg, environmentPath, pollutedPath := serviceRepairCLIFixture(t, "success")
	var smokeCalls int
	var stdout, stderr bytes.Buffer
	code := runServiceRepairWithDependencies([]string{
		"--config", configPath, "--environment-file", environmentPath, "--json",
	}, &stdout, &stderr, serviceRepairDependencies{
		qualify: func(
			_ context.Context, gotConfig, gotEnvironment string, got delegationconfig.Config,
			quarantinePath string,
		) error {
			smokeCalls++
			if gotConfig != configPath || gotEnvironment != environmentPath ||
				got.Peer.StateFile != cfg.Peer.StateFile || quarantinePath == "" {
				t.Fatalf("qualification inputs = %q, %q, %#v, %q",
					gotConfig, gotEnvironment, got, quarantinePath)
			}
			return nil
		},
	})
	if code != 0 || stderr.Len() != 0 || smokeCalls != 1 {
		t.Fatalf("service repair = %d, stdout %q, stderr %q, smoke %d",
			code, stdout.String(), stderr.String(), smokeCalls)
	}
	var result serviceRepairResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.State != "repaired" || result.ProfilesRemoved != 1 ||
		result.QuarantinePath == "" || result.ManifestPath == "" {
		t.Fatalf("repair result = %#v", result)
	}
	if _, err := os.Stat(pollutedPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("polluted managed-home entry remains: %v", err)
	}
	repaired, err := runtimeconfig.Read(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if slices.Contains(repaired.Peer.CLI.Arguments, "--profile") {
		t.Fatalf("repaired arguments = %#v", repaired.Peer.CLI.Arguments)
	}
	state, err := store.OpenPeer(context.Background(), cfg.Peer.StateFile)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	readiness, err := state.WorkerReadiness(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if readiness.Epoch != 1 || readiness.State != protocol.WorkerReadinessPending {
		t.Fatalf("successful repair readiness = %#v", readiness)
	}
}

func TestServiceRepairCLIRollsBackInjectedQualificationFailure(t *testing.T) {
	configPath, _, environmentPath, pollutedPath := serviceRepairCLIFixture(t, "failure")
	original, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	smokeErr := errors.New("injected qualification failure")
	var stdout, stderr bytes.Buffer
	code := runServiceRepairWithDependencies([]string{
		"--config", configPath, "--environment-file", environmentPath,
	}, &stdout, &stderr, serviceRepairDependencies{
		qualify: func(context.Context, string, string, delegationconfig.Config, string) error {
			return smokeErr
		},
	})
	if code == 0 || !strings.Contains(stderr.String(), smokeErr.Error()) {
		t.Fatalf("service repair = %d, stdout %q, stderr %q", code, stdout.String(), stderr.String())
	}
	rolledBack, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(rolledBack, original) {
		t.Fatalf("rolled-back config = %q, want %q", rolledBack, original)
	}
	if got, err := os.ReadFile(pollutedPath); err != nil || string(got) != "legacy" {
		t.Fatalf("rolled-back managed-home entry = %q, %v", got, err)
	}
}

func serviceRepairCLIFixture(
	t *testing.T, suffix string,
) (string, delegationconfig.Config, string, string) {
	t.Helper()
	configPath, cfg := setupConnectorRuntimeTest(
		t, "123e4567-e89b-42d3-a456-426614174725", "repair-"+suffix,
		"wss://broker.example.test/v1/connect",
	)
	cfg.HostKind = hostkind.TraeX
	cfg.Peer.CLI = &delegationconfig.CLIConfig{
		Command: testCodexBinary(t), Arguments: []string{"--profile", "legacy", "serve"},
		Launcher: &clilaunch.Spec{Executable: testCodexBinary(t)},
	}
	cfg.Peer.CodexBinary = ""
	rewriteRepairConfig(t, configPath, cfg)
	environmentPath := filepath.Join(filepath.Dir(configPath), "peer.env")
	writePeerServiceEnvironment(t, environmentPath, strings.Join([]string{
		codexconfig.EnvironmentVariable + "=" + serviceTestProviderConfig,
		"GATEWAY_KEY=secret",
		"",
	}, "\n"))
	pollutedPath := filepath.Join(cfg.Peer.CodexHome, "AGENTS.md")
	writeFileForServiceRepair(t, pollutedPath, []byte("legacy"))
	return configPath, cfg, environmentPath, pollutedPath
}

func TestPersistRepairRollbackFailureInitializesAndPreservesEpoch(t *testing.T) {
	root := privateTestDirectory(t)
	configPath := filepath.Join(root, "peer.json")
	environmentPath := filepath.Join(root, "peer.env")
	statePath := filepath.Join(root, "state", "peer.sqlite3")
	writeFileForServiceRepair(t, configPath, []byte("config\n"))
	writeFileForServiceRepair(t, environmentPath, []byte("environment\n"))
	if err := persistRepairRollbackFailure(statePath, configPath, environmentPath); err != nil {
		t.Fatal(err)
	}
	state, err := store.OpenPeer(context.Background(), statePath)
	if err != nil {
		t.Fatal(err)
	}
	first, err := state.WorkerReadiness(context.Background())
	if err != nil {
		state.Close()
		t.Fatal(err)
	}
	if first.Epoch != 1 || first.State != protocol.WorkerReadinessInterventionRequired ||
		first.FailureCode != protocol.WorkerRepairRollbackFailed {
		state.Close()
		t.Fatalf("initial rollback readiness = %#v", first)
	}
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond)
	if err := persistRepairRollbackFailure(statePath, configPath, environmentPath); err != nil {
		t.Fatal(err)
	}
	state, err = store.OpenPeer(context.Background(), statePath)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	second, err := state.WorkerReadiness(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if second.Epoch != first.Epoch || second.UpdatedAt <= first.UpdatedAt ||
		second.FailureCode != protocol.WorkerRepairRollbackFailed {
		t.Fatalf("repeated rollback readiness = %#v, first %#v", second, first)
	}
}

func TestServiceRuntimePersistsProfileFailureWithoutResettingEpoch(t *testing.T) {
	configPath, cfg := setupConnectorRuntimeTest(
		t, "123e4567-e89b-42d3-a456-426614174723", "profile-startup",
		"wss://broker.example.test/v1/connect",
	)
	cfg.HostKind = hostkind.TraeX
	cfg.Peer.CLI = &delegationconfig.CLIConfig{
		Command: testCodexBinary(t), Arguments: []string{"--profile=legacy"},
		Launcher: &clilaunch.Spec{Executable: testCodexBinary(t)},
	}
	cfg.Peer.CodexBinary = ""
	rewriteRepairConfig(t, configPath, cfg)
	environmentPath := filepath.Join(filepath.Dir(configPath), "peer.env")
	writeFileForServiceRepair(t, environmentPath, []byte("provider=test\n"))

	for attempt := 0; attempt < 2; attempt++ {
		_, err := readServiceRuntimeConfig(configPath, environmentPath, "linux")
		if !errors.Is(err, delegationconfig.ErrPeerCLIProfileArgumentsUnsupported) {
			t.Fatalf("startup classification %d error = %v", attempt+1, err)
		}
		if !strings.Contains(err.Error(), "state=intervention_required") ||
			!strings.Contains(err.Error(), "failureCode=profile_arguments_unsupported") {
			t.Fatalf("startup classification %d log = %v", attempt+1, err)
		}
	}
	state, err := store.OpenPeer(context.Background(), cfg.Peer.StateFile)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	readiness, err := state.WorkerReadiness(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if readiness.Epoch != 1 || readiness.State != protocol.WorkerReadinessInterventionRequired ||
		readiness.FailureCode != protocol.WorkerProfileUnsupported {
		t.Fatalf("profile startup readiness = %#v", readiness)
	}
}

func TestServiceRuntimePersistsUnsupportedWindowsTraeXHost(t *testing.T) {
	configPath, cfg := setupConnectorRuntimeTest(
		t, "123e4567-e89b-42d3-a456-426614174724", "windows-traex",
		"wss://broker.example.test/v1/connect",
	)
	cfg.HostKind = hostkind.TraeX
	cfg.Peer.CLI = &delegationconfig.CLIConfig{
		Command:  testCodexBinary(t),
		Launcher: &clilaunch.Spec{Executable: testCodexBinary(t)},
	}
	cfg.Peer.CodexBinary = ""
	rewriteRepairConfig(t, configPath, cfg)

	_, err := readServiceRuntimeConfig(configPath, "", "windows")
	if err == nil || !strings.Contains(err.Error(), "unsupported on Windows") {
		t.Fatalf("Windows TraeX startup error = %v", err)
	}
	if !strings.Contains(err.Error(), "state=intervention_required") ||
		!strings.Contains(err.Error(), "failureCode=unsupported_host") {
		t.Fatalf("Windows TraeX startup log = %v", err)
	}
	state, err := store.OpenPeer(context.Background(), cfg.Peer.StateFile)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	readiness, err := state.WorkerReadiness(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if readiness.State != protocol.WorkerReadinessInterventionRequired ||
		readiness.FailureCode != protocol.WorkerHostUnsupported {
		t.Fatalf("unsupported host readiness = %#v", readiness)
	}
}

func rewriteRepairConfig(t *testing.T, path string, cfg delegationconfig.Config) {
	t.Helper()
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func writeFileForServiceRepair(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestPeerServiceInstallValidatesEnvironmentBeforeWritingArtifact(t *testing.T) {
	configPath, cfg := setupConnectorRuntimeTest(
		t,
		"123e4567-e89b-42d3-a456-426614174721",
		"service-environment",
		"wss://broker.example.test/v1/connect",
	)
	configHome := filepath.Join(t.TempDir(), "xdg")
	t.Setenv("XDG_CONFIG_HOME", configHome)
	artifact := filepath.Join(configHome, "systemd", "user", userservice.SystemdPeerUnitName)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := Run([]string{"service", "install", "--config", configPath}, &stdout, &stderr)
	if code == 0 || !strings.Contains(stderr.String(), "requires --environment-file") {
		t.Fatalf("missing environment install = %d, %q", code, stderr.String())
	}
	assertPathAbsent(t, artifact)

	secret := "must-not-appear-in-service-output"
	environmentPath := filepath.Join(filepath.Dir(configPath), "peer.env")
	writePeerServiceEnvironment(t, environmentPath, strings.Join([]string{
		codexconfig.EnvironmentVariable + "=" + serviceTestProviderConfig,
		"GATEWAY_KEY=" + secret,
		"EXTRA=" + secret,
		"",
	}, "\n"))
	stdout.Reset()
	stderr.Reset()
	code = Run([]string{
		"service", "install", "--config", configPath,
		"--environment-file", environmentPath,
	}, &stdout, &stderr)
	if code == 0 || !strings.Contains(stderr.String(), "unreferenced variable \"EXTRA\"") ||
		strings.Contains(stdout.String(), secret) || strings.Contains(stderr.String(), secret) {
		t.Fatalf("invalid environment install = %d, stdout %q, stderr %q", code, stdout.String(), stderr.String())
	}
	assertPathAbsent(t, artifact)

	managedEnvironment := filepath.Join(cfg.Peer.CodexHome, "peer.env")
	writePeerServiceEnvironment(t, managedEnvironment, strings.Join([]string{
		codexconfig.EnvironmentVariable + "=" + serviceTestProviderConfig,
		"GATEWAY_KEY=" + secret,
		"",
	}, "\n"))
	stdout.Reset()
	stderr.Reset()
	code = Run([]string{
		"service", "install", "--config", configPath,
		"--environment-file", managedEnvironment,
	}, &stdout, &stderr)
	if code == 0 || !strings.Contains(stderr.String(), "must not be inside worker CODEX_HOME") {
		t.Fatalf("managed-home environment install = %d, %q", code, stderr.String())
	}
	assertPathAbsent(t, artifact)
}

func TestBrokerServiceRejectsPeerEnvironmentBeforeWritingArtifact(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "authority", "broker.json")
	configHome := filepath.Join(root, "xdg")
	t.Setenv("XDG_CONFIG_HOME", configHome)
	var setupOutput bytes.Buffer
	var setupError bytes.Buffer
	if code := Run([]string{
		"setup", "broker", "--config", configPath, "--auth-mode", "none",
	}, &setupOutput, &setupError); code != 0 {
		t.Fatalf("setup code = %d, stderr = %q", code, setupError.String())
	}
	environmentPath := filepath.Join(filepath.Dir(configPath), "peer.env")
	writePeerServiceEnvironment(t, environmentPath, "IGNORED=secret\n")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := Run([]string{
		"service", "install", "--config", configPath,
		"--environment-file", environmentPath,
	}, &stdout, &stderr)
	if code == 0 || !strings.Contains(stderr.String(), "broker service must not use --environment-file") {
		t.Fatalf("broker environment install = %d, %q", code, stderr.String())
	}
	assertPathAbsent(t, filepath.Join(configHome, "systemd", "user", userservice.SystemdBrokerUnitName))
}

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
	wantArtifact := filepath.Join(configHome, "systemd", "user", userservice.SystemdBrokerUnitName)
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
    printf 'FragmentPath=%s/systemd/user/delegation-broker.service\nDropInPaths=\n' "$XDG_CONFIG_HOME"
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
		"--user --no-ask-password show " + userservice.SystemdBrokerUnitName + " --property=FragmentPath --property=DropInPaths",
		"--user --no-ask-password enable --now " + userservice.SystemdBrokerUnitName,
		"--user --no-ask-password is-enabled --quiet " + userservice.SystemdBrokerUnitName,
		"--user --no-ask-password is-active --quiet " + userservice.SystemdBrokerUnitName,
		"--user --no-ask-password show " + userservice.SystemdBrokerUnitName + " --property=FragmentPath --property=DropInPaths",
		"",
	}, "\n")
	if string(log) != want {
		t.Fatalf("systemctl log = %q, want %q", log, want)
	}
}

func TestNamedBrokerServiceInstallsUseDistinctSystemdUnits(t *testing.T) {
	root := privateTestDirectory(t)
	configHome := filepath.Join(root, "xdg")
	commandLog := filepath.Join(root, "systemctl.log")
	bin := filepath.Join(root, "bin")
	if err := os.MkdirAll(bin, 0o700); err != nil {
		t.Fatal(err)
	}
	systemctl := filepath.Join(bin, "systemctl")
	if err := os.WriteFile(systemctl, []byte(`#!/bin/sh
printf '%s\n' "$*" >>"$DELEGATION_TEST_SYSTEMCTL_LOG"
case " $* " in
  *" show "*)
    for arg in "$@"; do
      case "$arg" in
        delegation-*-broker.service)
          printf 'FragmentPath=%s/systemd/user/%s\nDropInPaths=\n' "$XDG_CONFIG_HOME" "$arg"
          exit 0
          ;;
      esac
    done
    exit 1
    ;;
esac
`), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("XDG_CONFIG_HOME", configHome)
	t.Setenv("DELEGATION_TEST_SYSTEMCTL_LOG", commandLog)

	for index, instanceID := range []string{"alpha", "beta"} {
		listen := startTestBrokerReadiness(t, instanceID)
		configPath := filepath.Join(root, "instances", instanceID, "broker.json")
		var setupOutput bytes.Buffer
		var setupError bytes.Buffer
		if code := Run([]string{
			"setup", "broker",
			"--config", configPath,
			"--instance", instanceID,
			"--auth-mode", "none",
			"--controller-id", serviceTestControllerID,
			"--listen", listen,
			"--status-listen", fmt.Sprintf("127.0.0.1:%d", 18881+index),
		}, &setupOutput, &setupError); code != 0 {
			t.Fatalf("setup %s code = %d, stderr = %q", instanceID, code, setupError.String())
		}
		var stdout bytes.Buffer
		var stderr bytes.Buffer
		if code := Run(
			[]string{"service", "install", "--config", configPath, "--json"},
			&stdout,
			&stderr,
		); code != 0 {
			t.Fatalf("install %s code = %d, stderr = %q", instanceID, code, stderr.String())
		}
		var result serviceInstallResult
		if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		unitName := "delegation-" + instanceID + "-broker.service"
		wantArtifact := filepath.Join(configHome, "systemd", "user", unitName)
		if result.State != userservice.StateActive || result.Artifact != wantArtifact {
			t.Fatalf("install %s result = %#v", instanceID, result)
		}
		content, err := os.ReadFile(wantArtifact)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Contains(content, []byte("delegation-managed:v1:"+instanceID+":broker")) ||
			!bytes.Contains(content, []byte(configPath)) {
			t.Fatalf("named unit %s has wrong identity:\n%s", unitName, content)
		}
	}

	defaultArtifact := filepath.Join(configHome, "systemd", "user", userservice.SystemdBrokerUnitName)
	assertPathAbsent(t, defaultArtifact)
	log, err := os.ReadFile(commandLog)
	if err != nil {
		t.Fatal(err)
	}
	for _, unitName := range []string{
		"delegation-alpha-broker.service",
		"delegation-beta-broker.service",
	} {
		if !bytes.Contains(log, []byte("enable --now "+unitName)) ||
			!bytes.Contains(log, []byte("is-enabled --quiet "+unitName)) ||
			!bytes.Contains(log, []byte("is-active --quiet "+unitName)) {
			t.Fatalf("systemctl log omits lifecycle for %s:\n%s", unitName, log)
		}
	}
	if bytes.Contains(log, []byte(userservice.SystemdBrokerUnitName)) {
		t.Fatalf("named installs targeted default unit:\n%s", log)
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
	artifact := filepath.Join(configHome, "systemd", "user", userservice.SystemdBrokerUnitName)
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
	artifact := filepath.Join(configHome, "systemd", "user", userservice.SystemdBrokerUnitName)
	if _, err := os.Lstat(artifact); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("service artifact exists after authority preflight failure: %v", err)
	}
}

func TestServiceInstallPreflightsTailscaleAuthorityBeforeWritingArtifact(t *testing.T) {
	for _, test := range []struct {
		name         string
		role         delegationconfig.Role
		artifactName string
	}{
		{
			name:         "broker",
			role:         delegationconfig.RoleBroker,
			artifactName: userservice.SystemdBrokerUnitName,
		},
		{
			name:         "peer",
			role:         delegationconfig.RolePeer,
			artifactName: userservice.SystemdPeerUnitName,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := privateTestDirectory(t)
			configHome := filepath.Join(root, "xdg")
			t.Setenv("XDG_CONFIG_HOME", configHome)

			var configPath string
			var cfg delegationconfig.Config
			switch test.role {
			case delegationconfig.RoleBroker:
				configPath, cfg = setupBrokerRuntimeTest(t, "none")
			case delegationconfig.RolePeer:
				configPath, cfg = setupConnectorRuntimeTest(
					t,
					"123e4567-e89b-42d3-a456-426614174204",
					"tailscale-service-install",
					"wss://broker.example.test",
				)
			default:
				t.Fatalf("unsupported test role %q", test.role)
			}
			stateDir := writeTailscaleConfigFixture(
				t,
				configPath,
				&cfg,
				test.name+"-node",
			)
			invalidKey := "must-not-appear-in-service-output"
			if err := os.WriteFile(
				cfg.Transport.Tailscale.AuthKeyFile,
				[]byte(invalidKey),
				0o600,
			); err != nil {
				t.Fatal(err)
			}

			args := []string{"service", "install", "--config", configPath}
			if test.role == delegationconfig.RolePeer {
				args = append(
					args,
					"--environment-file",
					filepath.Join(root, "unused-peer.env"),
				)
			}
			var stdout bytes.Buffer
			var stderr bytes.Buffer
			code := Run(args, &stdout, &stderr)
			if code == 0 || !strings.Contains(stderr.String(), "tskey-auth- prefix") {
				t.Fatalf(
					"service install code = %d, stdout = %q, stderr = %q",
					code,
					stdout.String(),
					stderr.String(),
				)
			}
			if strings.Contains(stdout.String(), invalidKey) ||
				strings.Contains(stderr.String(), invalidKey) {
				t.Fatalf(
					"service install disclosed enrollment key: stdout = %q, stderr = %q",
					stdout.String(),
					stderr.String(),
				)
			}
			assertPathAbsent(
				t,
				filepath.Join(configHome, "systemd", "user", test.artifactName),
			)
			assertNoTailscaleRuntimeSideEffects(t, stateDir)
		})
	}
}

func TestServiceInstallRejectsTailscaleLeaseAuthorityCollisionsBeforeWritingArtifact(
	t *testing.T,
) {
	t.Run("broker configuration", func(t *testing.T) {
		root := privateTestDirectory(t)
		configHome := filepath.Join(root, "xdg")
		t.Setenv("XDG_CONFIG_HOME", configHome)
		configPath, cfg := setupBrokerRuntimeTest(t, "none")
		stateDir := writeTailscaleConfigFixture(t, configPath, &cfg, "broker-node")
		collidingConfigPath := stateDir + ".tailscale.lock"
		data, err := json.Marshal(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(collidingConfigPath, data, 0o600); err != nil {
			t.Fatal(err)
		}

		var stdout bytes.Buffer
		var stderr bytes.Buffer
		code := Run(
			[]string{"service", "install", "--config", collidingConfigPath},
			&stdout,
			&stderr,
		)
		if code == 0 || !strings.Contains(
			stderr.String(),
			"Tailscale state directory lease path conflicts with broker configuration",
		) {
			t.Fatalf(
				"service install code = %d, stdout = %q, stderr = %q",
				code,
				stdout.String(),
				stderr.String(),
			)
		}
		assertPathAbsent(
			t,
			filepath.Join(configHome, "systemd", "user", userservice.SystemdBrokerUnitName),
		)
		got, err := os.ReadFile(collidingConfigPath)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, data) {
			t.Fatal("service install changed colliding config/lease authority")
		}
	})

	t.Run("peer environment", func(t *testing.T) {
		root := privateTestDirectory(t)
		configHome := filepath.Join(root, "xdg")
		t.Setenv("XDG_CONFIG_HOME", configHome)
		configPath, cfg := setupConnectorRuntimeTest(
			t,
			"123e4567-e89b-42d3-a456-426614174207",
			"tailscale-environment-collision",
			"wss://broker.example.test",
		)
		stateDir := writeTailscaleConfigFixture(t, configPath, &cfg, "peer-node")
		environmentPath := stateDir + ".tailscale.lock"
		environment := strings.Join([]string{
			codexconfig.EnvironmentVariable + "=" + serviceTestProviderConfig,
			"GATEWAY_KEY=operator-secret",
			"",
		}, "\n")
		writePeerServiceEnvironment(t, environmentPath, environment)

		var stdout bytes.Buffer
		var stderr bytes.Buffer
		code := Run([]string{
			"service", "install",
			"--config", configPath,
			"--environment-file", environmentPath,
		}, &stdout, &stderr)
		if code == 0 || !strings.Contains(
			stderr.String(),
			"peer service environment path conflicts with Tailscale state directory lease",
		) {
			t.Fatalf(
				"service install code = %d, stdout = %q, stderr = %q",
				code,
				stdout.String(),
				stderr.String(),
			)
		}
		if strings.Contains(stdout.String(), "operator-secret") ||
			strings.Contains(stderr.String(), "operator-secret") {
			t.Fatalf(
				"service install disclosed provider credential: stdout = %q, stderr = %q",
				stdout.String(),
				stderr.String(),
			)
		}
		assertPathAbsent(
			t,
			filepath.Join(configHome, "systemd", "user", userservice.SystemdPeerUnitName),
		)
		got, err := os.ReadFile(environmentPath)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != environment {
			t.Fatal("service install changed colliding environment/lease authority")
		}
	})
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
	artifact := filepath.Join(configHome, "systemd", "user", userservice.SystemdBrokerUnitName)
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
	artifact := filepath.Join(configHome, "systemd", "user", userservice.SystemdBrokerUnitName)
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
    printf 'FragmentPath=%s/systemd/user/delegation-broker.service\nDropInPaths=\n' "$XDG_CONFIG_HOME"
    ;;
esac
exit 0
`), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
}

func startTestBrokerReadiness(t *testing.T, instanceIDs ...string) string {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set(broker.HealthServiceHeader, "broker")
		writer.Header().Set(broker.HealthControllerHeader, serviceTestControllerID)
		if len(instanceIDs) > 0 && instanceIDs[0] != delegationconfig.DefaultInstanceID {
			writer.Header().Set(broker.HealthInstanceHeader, instanceIDs[0])
		}
		_, _ = writer.Write([]byte("ok\n"))
	}))
	t.Cleanup(server.Close)
	return strings.TrimPrefix(server.URL, "http://")
}

func (failingWriter) Write([]byte) (int, error) {
	return 0, errors.New("closed output")
}

func writePeerServiceEnvironment(t *testing.T, path string, contents string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
}

func assertPathAbsent(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("path exists after preflight failure: %s: %v", path, err)
	}
}
