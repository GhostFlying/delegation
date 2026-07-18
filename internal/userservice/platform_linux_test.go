//go:build linux

package userservice

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestLinuxServiceLifecycleUsesXDGUserDirectory(t *testing.T) {
	configHome := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", configHome)

	result, err := Prepare("/opt/delegation/bin/delegation", "/home/test/.delegation/config.json")
	if err != nil || result.State != StatePrepared || result.Kind != KindSystemd {
		t.Fatalf("Install() = %#v, %v", result, err)
	}
	wantPath := filepath.Join(configHome, "systemd", "user", SystemdUnitName)
	if result.Artifact != wantPath {
		t.Fatalf("artifact = %q, want %q", result.Artifact, wantPath)
	}
	content, err := os.ReadFile(wantPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(content), "Prepared by Delegation M0") {
		t.Fatalf("service definition is not explicitly inactive:\n%s", content)
	}
}

func TestLinuxInstallEnablesStartsAndVerifiesService(t *testing.T) {
	configHome := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", configHome)
	originalRunner := runSystemctl
	t.Cleanup(func() { runSystemctl = originalRunner })
	var calls [][]string
	runSystemctl = func(args ...string) (userServiceCommandResult, error) {
		calls = append(calls, slices.Clone(args))
		return userServiceCommandResult{}, nil
	}
	result, err := Install("/opt/delegation/bin/delegation", "/home/test/.delegation/config.json")
	if err != nil || result.State != StateActive {
		t.Fatalf("Install() = %#v, %v", result, err)
	}
	want := [][]string{
		{"--user", "--no-ask-password", "daemon-reload"},
		{"--user", "--no-ask-password", "enable", "--now", SystemdUnitName},
		{"--user", "--no-ask-password", "is-enabled", "--quiet", SystemdUnitName},
		{"--user", "--no-ask-password", "is-active", "--quiet", SystemdUnitName},
	}
	if !slices.EqualFunc(calls, want, slices.Equal[[]string]) {
		t.Fatalf("systemctl calls = %q, want %q", calls, want)
	}
}

func TestLinuxInstallReconcilesLostActivationResponse(t *testing.T) {
	configHome := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", configHome)
	originalRunner := runSystemctl
	t.Cleanup(func() { runSystemctl = originalRunner })
	calls := 0
	runSystemctl = func(...string) (userServiceCommandResult, error) {
		calls++
		if calls == 2 {
			return userServiceCommandResult{}, errors.New("connection lost")
		}
		return userServiceCommandResult{}, nil
	}
	result, err := Install("/opt/delegation/bin/delegation", "/home/test/.delegation/config.json")
	if err != nil || result.State != StateActive {
		t.Fatalf("Install() = %#v, %v", result, err)
	}
	if calls != 4 {
		t.Fatalf("systemctl calls = %d, want 4", calls)
	}
	if _, statErr := os.Stat(result.Artifact); statErr != nil {
		t.Fatalf("prepared unit missing after activation reconciliation: %v", statErr)
	}
}

func TestLinuxInstallReportsPartialActivation(t *testing.T) {
	configHome := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", configHome)
	originalRunner := runSystemctl
	t.Cleanup(func() { runSystemctl = originalRunner })
	calls := 0
	runSystemctl = func(...string) (userServiceCommandResult, error) {
		calls++
		switch calls {
		case 2:
			return userServiceCommandResult{ExitCode: 1}, nil
		case 3:
			return userServiceCommandResult{}, nil
		case 4:
			return userServiceCommandResult{ExitCode: 3}, nil
		default:
			return userServiceCommandResult{}, nil
		}
	}
	result, err := Install("/opt/delegation/bin/delegation", "/home/test/.delegation/config.json")
	if err == nil || result.State != StateIndeterminate {
		t.Fatalf("Install() = %#v, %v", result, err)
	}
	if _, statErr := os.Stat(result.Artifact); statErr != nil {
		t.Fatalf("prepared unit missing after activation failure: %v", statErr)
	}
}

func TestLinuxServiceRejectsRelativeXDGConfigHome(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", "relative")
	if _, err := Prepare("/opt/delegation", "/home/test/config.json"); err == nil {
		t.Fatal("Prepare() accepted relative XDG_CONFIG_HOME")
	}
}
