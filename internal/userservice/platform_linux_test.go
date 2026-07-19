//go:build linux

package userservice

import (
	"errors"
	"fmt"
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
	stubLinuxServiceReadiness(t, nil)
	configHome := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", configHome)
	binaryPath := "/opt/delegation/bin/delegation"
	configPath := "/home/test/.delegation/config.json"
	artifact := filepath.Join(configHome, "systemd", "user", SystemdUnitName)
	originalRunner := runSystemctl
	t.Cleanup(func() { runSystemctl = originalRunner })
	var calls [][]string
	runSystemctl = func(args ...string) (userServiceCommandResult, error) {
		calls = append(calls, slices.Clone(args))
		if slices.Contains(args, "show") {
			return systemdIdentityResult(artifact, ""), nil
		}
		return userServiceCommandResult{}, nil
	}
	result, err := Install(binaryPath, configPath)
	if err != nil || result.State != StateActive {
		t.Fatalf("Install() = %#v, %v", result, err)
	}
	want := [][]string{
		{"--user", "--no-ask-password", "daemon-reload"},
		{"--user", "--no-ask-password", "show", SystemdUnitName, "--property=FragmentPath", "--property=DropInPaths"},
		{"--user", "--no-ask-password", "enable", "--now", SystemdUnitName},
		{"--user", "--no-ask-password", "is-enabled", "--quiet", SystemdUnitName},
		{"--user", "--no-ask-password", "is-active", "--quiet", SystemdUnitName},
		{"--user", "--no-ask-password", "show", SystemdUnitName, "--property=FragmentPath", "--property=DropInPaths"},
	}
	if !slices.EqualFunc(calls, want, slices.Equal[[]string]) {
		t.Fatalf("systemctl calls = %q, want %q", calls, want)
	}
}

func TestLinuxInstallReconcilesLostActivationResponse(t *testing.T) {
	stubLinuxServiceReadiness(t, nil)
	configHome := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", configHome)
	artifact := filepath.Join(configHome, "systemd", "user", SystemdUnitName)
	originalRunner := runSystemctl
	t.Cleanup(func() { runSystemctl = originalRunner })
	calls := 0
	runSystemctl = func(args ...string) (userServiceCommandResult, error) {
		calls++
		if slices.Contains(args, "show") {
			return systemdIdentityResult(artifact, ""), nil
		}
		if calls == 3 {
			return userServiceCommandResult{}, errors.New("connection lost")
		}
		return userServiceCommandResult{}, nil
	}
	result, err := Install("/opt/delegation/bin/delegation", "/home/test/.delegation/config.json")
	if err != nil || result.State != StateActive {
		t.Fatalf("Install() = %#v, %v", result, err)
	}
	if calls != 6 {
		t.Fatalf("systemctl calls = %d, want 6", calls)
	}
	if _, statErr := os.Stat(result.Artifact); statErr != nil {
		t.Fatalf("prepared unit missing after activation reconciliation: %v", statErr)
	}
}

func TestLinuxInstallReportsPartialActivation(t *testing.T) {
	stubLinuxServiceReadiness(t, nil)
	configHome := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", configHome)
	artifact := filepath.Join(configHome, "systemd", "user", SystemdUnitName)
	originalRunner := runSystemctl
	t.Cleanup(func() { runSystemctl = originalRunner })
	calls := 0
	runSystemctl = func(args ...string) (userServiceCommandResult, error) {
		calls++
		if slices.Contains(args, "show") {
			return systemdIdentityResult(artifact, ""), nil
		}
		switch calls {
		case 3:
			return userServiceCommandResult{ExitCode: 1}, nil
		case 5:
			return userServiceCommandResult{}, nil
		case 6:
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

func TestLinuxInstallRejectsShadowedOrOverriddenUnit(t *testing.T) {
	for _, test := range []struct {
		name     string
		fragment func(string) string
		dropIns  string
	}{
		{name: "shadowed fragment", fragment: func(string) string { return "/etc/systemd/user/delegation.service" }},
		{name: "drop-in override", fragment: func(path string) string { return path }, dropIns: "/tmp/override.conf"},
	} {
		t.Run(test.name, func(t *testing.T) {
			stubLinuxServiceReadiness(t, nil)
			configHome := t.TempDir()
			t.Setenv("XDG_CONFIG_HOME", configHome)
			artifact := filepath.Join(configHome, "systemd", "user", SystemdUnitName)
			originalRunner := runSystemctl
			t.Cleanup(func() { runSystemctl = originalRunner })
			var calls [][]string
			runSystemctl = func(args ...string) (userServiceCommandResult, error) {
				calls = append(calls, slices.Clone(args))
				if slices.Contains(args, "show") {
					return systemdIdentityResult(test.fragment(artifact), test.dropIns), nil
				}
				return userServiceCommandResult{}, nil
			}
			result, err := Install("/opt/delegation/bin/delegation", "/home/test/.delegation/config.json")
			if err == nil || result.State != StateForeignConflict {
				t.Fatalf("Install() = %#v, %v", result, err)
			}
			if len(calls) != 2 || slices.ContainsFunc(calls, func(args []string) bool {
				return slices.Contains(args, "enable")
			}) {
				t.Fatalf("shadowed unit activation calls = %q", calls)
			}
		})
	}
}

func TestLinuxInstallRejectsServiceThatNeverBecomesReady(t *testing.T) {
	readinessErr := errors.New("connector did not open its local bridge")
	stubLinuxServiceReadiness(t, readinessErr)
	configHome := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", configHome)
	artifact := filepath.Join(configHome, "systemd", "user", SystemdUnitName)
	originalRunner := runSystemctl
	t.Cleanup(func() { runSystemctl = originalRunner })
	runSystemctl = func(args ...string) (userServiceCommandResult, error) {
		if slices.Contains(args, "show") {
			return systemdIdentityResult(artifact, ""), nil
		}
		return userServiceCommandResult{}, nil
	}
	result, err := Install("/opt/delegation/bin/delegation", "/home/test/.delegation/config.json")
	if !errors.Is(err, readinessErr) || result.State != StateIndeterminate {
		t.Fatalf("Install() = %#v, %v", result, err)
	}
}

func stubLinuxServiceReadiness(t *testing.T, err error) {
	t.Helper()
	original := waitForLinuxServiceReady
	waitForLinuxServiceReady = func(string) error { return err }
	t.Cleanup(func() { waitForLinuxServiceReady = original })
}

func TestLinuxServiceRejectsRelativeXDGConfigHome(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", "relative")
	if _, err := Prepare("/opt/delegation", "/home/test/config.json"); err == nil {
		t.Fatal("Prepare() accepted relative XDG_CONFIG_HOME")
	}
}

func systemdIdentityResult(fragment, dropIns string) userServiceCommandResult {
	return userServiceCommandResult{Output: []byte(fmt.Sprintf(
		"FragmentPath=%s\nDropInPaths=%s\n", fragment, dropIns,
	))}
}
