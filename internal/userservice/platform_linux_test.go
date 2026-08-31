//go:build linux

package userservice

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestLinuxUpgradeLifecycleFencesDefinitionAndProcessTree(t *testing.T) {
	stubLinuxServiceReadiness(t, nil)
	configHome := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", configHome)
	source := testInvocation(
		ServiceRolePeer, "/opt/delegation/0.1.0/delegation",
		"/home/test/.delegation/peer.json",
	)
	target := source
	target.BinaryPath = "/opt/delegation/0.2.0/delegation"
	prepared, err := Prepare(ServiceRolePeer, source)
	if err != nil {
		t.Fatal(err)
	}
	originalRunner := runSystemctl
	t.Cleanup(func() { runSystemctl = originalRunner })
	active := true
	var calls [][]string
	runSystemctl = func(args ...string) (userServiceCommandResult, error) {
		calls = append(calls, slices.Clone(args))
		switch {
		case slices.Contains(args, "show"):
			state, mainPID := "inactive", 0
			if active {
				state, mainPID = "active", 4242
			}
			return systemdUpgradeResult(prepared.Artifact, state, mainPID), nil
		case slices.Contains(args, "stop"):
			active = false
		case slices.Contains(args, "start"):
			active = true
		}
		return userServiceCommandResult{}, nil
	}
	plan, err := PrepareUpgrade(context.Background(), ServiceRolePeer, source, target)
	if err != nil || plan.Artifact != prepared.Artifact || !slices.Equal(plan.ProcessIDs, []int{4242}) {
		t.Fatalf("PrepareUpgrade() = %#v, %v", plan, err)
	}
	if err := StopUpgrade(context.Background(), plan); err != nil {
		t.Fatal(err)
	}
	if err := SwitchUpgradeDefinition(context.Background(), plan, true); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(plan.Artifact)
	if err != nil || !strings.Contains(string(content), target.BinaryPath) || strings.Contains(string(content), source.BinaryPath) {
		t.Fatalf("switched definition = %q, %v", content, err)
	}
	if err := StartUpgrade(context.Background(), plan, true); err != nil {
		t.Fatal(err)
	}
	matched, err := UpgradeServiceMatches(context.Background(), plan, true)
	if err != nil || !matched {
		t.Fatalf("UpgradeServiceMatches() = %v, %v", matched, err)
	}
	if !slices.ContainsFunc(calls, func(call []string) bool { return slices.Contains(call, "stop") }) ||
		!slices.ContainsFunc(calls, func(call []string) bool { return slices.Contains(call, "daemon-reload") }) ||
		!slices.ContainsFunc(calls, func(call []string) bool { return slices.Contains(call, "start") }) {
		t.Fatalf("systemd upgrade calls = %q", calls)
	}
}

func TestLinuxDiscoversLegacyUpgradeSourceWithoutLocalBridge(t *testing.T) {
	configHome := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", configHome)
	source := testInvocation(
		ServiceRolePeer, "/opt/delegation/0.1.0-alpha.4/delegation",
		"/home/test/.delegation/peer.json",
	)
	prepared, err := Prepare(ServiceRolePeer, source)
	if err != nil {
		t.Fatal(err)
	}
	originalRunner := runSystemctl
	t.Cleanup(func() { runSystemctl = originalRunner })
	runSystemctl = func(args ...string) (userServiceCommandResult, error) {
		return systemdUpgradeResult(prepared.Artifact, "active", 4242), nil
	}
	expected := source
	expected.BinaryPath = ""
	got, err := DiscoverUpgradeSource(context.Background(), ServiceRolePeer, expected)
	if err != nil || got != source {
		t.Fatalf("DiscoverUpgradeSource() = %#v, %v; want %#v", got, err, source)
	}
	wrong := expected
	wrong.ConfigPath = "/home/test/.delegation/other.json"
	if _, err := DiscoverUpgradeSource(context.Background(), ServiceRolePeer, wrong); err == nil {
		t.Fatal("DiscoverUpgradeSource() accepted a mismatched config path")
	}
}

func TestLinuxUpgradeRejectsDefinitionDriftBeforeStop(t *testing.T) {
	configHome := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", configHome)
	source := testInvocation(
		ServiceRolePeer, "/opt/delegation/0.1.0/delegation",
		"/home/test/.delegation/peer.json",
	)
	target := source
	target.BinaryPath = "/opt/delegation/0.2.0/delegation"
	prepared, err := Prepare(ServiceRolePeer, source)
	if err != nil {
		t.Fatal(err)
	}
	originalRunner := runSystemctl
	t.Cleanup(func() { runSystemctl = originalRunner })
	runSystemctl = func(args ...string) (userServiceCommandResult, error) {
		return systemdUpgradeResult(prepared.Artifact, "active", 123), nil
	}
	plan, err := PrepareUpgrade(context.Background(), ServiceRolePeer, source, target)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(plan.Artifact, []byte("# foreign\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := StopUpgrade(context.Background(), plan); err == nil || !strings.Contains(err.Error(), "changed outside") {
		t.Fatalf("StopUpgrade() = %v", err)
	}
}

func TestLinuxUpgradeRejectsProcessIdentityDriftBeforeStop(t *testing.T) {
	configHome := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", configHome)
	source := testInvocation(
		ServiceRolePeer, "/opt/delegation/0.1.0/delegation",
		"/home/test/.delegation/peer.json",
	)
	target := source
	target.BinaryPath = "/opt/delegation/0.2.0/delegation"
	prepared, err := Prepare(ServiceRolePeer, source)
	if err != nil {
		t.Fatal(err)
	}
	originalRunner := runSystemctl
	t.Cleanup(func() { runSystemctl = originalRunner })
	mainPID := 123
	stopCalled := false
	runSystemctl = func(args ...string) (userServiceCommandResult, error) {
		if slices.Contains(args, "show") {
			return systemdUpgradeResult(prepared.Artifact, "active", mainPID), nil
		}
		if slices.Contains(args, "stop") {
			stopCalled = true
		}
		return userServiceCommandResult{}, nil
	}
	plan, err := PrepareUpgrade(context.Background(), ServiceRolePeer, source, target)
	if err != nil {
		t.Fatal(err)
	}
	mainPID = 456
	if err := StopUpgrade(context.Background(), plan); err == nil || !strings.Contains(err.Error(), "process identity changed") {
		t.Fatalf("StopUpgrade() = %v", err)
	}
	if stopCalled {
		t.Fatal("StopUpgrade() stopped a replacement process")
	}
}

func TestLinuxServiceLifecycleUsesXDGUserDirectory(t *testing.T) {
	configHome := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", configHome)

	result, err := Prepare(ServiceRolePeer, testInvocation(
		ServiceRolePeer, "/opt/delegation/bin/delegation", "/home/test/.delegation/config.json",
	))
	if err != nil || result.State != StatePrepared || result.Kind != KindSystemd {
		t.Fatalf("Install() = %#v, %v", result, err)
	}
	wantPath := filepath.Join(configHome, "systemd", "user", SystemdPeerUnitName)
	if result.Artifact != wantPath {
		t.Fatalf("artifact = %q, want %q", result.Artifact, wantPath)
	}
	content, err := os.ReadFile(wantPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(content), MarkerPeer) || !strings.Contains(string(content), "Description=Delegation peer") {
		t.Fatalf("service definition has the wrong peer identity:\n%s", content)
	}
}

func TestLinuxBrokerAndPeerDefinitionsCoexist(t *testing.T) {
	configHome := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", configHome)
	broker, err := Prepare(ServiceRoleBroker, testInvocation(
		ServiceRoleBroker, "/opt/delegation/bin/delegation", "/home/test/.delegation/broker.json",
	))
	if err != nil {
		t.Fatal(err)
	}
	peer, err := Prepare(ServiceRolePeer, testInvocation(
		ServiceRolePeer, "/opt/delegation/bin/delegation", "/home/test/.delegation/peer.json",
	))
	if err != nil {
		t.Fatal(err)
	}
	if broker.Artifact == peer.Artifact || broker.Role != ServiceRoleBroker || peer.Role != ServiceRolePeer {
		t.Fatalf("cohost results = %#v / %#v", broker, peer)
	}
	for path, marker := range map[string]string{broker.Artifact: MarkerBroker, peer.Artifact: MarkerPeer} {
		content, err := os.ReadFile(path)
		if err != nil || !strings.Contains(string(content), marker) {
			t.Fatalf("cohost definition %s = %q, error %v", path, content, err)
		}
	}
}

func TestLinuxInstallEnablesStartsAndVerifiesService(t *testing.T) {
	stubLinuxServiceReadiness(t, nil)
	configHome := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", configHome)
	binaryPath := "/opt/delegation/bin/delegation"
	configPath := "/home/test/.delegation/config.json"
	artifact := filepath.Join(configHome, "systemd", "user", SystemdPeerUnitName)
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
	result, err := Install(ServiceRolePeer, testInvocation(ServiceRolePeer, binaryPath, configPath))
	if err != nil || result.State != StateActive {
		t.Fatalf("Install() = %#v, %v", result, err)
	}
	want := [][]string{
		{"--user", "--no-ask-password", "daemon-reload"},
		{"--user", "--no-ask-password", "show", SystemdPeerUnitName, "--property=FragmentPath", "--property=DropInPaths"},
		{"--user", "--no-ask-password", "enable", "--now", SystemdPeerUnitName},
		{"--user", "--no-ask-password", "is-enabled", "--quiet", SystemdPeerUnitName},
		{"--user", "--no-ask-password", "is-active", "--quiet", SystemdPeerUnitName},
		{"--user", "--no-ask-password", "show", SystemdPeerUnitName, "--property=FragmentPath", "--property=DropInPaths"},
	}
	if !slices.EqualFunc(calls, want, slices.Equal[[]string]) {
		t.Fatalf("systemctl calls = %q, want %q", calls, want)
	}
}

func TestLinuxNamedInstanceLifecycleTargetsNamedUnit(t *testing.T) {
	stubLinuxServiceReadiness(t, nil)
	configHome := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", configHome)
	invocation := testInvocation(
		ServiceRolePeer, "/opt/delegation/bin/delegation", "/home/test/.delegation/config.json",
	)
	invocation.InstanceID = "alpha-2"
	unitName := "delegation-alpha-2-peer.service"
	artifact := filepath.Join(configHome, "systemd", "user", unitName)
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
	result, err := Install(ServiceRolePeer, invocation)
	if err != nil || result.State != StateActive || result.Artifact != artifact {
		t.Fatalf("Install() = %#v, %v", result, err)
	}
	for _, call := range calls {
		if slices.Contains(call, SystemdPeerUnitName) {
			t.Fatalf("named lifecycle targeted legacy unit: %q", calls)
		}
		if call[2] != "daemon-reload" && !slices.Contains(call, unitName) {
			t.Fatalf("named lifecycle omitted unit %q: %q", unitName, call)
		}
	}
}

func TestLinuxPrepareRejectsInvalidInstanceWithoutSideEffects(t *testing.T) {
	configHome := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", configHome)
	invocation := testInvocation(
		ServiceRolePeer, "/opt/delegation/bin/delegation", "/home/test/.delegation/config.json",
	)
	invocation.InstanceID = "unsafe/instance"
	if _, err := Prepare(ServiceRolePeer, invocation); err == nil {
		t.Fatal("Prepare() accepted invalid instance ID")
	}
	entries, err := os.ReadDir(configHome)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("invalid instance created files: %v", entries)
	}
}

func TestLinuxInstallReconcilesLostActivationResponse(t *testing.T) {
	stubLinuxServiceReadiness(t, nil)
	configHome := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", configHome)
	artifact := filepath.Join(configHome, "systemd", "user", SystemdPeerUnitName)
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
	result, err := Install(ServiceRolePeer, testInvocation(
		ServiceRolePeer, "/opt/delegation/bin/delegation", "/home/test/.delegation/config.json",
	))
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
	artifact := filepath.Join(configHome, "systemd", "user", SystemdPeerUnitName)
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
	result, err := Install(ServiceRolePeer, testInvocation(
		ServiceRolePeer, "/opt/delegation/bin/delegation", "/home/test/.delegation/config.json",
	))
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
			artifact := filepath.Join(configHome, "systemd", "user", SystemdPeerUnitName)
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
			result, err := Install(ServiceRolePeer, testInvocation(
				ServiceRolePeer, "/opt/delegation/bin/delegation", "/home/test/.delegation/config.json",
			))
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
	artifact := filepath.Join(configHome, "systemd", "user", SystemdPeerUnitName)
	originalRunner := runSystemctl
	t.Cleanup(func() { runSystemctl = originalRunner })
	runSystemctl = func(args ...string) (userServiceCommandResult, error) {
		if slices.Contains(args, "show") {
			return systemdIdentityResult(artifact, ""), nil
		}
		return userServiceCommandResult{}, nil
	}
	result, err := Install(ServiceRolePeer, testInvocation(
		ServiceRolePeer, "/opt/delegation/bin/delegation", "/home/test/.delegation/config.json",
	))
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
	if _, err := Prepare(ServiceRolePeer, testInvocation(
		ServiceRolePeer, "/opt/delegation", "/home/test/config.json",
	)); err == nil {
		t.Fatal("Prepare() accepted relative XDG_CONFIG_HOME")
	}
}

func systemdIdentityResult(fragment, dropIns string) userServiceCommandResult {
	return userServiceCommandResult{Output: []byte(fmt.Sprintf(
		"FragmentPath=%s\nDropInPaths=%s\n", fragment, dropIns,
	))}
}

func systemdUpgradeResult(fragment, state string, mainPID int) userServiceCommandResult {
	return userServiceCommandResult{Output: []byte(fmt.Sprintf(
		"FragmentPath=%s\nDropInPaths=\nControlGroup=/user.slice/delegation\nMainPID=%d\nControlPID=0\nActiveState=%s\nUnitFileState=enabled\n",
		fragment, mainPID, state,
	))}
}
