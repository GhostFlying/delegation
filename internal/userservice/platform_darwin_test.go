//go:build darwin

package userservice

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	delegationconfig "github.com/GhostFlying/delegation/internal/config"
)

func TestDarwinServiceLifecycleUsesLaunchAgents(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	result, err := Prepare(ServiceRolePeer, testInvocation(
		ServiceRolePeer, "/opt/delegation/bin/delegation", filepath.Join(home, ".delegation", "config.json"),
	))
	if err != nil || result.State != StatePrepared || result.Kind != KindLaunchAgent {
		t.Fatalf("Install() = %#v, %v", result, err)
	}
	wantPath := filepath.Join(home, "Library", "LaunchAgents", LaunchAgentPeerName+".plist")
	if result.Artifact != wantPath {
		t.Fatalf("artifact = %q, want %q", result.Artifact, wantPath)
	}
}

func TestDarwinBrokerAndPeerDefinitionsCoexist(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	broker, err := Prepare(
		ServiceRoleBroker,
		testInvocation(ServiceRoleBroker, "/opt/delegation/bin/delegation", filepath.Join(home, ".delegation", "broker.json")),
	)
	if err != nil {
		t.Fatal(err)
	}
	peer, err := Prepare(
		ServiceRolePeer,
		testInvocation(ServiceRolePeer, "/opt/delegation/bin/delegation", filepath.Join(home, ".delegation", "peer.json")),
	)
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

func TestDarwinInstallBootstrapsEnablesAndStartsService(t *testing.T) {
	stubDarwinServiceReadiness(t, nil)
	home := t.TempDir()
	t.Setenv("HOME", home)
	invocation := testInvocation(
		ServiceRolePeer,
		"/opt/delegation/bin/delegation",
		filepath.Join(home, ".delegation", "config.json"),
	)
	originalRunner := runLaunchctl
	t.Cleanup(func() { runLaunchctl = originalRunner })
	var calls [][]string
	targetPrints := 0
	runLaunchctl = func(args ...string) (userServiceCommandResult, error) {
		calls = append(calls, slices.Clone(args))
		if args[0] == "print" && len(args) == 2 && args[1] == fmt.Sprintf("gui/%d", os.Geteuid()) {
			return userServiceCommandResult{}, nil
		}
		if args[0] == "print" {
			targetPrints++
			if targetPrints > 1 {
				path := filepath.Join(home, "Library", "LaunchAgents", LaunchAgentPeerName+".plist")
				return launchctlTestStatus(path, "running", 123, invocation), nil
			}
			return userServiceCommandResult{ExitCode: 113}, nil
		}
		return userServiceCommandResult{}, nil
	}
	result, err := Install(ServiceRolePeer, invocation)
	if err != nil || result.State != StateActive {
		t.Fatalf("Install() = %#v, %v", result, err)
	}
	domain := fmt.Sprintf("gui/%d", os.Geteuid())
	target := domain + "/" + LaunchAgentPeerName
	want := [][]string{
		{"print", domain},
		{"print", target},
		{"enable", target},
		{"bootstrap", domain, result.Artifact},
		{"kickstart", target},
		{"print", target},
	}
	if !slices.EqualFunc(calls, want, slices.Equal[[]string]) {
		t.Fatalf("launchctl calls = %q, want %q", calls, want)
	}
}

func TestDarwinInstallWaitsForLaunchAgentRunning(t *testing.T) {
	stubDarwinServiceReadiness(t, nil)
	stubLaunchAgentStartTiming(t, time.Second, time.Millisecond)
	home := t.TempDir()
	t.Setenv("HOME", home)
	invocation := testInvocation(
		ServiceRolePeer,
		"/opt/delegation/bin/delegation",
		filepath.Join(home, ".delegation", "config.json"),
	)
	artifact := filepath.Join(home, "Library", "LaunchAgents", LaunchAgentPeerName+".plist")
	originalRunner := runLaunchctl
	t.Cleanup(func() { runLaunchctl = originalRunner })
	targetPrints := 0
	runLaunchctl = func(args ...string) (userServiceCommandResult, error) {
		if args[0] == "print" && len(args) == 2 && args[1] == fmt.Sprintf("gui/%d", os.Geteuid()) {
			return userServiceCommandResult{}, nil
		}
		if args[0] != "print" {
			return userServiceCommandResult{}, nil
		}
		targetPrints++
		switch targetPrints {
		case 1:
			return userServiceCommandResult{ExitCode: 113}, nil
		case 2, 3:
			return launchctlTestStatus(artifact, "waiting", 0, invocation), nil
		default:
			return launchctlTestStatus(artifact, "running", 123, invocation), nil
		}
	}
	result, err := Install(ServiceRolePeer, invocation)
	if err != nil || result.State != StateActive || targetPrints != 4 {
		t.Fatalf("Install() = %#v, %v, target prints = %d", result, err, targetPrints)
	}
}

func TestDarwinInstallWaitsForLaunchAgentPID(t *testing.T) {
	stubDarwinServiceReadiness(t, nil)
	stubLaunchAgentStartTiming(t, time.Second, time.Millisecond)
	home := t.TempDir()
	t.Setenv("HOME", home)
	invocation := testInvocation(
		ServiceRolePeer,
		"/opt/delegation/bin/delegation",
		filepath.Join(home, ".delegation", "config.json"),
	)
	artifact := filepath.Join(home, "Library", "LaunchAgents", LaunchAgentPeerName+".plist")
	originalRunner := runLaunchctl
	t.Cleanup(func() { runLaunchctl = originalRunner })
	targetPrints := 0
	runLaunchctl = func(args ...string) (userServiceCommandResult, error) {
		if args[0] == "print" && len(args) == 2 && args[1] == fmt.Sprintf("gui/%d", os.Geteuid()) {
			return userServiceCommandResult{}, nil
		}
		if args[0] != "print" {
			return userServiceCommandResult{}, nil
		}
		targetPrints++
		if targetPrints == 1 {
			return userServiceCommandResult{ExitCode: 113}, nil
		}
		pid := 0
		if targetPrints > 2 {
			pid = 123
		}
		return launchctlTestStatus(artifact, "running", pid, invocation), nil
	}
	result, err := Install(ServiceRolePeer, invocation)
	if err != nil || result.State != StateActive || targetPrints != 3 {
		t.Fatalf("Install() = %#v, %v, target prints = %d", result, err, targetPrints)
	}
}

func TestDarwinInstallAcceptsOmittedLaunchctlArguments(t *testing.T) {
	stubDarwinServiceReadiness(t, nil)
	home := t.TempDir()
	t.Setenv("HOME", home)
	invocation := testInvocation(
		ServiceRolePeer,
		"/opt/delegation/bin/delegation",
		filepath.Join(home, ".delegation", "config.json"),
	)
	artifact := filepath.Join(home, "Library", "LaunchAgents", LaunchAgentPeerName+".plist")
	originalRunner := runLaunchctl
	t.Cleanup(func() { runLaunchctl = originalRunner })
	targetPrints := 0
	runLaunchctl = func(args ...string) (userServiceCommandResult, error) {
		if args[0] == "print" && len(args) == 2 && args[1] == fmt.Sprintf("gui/%d", os.Geteuid()) {
			return userServiceCommandResult{}, nil
		}
		if args[0] != "print" {
			return userServiceCommandResult{}, nil
		}
		targetPrints++
		if targetPrints == 1 {
			return userServiceCommandResult{ExitCode: 113}, nil
		}
		return launchctlTestStatusWithoutArguments(artifact, "running", 123, invocation), nil
	}
	result, err := Install(ServiceRolePeer, invocation)
	if err != nil || result.State != StateActive {
		t.Fatalf("Install() = %#v, %v", result, err)
	}
}

func TestDarwinInstallAcceptsOmittedLaunchctlProgram(t *testing.T) {
	stubDarwinServiceReadiness(t, nil)
	home := t.TempDir()
	t.Setenv("HOME", home)
	invocation := testInvocation(
		ServiceRolePeer,
		"/opt/delegation/bin/delegation",
		filepath.Join(home, ".delegation", "config.json"),
	)
	artifact := filepath.Join(home, "Library", "LaunchAgents", LaunchAgentPeerName+".plist")
	originalRunner := runLaunchctl
	t.Cleanup(func() { runLaunchctl = originalRunner })
	targetPrints := 0
	runLaunchctl = func(args ...string) (userServiceCommandResult, error) {
		if args[0] == "print" && len(args) == 2 && args[1] == fmt.Sprintf("gui/%d", os.Geteuid()) {
			return userServiceCommandResult{}, nil
		}
		if args[0] != "print" {
			return userServiceCommandResult{}, nil
		}
		targetPrints++
		if targetPrints == 1 {
			return userServiceCommandResult{ExitCode: 113}, nil
		}
		return launchctlTestStatusWithoutProgram(artifact, "running", 123, invocation), nil
	}
	result, err := Install(ServiceRolePeer, invocation)
	if err != nil || result.State != StateActive {
		t.Fatalf("Install() = %#v, %v", result, err)
	}
}

func TestDarwinInstallRejectsUnconfirmedLaunchctlExecutable(t *testing.T) {
	stubDarwinServiceReadiness(t, nil)
	stubLaunchAgentStartTiming(t, 10*time.Millisecond, time.Millisecond)
	home := t.TempDir()
	t.Setenv("HOME", home)
	invocation := testInvocation(
		ServiceRolePeer,
		"/opt/delegation/bin/delegation",
		filepath.Join(home, ".delegation", "config.json"),
	)
	artifact := filepath.Join(home, "Library", "LaunchAgents", LaunchAgentPeerName+".plist")
	originalRunner := runLaunchctl
	t.Cleanup(func() { runLaunchctl = originalRunner })
	targetPrints := 0
	runLaunchctl = func(args ...string) (userServiceCommandResult, error) {
		if args[0] == "print" && len(args) == 2 && args[1] == fmt.Sprintf("gui/%d", os.Geteuid()) {
			return userServiceCommandResult{}, nil
		}
		if args[0] != "print" {
			return userServiceCommandResult{}, nil
		}
		targetPrints++
		if targetPrints == 1 {
			return userServiceCommandResult{ExitCode: 113}, nil
		}
		return launchctlTestStatusWithoutProgramOrArguments(artifact, "running", 123), nil
	}
	result, err := Install(ServiceRolePeer, invocation)
	if err == nil || result.State != StateIndeterminate || targetPrints < 3 {
		t.Fatalf("Install() = %#v, %v, target prints = %d", result, err, targetPrints)
	}
}

func TestDarwinInstallTimesOutWhenLaunchAgentNeverRuns(t *testing.T) {
	stubDarwinServiceReadiness(t, nil)
	stubLaunchAgentStartTiming(t, 10*time.Millisecond, time.Millisecond)
	home := t.TempDir()
	t.Setenv("HOME", home)
	invocation := testInvocation(
		ServiceRolePeer,
		"/opt/delegation/bin/delegation",
		filepath.Join(home, ".delegation", "config.json"),
	)
	artifact := filepath.Join(home, "Library", "LaunchAgents", LaunchAgentPeerName+".plist")
	originalRunner := runLaunchctl
	t.Cleanup(func() { runLaunchctl = originalRunner })
	targetPrints := 0
	runLaunchctl = func(args ...string) (userServiceCommandResult, error) {
		if args[0] == "print" && len(args) == 2 && args[1] == fmt.Sprintf("gui/%d", os.Geteuid()) {
			return userServiceCommandResult{}, nil
		}
		if args[0] != "print" {
			return userServiceCommandResult{}, nil
		}
		targetPrints++
		if targetPrints == 1 {
			return userServiceCommandResult{ExitCode: 113}, nil
		}
		return launchctlTestStatus(artifact, "waiting", 0, invocation), nil
	}
	result, err := Install(ServiceRolePeer, invocation)
	if err == nil || result.State != StateIndeterminate || targetPrints < 3 ||
		!strings.Contains(err.Error(), `state="waiting" pid=0`) {
		t.Fatalf("Install() = %#v, %v, target prints = %d", result, err, targetPrints)
	}
}

func TestDarwinInstallRejectsExecutableChangeDuringStart(t *testing.T) {
	stubDarwinServiceReadiness(t, nil)
	home := t.TempDir()
	t.Setenv("HOME", home)
	invocation := testInvocation(
		ServiceRolePeer,
		"/opt/delegation/bin/delegation",
		filepath.Join(home, ".delegation", "config.json"),
	)
	foreignInvocation := invocation
	foreignInvocation.BinaryPath = "/tmp/foreign-delegation"
	artifact := filepath.Join(home, "Library", "LaunchAgents", LaunchAgentPeerName+".plist")
	originalRunner := runLaunchctl
	t.Cleanup(func() { runLaunchctl = originalRunner })
	targetPrints := 0
	runLaunchctl = func(args ...string) (userServiceCommandResult, error) {
		if args[0] == "print" && len(args) == 2 && args[1] == fmt.Sprintf("gui/%d", os.Geteuid()) {
			return userServiceCommandResult{}, nil
		}
		if args[0] != "print" {
			return userServiceCommandResult{}, nil
		}
		targetPrints++
		if targetPrints == 1 {
			return userServiceCommandResult{ExitCode: 113}, nil
		}
		return launchctlTestStatus(artifact, "running", 123, foreignInvocation), nil
	}
	result, err := Install(ServiceRolePeer, invocation)
	if err == nil || result.State != StateForeignConflict ||
		!errors.Is(err, errLaunchAgentIdentityChangedDuringStart) {
		t.Fatalf("Install() = %#v, %v", result, err)
	}
}

func TestDarwinInstallRejectsArgumentChangeDuringStart(t *testing.T) {
	stubDarwinServiceReadiness(t, nil)
	home := t.TempDir()
	t.Setenv("HOME", home)
	invocation := testInvocation(
		ServiceRolePeer,
		"/opt/delegation/bin/delegation",
		filepath.Join(home, ".delegation", "config.json"),
	)
	foreignInvocation := invocation
	foreignInvocation.ConfigPath = filepath.Join(home, ".delegation", "foreign.json")
	artifact := filepath.Join(home, "Library", "LaunchAgents", LaunchAgentPeerName+".plist")
	originalRunner := runLaunchctl
	t.Cleanup(func() { runLaunchctl = originalRunner })
	targetPrints := 0
	runLaunchctl = func(args ...string) (userServiceCommandResult, error) {
		if args[0] == "print" && len(args) == 2 && args[1] == fmt.Sprintf("gui/%d", os.Geteuid()) {
			return userServiceCommandResult{}, nil
		}
		if args[0] != "print" {
			return userServiceCommandResult{}, nil
		}
		targetPrints++
		if targetPrints == 1 {
			return userServiceCommandResult{ExitCode: 113}, nil
		}
		return launchctlTestStatus(artifact, "running", 123, foreignInvocation), nil
	}
	result, err := Install(ServiceRolePeer, invocation)
	if err == nil || result.State != StateForeignConflict ||
		!errors.Is(err, errLaunchAgentIdentityChangedDuringStart) {
		t.Fatalf("Install() = %#v, %v", result, err)
	}
}

func TestDarwinNamedInstanceLifecycleTargetsNamedAgent(t *testing.T) {
	stubDarwinServiceReadiness(t, nil)
	home := t.TempDir()
	t.Setenv("HOME", home)
	invocation := testInvocation(
		ServiceRolePeer,
		"/opt/delegation/bin/delegation",
		filepath.Join(home, ".delegation", "config.json"),
	)
	invocation.InstanceID = "alpha-2"
	label := "com.github.ghostflying.delegation.alpha-2.peer"
	artifact := filepath.Join(home, "Library", "LaunchAgents", label+".plist")
	originalRunner := runLaunchctl
	t.Cleanup(func() { runLaunchctl = originalRunner })
	var calls [][]string
	targetPrints := 0
	runLaunchctl = func(args ...string) (userServiceCommandResult, error) {
		calls = append(calls, slices.Clone(args))
		if args[0] == "print" && len(args) == 2 && args[1] == fmt.Sprintf("gui/%d", os.Geteuid()) {
			return userServiceCommandResult{}, nil
		}
		if args[0] == "print" {
			targetPrints++
			if targetPrints > 1 {
				return launchctlTestStatus(artifact, "running", 123, invocation), nil
			}
			return userServiceCommandResult{ExitCode: 113}, nil
		}
		return userServiceCommandResult{}, nil
	}
	result, err := Install(ServiceRolePeer, invocation)
	if err != nil || result.State != StateActive || result.Artifact != artifact {
		t.Fatalf("Install() = %#v, %v", result, err)
	}
	target := fmt.Sprintf("gui/%d/%s", os.Geteuid(), label)
	for _, call := range calls {
		if slices.Contains(call, LaunchAgentPeerName) {
			t.Fatalf("named lifecycle targeted legacy label: %q", calls)
		}
		if (call[0] == "enable" || call[0] == "kickstart" || call[0] == "print" && len(call) == 2 &&
			call[1] != fmt.Sprintf("gui/%d", os.Geteuid())) && !slices.Contains(call, target) {
			t.Fatalf("named lifecycle omitted target %q: %q", target, call)
		}
		if call[0] == "bootstrap" && call[2] != artifact {
			t.Fatalf("named lifecycle bootstrapped %q, want %q", call[2], artifact)
		}
	}
}

func TestDarwinPrepareRejectsInvalidInstanceWithoutSideEffects(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	invocation := testInvocation(
		ServiceRolePeer,
		"/opt/delegation/bin/delegation",
		filepath.Join(home, ".delegation", "config.json"),
	)
	invocation.InstanceID = "unsafe/instance"
	if _, err := Prepare(ServiceRolePeer, invocation); err == nil {
		t.Fatal("Prepare() accepted invalid instance ID")
	}
	entries, err := os.ReadDir(home)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("invalid instance created files: %v", entries)
	}
}

func TestDarwinInstallAcceptsOnlyManagedLoadedPath(t *testing.T) {
	stubDarwinServiceReadiness(t, nil)
	home := t.TempDir()
	t.Setenv("HOME", home)
	invocation := testInvocation(
		ServiceRolePeer,
		"/opt/delegation/bin/delegation",
		filepath.Join(home, ".delegation", "config.json"),
	)
	originalRunner := runLaunchctl
	t.Cleanup(func() { runLaunchctl = originalRunner })
	foreign := false
	loaded := true
	bootedOut := false
	bootstrapped := false
	staleUnloadPrints := 0
	unloadPrints := 0
	runLaunchctl = func(args ...string) (userServiceCommandResult, error) {
		switch args[0] {
		case "bootout":
			loaded = false
			bootedOut = true
			staleUnloadPrints = 1
			return userServiceCommandResult{}, nil
		case "bootstrap":
			loaded = true
			bootstrapped = true
			return userServiceCommandResult{}, nil
		}
		if args[0] != "print" || len(args) != 2 || !strings.Contains(args[1], LaunchAgentPeerName) {
			return userServiceCommandResult{}, nil
		}
		if staleUnloadPrints > 0 {
			staleUnloadPrints--
			unloadPrints++
			path := filepath.Join(home, "Library", "LaunchAgents", LaunchAgentPeerName+".plist")
			return launchctlTestStatus(path, "running", 123, invocation), nil
		}
		if !loaded {
			unloadPrints++
			return userServiceCommandResult{ExitCode: 113}, nil
		}
		path := filepath.Join(home, "Library", "LaunchAgents", LaunchAgentPeerName+".plist")
		if foreign {
			path = "/tmp/foreign.plist"
		}
		return launchctlTestStatus(path, "running", 123, invocation), nil
	}
	result, err := Install(ServiceRolePeer, invocation)
	if err != nil || result.State != StateActive {
		t.Fatalf("Install() loaded managed path = %#v, %v", result, err)
	}
	if !bootedOut || !bootstrapped || unloadPrints != 2 {
		t.Fatalf(
			"loaded LaunchAgent was not reconciled: bootout=%v bootstrap=%v unloadPrints=%d",
			bootedOut,
			bootstrapped,
			unloadPrints,
		)
	}
	foreign = true
	loaded = true
	result, err = Install(ServiceRolePeer, invocation)
	if err == nil || result.State != StateForeignConflict {
		t.Fatalf("Install() loaded foreign path = %#v, %v", result, err)
	}
}

func TestDarwinInstallRejectsIdentityChangeDuringUnload(t *testing.T) {
	stubDarwinServiceReadiness(t, nil)
	home := t.TempDir()
	t.Setenv("HOME", home)
	invocation := testInvocation(
		ServiceRolePeer,
		"/opt/delegation/bin/delegation",
		filepath.Join(home, ".delegation", "config.json"),
	)
	originalRunner := runLaunchctl
	t.Cleanup(func() { runLaunchctl = originalRunner })
	targetPrints := 0
	runLaunchctl = func(args ...string) (userServiceCommandResult, error) {
		if args[0] == "print" && len(args) == 2 && strings.Contains(args[1], LaunchAgentPeerName) {
			targetPrints++
			path := filepath.Join(home, "Library", "LaunchAgents", LaunchAgentPeerName+".plist")
			if targetPrints > 1 {
				path = "/tmp/foreign.plist"
			}
			return launchctlTestStatus(path, "running", 123, invocation), nil
		}
		return userServiceCommandResult{}, nil
	}
	result, err := Install(ServiceRolePeer, invocation)
	if err == nil || result.State != StateForeignConflict ||
		!errors.Is(err, errLaunchAgentIdentityChangedDuringUnload) {
		t.Fatalf("Install() = %#v, %v", result, err)
	}
}

func TestDarwinInstallReportsPartialActivation(t *testing.T) {
	stubDarwinServiceReadiness(t, nil)
	home := t.TempDir()
	t.Setenv("HOME", home)
	originalRunner := runLaunchctl
	t.Cleanup(func() { runLaunchctl = originalRunner })
	runLaunchctl = func(args ...string) (userServiceCommandResult, error) {
		if args[0] == "print" && len(args) == 2 && strings.Contains(args[1], LaunchAgentPeerName) {
			return userServiceCommandResult{ExitCode: 113}, nil
		}
		if args[0] == "enable" {
			return userServiceCommandResult{}, errors.New("connection lost")
		}
		return userServiceCommandResult{}, nil
	}
	result, err := Install(ServiceRolePeer, testInvocation(
		ServiceRolePeer, "/opt/delegation/bin/delegation", filepath.Join(home, ".delegation", "config.json"),
	))
	if err == nil || result.State != StateIndeterminate {
		t.Fatalf("Install() = %#v, %v", result, err)
	}
}

func TestDarwinInstallReportsIdentityChangeAfterKickstart(t *testing.T) {
	stubDarwinServiceReadiness(t, nil)
	home := t.TempDir()
	t.Setenv("HOME", home)
	invocation := testInvocation(
		ServiceRolePeer,
		"/opt/delegation/bin/delegation",
		filepath.Join(home, ".delegation", "config.json"),
	)
	originalRunner := runLaunchctl
	t.Cleanup(func() { runLaunchctl = originalRunner })
	targetPrints := 0
	runLaunchctl = func(args ...string) (userServiceCommandResult, error) {
		if args[0] == "print" && len(args) == 2 && strings.Contains(args[1], LaunchAgentPeerName) {
			targetPrints++
			if targetPrints == 1 {
				return userServiceCommandResult{ExitCode: 113}, nil
			}
			return launchctlTestStatus("/tmp/foreign.plist", "running", 123, invocation), nil
		}
		return userServiceCommandResult{}, nil
	}
	result, err := Install(ServiceRolePeer, invocation)
	if err == nil || result.State != StateForeignConflict {
		t.Fatalf("Install() = %#v, %v", result, err)
	}
}

func TestDarwinInstallReconcilesLostBootstrapResponse(t *testing.T) {
	stubDarwinServiceReadiness(t, nil)
	home := t.TempDir()
	t.Setenv("HOME", home)
	invocation := testInvocation(
		ServiceRolePeer,
		"/opt/delegation/bin/delegation",
		filepath.Join(home, ".delegation", "config.json"),
	)
	originalRunner := runLaunchctl
	t.Cleanup(func() { runLaunchctl = originalRunner })
	targetPrints := 0
	runLaunchctl = func(args ...string) (userServiceCommandResult, error) {
		if args[0] == "print" && len(args) == 2 && strings.Contains(args[1], LaunchAgentPeerName) {
			targetPrints++
			if targetPrints == 1 {
				return userServiceCommandResult{ExitCode: 113}, nil
			}
			state := "waiting"
			if targetPrints > 2 {
				state = "running"
			}
			path := filepath.Join(home, "Library", "LaunchAgents", LaunchAgentPeerName+".plist")
			pid := 0
			if state == "running" {
				pid = 123
			}
			return launchctlTestStatus(path, state, pid, invocation), nil
		}
		if args[0] == "bootstrap" {
			return userServiceCommandResult{}, errors.New("connection lost")
		}
		return userServiceCommandResult{}, nil
	}
	result, err := Install(ServiceRolePeer, invocation)
	if err != nil || result.State != StateActive {
		t.Fatalf("Install() = %#v, %v", result, err)
	}
}

func TestDarwinInstallRejectsServiceThatNeverBecomesReady(t *testing.T) {
	readinessErr := errors.New("connector did not open its local bridge")
	stubDarwinServiceReadiness(t, readinessErr)
	home := t.TempDir()
	t.Setenv("HOME", home)
	invocation := testInvocation(
		ServiceRolePeer,
		"/opt/delegation/bin/delegation",
		filepath.Join(home, ".delegation", "config.json"),
	)
	originalRunner := runLaunchctl
	t.Cleanup(func() { runLaunchctl = originalRunner })
	targetPrints := 0
	runLaunchctl = func(args ...string) (userServiceCommandResult, error) {
		if args[0] == "print" && len(args) == 2 && strings.Contains(args[1], LaunchAgentPeerName) {
			targetPrints++
			if targetPrints == 1 {
				return userServiceCommandResult{ExitCode: 113}, nil
			}
			path := filepath.Join(home, "Library", "LaunchAgents", LaunchAgentPeerName+".plist")
			return launchctlTestStatus(path, "running", 123, invocation), nil
		}
		return userServiceCommandResult{}, nil
	}
	result, err := Install(ServiceRolePeer, invocation)
	if !errors.Is(err, readinessErr) || result.State != StateIndeterminate {
		t.Fatalf("Install() = %#v, %v", result, err)
	}
}

func TestDarwinInstallRejectsLoadedJobThatCannotBeUnloaded(t *testing.T) {
	stubDarwinServiceReadiness(t, nil)
	home := t.TempDir()
	t.Setenv("HOME", home)
	invocation := testInvocation(
		ServiceRolePeer,
		"/opt/delegation/bin/delegation",
		filepath.Join(home, ".delegation", "config.json"),
	)
	originalRunner := runLaunchctl
	t.Cleanup(func() { runLaunchctl = originalRunner })
	runLaunchctl = func(args ...string) (userServiceCommandResult, error) {
		if args[0] == "print" && len(args) == 2 && strings.Contains(args[1], LaunchAgentPeerName) {
			path := filepath.Join(home, "Library", "LaunchAgents", LaunchAgentPeerName+".plist")
			return launchctlTestStatus(path, "running", 123, invocation), nil
		}
		return userServiceCommandResult{}, nil
	}
	result, err := Install(ServiceRolePeer, invocation)
	if err == nil || result.State != StateIndeterminate || !strings.Contains(err.Error(), "remained loaded") {
		t.Fatalf("Install() = %#v, %v", result, err)
	}
}

func stubDarwinServiceReadiness(t *testing.T, err error) {
	t.Helper()
	original := waitForDarwinServiceReady
	waitForDarwinServiceReady = func(string) error { return err }
	t.Cleanup(func() { waitForDarwinServiceReady = original })
}

func stubLaunchAgentStartTiming(t *testing.T, timeout, poll time.Duration) {
	t.Helper()
	originalTimeout := launchAgentStartTimeout
	originalPoll := launchAgentStartPoll
	launchAgentStartTimeout = timeout
	launchAgentStartPoll = poll
	t.Cleanup(func() {
		launchAgentStartTimeout = originalTimeout
		launchAgentStartPoll = originalPoll
	})
}

func launchctlTestStatus(
	path string,
	state string,
	pid int,
	invocation Invocation,
) userServiceCommandResult {
	var output strings.Builder
	fmt.Fprintf(&output, "path = %s\nstate = %s\nprogram = %s\narguments = {\n", path, state, invocation.BinaryPath)
	for _, argument := range launchAgentArguments(invocation) {
		fmt.Fprintf(&output, "\t%s\n", argument)
	}
	output.WriteString("}\n")
	if pid > 0 {
		fmt.Fprintf(&output, "pid = %d\n", pid)
	}
	return userServiceCommandResult{Output: []byte(output.String())}
}

func launchctlTestStatusWithoutArguments(
	path string,
	state string,
	pid int,
	invocation Invocation,
) userServiceCommandResult {
	var output strings.Builder
	fmt.Fprintf(&output, "path = %s\nstate = %s\nprogram = %s\n", path, state, invocation.BinaryPath)
	if pid > 0 {
		fmt.Fprintf(&output, "pid = %d\n", pid)
	}
	return userServiceCommandResult{Output: []byte(output.String())}
}

func launchctlTestStatusWithoutProgram(
	path string,
	state string,
	pid int,
	invocation Invocation,
) userServiceCommandResult {
	var output strings.Builder
	fmt.Fprintf(&output, "path = %s\nstate = %s\narguments = {\n", path, state)
	for _, argument := range launchAgentArguments(invocation) {
		fmt.Fprintf(&output, "\t%s\n", argument)
	}
	output.WriteString("}\n")
	if pid > 0 {
		fmt.Fprintf(&output, "pid = %d\n", pid)
	}
	return userServiceCommandResult{Output: []byte(output.String())}
}

func launchctlTestStatusWithoutProgramOrArguments(
	path string,
	state string,
	pid int,
) userServiceCommandResult {
	var output strings.Builder
	fmt.Fprintf(&output, "path = %s\nstate = %s\n", path, state)
	if pid > 0 {
		fmt.Fprintf(&output, "pid = %d\n", pid)
	}
	return userServiceCommandResult{Output: []byte(output.String())}
}

func TestParseLaunchAgentStatusUsesTopLevelFields(t *testing.T) {
	result := userServiceCommandResult{Output: []byte(
		"gui/501/" + LaunchAgentPeerName + " = {\n" +
			"\tpath = /tmp/delegation.plist\n" +
			"\tstate = running\n" +
			"\tprogram = /opt/delegation/bin/delegation\n" +
			"\targuments = {\n" +
			"\t\t/opt/delegation/bin/delegation\n" +
			"\t\tservice\n" +
			"\t\trun\n" +
			"\t\t--config\n" +
			"\t\t/Users/test/config.json\n" +
			"\t}\n" +
			"\tendpoints = {\n" +
			"\t\tstate = active\n" +
			"\t}\n" +
			"\tpid = 123\n" +
			"}\n",
	)}
	status, err := parseLaunchAgentStatus(result)
	if err != nil {
		t.Fatal(err)
	}
	want := launchAgentStatus{
		Path:             "/tmp/delegation.plist",
		State:            "running",
		Program:          "/opt/delegation/bin/delegation",
		Arguments:        []string{"/opt/delegation/bin/delegation", "service", "run", "--config", "/Users/test/config.json"},
		ArgumentsPresent: true,
		PID:              123,
	}
	if status.Path != want.Path || status.State != want.State || status.Program != want.Program ||
		status.ArgumentsPresent != want.ArgumentsPresent || status.PID != want.PID ||
		!slices.Equal(status.Arguments, want.Arguments) {
		t.Fatalf("parseLaunchAgentStatus() = %#v, want %#v", status, want)
	}
}

func TestParseLaunchAgentStatusRejectsDuplicateTopLevelState(t *testing.T) {
	result := userServiceCommandResult{Output: []byte(
		"\tpath = /tmp/delegation.plist\n\tstate = running\n\tstate = waiting\n",
	)}
	if _, err := parseLaunchAgentStatus(result); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("parseLaunchAgentStatus() error = %v, want duplicate-field rejection", err)
	}
}

func TestDarwinServiceRejectsRelativeHome(t *testing.T) {
	t.Setenv("HOME", "relative")
	if _, err := Prepare(ServiceRolePeer, testInvocation(
		ServiceRolePeer, "/opt/delegation", "/Users/test/config.json",
	)); err == nil {
		t.Fatal("Prepare() accepted relative HOME")
	}
}

func TestDarwinLaunchAgentRoundTrip(t *testing.T) {
	if os.Getenv("DELEGATION_DARWIN_INTEGRATION") != "1" {
		t.Skip("set DELEGATION_DARWIN_INTEGRATION=1 to exercise the real LaunchAgent lifecycle")
	}
	binaryPath := os.Getenv("DELEGATION_DARWIN_BINARY")
	if !filepath.IsAbs(binaryPath) {
		t.Fatal("DELEGATION_DARWIN_BINARY must be an absolute path")
	}
	if info, err := os.Stat(binaryPath); err != nil || !info.Mode().IsRegular() {
		t.Fatalf("integration binary is unavailable: %v", err)
	}
	domain := fmt.Sprintf("gui/%d", os.Geteuid())
	target := domain + "/" + LaunchAgentPeerName
	artifact, err := darwinServicePath(ServiceRolePeer, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, loaded, err := printLaunchAgent(target); err != nil {
		t.Fatal(err)
	} else if loaded {
		t.Fatal("refusing to replace a pre-existing Delegation LaunchAgent")
	}
	if _, err := os.Lstat(artifact); err == nil {
		t.Fatalf("refusing to replace pre-existing LaunchAgent artifact %s", artifact)
	} else if !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	cleanupNeeded := false
	t.Cleanup(func() {
		if cleanupNeeded {
			if err := cleanupDarwinIntegration(target, artifact); err != nil {
				t.Errorf("clean up LaunchAgent integration fixture: %v", err)
			}
		}
	})

	configPath := filepath.Join(t.TempDir(), "peer.json")
	codexHome := filepath.Join(filepath.Dir(configPath), "codex")
	workspaceRoot := filepath.Join(filepath.Dir(configPath), "workspaces")
	for _, path := range []string{codexHome, workspaceRoot} {
		if err := delegationconfig.PreparePrivateDirectory(path); err != nil {
			t.Fatal(err)
		}
	}
	cfg := delegationconfig.Config{
		SchemaVersion: delegationconfig.CurrentSchemaVersion,
		Role:          delegationconfig.RolePeer,
		ControllerID:  "123e4567-e89b-42d3-a456-426614174780",
		DeviceID:      "123e4567-e89b-42d3-a456-426614174781",
		DeviceName:    "darwin-launchagent-integration",
		Broker: delegationconfig.BrokerConfig{
			URL:  "ws://127.0.0.1:9",
			Auth: delegationconfig.AuthConfig{Mode: delegationconfig.AuthModeNone},
		},
		Peer: delegationconfig.PeerConfig{
			CodexBinary: binaryPath, GitBinary: binaryPath,
			CodexHome: codexHome, WorkspaceRoot: workspaceRoot,
			StateFile:      filepath.Join(filepath.Dir(configPath), "state", "peer.sqlite3"),
			MaxWorkerSlots: 1,
		},
	}
	if err := delegationconfig.WriteNew(configPath, cfg); err != nil {
		t.Fatal(err)
	}
	writeIntegrationProviderEnvironment(t, configPath)
	cleanupNeeded = true
	for attempt := 1; attempt <= 2; attempt++ {
		result, err := Install(ServiceRolePeer, testInvocation(ServiceRolePeer, binaryPath, configPath))
		if err != nil || result.State != StateActive || result.Artifact != artifact {
			t.Fatalf("Install() attempt %d = %#v, %v", attempt, result, err)
		}
	}
	status, loaded, err := printLaunchAgent(target)
	if err != nil || !loaded || status.State != "running" || filepath.Clean(status.Path) != filepath.Clean(artifact) {
		t.Fatalf("active LaunchAgent = %#v, loaded %v, error %v", status, loaded, err)
	}

	if err := cleanupDarwinIntegration(target, artifact); err != nil {
		t.Fatal(err)
	}
	cleanupNeeded = false
}

func cleanupDarwinIntegration(target, artifact string) error {
	status, loaded, err := printLaunchAgent(target)
	if err != nil {
		return err
	}
	if loaded && filepath.Clean(status.Path) != filepath.Clean(artifact) {
		return errors.New("refusing to modify a LaunchAgent from an unexpected path")
	}

	disabled, disableErr := runLaunchctl("disable", target)
	var cleanupErr error
	if disableErr != nil || disabled.ExitCode != 0 {
		cleanupErr = errors.Join(disableErr, commandFailure("disable LaunchAgent fixture", disabled))
	}

	status, loaded, err = printLaunchAgent(target)
	if err != nil {
		return errors.Join(cleanupErr, err)
	}
	if loaded && filepath.Clean(status.Path) != filepath.Clean(artifact) {
		return errors.Join(cleanupErr, errors.New("refusing to unload a LaunchAgent from an unexpected path"))
	}
	if loaded {
		bootedOut, bootoutErr := runLaunchctl("bootout", target)
		if bootoutErr != nil || bootedOut.ExitCode != 0 {
			cleanupErr = errors.Join(
				cleanupErr,
				bootoutErr,
				commandFailure("unload LaunchAgent fixture", bootedOut),
			)
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		status, loaded, err = printLaunchAgent(target)
		if err != nil {
			return errors.Join(cleanupErr, err)
		}
		if !loaded {
			break
		}
		if filepath.Clean(status.Path) != filepath.Clean(artifact) {
			return errors.Join(cleanupErr, errors.New("LaunchAgent identity changed during integration cleanup"))
		}
		if time.Now().After(deadline) {
			return errors.Join(cleanupErr, errors.New("LaunchAgent remained loaded after integration cleanup"))
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err := os.Remove(artifact); err != nil && !errors.Is(err, os.ErrNotExist) {
		cleanupErr = errors.Join(cleanupErr, fmt.Errorf("remove LaunchAgent artifact: %w", err))
	}
	return cleanupErr
}
