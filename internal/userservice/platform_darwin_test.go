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
	result, err := Prepare("/opt/delegation/bin/delegation", filepath.Join(home, ".delegation", "config.json"))
	if err != nil || result.State != StatePrepared || result.Kind != KindLaunchAgent {
		t.Fatalf("Install() = %#v, %v", result, err)
	}
	wantPath := filepath.Join(home, "Library", "LaunchAgents", LaunchAgentName+".plist")
	if result.Artifact != wantPath {
		t.Fatalf("artifact = %q, want %q", result.Artifact, wantPath)
	}
}

func TestDarwinInstallBootstrapsEnablesAndStartsService(t *testing.T) {
	stubDarwinServiceReadiness(t, nil)
	home := t.TempDir()
	t.Setenv("HOME", home)
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
				path := filepath.Join(home, "Library", "LaunchAgents", LaunchAgentName+".plist")
				return launchctlTestStatus(path, "running"), nil
			}
			return userServiceCommandResult{ExitCode: 113}, nil
		}
		return userServiceCommandResult{}, nil
	}
	result, err := Install("/opt/delegation/bin/delegation", filepath.Join(home, ".delegation", "config.json"))
	if err != nil || result.State != StateActive {
		t.Fatalf("Install() = %#v, %v", result, err)
	}
	domain := fmt.Sprintf("gui/%d", os.Geteuid())
	target := domain + "/" + LaunchAgentName
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

func TestDarwinInstallAcceptsOnlyManagedLoadedPath(t *testing.T) {
	stubDarwinServiceReadiness(t, nil)
	home := t.TempDir()
	t.Setenv("HOME", home)
	originalRunner := runLaunchctl
	t.Cleanup(func() { runLaunchctl = originalRunner })
	foreign := false
	loaded := true
	bootedOut := false
	bootstrapped := false
	runLaunchctl = func(args ...string) (userServiceCommandResult, error) {
		switch args[0] {
		case "bootout":
			loaded = false
			bootedOut = true
			return userServiceCommandResult{}, nil
		case "bootstrap":
			loaded = true
			bootstrapped = true
			return userServiceCommandResult{}, nil
		}
		if args[0] != "print" || len(args) != 2 || !strings.Contains(args[1], LaunchAgentName) {
			return userServiceCommandResult{}, nil
		}
		if !loaded {
			return userServiceCommandResult{ExitCode: 113}, nil
		}
		path := filepath.Join(home, "Library", "LaunchAgents", LaunchAgentName+".plist")
		if foreign {
			path = "/tmp/foreign.plist"
		}
		return launchctlTestStatus(path, "running"), nil
	}
	result, err := Install("/opt/delegation/bin/delegation", filepath.Join(home, ".delegation", "config.json"))
	if err != nil || result.State != StateActive {
		t.Fatalf("Install() loaded managed path = %#v, %v", result, err)
	}
	if !bootedOut || !bootstrapped {
		t.Fatalf("loaded LaunchAgent was not reloaded: bootout=%v bootstrap=%v", bootedOut, bootstrapped)
	}
	foreign = true
	loaded = true
	result, err = Install("/opt/delegation/bin/delegation", filepath.Join(home, ".delegation", "config.json"))
	if err == nil || result.State != StateForeignConflict {
		t.Fatalf("Install() loaded foreign path = %#v, %v", result, err)
	}
}

func TestDarwinInstallReportsPartialActivation(t *testing.T) {
	stubDarwinServiceReadiness(t, nil)
	home := t.TempDir()
	t.Setenv("HOME", home)
	originalRunner := runLaunchctl
	t.Cleanup(func() { runLaunchctl = originalRunner })
	runLaunchctl = func(args ...string) (userServiceCommandResult, error) {
		if args[0] == "print" && len(args) == 2 && strings.Contains(args[1], LaunchAgentName) {
			return userServiceCommandResult{ExitCode: 113}, nil
		}
		if args[0] == "enable" {
			return userServiceCommandResult{}, errors.New("connection lost")
		}
		return userServiceCommandResult{}, nil
	}
	result, err := Install("/opt/delegation/bin/delegation", filepath.Join(home, ".delegation", "config.json"))
	if err == nil || result.State != StateIndeterminate {
		t.Fatalf("Install() = %#v, %v", result, err)
	}
}

func TestDarwinInstallReportsIdentityChangeAfterKickstart(t *testing.T) {
	stubDarwinServiceReadiness(t, nil)
	home := t.TempDir()
	t.Setenv("HOME", home)
	originalRunner := runLaunchctl
	t.Cleanup(func() { runLaunchctl = originalRunner })
	targetPrints := 0
	runLaunchctl = func(args ...string) (userServiceCommandResult, error) {
		if args[0] == "print" && len(args) == 2 && strings.Contains(args[1], LaunchAgentName) {
			targetPrints++
			if targetPrints == 1 {
				return userServiceCommandResult{ExitCode: 113}, nil
			}
			return launchctlTestStatus("/tmp/foreign.plist", "running"), nil
		}
		return userServiceCommandResult{}, nil
	}
	result, err := Install("/opt/delegation/bin/delegation", filepath.Join(home, ".delegation", "config.json"))
	if err == nil || result.State != StateForeignConflict {
		t.Fatalf("Install() = %#v, %v", result, err)
	}
}

func TestDarwinInstallReconcilesLostBootstrapResponse(t *testing.T) {
	stubDarwinServiceReadiness(t, nil)
	home := t.TempDir()
	t.Setenv("HOME", home)
	originalRunner := runLaunchctl
	t.Cleanup(func() { runLaunchctl = originalRunner })
	targetPrints := 0
	runLaunchctl = func(args ...string) (userServiceCommandResult, error) {
		if args[0] == "print" && len(args) == 2 && strings.Contains(args[1], LaunchAgentName) {
			targetPrints++
			if targetPrints == 1 {
				return userServiceCommandResult{ExitCode: 113}, nil
			}
			state := "waiting"
			if targetPrints > 2 {
				state = "running"
			}
			path := filepath.Join(home, "Library", "LaunchAgents", LaunchAgentName+".plist")
			return launchctlTestStatus(path, state), nil
		}
		if args[0] == "bootstrap" {
			return userServiceCommandResult{}, errors.New("connection lost")
		}
		return userServiceCommandResult{}, nil
	}
	result, err := Install("/opt/delegation/bin/delegation", filepath.Join(home, ".delegation", "config.json"))
	if err != nil || result.State != StateActive {
		t.Fatalf("Install() = %#v, %v", result, err)
	}
}

func TestDarwinInstallRejectsServiceThatNeverBecomesReady(t *testing.T) {
	readinessErr := errors.New("connector did not open its local bridge")
	stubDarwinServiceReadiness(t, readinessErr)
	home := t.TempDir()
	t.Setenv("HOME", home)
	originalRunner := runLaunchctl
	t.Cleanup(func() { runLaunchctl = originalRunner })
	targetPrints := 0
	runLaunchctl = func(args ...string) (userServiceCommandResult, error) {
		if args[0] == "print" && len(args) == 2 && strings.Contains(args[1], LaunchAgentName) {
			targetPrints++
			if targetPrints == 1 {
				return userServiceCommandResult{ExitCode: 113}, nil
			}
			path := filepath.Join(home, "Library", "LaunchAgents", LaunchAgentName+".plist")
			return launchctlTestStatus(path, "running"), nil
		}
		return userServiceCommandResult{}, nil
	}
	result, err := Install("/opt/delegation/bin/delegation", filepath.Join(home, ".delegation", "config.json"))
	if !errors.Is(err, readinessErr) || result.State != StateIndeterminate {
		t.Fatalf("Install() = %#v, %v", result, err)
	}
}

func TestDarwinInstallRejectsLoadedJobThatCannotBeUnloaded(t *testing.T) {
	stubDarwinServiceReadiness(t, nil)
	home := t.TempDir()
	t.Setenv("HOME", home)
	originalRunner := runLaunchctl
	t.Cleanup(func() { runLaunchctl = originalRunner })
	runLaunchctl = func(args ...string) (userServiceCommandResult, error) {
		if args[0] == "print" && len(args) == 2 && strings.Contains(args[1], LaunchAgentName) {
			path := filepath.Join(home, "Library", "LaunchAgents", LaunchAgentName+".plist")
			return launchctlTestStatus(path, "running"), nil
		}
		return userServiceCommandResult{}, nil
	}
	result, err := Install("/opt/delegation/bin/delegation", filepath.Join(home, ".delegation", "config.json"))
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

func launchctlTestStatus(path, state string) userServiceCommandResult {
	return userServiceCommandResult{Output: []byte("path = " + path + "\nstate = " + state + "\n")}
}

func TestParseLaunchAgentStatusUsesTopLevelFields(t *testing.T) {
	result := userServiceCommandResult{Output: []byte(
		"gui/501/" + LaunchAgentName + " = {\n" +
			"\tpath = /tmp/delegation.plist\n" +
			"\tstate = running\n" +
			"\tendpoints = {\n" +
			"\t\tstate = active\n" +
			"\t}\n" +
			"}\n",
	)}
	status, err := parseLaunchAgentStatus(result)
	if err != nil {
		t.Fatal(err)
	}
	want := launchAgentStatus{Path: "/tmp/delegation.plist", State: "running"}
	if status != want {
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
	if _, err := Prepare("/opt/delegation", "/Users/test/config.json"); err == nil {
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
	target := domain + "/" + LaunchAgentName
	artifact, err := darwinServicePath()
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

	configPath := filepath.Join(t.TempDir(), "config.json")
	cfg := delegationconfig.Config{
		SchemaVersion: delegationconfig.CurrentSchemaVersion,
		Role:          delegationconfig.RoleController,
		ControllerID:  "123e4567-e89b-42d3-a456-426614174780",
		DeviceID:      "123e4567-e89b-42d3-a456-426614174781",
		DeviceName:    "darwin-launchagent-integration",
		Broker: delegationconfig.BrokerConfig{
			URL:  "ws://127.0.0.1:9",
			Auth: delegationconfig.AuthConfig{Mode: delegationconfig.AuthModeNone},
		},
	}
	if err := delegationconfig.WriteNew(configPath, cfg); err != nil {
		t.Fatal(err)
	}
	cleanupNeeded = true
	for attempt := 1; attempt <= 2; attempt++ {
		result, err := Install(binaryPath, configPath)
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
