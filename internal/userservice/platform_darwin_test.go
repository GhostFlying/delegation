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
	home := t.TempDir()
	t.Setenv("HOME", home)
	originalRunner := runLaunchctl
	t.Cleanup(func() { runLaunchctl = originalRunner })
	foreign := false
	runLaunchctl = func(args ...string) (userServiceCommandResult, error) {
		if args[0] != "print" || len(args) != 2 || !strings.Contains(args[1], LaunchAgentName) {
			return userServiceCommandResult{}, nil
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
	foreign = true
	result, err = Install("/opt/delegation/bin/delegation", filepath.Join(home, ".delegation", "config.json"))
	if err == nil || result.State != StatePrepared {
		t.Fatalf("Install() loaded foreign path = %#v, %v", result, err)
	}
}

func TestDarwinInstallReportsPartialActivation(t *testing.T) {
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

func TestDarwinInstallReconcilesLostBootstrapResponse(t *testing.T) {
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

func launchctlTestStatus(path, state string) userServiceCommandResult {
	return userServiceCommandResult{Output: []byte("path = " + path + "\nstate = " + state + "\n")}
}

func TestDarwinServiceRejectsRelativeHome(t *testing.T) {
	t.Setenv("HOME", "relative")
	if _, err := Prepare("/opt/delegation", "/Users/test/config.json"); err == nil {
		t.Fatal("Prepare() accepted relative HOME")
	}
}
