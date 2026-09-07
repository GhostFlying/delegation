//go:build linux

package userservice

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/GhostFlying/delegation/internal/identity"
)

const testUpgradeTransactionID = "123e4567-e89b-42d3-a456-426614174988"

func TestLinuxUpgradeActivatorLifecycleUsesExactLinkedUnit(t *testing.T) {
	plan, linkPath := prepareLinuxActivatorTest(t, testUpgradeTransactionID)
	if plan.Kind != KindSystemd || plan.Name != filepath.Base(plan.DefinitionPath) ||
		!strings.Contains(string(plan.Definition), "Restart=on-failure") ||
		!strings.Contains(string(plan.Definition), "service upgrade-activate") {
		t.Fatalf("activator plan = %#v\n%s", plan, plan.Definition)
	}

	originalRunner := runSystemctl
	t.Cleanup(func() { runSystemctl = originalRunner })
	loaded := false
	var calls [][]string
	runSystemctl = func(args ...string) (userServiceCommandResult, error) {
		calls = append(calls, slices.Clone(args))
		switch {
		case slices.Contains(args, "link"):
			if err := os.MkdirAll(filepath.Dir(linkPath), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(plan.DefinitionPath, linkPath); err != nil {
				t.Fatal(err)
			}
			loaded = true
		case slices.Contains(args, "disable"):
			if err := os.Remove(linkPath); err != nil {
				t.Fatal(err)
			}
			loaded = false
		case slices.Contains(args, "show"):
			return linuxActivatorIdentityResult(loaded, linkPath, "", "linked"), nil
		}
		return userServiceCommandResult{}, nil
	}
	if err := InstallUpgradeActivator(context.Background(), plan); err != nil {
		t.Fatal(err)
	}
	if err := LaunchUpgradeActivator(context.Background(), plan); err != nil {
		t.Fatal(err)
	}
	if err := RemoveUpgradeActivator(context.Background(), plan); err != nil {
		t.Fatal(err)
	}
	for _, action := range []string{"link", "start", "disable"} {
		if !slices.ContainsFunc(calls, func(call []string) bool { return slices.Contains(call, action) }) {
			t.Fatalf("systemctl calls omit %s: %q", action, calls)
		}
	}
}

func TestLinuxUpgradeActivatorInstallRecoversLinkBeforeReload(t *testing.T) {
	plan, linkPath := prepareLinuxActivatorTest(t, testUpgradeTransactionID)
	if err := os.Symlink(plan.DefinitionPath, linkPath); err != nil {
		t.Fatal(err)
	}
	originalRunner := runSystemctl
	t.Cleanup(func() { runSystemctl = originalRunner })
	loaded := false
	var reloads int
	runSystemctl = func(args ...string) (userServiceCommandResult, error) {
		switch {
		case slices.Contains(args, "link"):
			return userServiceCommandResult{ExitCode: 1, Output: []byte("link already exists")}, nil
		case slices.Contains(args, "daemon-reload"):
			reloads++
			loaded = true
		case slices.Contains(args, "show"):
			return linuxActivatorIdentityResult(loaded, linkPath, "", "linked"), nil
		}
		return userServiceCommandResult{}, nil
	}
	if err := InstallUpgradeActivator(context.Background(), plan); err != nil {
		t.Fatalf("InstallUpgradeActivator() = %v", err)
	}
	if reloads != 1 {
		t.Fatalf("daemon reloads = %d, want 1", reloads)
	}
}

func TestLinuxUpgradeActivatorLaunchRejectsUnreloadedLink(t *testing.T) {
	plan, linkPath := prepareLinuxActivatorTest(t, testUpgradeTransactionID)
	if err := os.Symlink(plan.DefinitionPath, linkPath); err != nil {
		t.Fatal(err)
	}
	originalRunner := runSystemctl
	t.Cleanup(func() { runSystemctl = originalRunner })
	started := false
	runSystemctl = func(args ...string) (userServiceCommandResult, error) {
		if slices.Contains(args, "show") {
			return linuxActivatorIdentityResult(false, linkPath, "", "linked"), nil
		}
		if slices.Contains(args, "start") {
			started = true
		}
		return userServiceCommandResult{}, nil
	}
	if err := LaunchUpgradeActivator(context.Background(), plan); err == nil {
		t.Fatal("LaunchUpgradeActivator accepted an unreloaded link")
	}
	if started {
		t.Fatal("LaunchUpgradeActivator started an unreloaded link")
	}
}

func TestLinuxUpgradeActivatorRemoveRecoversLinkBeforeReload(t *testing.T) {
	plan, linkPath := prepareLinuxActivatorTest(t, testUpgradeTransactionID)
	if err := os.Symlink(plan.DefinitionPath, linkPath); err != nil {
		t.Fatal(err)
	}
	originalRunner := runSystemctl
	t.Cleanup(func() { runSystemctl = originalRunner })
	loaded := false
	disabled := false
	runSystemctl = func(args ...string) (userServiceCommandResult, error) {
		switch {
		case slices.Contains(args, "show"):
			return linuxActivatorIdentityResult(loaded, linkPath, "", "linked"), nil
		case slices.Contains(args, "daemon-reload"):
			if disabled {
				loaded = false
			} else {
				loaded = true
			}
		case slices.Contains(args, "disable"):
			if err := os.Remove(linkPath); err != nil {
				t.Fatal(err)
			}
			disabled = true
		}
		return userServiceCommandResult{}, nil
	}
	if err := RemoveUpgradeActivator(context.Background(), plan); err != nil {
		t.Fatalf("RemoveUpgradeActivator() = %v", err)
	}
	if !disabled {
		t.Fatal("RemoveUpgradeActivator did not disable the recovered link")
	}
}

func TestLinuxUpgradeActivatorRemoveRecoversDisableBeforeReload(t *testing.T) {
	plan, linkPath := prepareLinuxActivatorTest(t, testUpgradeTransactionID)
	originalRunner := runSystemctl
	t.Cleanup(func() { runSystemctl = originalRunner })
	loaded := true
	reloads := 0
	runSystemctl = func(args ...string) (userServiceCommandResult, error) {
		switch {
		case slices.Contains(args, "show"):
			return linuxActivatorIdentityResult(loaded, linkPath, "", "linked"), nil
		case slices.Contains(args, "daemon-reload"):
			reloads++
			loaded = false
		case slices.Contains(args, "disable"):
			t.Fatal("RemoveUpgradeActivator repeated disable after the link was already absent")
		}
		return userServiceCommandResult{}, nil
	}
	if err := RemoveUpgradeActivator(context.Background(), plan); err != nil {
		t.Fatalf("RemoveUpgradeActivator() = %v", err)
	}
	if reloads == 0 {
		t.Fatal("RemoveUpgradeActivator did not reload stale systemd state")
	}
}

func TestLinuxUpgradeActivatorRejectsForeignIdentityBeforeLaunch(t *testing.T) {
	tests := []struct {
		name           string
		fragment       func(string) string
		dropIns        string
		unitFileState  string
		linkTarget     string
		linkMissing    bool
		linkRegular    bool
		definitionMode os.FileMode
		foreignOwner   bool
	}{
		{name: "shadowed fragment", fragment: func(string) string { return "/tmp/foreign.service" }, unitFileState: "linked"},
		{name: "drop-in override", dropIns: "/tmp/foreign.conf", unitFileState: "linked"},
		{name: "enabled regular unit", unitFileState: "enabled"},
		{name: "missing canonical link", unitFileState: "linked", linkMissing: true},
		{name: "regular canonical path", unitFileState: "linked", linkRegular: true},
		{name: "relative link target", unitFileState: "linked", linkTarget: filepath.Base("/tmp/foreign.service")},
		{name: "foreign link target", unitFileState: "linked", linkTarget: "/tmp/foreign.service"},
		{name: "broad definition mode", unitFileState: "linked", definitionMode: 0o644},
		{name: "foreign owner", unitFileState: "linked", foreignOwner: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan, linkPath := prepareLinuxActivatorTest(t, testUpgradeTransactionID)
			target := plan.DefinitionPath
			if test.linkTarget != "" {
				target = test.linkTarget
			}
			if test.linkRegular {
				if err := os.WriteFile(linkPath, plan.Definition, 0o600); err != nil {
					t.Fatal(err)
				}
			} else if !test.linkMissing {
				if err := os.Symlink(target, linkPath); err != nil {
					t.Fatal(err)
				}
			}
			if test.definitionMode != 0 {
				if err := os.Chmod(plan.DefinitionPath, test.definitionMode); err != nil {
					t.Fatal(err)
				}
			}
			originalOwner := linuxActivatorOwnedByCurrentUser
			if test.foreignOwner {
				linuxActivatorOwnedByCurrentUser = func(os.FileInfo) bool { return false }
			}
			t.Cleanup(func() { linuxActivatorOwnedByCurrentUser = originalOwner })
			originalRunner := runSystemctl
			t.Cleanup(func() { runSystemctl = originalRunner })
			started := false
			runSystemctl = func(args ...string) (userServiceCommandResult, error) {
				if slices.Contains(args, "show") {
					fragment := linkPath
					if test.fragment != nil {
						fragment = test.fragment(linkPath)
					}
					return linuxActivatorIdentityResult(true, fragment, test.dropIns, test.unitFileState), nil
				}
				if slices.Contains(args, "start") {
					started = true
				}
				return userServiceCommandResult{}, nil
			}
			if err := LaunchUpgradeActivator(context.Background(), plan); err == nil {
				t.Fatal("LaunchUpgradeActivator accepted a foreign activator identity")
			}
			if started {
				t.Fatal("LaunchUpgradeActivator started a foreign activator")
			}
		})
	}
}

func TestLinuxUpgradeActivatorRemoveAcceptsDisableErrorAfterLinkDisappears(t *testing.T) {
	plan, linkPath := prepareLinuxActivatorTest(t, testUpgradeTransactionID)
	if err := os.Symlink(plan.DefinitionPath, linkPath); err != nil {
		t.Fatal(err)
	}
	originalRunner := runSystemctl
	t.Cleanup(func() { runSystemctl = originalRunner })
	loaded := true
	reloaded := false
	runSystemctl = func(args ...string) (userServiceCommandResult, error) {
		switch {
		case slices.Contains(args, "show"):
			return linuxActivatorIdentityResult(loaded, linkPath, "", "linked"), nil
		case slices.Contains(args, "disable"):
			if err := os.Remove(linkPath); err != nil {
				t.Fatal(err)
			}
			return userServiceCommandResult{ExitCode: 1, Output: []byte("link already absent")}, nil
		case slices.Contains(args, "daemon-reload"):
			loaded = false
			reloaded = true
			return userServiceCommandResult{}, nil
		default:
			return userServiceCommandResult{}, nil
		}
	}
	if err := RemoveUpgradeActivator(context.Background(), plan); err != nil {
		t.Fatalf("RemoveUpgradeActivator() = %v", err)
	}
	if !reloaded {
		t.Fatal("RemoveUpgradeActivator did not reload the user manager")
	}
}

func TestLinuxUpgradeActivatorRemoveRejectsSuccessfulDisableThatLeavesLink(t *testing.T) {
	plan, linkPath := prepareLinuxActivatorTest(t, testUpgradeTransactionID)
	if err := os.Symlink(plan.DefinitionPath, linkPath); err != nil {
		t.Fatal(err)
	}
	originalRunner := runSystemctl
	t.Cleanup(func() { runSystemctl = originalRunner })
	loaded := true
	runSystemctl = func(args ...string) (userServiceCommandResult, error) {
		switch {
		case slices.Contains(args, "show"):
			return linuxActivatorIdentityResult(loaded, linkPath, "", "linked"), nil
		case slices.Contains(args, "disable"):
			return userServiceCommandResult{}, nil
		case slices.Contains(args, "daemon-reload"):
			loaded = false
		}
		return userServiceCommandResult{}, nil
	}
	if err := RemoveUpgradeActivator(context.Background(), plan); err == nil ||
		!strings.Contains(err.Error(), "link remained after removal") {
		t.Fatalf("RemoveUpgradeActivator() error = %v", err)
	}
}

func TestLinuxUpgradeActivatorRejectsRelativeXDGConfigHome(t *testing.T) {
	plan, linkPath := prepareLinuxActivatorTest(t, testUpgradeTransactionID)
	if err := os.Symlink(plan.DefinitionPath, linkPath); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XDG_CONFIG_HOME", "relative")
	originalRunner := runSystemctl
	t.Cleanup(func() { runSystemctl = originalRunner })
	started := false
	runSystemctl = func(args ...string) (userServiceCommandResult, error) {
		if slices.Contains(args, "show") {
			return linuxActivatorIdentityResult(true, linkPath, "", "linked"), nil
		}
		if slices.Contains(args, "start") {
			started = true
		}
		return userServiceCommandResult{}, nil
	}
	if err := LaunchUpgradeActivator(context.Background(), plan); err == nil ||
		!strings.Contains(err.Error(), "XDG_CONFIG_HOME must be absolute") {
		t.Fatalf("LaunchUpgradeActivator() error = %v", err)
	}
	if started {
		t.Fatal("LaunchUpgradeActivator started with relative XDG_CONFIG_HOME")
	}
}

func TestLinuxUpgradeActivatorRefusesToRemoveForeignUnit(t *testing.T) {
	plan, linkPath := prepareLinuxActivatorTest(t, testUpgradeTransactionID)
	if err := os.Symlink("/tmp/foreign.service", linkPath); err != nil {
		t.Fatal(err)
	}
	originalRunner := runSystemctl
	t.Cleanup(func() { runSystemctl = originalRunner })
	mutated := false
	runSystemctl = func(args ...string) (userServiceCommandResult, error) {
		if slices.Contains(args, "show") {
			return linuxActivatorIdentityResult(true, linkPath, "", "linked"), nil
		}
		mutated = true
		return userServiceCommandResult{}, nil
	}
	if err := RemoveUpgradeActivator(context.Background(), plan); err == nil {
		t.Fatal("RemoveUpgradeActivator accepted a foreign unit")
	}
	if mutated {
		t.Fatal("RemoveUpgradeActivator mutated a foreign unit")
	}
}

func TestLinuxUpgradeActivatorRealSystemdLinkRoundTrip(t *testing.T) {
	if os.Getenv("DELEGATION_LINUX_SYSTEMD_INTEGRATION") != "1" {
		t.Skip("set DELEGATION_LINUX_SYSTEMD_INTEGRATION=1 to use the real user manager")
	}
	transactionID, err := identity.NewID()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("XDG_CONFIG_HOME", "")
	root := t.TempDir()
	definition := filepath.Join(root, "delegation-upgrade-"+transactionID+".service")
	plan, err := PrepareUpgradeActivator(
		"/opt/delegation/0.2.0/delegation", root, transactionID, definition,
		strconv.Itoa(os.Geteuid()),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(definition, plan.Definition, 0o600); err != nil {
		t.Fatal(err)
	}
	linkPath, err := linuxUserSystemdPath(plan.Name)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = runSystemctl("--user", "--no-ask-password", "disable", plan.Name)
		_, _ = runSystemctl("--user", "--no-ask-password", "daemon-reload")
	})
	if _, err := os.Lstat(linkPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("activator link was present before the test: %v", err)
	}
	if err := InstallUpgradeActivator(context.Background(), plan); err != nil {
		t.Fatal(err)
	}
	if target, err := os.Readlink(linkPath); err != nil || target != plan.DefinitionPath {
		t.Fatalf("systemd link target = %q, %v; want %q", target, err, plan.DefinitionPath)
	}
	if err := RemoveUpgradeActivator(context.Background(), plan); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(linkPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("activator link remained after removal: %v", err)
	}
}

func prepareLinuxActivatorTest(t *testing.T, transactionID string) (UpgradeActivatorPlan, string) {
	t.Helper()
	root := t.TempDir()
	configHome := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", configHome)
	definition := filepath.Join(root, "delegation-upgrade-"+transactionID+".service")
	plan, err := PrepareUpgradeActivator(
		"/opt/delegation/0.2.0/delegation", root, transactionID, definition,
		strconv.Itoa(os.Geteuid()),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(definition, plan.Definition, 0o600); err != nil {
		t.Fatal(err)
	}
	linkPath, err := linuxUserSystemdPath(plan.Name)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(linkPath), 0o700); err != nil {
		t.Fatal(err)
	}
	return plan, linkPath
}

func linuxActivatorIdentityResult(
	loaded bool, fragment, dropIns, unitFileState string,
) userServiceCommandResult {
	loadState := "not-found"
	if loaded {
		loadState = "loaded"
	} else {
		fragment = ""
		dropIns = ""
		unitFileState = ""
	}
	return userServiceCommandResult{Output: []byte(
		"LoadState=" + loadState + "\n" +
			"FragmentPath=" + fragment + "\n" +
			"DropInPaths=" + dropIns + "\n" +
			"UnitFileState=" + unitFileState + "\n",
	)}
}
