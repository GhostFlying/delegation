//go:build linux

package userservice

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

const testUpgradeTransactionID = "123e4567-e89b-42d3-a456-426614174988"

func TestLinuxUpgradeActivatorLifecycleUsesExactLinkedUnit(t *testing.T) {
	root := t.TempDir()
	definition := filepath.Join(root, "delegation-upgrade-"+testUpgradeTransactionID+".service")
	plan, err := PrepareUpgradeActivator(
		"/opt/delegation/0.2.0/delegation", root, testUpgradeTransactionID, definition,
		strconv.Itoa(os.Geteuid()),
	)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Kind != KindSystemd || plan.Name != filepath.Base(definition) ||
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
			loaded = true
		case slices.Contains(args, "revert"):
			loaded = false
		case slices.Contains(args, "show"):
			if !loaded {
				return userServiceCommandResult{Output: []byte("LoadState=not-found\nFragmentPath=\nDropInPaths=\n")}, nil
			}
			return userServiceCommandResult{Output: []byte(
				"LoadState=loaded\nFragmentPath=" + definition + "\nDropInPaths=\n",
			)}, nil
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
	for _, action := range []string{"link", "start", "revert"} {
		if !slices.ContainsFunc(calls, func(call []string) bool { return slices.Contains(call, action) }) {
			t.Fatalf("systemctl calls omit %s: %q", action, calls)
		}
	}
}

func TestLinuxUpgradeActivatorRejectsDropInBeforeLaunch(t *testing.T) {
	root := t.TempDir()
	definition := filepath.Join(root, "delegation-upgrade-"+testUpgradeTransactionID+".service")
	plan, err := PrepareUpgradeActivator(
		"/opt/delegation/0.2.0/delegation", root, testUpgradeTransactionID, definition,
		strconv.Itoa(os.Geteuid()),
	)
	if err != nil {
		t.Fatal(err)
	}
	originalRunner := runSystemctl
	t.Cleanup(func() { runSystemctl = originalRunner })
	started := false
	runSystemctl = func(args ...string) (userServiceCommandResult, error) {
		if slices.Contains(args, "show") {
			return userServiceCommandResult{Output: []byte(
				"LoadState=loaded\nFragmentPath=" + definition + "\nDropInPaths=/tmp/foreign.conf\n",
			)}, nil
		}
		if slices.Contains(args, "start") {
			started = true
		}
		return userServiceCommandResult{}, nil
	}
	if err := LaunchUpgradeActivator(context.Background(), plan); err == nil {
		t.Fatal("LaunchUpgradeActivator accepted a drop-in override")
	}
	if started {
		t.Fatal("LaunchUpgradeActivator started a shadowed unit")
	}
}

func TestLinuxUpgradeActivatorRefusesToRemoveForeignUnit(t *testing.T) {
	root := t.TempDir()
	definition := filepath.Join(root, "delegation-upgrade-"+testUpgradeTransactionID+".service")
	plan, err := PrepareUpgradeActivator(
		"/opt/delegation/0.2.0/delegation", root, testUpgradeTransactionID, definition,
		strconv.Itoa(os.Geteuid()),
	)
	if err != nil {
		t.Fatal(err)
	}
	originalRunner := runSystemctl
	t.Cleanup(func() { runSystemctl = originalRunner })
	mutated := false
	runSystemctl = func(args ...string) (userServiceCommandResult, error) {
		if slices.Contains(args, "show") {
			return userServiceCommandResult{Output: []byte(
				"LoadState=loaded\nFragmentPath=/tmp/foreign.service\nDropInPaths=\n",
			)}, nil
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
