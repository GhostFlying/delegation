//go:build darwin

package userservice

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
)

const testUpgradeTransactionID = "123e4567-e89b-42d3-a456-426614174988"

func TestDarwinUpgradeActivatorLifecycleUsesExactLaunchAgent(t *testing.T) {
	root := t.TempDir()
	name := "com.github.ghostflying.delegation.upgrade." + testUpgradeTransactionID
	definition := filepath.Join(root, name+".plist")
	plan, err := PrepareUpgradeActivator(
		"/opt/delegation/0.2.0/delegation", root, testUpgradeTransactionID, definition,
		strconv.Itoa(os.Geteuid()),
	)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Kind != KindLaunchAgent || plan.Name != name {
		t.Fatalf("activator plan = %#v", plan)
	}

	originalRunner := runLaunchctl
	t.Cleanup(func() { runLaunchctl = originalRunner })
	loaded := false
	var calls [][]string
	runLaunchctl = func(args ...string) (userServiceCommandResult, error) {
		calls = append(calls, slices.Clone(args))
		switch args[0] {
		case "print":
			if !loaded {
				return userServiceCommandResult{ExitCode: 3}, nil
			}
			return activatorLaunchctlStatus(plan), nil
		case "bootstrap":
			loaded = true
		case "bootout":
			loaded = false
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
	for _, action := range []string{"bootstrap", "kickstart", "bootout"} {
		if !slices.ContainsFunc(calls, func(call []string) bool { return slices.Contains(call, action) }) {
			t.Fatalf("launchctl calls omit %s: %q", action, calls)
		}
	}
}

func TestDarwinUpgradeActivatorRejectsMissingArguments(t *testing.T) {
	root := t.TempDir()
	name := "com.github.ghostflying.delegation.upgrade." + testUpgradeTransactionID
	definition := filepath.Join(root, name+".plist")
	plan, err := PrepareUpgradeActivator(
		"/opt/delegation/0.2.0/delegation", root, testUpgradeTransactionID, definition,
		strconv.Itoa(os.Geteuid()),
	)
	if err != nil {
		t.Fatal(err)
	}
	originalRunner := runLaunchctl
	t.Cleanup(func() { runLaunchctl = originalRunner })
	started := false
	runLaunchctl = func(args ...string) (userServiceCommandResult, error) {
		if args[0] == "print" {
			return userServiceCommandResult{Output: []byte(fmt.Sprintf(
				"path = %s\nstate = waiting\nprogram = %s\n", definition, plan.BinaryPath,
			))}, nil
		}
		started = true
		return userServiceCommandResult{}, nil
	}
	if err := LaunchUpgradeActivator(context.Background(), plan); err == nil {
		t.Fatal("LaunchUpgradeActivator accepted missing ProgramArguments")
	}
	if started {
		t.Fatal("LaunchUpgradeActivator started a mismatched LaunchAgent")
	}
}

func activatorLaunchctlStatus(plan UpgradeActivatorPlan) userServiceCommandResult {
	arguments := []string{
		plan.BinaryPath, "service", "upgrade-activate", "--upgrade-root", plan.UpgradeRoot,
		"--transaction-id", plan.TransactionID,
	}
	output := fmt.Sprintf("path = %s\nstate = waiting\nprogram = %s\narguments = {\n", plan.DefinitionPath, plan.BinaryPath)
	for _, argument := range arguments {
		output += "\t" + argument + "\n"
	}
	output += "}\n"
	return userServiceCommandResult{Output: []byte(output)}
}
