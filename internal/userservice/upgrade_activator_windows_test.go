//go:build windows

package userservice

import (
	"context"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

const testUpgradeTransactionID = "123e4567-e89b-42d3-a456-426614174988"

func TestWindowsUpgradeActivatorLifecycleUsesExactScheduledTask(t *testing.T) {
	sid, err := windowsUserSID()
	if err != nil {
		t.Fatal(err)
	}
	root := `C:\Users\test\.delegation\upgrades\default\peer`
	definition := filepath.Join(root, "transactions", testUpgradeTransactionID, "delegation-upgrade-"+testUpgradeTransactionID+".xml")
	plan, err := PrepareUpgradeActivator(
		`C:\Users\test\.delegation\runtime\0.2.0\delegation.exe`, root,
		testUpgradeTransactionID, definition, sid,
	)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Kind != KindScheduledTask || !strings.Contains(taskXMLText(t, plan.Definition), "delegation.exe</Command>") {
		t.Fatalf("activator plan = %#v", plan)
	}

	enabled, err := encodeTaskXMLUTF16LE(strings.Replace(
		taskXMLText(t, plan.Definition), "<Enabled>false</Enabled>", "<Enabled>true</Enabled>", 1,
	))
	if err != nil {
		t.Fatal(err)
	}
	originalRunner := runTaskCommand
	t.Cleanup(func() { runTaskCommand = originalRunner })
	current := []byte(nil)
	var calls [][]string
	runTaskCommand = func(args ...string) (taskCommandResult, error) {
		calls = append(calls, slices.Clone(args))
		switch args[0] {
		case "/Query":
			if current == nil {
				return taskCommandResult{ExitCode: 1}, nil
			}
			return taskCommandResult{Output: current}, nil
		case "/Create":
			if slices.Contains(args, "/F") {
				t.Fatal("activator registration used destructive /F replacement")
			}
			current = plan.Definition
		case "/Change":
			current = enabled
		case "/Delete":
			current = nil
		}
		return taskCommandResult{}, nil
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
	for _, action := range []string{"/Create", "/Change", "/Run", "/Delete"} {
		if !slices.ContainsFunc(calls, func(call []string) bool { return slices.Contains(call, action) }) {
			t.Fatalf("schtasks calls omit %s: %q", action, calls)
		}
	}
}

func TestWindowsUpgradeActivatorRejectsOccupiedTask(t *testing.T) {
	sid, err := windowsUserSID()
	if err != nil {
		t.Fatal(err)
	}
	root := `C:\Users\test\.delegation\upgrades\default\peer`
	definition := filepath.Join(root, "transactions", testUpgradeTransactionID, "delegation-upgrade-"+testUpgradeTransactionID+".xml")
	plan, err := PrepareUpgradeActivator(
		`C:\Users\test\.delegation\runtime\0.2.0\delegation.exe`, root,
		testUpgradeTransactionID, definition, sid,
	)
	if err != nil {
		t.Fatal(err)
	}
	foreign := slices.Clone(plan.Definition)
	normalized := strings.Replace(taskXMLText(t, foreign), "delegation-managed-upgrade", "foreign-upgrade", 1)
	foreign, err = encodeTaskXMLUTF16LE(normalized)
	if err != nil {
		t.Fatal(err)
	}
	originalRunner := runTaskCommand
	t.Cleanup(func() { runTaskCommand = originalRunner })
	created := false
	runTaskCommand = func(args ...string) (taskCommandResult, error) {
		if args[0] == "/Query" {
			return taskCommandResult{Output: foreign}, nil
		}
		created = true
		return taskCommandResult{}, nil
	}
	if err := InstallUpgradeActivator(context.Background(), plan); err == nil {
		t.Fatal("InstallUpgradeActivator accepted an occupied task name")
	}
	if created {
		t.Fatal("InstallUpgradeActivator replaced an occupied task")
	}
}
