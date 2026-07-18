//go:build windows

package userservice

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"golang.org/x/sys/windows"
)

func TestWindowsInstallCreatesTaskWithoutForce(t *testing.T) {
	originalRunner := runTaskCommand
	t.Cleanup(func() { runTaskCommand = originalRunner })
	var calls [][]string
	runTaskCommand = func(args ...string) (taskCommandResult, error) {
		calls = append(calls, slices.Clone(args))
		data, err := os.ReadFile(args[4])
		if err != nil {
			t.Fatal(err)
		}
		definition, err := parseTaskDefinition(data)
		document, normalizeErr := normalizeTaskXML(data)
		if err != nil || normalizeErr != nil || !taskOwned(definition) ||
			!bytes.HasPrefix(data, []byte{0xff, 0xfe}) || !strings.Contains(string(document), "<Enabled>false</Enabled>") {
			t.Fatalf("generated task = %#v, %v", definition, err)
		}
		return taskCommandResult{}, nil
	}

	result, err := platformPrepare(`C:\Delegation\delegation.exe`, `C:\Users\test\config.json`)
	if err != nil || result.State != StatePrepared {
		t.Fatalf("platformInstall() = %#v, %v", result, err)
	}
	if len(calls) != 1 || len(calls[0]) != 5 || calls[0][0] != "/Create" ||
		calls[0][2] != ScheduledTask || slices.Contains(calls[0], "/F") {
		t.Fatalf("schtasks calls = %q", calls)
	}
}

func TestWindowsInstallEnablesAndRunsTask(t *testing.T) {
	originalRunner := runTaskCommand
	t.Cleanup(func() { runTaskCommand = originalRunner })
	var calls [][]string
	var active []byte
	runTaskCommand = func(args ...string) (taskCommandResult, error) {
		calls = append(calls, slices.Clone(args))
		if args[0] == "/Create" {
			data, err := os.ReadFile(args[4])
			if err != nil {
				t.Fatal(err)
			}
			active, err = encodeTaskXMLUTF16LE(strings.Replace(
				taskXMLText(t, data), "<Enabled>false</Enabled>", "<Enabled>true</Enabled>", 1,
			))
			if err != nil {
				t.Fatal(err)
			}
		}
		if args[0] == "/Query" {
			return taskCommandResult{Output: active}, nil
		}
		return taskCommandResult{}, nil
	}
	result, err := Install(`C:\Delegation\delegation.exe`, `C:\Users\test\config.json`)
	if err != nil || result.State != StateActive {
		t.Fatalf("Install() = %#v, %v", result, err)
	}
	if len(calls) != 5 || calls[0][0] != "/Create" || calls[1][0] != "/Change" ||
		!slices.Contains(calls[1], "/ENABLE") || calls[2][0] != "/Query" ||
		calls[3][0] != "/Run" || calls[4][0] != "/Query" {
		t.Fatalf("scheduled task activation calls = %q", calls)
	}
}

func TestWindowsInstallReconcilesLostEnableResponse(t *testing.T) {
	originalRunner := runTaskCommand
	t.Cleanup(func() { runTaskCommand = originalRunner })
	var active []byte
	runTaskCommand = func(args ...string) (taskCommandResult, error) {
		switch args[0] {
		case "/Create":
			data, err := os.ReadFile(args[4])
			if err != nil {
				t.Fatal(err)
			}
			active, err = encodeTaskXMLUTF16LE(strings.Replace(
				taskXMLText(t, data), "<Enabled>false</Enabled>", "<Enabled>true</Enabled>", 1,
			))
			if err != nil {
				t.Fatal(err)
			}
		case "/Change":
			return taskCommandResult{}, errors.New("connection lost")
		case "/Query":
			return taskCommandResult{Output: active}, nil
		}
		return taskCommandResult{}, nil
	}
	result, err := Install(`C:\Delegation\delegation.exe`, `C:\Users\test\config.json`)
	if err != nil || result.State != StateActive {
		t.Fatalf("Install() = %#v, %v", result, err)
	}
}

func TestWindowsPrepareAcceptsAnOtherwiseEquivalentEnabledTask(t *testing.T) {
	originalRunner := runTaskCommand
	t.Cleanup(func() { runTaskCommand = originalRunner })
	sid, err := windowsUserSID()
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := RenderScheduledTask(
		`C:\Delegation\delegation.exe`, `C:\Users\test\config.json`,
		sid, ScheduledTask, windows.EscapeArg,
	)
	if err != nil {
		t.Fatal(err)
	}
	active, err := encodeTaskXMLUTF16LE(strings.Replace(
		taskXMLText(t, descriptor.Content), "<Enabled>false</Enabled>", "<Enabled>true</Enabled>", 1,
	))
	if err != nil {
		t.Fatal(err)
	}
	runTaskCommand = func(args ...string) (taskCommandResult, error) {
		if args[0] == "/Create" {
			return taskCommandResult{ExitCode: 1}, nil
		}
		return taskCommandResult{Output: active}, nil
	}
	result, err := platformPrepare(`C:\Delegation\delegation.exe`, `C:\Users\test\config.json`)
	if err != nil || result.State != StateActive {
		t.Fatalf("platformPrepare() = %#v, %v", result, err)
	}
}

func TestWindowsInstallReportsPartialActivation(t *testing.T) {
	originalRunner := runTaskCommand
	t.Cleanup(func() { runTaskCommand = originalRunner })
	runTaskCommand = func(args ...string) (taskCommandResult, error) {
		if args[0] == "/Change" {
			return taskCommandResult{Output: []byte("denied"), ExitCode: 5}, nil
		}
		return taskCommandResult{}, nil
	}
	result, err := Install(`C:\Delegation\delegation.exe`, `C:\Users\test\config.json`)
	if err == nil || result.State != StateIndeterminate {
		t.Fatalf("Install() = %#v, %v", result, err)
	}
}

func TestWindowsInstallRecognizesExactUTF16Task(t *testing.T) {
	originalRunner := runTaskCommand
	t.Cleanup(func() { runTaskCommand = originalRunner })
	var desired []byte
	runTaskCommand = func(args ...string) (taskCommandResult, error) {
		if args[0] == "/Create" {
			var err error
			desired, err = os.ReadFile(args[4])
			if err != nil {
				t.Fatal(err)
			}
			return taskCommandResult{ExitCode: 1}, nil
		}
		return taskCommandResult{Output: desired}, nil
	}

	result, err := platformPrepare(`C:\Delegation\delegation.exe`, `C:\Users\test\config.json`)
	if err != nil || result.State != StatePrepared {
		t.Fatalf("platformInstall() = %#v, %v", result, err)
	}
}

func TestWindowsInstallReconcilesCreateTransportError(t *testing.T) {
	originalRunner := runTaskCommand
	t.Cleanup(func() { runTaskCommand = originalRunner })
	var desired []byte
	runTaskCommand = func(args ...string) (taskCommandResult, error) {
		if args[0] == "/Create" {
			var err error
			desired, err = os.ReadFile(args[4])
			if err != nil {
				t.Fatal(err)
			}
			return taskCommandResult{}, errors.New("transport timeout after registration")
		}
		return taskCommandResult{Output: desired}, nil
	}

	result, err := platformPrepare(`C:\Delegation\delegation.exe`, `C:\Users\test\config.json`)
	if err != nil || result.State != StatePrepared {
		t.Fatalf("platformInstall() = %#v, %v", result, err)
	}
}

func TestWindowsInstallRejectsManagedDriftAndForeignSubstring(t *testing.T) {
	originalRunner := runTaskCommand
	t.Cleanup(func() { runTaskCommand = originalRunner })
	foreign := false
	runTaskCommand = func(args ...string) (taskCommandResult, error) {
		if args[0] == "/Create" {
			return taskCommandResult{ExitCode: 1}, nil
		}
		if foreign {
			return taskCommandResult{Output: []byte(`<?xml version="1.0"?><Task version="1.4" xmlns="` + taskXMLNamespace + `"><RegistrationInfo><Description>foreign ` + Marker + `</Description><URI>` + ScheduledTask + `</URI></RegistrationInfo><Triggers/><Principals/><Settings/><Actions/></Task>`)}, nil
		}
		descriptor, err := RenderScheduledTask(`C:\Other\delegation.exe`, `C:\Users\test\config.json`, "S-1-5-18", ScheduledTask, func(value string) string { return value })
		if err != nil {
			t.Fatal(err)
		}
		return taskCommandResult{Output: descriptor.Content}, nil
	}

	if result, err := platformPrepare(`C:\Delegation\delegation.exe`, `C:\Users\test\config.json`); err == nil || result.State != StatePrepared {
		t.Fatalf("managed drift = %#v, %v", result, err)
	}
	foreign = true
	if result, err := platformPrepare(`C:\Delegation\delegation.exe`, `C:\Users\test\config.json`); err == nil || result.State != StateForeignConflict {
		t.Fatalf("foreign substring = %#v, %v", result, err)
	}
}

func TestWindowsInstallFailsClosedWhenCreateAndQueryFail(t *testing.T) {
	originalRunner := runTaskCommand
	t.Cleanup(func() { runTaskCommand = originalRunner })
	runTaskCommand = func(args ...string) (taskCommandResult, error) {
		return taskCommandResult{Output: []byte("denied"), ExitCode: 5}, nil
	}
	result, err := platformPrepare(`C:\Delegation\delegation.exe`, `C:\Users\test\config.json`)
	if err == nil || !strings.Contains(err.Error(), "exit 5") || result.State != StateIndeterminate {
		t.Fatalf("platformInstall() = %#v, %v", result, err)
	}
}

func TestWindowsInstallTreatsUnparseableQueryAsIndeterminate(t *testing.T) {
	originalRunner := runTaskCommand
	t.Cleanup(func() { runTaskCommand = originalRunner })
	runTaskCommand = func(args ...string) (taskCommandResult, error) {
		if args[0] == "/Create" {
			return taskCommandResult{ExitCode: 1}, nil
		}
		return taskCommandResult{Output: []byte("<Task")}, nil
	}

	result, err := platformPrepare(`C:\Delegation\delegation.exe`, `C:\Users\test\config.json`)
	if err == nil || result.State != StateIndeterminate {
		t.Fatalf("platformInstall() = %#v, %v", result, err)
	}
}

func TestTaskSchedulerExecutableIgnoresSystemRoot(t *testing.T) {
	t.Setenv("SystemRoot", ".")
	path, err := taskSchedulerExecutable()
	if err != nil {
		t.Fatal(err)
	}
	if !filepath.IsAbs(path) || filepath.Base(path) != "schtasks.exe" {
		t.Fatalf("task scheduler executable = %q", path)
	}
}

func TestWindowsTaskUserIDsEqualUsesSIDIdentity(t *testing.T) {
	equal, err := windowsTaskUserIDsEqual("S-1-5-18", "S-1-5-18")
	if err != nil || !equal {
		t.Fatalf("windowsTaskUserIDsEqual() = %v, %v", equal, err)
	}
	equal, err = windowsTaskUserIDsEqual("S-1-5-18", "S-1-5-19")
	if err != nil || equal {
		t.Fatalf("windowsTaskUserIDsEqual() accepted different SIDs: %v, %v", equal, err)
	}
	if _, err := resolveTaskUserSID(""); err == nil {
		t.Fatal("resolveTaskUserSID() accepted an empty identity")
	}
}

func TestWindowsTaskSchedulerRoundTrip(t *testing.T) {
	if os.Getenv("DELEGATION_WINDOWS_INTEGRATION") != "1" {
		t.Skip("set DELEGATION_WINDOWS_INTEGRATION=1 to use the real Task Scheduler")
	}
	sid, err := windowsUserSID()
	if err != nil {
		t.Fatal(err)
	}
	taskName := fmt.Sprintf(`\Delegation Connector Test %d`, os.Getpid())
	descriptor, err := RenderScheduledTask(`C:\Windows\System32\cmd.exe`, "C:\\Windows\\Temp\\delegation-\u9a8c\u8bc1.json", sid, taskName, windows.EscapeArg)
	if err != nil {
		t.Fatal(err)
	}
	temp, err := os.CreateTemp("", "delegation-task-integration-*.xml")
	if err != nil {
		t.Fatal(err)
	}
	tempPath := temp.Name()
	t.Cleanup(func() { os.Remove(tempPath) })
	if _, err := temp.Write(descriptor.Content); err != nil {
		t.Fatal(err)
	}
	if err := temp.Close(); err != nil {
		t.Fatal(err)
	}
	created, err := executeTaskCommand("/Create", "/TN", taskName, "/XML", tempPath)
	if err != nil || created.ExitCode != 0 {
		t.Fatalf("create task = %#v, %v", created, err)
	}
	t.Cleanup(func() { _, _ = executeTaskCommand("/Delete", "/TN", taskName, "/F") })
	query, err := executeTaskCommand("/Query", "/TN", taskName, "/XML")
	if err != nil || query.ExitCode != 0 {
		t.Fatalf("query task = %#v, %v", query, err)
	}
	desired, err := parseTaskDefinition(descriptor.Content)
	if err != nil {
		t.Fatal(err)
	}
	existing, err := parseTaskDefinition(query.Output)
	if err != nil {
		t.Fatal(err)
	}
	equivalent, err := taskDefinitionsEquivalent(desired, existing, windowsTaskUserIDsEqual)
	if err != nil || !equivalent {
		t.Fatalf("round-trip task = %#v, %v; want %#v", existing, err, desired)
	}
	changed, err := executeTaskCommand("/Change", "/TN", taskName, "/ENABLE")
	if err != nil || changed.ExitCode != 0 {
		t.Fatalf("enable task = %#v, %v", changed, err)
	}
	query, err = executeTaskCommand("/Query", "/TN", taskName, "/XML")
	if err != nil || query.ExitCode != 0 {
		t.Fatalf("query enabled task = %#v, %v", query, err)
	}
	enabled, err := parseTaskDefinition(query.Output)
	if err != nil {
		t.Fatal(err)
	}
	desired.Enabled = true
	equivalent, err = taskDefinitionsEquivalent(desired, enabled, windowsTaskUserIDsEqual)
	if err != nil || !equivalent || !enabled.Enabled {
		t.Fatalf("enabled task = %#v, %v; want %#v", enabled, err, desired)
	}
	started, err := executeTaskCommand("/Run", "/TN", taskName)
	if err != nil || started.ExitCode != 0 {
		t.Fatalf("run task = %#v, %v", started, err)
	}
	second, err := executeTaskCommand("/Create", "/TN", taskName, "/XML", tempPath)
	if err != nil || second.ExitCode == 0 {
		t.Fatalf("second no-force create = %#v, %v", second, err)
	}
}
