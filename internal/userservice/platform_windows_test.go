//go:build windows

package userservice

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	delegationconfig "github.com/GhostFlying/delegation/internal/config"
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
		if err != nil || normalizeErr != nil || !taskOwned(definition, ServiceRolePeer) ||
			!bytes.HasPrefix(data, []byte{0xff, 0xfe}) || !strings.Contains(string(document), "<Enabled>false</Enabled>") {
			t.Fatalf("generated task = %#v, %v", definition, err)
		}
		return taskCommandResult{}, nil
	}

	result, err := platformPrepare(ServiceRolePeer, testInvocation(
		ServiceRolePeer, `C:\Delegation\delegation.exe`, `C:\Users\test\config.json`,
	))
	if err != nil || result.State != StatePrepared {
		t.Fatalf("platformInstall() = %#v, %v", result, err)
	}
	if len(calls) != 1 || len(calls[0]) != 5 || calls[0][0] != "/Create" ||
		calls[0][2] != ScheduledTaskPeer || slices.Contains(calls[0], "/F") {
		t.Fatalf("schtasks calls = %q", calls)
	}
}

func TestWindowsBrokerAndPeerDefinitionsCoexist(t *testing.T) {
	originalRunner := runTaskCommand
	t.Cleanup(func() { runTaskCommand = originalRunner })
	created := make(map[string]string)
	runTaskCommand = func(args ...string) (taskCommandResult, error) {
		if len(args) != 5 || args[0] != "/Create" || args[1] != "/TN" || args[3] != "/XML" {
			t.Fatalf("unexpected schtasks call = %q", args)
		}
		data, err := os.ReadFile(args[4])
		if err != nil {
			t.Fatal(err)
		}
		created[args[2]] = taskXMLText(t, data)
		return taskCommandResult{}, nil
	}

	broker, err := platformPrepare(
		ServiceRoleBroker,
		testInvocation(ServiceRoleBroker, `C:\Delegation\delegation.exe`, `C:\Users\test\broker.json`),
	)
	if err != nil {
		t.Fatal(err)
	}
	peer, err := platformPrepare(
		ServiceRolePeer,
		testInvocation(ServiceRolePeer, `C:\Delegation\delegation.exe`, `C:\Users\test\peer.json`),
	)
	if err != nil {
		t.Fatal(err)
	}
	if broker.Artifact == peer.Artifact || broker.Role != ServiceRoleBroker || peer.Role != ServiceRolePeer {
		t.Fatalf("cohost results = %#v / %#v", broker, peer)
	}
	if len(created) != 2 || !strings.Contains(created[ScheduledTaskBroker], MarkerBroker) ||
		!strings.Contains(created[ScheduledTaskBroker], `C:\Users\test\broker.json`) ||
		!strings.Contains(created[ScheduledTaskPeer], MarkerPeer) ||
		!strings.Contains(created[ScheduledTaskPeer], `C:\Users\test\peer.json`) {
		t.Fatalf("cohost tasks = %#v", created)
	}
}

func TestWindowsInstallEnablesAndRunsTask(t *testing.T) {
	stubScheduledTaskReadiness(t, nil)
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
	result, err := Install(ServiceRolePeer, testInvocation(
		ServiceRolePeer, `C:\Delegation\delegation.exe`, `C:\Users\test\config.json`,
	))
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
	stubScheduledTaskReadiness(t, nil)
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
	result, err := Install(ServiceRolePeer, testInvocation(
		ServiceRolePeer, `C:\Delegation\delegation.exe`, `C:\Users\test\config.json`,
	))
	if err != nil || result.State != StateActive {
		t.Fatalf("Install() = %#v, %v", result, err)
	}
}

func TestWindowsInstallRejectsTaskThatNeverBecomesReady(t *testing.T) {
	readinessErr := errors.New("connector exited before opening its local bridge")
	stubScheduledTaskReadiness(t, readinessErr)
	originalRunner := runTaskCommand
	t.Cleanup(func() { runTaskCommand = originalRunner })
	var active []byte
	runTaskCommand = func(args ...string) (taskCommandResult, error) {
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
	result, err := Install(ServiceRolePeer, testInvocation(
		ServiceRolePeer, `C:\Delegation\delegation.exe`, `C:\Users\test\config.json`,
	))
	if !errors.Is(err, readinessErr) || result.State != StateIndeterminate {
		t.Fatalf("Install() = %#v, %v", result, err)
	}
}

func TestWindowsInstallRejectsReadyEndpointWithoutRunningTask(t *testing.T) {
	stubScheduledTaskReadiness(t, nil)
	scheduledTaskRunning = func(ServiceRole) (bool, error) { return false, nil }
	originalRunner := runTaskCommand
	t.Cleanup(func() { runTaskCommand = originalRunner })
	var active []byte
	runTaskCommand = func(args ...string) (taskCommandResult, error) {
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
	result, err := Install(ServiceRolePeer, testInvocation(
		ServiceRolePeer, `C:\Delegation\delegation.exe`, `C:\Users\test\config.json`,
	))
	if err == nil || result.State != StateIndeterminate || !strings.Contains(err.Error(), "no running managed instance") {
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
		ServiceRolePeer,
		testInvocation(ServiceRolePeer, `C:\Delegation\delegation.exe`, `C:\Users\test\config.json`),
		sid, windows.EscapeArg,
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
	result, err := platformPrepare(ServiceRolePeer, testInvocation(
		ServiceRolePeer, `C:\Delegation\delegation.exe`, `C:\Users\test\config.json`,
	))
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
	result, err := Install(ServiceRolePeer, testInvocation(
		ServiceRolePeer, `C:\Delegation\delegation.exe`, `C:\Users\test\config.json`,
	))
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

	result, err := platformPrepare(ServiceRolePeer, testInvocation(
		ServiceRolePeer, `C:\Delegation\delegation.exe`, `C:\Users\test\config.json`,
	))
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

	result, err := platformPrepare(ServiceRolePeer, testInvocation(
		ServiceRolePeer, `C:\Delegation\delegation.exe`, `C:\Users\test\config.json`,
	))
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
			return taskCommandResult{Output: []byte(`<?xml version="1.0"?><Task version="1.4" xmlns="` + taskXMLNamespace + `"><RegistrationInfo><Description>foreign ` + MarkerPeer + `</Description><URI>` + ScheduledTaskPeer + `</URI></RegistrationInfo><Triggers/><Principals/><Settings/><Actions/></Task>`)}, nil
		}
		descriptor, err := RenderScheduledTask(
			ServiceRolePeer,
			testInvocation(ServiceRolePeer, `C:\Other\delegation.exe`, `C:\Users\test\config.json`),
			"S-1-5-18",
			func(value string) string { return value },
		)
		if err != nil {
			t.Fatal(err)
		}
		return taskCommandResult{Output: descriptor.Content}, nil
	}

	if result, err := platformPrepare(ServiceRolePeer, testInvocation(
		ServiceRolePeer, `C:\Delegation\delegation.exe`, `C:\Users\test\config.json`,
	)); err == nil || result.State != StatePrepared {
		t.Fatalf("managed drift = %#v, %v", result, err)
	}
	foreign = true
	if result, err := platformPrepare(ServiceRolePeer, testInvocation(
		ServiceRolePeer, `C:\Delegation\delegation.exe`, `C:\Users\test\config.json`,
	)); err == nil || result.State != StateForeignConflict {
		t.Fatalf("foreign substring = %#v, %v", result, err)
	}
}

func TestWindowsInstallFailsClosedWhenCreateAndQueryFail(t *testing.T) {
	originalRunner := runTaskCommand
	t.Cleanup(func() { runTaskCommand = originalRunner })
	runTaskCommand = func(args ...string) (taskCommandResult, error) {
		return taskCommandResult{Output: []byte("denied"), ExitCode: 5}, nil
	}
	result, err := platformPrepare(ServiceRolePeer, testInvocation(
		ServiceRolePeer, `C:\Delegation\delegation.exe`, `C:\Users\test\config.json`,
	))
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

	result, err := platformPrepare(ServiceRolePeer, testInvocation(
		ServiceRolePeer, `C:\Delegation\delegation.exe`, `C:\Users\test\config.json`,
	))
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

func stubScheduledTaskReadiness(t *testing.T, err error) {
	t.Helper()
	originalReadiness := waitForScheduledTaskReady
	originalRunning := scheduledTaskRunning
	waitForScheduledTaskReady = func(string) error { return err }
	scheduledTaskRunning = func(ServiceRole) (bool, error) { return true, nil }
	t.Cleanup(func() {
		waitForScheduledTaskReady = originalReadiness
		scheduledTaskRunning = originalRunning
	})
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
	descriptor, err := RenderScheduledTask(
		ServiceRolePeer,
		testInvocation(ServiceRolePeer, `C:\Windows\System32\cmd.exe`, "C:\\Windows\\Temp\\delegation-\u9a8c\u8bc1.json"),
		sid,
		windows.EscapeArg,
	)
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
	// schtasks.exe replaces RegistrationInfo/URI with the /TN value.
	desired.URI = taskName
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

func TestWindowsTaskSchedulerRequiresRuntimeReadiness(t *testing.T) {
	if os.Getenv("DELEGATION_WINDOWS_INTEGRATION") != "1" {
		t.Skip("set DELEGATION_WINDOWS_INTEGRATION=1 to use the real Task Scheduler")
	}
	binaryPath := os.Getenv("DELEGATION_WINDOWS_BINARY")
	if binaryPath == "" {
		t.Skip("set DELEGATION_WINDOWS_BINARY to a built delegation executable")
	}
	if !filepath.IsAbs(binaryPath) {
		t.Fatalf("DELEGATION_WINDOWS_BINARY must be absolute: %q", binaryPath)
	}
	root := t.TempDir()
	brokerConfigPath := filepath.Join(root, "broker", "broker.json")
	brokerConfig := windowsIntegrationBrokerConfig(t, root)
	writeWindowsIntegrationConfig(t, brokerConfigPath, brokerConfig)
	peerConfigPath := filepath.Join(root, "peer", "peer.json")
	writeWindowsIntegrationConfig(
		t,
		peerConfigPath,
		windowsIntegrationPeerConfig(t, root, binaryPath, brokerConfig),
	)
	writeIntegrationProviderEnvironment(t, peerConfigPath)
	brokerFixture := &windowsIntegrationTaskFixture{
		role: ServiceRoleBroker, binaryPath: binaryPath, configPath: brokerConfigPath,
	}
	peerFixture := &windowsIntegrationTaskFixture{
		role: ServiceRolePeer, binaryPath: binaryPath, configPath: peerConfigPath,
	}
	fixtures := []*windowsIntegrationTaskFixture{brokerFixture, peerFixture}
	for _, fixture := range fixtures {
		fixture.requireAbsent(t)
	}
	t.Cleanup(func() {
		for index := len(fixtures) - 1; index >= 0; index-- {
			if err := fixtures[index].cleanup(); err != nil {
				t.Errorf("cleanup %s Scheduled Task: %v", fixtures[index].role, err)
			}
		}
	})

	result, err := brokerFixture.install()
	if err != nil || result.State != StateActive || result.Artifact != ScheduledTaskBroker {
		t.Fatalf("activate broker task = %#v, %v", result, err)
	}
	result, err = peerFixture.install()
	if err != nil || result.State != StateActive || result.Artifact != ScheduledTaskPeer {
		t.Fatalf("activate peer task = %#v, %v", result, err)
	}
	if err := peerFixture.cleanup(); err != nil {
		t.Fatal(err)
	}
	if err := brokerFixture.cleanup(); err != nil {
		t.Fatal(err)
	}

	systemDirectory, err := windows.GetSystemDirectory()
	if err != nil {
		t.Fatal(err)
	}
	failedFixture := &windowsIntegrationTaskFixture{
		role: ServiceRolePeer, binaryPath: filepath.Join(systemDirectory, "where.exe"), configPath: peerConfigPath,
	}
	failedFixture.requireAbsent(t)
	fixtures = append(fixtures, failedFixture)
	result, err = failedFixture.install()
	if err == nil || result.State != StateIndeterminate {
		t.Fatalf("activate immediate-exit task = %#v, %v", result, err)
	}
}

type windowsIntegrationTaskFixture struct {
	role       ServiceRole
	binaryPath string
	configPath string
	attempted  bool
}

func (f *windowsIntegrationTaskFixture) requireAbsent(t *testing.T) {
	t.Helper()
	spec, err := specFor(f.role)
	if err != nil {
		t.Fatal(err)
	}
	query, err := executeTaskCommand("/Query", "/TN", spec.scheduled, "/XML")
	if err != nil {
		t.Fatal(err)
	}
	if query.ExitCode == 0 {
		t.Fatalf("refusing to modify pre-existing Scheduled Task %s", spec.scheduled)
	}
}

func (f *windowsIntegrationTaskFixture) install() (Result, error) {
	f.attempted = true
	return Install(f.role, testInvocation(f.role, f.binaryPath, f.configPath))
}

func (f *windowsIntegrationTaskFixture) cleanup() error {
	if !f.attempted {
		return nil
	}
	exists, matches, err := windowsIntegrationTaskMatches(f.role, f.binaryPath, f.configPath)
	if err != nil {
		return err
	}
	if !exists {
		f.attempted = false
		return nil
	}
	if !matches {
		return errors.New("refusing to delete a Scheduled Task that does not match the integration fixture")
	}
	spec, err := specFor(f.role)
	if err != nil {
		return err
	}
	_, _ = executeTaskCommand("/End", "/TN", spec.scheduled)
	deleted, err := executeTaskCommand("/Delete", "/TN", spec.scheduled, "/F")
	if err != nil {
		return fmt.Errorf("delete integration Scheduled Task: %w", err)
	}
	if deleted.ExitCode != 0 {
		return taskCommandError("delete integration Scheduled Task", deleted)
	}
	f.attempted = false
	return nil
}

func windowsIntegrationTaskMatches(
	role ServiceRole,
	binaryPath, configPath string,
) (bool, bool, error) {
	spec, err := specFor(role)
	if err != nil {
		return false, false, err
	}
	query, err := executeTaskCommand("/Query", "/TN", spec.scheduled, "/XML")
	if err != nil {
		return false, false, err
	}
	if query.ExitCode != 0 {
		return false, false, nil
	}
	existing, err := parseTaskDefinition(query.Output)
	if err != nil {
		return true, false, err
	}
	sid, err := windowsUserSID()
	if err != nil {
		return true, false, err
	}
	descriptor, err := RenderScheduledTask(role, testInvocation(role, binaryPath, configPath), sid, windows.EscapeArg)
	if err != nil {
		return true, false, err
	}
	desired, err := parseTaskDefinition(descriptor.Content)
	if err != nil {
		return true, false, err
	}
	desired.Enabled = existing.Enabled
	equivalent, err := taskDefinitionsEquivalent(desired, existing, windowsTaskUserIDsEqual)
	if err != nil {
		return true, false, err
	}
	return true, taskOwned(existing, role) && equivalent, nil
}

func windowsIntegrationBrokerConfig(t *testing.T, root string) delegationconfig.Config {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return delegationconfig.Config{
		SchemaVersion: delegationconfig.CurrentSchemaVersion,
		Role:          delegationconfig.RoleBroker,
		ControllerID:  "123e4567-e89b-42d3-a456-426614174701",
		Broker: delegationconfig.BrokerConfig{
			Listen:    address,
			StateFile: filepath.Join(root, "state", "broker.sqlite3"),
			Auth:      delegationconfig.AuthConfig{Mode: delegationconfig.AuthModeNone},
		},
	}
}

func windowsIntegrationPeerConfig(
	t *testing.T,
	root, binaryPath string,
	brokerConfig delegationconfig.Config,
) delegationconfig.Config {
	t.Helper()
	codexHome := filepath.Join(root, "peer", "codex")
	workspaceRoot := filepath.Join(root, "peer", "workspaces")
	for _, path := range []string{codexHome, workspaceRoot} {
		if err := delegationconfig.PreparePrivateDirectory(path); err != nil {
			t.Fatal(err)
		}
	}
	return delegationconfig.Config{
		SchemaVersion: delegationconfig.CurrentSchemaVersion,
		Role:          delegationconfig.RolePeer,
		ControllerID:  brokerConfig.ControllerID,
		DeviceID:      "123e4567-e89b-42d3-a456-426614174702",
		DeviceName:    "windows-peer",
		Broker: delegationconfig.BrokerConfig{
			URL:  "ws://" + brokerConfig.Broker.Listen + "/v1/connect",
			Auth: delegationconfig.AuthConfig{Mode: delegationconfig.AuthModeNone},
		},
		Peer: delegationconfig.PeerConfig{
			CodexBinary: binaryPath, CodexHome: codexHome, WorkspaceRoot: workspaceRoot,
			StateFile:      filepath.Join(root, "peer", "state", "peer.sqlite3"),
			MaxWorkerSlots: 1,
		},
	}
}

func writeWindowsIntegrationConfig(t *testing.T, path string, cfg delegationconfig.Config) {
	t.Helper()
	if err := delegationconfig.WriteNew(path, cfg); err != nil {
		t.Fatal(err)
	}
}
