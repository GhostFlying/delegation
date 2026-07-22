//go:build windows

package userservice

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/windows"
)

const (
	taskCommandTimeout = 30 * time.Second
)

type taskCommandResult struct {
	Output   []byte
	ExitCode int
}

var (
	runTaskCommand            = executeTaskCommand
	waitForScheduledTaskReady = waitForServiceReady
	scheduledTaskRunning      = queryScheduledTaskRunning
)

func platformPrepare(role ServiceRole, invocation Invocation) (Result, error) {
	spec, err := specFor(role)
	if err != nil {
		return Result{}, err
	}
	sid, err := windowsUserSID()
	if err != nil {
		return Result{}, err
	}
	descriptor, err := RenderScheduledTask(role, invocation, sid, windows.EscapeArg)
	if err != nil {
		return Result{}, err
	}
	desired, err := parseTaskDefinition(descriptor.Content)
	if err != nil {
		return Result{}, fmt.Errorf("parse generated task definition: %w", err)
	}
	temp, err := os.CreateTemp("", "delegation-task-*.xml")
	if err != nil {
		return Result{}, fmt.Errorf("create temporary task definition: %w", err)
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	if _, err := temp.Write(descriptor.Content); err != nil {
		temp.Close()
		return Result{}, fmt.Errorf("write task definition: %w", err)
	}
	if err := temp.Close(); err != nil {
		return Result{}, fmt.Errorf("close task definition: %w", err)
	}

	created, err := runTaskCommand("/Create", "/TN", spec.scheduled, "/XML", tempPath)
	result := Result{State: StatePrepared, Kind: descriptor.Kind, Artifact: spec.scheduled, Role: role}
	if err == nil && created.ExitCode == 0 {
		return result, nil
	}
	indeterminate := Result{State: StateIndeterminate, Kind: descriptor.Kind, Artifact: spec.scheduled, Role: role}
	var createFailure error
	if err != nil {
		createFailure = fmt.Errorf("register scheduled task: %w", err)
	} else {
		createFailure = taskCommandError("register scheduled task", created)
	}
	query, err := runTaskCommand("/Query", "/TN", spec.scheduled, "/XML")
	if err != nil {
		return indeterminate, errors.Join(createFailure, fmt.Errorf("query scheduled task after failed registration: %w", err))
	}
	if query.ExitCode != 0 {
		return indeterminate, errors.Join(
			createFailure,
			taskCommandError("query existing scheduled task", query),
		)
	}
	existing, err := parseTaskDefinition(query.Output)
	if err != nil {
		return indeterminate, errors.Join(createFailure, fmt.Errorf("parse existing scheduled task: %w", err))
	}
	if !taskOwned(existing, role) {
		return Result{State: StateForeignConflict, Kind: descriptor.Kind, Artifact: spec.scheduled, Role: role}, errors.New("scheduled task name is occupied by an unmanaged task")
	}
	equivalent, err := taskDefinitionsEquivalent(desired, existing, windowsTaskUserIDsEqual)
	if err != nil {
		return indeterminate, errors.Join(createFailure, fmt.Errorf("compare existing scheduled task identity: %w", err))
	}
	if !equivalent {
		desired.Enabled = existing.Enabled
		equivalent, err = taskDefinitionsEquivalent(desired, existing, windowsTaskUserIDsEqual)
		if err != nil {
			return indeterminate, errors.Join(createFailure, fmt.Errorf("compare existing scheduled task activation: %w", err))
		}
		if !equivalent || !existing.Enabled {
			return result, errors.New("managed scheduled task differs; remove it explicitly before reinstalling")
		}
		result.State = StateActive
	}
	return result, nil
}

func platformActivate(result Result, invocation Invocation) (Result, error) {
	if result.State != StatePrepared && result.State != StateActive {
		return result, fmt.Errorf("cannot activate scheduled task from state %s", result.State)
	}
	spec, err := specFor(result.Role)
	if err != nil {
		return result, err
	}
	changed, changeErr := runTaskCommand("/Change", "/TN", spec.scheduled, "/ENABLE")
	enabled, verifyErr := scheduledTaskEnabled(result.Role, invocation)
	if !enabled || verifyErr != nil {
		result.State = StateIndeterminate
		return result, errors.Join(
			changeErr,
			taskCommandFailure("enable scheduled task", changed),
			verifyErr,
		)
	}
	run, runErr := runTaskCommand("/Run", "/TN", spec.scheduled)
	if runErr != nil || run.ExitCode != 0 {
		result.State = StateIndeterminate
		return result, errors.Join(runErr, taskCommandFailure("start scheduled task", run))
	}
	if err := waitForScheduledTaskReady(invocation.ConfigPath); err != nil {
		result.State = StateIndeterminate
		return result, fmt.Errorf("scheduled task did not become ready: %w", err)
	}
	enabled, verifyErr = scheduledTaskEnabled(result.Role, invocation)
	if !enabled || verifyErr != nil {
		result.State = StateIndeterminate
		return result, errors.Join(verifyErr, errors.New("scheduled task changed after it was started"))
	}
	running, runningErr := scheduledTaskRunning(result.Role)
	if runningErr != nil || !running {
		result.State = StateIndeterminate
		return result, errors.Join(runningErr, errors.New("scheduled task has no running managed instance"))
	}
	result.State = StateActive
	return result, nil
}

func queryScheduledTaskRunning(role ServiceRole) (bool, error) {
	spec, err := specFor(role)
	if err != nil {
		return false, err
	}
	directory, err := windows.GetSystemDirectory()
	if err != nil {
		return false, fmt.Errorf("resolve Windows system directory: %w", err)
	}
	executable := filepath.Join(directory, "WindowsPowerShell", "v1.0", "powershell.exe")
	script := fmt.Sprintf(`$ErrorActionPreference='Stop';$s=New-Object -ComObject 'Schedule.Service';$s.Connect();$t=$s.GetFolder('\').GetTask('%s');[Console]::Out.Write($t.GetInstances(0).Count)`, strings.TrimPrefix(spec.scheduled, `\`))
	ctx, cancel := context.WithTimeout(context.Background(), taskCommandTimeout)
	defer cancel()
	output, err := exec.CommandContext(
		ctx, executable, "-NoLogo", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command", script,
	).Output()
	if ctx.Err() != nil {
		return false, fmt.Errorf("query scheduled task instances: %w", ctx.Err())
	}
	if err != nil {
		return false, fmt.Errorf("query scheduled task instances: %w", err)
	}
	count, err := strconv.Atoi(strings.TrimSpace(string(output)))
	if err != nil || count < 0 {
		return false, errors.New("Task Scheduler returned an invalid running instance count")
	}
	return count > 0, nil
}

func scheduledTaskEnabled(role ServiceRole, invocation Invocation) (bool, error) {
	spec, err := specFor(role)
	if err != nil {
		return false, err
	}
	sid, err := windowsUserSID()
	if err != nil {
		return false, err
	}
	descriptor, err := RenderScheduledTask(
		role, invocation, sid, windows.EscapeArg,
	)
	if err != nil {
		return false, err
	}
	desired, err := parseTaskDefinition(descriptor.Content)
	if err != nil {
		return false, err
	}
	desired.Enabled = true
	query, err := runTaskCommand("/Query", "/TN", spec.scheduled, "/XML")
	if err != nil {
		return false, err
	}
	if query.ExitCode != 0 {
		return false, taskCommandError("query scheduled task activation", query)
	}
	existing, err := parseTaskDefinition(query.Output)
	if err != nil {
		return false, fmt.Errorf("parse activated scheduled task: %w", err)
	}
	if !taskOwned(existing, role) {
		return false, errors.New("scheduled task changed ownership during activation")
	}
	equivalent, err := taskDefinitionsEquivalent(desired, existing, windowsTaskUserIDsEqual)
	if err != nil {
		return false, err
	}
	if !equivalent {
		return false, errors.New("scheduled task activation changed its managed definition")
	}
	return true, nil
}

func taskCommandFailure(action string, result taskCommandResult) error {
	if result.ExitCode == 0 {
		return nil
	}
	return taskCommandError(action, result)
}

func taskCommandError(action string, result taskCommandResult) error {
	return fmt.Errorf("%s: exit %d: %s", action, result.ExitCode, bytes.TrimSpace(result.Output))
}

func executeTaskCommand(args ...string) (taskCommandResult, error) {
	executable, err := taskSchedulerExecutable()
	if err != nil {
		return taskCommandResult{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), taskCommandTimeout)
	defer cancel()
	command := exec.CommandContext(ctx, executable, args...)
	output, err := command.CombinedOutput()
	if ctx.Err() != nil {
		return taskCommandResult{}, fmt.Errorf("run schtasks.exe: %w", ctx.Err())
	}
	if err == nil {
		return taskCommandResult{Output: output}, nil
	}
	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		return taskCommandResult{Output: output, ExitCode: exitError.ExitCode()}, nil
	}
	return taskCommandResult{}, fmt.Errorf("run schtasks.exe: %w", err)
}

func taskSchedulerExecutable() (string, error) {
	directory, err := windows.GetSystemDirectory()
	if err != nil {
		return "", fmt.Errorf("resolve Windows system directory: %w", err)
	}
	return filepath.Join(directory, "schtasks.exe"), nil
}

func windowsUserSID() (string, error) {
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return "", fmt.Errorf("resolve current Windows user: %w", err)
	}
	return user.User.Sid.String(), nil
}

func windowsTaskUserIDsEqual(left, right string) (bool, error) {
	leftSID, err := resolveTaskUserSID(left)
	if err != nil {
		return false, err
	}
	rightSID, err := resolveTaskUserSID(right)
	if err != nil {
		return false, err
	}
	return leftSID.Equals(rightSID), nil
}

func resolveTaskUserSID(identifier string) (*windows.SID, error) {
	if identifier == "" {
		return nil, errors.New("scheduled task user identity is empty")
	}
	if sid, err := windows.StringToSid(identifier); err == nil {
		return sid, nil
	}
	sid, _, _, err := windows.LookupSID("", identifier)
	if err != nil {
		return nil, fmt.Errorf("resolve scheduled task user identity: %w", err)
	}
	return sid, nil
}
