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
	"time"

	"golang.org/x/sys/windows"
)

const taskCommandTimeout = 30 * time.Second

type taskCommandResult struct {
	Output   []byte
	ExitCode int
}

var runTaskCommand = executeTaskCommand

func platformInstall(binaryPath, configPath string) (Result, error) {
	sid, err := windowsUserSID()
	if err != nil {
		return Result{}, err
	}
	descriptor, err := RenderScheduledTask(binaryPath, configPath, sid, ScheduledTask, windows.EscapeArg)
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

	created, err := runTaskCommand("/Create", "/TN", ScheduledTask, "/XML", tempPath)
	result := Result{State: StatePrepared, Kind: descriptor.Kind, Artifact: ScheduledTask}
	if err == nil && created.ExitCode == 0 {
		return result, nil
	}
	indeterminate := Result{State: StateIndeterminate, Kind: descriptor.Kind, Artifact: ScheduledTask}
	var createFailure error
	if err != nil {
		createFailure = fmt.Errorf("register scheduled task: %w", err)
	} else {
		createFailure = taskCommandError("register scheduled task", created)
	}
	query, err := runTaskCommand("/Query", "/TN", ScheduledTask, "/XML")
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
	if !taskOwned(existing) {
		return Result{State: StateForeignConflict, Kind: descriptor.Kind, Artifact: ScheduledTask}, errors.New("scheduled task name is occupied by an unmanaged task")
	}
	if existing != desired {
		return result, errors.New("managed scheduled task differs; remove it explicitly before reinstalling")
	}
	return result, nil
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
