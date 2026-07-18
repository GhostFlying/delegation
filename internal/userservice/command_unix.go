//go:build linux || darwin

package userservice

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"time"
)

const (
	userServiceCommandTimeout = 30 * time.Second
	maximumCommandOutput      = 64 * 1024
)

type userServiceCommandResult struct {
	Output    []byte
	ExitCode  int
	Truncated bool
}

type cappedCommandOutput struct {
	buffer    bytes.Buffer
	truncated bool
}

func (w *cappedCommandOutput) Write(data []byte) (int, error) {
	remaining := maximumCommandOutput - w.buffer.Len()
	if remaining > 0 {
		_, _ = w.buffer.Write(data[:min(len(data), remaining)])
	}
	if len(data) > remaining {
		w.truncated = true
	}
	return len(data), nil
}

func executeUserServiceCommand(executable string, args ...string) (userServiceCommandResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), userServiceCommandTimeout)
	defer cancel()
	command := exec.CommandContext(ctx, executable, args...)
	var output cappedCommandOutput
	command.Stdout = &output
	command.Stderr = &output
	err := command.Run()
	result := userServiceCommandResult{
		Output: bytes.TrimSpace(output.buffer.Bytes()), Truncated: output.truncated,
	}
	if ctx.Err() != nil {
		return result, fmt.Errorf("run %s: %w", executable, ctx.Err())
	}
	if err == nil {
		return result, nil
	}
	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		result.ExitCode = exitError.ExitCode()
		return result, nil
	}
	return result, fmt.Errorf("run %s: %w", executable, err)
}

func userServiceCommandError(action string, result userServiceCommandResult) error {
	suffix := ""
	if result.Truncated {
		suffix = " (output truncated)"
	}
	return fmt.Errorf("%s: exit %d: %s%s", action, result.ExitCode, result.Output, suffix)
}

func commandFailure(action string, result userServiceCommandResult) error {
	if result.ExitCode == 0 {
		return nil
	}
	return userServiceCommandError(action, result)
}
