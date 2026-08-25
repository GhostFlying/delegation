//go:build darwin

package userservice

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	launchAgentUnloadTimeout = 5 * time.Second
	launchAgentUnloadPoll    = 50 * time.Millisecond
)

var (
	errLaunchAgentIdentityChangedDuringUnload = errors.New("LaunchAgent identity changed during unload")
	errLaunchAgentIdentityChangedDuringStart  = errors.New("LaunchAgent identity changed during start")
	errManagedLaunchAgentRemainedLoaded       = errors.New("managed LaunchAgent remained loaded after bootout")
	launchAgentStartTimeout                   = 10 * time.Second
	launchAgentStartPoll                      = 100 * time.Millisecond
)

var runLaunchctl = func(args ...string) (userServiceCommandResult, error) {
	return executeUserServiceCommand("/bin/launchctl", args...)
}

var waitForDarwinServiceReady = waitForServiceReady

func platformPrepare(role ServiceRole, invocation Invocation) (Result, error) {
	descriptor, err := RenderLaunchAgent(role, invocation)
	if err != nil {
		return Result{}, err
	}
	path, err := darwinServicePath(role, invocation.InstanceID)
	if err != nil {
		return Result{}, err
	}
	state, err := installManagedFile(path, descriptor)
	return Result{State: state, Kind: descriptor.Kind, Artifact: path, Role: role}, err
}

func platformActivate(result Result, invocation Invocation) (Result, error) {
	if result.State != StatePrepared && result.State != StateActive {
		return result, fmt.Errorf("cannot activate LaunchAgent from state %s", result.State)
	}
	spec, err := specFor(result.Role, invocation.InstanceID)
	if err != nil {
		return result, err
	}
	matched, err := launchAgentDefinitionMatches(result, invocation)
	if err != nil {
		result.State = StateIndeterminate
		return result, err
	}
	if !matched {
		result.State = StateForeignConflict
		return result, errors.New("prepared LaunchAgent definition changed before activation")
	}
	domain := fmt.Sprintf("gui/%d", os.Geteuid())
	target := domain + "/" + spec.launchAgent
	domainStatus, err := runLaunchctl("print", domain)
	if err != nil || domainStatus.ExitCode != 0 {
		return result, errors.Join(err, commandFailure("inspect LaunchAgent domain", domainStatus))
	}
	status, loaded, err := printLaunchAgent(target)
	if err != nil {
		return result, err
	}
	if loaded {
		if filepath.Clean(status.Path) != filepath.Clean(result.Artifact) {
			result.State = StateForeignConflict
			return result, errors.New("LaunchAgent label is loaded from an unmanaged path")
		}
	}
	enabled, err := runLaunchctl("enable", target)
	if err != nil {
		result.State = StateIndeterminate
		return result, err
	}
	if enabled.ExitCode != 0 {
		result.State = StateIndeterminate
		return result, userServiceCommandError("enable LaunchAgent", enabled)
	}
	if loaded {
		bootedOut, bootoutErr := runLaunchctl("bootout", target)
		unloadErr := waitForLaunchAgentUnloaded(target, result.Artifact)
		if unloadErr != nil {
			if errors.Is(unloadErr, errLaunchAgentIdentityChangedDuringUnload) {
				result.State = StateForeignConflict
			} else {
				result.State = StateIndeterminate
			}
			return result, errors.Join(
				bootoutErr,
				commandFailure("unload LaunchAgent", bootedOut),
				unloadErr,
			)
		}
	}
	bootstrapped, err := runLaunchctl("bootstrap", domain, result.Artifact)
	if err != nil || bootstrapped.ExitCode != 0 {
		reconciled, nowLoaded, printErr := printLaunchAgent(target)
		if printErr != nil || !nowLoaded || filepath.Clean(reconciled.Path) != filepath.Clean(result.Artifact) {
			result.State = StateIndeterminate
			return result, errors.Join(
				err,
				commandFailure("bootstrap LaunchAgent", bootstrapped),
				printErr,
			)
		}
	}
	kicked, kickErr := runLaunchctl("kickstart", target)
	startErr := waitForLaunchAgentRunning(target, result, invocation)
	if startErr != nil {
		if errors.Is(startErr, errLaunchAgentIdentityChangedDuringStart) {
			result.State = StateForeignConflict
		} else {
			result.State = StateIndeterminate
		}
		return result, errors.Join(
			kickErr,
			commandFailure("start LaunchAgent", kicked),
			startErr,
		)
	}
	matched, definitionErr := launchAgentDefinitionMatches(result, invocation)
	if definitionErr != nil {
		result.State = StateIndeterminate
		return result, definitionErr
	}
	if !matched {
		result.State = StateForeignConflict
		return result, errors.New("LaunchAgent definition changed during activation")
	}
	if err := waitForDarwinServiceReady(invocation.ConfigPath); err != nil {
		result.State = StateIndeterminate
		return result, fmt.Errorf("LaunchAgent did not become ready: %w", err)
	}
	result.State = StateActive
	return result, nil
}

func waitForLaunchAgentUnloaded(target, artifact string) error {
	deadline := time.Now().Add(launchAgentUnloadTimeout)
	for {
		status, loaded, err := printLaunchAgent(target)
		if err != nil {
			return err
		}
		if !loaded {
			return nil
		}
		if filepath.Clean(status.Path) != filepath.Clean(artifact) {
			return errLaunchAgentIdentityChangedDuringUnload
		}
		if time.Now().After(deadline) {
			return errManagedLaunchAgentRemainedLoaded
		}
		time.Sleep(launchAgentUnloadPoll)
	}
}

func waitForLaunchAgentRunning(target string, result Result, invocation Invocation) error {
	deadline := time.Now().Add(launchAgentStartTimeout)
	expectedArguments := launchAgentArguments(invocation)
	var lastErr error
	var lastStatus launchAgentStatus
	for {
		status, loaded, err := printLaunchAgent(target)
		lastStatus = status
		lastErr = err
		if err == nil && loaded {
			if filepath.Clean(status.Path) != filepath.Clean(result.Artifact) {
				return fmt.Errorf(
					"%w: loaded path %q does not match %q",
					errLaunchAgentIdentityChangedDuringStart,
					status.Path,
					result.Artifact,
				)
			}
			if status.Program != "" && filepath.Clean(status.Program) != filepath.Clean(invocation.BinaryPath) {
				return fmt.Errorf(
					"%w: loaded program %q does not match %q",
					errLaunchAgentIdentityChangedDuringStart,
					status.Program,
					invocation.BinaryPath,
				)
			}
			if status.ArgumentsPresent && !slices.Equal(status.Arguments, expectedArguments) {
				return fmt.Errorf(
					"%w: loaded arguments do not match the managed definition",
					errLaunchAgentIdentityChangedDuringStart,
				)
			}
			executableConfirmed := filepath.Clean(status.Program) == filepath.Clean(invocation.BinaryPath) ||
				status.ArgumentsPresent
			if status.State == "running" && status.PID > 0 &&
				executableConfirmed {
				return nil
			}
		}
		if !time.Now().Before(deadline) {
			return errors.Join(
				lastErr,
				fmt.Errorf(
					"LaunchAgent did not reach the managed running state before timeout "+
						"(loaded=%t state=%q pid=%d)",
					loaded,
					lastStatus.State,
					lastStatus.PID,
				),
			)
		}
		time.Sleep(launchAgentStartPoll)
	}
}

func launchAgentArguments(invocation Invocation) []string {
	arguments := []string{
		invocation.BinaryPath,
		"service",
		"run",
		"--config",
		invocation.ConfigPath,
	}
	if invocation.EnvironmentFile != "" {
		arguments = append(arguments, "--environment-file", invocation.EnvironmentFile)
	}
	return arguments
}

func launchAgentDefinitionMatches(result Result, invocation Invocation) (bool, error) {
	descriptor, err := RenderLaunchAgent(result.Role, invocation)
	if err != nil {
		return false, err
	}
	state, content, err := inspectManagedFile(result.Artifact, KindLaunchAgent)
	if err != nil {
		return false, fmt.Errorf("inspect prepared LaunchAgent definition: %w", err)
	}
	return state == StatePrepared && bytes.Equal(content, descriptor.Content), nil
}

type launchAgentStatus struct {
	Path             string
	State            string
	Program          string
	Arguments        []string
	ArgumentsPresent bool
	PID              int
}

func printLaunchAgent(target string) (launchAgentStatus, bool, error) {
	result, err := runLaunchctl("print", target)
	if err != nil {
		return launchAgentStatus{}, false, err
	}
	if result.ExitCode != 0 {
		return launchAgentStatus{}, false, nil
	}
	status, err := parseLaunchAgentStatus(result)
	return status, true, err
}

func parseLaunchAgentStatus(result userServiceCommandResult) (launchAgentStatus, error) {
	if result.Truncated {
		return launchAgentStatus{}, errors.New("launchctl service description exceeds the output limit")
	}
	type line struct {
		raw    string
		text   string
		key    string
		value  string
		indent int
	}
	scanner := bufio.NewScanner(bytes.NewReader(result.Output))
	var lines []line
	for scanner.Scan() {
		raw := scanner.Text()
		text := strings.TrimSpace(raw)
		key, value, _ := strings.Cut(text, " = ")
		lines = append(lines, line{
			raw:    raw,
			text:   text,
			key:    key,
			value:  strings.TrimSpace(value),
			indent: len(raw) - len(strings.TrimLeft(raw, " \t")),
		})
	}
	if err := scanner.Err(); err != nil {
		return launchAgentStatus{}, fmt.Errorf("parse launchctl service description: %w", err)
	}
	topLevelIndent := -1
	for _, candidate := range lines {
		if candidate.key == "path" && candidate.value != "" &&
			(topLevelIndent == -1 || candidate.indent < topLevelIndent) {
			topLevelIndent = candidate.indent
		}
	}
	if topLevelIndent < 0 {
		return launchAgentStatus{}, errors.New("launchctl service description has no absolute managed path")
	}
	var status launchAgentStatus
	seen := make(map[string]bool)
	for index, candidate := range lines {
		if candidate.indent != topLevelIndent {
			continue
		}
		switch candidate.key {
		case "path":
			if seen[candidate.key] {
				return launchAgentStatus{}, errors.New("launchctl service description contains duplicate top-level paths")
			}
			seen[candidate.key] = true
			status.Path = candidate.value
		case "state":
			if seen[candidate.key] {
				return launchAgentStatus{}, errors.New("launchctl service description contains duplicate top-level states")
			}
			seen[candidate.key] = true
			status.State = candidate.value
		case "program":
			if seen[candidate.key] {
				return launchAgentStatus{}, errors.New("launchctl service description contains duplicate top-level programs")
			}
			seen[candidate.key] = true
			status.Program = candidate.value
		case "pid":
			if seen[candidate.key] {
				return launchAgentStatus{}, errors.New("launchctl service description contains duplicate top-level PIDs")
			}
			seen[candidate.key] = true
			pid, err := strconv.Atoi(candidate.value)
			if err != nil || pid < 0 {
				return launchAgentStatus{}, errors.New("launchctl service description contains an invalid top-level PID")
			}
			status.PID = pid
		case "arguments":
			if candidate.value != "{" {
				continue
			}
			if seen[candidate.key] {
				return launchAgentStatus{}, errors.New("launchctl service description contains duplicate top-level arguments")
			}
			seen[candidate.key] = true
			status.ArgumentsPresent = true
			closed := false
			for nested := index + 1; nested < len(lines); nested++ {
				if lines[nested].indent <= topLevelIndent {
					if lines[nested].indent == topLevelIndent && lines[nested].text == "}" {
						closed = true
					}
					break
				}
				if lines[nested].text != "" {
					status.Arguments = append(status.Arguments, lines[nested].text)
				}
			}
			if !closed {
				return launchAgentStatus{}, errors.New("launchctl service description contains unterminated top-level arguments")
			}
		}
	}
	if status.Path == "" || !filepath.IsAbs(status.Path) {
		return launchAgentStatus{}, errors.New("launchctl service description has no absolute managed path")
	}
	return status, nil
}

func darwinServicePath(role ServiceRole, instanceID string) (string, error) {
	spec, err := specFor(role, instanceID)
	if err != nil {
		return "", err
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	if !filepath.IsAbs(home) {
		return "", errors.New("user home must be absolute")
	}
	return filepath.Join(home, "Library", "LaunchAgents", spec.launchAgent+".plist"), nil
}
