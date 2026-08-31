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
	"slices"
	"strconv"
	"strings"
	"time"
)

const windowsUpgradeStopTimeout = 30 * time.Second

var (
	windowsUpgradePollInterval = 100 * time.Millisecond
	runTaskkill                = executeTaskkill
	queryUpgradePIDs           = queryScheduledTaskInstancePIDs
)

func platformValidateUpgradePlan(plan UpgradePlan, oldDescriptor, newDescriptor Descriptor) error {
	oldDefinition, err := parseTaskDefinition(plan.OldDefinition)
	if err != nil {
		return err
	}
	wantOld, err := parseTaskDefinition(oldDescriptor.Content)
	if err != nil {
		return err
	}
	newDefinition, err := parseTaskDefinition(plan.NewDefinition)
	if err != nil {
		return err
	}
	wantNew, err := parseTaskDefinition(newDescriptor.Content)
	if err != nil {
		return err
	}
	oldMatches, err := taskDefinitionsEquivalent(wantOld, oldDefinition, windowsTaskUserIDsEqual)
	if err != nil {
		return err
	}
	newMatches, err := taskDefinitionsEquivalent(wantNew, newDefinition, windowsTaskUserIDsEqual)
	if err != nil {
		return err
	}
	if !oldMatches || !newMatches {
		return errors.New("Scheduled Task upgrade plan definition changed")
	}
	return nil
}

func platformInspectUpgrade(ctx context.Context, plan UpgradePlan) (UpgradePlan, error) {
	if err := ctx.Err(); err != nil {
		return UpgradePlan{}, err
	}
	sid, err := windowsUserSID()
	if err != nil {
		return UpgradePlan{}, err
	}
	current, err := queryUpgradeTask(plan)
	if err != nil {
		return UpgradePlan{}, err
	}
	want, err := parseTaskDefinition(plan.OldDefinition)
	if err != nil {
		return UpgradePlan{}, err
	}
	want.Enabled = true
	if !taskOwned(current, plan.Role, plan.SourceInvocation.InstanceID) {
		return UpgradePlan{}, errors.New("Scheduled Task is not owned by the requested service")
	}
	equal, err := taskDefinitionsEquivalent(want, current, windowsTaskUserIDsEqual)
	if err != nil || !equal || !current.Enabled {
		return UpgradePlan{}, errors.Join(err, errors.New("Scheduled Task is not the exact enabled source definition"))
	}
	pids, err := queryUpgradePIDs(plan.Role, plan.SourceInvocation.InstanceID)
	if err != nil {
		return UpgradePlan{}, err
	}
	if len(pids) == 0 {
		return UpgradePlan{}, errors.New("Scheduled Task has no running engine process")
	}
	plan.Artifact = plan.NativeName
	plan.UserIdentity = sid
	plan.ProcessIDs = pids
	if err := validateUpgradePlan(plan); err != nil {
		return UpgradePlan{}, err
	}
	return plan, nil
}

func platformStopUpgrade(ctx context.Context, plan UpgradePlan) error {
	current, err := requireUpgradeTaskDefinitionAnyState(plan, nil)
	if err != nil {
		return err
	}
	pids, err := queryUpgradePIDs(plan.Role, plan.SourceInvocation.InstanceID)
	if err != nil {
		return err
	}
	if len(pids) > 0 && !sameProcessIDs(pids, plan.ProcessIDs) {
		return errors.New("Scheduled Task process identity changed since upgrade preparation")
	}
	if current.Enabled {
		disabled, disableErr := runTaskCommand("/Change", "/TN", plan.NativeName, "/DISABLE")
		if disableErr != nil || disabled.ExitCode != 0 {
			return errors.Join(disableErr, taskCommandFailure("disable scheduled task for upgrade", disabled))
		}
	}
	if len(pids) == 0 {
		return requireUpgradeTaskDefinition(plan, nil, false)
	}
	ended, endErr := runTaskCommand("/End", "/TN", plan.NativeName)
	if endErr != nil || ended.ExitCode != 0 {
		return errors.Join(endErr, taskCommandFailure("end scheduled task for upgrade", ended))
	}
	for _, pid := range plan.ProcessIDs {
		result, killErr := runTaskkill(pid)
		if killErr != nil || (result.ExitCode != 0 && !taskkillAlreadyExited(result.Output)) {
			return errors.Join(killErr, taskCommandFailure("stop scheduled task process tree", result))
		}
	}
	deadline := time.Now().Add(windowsUpgradeStopTimeout)
	for {
		pids, err := queryUpgradePIDs(plan.Role, plan.SourceInvocation.InstanceID)
		if err != nil {
			return err
		}
		if len(pids) == 0 {
			return requireUpgradeTaskDefinition(plan, nil, false)
		}
		if !time.Now().Before(deadline) {
			return errors.New("Scheduled Task process tree did not stop before timeout")
		}
		if err := waitWindowsUpgradePoll(ctx, windowsUpgradePollInterval); err != nil {
			return err
		}
	}
}

func platformSwitchUpgradeDefinition(ctx context.Context, plan UpgradePlan, target bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if pids, err := queryUpgradePIDs(plan.Role, plan.SourceInvocation.InstanceID); err != nil {
		return err
	} else if len(pids) != 0 {
		return errors.New("Scheduled Task must be stopped before definition replacement")
	}
	if err := requireUpgradeTaskDefinition(plan, upgradeDefinitionPairFirst(plan, target), false); err != nil {
		if matchErr := requireUpgradeTaskDefinition(plan, upgradeDefinition(plan, target), false); matchErr == nil {
			return nil
		}
		return err
	}
	if err := registerUpgradeTask(plan.NativeName, upgradeDefinition(plan, target)); err != nil {
		if matchErr := requireUpgradeTaskDefinition(plan, upgradeDefinition(plan, target), false); matchErr == nil {
			return nil
		}
		return err
	}
	return requireUpgradeTaskDefinition(plan, upgradeDefinition(plan, target), false)
}

func platformStartUpgrade(ctx context.Context, plan UpgradePlan, target bool) error {
	if err := requireUpgradeTaskDefinition(plan, upgradeDefinition(plan, target), false); err != nil {
		return err
	}
	enabled, err := runTaskCommand("/Change", "/TN", plan.NativeName, "/ENABLE")
	if err != nil || enabled.ExitCode != 0 {
		return errors.Join(err, taskCommandFailure("enable scheduled task after upgrade", enabled))
	}
	started, err := runTaskCommand("/Run", "/TN", plan.NativeName)
	if err != nil || started.ExitCode != 0 {
		return errors.Join(err, taskCommandFailure("start scheduled task after upgrade", started))
	}
	invocation := upgradeInvocation(plan, target)
	if err := waitForScheduledTaskReady(invocation.ConfigPath); err != nil {
		return fmt.Errorf("Scheduled Task did not become ready after upgrade: %w", err)
	}
	matched, err := platformUpgradeServiceMatches(ctx, plan, target)
	if err != nil {
		return err
	}
	if !matched {
		return errors.New("Scheduled Task did not start with the requested upgrade definition")
	}
	return nil
}

func platformUpgradeServiceMatches(ctx context.Context, plan UpgradePlan, target bool) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := requireUpgradeTaskDefinition(plan, upgradeDefinition(plan, target), true); err != nil {
		return false, nil
	}
	pids, err := queryUpgradePIDs(plan.Role, plan.SourceInvocation.InstanceID)
	return len(pids) != 0, err
}

func queryUpgradeTask(plan UpgradePlan) (taskDefinition, error) {
	result, err := runTaskCommand("/Query", "/TN", plan.NativeName, "/XML")
	if err != nil || result.ExitCode != 0 {
		return taskDefinition{}, errors.Join(err, taskCommandFailure("query scheduled task for upgrade", result))
	}
	definition, err := parseTaskDefinition(result.Output)
	if err != nil {
		return taskDefinition{}, fmt.Errorf("parse scheduled task for upgrade: %w", err)
	}
	return definition, nil
}

func requireUpgradeTaskDefinition(plan UpgradePlan, expected []byte, enabled bool) error {
	current, err := queryUpgradeTask(plan)
	if err != nil {
		return err
	}
	if !taskOwned(current, plan.Role, plan.SourceInvocation.InstanceID) {
		return errors.New("Scheduled Task ownership changed during upgrade")
	}
	if expected == nil {
		for _, candidate := range [][]byte{plan.OldDefinition, plan.NewDefinition} {
			if taskDefinitionMatchesBytes(current, candidate, enabled) {
				return nil
			}
		}
		return errors.New("Scheduled Task definition changed outside the upgrade transaction")
	}
	if !taskDefinitionMatchesBytes(current, expected, enabled) {
		return errors.New("Scheduled Task definition does not match the upgrade journal")
	}
	return nil
}

func requireUpgradeTaskDefinitionAnyState(plan UpgradePlan, expected []byte) (taskDefinition, error) {
	current, err := queryUpgradeTask(plan)
	if err != nil {
		return taskDefinition{}, err
	}
	if !taskOwned(current, plan.Role, plan.SourceInvocation.InstanceID) {
		return taskDefinition{}, errors.New("Scheduled Task ownership changed during upgrade")
	}
	candidates := [][]byte{expected}
	if expected == nil {
		candidates = [][]byte{plan.OldDefinition, plan.NewDefinition}
	}
	for _, candidate := range candidates {
		if taskDefinitionMatchesBytes(current, candidate, current.Enabled) {
			return current, nil
		}
	}
	return taskDefinition{}, errors.New("Scheduled Task definition does not match the upgrade journal")
}

func taskDefinitionMatchesBytes(current taskDefinition, expected []byte, enabled bool) bool {
	want, err := parseTaskDefinition(expected)
	if err != nil {
		return false
	}
	want.Enabled = enabled
	equal, err := taskDefinitionsEquivalent(want, current, windowsTaskUserIDsEqual)
	return err == nil && equal
}

func registerUpgradeTask(name string, definition []byte) error {
	temporary, err := os.CreateTemp("", "delegation-upgrade-task-*.xml")
	if err != nil {
		return err
	}
	path := temporary.Name()
	defer os.Remove(path)
	if _, err := temporary.Write(definition); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	result, err := runTaskCommand("/Create", "/F", "/TN", name, "/XML", path)
	if err != nil || result.ExitCode != 0 {
		return errors.Join(err, taskCommandFailure("replace scheduled task for upgrade", result))
	}
	return nil
}

func queryScheduledTaskInstancePIDs(role ServiceRole, instanceID string) ([]int, error) {
	spec, err := specFor(role, instanceID)
	if err != nil {
		return nil, err
	}
	directory, err := windowsSystemDirectory()
	if err != nil {
		return nil, err
	}
	executable := filepath.Join(directory, "WindowsPowerShell", "v1.0", "powershell.exe")
	name := strings.ReplaceAll(strings.TrimPrefix(spec.scheduled, `\`), `'`, `''`)
	script := fmt.Sprintf(`$ErrorActionPreference='Stop';$s=New-Object -ComObject 'Schedule.Service';$s.Connect();$t=$s.GetFolder('\').GetTask('%s');[Console]::Out.Write(($t.GetInstances(0)|ForEach-Object {[string]$_.EnginePID}) -join ',')`, name)
	ctx, cancel := context.WithTimeout(context.Background(), taskCommandTimeout)
	defer cancel()
	output, err := exec.CommandContext(ctx, executable, "-NoLogo", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command", script).Output()
	if ctx.Err() != nil {
		return nil, fmt.Errorf("query scheduled task process IDs: %w", ctx.Err())
	}
	if err != nil {
		return nil, fmt.Errorf("query scheduled task process IDs: %w", err)
	}
	text := strings.TrimSpace(string(output))
	if text == "" {
		return nil, nil
	}
	var pids []int
	for _, item := range strings.Split(text, ",") {
		pid, err := strconv.Atoi(strings.TrimSpace(item))
		if err != nil || pid <= 0 || slices.Contains(pids, pid) {
			return nil, errors.New("Task Scheduler returned invalid engine process IDs")
		}
		pids = append(pids, pid)
	}
	return pids, nil
}

func executeTaskkill(pid int) (taskCommandResult, error) {
	directory, err := windowsSystemDirectory()
	if err != nil {
		return taskCommandResult{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), taskCommandTimeout)
	defer cancel()
	command := exec.CommandContext(ctx, filepath.Join(directory, "taskkill.exe"), "/PID", strconv.Itoa(pid), "/T", "/F")
	output, err := command.CombinedOutput()
	if ctx.Err() != nil {
		return taskCommandResult{}, ctx.Err()
	}
	if err == nil {
		return taskCommandResult{Output: output}, nil
	}
	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		return taskCommandResult{Output: output, ExitCode: exitError.ExitCode()}, nil
	}
	return taskCommandResult{}, err
}

func windowsSystemDirectory() (string, error) {
	return taskSchedulerExecutableDirectory()
}

func taskSchedulerExecutableDirectory() (string, error) {
	path, err := taskSchedulerExecutable()
	if err != nil {
		return "", err
	}
	return filepath.Dir(path), nil
}

func taskkillAlreadyExited(output []byte) bool {
	text := strings.ToLower(string(bytes.TrimSpace(output)))
	return strings.Contains(text, "not found") || strings.Contains(text, "no running instance")
}

func sameProcessIDs(left, right []int) bool {
	left = slices.Clone(left)
	right = slices.Clone(right)
	slices.Sort(left)
	slices.Sort(right)
	return slices.Equal(left, right)
}

func upgradeDefinitionPairFirst(plan UpgradePlan, target bool) []byte {
	expected, _ := upgradeDefinitionPair(plan, target)
	return expected
}

func waitWindowsUpgradePoll(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
