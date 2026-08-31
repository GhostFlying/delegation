//go:build darwin

package userservice

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
)

func platformValidateUpgradePlan(plan UpgradePlan, oldDescriptor, newDescriptor Descriptor) error {
	if plan.Artifact != filepath.Clean(plan.Artifact) ||
		!bytes.Equal(plan.OldDefinition, oldDescriptor.Content) ||
		!bytes.Equal(plan.NewDefinition, newDescriptor.Content) {
		return errors.New("LaunchAgent upgrade plan definition changed")
	}
	return nil
}

func platformInspectUpgrade(ctx context.Context, plan UpgradePlan) (UpgradePlan, error) {
	if err := ctx.Err(); err != nil {
		return UpgradePlan{}, err
	}
	artifact, err := darwinServicePath(plan.Role, plan.SourceInvocation.InstanceID)
	if err != nil {
		return UpgradePlan{}, err
	}
	state, content, err := inspectManagedFile(artifact, KindLaunchAgent)
	if err != nil {
		return UpgradePlan{}, err
	}
	if state != StatePrepared || !bytes.Equal(content, plan.OldDefinition) {
		return UpgradePlan{}, errors.New("LaunchAgent definition is not the exact managed source definition")
	}
	target := launchAgentUpgradeTarget(plan.NativeName)
	status, loaded, err := printLaunchAgent(target)
	if err != nil {
		return UpgradePlan{}, err
	}
	if !loaded || filepath.Clean(status.Path) != filepath.Clean(artifact) ||
		status.State != "running" || status.PID <= 0 ||
		!launchAgentStatusMatchesInvocation(status, plan.SourceInvocation) {
		return UpgradePlan{}, errors.New("LaunchAgent is not exact and running")
	}
	plan.Artifact = filepath.Clean(artifact)
	plan.UserIdentity = strconv.Itoa(os.Geteuid())
	plan.ProcessIDs = []int{status.PID}
	if err := validateUpgradePlan(plan); err != nil {
		return UpgradePlan{}, err
	}
	return plan, nil
}

func platformStopUpgrade(ctx context.Context, plan UpgradePlan) error {
	if err := requireLaunchAgentDefinition(plan, nil); err != nil {
		return err
	}
	target := launchAgentUpgradeTarget(plan.NativeName)
	status, loaded, err := printLaunchAgent(target)
	if err != nil {
		return err
	}
	if !loaded {
		return nil
	}
	if filepath.Clean(status.Path) != filepath.Clean(plan.Artifact) ||
		(!launchAgentStatusMatchesInvocation(status, plan.SourceInvocation) &&
			!launchAgentStatusMatchesInvocation(status, plan.TargetInvocation)) {
		return errors.New("loaded LaunchAgent identity differs from the upgrade transaction")
	}
	if len(plan.ProcessIDs) != 1 || status.PID != plan.ProcessIDs[0] {
		return errors.New("loaded LaunchAgent process changed since upgrade preparation")
	}
	result, runErr := runLaunchctl("bootout", target)
	if runErr != nil || result.ExitCode != 0 {
		return errors.Join(runErr, commandFailure("unload LaunchAgent", result))
	}
	done := make(chan error, 1)
	go func() { done <- waitForLaunchAgentUnloaded(target, plan.Artifact) }()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-done:
		return err
	}
}

func platformSwitchUpgradeDefinition(ctx context.Context, plan UpgradePlan, target bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, loaded, err := printLaunchAgent(launchAgentUpgradeTarget(plan.NativeName)); err != nil {
		return err
	} else if loaded {
		return errors.New("LaunchAgent must be unloaded before definition replacement")
	}
	expected, replacement := upgradeDefinitionPair(plan, target)
	if err := replaceManagedFile(plan.Artifact, plan.Kind, expected, replacement); err != nil {
		return err
	}
	return requireLaunchAgentDefinition(plan, replacement)
}

func platformStartUpgrade(ctx context.Context, plan UpgradePlan, targetVersion bool) error {
	expected := upgradeDefinition(plan, targetVersion)
	invocation := upgradeInvocation(plan, targetVersion)
	if err := requireLaunchAgentDefinition(plan, expected); err != nil {
		return err
	}
	domain := fmt.Sprintf("gui/%d", os.Geteuid())
	target := launchAgentUpgradeTarget(plan.NativeName)
	if status, loaded, err := printLaunchAgent(target); err != nil {
		return err
	} else if loaded {
		if filepath.Clean(status.Path) != filepath.Clean(plan.Artifact) ||
			!launchAgentStatusMatchesInvocation(status, invocation) {
			return errors.New("loaded LaunchAgent conflicts with upgrade transaction")
		}
	} else {
		result, err := runLaunchctl("bootstrap", domain, plan.Artifact)
		if err != nil || result.ExitCode != 0 {
			return errors.Join(err, commandFailure("bootstrap LaunchAgent", result))
		}
	}
	if result, err := runLaunchctl("enable", target); err != nil || result.ExitCode != 0 {
		return errors.Join(err, commandFailure("enable LaunchAgent", result))
	}
	if result, err := runLaunchctl("kickstart", target); err != nil || result.ExitCode != 0 {
		return errors.Join(err, commandFailure("start LaunchAgent", result))
	}
	result := Result{State: StatePrepared, Kind: plan.Kind, Artifact: plan.Artifact, Role: plan.Role}
	done := make(chan error, 1)
	go func() { done <- waitForLaunchAgentRunning(target, result, invocation) }()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-done:
		if err != nil {
			return err
		}
	}
	if err := waitForDarwinServiceReady(invocation.ConfigPath); err != nil {
		return fmt.Errorf("LaunchAgent did not become ready: %w", err)
	}
	return nil
}

func platformUpgradeServiceMatches(ctx context.Context, plan UpgradePlan, targetVersion bool) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := requireLaunchAgentDefinition(plan, upgradeDefinition(plan, targetVersion)); err != nil {
		return false, nil
	}
	status, loaded, err := printLaunchAgent(launchAgentUpgradeTarget(plan.NativeName))
	if err != nil {
		return false, err
	}
	return loaded && filepath.Clean(status.Path) == filepath.Clean(plan.Artifact) &&
		status.State == "running" && status.PID > 0 &&
		launchAgentStatusMatchesInvocation(status, upgradeInvocation(plan, targetVersion)), nil
}

func requireLaunchAgentDefinition(plan UpgradePlan, expected []byte) error {
	state, current, err := inspectManagedFile(plan.Artifact, plan.Kind)
	if err != nil {
		return err
	}
	if state != StatePrepared {
		return errors.New("LaunchAgent definition is absent or not managed")
	}
	if expected == nil {
		if bytes.Equal(current, plan.OldDefinition) || bytes.Equal(current, plan.NewDefinition) {
			return nil
		}
	} else if bytes.Equal(current, expected) {
		return nil
	}
	return errors.New("LaunchAgent definition does not match the upgrade journal")
}

func launchAgentStatusMatchesInvocation(status launchAgentStatus, invocation Invocation) bool {
	if status.Program != "" && filepath.Clean(status.Program) != filepath.Clean(invocation.BinaryPath) {
		return false
	}
	if status.ArgumentsPresent {
		return slices.Equal(status.Arguments, launchAgentArguments(invocation))
	}
	return status.Program != ""
}

func launchAgentUpgradeTarget(name string) string {
	return fmt.Sprintf("gui/%d/%s", os.Geteuid(), name)
}
