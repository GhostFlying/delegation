//go:build linux

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
	"strings"
	"time"
)

const linuxUpgradeStopTimeout = 30 * time.Second

var linuxUpgradePollInterval = 100 * time.Millisecond

type systemdUpgradeStatus struct {
	FragmentPath  string
	DropInPaths   string
	ControlGroup  string
	MainPID       int
	ControlPID    int
	ActiveState   string
	UnitFileState string
}

func platformValidateUpgradePlan(plan UpgradePlan, oldDescriptor, newDescriptor Descriptor) error {
	if plan.Artifact != filepath.Clean(plan.Artifact) ||
		plan.ProcessGroup == "" || len(plan.ProcessIDs) == 0 ||
		!bytes.Equal(plan.OldDefinition, oldDescriptor.Content) ||
		!bytes.Equal(plan.NewDefinition, newDescriptor.Content) {
		return errors.New("systemd upgrade plan definition changed")
	}
	return nil
}

func platformInspectUpgrade(ctx context.Context, plan UpgradePlan) (UpgradePlan, error) {
	artifact, err := linuxServicePath(plan.Role, plan.SourceInvocation.InstanceID)
	if err != nil {
		return UpgradePlan{}, err
	}
	state, content, err := inspectManagedFile(artifact, KindSystemd)
	if err != nil {
		return UpgradePlan{}, err
	}
	if state != StatePrepared || !bytes.Equal(content, plan.OldDefinition) {
		return UpgradePlan{}, errors.New("systemd service definition is not the exact managed source definition")
	}
	status, err := inspectSystemdUpgradeStatus(ctx, plan.NativeName)
	if err != nil {
		return UpgradePlan{}, err
	}
	if filepath.Clean(status.FragmentPath) != filepath.Clean(artifact) ||
		strings.TrimSpace(status.DropInPaths) != "" || status.ActiveState != "active" ||
		(status.UnitFileState != "enabled" && status.UnitFileState != "enabled-runtime") ||
		status.MainPID <= 0 || status.ControlGroup == "" {
		return UpgradePlan{}, errors.New("systemd service is not exact, enabled, and running")
	}
	plan.Artifact = filepath.Clean(artifact)
	plan.UserIdentity = strconv.Itoa(os.Geteuid())
	plan.ProcessIDs = nonzeroPIDs(status.MainPID, status.ControlPID)
	plan.ProcessGroup = status.ControlGroup
	if err := validateUpgradePlan(plan); err != nil {
		return UpgradePlan{}, err
	}
	return plan, nil
}

func platformStopUpgrade(ctx context.Context, plan UpgradePlan) error {
	if err := requireSystemdDefinition(plan); err != nil {
		return err
	}
	status, err := inspectSystemdUpgradeStatus(ctx, plan.NativeName)
	if err != nil {
		return err
	}
	if err := validateSystemdManagerIdentity(status, plan); err != nil {
		return err
	}
	if status.ActiveState == "active" {
		if status.ControlGroup != plan.ProcessGroup ||
			!slices.Equal(nonzeroPIDs(status.MainPID, status.ControlPID), plan.ProcessIDs) {
			return errors.New("systemd running process identity changed since upgrade preparation")
		}
	} else if status.ActiveState == "inactive" {
		if status.MainPID != 0 || status.ControlPID != 0 {
			return errors.New("inactive systemd service still reports managed processes")
		}
	} else if status.ControlGroup != plan.ProcessGroup {
		return errors.New("systemd process group changed while stopping upgrade service")
	}
	if status.ActiveState != "inactive" {
		result, runErr := runSystemctl("--user", "--no-ask-password", "stop", plan.NativeName)
		if runErr != nil || result.ExitCode != 0 {
			return errors.Join(runErr, commandFailure("stop systemd user service", result))
		}
	}
	deadline := time.Now().Add(linuxUpgradeStopTimeout)
	for {
		status, err = inspectSystemdUpgradeStatus(ctx, plan.NativeName)
		if err != nil {
			return err
		}
		if err := validateSystemdManagerIdentity(status, plan); err != nil {
			return err
		}
		if status.ActiveState == "inactive" && status.MainPID == 0 && status.ControlPID == 0 {
			return nil
		}
		if !time.Now().Before(deadline) {
			return errors.New("systemd managed process tree did not stop before timeout")
		}
		if err := waitUpgradePoll(ctx, linuxUpgradePollInterval); err != nil {
			return err
		}
	}
}

func platformSwitchUpgradeDefinition(_ context.Context, plan UpgradePlan, target bool) error {
	status, err := inspectSystemdUpgradeStatus(context.Background(), plan.NativeName)
	if err != nil {
		return err
	}
	if err := validateSystemdManagerIdentity(status, plan); err != nil {
		return err
	}
	if status.ActiveState != "inactive" || status.MainPID != 0 || status.ControlPID != 0 {
		return errors.New("systemd service must be stopped before definition replacement")
	}
	expected, replacement := upgradeDefinitionPair(plan, target)
	if err := replaceManagedFile(plan.Artifact, plan.Kind, expected, replacement); err != nil {
		return err
	}
	result, runErr := runSystemctl("--user", "--no-ask-password", "daemon-reload")
	if runErr != nil || result.ExitCode != 0 {
		return errors.Join(runErr, commandFailure("reload systemd user manager", result))
	}
	return requireSystemdDefinitionBytes(plan, replacement)
}

func platformStartUpgrade(ctx context.Context, plan UpgradePlan, target bool) error {
	want := plan.SourceInvocation
	if target {
		want = plan.TargetInvocation
	}
	if err := requireSystemdDefinitionBytes(plan, upgradeDefinition(plan, target)); err != nil {
		return err
	}
	result, runErr := runSystemctl("--user", "--no-ask-password", "start", plan.NativeName)
	if runErr != nil || result.ExitCode != 0 {
		return errors.Join(runErr, commandFailure("start systemd user service", result))
	}
	if err := waitForLinuxServiceReady(want.ConfigPath); err != nil {
		return fmt.Errorf("systemd user service did not become ready: %w", err)
	}
	matched, err := platformUpgradeServiceMatches(ctx, plan, target)
	if err != nil {
		return err
	}
	if !matched {
		return errors.New("systemd service did not start with the requested upgrade definition")
	}
	return nil
}

func platformUpgradeServiceMatches(ctx context.Context, plan UpgradePlan, target bool) (bool, error) {
	if err := requireSystemdDefinitionBytes(plan, upgradeDefinition(plan, target)); err != nil {
		return false, nil
	}
	status, err := inspectSystemdUpgradeStatus(ctx, plan.NativeName)
	if err != nil {
		return false, err
	}
	if err := validateSystemdManagerIdentity(status, plan); err != nil {
		return false, err
	}
	return status.ActiveState == "active" && status.MainPID > 0 &&
		(status.UnitFileState == "enabled" || status.UnitFileState == "enabled-runtime"), nil
}

func inspectSystemdUpgradeStatus(ctx context.Context, name string) (systemdUpgradeStatus, error) {
	if err := ctx.Err(); err != nil {
		return systemdUpgradeStatus{}, err
	}
	result, err := runSystemctl(
		"--user", "--no-ask-password", "show", name,
		"--property=FragmentPath", "--property=DropInPaths", "--property=ControlGroup",
		"--property=MainPID", "--property=ControlPID", "--property=ActiveState",
		"--property=UnitFileState",
	)
	if err != nil || result.ExitCode != 0 {
		return systemdUpgradeStatus{}, errors.Join(err, commandFailure("inspect systemd upgrade service", result))
	}
	properties := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(string(result.Output)), "\n") {
		key, value, found := strings.Cut(line, "=")
		if !found || key == "" || properties[key] != "" {
			return systemdUpgradeStatus{}, errors.New("systemd returned malformed upgrade identity")
		}
		properties[key] = value
	}
	if len(properties) != 7 {
		return systemdUpgradeStatus{}, errors.New("systemd omitted upgrade identity properties")
	}
	mainPID, err := strconv.Atoi(properties["MainPID"])
	if err != nil || mainPID < 0 {
		return systemdUpgradeStatus{}, errors.New("systemd returned invalid MainPID")
	}
	controlPID, err := strconv.Atoi(properties["ControlPID"])
	if err != nil || controlPID < 0 {
		return systemdUpgradeStatus{}, errors.New("systemd returned invalid ControlPID")
	}
	return systemdUpgradeStatus{
		FragmentPath: properties["FragmentPath"], DropInPaths: properties["DropInPaths"],
		ControlGroup: properties["ControlGroup"], MainPID: mainPID, ControlPID: controlPID,
		ActiveState: properties["ActiveState"], UnitFileState: properties["UnitFileState"],
	}, nil
}

func validateSystemdManagerIdentity(status systemdUpgradeStatus, plan UpgradePlan) error {
	if filepath.Clean(status.FragmentPath) != filepath.Clean(plan.Artifact) ||
		strings.TrimSpace(status.DropInPaths) != "" {
		return errors.New("systemd unit is shadowed or has drop-in overrides")
	}
	return nil
}

func requireSystemdDefinition(plan UpgradePlan) error {
	state, current, err := inspectManagedFile(plan.Artifact, plan.Kind)
	if err != nil {
		return err
	}
	if state != StatePrepared || (!bytes.Equal(current, plan.OldDefinition) && !bytes.Equal(current, plan.NewDefinition)) {
		return errors.New("systemd service definition changed outside the upgrade transaction")
	}
	return nil
}

func requireSystemdDefinitionBytes(plan UpgradePlan, expected []byte) error {
	state, current, err := inspectManagedFile(plan.Artifact, plan.Kind)
	if err != nil {
		return err
	}
	if state != StatePrepared || !bytes.Equal(current, expected) {
		return errors.New("systemd service definition does not match the upgrade journal")
	}
	return nil
}

func nonzeroPIDs(values ...int) []int {
	result := make([]int, 0, len(values))
	for _, value := range values {
		if value > 0 {
			result = append(result, value)
		}
	}
	return result
}

func waitUpgradePoll(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
