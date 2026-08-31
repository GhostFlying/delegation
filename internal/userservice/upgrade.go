package userservice

import (
	"bytes"
	"context"
	"errors"
	"fmt"
)

// UpgradePlan is the immutable service-manager identity captured while the
// source service is still running. Definition bytes are kept so every later
// mutation can compare the live manager state before changing it.
type UpgradePlan struct {
	Role             ServiceRole
	Kind             Kind
	NativeName       string
	Artifact         string
	UserIdentity     string
	SourceInvocation Invocation
	TargetInvocation Invocation
	OldDefinition    []byte
	NewDefinition    []byte
	ProcessIDs       []int
	ProcessGroup     string
}

// PrepareUpgrade inspects an active managed service and proves that the new
// definition changes only its runtime executable. It is read-only.
func PrepareUpgrade(
	ctx context.Context, role ServiceRole, source, target Invocation,
) (UpgradePlan, error) {
	if source.ConfigPath != target.ConfigPath ||
		source.EnvironmentFile != target.EnvironmentFile ||
		source.InstanceID != target.InstanceID {
		return UpgradePlan{}, errors.New("upgrade must preserve config, environment, and instance identity")
	}
	if source.BinaryPath == target.BinaryPath {
		return UpgradePlan{}, errors.New("upgrade target runtime must differ from the source runtime")
	}
	oldDescriptor, err := renderPlatformDescriptor(role, source)
	if err != nil {
		return UpgradePlan{}, err
	}
	newDescriptor, err := renderPlatformDescriptor(role, target)
	if err != nil {
		return UpgradePlan{}, err
	}
	if oldDescriptor.Kind != newDescriptor.Kind || oldDescriptor.Name != newDescriptor.Name {
		return UpgradePlan{}, errors.New("upgrade changed native service identity")
	}
	plan := UpgradePlan{
		Role: role, Kind: oldDescriptor.Kind, NativeName: oldDescriptor.Name,
		SourceInvocation: source, TargetInvocation: target,
		OldDefinition: append([]byte(nil), oldDescriptor.Content...),
		NewDefinition: append([]byte(nil), newDescriptor.Content...),
	}
	return platformInspectUpgrade(ctx, plan)
}

// StopUpgrade stops only the exact service captured by PrepareUpgrade and
// waits for its managed process tree to leave. Repeated calls reconcile an
// already stopped service.
func StopUpgrade(ctx context.Context, plan UpgradePlan) error {
	if err := validateUpgradePlan(plan); err != nil {
		return err
	}
	return platformStopUpgrade(ctx, plan)
}

// SwitchUpgradeDefinition compare-and-replaces the managed definition. A
// repeated call succeeds when the requested definition is already installed.
func SwitchUpgradeDefinition(ctx context.Context, plan UpgradePlan, target bool) error {
	if err := validateUpgradePlan(plan); err != nil {
		return err
	}
	return platformSwitchUpgradeDefinition(ctx, plan, target)
}

// StartUpgrade starts and validates either the target or source definition.
// The latter is used only by pre-authorization rollback.
func StartUpgrade(ctx context.Context, plan UpgradePlan, target bool) error {
	if err := validateUpgradePlan(plan); err != nil {
		return err
	}
	return platformStartUpgrade(ctx, plan, target)
}

// UpgradeServiceMatches reports whether the manager currently owns the exact
// requested definition and running process identity.
func UpgradeServiceMatches(ctx context.Context, plan UpgradePlan, target bool) (bool, error) {
	if err := validateUpgradePlan(plan); err != nil {
		return false, err
	}
	return platformUpgradeServiceMatches(ctx, plan, target)
}

func validateUpgradePlan(plan UpgradePlan) error {
	if plan.Role != ServiceRoleBroker && plan.Role != ServiceRolePeer {
		return fmt.Errorf("unsupported upgrade service role %q", plan.Role)
	}
	if plan.NativeName == "" || plan.Artifact == "" || plan.UserIdentity == "" ||
		len(plan.OldDefinition) == 0 || len(plan.NewDefinition) == 0 {
		return errors.New("upgrade service plan is incomplete")
	}
	if bytes.Equal(plan.OldDefinition, plan.NewDefinition) {
		return errors.New("upgrade service definitions must differ")
	}
	oldDescriptor, err := renderPlatformDescriptor(plan.Role, plan.SourceInvocation)
	if err != nil {
		return err
	}
	newDescriptor, err := renderPlatformDescriptor(plan.Role, plan.TargetInvocation)
	if err != nil {
		return err
	}
	if plan.Kind != oldDescriptor.Kind || plan.Kind != newDescriptor.Kind ||
		plan.NativeName != oldDescriptor.Name || plan.NativeName != newDescriptor.Name {
		return errors.New("upgrade service plan does not match its immutable invocation")
	}
	return platformValidateUpgradePlan(plan, oldDescriptor, newDescriptor)
}

func upgradeDefinition(plan UpgradePlan, target bool) []byte {
	if target {
		return plan.NewDefinition
	}
	return plan.OldDefinition
}

func upgradeInvocation(plan UpgradePlan, target bool) Invocation {
	if target {
		return plan.TargetInvocation
	}
	return plan.SourceInvocation
}

func upgradeDefinitionPair(plan UpgradePlan, target bool) ([]byte, []byte) {
	if target {
		return plan.OldDefinition, plan.NewDefinition
	}
	return plan.NewDefinition, plan.OldDefinition
}
