package userservice

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/GhostFlying/delegation/internal/identity"
)

// UpgradeActivatorPlan is the deterministic native definition for an
// independent, one-shot local upgrade activator.
type UpgradeActivatorPlan struct {
	Kind           Kind
	Name           string
	DefinitionPath string
	Definition     []byte
	BinaryPath     string
	UpgradeRoot    string
	TransactionID  string
	UserIdentity   string
}

func PrepareUpgradeActivator(
	binaryPath, upgradeRoot, transactionID, definitionPath, userIdentity string,
) (UpgradeActivatorPlan, error) {
	if err := identity.ValidateID(transactionID); err != nil {
		return UpgradeActivatorPlan{}, fmt.Errorf("transactionId %w", err)
	}
	for name, candidate := range map[string]string{
		"binary": binaryPath, "upgrade root": upgradeRoot, "definition": definitionPath,
	} {
		if candidate == "" || !filepath.IsAbs(candidate) || filepath.Clean(candidate) != candidate {
			return UpgradeActivatorPlan{}, fmt.Errorf("activator %s path must be absolute and clean", name)
		}
	}
	if userIdentity == "" {
		return UpgradeActivatorPlan{}, errors.New("activator user identity is required")
	}
	plan := UpgradeActivatorPlan{
		BinaryPath: binaryPath, UpgradeRoot: upgradeRoot, TransactionID: transactionID,
		DefinitionPath: definitionPath, UserIdentity: userIdentity,
	}
	return platformPrepareUpgradeActivator(plan)
}

func InstallUpgradeActivator(ctx context.Context, plan UpgradeActivatorPlan) error {
	if err := validateUpgradeActivatorPlan(plan); err != nil {
		return err
	}
	return platformInstallUpgradeActivator(ctx, plan)
}

func LaunchUpgradeActivator(ctx context.Context, plan UpgradeActivatorPlan) error {
	if err := validateUpgradeActivatorPlan(plan); err != nil {
		return err
	}
	return platformLaunchUpgradeActivator(ctx, plan)
}

func RemoveUpgradeActivator(ctx context.Context, plan UpgradeActivatorPlan) error {
	if err := validateUpgradeActivatorPlan(plan); err != nil {
		return err
	}
	return platformRemoveUpgradeActivator(ctx, plan)
}

func validateUpgradeActivatorPlan(plan UpgradeActivatorPlan) error {
	recreated, err := PrepareUpgradeActivator(
		plan.BinaryPath, plan.UpgradeRoot, plan.TransactionID, plan.DefinitionPath, plan.UserIdentity,
	)
	if err != nil {
		return err
	}
	if plan.Kind != recreated.Kind || plan.Name != recreated.Name ||
		!bytes.Equal(plan.Definition, recreated.Definition) {
		return errors.New("upgrade activator plan changed after preparation")
	}
	return nil
}
