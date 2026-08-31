package localupgrade

import (
	"context"
	"errors"
	"os"
	"path/filepath"

	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/securefs"
	"github.com/GhostFlying/delegation/internal/userservice"
)

func platformInstallOneShot(ctx context.Context, journal Journal, upgradeRoot string) error {
	plan, err := oneShotPlan(journal, upgradeRoot)
	if err != nil {
		return err
	}
	if err := ensureOneShotDefinition(journal.ActivatorPath, plan.Definition); err != nil {
		return err
	}
	return userservice.InstallUpgradeActivator(ctx, plan)
}

func platformLaunchOneShot(ctx context.Context, journal Journal, upgradeRoot string) error {
	plan, err := oneShotPlan(journal, upgradeRoot)
	if err != nil {
		return err
	}
	if err := requireOneShotDefinition(journal.ActivatorPath, plan.Definition); err != nil {
		return err
	}
	return userservice.LaunchUpgradeActivator(ctx, plan)
}

func platformRemoveOneShot(ctx context.Context, journal Journal, upgradeRoot string) error {
	plan, err := oneShotPlan(journal, upgradeRoot)
	if err != nil {
		return err
	}
	if err := requireOneShotDefinitionOrAbsent(journal.ActivatorPath, plan.Definition); err != nil {
		return err
	}
	if err := userservice.RemoveUpgradeActivator(ctx, plan); err != nil {
		return err
	}
	return removeOneShotDefinition(journal.ActivatorPath, plan.Definition)
}

func oneShotPlan(journal Journal, upgradeRoot string) (userservice.UpgradeActivatorPlan, error) {
	if filepath.Clean(upgradeRoot) != upgradeRoot || !filepath.IsAbs(upgradeRoot) {
		return userservice.UpgradeActivatorPlan{}, errors.New("upgrade root must be absolute and clean")
	}
	transactionRoot := filepath.Join(upgradeRoot, "transactions", journal.TransactionID)
	if filepath.Dir(journal.ActivatorPath) != transactionRoot {
		return userservice.UpgradeActivatorPlan{}, errors.New("activator definition is outside the protected transaction root")
	}
	return userservice.PrepareUpgradeActivator(
		journal.Invocation.TargetBinaryPath, upgradeRoot, journal.TransactionID,
		journal.ActivatorPath, journal.Invocation.UserIdentity,
	)
}

func ensureOneShotDefinition(path string, expected []byte) error {
	if err := requireOneShotDefinition(path, expected); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return writeProtectedMaterial(filepath.Dir(path), filepath.Base(path), expected)
}

func requireOneShotDefinition(path string, expected []byte) error {
	actual, err := delegationconfig.ReadProtectedFile(path, maximumDefinitionBytes)
	if err != nil {
		return err
	}
	if !equalBytes(actual, expected) {
		return errors.New("protected activator definition changed")
	}
	return nil
}

func requireOneShotDefinitionOrAbsent(path string, expected []byte) error {
	err := requireOneShotDefinition(path, expected)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func removeOneShotDefinition(path string, expected []byte) error {
	if err := requireOneShotDefinitionOrAbsent(path, expected); err != nil {
		return err
	}
	root, err := securefs.OpenRoot(filepath.Dir(path), nil)
	if err != nil {
		return err
	}
	defer root.Close()
	if _, err := root.Lstat(filepath.Base(path)); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	if err := root.Remove(filepath.Base(path)); err != nil {
		return err
	}
	return root.Sync()
}

func equalBytes(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
