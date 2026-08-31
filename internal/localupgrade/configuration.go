package localupgrade

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/securefs"
)

func PrepareConfiguration(configuration Configuration) (Configuration, error) {
	if err := validateConfigurationPaths(configuration); err != nil {
		return Configuration{}, err
	}
	root, err := securefs.OpenRoot(filepath.Dir(configuration.CanonicalPath), nil)
	if err != nil {
		return Configuration{}, err
	}
	defer root.Close()
	canonical := filepath.Base(configuration.CanonicalPath)
	shadow := filepath.Base(configuration.ShadowPath)
	rollback := filepath.Base(configuration.RollbackPath)
	currentDigest, err := digestAt(root, canonical)
	if err != nil {
		return Configuration{}, err
	}
	if configuration.SourceDigest != configuration.TargetDigest &&
		currentDigest == configuration.TargetDigest {
		if err := requireDigestAt(root, rollback, configuration.SourceDigest); err != nil {
			return Configuration{}, fmt.Errorf("validate configuration rollback material: %w", err)
		}
		return configuration, nil
	}
	if currentDigest != configuration.SourceDigest {
		return Configuration{}, errors.New("canonical configuration differs from the recorded source or target")
	}
	sourceData, err := delegationconfig.ReadProtectedFile(configuration.SourcePath, maximumConfigDigestFile)
	if err != nil {
		return Configuration{}, fmt.Errorf("read protected source configuration: %w", err)
	}
	if digestBytes(sourceData) != configuration.SourceDigest {
		return Configuration{}, errors.New("protected source configuration digest changed")
	}
	targetData, err := delegationconfig.ReadProtectedFile(configuration.TargetPath, maximumConfigDigestFile)
	if err != nil {
		return Configuration{}, fmt.Errorf("read protected target configuration: %w", err)
	}
	if digestBytes(targetData) != configuration.TargetDigest {
		return Configuration{}, errors.New("protected target configuration digest changed")
	}
	if err := ensureFileBytes(root, rollback, sourceData, configuration.SourceDigest); err != nil {
		return Configuration{}, fmt.Errorf("prepare configuration rollback: %w", err)
	}
	if err := ensureFileBytes(root, shadow, targetData, configuration.TargetDigest); err != nil {
		return Configuration{}, fmt.Errorf("prepare configuration shadow: %w", err)
	}
	return configuration, nil
}

func ReconcileConfigurationSwitch(configuration Configuration) (bool, error) {
	return reconcileConfiguration(configuration, configuration.ShadowPath, configuration.TargetDigest)
}

func ReconcileConfigurationRollback(configuration Configuration) (bool, error) {
	return reconcileConfiguration(configuration, configuration.RollbackPath, configuration.SourceDigest)
}

func reconcileConfiguration(
	configuration Configuration, materialPath, expectedDigest string,
) (bool, error) {
	if err := validateConfigurationPaths(configuration); err != nil {
		return false, err
	}
	root, err := securefs.OpenRoot(filepath.Dir(configuration.CanonicalPath), nil)
	if err != nil {
		return false, err
	}
	defer root.Close()
	canonical := filepath.Base(configuration.CanonicalPath)
	material := filepath.Base(materialPath)
	if digest, err := digestAt(root, canonical); err == nil && digest == expectedDigest {
		return false, nil
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	if err := requireDigestAt(root, material, expectedDigest); err != nil {
		return false, err
	}
	temporary := fmt.Sprintf(".%s-upgrade-%d.tmp", canonical, time.Now().UnixNano())
	if err := copyDatabase(root, material, temporary); err != nil {
		return false, err
	}
	committed, replaceErr := root.Replace(temporary, canonical)
	if !committed {
		_ = root.Remove(temporary)
	}
	if replaceErr != nil {
		after, digestErr := digestAt(root, canonical)
		if after != expectedDigest {
			return false, errors.Join(replaceErr, digestErr)
		}
	}
	if err := requireDigestAt(root, canonical, expectedDigest); err != nil {
		return false, err
	}
	return true, nil
}

func ensureFileBytes(root *securefs.Root, destination string, data []byte, digest string) error {
	if _, err := root.Lstat(destination); err == nil {
		return requireDigestAt(root, destination, digest)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	file, err := root.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	writeErr := writeAll(file, data)
	if writeErr == nil {
		writeErr = file.Sync()
	}
	closeErr := file.Close()
	if err := errors.Join(writeErr, closeErr); err != nil {
		_ = root.Remove(destination)
		return err
	}
	if err := root.Sync(); err != nil {
		return err
	}
	return requireDigestAt(root, destination, digest)
}

func requireDigestAt(root *securefs.Root, name, expected string) error {
	actual, err := digestAt(root, name)
	if err != nil {
		return err
	}
	if actual != expected {
		return errors.New("protected configuration material digest changed")
	}
	return nil
}

func validateConfigurationPaths(configuration Configuration) error {
	paths := []string{
		configuration.CanonicalPath, configuration.SourcePath, configuration.TargetPath,
		configuration.ShadowPath, configuration.RollbackPath,
	}
	for _, path := range paths {
		if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
			return errors.New("configuration paths must be absolute and clean")
		}
	}
	if filepath.Dir(configuration.CanonicalPath) != filepath.Dir(configuration.ShadowPath) ||
		filepath.Dir(configuration.CanonicalPath) != filepath.Dir(configuration.RollbackPath) ||
		configuration.CanonicalPath == configuration.ShadowPath ||
		configuration.CanonicalPath == configuration.RollbackPath ||
		configuration.ShadowPath == configuration.RollbackPath {
		return errors.New("configuration switch material must use distinct names in one directory")
	}
	for _, digest := range []string{configuration.SourceDigest, configuration.TargetDigest} {
		if !validDigest(digest) {
			return errors.New("configuration digest is invalid")
		}
	}
	return nil
}
