package localupgrade

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/GhostFlying/delegation/internal/securefs"
	"github.com/GhostFlying/delegation/internal/store"
)

type DatabaseMigrator func(context.Context, string, store.DatabaseIdentity) error

func PrepareDatabase(
	ctx context.Context, database Database, migrate DatabaseMigrator,
) (Database, error) {
	if err := validateDatabasePaths(database); err != nil {
		return Database{}, err
	}
	root, err := store.OpenUpgradeDatabaseRoot(database.CanonicalPath)
	if err != nil {
		return Database{}, err
	}
	defer root.Close()
	canonicalIdentity, err := store.InspectUpgradeDatabase(ctx, database.CanonicalPath, database.Kind)
	if err != nil {
		return Database{}, err
	}
	canonicalName := filepath.Base(database.CanonicalPath)
	shadowName := filepath.Base(database.ShadowPath)
	rollbackName := filepath.Base(database.RollbackPath)
	canonicalDigest, err := digestAt(root, canonicalName)
	if err != nil {
		return Database{}, err
	}
	rollbackExists, rollbackDigest, err := existingMaterial(root, rollbackName)
	if err != nil {
		return Database{}, err
	}
	if canonicalIdentity == database.TargetIdentity && database.TargetDigest != "" &&
		canonicalDigest == database.TargetDigest {
		if !rollbackExists || database.SourceDigest == "" || rollbackDigest != database.SourceDigest {
			return Database{}, errors.New("already-switched database lacks its recorded rollback material")
		}
		if err := store.ValidateUpgradeDatabase(ctx, database.RollbackPath, database.Kind, database.SourceIdentity); err != nil {
			return Database{}, fmt.Errorf("validate existing rollback database: %w", err)
		}
		return database, nil
	}
	if canonicalIdentity != database.SourceIdentity {
		return Database{}, fmt.Errorf(
			"canonical database identity is application ID %d schema %d, expected source %d/%d or target %d/%d",
			canonicalIdentity.ApplicationID, canonicalIdentity.SchemaVersion,
			database.SourceIdentity.ApplicationID, database.SourceIdentity.SchemaVersion,
			database.TargetIdentity.ApplicationID, database.TargetIdentity.SchemaVersion,
		)
	}
	if database.SourceDigest != "" && canonicalDigest != database.SourceDigest {
		return Database{}, errors.New("canonical database differs from the recorded source")
	}
	if _, err := store.CheckpointUpgradeDatabase(
		ctx, database.CanonicalPath, database.Kind, database.SourceIdentity,
	); err != nil {
		return Database{}, err
	}
	canonicalDigest, err = digestAt(root, canonicalName)
	if err != nil {
		return Database{}, err
	}
	if database.SourceDigest != "" && canonicalDigest != database.SourceDigest {
		return Database{}, errors.New("checkpointed database differs from the recorded source")
	}
	database.SourceDigest = canonicalDigest
	if rollbackExists {
		if err := store.ValidateUpgradeDatabase(ctx, database.RollbackPath, database.Kind, database.SourceIdentity); err != nil {
			return Database{}, fmt.Errorf("validate existing rollback database: %w", err)
		}
		if database.SourceDigest != "" && rollbackDigest != database.SourceDigest {
			return Database{}, errors.New("protected rollback database differs from the recorded source")
		}
	} else if err := copyDatabase(root, canonicalName, rollbackName); err != nil {
		return Database{}, fmt.Errorf("create rollback database: %w", err)
	}
	if shadowExists, _, err := existingMaterial(root, shadowName); err != nil {
		return Database{}, err
	} else if shadowExists {
		if database.TargetDigest != "" {
			shadowDigest, digestErr := digestAt(root, shadowName)
			if digestErr != nil {
				return Database{}, digestErr
			}
			if shadowDigest == database.TargetDigest {
				if err := store.ValidateUpgradeDatabase(ctx, database.ShadowPath, database.Kind, database.TargetIdentity); err != nil {
					return Database{}, fmt.Errorf("validate existing shadow database: %w", err)
				}
				return database, nil
			}
		}
		if err := root.Remove(shadowName); err != nil {
			return Database{}, fmt.Errorf("remove reconciled incomplete shadow database: %w", err)
		}
		if err := root.Sync(); err != nil {
			return Database{}, fmt.Errorf("sync removed incomplete shadow database: %w", err)
		}
	}
	if err := copyDatabase(root, canonicalName, shadowName); err != nil {
		return Database{}, fmt.Errorf("create shadow database: %w", err)
	}
	if migrate != nil {
		if err := migrate(ctx, database.ShadowPath, database.TargetIdentity); err != nil {
			return Database{}, fmt.Errorf("migrate shadow database: %w", err)
		}
	}
	if err := store.ValidateUpgradeDatabase(ctx, database.ShadowPath, database.Kind, database.TargetIdentity); err != nil {
		return Database{}, fmt.Errorf("validate shadow database: %w", err)
	}
	database.TargetDigest, err = digestAt(root, shadowName)
	if err != nil {
		return Database{}, err
	}
	rollbackDigest, err = digestAt(root, rollbackName)
	if err != nil {
		return Database{}, err
	}
	if rollbackDigest != database.SourceDigest {
		return Database{}, errors.New("rollback database differs from checkpointed source")
	}
	return database, nil
}

func ReconcileDatabaseSwitch(database Database) (bool, error) {
	return reconcileDatabase(database, database.ShadowPath, database.TargetDigest, database.TargetIdentity)
}

func ReconcileDatabaseRollback(database Database) (bool, error) {
	return reconcileDatabase(database, database.RollbackPath, database.SourceDigest, database.SourceIdentity)
}

func reconcileDatabase(
	database Database, materialPath, expectedDigest string, expectedIdentity store.DatabaseIdentity,
) (bool, error) {
	if err := validateDatabasePaths(database); err != nil {
		return false, err
	}
	if !validDigest(expectedDigest) {
		return false, errors.New("database reconciliation digest is invalid")
	}
	root, err := store.OpenUpgradeDatabaseRoot(database.CanonicalPath)
	if err != nil {
		return false, err
	}
	defer root.Close()
	canonicalName := filepath.Base(database.CanonicalPath)
	materialName := filepath.Base(materialPath)
	canonicalDigest, err := digestAt(root, canonicalName)
	if err == nil && canonicalDigest == expectedDigest {
		if err := store.ValidateUpgradeDatabase(
			context.Background(), database.CanonicalPath, database.Kind, expectedIdentity,
		); err != nil {
			return false, err
		}
		return false, nil
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	materialDigest, err := digestAt(root, materialName)
	if err != nil {
		return false, err
	}
	if materialDigest != expectedDigest {
		return false, errors.New("database rollback material digest changed")
	}
	if err := store.ValidateUpgradeDatabase(
		context.Background(), materialPath, database.Kind, expectedIdentity,
	); err != nil {
		return false, err
	}
	temporary := fmt.Sprintf(".%s-upgrade-%d.tmp", canonicalName, time.Now().UnixNano())
	if err := copyDatabase(root, materialName, temporary); err != nil {
		return false, err
	}
	committed, replaceErr := root.Replace(temporary, canonicalName)
	if !committed {
		_ = root.Remove(temporary)
	}
	if replaceErr != nil {
		after, digestErr := digestAt(root, canonicalName)
		if after != expectedDigest {
			return false, errors.Join(replaceErr, digestErr)
		}
	}
	if err := store.ValidateUpgradeDatabase(
		context.Background(), database.CanonicalPath, database.Kind, expectedIdentity,
	); err != nil {
		return false, err
	}
	return true, nil
}

func FileDigest(path string) (string, error) {
	root, err := store.OpenUpgradeDatabaseRoot(path)
	if err != nil {
		return "", err
	}
	defer root.Close()
	return digestAt(root, filepath.Base(path))
}

func digestAt(root *securefs.Root, name string) (string, error) {
	info, err := root.Lstat(name)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("database material must be a regular file, not a symbolic link")
	}
	file, err := root.OpenFile(name, os.O_RDONLY, 0)
	if err != nil {
		return "", err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return "", errors.Join(err, errors.New("database material changed while it was opened"))
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	if err := root.VerifyPath(); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func copyDatabase(root *securefs.Root, source, destination string) error {
	info, err := root.Lstat(source)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("database source is not a regular file, not a symbolic link")
	}
	input, err := root.OpenFile(source, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer input.Close()
	if opened, err := input.Stat(); err != nil || !opened.Mode().IsRegular() || !os.SameFile(info, opened) {
		return errors.Join(err, errors.New("database source changed while it was opened"))
	}
	output, err := root.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(output, input)
	if copyErr == nil {
		copyErr = output.Sync()
	}
	closeErr := output.Close()
	if err := errors.Join(copyErr, closeErr); err != nil {
		_ = root.Remove(destination)
		return err
	}
	return root.Sync()
}

func existingMaterial(root *securefs.Root, name string) (bool, string, error) {
	_, err := root.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		return false, "", nil
	}
	if err != nil {
		return false, "", err
	}
	digest, err := digestAt(root, name)
	return true, digest, err
}

func validateDatabasePaths(database Database) error {
	paths := []string{database.CanonicalPath, database.ShadowPath, database.RollbackPath}
	for _, path := range paths {
		if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
			return errors.New("database paths must be absolute and clean")
		}
	}
	if filepath.Dir(paths[0]) != filepath.Dir(paths[1]) || filepath.Dir(paths[0]) != filepath.Dir(paths[2]) ||
		paths[0] == paths[1] || paths[0] == paths[2] || paths[1] == paths[2] {
		return errors.New("database material must use distinct names in one directory")
	}
	return nil
}
