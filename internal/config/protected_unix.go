//go:build linux || darwin

package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/GhostFlying/delegation/internal/securefs"
)

func createConfigDirectory(path string) error {
	return os.Mkdir(path, 0o700)
}

func validateConfigDirectoryLocation(string) error {
	return nil
}

func holdConfigDirectory(path string) (*securefs.Root, error) {
	if err := validateConfigDirectory(path); err != nil {
		return nil, err
	}
	afterConfigPathValidation()
	directory, err := securefs.OpenRoot(path, validateConfigDirectoryHandle)
	if err != nil {
		return nil, fmt.Errorf("hold config directory: %w", err)
	}
	return directory, nil
}

func protectPrivateDirectory(path string) error {
	directory, err := securefs.OpenRoot(path, func(opened *os.File) error {
		if _, err := inspectOwnedPrivateDirectory(opened); err != nil {
			return err
		}
		return opened.Chmod(0o700)
	})
	if err != nil {
		return err
	}
	return directory.Close()
}

func holdPrivateDirectory(path string) (*securefs.Root, error) {
	if err := validateConfigDirectory(path); err != nil {
		return nil, err
	}
	return securefs.OpenRoot(path, func(directory *os.File) error {
		info, err := inspectOwnedPrivateDirectory(directory)
		if err != nil {
			return err
		}
		if info.Mode().Perm() != 0o700 {
			return errors.New("private directory must have mode 0700")
		}
		return nil
	})
}

func inspectOwnedPrivateDirectory(directory *os.File) (os.FileInfo, error) {
	info, err := directory.Stat()
	if err != nil {
		return nil, fmt.Errorf("inspect opened private directory: %w", err)
	}
	if !info.IsDir() {
		return nil, errors.New("private directory must be a directory")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) {
		return nil, errors.New("private directory must be owned by the current user")
	}
	return info, nil
}

func validateConfigDirectoryHandle(directory *os.File) error {
	info, err := directory.Stat()
	if err != nil {
		return fmt.Errorf("inspect opened config directory: %w", err)
	}
	if !info.IsDir() || info.Mode().Perm()&0o022 != 0 {
		return errors.New("config directory must not be writable by other users")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) {
		return errors.New("config directory must be owned by the current user")
	}
	return nil
}

func createConfigTemp(directory *securefs.Root) (string, *os.File, error) {
	for range 100 {
		suffix, err := randomConfigSuffix()
		if err != nil {
			return "", nil, err
		}
		name := ".config-" + suffix + ".tmp"
		temp, err := directory.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if errors.Is(err, os.ErrExist) {
			continue
		}
		if err != nil {
			return "", nil, err
		}
		if err := temp.Chmod(0o600); err != nil {
			_ = temp.Close()
			_ = directory.Remove(name)
			return "", nil, err
		}
		return name, temp, nil
	}
	return "", nil, errors.New("create temporary config: exhausted name attempts")
}

func openProtectedConfig(path string) (*os.File, error) {
	if !filepath.IsAbs(path) {
		return nil, errors.New("config file path must be absolute")
	}
	directory, err := holdConfigDirectory(filepath.Dir(path))
	if err != nil {
		return nil, err
	}
	defer directory.Close()
	name := filepath.Base(path)
	before, err := directory.Lstat(name)
	if err != nil {
		return nil, fmt.Errorf("inspect config: %w", err)
	}
	if !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("config must be a regular file, not a symbolic link")
	}
	file, err := directory.OpenFile(name, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fmt.Errorf("open config: %w", err)
	}
	fail := func(err error) (*os.File, error) {
		_ = file.Close()
		return nil, err
	}
	after, err := file.Stat()
	if err != nil {
		return fail(fmt.Errorf("inspect opened config: %w", err))
	}
	if !os.SameFile(before, after) {
		return fail(errors.New("config changed while it was being opened"))
	}
	if after.Mode().Perm() != 0o600 {
		return fail(errors.New("config must have mode 0600"))
	}
	stat, ok := after.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) {
		return fail(errors.New("config must be owned by the current user"))
	}
	if err := directory.VerifyPath(); err != nil {
		return fail(err)
	}
	return file, nil
}

var afterConfigPathValidation = func() {}

func validateConfigDirectory(path string) error {
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return fmt.Errorf("resolve config directory aliases: %w", err)
	}
	for _, candidate := range []string{filepath.Clean(path), filepath.Clean(resolved)} {
		if err := validateConfigAncestry(candidate); err != nil {
			return err
		}
	}
	info, err := os.Lstat(resolved)
	if err != nil {
		return fmt.Errorf("inspect config directory: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o022 != 0 {
		return errors.New("config directory must be a non-symlink directory not writable by other users")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) {
		return errors.New("config directory must be owned by the current user")
	}
	return nil
}

func validateConfigAncestry(path string) error {
	current := filepath.Clean(path)
	for {
		parent := filepath.Dir(current)
		if parent == current {
			return nil
		}
		child, err := os.Lstat(current)
		if err != nil {
			return fmt.Errorf("inspect config path ancestry: %w", err)
		}
		parentInfo, err := os.Stat(parent)
		if err != nil {
			return fmt.Errorf("inspect config path parent: %w", err)
		}
		if parentInfo.Mode().Perm()&0o022 != 0 {
			parentStat, parentOK := parentInfo.Sys().(*syscall.Stat_t)
			childStat, childOK := child.Sys().(*syscall.Stat_t)
			sticky := parentInfo.Mode()&os.ModeSticky != 0
			protectedByStickyOwner := sticky && parentOK && childOK &&
				(parentStat.Uid == uint32(os.Geteuid()) || childStat.Uid == uint32(os.Geteuid()))
			if !protectedByStickyOwner {
				return errors.New("config path has an ancestor replaceable by another user")
			}
		}
		current = parent
	}
}
