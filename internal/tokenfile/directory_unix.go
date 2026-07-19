//go:build linux || darwin

package tokenfile

import (
	"errors"
	"fmt"
	"os"
	"syscall"

	"github.com/GhostFlying/delegation/internal/securefs"
)

func createTokenDirectory(path string) error {
	return os.Mkdir(path, 0o700)
}

func validateTokenDirectoryLocation(string) error {
	return nil
}

func holdTokenDirectory(path string) (*securefs.Root, error) {
	if err := validateTokenDirectory(path); err != nil {
		return nil, err
	}
	afterTokenPathValidation()
	file, err := securefs.OpenRoot(path, validateTokenDirectoryHandle)
	if err != nil {
		return nil, fmt.Errorf("hold token directory: %w", err)
	}
	return file, nil
}

var afterTokenPathValidation = func() {}

func validateTokenDirectoryHandle(directory *os.File) error {
	info, err := directory.Stat()
	if err != nil {
		return fmt.Errorf("inspect opened token directory: %w", err)
	}
	if !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
		return errors.New("token directory must not be accessible by group or other users")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) {
		return errors.New("token directory must be owned by the current user")
	}
	return nil
}

func validateTokenDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect token directory: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("token directory must be a directory, not a symbolic link")
	}
	if info.Mode().Perm()&0o077 != 0 {
		return errors.New("token directory must not be accessible by group or other users")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) {
		return errors.New("token directory must be owned by the current user")
	}
	return nil
}
