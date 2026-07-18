//go:build linux || darwin

package tokenfile

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

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
