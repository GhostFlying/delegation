//go:build linux || darwin

package tokenfile

import (
	"errors"
	"os"
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/GhostFlying/delegation/internal/securefs"
)

func openSecureNew(directory *securefs.Root, name string) (*os.File, error) {
	return directory.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
}

func openSecureRead(directory *securefs.Root, name string) (*os.File, error) {
	return directory.OpenFile(name, os.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
}

func validateFilePermissions(_ *os.File, info os.FileInfo) error {
	if info.Mode().Perm()&0o077 != 0 {
		return errors.New("token file must not be accessible by group or other users")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) {
		return errors.New("token file must be owned by the current user")
	}
	return nil
}

func syncParentDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
