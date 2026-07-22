//go:build linux || darwin

package codexcommand

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

func openRegularFile(path string) (*os.File, error) {
	descriptor, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NONBLOCK|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(descriptor), path)
	if file == nil {
		_ = unix.Close(descriptor)
		return nil, errors.New("open metadata file")
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if !info.Mode().IsRegular() {
		_ = file.Close()
		return nil, errors.New("metadata must be a regular file")
	}
	return file, nil
}
