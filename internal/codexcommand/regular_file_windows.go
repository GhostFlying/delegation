//go:build windows

package codexcommand

import (
	"errors"
	"os"
)

func openRegularFile(path string) (*os.File, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
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
