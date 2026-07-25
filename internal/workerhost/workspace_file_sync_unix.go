//go:build !windows

package workerhost

import "os"

func openWorkspaceFileForSync(
	root *os.Root,
	name string,
	_ os.FileInfo,
) (*os.File, func() error, error) {
	file, err := root.Open(name)
	return file, func() error { return nil }, err
}
