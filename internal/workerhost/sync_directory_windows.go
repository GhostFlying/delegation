//go:build windows

package workerhost

import "os"

// Windows does not support syncing directory handles. Rename still publishes
// the workspace atomically; SQLite remains the durable readiness marker.
func syncDirectory(_ *os.Root) error {
	return nil
}

func syncDirectoryHandle(_ *os.File) error {
	return nil
}
