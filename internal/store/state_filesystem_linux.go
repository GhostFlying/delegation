//go:build linux

package store

import (
	"errors"
	"fmt"

	"golang.org/x/sys/unix"
)

func validateLocalStatePath(path string) error {
	existing, err := nearestExistingPath(path)
	if err != nil {
		return err
	}
	var status unix.Statfs_t
	if err := unix.Statfs(existing, &status); err != nil {
		return fmt.Errorf("inspect broker state filesystem: %w", err)
	}
	if isLinuxNetworkFilesystem(status.Type) {
		return errors.New("broker state must not use a network filesystem because SQLite WAL requires local shared memory")
	}
	return nil
}

func isLinuxNetworkFilesystem(filesystemType int64) bool {
	switch filesystemType {
	case unix.AAFS_MAGIC,
		unix.AFS_FS_MAGIC,
		unix.AFS_SUPER_MAGIC,
		unix.CEPH_SUPER_MAGIC,
		unix.CIFS_SUPER_MAGIC,
		unix.CODA_SUPER_MAGIC,
		unix.FUSE_SUPER_MAGIC,
		unix.NCP_SUPER_MAGIC,
		unix.NFS_SUPER_MAGIC,
		unix.SMB_SUPER_MAGIC,
		unix.SMB2_SUPER_MAGIC,
		unix.V9FS_MAGIC:
		return true
	default:
		return false
	}
}
