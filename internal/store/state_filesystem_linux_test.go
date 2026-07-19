//go:build linux

package store

import (
	"testing"

	"golang.org/x/sys/unix"
)

func TestLinuxNetworkFilesystemsAreRejectedForWALState(t *testing.T) {
	for _, filesystemType := range []int64{
		unix.NFS_SUPER_MAGIC,
		unix.CIFS_SUPER_MAGIC,
		unix.SMB2_SUPER_MAGIC,
		unix.FUSE_SUPER_MAGIC,
		unix.V9FS_MAGIC,
	} {
		if !isLinuxNetworkFilesystem(filesystemType) {
			t.Fatalf("network filesystem %#x was accepted", filesystemType)
		}
	}
	if isLinuxNetworkFilesystem(unix.TMPFS_MAGIC) {
		t.Fatal("tmpfs was classified as a network filesystem")
	}
}
