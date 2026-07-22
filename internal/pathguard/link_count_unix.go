//go:build !windows

package pathguard

import (
	"errors"
	"os"
	"syscall"
)

func openedFileLinkCount(_ *os.File, info os.FileInfo) (uint64, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, errors.New("file link-count metadata is unavailable")
	}
	return uint64(stat.Nlink), nil
}
