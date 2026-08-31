//go:build darwin

package securefs

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func moveNoReplace(source *os.Root, oldName string, destination *os.Root, newName string) error {
	sourceDirectory, err := source.Open(".")
	if err != nil {
		return err
	}
	defer sourceDirectory.Close()
	destinationDirectory, err := destination.Open(".")
	if err != nil {
		return err
	}
	defer destinationDirectory.Close()
	if err := unix.RenameatxNp(
		int(sourceDirectory.Fd()), oldName,
		int(destinationDirectory.Fd()), newName,
		unix.RENAME_EXCL,
	); err != nil {
		if err == unix.EXDEV {
			return fmt.Errorf("secure move crosses filesystems: %w", err)
		}
		return err
	}
	return nil
}
