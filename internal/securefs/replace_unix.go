//go:build linux || darwin

package securefs

import "os"

func replace(root *os.Root, temporary, destination string) (bool, error) {
	if err := root.Rename(temporary, destination); err != nil {
		return false, err
	}
	if err := syncRoot(root); err != nil {
		return true, err
	}
	return true, nil
}
