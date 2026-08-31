//go:build windows

package securefs

import (
	"errors"
	"os"
)

func moveNoReplace(*os.Root, string, *os.Root, string) error {
	return errors.New("secure cross-directory move is unsupported on Windows")
}
