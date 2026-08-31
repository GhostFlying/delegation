//go:build windows

package securefs

import (
	"errors"
	"os"
)

func exchange(*os.Root, string, string) error {
	return errors.New("secure file exchange is unsupported on Windows")
}
