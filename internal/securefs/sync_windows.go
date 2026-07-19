//go:build windows

package securefs

import "os"

func syncRoot(*os.Root) error {
	return nil
}
