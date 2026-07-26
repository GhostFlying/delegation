//go:build windows

package resultpackagefiles

import "os"

func syncDirectory(*os.Root) error {
	return nil
}
