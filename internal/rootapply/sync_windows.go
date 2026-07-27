//go:build windows

package rootapply

import "os"

func syncDirectory(*os.Root) error { return nil }
