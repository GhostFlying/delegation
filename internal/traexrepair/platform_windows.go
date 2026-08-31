//go:build windows

package traexrepair

import "errors"

func checkSupportedPlatform() error {
	return errors.New("TraeX repair is unsupported on Windows")
}
