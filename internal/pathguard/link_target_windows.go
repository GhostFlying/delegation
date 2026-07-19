//go:build windows

package pathguard

import (
	"errors"
	"os"
	"path/filepath"
)

func resolveLinkTarget(parent, target string) (string, error) {
	if filepath.IsAbs(target) {
		return target, nil
	}
	if filepath.VolumeName(target) != "" {
		return "", errors.New("drive-relative symbolic-link targets are unsupported")
	}
	if len(target) > 0 && os.IsPathSeparator(target[0]) {
		volume := filepath.VolumeName(parent)
		if volume == "" {
			return "", errors.New("root-relative symbolic-link target has no parent volume")
		}
		return volume + target, nil
	}
	return filepath.Join(parent, target), nil
}
