//go:build !windows

package pathguard

import "path/filepath"

func resolveLinkTarget(parent, target string) (string, error) {
	if filepath.IsAbs(target) {
		return target, nil
	}
	return filepath.Join(parent, target), nil
}
