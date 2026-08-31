//go:build windows

package releaseverify

import "github.com/GhostFlying/delegation/internal/securefs"

func publishRuntimeDirectory(root *securefs.Root, temporary, destination string) error {
	return root.MoveNoReplace(temporary, root, destination)
}
