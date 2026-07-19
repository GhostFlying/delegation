package store

import (
	"fmt"

	"github.com/GhostFlying/delegation/internal/securefs"
)

func openStateDirectoryGuard(path string) (*securefs.Root, error) {
	directory, err := securefs.OpenRoot(path, validateStateDirectoryHandle)
	if err != nil {
		return nil, fmt.Errorf("open broker state directory: %w", err)
	}
	return directory, nil
}
