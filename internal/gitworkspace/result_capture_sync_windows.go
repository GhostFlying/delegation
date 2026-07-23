//go:build windows

package gitworkspace

// Windows does not support syncing directory handles. Each artifact file is
// still synced before the completed capture is published.
func syncResultArtifactDirectories(...string) error {
	return nil
}
