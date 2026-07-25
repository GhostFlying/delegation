package workerhost

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const workspaceSyncReadBatch = 256

func syncWorkspaceTree(root *os.Root) error {
	if root == nil {
		return errors.New("prepared workspace root is required")
	}
	return syncWorkspaceTreeDirectory(root, ".")
}

func syncWorkspaceTreeDirectory(root *os.Root, name string) (returnErr error) {
	expected, err := root.Lstat(name)
	if err != nil {
		return fmt.Errorf("inspect prepared workspace directory %q: %w", name, err)
	}
	if expected.Mode()&os.ModeSymlink != 0 || !expected.IsDir() {
		return fmt.Errorf("prepared workspace directory %q must be a real directory", name)
	}
	directory, err := root.Open(name)
	if err != nil {
		return fmt.Errorf("open prepared workspace directory %q: %w", name, err)
	}
	defer func() {
		returnErr = errors.Join(returnErr, directory.Close())
	}()
	actual, err := directory.Stat()
	if err != nil || !actual.IsDir() || !os.SameFile(expected, actual) {
		return errors.Join(
			fmt.Errorf("prepared workspace directory %q changed while it was opened", name),
			err,
		)
	}
	for {
		entries, readErr := directory.ReadDir(workspaceSyncReadBatch)
		for _, entry := range entries {
			path := filepath.Join(name, entry.Name())
			info, err := root.Lstat(path)
			if err != nil {
				return fmt.Errorf("inspect prepared workspace entry %q: %w", path, err)
			}
			switch {
			case info.Mode()&os.ModeSymlink != 0:
				continue
			case info.IsDir():
				if err := syncWorkspaceTreeDirectory(root, path); err != nil {
					return err
				}
			case info.Mode().IsRegular():
				if err := syncWorkspaceTreeFile(root, path, info); err != nil {
					return err
				}
			default:
				return fmt.Errorf("prepared workspace entry %q has an unsupported file type", path)
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return fmt.Errorf("read prepared workspace directory %q: %w", name, readErr)
		}
	}
	if err := syncDirectoryHandle(directory); err != nil {
		return fmt.Errorf("sync prepared workspace directory %q: %w", name, err)
	}
	return nil
}

func syncWorkspaceTreeFile(root *os.Root, name string, expected os.FileInfo) (returnErr error) {
	file, restoreMode, err := openWorkspaceFileForSync(root, name, expected)
	if err != nil {
		return fmt.Errorf("open prepared workspace file %q: %w", name, err)
	}
	defer func() {
		returnErr = errors.Join(returnErr, restoreMode(), file.Close())
	}()
	actual, err := file.Stat()
	if err != nil || !actual.Mode().IsRegular() || !os.SameFile(expected, actual) {
		return errors.Join(
			fmt.Errorf("prepared workspace file %q changed while it was opened", name),
			err,
		)
	}
	if err := restoreMode(); err != nil {
		return fmt.Errorf("restore prepared workspace file mode %q: %w", name, err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync prepared workspace file %q: %w", name, err)
	}
	return nil
}
