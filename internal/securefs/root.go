package securefs

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Root binds authority-file operations to one opened directory. Operations
// continue to address that directory if its path is renamed or replaced.
type Root struct {
	path string
	root *os.Root
	info os.FileInfo
}

// OpenRoot opens path and validates the opened directory handle before use.
func OpenRoot(path string, validate func(*os.File) error) (*Root, error) {
	if !filepath.IsAbs(path) {
		return nil, errors.New("secure directory path must be absolute")
	}
	before, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !before.IsDir() || before.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("secure directory must be a directory, not a symbolic link")
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*Root, error) {
		_ = root.Close()
		return nil, err
	}
	directory, err := root.Open(".")
	if err != nil {
		return fail(err)
	}
	opened, statErr := directory.Stat()
	if statErr == nil && !os.SameFile(before, opened) {
		statErr = errors.New("secure directory changed while it was being opened")
	}
	if statErr == nil && validate != nil {
		statErr = validate(directory)
	}
	closeErr := directory.Close()
	if statErr != nil || closeErr != nil {
		return fail(errors.Join(statErr, closeErr))
	}
	result := &Root{path: path, root: root, info: opened}
	if err := result.VerifyPath(); err != nil {
		return fail(err)
	}
	return result, nil
}

func (r *Root) Close() error {
	return r.root.Close()
}

func (r *Root) OpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	if err := validateLeaf(name); err != nil {
		return nil, err
	}
	return r.root.OpenFile(name, flag, perm)
}

func (r *Root) Lstat(name string) (os.FileInfo, error) {
	if err := validateLeaf(name); err != nil {
		return nil, err
	}
	return r.root.Lstat(name)
}

func (r *Root) Remove(name string) error {
	if err := validateLeaf(name); err != nil {
		return err
	}
	return r.root.Remove(name)
}

// PublishNoReplace atomically renames a temporary file without replacing an
// existing destination. The returned boolean reports whether the destination
// was committed, including when post-rename durability work fails.
func (r *Root) PublishNoReplace(temporary, destination string) (bool, error) {
	if err := validateLeaf(temporary); err != nil {
		return false, err
	}
	if err := validateLeaf(destination); err != nil {
		return false, err
	}
	return publishNoReplace(r.root, temporary, destination)
}

// Sync flushes directory metadata on platforms where directory sync is
// supported.
func (r *Root) Sync() error {
	return syncRoot(r.root)
}

// VerifyPath checks that the configured path still names the opened directory.
func (r *Root) VerifyPath() error {
	current, err := os.Lstat(r.path)
	if err != nil {
		return fmt.Errorf("inspect secure directory path: %w", err)
	}
	if current.Mode()&os.ModeSymlink != 0 || !os.SameFile(r.info, current) {
		return errors.New("secure directory path changed during the operation")
	}
	return nil
}

func validateLeaf(name string) error {
	if name == "" || name == "." || name == ".." || filepath.IsAbs(name) ||
		strings.ContainsAny(name, `/\\`) {
		return errors.New("secure file name must be one relative path component")
	}
	return nil
}
