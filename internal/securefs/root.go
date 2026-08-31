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
	path   string
	root   *os.Root
	info   os.FileInfo
	parent *Root
	name   string
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

// OpenRoot opens and pins one direct child directory without following a
// symbolic-link replacement. The returned root remains bound to the opened
// child if its name is later replaced.
func (r *Root) OpenRoot(name string, validate func(*os.File) error) (*Root, error) {
	if err := validateLeaf(name); err != nil {
		return nil, err
	}
	before, err := r.root.Lstat(name)
	if err != nil {
		return nil, err
	}
	if !before.IsDir() || before.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("secure child must be a directory, not a symbolic link")
	}
	child, err := r.root.OpenRoot(name)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*Root, error) {
		_ = child.Close()
		return nil, err
	}
	directory, err := child.Open(".")
	if err != nil {
		return fail(err)
	}
	opened, statErr := directory.Stat()
	if statErr == nil && !os.SameFile(before, opened) {
		statErr = errors.New("secure child changed while it was being opened")
	}
	if statErr == nil && validate != nil {
		statErr = validate(directory)
	}
	closeErr := directory.Close()
	if statErr != nil || closeErr != nil {
		return fail(errors.Join(statErr, closeErr))
	}
	result := &Root{root: child, info: opened, parent: r, name: name}
	if err := result.VerifyPath(); err != nil {
		return fail(err)
	}
	return result, nil
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

func (r *Root) RemoveAll(name string) error {
	if err := validateLeaf(name); err != nil {
		return err
	}
	return r.root.RemoveAll(name)
}

func (r *Root) Mkdir(name string, perm os.FileMode) error {
	if err := validateLeaf(name); err != nil {
		return err
	}
	return r.root.Mkdir(name, perm)
}

func (r *Root) ReadDir(name string) ([]os.DirEntry, error) {
	if err := validateLeaf(name); err != nil {
		return nil, err
	}
	directory, err := r.root.Open(name)
	if err != nil {
		return nil, err
	}
	entries, readErr := directory.ReadDir(-1)
	return entries, errors.Join(readErr, directory.Close())
}

func (r *Root) Entries() ([]os.DirEntry, error) {
	directory, err := r.root.Open(".")
	if err != nil {
		return nil, err
	}
	entries, readErr := directory.ReadDir(-1)
	return entries, errors.Join(readErr, directory.Close())
}

func (r *Root) Readlink(name string) (string, error) {
	if err := validateLeaf(name); err != nil {
		return "", err
	}
	return r.root.Readlink(name)
}

// MoveNoReplace atomically moves one direct child between held directories
// while refusing to replace an existing destination.
func (r *Root) MoveNoReplace(oldName string, destination *Root, newName string) error {
	if destination == nil {
		return errors.New("secure destination root is required")
	}
	if err := validateLeaf(oldName); err != nil {
		return err
	}
	if err := validateLeaf(newName); err != nil {
		return err
	}
	return moveNoReplace(r.root, oldName, destination.root, newName)
}

// Rename atomically renames one leaf within the held directory.
func (r *Root) Rename(oldName, newName string) error {
	if err := validateLeaf(oldName); err != nil {
		return err
	}
	if err := validateLeaf(newName); err != nil {
		return err
	}
	return r.root.Rename(oldName, newName)
}

// Exchange atomically swaps two leaves within the held directory.
func (r *Root) Exchange(first, second string) error {
	if err := validateLeaf(first); err != nil {
		return err
	}
	if err := validateLeaf(second); err != nil {
		return err
	}
	return exchange(r.root, first, second)
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
	if r.parent != nil {
		if err := r.parent.VerifyPath(); err != nil {
			return err
		}
		current, err := r.parent.Lstat(r.name)
		if err != nil {
			return fmt.Errorf("inspect secure child path: %w", err)
		}
		if current.Mode()&os.ModeSymlink != 0 || !os.SameFile(r.info, current) {
			return errors.New("secure child path changed during the operation")
		}
		return nil
	}
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
