package rolloutcapture

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/GhostFlying/delegation/internal/identity"
)

// Locator identifies one validated managed Codex rollout and the byte offset
// immediately before a turn/start request.
type Locator struct {
	Path   string
	Offset int64
}

// Locate validates an app-server Thread.path without searching for a newer
// session file. Callers must locate again before capture if the path may have
// changed since the returned offset was recorded.
func Locate(codexHome, threadID, path string) (Locator, error) {
	file, locator, err := OpenValidated(codexHome, threadID, path)
	if err != nil {
		return Locator{}, err
	}
	if err := file.Close(); err != nil {
		return Locator{}, fmt.Errorf("close managed rollout: %w", err)
	}
	return locator, nil
}

// OpenValidated opens the validated rollout and returns the same handle whose
// identity and size produced the locator. Callers that consume rollout bytes
// must use this handle instead of reopening Locator.Path.
func OpenValidated(codexHome, threadID, path string) (*os.File, Locator, error) {
	if err := identity.ValidateID(threadID); err != nil {
		return nil, Locator{}, fmt.Errorf("threadId %w", err)
	}
	if !filepath.IsAbs(codexHome) || !filepath.IsAbs(path) {
		return nil, Locator{}, errors.New("Codex home and rollout path must be absolute")
	}
	cleanPath := filepath.Clean(path)
	resolvedHome, err := filepath.EvalSymlinks(filepath.Clean(codexHome))
	if err != nil {
		return nil, Locator{}, fmt.Errorf("resolve managed Codex home: %w", err)
	}
	homeInfo, err := os.Stat(resolvedHome)
	if err != nil {
		return nil, Locator{}, fmt.Errorf("inspect managed Codex home: %w", err)
	}
	if !homeInfo.IsDir() {
		return nil, Locator{}, errors.New("managed Codex home must be a directory")
	}
	resolvedPath, err := filepath.EvalSymlinks(cleanPath)
	if err != nil {
		return nil, Locator{}, fmt.Errorf("resolve managed rollout path: %w", err)
	}
	resolvedRelative, err := filepath.Rel(resolvedHome, resolvedPath)
	if err != nil || resolvedRelative == "." || filepath.IsAbs(resolvedRelative) || resolvedRelative == ".." ||
		strings.HasPrefix(resolvedRelative, ".."+string(filepath.Separator)) {
		return nil, Locator{}, errors.New("managed rollout path is outside Codex home")
	}
	relative, err := relativeToHomeAlias(homeInfo, cleanPath)
	if err != nil {
		return nil, Locator{}, err
	}
	components := splitPath(relative)
	if len(components) < 2 || !sameComponent(components[0], "sessions") {
		return nil, Locator{}, errors.New("managed rollout path is outside the active sessions hierarchy")
	}
	expectedSuffix := "-" + threadID + ".jsonl"
	if !strings.HasPrefix(components[len(components)-1], "rollout-") ||
		!strings.HasSuffix(components[len(components)-1], expectedSuffix) {
		return nil, Locator{}, errors.New("managed rollout path does not name the expected thread")
	}
	root, err := os.OpenRoot(resolvedHome)
	if err != nil {
		return nil, Locator{}, fmt.Errorf("open managed Codex home: %w", err)
	}
	defer root.Close()
	before, err := inspectNoSymlink(root, components)
	if err != nil {
		return nil, Locator{}, err
	}
	file, err := root.Open(relative)
	if err != nil {
		return nil, Locator{}, fmt.Errorf("open managed rollout: %w", err)
	}
	opened, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, Locator{}, fmt.Errorf("inspect opened managed rollout: %w", err)
	}
	if !opened.Mode().IsRegular() || !os.SameFile(before, opened) {
		_ = file.Close()
		return nil, Locator{}, errors.New("managed rollout changed while it was being opened")
	}
	return file, Locator{Path: resolvedPath, Offset: opened.Size()}, nil
}

func relativeToHomeAlias(homeInfo os.FileInfo, path string) (string, error) {
	current := filepath.Dir(path)
	components := []string{filepath.Base(path)}
	for {
		info, err := os.Stat(current)
		if err != nil {
			return "", fmt.Errorf("inspect managed rollout parent: %w", err)
		}
		if os.SameFile(homeInfo, info) {
			for left, right := 0, len(components)-1; left < right; left, right = left+1, right-1 {
				components[left], components[right] = components[right], components[left]
			}
			return filepath.Join(components...), nil
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", errors.New("managed rollout path is outside Codex home")
		}
		components = append(components, filepath.Base(current))
		current = parent
	}
}

func inspectNoSymlink(root *os.Root, components []string) (os.FileInfo, error) {
	current := ""
	var info os.FileInfo
	for index, component := range components {
		current = filepath.Join(current, component)
		var err error
		info, err = root.Lstat(current)
		if err != nil {
			return nil, fmt.Errorf("inspect managed rollout component: %w", err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return nil, errors.New("managed rollout path must not contain symbolic links")
		}
		if index < len(components)-1 && !info.IsDir() {
			return nil, errors.New("managed rollout parent must be a directory")
		}
	}
	if info == nil || !info.Mode().IsRegular() {
		return nil, errors.New("managed rollout must be a regular file")
	}
	return info, nil
}

func sameComponent(left, right string) bool {
	if runtime.GOOS == "windows" {
		return strings.EqualFold(left, right)
	}
	return left == right
}

func splitPath(path string) []string {
	return strings.Split(path, string(filepath.Separator))
}
