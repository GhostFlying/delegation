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
	if err := identity.ValidateID(threadID); err != nil {
		return Locator{}, fmt.Errorf("threadId %w", err)
	}
	if !filepath.IsAbs(codexHome) || !filepath.IsAbs(path) {
		return Locator{}, errors.New("Codex home and rollout path must be absolute")
	}
	cleanPath := filepath.Clean(path)
	resolvedHome, err := filepath.EvalSymlinks(filepath.Clean(codexHome))
	if err != nil {
		return Locator{}, fmt.Errorf("resolve managed Codex home: %w", err)
	}
	resolvedPath, err := filepath.EvalSymlinks(cleanPath)
	if err != nil {
		return Locator{}, fmt.Errorf("resolve managed rollout path: %w", err)
	}
	if !samePath(cleanPath, resolvedPath) {
		return Locator{}, errors.New("managed rollout path must not contain symbolic links")
	}
	relative, err := filepath.Rel(resolvedHome, resolvedPath)
	if err != nil || relative == "." || filepath.IsAbs(relative) || relative == ".." ||
		strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return Locator{}, errors.New("managed rollout path is outside Codex home")
	}
	components := splitPath(relative)
	if len(components) < 2 || !sameComponent(components[0], "sessions") {
		return Locator{}, errors.New("managed rollout path is outside the active sessions hierarchy")
	}
	expectedSuffix := "-" + threadID + ".jsonl"
	if !strings.HasPrefix(components[len(components)-1], "rollout-") ||
		!strings.HasSuffix(components[len(components)-1], expectedSuffix) {
		return Locator{}, errors.New("managed rollout path does not name the expected thread")
	}
	before, err := os.Lstat(resolvedPath)
	if err != nil {
		return Locator{}, fmt.Errorf("inspect managed rollout: %w", err)
	}
	if !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 {
		return Locator{}, errors.New("managed rollout must be a regular file")
	}
	root, err := os.OpenRoot(resolvedHome)
	if err != nil {
		return Locator{}, fmt.Errorf("open managed Codex home: %w", err)
	}
	defer root.Close()
	file, err := root.Open(relative)
	if err != nil {
		return Locator{}, fmt.Errorf("open managed rollout: %w", err)
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil {
		return Locator{}, fmt.Errorf("inspect opened managed rollout: %w", err)
	}
	if !opened.Mode().IsRegular() || !os.SameFile(before, opened) {
		return Locator{}, errors.New("managed rollout changed while it was being opened")
	}
	return Locator{Path: resolvedPath, Offset: opened.Size()}, nil
}

func samePath(left, right string) bool {
	if runtime.GOOS == "windows" {
		return strings.EqualFold(left, right)
	}
	return left == right
}

func sameComponent(left, right string) bool {
	if runtime.GOOS == "windows" {
		return strings.EqualFold(left, right)
	}
	return left == right
}

func splitPath(path string) []string {
	return strings.FieldsFunc(path, func(char rune) bool {
		return char == '/' || char == '\\'
	})
}
