package clilaunch

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
)

const (
	MaximumPrefixArguments = 16
	MaximumPrefixBytes     = 16 << 10
)

// Spec describes a shell-free CLI launcher prefix. The launcher must preserve
// stdio. On Linux, it must replace itself with the target CLI process rather
// than spawn and wait for it, so parent-death ownership remains attached to the
// app-server. On macOS and Windows, it must remain attached to the target
// process and must not daemonize or detach.
type Spec struct {
	Executable      string   `json:"executable"`
	PrefixArguments []string `json:"prefixArguments,omitempty"`
}

func Validate(spec Spec) error {
	if err := validatePath(spec.Executable, "CLI launcher executable"); err != nil {
		return err
	}
	if len(spec.PrefixArguments) > MaximumPrefixArguments {
		return fmt.Errorf(
			"CLI launch prefix must contain at most %d arguments",
			MaximumPrefixArguments,
		)
	}
	prefixBytes := 0
	for _, argument := range spec.PrefixArguments {
		if strings.ContainsRune(argument, '\x00') {
			return errors.New("CLI launch prefix arguments must not contain NUL")
		}
		prefixBytes += len(argument)
		if prefixBytes > MaximumPrefixBytes {
			return fmt.Errorf(
				"CLI launch prefix must contain at most %d bytes",
				MaximumPrefixBytes,
			)
		}
	}
	return nil
}

func Resolve(spec Spec) (Spec, error) {
	if err := Validate(spec); err != nil {
		return Spec{}, err
	}
	executable, err := resolveExecutable(spec.Executable, "CLI launcher executable")
	if err != nil {
		return Spec{}, err
	}
	return Spec{
		Executable:      executable,
		PrefixArguments: slices.Clone(spec.PrefixArguments),
	}, nil
}

func ResolveRuntimeExecutable(path string) (string, error) {
	if err := validatePath(path, "CLI runtime executable"); err != nil {
		return "", err
	}
	return resolveExecutable(path, "CLI runtime executable")
}

func validatePath(path, name string) error {
	if path == "" || !filepath.IsAbs(path) {
		return fmt.Errorf("%s must be an absolute path", name)
	}
	if strings.ContainsRune(path, '\x00') {
		return fmt.Errorf("%s must not contain NUL", name)
	}
	return nil
}

func resolveExecutable(path, name string) (string, error) {
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", fmt.Errorf("resolve %s: %w", name, err)
	}
	if runtime.GOOS == "windows" {
		switch strings.ToLower(filepath.Ext(resolved)) {
		case ".bat", ".cmd", ".ps1":
			return "", fmt.Errorf("%s must be a native executable on Windows", name)
		}
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return "", fmt.Errorf("inspect %s: %w", name, err)
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("%s must be a regular file", name)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0o111 == 0 {
		return "", fmt.Errorf("%s must be executable", name)
	}
	return resolved, nil
}
