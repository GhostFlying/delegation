package pathguard

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type namedPath struct {
	name string
	path string
}

// ValidatePeerAuthority rejects aliases between the configuration, token, and
// reservation database files that make up a peer authority.
func ValidatePeerAuthority(configPath, statePath, tokenPath string) error {
	files := peerAuthorityFiles(configPath, statePath, tokenPath)
	for firstIndex, first := range files {
		for _, second := range files[firstIndex+1:] {
			conflicts, err := equivalent(first.path, second.path)
			if err != nil {
				return err
			}
			if conflicts {
				return fmt.Errorf("%s path conflicts with %s", second.name, first.name)
			}
		}
	}
	return nil
}

// ValidateBrokerAuthority rejects aliases between the configuration, master
// token, and SQLite database files that make up a broker authority.
func ValidateBrokerAuthority(configPath, statePath, masterPath string) error {
	files := brokerAuthorityFiles(configPath, statePath, masterPath)
	for firstIndex, first := range files {
		for _, second := range files[firstIndex+1:] {
			conflicts, err := equivalent(first.path, second.path)
			if err != nil {
				return err
			}
			if conflicts {
				return fmt.Errorf("%s path conflicts with %s", second.name, first.name)
			}
		}
	}
	return nil
}

// ValidateCredentialOutput rejects an output token path that aliases any file
// belonging to the broker authority.
func ValidateCredentialOutput(tokenPath, configPath, statePath, masterPath string) error {
	for _, candidate := range brokerAuthorityFiles(configPath, statePath, masterPath) {
		conflicts, err := equivalent(tokenPath, candidate.path)
		if err != nil {
			return err
		}
		if conflicts {
			return fmt.Errorf("output token path conflicts with %s", candidate.name)
		}
	}
	return nil
}

func brokerAuthorityFiles(configPath, statePath, masterPath string) []namedPath {
	files := []namedPath{{name: "broker configuration", path: configPath}}
	if masterPath != "" {
		files = append(files, namedPath{name: "broker master token", path: masterPath})
	}
	return append(files,
		namedPath{name: "broker state", path: statePath},
		namedPath{name: "broker rollback journal", path: statePath + "-journal"},
		namedPath{name: "broker WAL", path: statePath + "-wal"},
		namedPath{name: "broker shared memory", path: statePath + "-shm"},
		namedPath{name: "broker instance lease", path: statePath + ".broker.lock"},
	)
}

func peerAuthorityFiles(configPath, statePath, tokenPath string) []namedPath {
	files := []namedPath{{name: "peer configuration", path: configPath}}
	if tokenPath != "" {
		files = append(files, namedPath{name: "peer token", path: tokenPath})
	}
	return append(files,
		namedPath{name: "peer state", path: statePath},
		namedPath{name: "peer rollback journal", path: statePath + "-journal"},
		namedPath{name: "peer WAL", path: statePath + "-wal"},
		namedPath{name: "peer shared memory", path: statePath + "-shm"},
		namedPath{name: "peer instance lease", path: statePath + ".peer.lock"},
	)
}

func equivalent(first, second string) (bool, error) {
	firstCanonical, err := canonicalFuturePath(first)
	if err != nil {
		return false, err
	}
	secondCanonical, err := canonicalFuturePath(second)
	if err != nil {
		return false, err
	}
	// Conservatively cover case-insensitive macOS and removable filesystems.
	if strings.EqualFold(firstCanonical, secondCanonical) {
		return true, nil
	}
	firstInfo, firstErr := os.Stat(firstCanonical)
	if firstErr != nil && !errors.Is(firstErr, os.ErrNotExist) {
		return false, fmt.Errorf("inspect path identity for %s: %w", firstCanonical, firstErr)
	}
	secondInfo, secondErr := os.Stat(secondCanonical)
	if secondErr != nil && !errors.Is(secondErr, os.ErrNotExist) {
		return false, fmt.Errorf("inspect path identity for %s: %w", secondCanonical, secondErr)
	}
	return firstErr == nil && secondErr == nil && os.SameFile(firstInfo, secondInfo), nil
}

func canonicalFuturePath(path string) (string, error) {
	return resolveFuturePath(filepath.Clean(path), 0)
}

func resolveFuturePath(path string, followedLinks int) (string, error) {
	root, components := splitAbsolutePath(path)
	resolved := root
	for index, component := range components {
		candidate := filepath.Join(resolved, component)
		info, err := os.Lstat(candidate)
		if errors.Is(err, os.ErrNotExist) {
			return filepath.Join(append([]string{resolved}, components[index:]...)...), nil
		}
		if err != nil {
			return "", fmt.Errorf("resolve path aliases for %s: %w", path, err)
		}
		if info.Mode()&os.ModeSymlink == 0 {
			resolved = candidate
			continue
		}
		if followedLinks >= 255 {
			return "", fmt.Errorf("resolve path aliases for %s: too many symbolic links", path)
		}
		target, err := os.Readlink(candidate)
		if err != nil {
			return "", fmt.Errorf("read path alias %s: %w", candidate, err)
		}
		target, err = resolveLinkTarget(resolved, target)
		if err != nil {
			return "", fmt.Errorf("resolve path alias %s: %w", candidate, err)
		}
		remaining := append([]string{target}, components[index+1:]...)
		return resolveFuturePath(filepath.Join(remaining...), followedLinks+1)
	}
	return filepath.Clean(resolved), nil
}

func splitAbsolutePath(path string) (string, []string) {
	current := filepath.Clean(path)
	var reversed []string
	for {
		parent := filepath.Dir(current)
		if parent == current {
			break
		}
		reversed = append(reversed, filepath.Base(current))
		current = parent
	}
	components := make([]string, len(reversed))
	for index := range reversed {
		components[len(reversed)-1-index] = reversed[index]
	}
	return current, components
}
