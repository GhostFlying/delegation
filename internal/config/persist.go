package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/GhostFlying/delegation/internal/securefs"
)

const maximumProtectedReadSize = 1 << 20

type CommittedError struct {
	Err error
}

func (e *CommittedError) Error() string {
	return fmt.Sprintf("config was committed but final durability work failed: %v", e.Err)
}

func (e *CommittedError) Unwrap() error {
	return e.Err
}

func IsCommitted(err error) bool {
	var committed *CommittedError
	return errors.As(err, &committed)
}

func WriteNew(path string, cfg Config) error {
	return writeNewForRuntime(path, cfg, RuntimeCapabilities{})
}

// WriteNewForRuntime atomically writes a new configuration after validating it
// against explicitly available runtime transport capabilities.
func WriteNewForRuntime(path string, cfg Config, capabilities RuntimeCapabilities) error {
	return writeNewForRuntime(path, cfg, capabilities)
}

// EncodeForRuntime returns the canonical on-disk representation after full
// runtime-aware validation.
func EncodeForRuntime(cfg Config, capabilities RuntimeCapabilities) ([]byte, error) {
	if err := cfg.ValidateForRuntime(capabilities); err != nil {
		return nil, err
	}
	var data bytes.Buffer
	encoder := json.NewEncoder(&data)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(cfg); err != nil {
		return nil, fmt.Errorf("encode config: %w", err)
	}
	return data.Bytes(), nil
}

// ReplaceProtectedFile atomically replaces an existing protected regular file
// only if its bytes still equal expected. This prevents repair from overwriting
// a concurrent operator edit.
func ReplaceProtectedFile(path string, expected, replacement []byte) error {
	if len(replacement) == 0 || len(replacement) > maximumConfigSize {
		return errors.New("replacement config size is invalid")
	}
	directory, err := holdConfigDirectory(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer directory.Close()
	name := filepath.Base(path)
	current, _, err := readProtectedConfigAt(directory, name, maximumConfigSize)
	if err != nil {
		return err
	}
	if !bytes.Equal(current, expected) {
		return errors.New("config changed during repair")
	}
	tempName, temp, err := createConfigTemp(directory)
	if err != nil {
		return fmt.Errorf("create replacement config: %w", err)
	}
	removeTemp := true
	defer func() {
		if removeTemp {
			_ = directory.Remove(tempName)
		}
	}()
	if _, err := temp.Write(replacement); err != nil {
		temp.Close()
		return fmt.Errorf("write replacement config: %w", err)
	}
	if err := temp.Sync(); err != nil {
		temp.Close()
		return fmt.Errorf("sync replacement config: %w", err)
	}
	replacementInfo, err := temp.Stat()
	if err != nil {
		temp.Close()
		return fmt.Errorf("inspect replacement config: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close replacement config: %w", err)
	}
	if err := directory.VerifyPath(); err != nil {
		return err
	}
	beforeProtectedFileExchange()
	if err := directory.Exchange(tempName, name); err != nil {
		return fmt.Errorf("replace config: %w", err)
	}
	displaced, _, displacedErr := readProtectedConfigAt(directory, tempName, maximumConfigSize)
	installed, installedErr := directory.Lstat(name)
	if displacedErr != nil || installedErr != nil || !os.SameFile(replacementInfo, installed) ||
		!bytes.Equal(displaced, expected) {
		if installedErr != nil || !os.SameFile(replacementInfo, installed) {
			removeTemp = false
			return &CommittedError{Err: errors.Join(
				errors.New("config path changed after replacement"), displacedErr, installedErr,
			)}
		}
		if restoreErr := directory.Exchange(tempName, name); restoreErr != nil {
			removeTemp = false
			return &CommittedError{Err: fmt.Errorf(
				"config changed during repair and restoration failed: %w", restoreErr,
			)}
		}
		if syncErr := directory.Sync(); syncErr != nil {
			return &CommittedError{Err: fmt.Errorf("sync restored config: %w", syncErr)}
		}
		return errors.Join(errors.New("config changed during repair"), displacedErr)
	}
	if err := directory.Sync(); err != nil {
		return &CommittedError{Err: fmt.Errorf("sync replaced config: %w", err)}
	}
	if err := directory.Remove(tempName); err != nil {
		removeTemp = false
		return &CommittedError{Err: fmt.Errorf("remove displaced config: %w", err)}
	}
	removeTemp = false
	if err := directory.Sync(); err != nil {
		return &CommittedError{Err: fmt.Errorf("sync displaced config removal: %w", err)}
	}
	if err := directory.VerifyPath(); err != nil {
		return &CommittedError{Err: err}
	}
	return nil
}

var beforeProtectedFileExchange = func() {}

func writeNewForRuntime(path string, cfg Config, capabilities RuntimeCapabilities) error {
	if err := cfg.ValidateForRuntime(capabilities); err != nil {
		return err
	}
	directoryLease, name, err := prepareWrite(path)
	if err != nil {
		return err
	}
	defer directoryLease.Close()

	tempName, temp, err := createConfigTemp(directoryLease)
	if err != nil {
		return fmt.Errorf("create temporary config: %w", err)
	}
	defer directoryLease.Remove(tempName)

	encoder := json.NewEncoder(temp)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(cfg); err != nil {
		temp.Close()
		return fmt.Errorf("encode config: %w", err)
	}
	if err := temp.Sync(); err != nil {
		temp.Close()
		return fmt.Errorf("sync config: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close config: %w", err)
	}
	if err := directoryLease.VerifyPath(); err != nil {
		return fmt.Errorf("verify config directory before install: %w", err)
	}
	committed, installErr := directoryLease.PublishNoReplace(tempName, name)
	if installErr != nil && !IsCommitted(installErr) {
		if committed {
			return &CommittedError{Err: fmt.Errorf("install config: %w", installErr)}
		}
		return fmt.Errorf("install config: %w", installErr)
	}
	if err := syncPublishedConfig(directoryLease); err != nil {
		if installErr != nil {
			err = errors.Join(installErr, err)
		}
		return &CommittedError{Err: err}
	}
	if err := directoryLease.VerifyPath(); err != nil {
		return &CommittedError{Err: err}
	}
	if installErr != nil {
		return installErr
	}
	return nil
}

// PrepareWrite creates and validates the config directory and confirms that
// path is available. Callers that create related credentials before WriteNew
// use this to reject an unsafe config authority before writing secret state.
func PrepareWrite(path string) error {
	directoryLease, _, err := prepareWrite(path)
	if err != nil {
		return err
	}
	return directoryLease.Close()
}

// PreparePrivateDirectory creates and validates a current-user-owned directory
// using the same platform protections as the config authority.
func PreparePrivateDirectory(path string) error {
	if !filepath.IsAbs(path) {
		return errors.New("private directory path must be absolute")
	}
	if err := createDirectoriesDurably(path); err != nil {
		return fmt.Errorf("create private directory: %w", err)
	}
	if err := protectPrivateDirectory(path); err != nil {
		return fmt.Errorf("protect private directory: %w", err)
	}
	return ValidatePrivateDirectory(path)
}

// ValidatePrivateDirectory non-mutatingly verifies the current-user authority
// established by PreparePrivateDirectory.
func ValidatePrivateDirectory(path string) error {
	if !filepath.IsAbs(path) {
		return errors.New("private directory path must be absolute")
	}
	directoryLease, err := holdPrivateDirectory(path)
	if err != nil {
		return err
	}
	return directoryLease.Close()
}

// ReadProtectedFile reads a bounded current-user-only regular file through the
// same no-alias authority checks used for Delegation configuration.
func ReadProtectedFile(path string, maximumBytes int) ([]byte, error) {
	return readProtectedFile(path, maximumBytes, false)
}

// ReadProtectedSingleLinkFile applies ReadProtectedFile's protections and
// additionally requires the opened file to have exactly one hard link. Use it
// for an authority whose contents must not be reachable through another path.
func ReadProtectedSingleLinkFile(path string, maximumBytes int) ([]byte, error) {
	return readProtectedFile(path, maximumBytes, true)
}

func readProtectedFile(path string, maximumBytes int, requireSingleLink bool) ([]byte, error) {
	if maximumBytes < 1 || maximumBytes > maximumProtectedReadSize {
		return nil, fmt.Errorf("protected file limit must be from 1 through %d bytes", maximumProtectedReadSize)
	}
	file, err := openProtectedConfig(path)
	if err != nil {
		return nil, err
	}
	if requireSingleLink {
		count, linkErr := openedProtectedFileLinkCount(file)
		if linkErr != nil {
			_ = file.Close()
			return nil, fmt.Errorf("inspect protected file hard-link count: %w", linkErr)
		}
		if count != 1 {
			_ = file.Close()
			return nil, fmt.Errorf("protected file has unexpected hard-link count %d; expected 1", count)
		}
	}
	data, readErr := io.ReadAll(io.LimitReader(file, int64(maximumBytes)+1))
	closeErr := file.Close()
	if readErr != nil {
		return nil, fmt.Errorf("read protected file: %w", readErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close protected file: %w", closeErr)
	}
	if len(data) > maximumBytes {
		return nil, fmt.Errorf("protected file exceeds %d-byte limit", maximumBytes)
	}
	return data, nil
}

func prepareWrite(path string) (*securefs.Root, string, error) {
	if !filepath.IsAbs(path) {
		return nil, "", errors.New("config file path must be absolute")
	}

	dir := filepath.Dir(path)
	if err := createDirectoriesDurably(dir); err != nil {
		return nil, "", fmt.Errorf("create config directory: %w", err)
	}
	directoryLease, err := holdConfigDirectory(dir)
	if err != nil {
		return nil, "", err
	}
	name := filepath.Base(path)
	if _, err := directoryLease.Lstat(name); err == nil {
		_ = directoryLease.Close()
		return nil, "", fmt.Errorf("config already exists: %s", path)
	} else if !errors.Is(err, os.ErrNotExist) {
		_ = directoryLease.Close()
		return nil, "", fmt.Errorf("inspect config: %w", err)
	}
	return directoryLease, name, nil
}

var syncInstalledConfig = syncParentDirectory
var syncPublishedConfig = func(root *securefs.Root) error {
	return root.Sync()
}

func createDirectoriesDurably(path string) error {
	var missing []string
	current := filepath.Clean(path)
	for {
		info, err := os.Stat(current)
		if err == nil {
			if !info.IsDir() {
				return fmt.Errorf("path component is not a directory: %s", current)
			}
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		missing = append(missing, current)
		parent := filepath.Dir(current)
		if parent == current {
			return fmt.Errorf("no existing parent directory for %s", path)
		}
		current = parent
	}
	if err := validateConfigDirectoryLocation(current); err != nil {
		return fmt.Errorf("validate existing config directory location %s: %w", current, err)
	}
	// A prior attempt may have created the existing anchor but failed while
	// syncing its parent. Syncing it again makes retries close that durability
	// gap before creating any descendants.
	if err := syncInstalledConfig(filepath.Dir(current)); err != nil {
		return fmt.Errorf("sync parent of existing directory %s: %w", current, err)
	}

	for i := len(missing) - 1; i >= 0; i-- {
		directory := missing[i]
		if err := createConfigDirectory(directory); err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
		if err := validateConfigDirectoryLocation(directory); err != nil {
			return fmt.Errorf("validate new config directory location %s: %w", directory, err)
		}
		if err := syncInstalledConfig(filepath.Dir(directory)); err != nil {
			return fmt.Errorf("sync parent of new directory %s: %w", directory, err)
		}
	}
	return nil
}
