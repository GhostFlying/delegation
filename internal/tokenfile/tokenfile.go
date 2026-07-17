package tokenfile

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const (
	tokenBytes       = 32
	maxTokenFileSize = 128
)

// Ensure creates a protected token file when it does not exist. Existing files
// are validated and never replaced.
func Ensure(path string) (bool, error) {
	if !filepath.IsAbs(path) {
		return false, errors.New("token file path must be absolute")
	}
	if _, err := os.Lstat(path); err == nil {
		return false, Validate(path)
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, fmt.Errorf("inspect token file: %w", err)
	}

	if err := createDirectoriesDurably(filepath.Dir(path)); err != nil {
		return false, fmt.Errorf("create token directory: %w", err)
	}
	random := make([]byte, tokenBytes)
	if _, err := rand.Read(random); err != nil {
		return false, fmt.Errorf("generate token: %w", err)
	}
	encoded := base64.RawURLEncoding.EncodeToString(random) + "\n"

	file, err := openSecureNew(path)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return false, Validate(path)
		}
		return false, fmt.Errorf("create token file: %w", err)
	}
	removeCreated := func() {
		info, statErr := file.Stat()
		file.Close()
		if statErr != nil {
			return
		}
		current, currentErr := os.Lstat(path)
		if currentErr == nil && os.SameFile(info, current) {
			_ = os.Remove(path)
		}
	}

	if _, err := io.WriteString(file, encoded); err != nil {
		removeCreated()
		return false, fmt.Errorf("write token file: %w", err)
	}
	if err := file.Sync(); err != nil {
		removeCreated()
		return false, fmt.Errorf("sync token file: %w", err)
	}
	if err := file.Close(); err != nil {
		return true, fmt.Errorf("token file was created but close failed: %w", err)
	}
	if err := syncTokenDirectory(filepath.Dir(path)); err != nil {
		return true, fmt.Errorf("token file was created but directory sync failed: %w", err)
	}
	return true, nil
}

// Validate checks token format and platform-specific file protections.
func Validate(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect token file: %w", err)
	}
	if !info.Mode().IsRegular() {
		return errors.New("token file must be a regular file")
	}
	file, err := openSecureRead(path)
	if err != nil {
		return fmt.Errorf("open token file: %w", err)
	}
	defer file.Close()
	openedInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("inspect opened token file: %w", err)
	}
	if !os.SameFile(info, openedInfo) {
		return errors.New("token file changed while it was being opened")
	}
	if err := validateFilePermissions(file, openedInfo); err != nil {
		return err
	}

	data, err := io.ReadAll(io.LimitReader(file, maxTokenFileSize+1))
	if err != nil {
		return fmt.Errorf("read token file: %w", err)
	}
	if len(data) > maxTokenFileSize {
		return errors.New("token file is too large")
	}
	data = bytes.TrimSuffix(data, []byte("\n"))
	decoded, err := base64.RawURLEncoding.DecodeString(string(data))
	if err != nil || len(decoded) != tokenBytes {
		return errors.New("token file must contain one 256-bit base64url token")
	}
	return nil
}

var syncTokenDirectory = syncParentDirectory

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
	if err := syncTokenDirectory(filepath.Dir(current)); err != nil {
		return fmt.Errorf("sync parent of existing directory %s: %w", current, err)
	}

	for i := len(missing) - 1; i >= 0; i-- {
		directory := missing[i]
		if err := os.Mkdir(directory, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
		if err := syncTokenDirectory(filepath.Dir(directory)); err != nil {
			return fmt.Errorf("sync parent of new directory %s: %w", directory, err)
		}
	}
	return nil
}
