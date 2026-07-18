package tokenfile

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
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

type Token [tokenBytes]byte

func Generate() (Token, error) {
	var token Token
	if _, err := rand.Read(token[:]); err != nil {
		return Token{}, fmt.Errorf("generate token: %w", err)
	}
	return token, nil
}

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

	token, err := Generate()
	if err != nil {
		return false, err
	}
	created, err := WriteNew(path, token)
	if errors.Is(err, os.ErrExist) {
		return false, Validate(path)
	}
	return created, err
}

// WriteNew writes token to a protected file without replacing an existing path.
// The returned boolean reports whether the file was created, including when
// final durability work fails after creation.
func WriteNew(path string, token Token) (bool, error) {
	if !filepath.IsAbs(path) {
		return false, errors.New("token file path must be absolute")
	}
	if err := createDirectoriesDurably(filepath.Dir(path)); err != nil {
		return false, fmt.Errorf("create token directory: %w", err)
	}
	if err := validateTokenDirectory(filepath.Dir(path)); err != nil {
		return false, err
	}
	encoded := base64.RawURLEncoding.EncodeToString(token[:]) + "\n"

	tempPath, file, err := createSecureTemp(filepath.Dir(path))
	if err != nil {
		return false, err
	}
	tempInstalled := false
	cleanupTemp := func() error {
		if tempInstalled {
			return nil
		}
		if err := os.Remove(tempPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove temporary token file: %w", err)
		}
		return nil
	}

	if _, err := io.WriteString(file, encoded); err != nil {
		closeErr := file.Close()
		return false, errors.Join(fmt.Errorf("write temporary token file: %w", err), closeErr, cleanupTemp())
	}
	if err := file.Sync(); err != nil {
		closeErr := file.Close()
		return false, errors.Join(fmt.Errorf("sync temporary token file: %w", err), closeErr, cleanupTemp())
	}
	if err := file.Close(); err != nil {
		return false, errors.Join(fmt.Errorf("close temporary token file: %w", err), cleanupTemp())
	}
	committed, err := installTokenNoReplace(tempPath, path)
	if committed && err == nil {
		tempInstalled = true
	}
	if err != nil {
		return committed, errors.Join(fmt.Errorf("install token file: %w", err), cleanupTemp())
	}
	if err := syncTokenDirectory(filepath.Dir(path)); err != nil {
		return true, fmt.Errorf("token file was created but directory sync failed: %w", err)
	}
	return true, nil
}

// Validate checks token format and platform-specific file protections.
func Validate(path string) error {
	_, err := Read(path)
	return err
}

// Read validates and returns token material from a protected token file.
func Read(path string) (Token, error) {
	token, file, _, err := openAndRead(path)
	if err != nil {
		return Token{}, err
	}
	if err := file.Close(); err != nil {
		return Token{}, fmt.Errorf("close token file: %w", err)
	}
	return token, nil
}

func openAndRead(path string) (Token, *os.File, os.FileInfo, error) {
	if !filepath.IsAbs(path) {
		return Token{}, nil, nil, errors.New("token file path must be absolute")
	}
	info, err := os.Lstat(path)
	if err != nil {
		return Token{}, nil, nil, fmt.Errorf("inspect token file: %w", err)
	}
	if !info.Mode().IsRegular() {
		return Token{}, nil, nil, errors.New("token file must be a regular file")
	}
	file, err := openSecureRead(path)
	if err != nil {
		return Token{}, nil, nil, fmt.Errorf("open token file: %w", err)
	}
	fail := func(err error) (Token, *os.File, os.FileInfo, error) {
		_ = file.Close()
		return Token{}, nil, nil, err
	}
	openedInfo, err := file.Stat()
	if err != nil {
		return fail(fmt.Errorf("inspect opened token file: %w", err))
	}
	if !os.SameFile(info, openedInfo) {
		return fail(errors.New("token file changed while it was being opened"))
	}
	if err := validateFilePermissions(file, openedInfo); err != nil {
		return fail(err)
	}

	data, err := io.ReadAll(io.LimitReader(file, maxTokenFileSize+1))
	if err != nil {
		return fail(fmt.Errorf("read token file: %w", err))
	}
	if len(data) > maxTokenFileSize {
		return fail(errors.New("token file is too large"))
	}
	data = bytes.TrimSuffix(data, []byte("\n"))
	decoded, err := base64.RawURLEncoding.DecodeString(string(data))
	if err != nil || len(decoded) != tokenBytes {
		return fail(errors.New("token file must contain one 256-bit base64url token"))
	}
	var token Token
	copy(token[:], decoded)
	return token, file, openedInfo, nil
}

func createSecureTemp(directory string) (string, *os.File, error) {
	for range 100 {
		random := make([]byte, 16)
		if _, err := rand.Read(random); err != nil {
			return "", nil, fmt.Errorf("generate temporary token name: %w", err)
		}
		path := filepath.Join(directory, ".token-"+hex.EncodeToString(random)+".tmp")
		file, err := openSecureNew(path)
		if errors.Is(err, os.ErrExist) {
			continue
		}
		if err != nil {
			return "", nil, fmt.Errorf("create temporary token file: %w", err)
		}
		return path, file, nil
	}
	return "", nil, errors.New("create temporary token file: exhausted name attempts")
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
