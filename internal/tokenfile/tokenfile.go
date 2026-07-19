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

	"github.com/GhostFlying/delegation/internal/securefs"
)

const (
	tokenBytes       = 32
	tokenEncodedSize = 43
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

func Encode(token Token) string {
	return base64.RawURLEncoding.EncodeToString(token[:])
}

func Parse(value string) (Token, error) {
	if len(value) != tokenEncodedSize {
		return Token{}, errors.New("token must be one 256-bit base64url value")
	}
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || len(decoded) != tokenBytes {
		return Token{}, errors.New("token must be one 256-bit base64url value")
	}
	var token Token
	copy(token[:], decoded)
	if Encode(token) != value {
		return Token{}, errors.New("token must use canonical base64url encoding")
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
	directoryLease, err := holdTokenDirectory(filepath.Dir(path))
	if err != nil {
		return false, err
	}
	defer directoryLease.Close()
	encoded := Encode(token) + "\n"

	tempName, file, err := createSecureTemp(directoryLease)
	if err != nil {
		return false, err
	}
	tempInstalled := false
	cleanupTemp := func() error {
		if tempInstalled {
			return nil
		}
		if err := directoryLease.Remove(tempName); err != nil && !errors.Is(err, os.ErrNotExist) {
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
	if err := directoryLease.VerifyPath(); err != nil {
		return false, errors.Join(fmt.Errorf("verify token directory before install: %w", err), cleanupTemp())
	}
	committed, err := directoryLease.PublishNoReplace(tempName, filepath.Base(path))
	if committed && err == nil {
		tempInstalled = true
	}
	if err != nil {
		return committed, errors.Join(fmt.Errorf("install token file: %w", err), cleanupTemp())
	}
	if err := syncPublishedToken(directoryLease); err != nil {
		return true, fmt.Errorf("token file was created but directory sync failed: %w", err)
	}
	if err := directoryLease.VerifyPath(); err != nil {
		return true, fmt.Errorf("token file was created but directory path changed: %w", err)
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
	if !filepath.IsAbs(path) {
		return Token{}, errors.New("token file path must be absolute")
	}
	directory, err := holdTokenDirectory(filepath.Dir(path))
	if err != nil {
		return Token{}, err
	}
	defer directory.Close()
	name := filepath.Base(path)
	info, err := directory.Lstat(name)
	if err != nil {
		return Token{}, fmt.Errorf("inspect token file: %w", err)
	}
	if !info.Mode().IsRegular() {
		return Token{}, errors.New("token file must be a regular file")
	}
	file, err := openSecureRead(directory, name)
	if err != nil {
		return Token{}, fmt.Errorf("open token file: %w", err)
	}
	fail := func(err error) (Token, error) {
		_ = file.Close()
		return Token{}, err
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
	if err := directory.VerifyPath(); err != nil {
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
	token, err := Parse(string(data))
	if err != nil {
		return fail(errors.New("token file must contain one 256-bit base64url token"))
	}
	if err := file.Close(); err != nil {
		return Token{}, fmt.Errorf("close token file: %w", err)
	}
	return token, nil
}

func createSecureTemp(directory *securefs.Root) (string, *os.File, error) {
	for range 100 {
		random := make([]byte, 16)
		if _, err := rand.Read(random); err != nil {
			return "", nil, fmt.Errorf("generate temporary token name: %w", err)
		}
		name := ".token-" + hex.EncodeToString(random) + ".tmp"
		file, err := openSecureNew(directory, name)
		if errors.Is(err, os.ErrExist) {
			continue
		}
		if err != nil {
			return "", nil, fmt.Errorf("create temporary token file: %w", err)
		}
		return name, file, nil
	}
	return "", nil, errors.New("create temporary token file: exhausted name attempts")
}

var syncTokenDirectory = syncParentDirectory
var syncPublishedToken = func(root *securefs.Root) error {
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
	if err := validateTokenDirectoryLocation(current); err != nil {
		return fmt.Errorf("validate existing token directory location %s: %w", current, err)
	}
	if err := syncTokenDirectory(filepath.Dir(current)); err != nil {
		return fmt.Errorf("sync parent of existing directory %s: %w", current, err)
	}

	for i := len(missing) - 1; i >= 0; i-- {
		directory := missing[i]
		if err := createTokenDirectory(directory); err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
		if err := validateTokenDirectoryLocation(directory); err != nil {
			return fmt.Errorf("validate new token directory location %s: %w", directory, err)
		}
		if err := syncTokenDirectory(filepath.Dir(directory)); err != nil {
			return fmt.Errorf("sync parent of new directory %s: %w", directory, err)
		}
	}
	return nil
}
