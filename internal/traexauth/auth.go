// Package traexauth validates and synchronizes the one authentication artifact
// that an isolated managed TraeX app-server is allowed to consume.
package traexauth

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/securefs"
)

const (
	maximumAuthBytes = 64 << 10
	managedAuthName  = "auth.json"
)

var (
	ErrInvalidSource      = errors.New("TraeX authentication source is invalid")
	ErrInvalidManagedCopy = errors.New("managed TraeX authentication copy is invalid")
)

type authDocument struct {
	AuthMode string      `json:"auth_mode"`
	Trae     traeAccount `json:"trae"`
}

type traeAccount struct {
	AccessToken    string `json:"access_token"`
	CredentialKind string `json:"credential_kind"`
	LoginMethod    string `json:"login_method"`
	Region         string `json:"region"`
	Version        int    `json:"version"`
}

// ManagedPath returns the only credential location permitted in a managed
// TraeX home. The worker profile denies this exact file to model tools.
func ManagedPath(managedHome string) string {
	return filepath.Join(managedHome, "cli", managedAuthName)
}

// ReadSource reads a current-user-only source and verifies the minimum Trae
// account shape without exposing any credential value in an error. Unknown
// fields remain forward compatible with TraeX-owned auth schema additions.
func ReadSource(path string) ([]byte, error) {
	data, err := delegationconfig.ReadProtectedSingleLinkFile(path, maximumAuthBytes)
	if err != nil {
		return nil, fmt.Errorf("%w: read protected source: %v", ErrInvalidSource, err)
	}
	if err := Validate(data); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidSource, err)
	}
	return data, nil
}

// Validate verifies the non-secret shape needed by the supported interactive
// Trae account flow. It deliberately never includes a field value in errors.
func Validate(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	var document authDocument
	if err := decoder.Decode(&document); err != nil {
		return errors.New("TraeX authentication source must contain valid JSON")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("TraeX authentication source must contain one JSON value")
	}
	if document.AuthMode != "trae" {
		return errors.New("TraeX authentication source must use auth_mode trae")
	}
	if strings.TrimSpace(document.Trae.AccessToken) == "" {
		return errors.New("TraeX authentication source is missing a Trae access token")
	}
	if document.Trae.CredentialKind != "cloud_cli_jwt" {
		return errors.New("TraeX authentication source has an unsupported credential kind")
	}
	if strings.TrimSpace(document.Trae.LoginMethod) == "" ||
		strings.TrimSpace(document.Trae.Region) == "" || document.Trae.Version < 1 {
		return errors.New("TraeX authentication source has incomplete account metadata")
	}
	return nil
}

// ValidateManagedCopy applies the same protected-file and schema checks as the
// source. An absent copy is accepted only before the first service start.
func ValidateManagedCopy(managedHome string, required bool) error {
	path := ManagedPath(managedHome)
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		if required {
			return fmt.Errorf("%w: copy is missing", ErrInvalidManagedCopy)
		}
		return nil
	} else if err != nil {
		return fmt.Errorf("%w: inspect copy: %v", ErrInvalidManagedCopy, err)
	}
	data, err := delegationconfig.ReadProtectedSingleLinkFile(path, maximumAuthBytes)
	if err != nil {
		return fmt.Errorf("%w: validate copy protection: %v", ErrInvalidManagedCopy, err)
	}
	if err := Validate(data); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidManagedCopy, err)
	}
	return nil
}

// Sync reads the protected source once and atomically installs those exact
// bytes as a private managed copy. Existing copies are derived state and may
// be replaced; directories and symbolic links are rejected.
func Sync(data []byte, managedHome string) (string, error) {
	if err := Validate(data); err != nil {
		return "", err
	}
	cliHome := filepath.Join(managedHome, "cli")
	if err := delegationconfig.PreparePrivateDirectory(cliHome); err != nil {
		return "", fmt.Errorf("prepare managed TRAECLI_HOME: %w", err)
	}
	root, err := securefs.OpenRoot(cliHome, nil)
	if err != nil {
		return "", fmt.Errorf("hold managed TRAECLI_HOME: %w", err)
	}
	defer root.Close()
	if info, statErr := root.Lstat(managedAuthName); statErr == nil {
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			return "", errors.New("managed TraeX authentication copy must be a regular file, not a symbolic link")
		}
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return "", fmt.Errorf("inspect managed TraeX authentication copy: %w", statErr)
	}
	temporary, file, err := createTemporary(root)
	if err != nil {
		return "", err
	}
	removeTemporary := true
	defer func() {
		if removeTemporary {
			_ = root.Remove(temporary)
		}
	}()
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return "", fmt.Errorf("write managed TraeX authentication copy: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return "", fmt.Errorf("sync managed TraeX authentication copy: %w", err)
	}
	if err := file.Close(); err != nil {
		return "", fmt.Errorf("close managed TraeX authentication copy: %w", err)
	}
	if err := root.VerifyPath(); err != nil {
		return "", err
	}
	committed, err := root.Replace(temporary, managedAuthName)
	if committed {
		removeTemporary = false
	}
	if err != nil {
		return "", fmt.Errorf("publish managed TraeX authentication copy: %w", err)
	}
	destination := ManagedPath(managedHome)
	installed, err := delegationconfig.ReadProtectedSingleLinkFile(destination, maximumAuthBytes)
	if err != nil || !bytes.Equal(installed, data) {
		return "", errors.Join(err, errors.New("managed TraeX authentication copy verification failed"))
	}
	return destination, nil
}

func createTemporary(root *securefs.Root) (string, *os.File, error) {
	for attempt := range 100 {
		name := fmt.Sprintf(".auth-%d-%d.tmp", os.Getpid(), attempt)
		file, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if errors.Is(err, os.ErrExist) {
			continue
		}
		if err != nil {
			return "", nil, fmt.Errorf("create managed TraeX authentication temporary file: %w", err)
		}
		if err := file.Chmod(0o600); err != nil {
			_ = file.Close()
			_ = root.Remove(name)
			return "", nil, err
		}
		return name, file, nil
	}
	return "", nil, errors.New("create managed TraeX authentication temporary file: exhausted attempts")
}
