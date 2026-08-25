package tailscaleauth

import (
	"bytes"
	"errors"
	"fmt"
	"os"

	delegationconfig "github.com/GhostFlying/delegation/internal/config"
)

const (
	authKeyPrefix       = "tskey-auth-"
	maximumKeyFileBytes = 4 * 1024
	redactedKey         = "[REDACTED]"
)

// Key holds a Tailscale enrollment auth key without exposing it through
// formatting. Enrollment key files use the protected config authority,
// including the exact mode 0600 requirement on Unix.
type Key func(keyAccess) string

type keyAccess struct {
	packageToken struct{}
}

// Key is a function so fmt's special %p path emits only the shared
// closure code address, not a bad-verb reflection of captured key material.
// Read loads and validates a Tailscale enrollment auth key from a protected
// current-user-only regular file. On Unix, the file must have exact mode 0600.
func Read(path string) (Key, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("inspect protected Tailscale enrollment key file %q: %w", path, err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("protected Tailscale enrollment key file %q must not be a symbolic link", path)
	}
	data, err := delegationconfig.ReadProtectedFile(path, maximumKeyFileBytes)
	if err != nil {
		return nil, fmt.Errorf(
			"read protected Tailscale enrollment key file %q (exact mode 0600 required on Unix): %w",
			path,
			err,
		)
	}
	value, err := parse(data)
	if err != nil {
		return nil, fmt.Errorf("validate Tailscale enrollment key file %q: %w", path, err)
	}
	return func(keyAccess) string { return value }, nil
}

// AuthKey returns the key value for explicit handoff to Tailscale.
func (key Key) AuthKey() string {
	if key == nil {
		return ""
	}
	return key(keyAccess{})
}

// Format writes a stable redaction for every formatter-controlled verb,
// ignoring flags, width, and precision.
func (Key) Format(state fmt.State, _ rune) {
	_, _ = state.Write([]byte(redactedKey))
}

func (Key) String() string {
	return redactedKey
}

func (Key) GoString() string {
	return redactedKey
}

func parse(data []byte) (string, error) {
	line := data
	if len(line) > 0 && line[len(line)-1] == '\n' {
		line = line[:len(line)-1]
		if len(line) > 0 && line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
	}
	if len(line) == 0 {
		return "", errors.New("auth key file must contain a non-empty key")
	}
	for _, character := range line {
		if character < 0x21 || character > 0x7e {
			return "", errors.New("auth key must be one visible ASCII line")
		}
	}
	if !bytes.HasPrefix(line, []byte(authKeyPrefix)) {
		return "", errors.New("auth key must use the tskey-auth- prefix")
	}
	if len(line) == len(authKeyPrefix) {
		return "", errors.New("auth key must contain a value after the tskey-auth- prefix")
	}
	return string(line), nil
}
