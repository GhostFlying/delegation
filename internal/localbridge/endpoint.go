package localbridge

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"

	"github.com/GhostFlying/delegation/internal/identity"
)

func Endpoint(configPath, deviceID string) (string, error) {
	if err := identity.ValidateID(deviceID); err != nil {
		return "", fmt.Errorf("deviceId %w", err)
	}
	absolute, err := filepath.Abs(configPath)
	if err != nil {
		return "", fmt.Errorf("resolve local bridge config path: %w", err)
	}
	clean := normalizeConfigPath(filepath.Clean(absolute))
	digest := sha256.Sum256([]byte(clean + "\x00" + deviceID))
	name := hex.EncodeToString(digest[:16])
	return platformEndpoint(filepath.Dir(absolute), name)
}
