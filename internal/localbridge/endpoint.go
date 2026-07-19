package localbridge

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/GhostFlying/delegation/internal/identity"
)

func Endpoint(controllerID, deviceID string) (string, error) {
	if err := identity.ValidateID(controllerID); err != nil {
		return "", fmt.Errorf("controllerId %w", err)
	}
	if err := identity.ValidateID(deviceID); err != nil {
		return "", fmt.Errorf("deviceId %w", err)
	}
	digest := sha256.Sum256([]byte("delegation-localbridge-v1\x00" + controllerID + "\x00" + deviceID))
	name := hex.EncodeToString(digest[:16])
	return platformEndpoint(name)
}
