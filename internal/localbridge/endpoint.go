package localbridge

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/GhostFlying/delegation/internal/identity"
	"github.com/GhostFlying/delegation/internal/instanceid"
)

func Endpoint(controllerID, deviceID string) (string, error) {
	return EndpointForInstance("default", controllerID, deviceID)
}

func EndpointForInstance(instanceID, controllerID, deviceID string) (string, error) {
	if err := instanceid.Validate(instanceID); err != nil {
		return "", err
	}
	if err := identity.ValidateID(controllerID); err != nil {
		return "", fmt.Errorf("controllerId %w", err)
	}
	if err := identity.ValidateID(deviceID); err != nil {
		return "", fmt.Errorf("deviceId %w", err)
	}
	namespace := "delegation-localbridge-v1\x00" + controllerID + "\x00" + deviceID
	if instanceID != "default" {
		namespace = "delegation-localbridge-v2\x00" + instanceID + "\x00" + controllerID + "\x00" + deviceID
	}
	digest := sha256.Sum256([]byte(namespace))
	name := hex.EncodeToString(digest[:16])
	return platformEndpoint(name)
}
