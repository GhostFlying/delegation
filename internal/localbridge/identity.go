package localbridge

import (
	"fmt"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/identity"
)

const methodIdentity = "bridge.identity"

// ServiceIdentity binds a local bridge to one configured connector principal.
type ServiceIdentity struct {
	ControllerID string             `json:"controllerId"`
	DeviceID     string             `json:"deviceId"`
	Role         control.DeviceRole `json:"role"`
}

func (i ServiceIdentity) Validate() error {
	if err := identity.ValidateID(i.ControllerID); err != nil {
		return fmt.Errorf("controllerId %w", err)
	}
	if err := identity.ValidateID(i.DeviceID); err != nil {
		return fmt.Errorf("deviceId %w", err)
	}
	if err := i.Role.Validate(); err != nil {
		return fmt.Errorf("role: %w", err)
	}
	return nil
}
