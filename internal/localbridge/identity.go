package localbridge

import (
	"fmt"

	"github.com/GhostFlying/delegation/internal/identity"
	"github.com/GhostFlying/delegation/internal/instanceid"
)

const methodIdentity = "bridge.identity"

// ServiceIdentity binds a local bridge to one configured connector principal.
type ServiceIdentity struct {
	ControllerID string `json:"controllerId"`
	DeviceID     string `json:"deviceId"`
	InstanceID   string `json:"instanceId,omitempty"`
}

func (i ServiceIdentity) Validate() error {
	if i.InstanceID != "" {
		if err := instanceid.Validate(i.InstanceID); err != nil {
			return err
		}
	}
	if err := identity.ValidateID(i.ControllerID); err != nil {
		return fmt.Errorf("controllerId %w", err)
	}
	if err := identity.ValidateID(i.DeviceID); err != nil {
		return fmt.Errorf("deviceId %w", err)
	}
	return nil
}

func (i ServiceIdentity) EffectiveInstanceID() string {
	if i.InstanceID == "" {
		return "default"
	}
	return i.InstanceID
}

func (i ServiceIdentity) Equal(other ServiceIdentity) bool {
	return i.ControllerID == other.ControllerID &&
		i.DeviceID == other.DeviceID &&
		i.EffectiveInstanceID() == other.EffectiveInstanceID()
}
