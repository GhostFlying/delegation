package localbridge

import (
	"fmt"

	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/identity"
	"github.com/GhostFlying/delegation/internal/instanceid"
)

const methodIdentity = "bridge.identity"

// ServiceIdentity binds a local bridge to one configured connector principal.
type ServiceIdentity struct {
	Role         delegationconfig.Role `json:"role,omitempty"`
	ControllerID string                `json:"controllerId"`
	DeviceID     string                `json:"deviceId,omitempty"`
	InstanceID   string                `json:"instanceId,omitempty"`
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
	switch i.EffectiveRole() {
	case delegationconfig.RoleBroker:
		if i.DeviceID != "" {
			return fmt.Errorf("broker local bridge must not contain a deviceId")
		}
	case delegationconfig.RolePeer:
		if err := identity.ValidateID(i.DeviceID); err != nil {
			return fmt.Errorf("deviceId %w", err)
		}
	default:
		return fmt.Errorf("unsupported local bridge role %q", i.Role)
	}
	return nil
}

func (i ServiceIdentity) EffectiveRole() delegationconfig.Role {
	if i.Role == "" {
		return delegationconfig.RolePeer
	}
	return i.Role
}

func (i ServiceIdentity) EffectiveInstanceID() string {
	if i.InstanceID == "" {
		return "default"
	}
	return i.InstanceID
}

func (i ServiceIdentity) Equal(other ServiceIdentity) bool {
	return i.EffectiveRole() == other.EffectiveRole() &&
		i.ControllerID == other.ControllerID &&
		i.DeviceID == other.DeviceID &&
		i.EffectiveInstanceID() == other.EffectiveInstanceID()
}
