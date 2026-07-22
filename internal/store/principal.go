package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/identity"
)

// CreateWorkerPrincipal records the broker-side identity used by a managed
// worker. The caller remains responsible for reserving and starting that worker
// on its target peer.
func (s *Store) CreateWorkerPrincipal(
	ctx context.Context,
	controllerID, treeID, agentID, parentAgentID, deviceID string,
	createdAt time.Time,
) (control.Principal, error) {
	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "controllerId", value: controllerID},
		{name: "treeId", value: treeID},
		{name: "agentId", value: agentID},
		{name: "parentAgentId", value: parentAgentID},
		{name: "deviceId", value: deviceID},
	} {
		if err := identity.ValidateID(field.value); err != nil {
			return control.Principal{}, fmt.Errorf("%s %w", field.name, err)
		}
	}
	timestamp, err := unixTime(createdAt, "createdAt")
	if err != nil {
		return control.Principal{}, err
	}
	principal := control.NewWorkerPrincipal(controllerID, treeID, agentID, parentAgentID, deviceID)
	if err := principal.Validate(); err != nil {
		return control.Principal{}, err
	}
	capabilitiesJSON, err := json.Marshal(principal.Capabilities)
	if err != nil {
		return control.Principal{}, fmt.Errorf("encode worker capabilities: %w", err)
	}

	var stored control.Principal
	err = s.withImmediateTransaction(ctx, func(connection *sql.Conn) error {
		stored, err = queryPrincipal(ctx, connection, controllerID, treeID, agentID)
		if err == nil {
			if !stored.Matches(principal.Identity()) {
				return fmt.Errorf("%w: worker principal already exists with another identity", ErrConflict)
			}
			return nil
		}
		if !errors.Is(err, ErrNotFound) {
			return err
		}
		if _, err := queryTreeByID(ctx, connection, controllerID, treeID); err != nil {
			return err
		}
		if _, err := queryPrincipal(ctx, connection, controllerID, treeID, parentAgentID); err != nil {
			return fmt.Errorf("worker parent: %w", err)
		}
		device, err := queryDevice(ctx, connection, controllerID, deviceID)
		if err != nil {
			return fmt.Errorf("worker device: %w", err)
		}
		if !device.Online {
			return fmt.Errorf("%w: worker device must be online", ErrConflict)
		}
		if _, err := connection.ExecContext(ctx, `
INSERT INTO principals(
    controller_id, tree_id, agent_id, parent_agent_id, device_id, capabilities_json, created_at
) VALUES (?, ?, ?, ?, ?, ?, ?)
`, controllerID, treeID, agentID, parentAgentID, deviceID, string(capabilitiesJSON), timestamp); err != nil {
			return fmt.Errorf("create worker principal: %w", err)
		}
		stored = principal
		return nil
	})
	return stored, err
}
