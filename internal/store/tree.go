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

const maximumCapabilityJSON = 4 * 1024

func (s *Store) EnsureRootTree(
	ctx context.Context,
	controllerID, externalThreadID, rootDeviceID string,
	createdAt time.Time,
) (control.Tree, control.Principal, error) {
	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "controllerId", value: controllerID},
		{name: "threadId", value: externalThreadID},
		{name: "rootDeviceId", value: rootDeviceID},
	} {
		if err := identity.ValidateID(field.value); err != nil {
			return control.Tree{}, control.Principal{}, fmt.Errorf("%s %w", field.name, err)
		}
	}
	createdAtUnix, err := unixTime(createdAt, "createdAt")
	if err != nil {
		return control.Tree{}, control.Principal{}, err
	}
	treeID, err := identity.NewID()
	if err != nil {
		return control.Tree{}, control.Principal{}, err
	}
	agentID, err := identity.NewID()
	if err != nil {
		return control.Tree{}, control.Principal{}, err
	}
	var tree control.Tree
	var principal control.Principal
	err = s.withImmediateTransaction(ctx, func(connection *sql.Conn) error {
		tree, err = queryTreeByThread(ctx, connection, controllerID, externalThreadID)
		if err == nil {
			if tree.RootDeviceID != rootDeviceID {
				return fmt.Errorf("%w: thread is bound to a different root device", ErrConflict)
			}
			principal, err = queryPrincipal(ctx, connection, controllerID, tree.TreeID, tree.RootAgentID)
			if err != nil {
				return err
			}
			expected := control.NewRootPrincipal(controllerID, tree.TreeID, tree.RootAgentID, rootDeviceID)
			if !principal.Matches(expected.Identity()) {
				return errors.New("stored root principal does not match its tree")
			}
			return nil
		}
		if !errors.Is(err, ErrNotFound) {
			return err
		}
		device, err := queryDevice(ctx, connection, controllerID, rootDeviceID)
		if err != nil {
			return err
		}
		if !device.Online {
			return fmt.Errorf("%w: root device must be an online peer", ErrConflict)
		}
		tree = control.Tree{
			ControllerID:     controllerID,
			TreeID:           treeID,
			ExternalThreadID: externalThreadID,
			RootAgentID:      agentID,
			RootDeviceID:     rootDeviceID,
			CreatedAt:        createdAtUnix,
		}
		principal = control.NewRootPrincipal(controllerID, treeID, agentID, rootDeviceID)
		capabilitiesJSON, err := json.Marshal(principal.Capabilities)
		if err != nil {
			return fmt.Errorf("encode root capabilities: %w", err)
		}
		if _, err := connection.ExecContext(ctx, `
INSERT INTO trees(
    controller_id, external_thread_id, tree_id, root_agent_id, root_device_id, created_at
) VALUES (?, ?, ?, ?, ?, ?)
`, controllerID, externalThreadID, treeID, agentID, rootDeviceID, createdAtUnix); err != nil {
			return fmt.Errorf("create root tree: %w", err)
		}
		if _, err := connection.ExecContext(ctx, `
INSERT INTO principals(
    controller_id, tree_id, agent_id, parent_agent_id, device_id, capabilities_json, created_at
) VALUES (?, ?, ?, '', ?, ?, ?)
`, controllerID, treeID, agentID, rootDeviceID, string(capabilitiesJSON), createdAtUnix); err != nil {
			return fmt.Errorf("create root principal: %w", err)
		}
		return nil
	})
	return tree, principal, err
}

func (s *Store) AuthorizePrincipal(
	ctx context.Context,
	claimed control.PrincipalIdentity,
	required control.Capability,
) (control.Principal, error) {
	if err := claimed.Validate(); err != nil {
		return control.Principal{}, fmt.Errorf("%w: invalid claimed identity", ErrAuthorizationDenied)
	}
	if err := required.Validate(); err != nil {
		return control.Principal{}, fmt.Errorf("%w: invalid required capability", ErrAuthorizationDenied)
	}
	transaction, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return control.Principal{}, fmt.Errorf("begin principal authorization: %w", err)
	}
	defer transaction.Rollback()
	principal, err := queryPrincipal(ctx, transaction, claimed.ControllerID, claimed.TreeID, claimed.AgentID)
	if errors.Is(err, ErrNotFound) {
		return control.Principal{}, ErrAuthorizationDenied
	}
	if err != nil {
		return control.Principal{}, err
	}
	if !principal.Matches(claimed) {
		return control.Principal{}, ErrAuthorizationDenied
	}
	if principal.ParentAgentID == "" {
		tree, err := queryTreeByID(ctx, transaction, principal.ControllerID, principal.TreeID)
		if errors.Is(err, ErrNotFound) {
			return control.Principal{}, ErrAuthorizationDenied
		}
		if err != nil {
			return control.Principal{}, err
		}
		if tree.RootAgentID != principal.AgentID || tree.RootDeviceID != principal.DeviceID {
			return control.Principal{}, ErrAuthorizationDenied
		}
	} else {
		var parentExists bool
		if err := transaction.QueryRowContext(ctx, `
SELECT EXISTS(
    SELECT 1 FROM principals
    WHERE controller_id = ? AND tree_id = ? AND agent_id = ?
)
`, principal.ControllerID, principal.TreeID, principal.ParentAgentID).Scan(&parentExists); err != nil {
			return control.Principal{}, fmt.Errorf("verify principal parent: %w", err)
		}
		if !parentExists {
			return control.Principal{}, ErrAuthorizationDenied
		}
	}
	if control.Require(principal, required) != nil {
		return control.Principal{}, ErrAuthorizationDenied
	}
	if err := transaction.Commit(); err != nil {
		return control.Principal{}, fmt.Errorf("commit principal authorization: %w", err)
	}
	return principal, nil
}

func queryTreeByThread(
	ctx context.Context,
	queryer rowQueryer,
	controllerID, externalThreadID string,
) (control.Tree, error) {
	return scanTree(queryer.QueryRowContext(ctx, `
SELECT controller_id, tree_id, external_thread_id, root_agent_id, root_device_id, created_at
FROM trees
WHERE controller_id = ? AND external_thread_id = ?

`, controllerID, externalThreadID))
}

func queryTreeByID(
	ctx context.Context,
	queryer rowQueryer,
	controllerID, treeID string,
) (control.Tree, error) {
	return scanTree(queryer.QueryRowContext(ctx, `
SELECT controller_id, tree_id, external_thread_id, root_agent_id, root_device_id, created_at
FROM trees
WHERE controller_id = ? AND tree_id = ?
`, controllerID, treeID))
}

func scanTree(scanner rowScanner) (control.Tree, error) {
	var tree control.Tree
	err := scanner.Scan(
		&tree.ControllerID,
		&tree.TreeID,
		&tree.ExternalThreadID,
		&tree.RootAgentID,
		&tree.RootDeviceID,
		&tree.CreatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return control.Tree{}, ErrNotFound
	}
	if err != nil {
		return control.Tree{}, fmt.Errorf("load root tree: %w", err)
	}
	if err := tree.Validate(); err != nil {
		return control.Tree{}, fmt.Errorf("stored root tree is invalid: %w", err)
	}
	return tree, nil
}

func queryPrincipal(
	ctx context.Context,
	queryer rowQueryer,
	controllerID, treeID, agentID string,
) (control.Principal, error) {
	var principal control.Principal
	var capabilitiesJSON sql.NullString
	var createdAt int64
	err := queryer.QueryRowContext(ctx, principalSelect+`
WHERE controller_id = ? AND tree_id = ? AND agent_id = ?
`, controllerID, treeID, agentID).Scan(
		&principal.ControllerID,
		&principal.TreeID,
		&principal.AgentID,
		&principal.ParentAgentID,
		&principal.DeviceID,
		&capabilitiesJSON,
		&createdAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return control.Principal{}, ErrNotFound
	}
	if err != nil {
		return control.Principal{}, fmt.Errorf("load principal: %w", err)
	}
	if !capabilitiesJSON.Valid {
		return control.Principal{}, errors.New("stored principal capabilities exceed size limit")
	}
	if createdAt < 0 {
		return control.Principal{}, errors.New("stored principal createdAt is negative")
	}
	if err := json.Unmarshal([]byte(capabilitiesJSON.String), &principal.Capabilities); err != nil {
		return control.Principal{}, fmt.Errorf("decode stored principal capabilities: %w", err)
	}
	if err := principal.Validate(); err != nil {
		return control.Principal{}, fmt.Errorf("stored principal is invalid: %w", err)
	}
	return principal, nil
}

var principalSelect = fmt.Sprintf(`
SELECT controller_id, tree_id, agent_id, parent_agent_id, device_id,
       CASE WHEN length(CAST(capabilities_json AS BLOB)) <= %d
            THEN capabilities_json END,
       created_at
FROM principals
`, maximumCapabilityJSON)
