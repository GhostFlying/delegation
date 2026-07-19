package store

import (
	"context"
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/identity"
)

const (
	maximumFeaturesJSON = 8 * 1024
)

var (
	ErrAuthorizationDenied = errors.New("store authorization denied")
	ErrRevisionExhausted   = errors.New("device registry revision exhausted")
	ErrStaleRevision       = errors.New("stale device revision")
)

type PresenceTransition struct {
	Revision uint64
	Count    int64
}

func (s *Store) RegisterAuthenticatedDevice(
	ctx context.Context,
	mac CredentialMAC,
	descriptor control.DeviceDescriptor,
	observedAt time.Time,
) (control.Device, error) {
	return s.registerDevice(ctx, &mac, descriptor, observedAt)
}

func (s *Store) RegisterTrustedDevice(
	ctx context.Context,
	descriptor control.DeviceDescriptor,
	observedAt time.Time,
) (control.Device, error) {
	return s.registerDevice(ctx, nil, descriptor, observedAt)
}

func (s *Store) registerDevice(
	ctx context.Context,
	mac *CredentialMAC,
	descriptor control.DeviceDescriptor,
	observedAt time.Time,
) (control.Device, error) {
	descriptor.Features = slices.Clone(descriptor.Features)
	if descriptor.Features == nil {
		descriptor.Features = []string{}
	}
	if err := descriptor.Validate(); err != nil {
		return control.Device{}, err
	}
	lastSeenAt, err := unixTime(observedAt, "lastSeenAt")
	if err != nil {
		return control.Device{}, err
	}
	featuresJSON, err := json.Marshal(descriptor.Features)
	if err != nil {
		return control.Device{}, fmt.Errorf("encode device features: %w", err)
	}
	var registered control.Device
	err = s.withImmediateTransaction(ctx, func(connection *sql.Conn) error {
		if mac != nil {
			credential, err := queryCredentialByMAC(ctx, connection, *mac)
			if err != nil {
				return err
			}
			if credential.Disabled || credential.Pending {
				return ErrCredentialDisabled
			}
			if credential.ControllerID != descriptor.ControllerID ||
				credential.DeviceID != descriptor.DeviceID ||
				credential.Role != descriptor.Role ||
				subtle.ConstantTimeCompare(credential.MAC[:], mac[:]) != 1 {
				return ErrAuthorizationDenied
			}
		}
		revision, err := nextControllerRevision(ctx, connection, descriptor.ControllerID)
		if err != nil {
			return err
		}
		registered = deviceFromDescriptor(descriptor, lastSeenAt, revision)
		if _, err := connection.ExecContext(ctx, `
INSERT INTO devices(
    controller_id, device_id, name, role, os, arch, runtime_version,
    protocol_version, features_json, online, last_seen_at, revision
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
ON CONFLICT(controller_id, device_id) DO UPDATE SET
    name = excluded.name,
    role = excluded.role,
    os = excluded.os,
    arch = excluded.arch,
    runtime_version = excluded.runtime_version,
    protocol_version = excluded.protocol_version,
    features_json = excluded.features_json,
    online = 1,
    last_seen_at = excluded.last_seen_at,
    revision = excluded.revision
`,
			registered.ControllerID,
			registered.DeviceID,
			registered.Name,
			registered.Role,
			registered.OS,
			registered.Arch,
			registered.RuntimeVersion,
			registered.ProtocolVersion,
			string(featuresJSON),
			registered.LastSeenAt,
			registered.Revision,
		); err != nil {
			return fmt.Errorf("register device: %w", err)
		}
		return nil
	})
	return registered, err
}

func (s *Store) HeartbeatDevice(
	ctx context.Context,
	controllerID, deviceID string,
	expectedRevision uint64,
	observedAt time.Time,
) (control.Device, error) {
	return s.updateDevicePresence(ctx, controllerID, deviceID, expectedRevision, observedAt, false)
}

func (s *Store) MarkDeviceOffline(
	ctx context.Context,
	controllerID, deviceID string,
	expectedRevision uint64,
	observedAt time.Time,
) (control.Device, error) {
	return s.updateDevicePresence(ctx, controllerID, deviceID, expectedRevision, observedAt, true)
}

func (s *Store) updateDevicePresence(
	ctx context.Context,
	controllerID, deviceID string,
	expectedRevision uint64,
	observedAt time.Time,
	markOffline bool,
) (control.Device, error) {
	if err := validateDeviceIdentity(controllerID, deviceID); err != nil {
		return control.Device{}, err
	}
	if expectedRevision == 0 {
		return control.Device{}, errors.New("expectedRevision must be positive")
	}
	lastSeenAt, err := unixTime(observedAt, "lastSeenAt")
	if err != nil {
		return control.Device{}, err
	}
	var updated control.Device
	err = s.withImmediateTransaction(ctx, func(connection *sql.Conn) error {
		device, err := queryDevice(ctx, connection, controllerID, deviceID)
		if err != nil {
			return err
		}
		if device.Revision != expectedRevision {
			return ErrStaleRevision
		}
		if !device.Online {
			if markOffline {
				updated = device
				return nil
			}
			return fmt.Errorf("%w: device is offline", ErrConflict)
		}
		if !markOffline && lastSeenAt <= device.LastSeenAt {
			updated = device
			return nil
		}
		if !markOffline {
			device.LastSeenAt = lastSeenAt
			if _, err := connection.ExecContext(ctx, `
UPDATE devices SET last_seen_at = ?
WHERE controller_id = ? AND device_id = ?
`, device.LastSeenAt, controllerID, deviceID); err != nil {
				return fmt.Errorf("update device heartbeat: %w", err)
			}
			updated = device
			return nil
		}
		if lastSeenAt > device.LastSeenAt {
			device.LastSeenAt = lastSeenAt
		}
		device.Revision, err = nextControllerRevision(ctx, connection, controllerID)
		if err != nil {
			return err
		}
		device.Online = false
		if _, err := connection.ExecContext(ctx, `
UPDATE devices SET online = ?, last_seen_at = ?, revision = ?
WHERE controller_id = ? AND device_id = ?
`, device.Online, device.LastSeenAt, device.Revision, controllerID, deviceID); err != nil {
			return fmt.Errorf("update device presence: %w", err)
		}
		updated = device
		return nil
	})
	return updated, err
}

func (s *Store) BeginBrokerEpoch(ctx context.Context, controllerID string) (PresenceTransition, error) {
	return s.markMatchingDevicesOffline(ctx, controllerID, "online = 1", nil)
}

func (s *Store) ExpireDevices(
	ctx context.Context,
	controllerID string,
	lastSeenCutoff time.Time,
) (PresenceTransition, error) {
	cutoff, err := unixTime(lastSeenCutoff, "lastSeenCutoff")
	if err != nil {
		return PresenceTransition{}, err
	}
	return s.markMatchingDevicesOffline(ctx, controllerID, "online = 1 AND last_seen_at <= ?", []any{cutoff})
}

func (s *Store) markMatchingDevicesOffline(
	ctx context.Context,
	controllerID, predicate string,
	arguments []any,
) (PresenceTransition, error) {
	if err := identity.ValidateID(controllerID); err != nil {
		return PresenceTransition{}, fmt.Errorf("controllerId %w", err)
	}
	var transition PresenceTransition
	err := s.withImmediateTransaction(ctx, func(connection *sql.Conn) error {
		queryArguments := append([]any{controllerID}, arguments...)
		if err := connection.QueryRowContext(ctx, `
SELECT count(*) FROM devices WHERE controller_id = ? AND `+predicate,
			queryArguments...,
		).Scan(&transition.Count); err != nil {
			return fmt.Errorf("count devices for offline transition: %w", err)
		}
		if transition.Count == 0 {
			revision, err := controllerRevision(ctx, connection, controllerID)
			transition.Revision = revision
			return err
		}
		revision, err := nextControllerRevision(ctx, connection, controllerID)
		if err != nil {
			return err
		}
		transition.Revision = revision
		updateArguments := append([]any{transition.Revision, controllerID}, arguments...)
		if _, err := connection.ExecContext(ctx, `
UPDATE devices SET online = 0, revision = ?
WHERE controller_id = ? AND `+predicate,
			updateArguments...,
		); err != nil {
			return fmt.Errorf("mark matching devices offline: %w", err)
		}
		return nil
	})
	return transition, err
}

var deviceSelect = fmt.Sprintf(`
SELECT controller_id, device_id, name, role, os, arch, runtime_version,
       protocol_version,
       CASE WHEN length(CAST(features_json AS BLOB)) <= %d THEN features_json END,
       online, last_seen_at, revision
FROM devices
`, maximumFeaturesJSON)

func queryDevice(
	ctx context.Context,
	queryer rowQueryer,
	controllerID, deviceID string,
) (control.Device, error) {
	return scanDevice(queryer.QueryRowContext(ctx, deviceSelect+`
WHERE controller_id = ? AND device_id = ?
`, controllerID, deviceID))
}

func scanDevice(scanner rowScanner) (control.Device, error) {
	var device control.Device
	var featuresJSON sql.NullString
	err := scanner.Scan(
		&device.ControllerID,
		&device.DeviceID,
		&device.Name,
		&device.Role,
		&device.OS,
		&device.Arch,
		&device.RuntimeVersion,
		&device.ProtocolVersion,
		&featuresJSON,
		&device.Online,
		&device.LastSeenAt,
		&device.Revision,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return control.Device{}, ErrNotFound
	}
	if err != nil {
		return control.Device{}, fmt.Errorf("load device: %w", err)
	}
	if !featuresJSON.Valid {
		return control.Device{}, errors.New("stored device features exceed size limit")
	}
	if err := json.Unmarshal([]byte(featuresJSON.String), &device.Features); err != nil {
		return control.Device{}, fmt.Errorf("decode stored device features: %w", err)
	}
	if err := device.Validate(); err != nil {
		return control.Device{}, fmt.Errorf("stored device is invalid: %w", err)
	}
	return device, nil
}

func nextControllerRevision(ctx context.Context, connection *sql.Conn, controllerID string) (uint64, error) {
	if _, err := connection.ExecContext(ctx, `
INSERT INTO controller_registries(controller_id, revision)
VALUES (?, 0)
ON CONFLICT(controller_id) DO NOTHING
`, controllerID); err != nil {
		return 0, fmt.Errorf("initialize controller revision: %w", err)
	}
	var current int64
	if err := connection.QueryRowContext(ctx, `
SELECT revision FROM controller_registries WHERE controller_id = ?
`, controllerID).Scan(&current); err != nil {
		return 0, fmt.Errorf("load controller revision: %w", err)
	}
	if current < 0 {
		return 0, errors.New("stored controller revision is negative")
	}
	if current == math.MaxInt64 {
		return 0, ErrRevisionExhausted
	}
	next := current + 1
	result, err := connection.ExecContext(ctx, `
UPDATE controller_registries SET revision = ?
WHERE controller_id = ? AND revision = ?
`, next, controllerID, current)
	if err != nil {
		return 0, fmt.Errorf("advance controller revision: %w", err)
	}
	if err := requireAffectedRow(result, "advance controller revision"); err != nil {
		return 0, err
	}
	return uint64(next), nil
}

func controllerRevision(ctx context.Context, queryer rowQueryer, controllerID string) (uint64, error) {
	var revision int64
	err := queryer.QueryRowContext(ctx, `
SELECT revision FROM controller_registries WHERE controller_id = ?
`, controllerID).Scan(&revision)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("load controller revision: %w", err)
	}
	if revision < 0 {
		return 0, errors.New("stored controller revision is negative")
	}
	return uint64(revision), nil
}

func validateDeviceIdentity(controllerID, deviceID string) error {
	if err := identity.ValidateID(controllerID); err != nil {
		return fmt.Errorf("controllerId %w", err)
	}
	if err := identity.ValidateID(deviceID); err != nil {
		return fmt.Errorf("deviceId %w", err)
	}
	return nil
}

func unixTime(value time.Time, name string) (int64, error) {
	seconds := value.UTC().Unix()
	if seconds < 0 {
		return 0, fmt.Errorf("%s must not be negative", name)
	}
	return seconds, nil
}

func deviceFromDescriptor(descriptor control.DeviceDescriptor, lastSeenAt int64, revision uint64) control.Device {
	return control.Device{
		ControllerID:    descriptor.ControllerID,
		DeviceID:        descriptor.DeviceID,
		Name:            descriptor.Name,
		Role:            descriptor.Role,
		OS:              descriptor.OS,
		Arch:            descriptor.Arch,
		RuntimeVersion:  descriptor.RuntimeVersion,
		ProtocolVersion: descriptor.ProtocolVersion,
		Features:        slices.Clone(descriptor.Features),
		Online:          true,
		LastSeenAt:      lastSeenAt,
		Revision:        revision,
	}
}
