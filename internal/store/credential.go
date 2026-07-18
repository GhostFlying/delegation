package store

import (
	"context"
	"crypto/subtle"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/identity"
	moderncsqlite "modernc.org/sqlite"
)

const CredentialMACSize = 32

var (
	ErrNotFound           = errors.New("store record not found")
	ErrConflict           = errors.New("store record conflicts with existing state")
	ErrCredentialDisabled = errors.New("credential is disabled")
)

type CredentialMAC [CredentialMACSize]byte

type Credential struct {
	ControllerID string
	DeviceID     string
	Role         control.DeviceRole
	MAC          CredentialMAC
	Disabled     bool
	IssuedAt     int64
}

func (c Credential) Validate() error {
	if err := identity.ValidateID(c.ControllerID); err != nil {
		return fmt.Errorf("controllerId %w", err)
	}
	if err := identity.ValidateID(c.DeviceID); err != nil {
		return fmt.Errorf("deviceId %w", err)
	}
	if err := c.Role.Validate(); err != nil {
		return err
	}
	if c.IssuedAt < 0 {
		return errors.New("issuedAt must not be negative")
	}
	return nil
}

func (s *Store) CreateCredential(ctx context.Context, credential Credential) error {
	if err := credential.Validate(); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, `
INSERT INTO credentials(
    controller_id, device_id, role, token_mac, disabled, issued_at
) VALUES (?, ?, ?, ?, ?, ?)
`,
		credential.ControllerID,
		credential.DeviceID,
		credential.Role,
		credential.MAC[:],
		credential.Disabled,
		credential.IssuedAt,
	)
	if err != nil {
		var sqliteError *moderncsqlite.Error
		if errors.As(err, &sqliteError) && sqliteError.Code()&0xff == 19 {
			return fmt.Errorf("%w: device credential already exists", ErrConflict)
		}
		return fmt.Errorf("create device credential: %w", err)
	}
	return nil
}

func (s *Store) AuthenticateCredential(ctx context.Context, mac CredentialMAC) (Credential, error) {
	var credential Credential
	var storedMAC []byte
	err := s.db.QueryRowContext(ctx, `
SELECT controller_id, device_id, role, token_mac, disabled, issued_at
FROM credentials
WHERE token_mac = ?
`, mac[:]).Scan(
		&credential.ControllerID,
		&credential.DeviceID,
		&credential.Role,
		&storedMAC,
		&credential.Disabled,
		&credential.IssuedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return Credential{}, ErrNotFound
	}
	if err != nil {
		return Credential{}, fmt.Errorf("load device credential: %w", err)
	}
	if len(storedMAC) != CredentialMACSize || subtle.ConstantTimeCompare(storedMAC, mac[:]) != 1 {
		return Credential{}, ErrNotFound
	}
	copy(credential.MAC[:], storedMAC)
	if err := credential.Validate(); err != nil {
		return Credential{}, fmt.Errorf("stored device credential is invalid: %w", err)
	}
	if credential.Disabled {
		return Credential{}, ErrCredentialDisabled
	}
	return credential, nil
}

func (s *Store) DisableCredential(ctx context.Context, controllerID, deviceID string) error {
	if err := identity.ValidateID(controllerID); err != nil {
		return fmt.Errorf("controllerId %w", err)
	}
	if err := identity.ValidateID(deviceID); err != nil {
		return fmt.Errorf("deviceId %w", err)
	}
	result, err := s.db.ExecContext(ctx, `
UPDATE credentials SET disabled = 1
WHERE controller_id = ? AND device_id = ?
`, controllerID, deviceID)
	if err != nil {
		return fmt.Errorf("disable device credential: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect disabled device credential: %w", err)
	}
	if count == 0 {
		return ErrNotFound
	}
	return nil
}

func NewCredential(controllerID, deviceID string, role control.DeviceRole, mac CredentialMAC, now time.Time) Credential {
	return Credential{
		ControllerID: controllerID,
		DeviceID:     deviceID,
		Role:         role,
		MAC:          mac,
		IssuedAt:     now.UTC().Unix(),
	}
}
