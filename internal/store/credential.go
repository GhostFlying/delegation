package store

import (
	"context"
	"crypto/subtle"
	"database/sql"
	"errors"
	"fmt"
	"time"

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
	MAC          CredentialMAC
	Disabled     bool
	Pending      bool
	IssuedAt     int64
}

type rowScanner interface {
	Scan(...any) error
}

func (c Credential) Validate() error {
	if err := identity.ValidateID(c.ControllerID); err != nil {
		return fmt.Errorf("controllerId %w", err)
	}
	if err := identity.ValidateID(c.DeviceID); err != nil {
		return fmt.Errorf("deviceId %w", err)
	}
	if c.IssuedAt < 0 {
		return errors.New("issuedAt must not be negative")
	}
	if c.Pending && !c.Disabled {
		return errors.New("pending credentials must be disabled")
	}
	return nil
}

func (s *Store) CreateCredential(ctx context.Context, credential Credential) error {
	if err := credential.Validate(); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, `
INSERT INTO credentials(
    controller_id, device_id, token_mac, disabled, issued_at, pending
) VALUES (?, ?, ?, ?, ?, ?)
`,
		credential.ControllerID,
		credential.DeviceID,
		credential.MAC[:],
		credential.Disabled,
		credential.IssuedAt,
		credential.Pending,
	)
	if err != nil {
		var sqliteError *moderncsqlite.Error
		if errors.As(err, &sqliteError) && sqliteError.Code()&0xff == 19 {
			return fmt.Errorf("%w: peer credential already exists", ErrConflict)
		}
		return fmt.Errorf("create peer credential: %w", err)
	}
	return nil
}

func (s *Store) AuthenticateCredential(ctx context.Context, mac CredentialMAC) (Credential, error) {
	credential, err := queryCredentialByMAC(ctx, s.db, mac)
	if err != nil {
		return Credential{}, err
	}
	if credential.Disabled || credential.Pending {
		return Credential{}, ErrCredentialDisabled
	}
	return credential, nil
}

func queryCredentialByMAC(ctx context.Context, queryer rowQueryer, mac CredentialMAC) (Credential, error) {
	credential, err := scanCredential(queryer.QueryRowContext(ctx, `
SELECT controller_id, device_id, token_mac, disabled, issued_at, pending
FROM credentials
WHERE token_mac = ?
`, mac[:]))
	if err != nil {
		return Credential{}, err
	}
	if subtle.ConstantTimeCompare(credential.MAC[:], mac[:]) != 1 {
		return Credential{}, ErrNotFound
	}
	return credential, nil
}

func (s *Store) Credential(ctx context.Context, controllerID, deviceID string) (Credential, error) {
	if err := validateCredentialIdentity(controllerID, deviceID); err != nil {
		return Credential{}, err
	}
	return queryCredential(ctx, s.db, controllerID, deviceID)
}

func queryCredential(ctx context.Context, queryer rowQueryer, controllerID, deviceID string) (Credential, error) {
	return scanCredential(queryer.QueryRowContext(ctx, `
SELECT controller_id, device_id, token_mac, disabled, issued_at, pending
FROM credentials
WHERE controller_id = ? AND device_id = ?
`, controllerID, deviceID))
}

func (s *Store) ActivateCredential(ctx context.Context, controllerID, deviceID string, mac CredentialMAC) error {
	if err := validateCredentialIdentity(controllerID, deviceID); err != nil {
		return err
	}
	result, err := s.db.ExecContext(ctx, `
UPDATE credentials SET disabled = 0, pending = 0
WHERE controller_id = ? AND device_id = ? AND token_mac = ? AND pending = 1
`, controllerID, deviceID, mac[:])
	if err != nil {
		return fmt.Errorf("activate peer credential: %w", err)
	}
	if err := requireAffectedRow(result, "activate peer credential"); err == nil {
		return nil
	} else if !errors.Is(err, ErrNotFound) {
		return err
	}
	credential, err := s.Credential(ctx, controllerID, deviceID)
	if err != nil {
		return err
	}
	if !credential.Disabled && !credential.Pending && subtle.ConstantTimeCompare(credential.MAC[:], mac[:]) == 1 {
		return nil
	}
	return ErrNotFound
}

// PublishPendingCredential fences a token publication against replacement of
// the pending enrollment. The callback must not call Store methods because it
// runs while an immediate write transaction is held.
func (s *Store) PublishPendingCredential(
	ctx context.Context,
	controllerID, deviceID string,
	mac CredentialMAC,
	publish func() (bool, error),
) (bool, error) {
	if err := validateCredentialIdentity(controllerID, deviceID); err != nil {
		return false, err
	}
	if publish == nil {
		return false, errors.New("credential publisher is required")
	}
	committed := false
	err := s.withImmediateTransaction(ctx, func(connection *sql.Conn) error {
		credential, err := queryCredential(ctx, connection, controllerID, deviceID)
		if err != nil {
			return err
		}
		if !credential.Disabled || !credential.Pending ||
			subtle.ConstantTimeCompare(credential.MAC[:], mac[:]) != 1 {
			return ErrNotFound
		}
		committed, err = publish()
		if err != nil {
			return err
		}
		result, err := connection.ExecContext(ctx, `
UPDATE credentials SET disabled = 0, pending = 0
WHERE controller_id = ? AND device_id = ? AND token_mac = ? AND disabled = 1 AND pending = 1
`, controllerID, deviceID, mac[:])
		if err != nil {
			return fmt.Errorf("activate published peer credential: %w", err)
		}
		return requireAffectedRow(result, "activate published peer credential")
	})
	return committed, err
}

func (s *Store) DeletePendingCredential(ctx context.Context, controllerID, deviceID string, mac CredentialMAC) error {
	if err := validateCredentialIdentity(controllerID, deviceID); err != nil {
		return err
	}
	result, err := s.db.ExecContext(ctx, `
DELETE FROM credentials
WHERE controller_id = ? AND device_id = ? AND token_mac = ? AND disabled = 1 AND pending = 1
`, controllerID, deviceID, mac[:])
	if err != nil {
		return fmt.Errorf("delete pending peer credential: %w", err)
	}
	return requireAffectedRow(result, "delete pending peer credential")
}

func (s *Store) DisableCredential(ctx context.Context, controllerID, deviceID string) error {
	if err := validateCredentialIdentity(controllerID, deviceID); err != nil {
		return err
	}
	return s.withImmediateTransaction(ctx, func(connection *sql.Conn) error {
		if _, err := queryCredential(ctx, connection, controllerID, deviceID); err != nil {
			return err
		}
		if _, err := connection.ExecContext(ctx, `
UPDATE credentials SET disabled = 1, pending = 0
WHERE controller_id = ? AND device_id = ?
`, controllerID, deviceID); err != nil {
			return fmt.Errorf("disable peer credential: %w", err)
		}
		var online bool
		err := connection.QueryRowContext(ctx, `
SELECT online FROM devices WHERE controller_id = ? AND device_id = ?
`, controllerID, deviceID).Scan(&online)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("load revoked device presence: %w", err)
		}
		if !online {
			return nil
		}
		revision, err := nextControllerRevision(ctx, connection, controllerID)
		if err != nil {
			return err
		}
		if _, err := connection.ExecContext(ctx, `
UPDATE devices SET online = 0, revision = ?
WHERE controller_id = ? AND device_id = ?
`, revision, controllerID, deviceID); err != nil {
			return fmt.Errorf("mark revoked device offline: %w", err)
		}
		return nil
	})
}

type rowQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func scanCredential(row rowScanner) (Credential, error) {
	var credential Credential
	var storedMAC []byte
	err := row.Scan(
		&credential.ControllerID,
		&credential.DeviceID,
		&storedMAC,
		&credential.Disabled,
		&credential.IssuedAt,
		&credential.Pending,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return Credential{}, ErrNotFound
	}
	if err != nil {
		return Credential{}, fmt.Errorf("load peer credential: %w", err)
	}
	if len(storedMAC) != CredentialMACSize {
		return Credential{}, errors.New("stored peer credential MAC has invalid length")
	}
	copy(credential.MAC[:], storedMAC)
	if err := credential.Validate(); err != nil {
		return Credential{}, fmt.Errorf("stored peer credential is invalid: %w", err)
	}
	return credential, nil
}

func validateCredentialIdentity(controllerID, deviceID string) error {
	if err := identity.ValidateID(controllerID); err != nil {
		return fmt.Errorf("controllerId %w", err)
	}
	if err := identity.ValidateID(deviceID); err != nil {
		return fmt.Errorf("deviceId %w", err)
	}
	return nil
}

func requireAffectedRow(result sql.Result, action string) error {
	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect %s: %w", action, err)
	}
	if count == 0 {
		return ErrNotFound
	}
	return nil
}

func NewCredential(controllerID, deviceID string, mac CredentialMAC, now time.Time) Credential {
	return Credential{
		ControllerID: controllerID,
		DeviceID:     deviceID,
		MAC:          mac,
		IssuedAt:     now.UTC().Unix(),
	}
}
