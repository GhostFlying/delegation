package localupgrade

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
)

// OneShotOperations owns the native one-shot activator definition. Install and
// Remove are idempotent and fail closed when an existing native definition is
// not the exact definition derived from the protected journal. Launch returns
// only after the native manager has durably accepted the activation request.
type OneShotOperations struct {
	Install func(context.Context, Journal, string) error
	Launch  func(context.Context, Journal, string) error
	Remove  func(context.Context, Journal, string) error
}

// DefaultOneShotOperations returns the platform-native activator operations.
func DefaultOneShotOperations() OneShotOperations {
	return OneShotOperations{
		Install: platformInstallOneShot,
		Launch:  platformLaunchOneShot,
		Remove:  platformRemoveOneShot,
	}
}

// Manager owns the protected local management transitions around Activator.
// Controller-wide coordination and its COMMIT decision remain outside this
// type; AuthorizeAndLaunch consumes that decision after it has been made.
type Manager struct {
	store      *Store
	operations OneShotOperations
}

func NewManager(transactionStore *Store, operations OneShotOperations) (*Manager, error) {
	if transactionStore == nil {
		return nil, errors.New("upgrade transaction store is required")
	}
	if operations.Install == nil || operations.Launch == nil || operations.Remove == nil {
		return nil, errors.New("upgrade one-shot operations are incomplete")
	}
	return &Manager{store: transactionStore, operations: operations}, nil
}

// Arm persists and registers the exact one-shot definition before recording
// the armed state. A crash between those actions is reconciled by repeating the
// idempotent installation while the journal remains prepared.
func (m *Manager) Arm(ctx context.Context, transactionID string) (Journal, error) {
	lock, err := acquireJournalLock(filepath.Join(m.store.path, "management.lock"))
	if err != nil {
		return Journal{}, fmt.Errorf("acquire upgrade management lock: %w", err)
	}
	defer lock.Close()
	j, err := m.loadTransaction(transactionID)
	if err != nil {
		return Journal{}, err
	}
	switch j.State {
	case StatePrepared, StateArmed:
		if err := m.operations.Install(ctx, j, m.store.path); err != nil {
			return j, fmt.Errorf("install upgrade activator: %w", err)
		}
		if j.State == StateArmed {
			return j, nil
		}
		return m.store.Update(transactionID, func(current *Journal) error {
			current.State = StateArmed
			return nil
		})
	case StateActivating, StateStarted, StateQualified, StateForwardRecoveryRequired, StateCommitted:
		if !j.CommitAuthorized {
			return j, errors.New("upgrade activation state lacks durable authorization")
		}
		return j, nil
	default:
		return j, fmt.Errorf("upgrade transaction cannot be armed from %s", j.State)
	}
}

// AuthorizeAndLaunch makes the irreversible local COMMIT decision durable
// before asking the native service manager to start the independent activator.
// Once this method crosses that boundary, every retry is forward-only.
func (m *Manager) AuthorizeAndLaunch(ctx context.Context, transactionID string) (Journal, error) {
	lock, err := acquireJournalLock(filepath.Join(m.store.path, "management.lock"))
	if err != nil {
		return Journal{}, fmt.Errorf("acquire upgrade management lock: %w", err)
	}
	defer lock.Close()
	j, err := m.loadTransaction(transactionID)
	if err != nil {
		return Journal{}, err
	}
	switch j.State {
	case StateArmed:
		if err := validateActivationMaterial(j); err != nil {
			return j, err
		}
		j, err = m.store.Update(transactionID, func(current *Journal) error {
			current.State = StateActivating
			current.CommitAuthorized = true
			return nil
		})
		if err != nil {
			return j, err
		}
	case StateActivating, StateStarted, StateQualified, StateForwardRecoveryRequired:
		if !j.CommitAuthorized {
			return j, errors.New("upgrade activation state lacks durable authorization")
		}
	case StateCommitted:
		return j, nil
	default:
		return j, fmt.Errorf("upgrade transaction cannot activate from %s", j.State)
	}
	if err := m.operations.Launch(ctx, j, m.store.path); err != nil {
		return j, fmt.Errorf("launch upgrade activator after durable authorization: %w", err)
	}
	return j, nil
}

// Cancel removes any prepared one-shot definition before recording the
// reversible transaction as rolled back. Authorized transactions are never
// passed to the platform cleanup path.
func (m *Manager) Cancel(ctx context.Context, transactionID string) (Journal, error) {
	lock, err := acquireJournalLock(filepath.Join(m.store.path, "management.lock"))
	if err != nil {
		return Journal{}, fmt.Errorf("acquire upgrade management lock: %w", err)
	}
	defer lock.Close()
	j, err := m.loadTransaction(transactionID)
	if err != nil {
		return Journal{}, err
	}
	if j.CommitAuthorized {
		return j, errors.New("upgrade transaction can no longer be canceled")
	}
	switch j.State {
	case StatePrepared, StateArmed, StateRollbackRequired, StateRolledBack:
		if err := m.operations.Remove(ctx, j, m.store.path); err != nil {
			return j, fmt.Errorf("remove upgrade activator before cancellation: %w", err)
		}
		if j.State == StateRolledBack {
			return j, nil
		}
		return Cancel(m.store, transactionID)
	default:
		return j, errors.New("upgrade transaction can no longer be canceled")
	}
}

// Cleanup removes the one-shot definition after an activator has committed.
// It is safe for the hidden activator entrypoint and later status reconciliation
// to repeat this operation after response loss or process termination.
func (m *Manager) Cleanup(ctx context.Context, transactionID string) (Journal, error) {
	j, err := m.loadTransaction(transactionID)
	if err != nil {
		return Journal{}, err
	}
	if j.State != StateCommitted {
		return j, fmt.Errorf("upgrade activator cannot be removed from %s", j.State)
	}
	if err := m.operations.Remove(ctx, j, m.store.path); err != nil {
		return j, fmt.Errorf("remove committed upgrade activator: %w", err)
	}
	return j, nil
}

func (m *Manager) Snapshot() (*Snapshot, error) {
	j, err := m.store.Load()
	if err != nil {
		return nil, err
	}
	snapshot := j.Snapshot()
	return &snapshot, nil
}

func (m *Manager) loadTransaction(transactionID string) (Journal, error) {
	j, err := m.store.Load()
	if err != nil {
		return Journal{}, err
	}
	if j.TransactionID != transactionID {
		return Journal{}, errors.New("upgrade transaction ID does not match the active journal")
	}
	return j, nil
}
