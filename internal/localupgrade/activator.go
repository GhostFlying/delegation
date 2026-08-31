package localupgrade

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"

	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/store"
	"github.com/GhostFlying/delegation/internal/userservice"
)

const maximumDefinitionBytes = 1 << 20

type activationStep string

const (
	stepServiceStop      activationStep = "service_stop"
	stepDatabasePrepare  activationStep = "database_prepare"
	stepDefinitionSwitch activationStep = "definition_switch"
	stepDatabaseSwitch   activationStep = "database_switch"
	stepServiceStart     activationStep = "service_start"
	stepQualification    activationStep = "qualification"
)

// ActivationLease proves that the stopped service no longer owns its state
// database. It remains held through the database switch and is released before
// starting the target service.
type ActivationLease interface {
	Close() error
}

// ActivationOperations isolates the crash-consistent state machine from the
// native service manager and qualification implementation.
type ActivationOperations struct {
	StopService      func(context.Context, userservice.UpgradePlan) error
	SwitchDefinition func(context.Context, userservice.UpgradePlan, bool) error
	StartService     func(context.Context, userservice.UpgradePlan, bool) error
	ServiceMatches   func(context.Context, userservice.UpgradePlan, bool) (bool, error)
	AcquireLease     func(Journal) (ActivationLease, error)
	PrepareDatabase  func(context.Context, Database) (Database, error)
	SwitchDatabase   func(Database) (bool, error)
	Qualify          func(context.Context, Journal) error
}

// DefaultActivationOperations returns the native operations used by the
// one-shot activator. Qualification remains an explicit dependency because it
// must validate the role-specific local management endpoint and runtime.
func DefaultActivationOperations(qualify func(context.Context, Journal) error) ActivationOperations {
	return ActivationOperations{
		StopService:      userservice.StopUpgrade,
		SwitchDefinition: userservice.SwitchUpgradeDefinition,
		StartService:     userservice.StartUpgrade,
		ServiceMatches:   userservice.UpgradeServiceMatches,
		AcquireLease:     acquireActivationLease,
		PrepareDatabase: func(ctx context.Context, database Database) (Database, error) {
			return PrepareDatabase(ctx, database, nil)
		},
		SwitchDatabase: ReconcileDatabaseSwitch,
		Qualify:        qualify,
	}
}

type Activator struct {
	store       *Store
	operations  ActivationOperations
	afterAction func(activationStep) error
}

func NewActivator(transactionStore *Store, operations ActivationOperations) (*Activator, error) {
	if transactionStore == nil {
		return nil, errors.New("upgrade transaction store is required")
	}
	if operations.StopService == nil || operations.SwitchDefinition == nil ||
		operations.StartService == nil || operations.ServiceMatches == nil ||
		operations.AcquireLease == nil || operations.PrepareDatabase == nil ||
		operations.SwitchDatabase == nil || operations.Qualify == nil {
		return nil, errors.New("upgrade activation operations are incomplete")
	}
	return &Activator{store: transactionStore, operations: operations}, nil
}

// Run reconciles an authorized transaction to the committed target. Before
// authorization it refuses to perform any service or database mutation.
func (a *Activator) Run(ctx context.Context, transactionID string) (Journal, error) {
	lock, err := acquireJournalLock(filepath.Join(a.store.path, "activation.lock"))
	if err != nil {
		return Journal{}, fmt.Errorf("acquire upgrade activator lock: %w", err)
	}
	defer lock.Close()
	journal, err := a.store.Load()
	if err != nil {
		return Journal{}, err
	}
	if journal.TransactionID != transactionID {
		return Journal{}, errors.New("upgrade transaction ID does not match the active journal")
	}
	switch journal.State {
	case StateCommitted:
		return journal, nil
	case StateRolledBack:
		return journal, errors.New("upgrade transaction was canceled")
	case StateRollbackFailed:
		return journal, errors.New("upgrade rollback requires intervention")
	case StatePrepared, StateArmed, StateRollbackRequired:
		return journal, errors.New("upgrade transaction is not commit-authorized")
	case StateActivating, StateStarted, StateQualified, StateForwardRecoveryRequired:
		if !journal.CommitAuthorized {
			return journal, errors.New("upgrade transaction lacks durable commit authorization")
		}
	default:
		return journal, fmt.Errorf("upgrade transaction cannot activate from %s", journal.State)
	}
	plan, err := servicePlanFromJournal(journal)
	if err != nil {
		return a.failForward(journal, "service_plan_invalid", err)
	}
	if err := validateActivationMaterial(journal); err != nil {
		return a.failForward(journal, "activation_material_changed", err)
	}
	if !journal.Progress.ServiceStopped {
		if err := a.operations.StopService(ctx, plan); err != nil {
			return a.failForward(journal, "service_stop_failed", err)
		}
		if err := a.after(stepServiceStop); err != nil {
			return journal, err
		}
		journal, err = a.updateProgress(journal, func(progress *Progress) { progress.ServiceStopped = true })
		if err != nil {
			return journal, err
		}
	}
	if !journal.Progress.DatabaseSwitched {
		lease, leaseErr := a.operations.AcquireLease(journal)
		if leaseErr != nil {
			return a.failForward(journal, "database_lease_failed", leaseErr)
		}
		journal, err = a.reconcileStoppedDatabase(ctx, journal, plan)
		closeErr := lease.Close()
		if err != nil || closeErr != nil {
			return a.failForward(journal, "database_switch_failed", errors.Join(err, closeErr))
		}
	}
	matched, err := a.operations.ServiceMatches(ctx, plan, true)
	if err != nil {
		return a.failForward(journal, "service_start_failed", err)
	}
	if !matched {
		if err := a.operations.StartService(ctx, plan, true); err != nil {
			return a.failForward(journal, "service_start_failed", err)
		}
		if err := a.after(stepServiceStart); err != nil {
			return journal, err
		}
	}
	if !journal.Progress.ServiceStarted {
		journal, err = a.updateProgress(journal, func(progress *Progress) { progress.ServiceStarted = true })
		if err != nil {
			return journal, err
		}
		if journal.State == StateActivating {
			journal, err = a.updateState(journal, StateStarted)
			if err != nil {
				return journal, err
			}
		}
	}
	if !journal.Progress.Qualified {
		if err := a.operations.Qualify(ctx, journal); err != nil {
			return a.failForward(journal, "qualification_failed", err)
		}
		if err := a.after(stepQualification); err != nil {
			return journal, err
		}
		journal, err = a.updateProgress(journal, func(progress *Progress) { progress.Qualified = true })
		if err != nil {
			return journal, err
		}
		if journal.State == StateStarted {
			journal, err = a.updateState(journal, StateQualified)
			if err != nil {
				return journal, err
			}
		}
	}
	journal, err = a.store.Update(journal.TransactionID, func(current *Journal) error {
		current.State = StateCommitted
		current.FailureCode = ""
		return nil
	})
	return journal, err
}

func (a *Activator) reconcileStoppedDatabase(
	ctx context.Context, journal Journal, plan userservice.UpgradePlan,
) (Journal, error) {
	prepared, err := a.operations.PrepareDatabase(ctx, journal.Database)
	if err != nil {
		return journal, err
	}
	if err := a.after(stepDatabasePrepare); err != nil {
		return journal, err
	}
	if !journal.Progress.DatabasePrepared {
		journal, err = a.store.Update(journal.TransactionID, func(current *Journal) error {
			current.Database.SourceDigest = prepared.SourceDigest
			current.Database.TargetDigest = prepared.TargetDigest
			current.Progress.DatabasePrepared = true
			return nil
		})
		if err != nil {
			return journal, err
		}
	} else if prepared.SourceDigest != journal.Database.SourceDigest ||
		prepared.TargetDigest != journal.Database.TargetDigest {
		return journal, errors.New("prepared database differs from the upgrade journal")
	}
	if err := a.operations.SwitchDefinition(ctx, plan, true); err != nil {
		return journal, err
	}
	if err := a.after(stepDefinitionSwitch); err != nil {
		return journal, err
	}
	if !journal.Progress.DefinitionSwitched {
		journal, err = a.updateProgress(journal, func(progress *Progress) { progress.DefinitionSwitched = true })
		if err != nil {
			return journal, err
		}
	}
	if _, err := a.operations.SwitchDatabase(journal.Database); err != nil {
		return journal, err
	}
	if err := a.after(stepDatabaseSwitch); err != nil {
		return journal, err
	}
	if !journal.Progress.DatabaseSwitched {
		journal, err = a.updateProgress(journal, func(progress *Progress) { progress.DatabaseSwitched = true })
	}
	return journal, err
}

func (a *Activator) after(step activationStep) error {
	if a.afterAction == nil {
		return nil
	}
	return a.afterAction(step)
}

func (a *Activator) updateProgress(journal Journal, mutate func(*Progress)) (Journal, error) {
	return a.store.Update(journal.TransactionID, func(current *Journal) error {
		mutate(&current.Progress)
		return nil
	})
}

func (a *Activator) updateState(journal Journal, state State) (Journal, error) {
	return a.store.Update(journal.TransactionID, func(current *Journal) error {
		current.State = state
		return nil
	})
}

func (a *Activator) failForward(journal Journal, code string, cause error) (Journal, error) {
	if cause == nil {
		cause = errors.New("upgrade activation failed")
	}
	updated, updateErr := a.store.Update(journal.TransactionID, func(current *Journal) error {
		current.State = StateForwardRecoveryRequired
		current.FailureCode = code
		return nil
	})
	return updated, errors.Join(cause, updateErr)
}

func servicePlanFromJournal(journal Journal) (userservice.UpgradePlan, error) {
	oldDefinition, err := delegationconfig.ReadProtectedFile(journal.Definition.OldPath, maximumDefinitionBytes)
	if err != nil {
		return userservice.UpgradePlan{}, fmt.Errorf("read old service definition: %w", err)
	}
	newDefinition, err := delegationconfig.ReadProtectedFile(journal.Definition.NewPath, maximumDefinitionBytes)
	if err != nil {
		return userservice.UpgradePlan{}, fmt.Errorf("read new service definition: %w", err)
	}
	if digestBytes(oldDefinition) != journal.Definition.OldDigest ||
		digestBytes(newDefinition) != journal.Definition.NewDigest {
		return userservice.UpgradePlan{}, errors.New("protected service definition digest changed")
	}
	source := userservice.Invocation{
		BinaryPath: journal.Invocation.BinaryPath, ConfigPath: journal.Invocation.ConfigPath,
		EnvironmentFile: journal.Invocation.EnvironmentFile, InstanceID: journal.InstanceID,
	}
	target := source
	target.BinaryPath = journal.Invocation.TargetBinaryPath
	role := userservice.ServiceRole(journal.Role)
	return userservice.UpgradePlan{
		Role: role, Kind: journal.Definition.Kind, NativeName: journal.Invocation.NativeName,
		Artifact: journal.Invocation.DefinitionPath, UserIdentity: journal.Invocation.UserIdentity,
		SourceInvocation: source, TargetInvocation: target, OldDefinition: oldDefinition,
		NewDefinition: newDefinition, ProcessIDs: append([]int(nil), journal.Invocation.ProcessIDs...),
		ProcessGroup: journal.Invocation.ProcessGroup,
	}, nil
}

func acquireActivationLease(journal Journal) (ActivationLease, error) {
	switch journal.Role {
	case delegationconfig.RoleBroker:
		return store.AcquireBrokerLease(journal.Database.CanonicalPath)
	case delegationconfig.RolePeer:
		return store.AcquirePeerLease(journal.Database.CanonicalPath)
	default:
		return nil, fmt.Errorf("unsupported upgrade role %q", journal.Role)
	}
}

func digestBytes(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

// Cancel marks an untouched pre-COMMIT transaction rolled back. Activation
// states cannot be canceled because authorization is irreversible.
func Cancel(transactionStore *Store, transactionID string) (Journal, error) {
	journal, err := transactionStore.Load()
	if err != nil {
		return Journal{}, err
	}
	if journal.TransactionID != transactionID {
		return Journal{}, errors.New("upgrade transaction ID does not match the active journal")
	}
	switch journal.State {
	case StateRolledBack:
		return journal, nil
	case StatePrepared:
		return transactionStore.Update(transactionID, func(current *Journal) error {
			current.State = StateRolledBack
			current.FailureCode = ""
			return nil
		})
	case StateArmed, StateRollbackRequired:
		if journal.State == StateArmed {
			journal, err = transactionStore.Update(transactionID, func(current *Journal) error {
				current.State = StateRollbackRequired
				return nil
			})
			if err != nil {
				return journal, err
			}
		}
		return transactionStore.Update(transactionID, func(current *Journal) error {
			current.State = StateRolledBack
			current.FailureCode = ""
			return nil
		})
	default:
		return journal, errors.New("upgrade transaction can no longer be canceled")
	}
}
