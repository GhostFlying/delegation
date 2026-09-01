package coordinatedupgrade

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/GhostFlying/delegation/internal/broker"
	"github.com/GhostFlying/delegation/internal/identity"
	"github.com/GhostFlying/delegation/internal/localupgrade"
	"github.com/GhostFlying/delegation/internal/protocol"
)

const (
	defaultCompletionTimeout = 10 * time.Minute
	retryInterval            = time.Second
)

type BrokerControl interface {
	BeginUpgradeDrain()
	EndUpgradeDrain()
	UpgradeDraining() bool
	RequireRuntimeVersion(string)
	FreezeUpgradePeers() []broker.UpgradePeer
	CallPinnedUpgrade(context.Context, broker.UpgradePeer, string, any) (protocol.UpgradeSnapshot, error)
	CallCurrentUpgrade(context.Context, string, string, any) (protocol.UpgradeSnapshot, error)
	UpgradePeerState(string) broker.UpgradePeerState
	SetUpgradeIntervention(string, bool)
}

type LocalManager interface {
	PrepareCoordinatedUpgrade(context.Context, protocol.PrepareUpgradeParams) (protocol.UpgradeSnapshot, error)
	ArmCoordinatedUpgrade(context.Context, protocol.UpgradeTransactionParams) (protocol.UpgradeSnapshot, error)
	ActivateCoordinatedUpgrade(context.Context, protocol.UpgradeTransactionParams) (protocol.UpgradeSnapshot, error)
	CancelCoordinatedUpgrade(context.Context, protocol.UpgradeTransactionParams) (protocol.UpgradeSnapshot, error)
	CoordinatedUpgradeStatus(context.Context, protocol.UpgradeTransactionParams) (protocol.UpgradeSnapshot, error)
}

type Options struct {
	Store             *Store
	Broker            BrokerControl
	Local             LocalManager
	SourceVersion     string
	ControllerID      string
	InstanceID        string
	NewID             func() (string, error)
	Now               func() time.Time
	CompletionTimeout time.Duration
	ReportError       func(error)
}

type Manager struct {
	store             *Store
	broker            BrokerControl
	local             LocalManager
	sourceVersion     string
	controllerID      string
	instanceID        string
	newID             func() (string, error)
	now               func() time.Time
	completionTimeout time.Duration
	reportError       func(error)
	mu                sync.Mutex
}

func NewManager(options Options) (*Manager, error) {
	if options.Store == nil || options.Broker == nil || options.Local == nil {
		return nil, errors.New("coordinated upgrade dependencies are required")
	}
	if err := identity.ValidateID(options.ControllerID); err != nil {
		return nil, fmt.Errorf("coordinated upgrade controller ID: %w", err)
	}
	if options.InstanceID == "" {
		return nil, errors.New("coordinated upgrade instance ID is required")
	}
	if options.NewID == nil {
		options.NewID = identity.NewID
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.CompletionTimeout == 0 {
		options.CompletionTimeout = defaultCompletionTimeout
	}
	if options.CompletionTimeout <= 0 {
		return nil, errors.New("coordinated upgrade completion timeout must be positive")
	}
	if options.ReportError == nil {
		options.ReportError = func(error) {}
	}
	return &Manager{
		store: options.Store, broker: options.Broker, local: options.Local,
		sourceVersion: options.SourceVersion, controllerID: options.ControllerID,
		instanceID: options.InstanceID, newID: options.NewID, now: options.Now,
		completionTimeout: options.CompletionTimeout, reportError: options.ReportError,
	}, nil
}

func (m *Manager) Start(ctx context.Context, targetVersion string, timeout time.Duration) (Journal, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if timeout <= 0 {
		return Journal{}, errors.New("coordinated upgrade timeout must be positive")
	}
	m.broker.BeginUpgradeDrain()
	existing, err := m.store.Load()
	if err == nil {
		if err := m.validateIdentity(existing); err != nil {
			return existing, err
		}
		if existing.TargetVersion != targetVersion && !existing.Terminal() {
			return existing, ErrTargetConflict
		}
		if existing.TargetVersion == targetVersion {
			if existing.Terminal() {
				m.reconcileTerminalDrain(existing)
				return existing, nil
			}
			return m.advance(ctx, existing)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return Journal{}, err
	}
	transactionID, err := m.newID()
	if err != nil {
		m.broker.EndUpgradeDrain()
		return Journal{}, err
	}
	now := m.now().UnixMilli()
	peers := m.broker.FreezeUpgradePeers()
	participants := make([]Participant, len(peers))
	for index, peer := range peers {
		localTransactionID, idErr := m.newID()
		if idErr != nil {
			m.broker.EndUpgradeDrain()
			return Journal{}, fmt.Errorf("create peer local upgrade transaction ID: %w", idErr)
		}
		participants[index] = Participant{
			DeviceID: peer.DeviceID, ConnectionID: peer.ConnectionID,
			SourceVersion: peer.RuntimeVersion, LocalTransactionID: localTransactionID,
			State: ParticipantPending, UpdatedAt: now,
		}
	}
	brokerTransactionID, err := m.newID()
	if err != nil {
		m.broker.EndUpgradeDrain()
		return Journal{}, fmt.Errorf("create broker local upgrade transaction ID: %w", err)
	}
	journal, _, err := m.store.CreateOrResume(Journal{
		SchemaVersion: JournalSchemaVersion, TransactionID: transactionID, State: StatePreparing,
		ControllerID: m.controllerID, InstanceID: m.instanceID,
		SourceVersion: m.sourceVersion, TargetVersion: targetVersion, Deadline: now + timeout.Milliseconds(),
		Participants: participants, Broker: LocalParticipant{TransactionID: brokerTransactionID, UpdatedAt: now},
		CreatedAt: now, UpdatedAt: now,
	})
	if err != nil {
		m.broker.EndUpgradeDrain()
		return Journal{}, err
	}
	return m.advance(ctx, journal)
}

func (m *Manager) Resume(ctx context.Context) (Journal, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	journal, err := m.store.Load()
	if err != nil {
		return Journal{}, err
	}
	if err := m.validateIdentity(journal); err != nil {
		return journal, err
	}
	if journal.Terminal() {
		m.reconcileTerminalDrain(journal)
		return journal, nil
	}
	m.broker.BeginUpgradeDrain()
	return m.advance(ctx, journal)
}

func (m *Manager) Cancel(ctx context.Context, transactionID string) (Journal, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	journal, err := m.store.Load()
	if err != nil {
		return Journal{}, err
	}
	if err := m.validateIdentity(journal); err != nil {
		return journal, err
	}
	if journal.TransactionID != transactionID {
		return journal, errors.New("coordinated upgrade transaction ID does not match")
	}
	if journal.State == StateCanceled {
		m.reconcileTerminalDrain(journal)
		return journal, nil
	}
	if journal.GlobalCommit {
		return journal, errors.New("coordinated upgrade cannot be canceled after global COMMIT")
	}
	journal, err = m.beginCancel(journal, "operator_canceled")
	if err != nil {
		return journal, err
	}
	return m.cancelPrepared(ctx, journal)
}

func (m *Manager) Status() (*Journal, error) {
	journal, err := m.store.Load()
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if err := m.validateIdentity(journal); err != nil {
		return nil, err
	}
	return &journal, nil
}

func (m *Manager) validateIdentity(journal Journal) error {
	if journal.ControllerID != m.controllerID || journal.InstanceID != m.instanceID {
		return errors.New("coordinated upgrade journal does not match this broker identity")
	}
	return nil
}

// RestoreFence reapplies the persisted dispatch fence without advancing the
// transaction. The broker calls it before accepting peer or management
// traffic, then RunRecovery advances the transaction once listeners are live.
func (m *Manager) RestoreFence() (*Journal, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	journal, err := m.store.Load()
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if err := m.validateIdentity(journal); err != nil {
		return nil, err
	}
	for _, participant := range journal.Participants {
		if participant.State == ParticipantIntervention {
			m.broker.SetUpgradeIntervention(participant.DeviceID, true)
		}
	}
	if journal.Terminal() {
		m.reconcileTerminalDrain(journal)
	} else {
		m.broker.BeginUpgradeDrain()
	}
	if journal.GlobalCommit {
		m.broker.RequireRuntimeVersion(journal.TargetVersion)
	} else {
		m.broker.RequireRuntimeVersion(journal.SourceVersion)
	}
	return &journal, nil
}

func (m *Manager) advance(ctx context.Context, journal Journal) (Journal, error) {
	for {
		if !journal.GlobalCommit && m.now().UnixMilli() >= journal.Deadline && journal.State != StateCanceling {
			var err error
			journal, err = m.beginCancel(journal, "prepare_timeout")
			if err != nil {
				return journal, err
			}
		}
		var (
			next Journal
			err  error
		)
		switch journal.State {
		case StatePreparing:
			next, err = m.prepare(ctx, journal)
		case StateArming:
			next, err = m.arm(ctx, journal)
		case StateArmed:
			next, err = m.commit(journal)
		case StateCommitted:
			next, err = m.beginPeerActivation(journal)
		case StateActivatingPeers:
			next, err = m.activatePeers(ctx, journal)
		case StateActivatingBroker:
			next, err = m.activateOrQualifyBroker(ctx, journal)
		case StateQualifying:
			return m.qualify(ctx, journal)
		case StateCanceling:
			return m.cancelPrepared(ctx, journal)
		default:
			return journal, nil
		}
		if err != nil {
			if journal.GlobalCommit {
				return journal, err
			}
			canceled, cancelErr := m.beginCancel(journal, "prepare_failed")
			if cancelErr != nil {
				return journal, errors.Join(err, cancelErr)
			}
			return m.cancelPrepared(ctx, canceled)
		}
		journal = next
	}
}

func (m *Manager) prepare(ctx context.Context, journal Journal) (Journal, error) {
	for index := range journal.Participants {
		participant := journal.Participants[index]
		if participant.State != ParticipantPending {
			continue
		}
		snapshot, err := m.broker.CallPinnedUpgrade(ctx, broker.UpgradePeer{
			DeviceID: participant.DeviceID, ConnectionID: participant.ConnectionID,
			RuntimeVersion: participant.SourceVersion,
		}, protocol.MethodPrepareUpgrade, protocol.PrepareUpgradeParams{
			ControllerTransactionID: journal.TransactionID, TransactionID: participant.LocalTransactionID,
			TargetVersion: journal.TargetVersion,
		})
		if err != nil {
			return journal, err
		}
		if err := validatePeerSnapshot(journal, participant, snapshot, false); err != nil {
			return journal, err
		}
		if snapshot.State != string(localupgrade.StatePrepared) {
			return journal, errors.New("peer did not durably prepare its upgrade")
		}
		journal, err = m.store.Update(journal.TransactionID, func(current *Journal) error {
			current.Participants[index].LocalTransactionID = snapshot.TransactionID
			current.Participants[index].TargetRuntimeDigest = snapshot.TargetRuntimeDigest
			current.Participants[index].ConfigDigest = snapshot.ConfigDigest
			current.Participants[index].SourceReadinessEpoch = snapshot.SourceReadinessEpoch
			current.Participants[index].State = ParticipantPrepared
			current.Participants[index].UpdatedAt = snapshot.UpdatedAt
			return nil
		})
		if err != nil {
			return journal, err
		}
	}
	if journal.Broker.TransactionID == "" {
		return journal, errors.New("broker local upgrade transaction ID was not reserved")
	}
	if journal.Broker.State == "" {
		snapshot, err := m.local.PrepareCoordinatedUpgrade(ctx, protocol.PrepareUpgradeParams{
			ControllerTransactionID: journal.TransactionID, TransactionID: journal.Broker.TransactionID,
			TargetVersion: journal.TargetVersion,
		})
		if err != nil {
			return journal, err
		}
		if err := validateLocalSnapshot(journal, snapshot, false); err != nil {
			return journal, err
		}
		if snapshot.State != string(localupgrade.StatePrepared) {
			return journal, errors.New("broker did not durably prepare its upgrade")
		}
		journal, err = m.store.Update(journal.TransactionID, func(current *Journal) error {
			current.Broker = localParticipant(snapshot)
			return nil
		})
		if err != nil {
			return journal, err
		}
	}
	return m.store.Update(journal.TransactionID, func(current *Journal) error {
		current.State = StateArming
		return nil
	})
}

func (m *Manager) arm(ctx context.Context, journal Journal) (Journal, error) {
	for index := range journal.Participants {
		participant := journal.Participants[index]
		if participant.State == ParticipantArmed {
			continue
		}
		snapshot, err := m.broker.CallPinnedUpgrade(ctx, broker.UpgradePeer{
			DeviceID: participant.DeviceID, ConnectionID: participant.ConnectionID,
			RuntimeVersion: participant.SourceVersion,
		}, protocol.MethodArmUpgrade, transactionParams(journal, participant))
		if err != nil {
			return journal, err
		}
		if err := validatePeerSnapshot(journal, participant, snapshot, false); err != nil ||
			snapshot.State != string(localupgrade.StateArmed) || snapshot.CommitAuthorized {
			return journal, errors.Join(err, errors.New("peer did not durably arm its upgrade"))
		}
		journal, err = m.store.Update(journal.TransactionID, func(current *Journal) error {
			current.Participants[index].State = ParticipantArmed
			current.Participants[index].UpdatedAt = snapshot.UpdatedAt
			return nil
		})
		if err != nil {
			return journal, err
		}
	}
	if journal.Broker.State != string(localupgrade.StateArmed) {
		params := protocol.UpgradeTransactionParams{
			ControllerTransactionID: journal.TransactionID, TransactionID: journal.Broker.TransactionID,
		}
		snapshot, err := m.local.ArmCoordinatedUpgrade(ctx, params)
		if err != nil {
			return journal, err
		}
		if err := validateLocalSnapshot(journal, snapshot, false); err != nil ||
			snapshot.State != string(localupgrade.StateArmed) || snapshot.CommitAuthorized {
			return journal, errors.Join(err, errors.New("broker did not durably arm its upgrade"))
		}
		journal, err = m.store.Update(journal.TransactionID, func(current *Journal) error {
			current.Broker = localParticipant(snapshot)
			return nil
		})
		if err != nil {
			return journal, err
		}
	}
	return m.store.Update(journal.TransactionID, func(current *Journal) error {
		current.State = StateArmed
		return nil
	})
}

func (m *Manager) commit(journal Journal) (Journal, error) {
	committed, err := m.store.Update(journal.TransactionID, func(current *Journal) error {
		current.GlobalCommit = true
		current.State = StateCommitted
		return nil
	})
	if err == nil {
		m.broker.RequireRuntimeVersion(committed.TargetVersion)
	}
	return committed, err
}

func (m *Manager) beginPeerActivation(journal Journal) (Journal, error) {
	return m.store.Update(journal.TransactionID, func(current *Journal) error {
		current.State = StateActivatingPeers
		return nil
	})
}

func (m *Manager) activatePeers(ctx context.Context, journal Journal) (Journal, error) {
	for index := range journal.Participants {
		participant := journal.Participants[index]
		if participant.State == ParticipantArmed {
			var err error
			journal, err = m.store.Update(journal.TransactionID, func(current *Journal) error {
				current.Participants[index].State = ParticipantActivationRequested
				current.Participants[index].UpdatedAt = m.now().UnixMilli()
				return nil
			})
			if err != nil {
				return journal, err
			}
			participant = journal.Participants[index]
		}
		snapshot, err := m.broker.CallPinnedUpgrade(ctx, broker.UpgradePeer{
			DeviceID: participant.DeviceID, ConnectionID: participant.ConnectionID,
			RuntimeVersion: participant.SourceVersion,
		}, protocol.MethodActivateUpgrade, transactionParams(journal, participant))
		if err != nil {
			pinnedErr := err
			snapshot, err = m.broker.CallCurrentUpgrade(
				ctx, participant.DeviceID, protocol.MethodActivateUpgrade, transactionParams(journal, participant),
			)
			if err != nil {
				m.reportError(fmt.Errorf(
					"activate upgrade peer %s: %w", participant.DeviceID, errors.Join(pinnedErr, err),
				))
			}
		}
		if err == nil {
			if validationErr := validatePeerSnapshot(journal, participant, snapshot, true); validationErr != nil {
				m.reportError(fmt.Errorf(
					"validate activated upgrade peer %s: %w", participant.DeviceID, validationErr,
				))
			}
		}
	}
	return m.store.Update(journal.TransactionID, func(current *Journal) error {
		current.State = StateActivatingBroker
		return nil
	})
}

func (m *Manager) activateOrQualifyBroker(ctx context.Context, journal Journal) (Journal, error) {
	params := protocol.UpgradeTransactionParams{
		ControllerTransactionID: journal.TransactionID, TransactionID: journal.Broker.TransactionID,
	}
	snapshot, statusErr := m.local.CoordinatedUpgradeStatus(ctx, params)
	if statusErr == nil && snapshot.State == string(localupgrade.StateCommitted) {
		if err := validateLocalSnapshot(journal, snapshot, true); err != nil {
			return journal, err
		}
		return m.store.Update(journal.TransactionID, func(current *Journal) error {
			current.Broker = localParticipant(snapshot)
			current.State = StateQualifying
			return nil
		})
	}
	snapshot, err := m.local.ActivateCoordinatedUpgrade(ctx, params)
	if err != nil {
		return journal, err
	}
	if err := validateLocalSnapshot(journal, snapshot, true); err != nil {
		return journal, err
	}
	journal, err = m.store.Update(journal.TransactionID, func(current *Journal) error {
		current.Broker = localParticipant(snapshot)
		current.State = StateQualifying
		return nil
	})
	return journal, err
}

func (m *Manager) qualify(ctx context.Context, journal Journal) (Journal, error) {
	if journal.CompletionDeadline == 0 {
		if m.sourceVersion != journal.TargetVersion {
			return journal, nil
		}
		var err error
		journal, err = m.store.Update(journal.TransactionID, func(current *Journal) error {
			current.CompletionDeadline = m.now().Add(m.completionTimeout).UnixMilli()
			return nil
		})
		if err != nil {
			return journal, err
		}
	}
	now := m.now().UnixMilli()
	brokerQualified := journal.Broker.State == string(localupgrade.StateCommitted)
	brokerFailed := journal.Broker.FailureCode != ""
	if !brokerQualified && !brokerFailed {
		params := protocol.UpgradeTransactionParams{
			ControllerTransactionID: journal.TransactionID, TransactionID: journal.Broker.TransactionID,
		}
		snapshot, statusErr := m.local.CoordinatedUpgradeStatus(ctx, params)
		if statusErr == nil {
			statusErr = validateLocalSnapshot(journal, snapshot, true)
		}
		if statusErr == nil && snapshot.State == string(localupgrade.StateCommitted) {
			var err error
			journal, err = m.store.Update(journal.TransactionID, func(current *Journal) error {
				current.Broker = localParticipant(snapshot)
				return nil
			})
			if err != nil {
				return journal, err
			}
			brokerQualified = true
		} else if now >= journal.CompletionDeadline {
			failureCode := snapshot.FailureCode
			if failureCode == "" {
				failureCode = "upgrade_qualification_timeout"
			}
			var err error
			journal, err = m.store.Update(journal.TransactionID, func(current *Journal) error {
				current.Broker.FailureCode = failureCode
				current.Broker.UpdatedAt = now
				return nil
			})
			if err != nil {
				return journal, err
			}
			brokerFailed = true
		}
	}
	for index := range journal.Participants {
		participant := journal.Participants[index]
		if participant.State == ParticipantQualified || participant.State == ParticipantIntervention {
			continue
		}
		state := m.broker.UpgradePeerState(participant.DeviceID)
		if state.Connected && state.RuntimeVersion == participant.SourceVersion {
			_, activateErr := m.broker.CallCurrentUpgrade(
				ctx, participant.DeviceID, protocol.MethodActivateUpgrade, transactionParams(journal, participant),
			)
			if activateErr != nil {
				m.reportError(fmt.Errorf("resume upgrade peer %s activation: %w", participant.DeviceID, activateErr))
			}
			// A successful activate can replace the source connection before the
			// response is observed. Re-read the broker view so the same recovery
			// pass can qualify a target-version replacement.
			state = m.broker.UpgradePeerState(participant.DeviceID)
		}
		qualified := state.Connected && state.RuntimeVersion == journal.TargetVersion && state.WorkerSyncReady &&
			state.WorkerReadiness.Epoch > participant.SourceReadinessEpoch &&
			state.WorkerReadiness.State == protocol.WorkerReadinessReady &&
			state.WorkerReadiness.RuntimeDigest == participant.TargetRuntimeDigest &&
			state.WorkerReadiness.ConfigDigest == participant.ConfigDigest
		if qualified {
			snapshot, err := m.broker.CallCurrentUpgrade(
				ctx, participant.DeviceID, protocol.MethodStatusUpgrade, transactionParams(journal, participant),
			)
			qualified = err == nil && validatePeerSnapshot(journal, participant, snapshot, true) == nil &&
				snapshot.State == string(localupgrade.StateCommitted)
		}
		intervention := state.Connected && state.RuntimeVersion == journal.TargetVersion &&
			state.WorkerReadiness.State == protocol.WorkerReadinessInterventionRequired
		if !qualified && !intervention && now < journal.CompletionDeadline {
			continue
		}
		participantState := ParticipantQualified
		failureCode := ""
		if !qualified {
			participantState = ParticipantIntervention
			failureCode = state.WorkerReadiness.FailureCode
			if failureCode == "" {
				failureCode = "upgrade_qualification_timeout"
			}
		}
		var err error
		journal, err = m.store.Update(journal.TransactionID, func(current *Journal) error {
			current.Participants[index].State = participantState
			current.Participants[index].FailureCode = failureCode
			current.Participants[index].UpdatedAt = now
			return nil
		})
		if err != nil {
			return journal, err
		}
		m.broker.SetUpgradeIntervention(participant.DeviceID, participantState == ParticipantIntervention)
	}
	complete, failed := brokerQualified || brokerFailed, brokerFailed
	for _, participant := range journal.Participants {
		complete = complete && (participant.State == ParticipantQualified || participant.State == ParticipantIntervention)
		failed = failed || participant.State == ParticipantIntervention
	}
	if !complete {
		return journal, nil
	}
	journal, err := m.store.Update(journal.TransactionID, func(current *Journal) error {
		current.State = StateCompleted
		if failed {
			current.State = StateCompletedErrors
			current.FailureCode = "upgrade_intervention_required"
		}
		return nil
	})
	if err == nil && !brokerFailed {
		m.broker.EndUpgradeDrain()
	}
	return journal, err
}

func (m *Manager) reconcileTerminalDrain(journal Journal) {
	if journal.State == StateCompletedErrors && journal.Broker.FailureCode != "" {
		m.broker.BeginUpgradeDrain()
		return
	}
	m.broker.EndUpgradeDrain()
}

func (m *Manager) beginCancel(journal Journal, failureCode string) (Journal, error) {
	if journal.State == StateCanceling {
		return journal, nil
	}
	return m.store.Update(journal.TransactionID, func(current *Journal) error {
		current.State = StateCanceling
		current.FailureCode = failureCode
		return nil
	})
}

func (m *Manager) cancelPrepared(ctx context.Context, journal Journal) (Journal, error) {
	var failures []error
	for index := range journal.Participants {
		participant := journal.Participants[index]
		if participant.State == ParticipantCanceled {
			continue
		}
		snapshot, err := m.broker.CallCurrentUpgrade(
			ctx, participant.DeviceID, protocol.MethodCancelUpgrade, transactionParams(journal, participant),
		)
		if errors.Is(err, broker.ErrUpgradeTransactionNotFound) {
			var updateErr error
			journal, updateErr = m.store.Update(journal.TransactionID, func(current *Journal) error {
				current.Participants[index].State = ParticipantCanceled
				current.Participants[index].UpdatedAt = max(participant.UpdatedAt+1, m.now().UnixMilli())
				return nil
			})
			if updateErr != nil {
				failures = append(failures, updateErr)
			}
			continue
		}
		validationErr := validatePeerSnapshot(journal, participant, snapshot, false)
		if err != nil || validationErr != nil || snapshot.State != string(localupgrade.StateRolledBack) {
			failures = append(failures, errors.Join(
				err, validationErr, fmt.Errorf("peer %s did not cancel", participant.DeviceID),
			))
			continue
		}
		var updateErr error
		journal, updateErr = m.store.Update(journal.TransactionID, func(current *Journal) error {
			current.Participants[index].State = ParticipantCanceled
			current.Participants[index].UpdatedAt = snapshot.UpdatedAt
			return nil
		})
		if updateErr != nil {
			failures = append(failures, updateErr)
		}
	}
	if journal.Broker.State != string(localupgrade.StateRolledBack) {
		params := protocol.UpgradeTransactionParams{
			ControllerTransactionID: journal.TransactionID, TransactionID: journal.Broker.TransactionID,
		}
		snapshot, err := m.local.CancelCoordinatedUpgrade(ctx, params)
		if errors.Is(err, os.ErrNotExist) {
			err = nil
		} else {
			validationErr := validateLocalSnapshot(journal, snapshot, false)
			if err != nil || validationErr != nil || snapshot.State != string(localupgrade.StateRolledBack) {
				failures = append(failures, errors.Join(
					err, validationErr, errors.New("broker local upgrade did not cancel"),
				))
			} else {
				journal, err = m.store.Update(journal.TransactionID, func(current *Journal) error {
					current.Broker = localParticipant(snapshot)
					return nil
				})
				if err != nil {
					failures = append(failures, err)
				}
			}
		}
	}
	if len(failures) != 0 {
		return journal, errors.Join(failures...)
	}
	journal, err := m.store.Update(journal.TransactionID, func(current *Journal) error {
		current.State = StateCanceled
		return nil
	})
	if err == nil {
		m.broker.RequireRuntimeVersion(journal.SourceVersion)
		m.broker.EndUpgradeDrain()
	}
	return journal, err
}

func transactionParams(journal Journal, participant Participant) protocol.UpgradeTransactionParams {
	return protocol.UpgradeTransactionParams{
		ControllerTransactionID: journal.TransactionID, TransactionID: participant.LocalTransactionID,
	}
}

func validatePeerSnapshot(
	journal Journal, participant Participant, snapshot protocol.UpgradeSnapshot, committed bool,
) error {
	if err := snapshot.Validate(); err != nil {
		return err
	}
	if snapshot.ControllerTransactionID != journal.TransactionID || snapshot.TargetVersion != journal.TargetVersion ||
		snapshot.SourceVersion != participant.SourceVersion ||
		(participant.LocalTransactionID != "" && snapshot.TransactionID != participant.LocalTransactionID) ||
		(participant.TargetRuntimeDigest != "" && snapshot.TargetRuntimeDigest != participant.TargetRuntimeDigest) ||
		(participant.ConfigDigest != "" && snapshot.ConfigDigest != participant.ConfigDigest) ||
		(participant.SourceReadinessEpoch != 0 && snapshot.SourceReadinessEpoch != participant.SourceReadinessEpoch) ||
		snapshot.CommitAuthorized != committed {
		return errors.New("peer upgrade snapshot does not match the controller transaction")
	}
	return nil
}

func validateLocalSnapshot(journal Journal, snapshot protocol.UpgradeSnapshot, committed bool) error {
	participant := Participant{
		SourceVersion: journal.SourceVersion, LocalTransactionID: journal.Broker.TransactionID,
		TargetRuntimeDigest: journal.Broker.TargetRuntimeDigest, ConfigDigest: journal.Broker.ConfigDigest,
	}
	return validatePeerSnapshot(journal, participant, snapshot, committed)
}

func localParticipant(snapshot protocol.UpgradeSnapshot) LocalParticipant {
	return LocalParticipant{
		TransactionID: snapshot.TransactionID, State: snapshot.State,
		TargetRuntimeDigest: snapshot.TargetRuntimeDigest, ConfigDigest: snapshot.ConfigDigest,
		CommitAuthorized: snapshot.CommitAuthorized, FailureCode: snapshot.FailureCode,
		UpdatedAt: snapshot.UpdatedAt,
	}
}

func (m *Manager) RunRecovery(ctx context.Context) {
	ticker := time.NewTicker(retryInterval)
	defer ticker.Stop()
	for {
		journal, err := m.Resume(ctx)
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) && !errors.Is(err, context.Canceled) {
				m.reportError(fmt.Errorf("resume coordinated upgrade: %w", err))
			}
		} else if journal.Terminal() {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
