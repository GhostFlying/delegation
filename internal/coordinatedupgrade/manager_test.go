package coordinatedupgrade

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/broker"
	"github.com/GhostFlying/delegation/internal/localupgrade"
	"github.com/GhostFlying/delegation/internal/protocol"
)

const (
	testControllerTransactionID = "123e4567-e89b-42d3-a456-426614174701"
	testPeerDeviceID            = "123e4567-e89b-42d3-a456-426614174702"
	testPeerConnectionID        = "123e4567-e89b-42d3-a456-426614174703"
	testPeerTransactionID       = "123e4567-e89b-42d3-a456-426614174704"
	testBrokerTransactionID     = "123e4567-e89b-42d3-a456-426614174705"
	testReplacementConnectionID = "123e4567-e89b-42d3-a456-426614174706"
	testSourceVersion           = "0.1.0"
	testTargetVersion           = "0.2.0"
	testPeerRuntimeDigest       = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	testPeerConfigDigest        = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	testBrokerRuntimeDigest     = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	testBrokerConfigDigest      = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
)

type coordinatorFixture struct {
	t          *testing.T
	now        time.Time
	store      *Store
	broker     *fakeBrokerControl
	local      *fakeLocalManager
	errors     []error
	manager    *Manager
	sharedCall *[]string
}

func newCoordinatorFixture(t *testing.T) *coordinatorFixture {
	t.Helper()
	root := filepath.Join(t.TempDir(), "controller")
	store, err := OpenStore(root)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_800_000_000, 0)
	fixture := &coordinatorFixture{t: t, now: now, store: store}
	calls := []string{}
	fixture.sharedCall = &calls
	fixture.broker = newFakeBrokerControl(&calls, now)
	fixture.local = newFakeLocalManager(&calls, now)
	store.now = func() time.Time { return fixture.now }
	fixture.manager = fixture.newManager()
	return fixture
}

func (f *coordinatorFixture) newManager() *Manager {
	return f.newManagerAtVersion(testSourceVersion)
}

func (f *coordinatorFixture) newManagerAtVersion(version string) *Manager {
	f.t.Helper()
	manager, err := NewManager(Options{
		Store: f.store, Broker: f.broker, Local: f.local, SourceVersion: version,
		ControllerID: testPeerDeviceID, InstanceID: "default",
		NewID: func() (string, error) { return testControllerTransactionID, nil },
		Now:   func() time.Time { return f.now }, CompletionTimeout: time.Minute,
		ReportError: func(err error) { f.errors = append(f.errors, err) },
	})
	if err != nil {
		f.t.Fatal(err)
	}
	return manager
}

func TestManagerCompletesPeerFirstBrokerLastUpgrade(t *testing.T) {
	fixture := newCoordinatorFixture(t)
	journal, err := fixture.manager.Start(context.Background(), testTargetVersion, 30*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if journal.State != StateQualifying || !journal.GlobalCommit || !fixture.broker.draining {
		t.Fatalf("source broker journal = %#v, draining=%v", journal, fixture.broker.draining)
	}
	journal, err = fixture.newManagerAtVersion(testTargetVersion).Resume(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if journal.State != StateCompleted || !journal.GlobalCommit || fixture.broker.draining {
		t.Fatalf("completed journal = %#v, draining=%v", journal, fixture.broker.draining)
	}
	if journal.Participants[0].State != ParticipantQualified ||
		journal.Broker.State != string(localupgrade.StateCommitted) {
		t.Fatalf("qualified participants = %#v / %#v", journal.Participants, journal.Broker)
	}
	wantOrder := []string{
		"peer:prepare", "broker:prepare", "peer:arm", "broker:arm",
		"peer:activate", "broker:status", "broker:activate", "broker:status", "peer:status",
	}
	if !slices.Equal(*fixture.sharedCall, wantOrder) {
		t.Fatalf("upgrade call order = %q, want %q", *fixture.sharedCall, wantOrder)
	}
}

func TestManagerCancelsAllPreparedWorkWhenArmFails(t *testing.T) {
	fixture := newCoordinatorFixture(t)
	fixture.broker.failMethod[protocol.MethodArmUpgrade] = errors.New("injected arm failure")
	journal, err := fixture.manager.Start(context.Background(), testTargetVersion, 30*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if journal.State != StateCanceled || journal.GlobalCommit || fixture.broker.draining {
		t.Fatalf("canceled journal = %#v, draining=%v", journal, fixture.broker.draining)
	}
	if journal.Participants[0].State != ParticipantCanceled ||
		journal.Broker.State != string(localupgrade.StateRolledBack) {
		t.Fatalf("canceled participants = %#v / %#v", journal.Participants, journal.Broker)
	}
	if slices.Contains(*fixture.sharedCall, "peer:activate") || slices.Contains(*fixture.sharedCall, "broker:activate") {
		t.Fatalf("pre-COMMIT failure activated service: %q", *fixture.sharedCall)
	}
	calls := len(*fixture.sharedCall)
	resumed, err := fixture.manager.Cancel(context.Background(), journal.TransactionID)
	if err != nil || resumed.State != StateCanceled || len(*fixture.sharedCall) != calls {
		t.Fatalf("idempotent cancel = %#v, %v, calls=%q", resumed, err, *fixture.sharedCall)
	}
}

func TestManagerRejectsReplacementConnectionBeforeCommit(t *testing.T) {
	fixture := newCoordinatorFixture(t)
	fixture.broker.replaceBeforeMethod = protocol.MethodArmUpgrade
	journal, err := fixture.manager.Start(context.Background(), testTargetVersion, 30*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if journal.State != StateCanceled || journal.GlobalCommit {
		t.Fatalf("replacement connection journal = %#v", journal)
	}
	if fixture.broker.pinnedReplacementAccepted {
		t.Fatal("generation-pinned upgrade call accepted a replacement connection")
	}
}

func TestManagerTreatsLostPeerActivationResponseAsAmbiguousAfterCommit(t *testing.T) {
	fixture := newCoordinatorFixture(t)
	fixture.broker.loseActivationResponse = true
	journal, err := fixture.manager.Start(context.Background(), testTargetVersion, 30*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	journal, err = fixture.newManagerAtVersion(testTargetVersion).Resume(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if journal.State != StateCompleted || !journal.GlobalCommit || len(fixture.errors) != 1 {
		t.Fatalf("lost response journal = %#v, reports=%v", journal, fixture.errors)
	}
	if journal.Participants[0].State != ParticipantQualified {
		t.Fatalf("lost activation response did not qualify through current state: %#v", journal.Participants[0])
	}
	if _, err := fixture.manager.Cancel(context.Background(), journal.TransactionID); err == nil {
		t.Fatal("Cancel accepted a transaction after global COMMIT")
	}
}

func TestManagerActivatesPeerThroughReplacementConnectionAfterCommit(t *testing.T) {
	fixture := newCoordinatorFixture(t)
	fixture.broker.replaceBeforeMethod = protocol.MethodActivateUpgrade
	journal, err := fixture.manager.Start(context.Background(), testTargetVersion, 30*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	journal, err = fixture.newManagerAtVersion(testTargetVersion).Resume(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if journal.State != StateCompleted || journal.Participants[0].State != ParticipantQualified {
		t.Fatalf("replacement activation journal = %#v", journal)
	}
	wantActivationCalls := 0
	for _, call := range *fixture.sharedCall {
		if call == "peer:activate" {
			wantActivationCalls++
		}
	}
	if wantActivationCalls != 2 {
		t.Fatalf("replacement activation calls = %d, calls=%q", wantActivationCalls, *fixture.sharedCall)
	}
}

func TestManagerRetriesLostPeerActivationAfterTargetBrokerRestart(t *testing.T) {
	fixture := newCoordinatorFixture(t)
	fixture.broker.activationFailures = 2
	fixture.broker.state = broker.UpgradePeerState{
		UpgradePeer: broker.UpgradePeer{
			DeviceID: testPeerDeviceID, ConnectionID: testReplacementConnectionID,
			RuntimeVersion: testSourceVersion,
		},
		Connected: true, WorkerSyncReady: true, WorkerReadiness: readyWorkerReadiness(),
	}
	first, err := fixture.manager.Start(context.Background(), testTargetVersion, 30*time.Minute)
	if err != nil || first.State != StateQualifying || first.Participants[0].State != ParticipantActivationRequested {
		t.Fatalf("initial lost activation = %#v, %v", first, err)
	}
	if len(fixture.errors) != 1 {
		t.Fatalf("initial activation reports = %v", fixture.errors)
	}

	resumed, err := fixture.newManagerAtVersion(testTargetVersion).Resume(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if resumed.State != StateCompleted || resumed.Participants[0].State != ParticipantQualified {
		t.Fatalf("resumed peer activation = %#v", resumed)
	}
	activationCalls := 0
	for _, call := range *fixture.sharedCall {
		if call == "peer:activate" {
			activationCalls++
		}
	}
	if activationCalls != 3 {
		t.Fatalf("peer activation calls = %d, calls=%q", activationCalls, *fixture.sharedCall)
	}
}

func TestManagerResumesBrokerActivationAfterRestart(t *testing.T) {
	fixture := newCoordinatorFixture(t)
	fixture.local.failActivate = errors.New("injected broker activation response loss")
	first, err := fixture.manager.Start(context.Background(), testTargetVersion, 30*time.Minute)
	if err == nil || first.State != StateActivatingBroker || !first.GlobalCommit {
		t.Fatalf("first activation = %#v, %v", first, err)
	}
	fixture.local.failActivate = nil
	resumed, err := fixture.newManagerAtVersion(testTargetVersion).Resume(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if resumed.State != StateCompleted || !resumed.GlobalCommit || fixture.broker.draining {
		t.Fatalf("resumed activation = %#v, draining=%v", resumed, fixture.broker.draining)
	}
}

func TestManagerTimesOutUnavailablePeerAndRequiresIntervention(t *testing.T) {
	fixture := newCoordinatorFixture(t)
	fixture.broker.qualifyAfterActivate = false
	journal, err := fixture.manager.Start(context.Background(), testTargetVersion, 30*time.Minute)
	if err != nil || journal.State != StateQualifying || !fixture.broker.draining {
		t.Fatalf("initial qualification = %#v, %v, draining=%v", journal, err, fixture.broker.draining)
	}
	targetManager := fixture.newManagerAtVersion(testTargetVersion)
	journal, err = targetManager.Resume(context.Background())
	if err != nil || journal.State != StateQualifying {
		t.Fatalf("target broker qualification = %#v, %v", journal, err)
	}
	fixture.now = fixture.now.Add(2 * time.Minute)
	journal, err = targetManager.Resume(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if journal.State != StateCompletedErrors || journal.Participants[0].State != ParticipantIntervention ||
		journal.Participants[0].FailureCode != "upgrade_qualification_timeout" ||
		!fixture.broker.intervention[testPeerDeviceID] || fixture.broker.draining {
		t.Fatalf("timed out qualification = %#v, intervention=%v, draining=%v",
			journal, fixture.broker.intervention, fixture.broker.draining)
	}
}

func TestManagerStartsCompletionDeadlineOnlyOnTargetBroker(t *testing.T) {
	fixture := newCoordinatorFixture(t)
	fixture.broker.qualifyAfterActivate = false
	journal, err := fixture.manager.Start(context.Background(), testTargetVersion, 30*time.Minute)
	if err != nil || journal.State != StateQualifying || journal.CompletionDeadline != 0 {
		t.Fatalf("source broker qualification = %#v, %v", journal, err)
	}
	fixture.now = fixture.now.Add(9 * time.Minute)
	target := fixture.newManagerAtVersion(testTargetVersion)
	journal, err = target.Resume(context.Background())
	if err != nil || journal.CompletionDeadline != fixture.now.Add(time.Minute).UnixMilli() {
		t.Fatalf("target broker completion deadline = %#v, %v", journal, err)
	}
}

func TestManagerRejectsJournalForAnotherBrokerIdentity(t *testing.T) {
	fixture := newCoordinatorFixture(t)
	journal := testControllerJournal()
	journal.ControllerID = testControllerTransactionID
	if _, _, err := fixture.store.CreateOrResume(journal); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.manager.RestoreFence(); err == nil {
		t.Fatal("RestoreFence accepted another broker's controller journal")
	}
}

type fakeBrokerControl struct {
	now                       time.Time
	calls                     *[]string
	peers                     []broker.UpgradePeer
	currentConnection         string
	prepared                  protocol.UpgradeSnapshot
	state                     broker.UpgradePeerState
	failMethod                map[string]error
	replaceBeforeMethod       string
	replaced                  bool
	pinnedReplacementAccepted bool
	loseActivationResponse    bool
	activationFailures        int
	qualifyAfterActivate      bool
	draining                  bool
	intervention              map[string]bool
	requiredRuntimeVersion    string
}

func newFakeBrokerControl(calls *[]string, now time.Time) *fakeBrokerControl {
	peer := broker.UpgradePeer{
		DeviceID: testPeerDeviceID, ConnectionID: testPeerConnectionID, RuntimeVersion: testSourceVersion,
	}
	return &fakeBrokerControl{
		now: now, calls: calls, peers: []broker.UpgradePeer{peer}, currentConnection: peer.ConnectionID,
		failMethod: map[string]error{}, intervention: map[string]bool{}, qualifyAfterActivate: true,
	}
}

func (f *fakeBrokerControl) BeginUpgradeDrain()    { f.draining = true }
func (f *fakeBrokerControl) EndUpgradeDrain()      { f.draining = false }
func (f *fakeBrokerControl) UpgradeDraining() bool { return f.draining }
func (f *fakeBrokerControl) RequireRuntimeVersion(version string) {
	f.requiredRuntimeVersion = version
}
func (f *fakeBrokerControl) FreezeUpgradePeers() []broker.UpgradePeer {
	return slices.Clone(f.peers)
}
func (f *fakeBrokerControl) CallPinnedUpgrade(
	_ context.Context, peer broker.UpgradePeer, method string, params any,
) (protocol.UpgradeSnapshot, error) {
	*f.calls = append(*f.calls, "peer:"+upgradeMethodName(method))
	if f.replaceBeforeMethod == method && !f.replaced {
		f.currentConnection = testReplacementConnectionID
		f.replaced = true
	}
	if peer.ConnectionID != f.currentConnection {
		return protocol.UpgradeSnapshot{}, errors.New("upgrade peer connection generation changed")
	}
	if f.replaced {
		f.pinnedReplacementAccepted = true
	}
	if err := f.failMethod[method]; err != nil {
		return protocol.UpgradeSnapshot{}, err
	}
	return f.upgrade(method, params)
}
func (f *fakeBrokerControl) CallCurrentUpgrade(
	_ context.Context, _ string, method string, params any,
) (protocol.UpgradeSnapshot, error) {
	*f.calls = append(*f.calls, "peer:"+upgradeMethodName(method))
	return f.upgrade(method, params)
}
func (f *fakeBrokerControl) upgrade(method string, params any) (protocol.UpgradeSnapshot, error) {
	switch method {
	case protocol.MethodPrepareUpgrade:
		prepare := params.(protocol.PrepareUpgradeParams)
		f.prepared = protocol.UpgradeSnapshot{
			ControllerTransactionID: prepare.ControllerTransactionID, TransactionID: testPeerTransactionID,
			State: string(localupgrade.StatePrepared), SourceVersion: testSourceVersion, TargetVersion: prepare.TargetVersion,
			TargetRuntimeDigest: testPeerRuntimeDigest, ConfigDigest: testPeerConfigDigest,
			SourceReadinessEpoch: 4, UpdatedAt: f.now.UnixMilli(),
		}
	case protocol.MethodArmUpgrade:
		f.prepared.State = string(localupgrade.StateArmed)
	case protocol.MethodActivateUpgrade:
		if f.activationFailures > 0 {
			f.activationFailures--
			return protocol.UpgradeSnapshot{}, errors.New("injected activation failure")
		}
		f.prepared.State = string(localupgrade.StateCommitted)
		f.prepared.CommitAuthorized = true
		if f.qualifyAfterActivate {
			f.state = broker.UpgradePeerState{
				UpgradePeer: broker.UpgradePeer{
					DeviceID: testPeerDeviceID, ConnectionID: testReplacementConnectionID, RuntimeVersion: testTargetVersion,
				},
				Connected: true, WorkerSyncReady: true, WorkerReadiness: readyWorkerReadiness(),
			}
		}
		if f.loseActivationResponse {
			return protocol.UpgradeSnapshot{}, errors.New("activation response lost")
		}
	case protocol.MethodCancelUpgrade:
		f.prepared.State = string(localupgrade.StateRolledBack)
	case protocol.MethodStatusUpgrade:
	}
	f.prepared.UpdatedAt++
	return f.prepared, nil
}
func (f *fakeBrokerControl) UpgradePeerState(string) broker.UpgradePeerState { return f.state }
func (f *fakeBrokerControl) SetUpgradeIntervention(deviceID string, required bool) {
	f.intervention[deviceID] = required
}

type fakeLocalManager struct {
	now          time.Time
	calls        *[]string
	prepared     protocol.UpgradeSnapshot
	failActivate error
}

func newFakeLocalManager(calls *[]string, now time.Time) *fakeLocalManager {
	return &fakeLocalManager{calls: calls, now: now}
}
func (f *fakeLocalManager) PrepareCoordinatedUpgrade(
	_ context.Context, params protocol.PrepareUpgradeParams,
) (protocol.UpgradeSnapshot, error) {
	*f.calls = append(*f.calls, "broker:prepare")
	f.prepared = protocol.UpgradeSnapshot{
		ControllerTransactionID: params.ControllerTransactionID, TransactionID: testBrokerTransactionID,
		State: string(localupgrade.StatePrepared), SourceVersion: testSourceVersion, TargetVersion: params.TargetVersion,
		TargetRuntimeDigest: testBrokerRuntimeDigest, ConfigDigest: testBrokerConfigDigest,
		SourceReadinessEpoch: 7, UpdatedAt: f.now.UnixMilli(),
	}
	return f.prepared, nil
}
func (f *fakeLocalManager) ArmCoordinatedUpgrade(
	context.Context, protocol.UpgradeTransactionParams,
) (protocol.UpgradeSnapshot, error) {
	*f.calls = append(*f.calls, "broker:arm")
	f.prepared.State = string(localupgrade.StateArmed)
	f.prepared.UpdatedAt++
	return f.prepared, nil
}
func (f *fakeLocalManager) ActivateCoordinatedUpgrade(
	context.Context, protocol.UpgradeTransactionParams,
) (protocol.UpgradeSnapshot, error) {
	*f.calls = append(*f.calls, "broker:activate")
	f.prepared.State = string(localupgrade.StateActivating)
	f.prepared.CommitAuthorized = true
	f.prepared.UpdatedAt++
	return f.prepared, f.failActivate
}
func (f *fakeLocalManager) CancelCoordinatedUpgrade(
	context.Context, protocol.UpgradeTransactionParams,
) (protocol.UpgradeSnapshot, error) {
	*f.calls = append(*f.calls, "broker:cancel")
	f.prepared.State = string(localupgrade.StateRolledBack)
	f.prepared.UpdatedAt++
	return f.prepared, nil
}
func (f *fakeLocalManager) CoordinatedUpgradeStatus(
	context.Context, protocol.UpgradeTransactionParams,
) (protocol.UpgradeSnapshot, error) {
	*f.calls = append(*f.calls, "broker:status")
	if f.prepared.CommitAuthorized && f.failActivate == nil {
		f.prepared.State = string(localupgrade.StateCommitted)
	}
	f.prepared.UpdatedAt++
	return f.prepared, nil
}

func readyWorkerReadiness() protocol.WorkerReadiness {
	return protocol.WorkerReadiness{
		Epoch: 5, State: protocol.WorkerReadinessReady, AttemptCount: 1,
		RuntimeDigest: testPeerRuntimeDigest, ConfigDigest: testPeerConfigDigest,
		EpochStartedAt: 1, LastAttemptAt: 1, UpdatedAt: 1,
	}
}

func upgradeMethodName(method string) string {
	switch method {
	case protocol.MethodPrepareUpgrade:
		return "prepare"
	case protocol.MethodArmUpgrade:
		return "arm"
	case protocol.MethodActivateUpgrade:
		return "activate"
	case protocol.MethodCancelUpgrade:
		return "cancel"
	case protocol.MethodStatusUpgrade:
		return "status"
	default:
		return method
	}
}
