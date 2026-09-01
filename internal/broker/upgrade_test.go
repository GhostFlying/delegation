package broker

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/protocol"
)

const (
	upgradeTestDeviceA               = "123e4567-e89b-42d3-a456-426614174181"
	upgradeTestDeviceB               = "123e4567-e89b-42d3-a456-426614174182"
	upgradeTestConnectionA           = "123e4567-e89b-42d3-a456-426614174183"
	upgradeTestConnectionB           = "123e4567-e89b-42d3-a456-426614174184"
	testUpgradeReplacementConnection = "123e4567-e89b-42d3-a456-426614174185"
)

func TestUpgradeDrainWaitsForAdmittedMutationAndRejectsNewOnes(t *testing.T) {
	server := upgradeTestServer()
	release, err := server.admitMutation()
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	drained := make(chan struct{})
	go func() {
		close(started)
		server.BeginUpgradeDrain()
		close(drained)
	}()
	<-started
	select {
	case <-drained:
		t.Fatal("BeginUpgradeDrain returned before an admitted mutation finished")
	case <-time.After(25 * time.Millisecond):
	}
	release()
	select {
	case <-drained:
	case <-time.After(time.Second):
		t.Fatal("BeginUpgradeDrain did not establish its admission fence")
	}
	if release, err := server.admitMutation(); !errors.Is(err, ErrUpgradeDrainActive) || release != nil {
		t.Fatalf("admission during drain returned release=%t, error=%v", release != nil, err)
	}
	server.EndUpgradeDrain()
	if release, err := server.admitMutation(); err != nil {
		t.Fatal(err)
	} else {
		release()
	}
}

func TestUpgradeParticipantFreezeIsSortedCapableAndGenerationPinned(t *testing.T) {
	server := upgradeTestServer()
	server.connections[upgradeTestDeviceB] = upgradeSession(
		upgradeTestDeviceB, upgradeTestConnectionB, true,
	)
	server.connections[upgradeTestDeviceA] = upgradeSession(
		upgradeTestDeviceA, upgradeTestConnectionA, false,
	)
	incompatible := upgradeSession(
		"123e4567-e89b-42d3-a456-426614174186", testUpgradeReplacementConnection, true,
	)
	incompatible.versionCompatible.Store(false)
	server.connections[incompatible.deviceID] = incompatible
	peers := server.FreezeUpgradePeers()
	if len(peers) != 1 || peers[0].DeviceID != upgradeTestDeviceB ||
		peers[0].ConnectionID != upgradeTestConnectionB {
		t.Fatalf("frozen upgrade peers = %#v", peers)
	}
	frozen := peers[0]
	server.connections[upgradeTestDeviceB] = upgradeSession(
		upgradeTestDeviceB, upgradeTestConnectionA, true,
	)
	if _, err := server.CallPinnedUpgrade(
		context.Background(), frozen, protocol.MethodPrepareUpgrade, protocol.PrepareUpgradeParams{},
	); err == nil {
		t.Fatal("CallPinnedUpgrade accepted a replacement connection generation")
	}
}

func TestUpgradeDrainAndInterventionCarryAcrossConnections(t *testing.T) {
	server := upgradeTestServer()
	server.connections[upgradeTestDeviceA] = upgradeSession(
		upgradeTestDeviceA, upgradeTestConnectionA, true,
	)
	initialGeneration := server.statusGeneration
	server.BeginUpgradeDrain()
	if !server.connections[upgradeTestDeviceA].draining.Load() ||
		server.statusGeneration != initialGeneration+1 {
		t.Fatalf("drain state = %v, generation=%d",
			server.connections[upgradeTestDeviceA].draining.Load(), server.statusGeneration)
	}
	replacement := upgradeSession(upgradeTestDeviceA, upgradeTestConnectionB, true)
	if _, active := server.activate(replacement); !active || !replacement.draining.Load() {
		t.Fatal("replacement connection did not inherit controller drain")
	}
	server.SetUpgradeIntervention(upgradeTestDeviceA, true)
	server.EndUpgradeDrain()
	if !replacement.draining.Load() {
		t.Fatal("ending controller drain cleared per-device intervention")
	}
	server.SetUpgradeIntervention(upgradeTestDeviceA, false)
	if replacement.draining.Load() {
		t.Fatal("clearing intervention left the device non-dispatchable")
	}
}

func TestUpgradePeerStateCarriesReadinessSnapshot(t *testing.T) {
	server := upgradeTestServer()
	current := upgradeSession(upgradeTestDeviceA, upgradeTestConnectionA, true)
	current.workerSyncReady.Store(true)
	current.workerReadiness = protocol.WorkerReadiness{
		Epoch: 9, State: protocol.WorkerReadinessReady, AttemptCount: 1,
		RuntimeDigest:  "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		ConfigDigest:   "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		EpochStartedAt: 1, LastAttemptAt: 1, UpdatedAt: 1,
	}
	server.connections[upgradeTestDeviceA] = current
	state := server.UpgradePeerState(upgradeTestDeviceA)
	if !state.Connected || !state.WorkerSyncReady || state.WorkerReadiness.Epoch != 9 ||
		state.RuntimeVersion != current.runtimeVersion {
		t.Fatalf("upgrade peer state = %#v", state)
	}
}

func TestRequiredRuntimeVersionKeepsLateOldPeerNonDispatchable(t *testing.T) {
	server := upgradeTestServer()
	oldPeer := upgradeSession(upgradeTestDeviceA, upgradeTestConnectionA, true)
	newPeer := upgradeSession(upgradeTestDeviceB, upgradeTestConnectionB, true)
	newPeer.runtimeVersion = "0.2.0"
	oldPeer.versionCompatible.Store(true)
	newPeer.versionCompatible.Store(true)
	server.connections[upgradeTestDeviceA] = oldPeer
	server.connections[upgradeTestDeviceB] = newPeer
	server.RequireRuntimeVersion("0.2.0")
	if oldPeer.versionCompatible.Load() || !newPeer.versionCompatible.Load() {
		t.Fatalf("version compatibility old=%v new=%v",
			oldPeer.versionCompatible.Load(), newPeer.versionCompatible.Load())
	}
	replacement := upgradeSession(upgradeTestDeviceA, testUpgradeReplacementConnection, true)
	if _, active := server.activate(replacement); !active {
		t.Fatal("replacement connection was not activated")
	}
	if replacement.versionCompatible.Load() {
		t.Fatal("late old-version peer bypassed committed version fence")
	}
}

func upgradeTestServer() *Server {
	return &Server{
		connections: map[string]*session{}, latestRevisions: map[string]uint64{},
		upgradeIntervention: map[string]bool{},
	}
}

func upgradeSession(deviceID, connectionID string, capable bool) *session {
	features := []string{protocol.FeatureWorkerDispatch}
	if capable {
		features = append(features, protocol.FeatureCoordinatedUpgrade)
		slices.Sort(features)
	}
	current := &session{
		deviceID: deviceID, connectionID: connectionID, runtimeVersion: "0.1.0",
		features: features,
	}
	current.revision.Store(1)
	current.versionCompatible.Store(true)
	return current
}
