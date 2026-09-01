package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/GhostFlying/delegation/internal/protocol"
)

var ErrUpgradeDrainActive = errors.New("broker upgrade drain is active")

// UpgradePeer is the non-secret, generation-pinned view used to freeze a
// controller transaction's participant set.
type UpgradePeer struct {
	DeviceID       string
	ConnectionID   string
	RuntimeVersion string
}

// UpgradePeerState is the current target qualification view for one device.
type UpgradePeerState struct {
	UpgradePeer
	Connected       bool
	WorkerSyncReady bool
	WorkerReadiness protocol.WorkerReadiness
}

// BeginUpgradeDrain establishes the admission fence before returning. Any
// mutation whose durable begin already held the read side finishes first; no
// later mutation can create a receipt until EndUpgradeDrain.
func (s *Server) BeginUpgradeDrain() {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	changed := !s.upgradeDraining.Swap(true)
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, current := range s.connections {
		current.draining.Store(true)
	}
	if changed {
		s.statusGeneration++
	}
}

func (s *Server) EndUpgradeDrain() {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	changed := s.upgradeDraining.Swap(false)
	s.mu.Lock()
	defer s.mu.Unlock()
	for deviceID, current := range s.connections {
		current.draining.Store(s.upgradeIntervention[deviceID])
	}
	if changed {
		s.statusGeneration++
	}
}

func (s *Server) UpgradeDraining() bool {
	return s.upgradeDraining.Load()
}

func (s *Server) SetUpgradeIntervention(deviceID string, required bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if required {
		s.upgradeIntervention[deviceID] = true
	} else {
		delete(s.upgradeIntervention, deviceID)
	}
	if current := s.currentConnectionLocked(deviceID); current != nil {
		current.draining.Store(s.upgradeDraining.Load() || required)
	}
	s.statusGeneration++
}

func (s *Server) FreezeUpgradePeers() []UpgradePeer {
	s.mu.Lock()
	defer s.mu.Unlock()
	peers := make([]UpgradePeer, 0, len(s.connections))
	for deviceID := range s.connections {
		current := s.currentConnectionLocked(deviceID)
		if current == nil || !slices.Contains(current.features, protocol.FeatureCoordinatedUpgrade) {
			continue
		}
		peers = append(peers, UpgradePeer{
			DeviceID: deviceID, ConnectionID: current.connectionID,
			RuntimeVersion: current.runtimeVersion,
		})
	}
	slices.SortFunc(peers, func(left, right UpgradePeer) int {
		return compareStrings(left.DeviceID, right.DeviceID)
	})
	return peers
}

func compareStrings(left, right string) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}

func (s *Server) CallPinnedUpgrade(
	ctx context.Context, peer UpgradePeer, method string, params any,
) (protocol.UpgradeSnapshot, error) {
	s.mu.Lock()
	current := s.currentConnectionLocked(peer.DeviceID)
	if current == nil || current.connectionID != peer.ConnectionID ||
		!slices.Contains(current.features, protocol.FeatureCoordinatedUpgrade) {
		current = nil
	}
	s.mu.Unlock()
	if current == nil {
		return protocol.UpgradeSnapshot{}, errors.New("upgrade peer connection generation changed")
	}
	return callUpgradePeer(ctx, current, method, params)
}

func (s *Server) CallCurrentUpgrade(
	ctx context.Context, deviceID, method string, params any,
) (protocol.UpgradeSnapshot, error) {
	s.mu.Lock()
	current := s.currentConnectionLocked(deviceID)
	if current == nil || !slices.Contains(current.features, protocol.FeatureCoordinatedUpgrade) {
		current = nil
	}
	s.mu.Unlock()
	if current == nil {
		return protocol.UpgradeSnapshot{}, errors.New("upgrade peer is unavailable")
	}
	return callUpgradePeer(ctx, current, method, params)
}

func callUpgradePeer(
	ctx context.Context, current *session, method string, params any,
) (protocol.UpgradeSnapshot, error) {
	switch method {
	case protocol.MethodPrepareUpgrade, protocol.MethodArmUpgrade, protocol.MethodActivateUpgrade,
		protocol.MethodCancelUpgrade, protocol.MethodStatusUpgrade:
	default:
		return protocol.UpgradeSnapshot{}, fmt.Errorf("unsupported upgrade peer method %q", method)
	}
	payload, err := current.callPeerRequest(ctx, method, "", nil, params)
	if err != nil {
		return protocol.UpgradeSnapshot{}, err
	}
	var snapshot protocol.UpgradeSnapshot
	if err := json.Unmarshal(payload, &snapshot); err != nil {
		return protocol.UpgradeSnapshot{}, fmt.Errorf("decode peer upgrade state: %w", err)
	}
	if err := snapshot.Validate(); err != nil {
		_ = current.connection.CloseNow()
		return protocol.UpgradeSnapshot{}, fmt.Errorf("validate peer upgrade state: %w", err)
	}
	return snapshot, nil
}

func (s *Server) UpgradePeerState(deviceID string) UpgradePeerState {
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.currentConnectionLocked(deviceID)
	if current == nil {
		return UpgradePeerState{UpgradePeer: UpgradePeer{DeviceID: deviceID}}
	}
	return UpgradePeerState{
		UpgradePeer: UpgradePeer{
			DeviceID: deviceID, ConnectionID: current.connectionID, RuntimeVersion: current.runtimeVersion,
		},
		Connected: true, WorkerSyncReady: current.workerSyncReady.Load(),
		WorkerReadiness: current.workerReadiness,
	}
}

func (s *Server) admitMutation() (func(), error) {
	s.mutationMu.RLock()
	if s.upgradeDraining.Load() {
		s.mutationMu.RUnlock()
		return nil, ErrUpgradeDrainActive
	}
	return s.mutationMu.RUnlock, nil
}

func (s *session) rejectDrainedMutation(
	ctx context.Context, request protocol.Envelope,
) (func(), error) {
	release, err := s.server.admitMutation()
	if err == nil {
		return release, nil
	}
	return nil, s.writeError(ctx, request, protocol.ErrorUnavailable, "broker upgrade drain is active")
}
