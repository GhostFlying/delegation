package broker

import (
	"context"
	"errors"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
)

const maximumAgentSnapshotAttempts = 3

type agentTargetSnapshot struct {
	connected    bool
	syncReady    bool
	dispatchable bool
}

type agentConnectionSnapshot struct {
	generation uint64
	targets    map[string]agentTargetSnapshot
}

func (s *Server) listAgentStates(
	ctx context.Context,
	source control.PrincipalIdentity,
	request store.AgentPageRequest,
) (protocol.ListAgentsResult, error) {
	for range maximumAgentSnapshotAttempts {
		connections := s.captureAgentConnections()
		page, err := s.registry.ListAgents(ctx, source, request)
		if err != nil {
			return protocol.ListAgentsResult{}, err
		}
		if !s.connectionStatusGenerationMatches(connections.generation) {
			if err := ctx.Err(); err != nil {
				return protocol.ListAgentsResult{}, err
			}
			continue
		}
		result := protocol.ListAgentsResult{
			Agents:       make([]protocol.AgentState, 0, len(page.Agents)),
			NextSequence: page.NextSequence,
		}
		for _, record := range page.Agents {
			result.Agents = append(result.Agents, projectAgentState(
				record, connections.targets[record.Spawn.Principal.DeviceID],
			))
		}
		return result, nil
	}
	return protocol.ListAgentsResult{}, errors.New(
		"broker connections changed during agent snapshot",
	)
}

func (s *Server) captureAgentConnections() agentConnectionSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	snapshot := agentConnectionSnapshot{
		generation: s.statusGeneration,
		targets:    make(map[string]agentTargetSnapshot, len(s.connections)),
	}
	for deviceID := range s.connections {
		current := s.currentConnectionLocked(deviceID)
		if current == nil {
			continue
		}
		snapshot.targets[deviceID] = agentTargetSnapshot{
			connected: true,
			syncReady: current.workerSyncReady.Load(),
			dispatchable: current.workerSyncReady.Load() && current.workerReady.Load() &&
				current.versionCompatible.Load() && !current.draining.Load(),
		}
	}
	return snapshot
}

func projectAgentState(
	record store.AgentCurrentRecord,
	target agentTargetSnapshot,
) protocol.AgentState {
	spawn := record.Spawn
	state := protocol.AgentState{
		SpawnID: spawn.SpawnID, Principal: spawn.Principal, TaskName: spawn.TaskName,
		SpawnStatus: spawn.SpawnStatus, SpawnFailureCode: spawn.SpawnFailureCode,
		WorkspaceID: spawn.WorkspaceID, Sequence: spawn.Sequence,
		LifecycleFreshness: protocol.AgentLifecycleMissing,
		TargetDispatchable: target.dispatchable,
	}
	if lifecycle := record.Lifecycle; lifecycle != nil {
		state.LifecyclePhase = lifecycle.Phase
		state.LifecycleFailureCode = lifecycle.FailureCode
		state.LifecycleTargetRevision = lifecycle.TargetRevision
		state.LifecycleObservedAt = lifecycle.ObservedAt
		switch {
		case !target.connected:
			state.LifecycleFreshness = protocol.AgentLifecycleOffline
		case !target.syncReady:
			state.LifecycleFreshness = protocol.AgentLifecycleSyncing
		default:
			state.LifecycleFreshness = protocol.AgentLifecycleCurrent
		}
	}
	state.EffectiveStatus, state.EffectiveFailureCode, state.FailureSource =
		protocol.ProjectAgentEffectiveState(
			state.SpawnStatus, state.SpawnFailureCode, state.LifecyclePhase,
			state.LifecycleFailureCode, state.LifecycleFreshness,
		)
	return state
}
