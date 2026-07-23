package cli

import (
	"context"
	"errors"

	"github.com/GhostFlying/delegation/internal/connector"
	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
	"github.com/GhostFlying/delegation/internal/workerhost"
)

type managedWorkerSpawner struct {
	host         managedWorkerHost
	state        managedWorkerState
	controllerID string
	deviceID     string
}

type managedWorkerHost interface {
	Spawn(context.Context, workerhost.SpawnRequest) (workerhost.StartedTurn, error)
	Send(context.Context, workerhost.SendRequest) (workerhost.OperationResult, error)
	Followup(context.Context, workerhost.FollowupRequest) (workerhost.OperationResult, error)
	Interrupt(context.Context, workerhost.InterruptRequest) (workerhost.OperationResult, error)
	InspectWorkspace(context.Context, workerhost.WorkspaceInspectRequest) (protocol.InspectWorkspaceResult, error)
	PrepareWorkspace(context.Context, workerhost.WorkspacePrepareRequest) (protocol.PrepareWorkspaceResult, error)
}

type managedWorkerState interface {
	GetWorker(context.Context, store.WorkerKey) (store.WorkerReservation, error)
}

func (s managedWorkerSpawner) SpawnWorker(
	ctx context.Context,
	request connector.WorkerSpawnRequest,
) (protocol.SpawnWorkerResult, error) {
	principal := control.NewWorkerPrincipal(
		s.controllerID,
		request.TreeID,
		request.Params.AgentID,
		request.Source.AgentID,
		s.deviceID,
	).Identity()
	result := protocol.SpawnWorkerResult{
		SpawnID:   request.Params.SpawnID,
		Principal: principal,
		Outcome:   protocol.AgentSpawnOutcomeIndeterminate,
	}
	if request.Source.ControllerID != s.controllerID || request.Source.TreeID != request.TreeID ||
		request.Source.ParentAgentID != "" {
		return protocol.SpawnWorkerResult{}, errors.New("worker dispatch source is invalid")
	}
	started, err := s.host.Spawn(ctx, workerhost.SpawnRequest{
		TreeID:        request.TreeID,
		AgentID:       request.Params.AgentID,
		ParentAgentID: request.Source.AgentID,
		TaskName:      request.Params.TaskName,
		Prompt:        request.Params.Message,
		WorkspaceID:   request.Params.WorkspaceID,
	})
	if err == nil {
		result.Outcome = protocol.AgentSpawnOutcomeStarted
		return result, nil
	}
	if started.Worker.Status == store.WorkerFailed {
		result.Outcome = protocol.AgentSpawnOutcomeFailed
		result.FailureCode = started.Worker.FailureCode
		if protocol.ValidateFailureCode(result.FailureCode) != nil {
			result.FailureCode = "worker_failed"
		}
		return result, nil
	}
	if errors.Is(err, workerhost.ErrWorkerInterrupted) {
		result.Outcome = protocol.AgentSpawnOutcomeStarted
		return result, nil
	}
	if errors.Is(err, store.ErrWorkerBusy) {
		result.Outcome = protocol.AgentSpawnOutcomeBusy
		return result, nil
	}
	if errors.Is(err, store.ErrWorkerTransition) {
		return result, nil
	}
	if errors.Is(err, workerhost.ErrMCPInjectionBlocked) {
		result.Outcome = protocol.AgentSpawnOutcomeFailed
		result.FailureCode = "mcp_injection_blocked"
		return result, nil
	}
	if errors.Is(err, store.ErrWorkerReservationConflict) {
		result.Outcome = protocol.AgentSpawnOutcomeFailed
		result.FailureCode = "reservation_conflict"
		return result, nil
	}
	if errors.Is(err, workerhost.ErrWorkerFailed) {
		result.Outcome = protocol.AgentSpawnOutcomeFailed
		result.FailureCode = "worker_failed"
		return result, nil
	}
	return protocol.SpawnWorkerResult{}, err
}
