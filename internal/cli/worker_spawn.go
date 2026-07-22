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
	host         *workerhost.Host
	controllerID string
	deviceID     string
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
		Status:    protocol.AgentSpawnPending,
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
	})
	if err == nil {
		result.Status = protocol.AgentSpawnStarted
		return result, nil
	}
	if started.Worker.Status == store.WorkerFailed {
		result.Status = protocol.AgentSpawnFailed
		result.FailureCode = started.Worker.FailureCode
		if protocol.ValidateFailureCode(result.FailureCode) != nil {
			result.FailureCode = "worker_failed"
		}
		return result, nil
	}
	if errors.Is(err, workerhost.ErrWorkerInterrupted) {
		result.Status = protocol.AgentSpawnStarted
		return result, nil
	}
	if errors.Is(err, store.ErrWorkerBusy) || errors.Is(err, store.ErrWorkerTransition) {
		return result, nil
	}
	if errors.Is(err, workerhost.ErrMCPInjectionBlocked) {
		result.Status = protocol.AgentSpawnFailed
		result.FailureCode = "mcp_injection_blocked"
		return result, nil
	}
	if errors.Is(err, store.ErrWorkerReservationConflict) {
		result.Status = protocol.AgentSpawnFailed
		result.FailureCode = "reservation_conflict"
		return result, nil
	}
	if errors.Is(err, workerhost.ErrWorkerFailed) {
		result.Status = protocol.AgentSpawnFailed
		result.FailureCode = "worker_failed"
		return result, nil
	}
	return protocol.SpawnWorkerResult{}, err
}
