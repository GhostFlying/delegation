package broker

import (
	"context"
	"errors"

	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
)

func (s *session) handleUpdateWorkerReadiness(
	ctx context.Context, request protocol.Envelope,
) error {
	if request.TreeID != "" || request.Source != nil {
		return s.writeError(ctx, request, protocol.ErrorInvalidRequest,
			"worker readiness update must not contain a principal")
	}
	params, err := protocol.DecodePayload[protocol.UpdateWorkerReadinessParams](request.Payload)
	if err != nil || params.Validate() != nil {
		return s.writeError(ctx, request, protocol.ErrorInvalidParams, "invalid worker readiness update")
	}
	persisted, err := s.server.registry.PutWorkerReadiness(
		ctx, s.server.controllerID, s.deviceID, params.Readiness,
	)
	if err != nil {
		if errors.Is(err, store.ErrWorkerReadinessStale) {
			return s.writeError(ctx, request, protocol.ErrorConflict, "worker readiness update is stale")
		}
		if isContextError(err) {
			return err
		}
		_ = s.writeError(ctx, request, protocol.ErrorUnavailable, "broker unavailable")
		return &internalError{operation: "persist worker readiness update", err: err}
	}
	s.server.mu.Lock()
	if s.server.currentConnectionLocked(s.deviceID) == s {
		ready := persisted.IsReady()
		s.workerReadiness = persisted
		if s.workerReady.Load() != ready {
			s.workerReady.Store(ready)
			s.server.statusGeneration++
		}
	}
	s.server.mu.Unlock()
	return s.writeResult(ctx, request, protocol.UpdateWorkerReadinessResult{Readiness: persisted})
}
