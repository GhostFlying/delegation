package broker

import (
	"context"
	"errors"

	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
)

func (s *session) handleSyncWorkerLifecycle(
	ctx context.Context,
	request protocol.Envelope,
) error {
	if request.TreeID != "" || request.Source != nil {
		return s.writeError(
			ctx,
			request,
			protocol.ErrorInvalidRequest,
			"worker lifecycle sync must not contain a principal",
		)
	}
	params, err := protocol.DecodePayload[protocol.SyncWorkerLifecycleParams](request.Payload)
	if err != nil || params.Validate() != nil {
		return s.writeError(
			ctx, request, protocol.ErrorInvalidParams, "invalid worker lifecycle page",
		)
	}
	if params.BaseRevision != s.workerApplied.Load() || params.ThroughRevision < s.workerHigh.Load() {
		return s.writeError(
			ctx,
			request,
			protocol.ErrorConflict,
			"worker lifecycle cursor conflicts with broker state",
		)
	}
	result, err := s.server.registry.ApplyWorkerLifecyclePage(
		ctx,
		store.WorkerLifecyclePageApply{
			Session:    s.workerLifecycleSession(),
			Page:       params,
			ObservedAt: s.server.now(),
		},
	)
	if err != nil {
		return s.handleWorkerLifecycleStoreError(ctx, request, err)
	}
	s.workerApplied.Store(result.AppliedRevision)
	for {
		current := s.workerHigh.Load()
		if params.ThroughRevision <= current ||
			s.workerHigh.CompareAndSwap(current, params.ThroughRevision) {
			break
		}
	}
	ready := result.AppliedRevision >= s.workerInitial
	seenTrees := make(map[string]struct{}, len(params.Workers))
	for _, worker := range params.Workers {
		if _, seen := seenTrees[worker.TreeID]; seen {
			continue
		}
		seenTrees[worker.TreeID] = struct{}{}
		s.server.lifecycleNotifier.notify(treeKey{
			controllerID: s.server.controllerID,
			treeID:       worker.TreeID,
		})
	}
	if err := s.writeResult(ctx, request, result); err != nil {
		return err
	}
	if ready {
		s.workerReady.Store(true)
	}
	return nil
}

func (s *session) handleWorkerLifecycleStoreError(
	ctx context.Context,
	request protocol.Envelope,
	err error,
) error {
	switch {
	case errors.Is(err, store.ErrWorkerLifecycleStaleBase),
		errors.Is(err, store.ErrWorkerLifecycleRevisionNotForward),
		errors.Is(err, store.ErrConflict):
		return s.writeError(
			ctx,
			request,
			protocol.ErrorConflict,
			"worker lifecycle page conflicts with broker state",
		)
	case errors.Is(err, store.ErrWorkerLifecycleSessionFenced),
		errors.Is(err, store.ErrAuthorizationDenied):
		_ = s.writeError(
			ctx, request, protocol.ErrorForbidden, "worker lifecycle session is fenced",
		)
		return err
	case isContextError(err):
		return err
	default:
		_ = s.writeError(ctx, request, protocol.ErrorUnavailable, "broker unavailable")
		return &internalError{operation: "apply worker lifecycle page", err: err}
	}
}

func (s *session) workerLifecycleSession() store.WorkerLifecycleSession {
	return store.WorkerLifecycleSession{
		ControllerID:  s.server.controllerID,
		DeviceID:      s.deviceID,
		ConnectionID:  s.connectionID,
		LeaseRevision: s.revision.Load(),
	}
}
