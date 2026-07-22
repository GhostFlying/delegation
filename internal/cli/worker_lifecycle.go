package cli

import (
	"context"
	"errors"
	"fmt"

	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
)

type managedWorkerLifecycleHost interface {
	Changes() <-chan struct{}
	ListWorkers(context.Context) ([]store.WorkerReservation, error)
	WorkerRevision() uint64
}

type managedWorkerLifecycleSource struct {
	host         managedWorkerLifecycleHost
	controllerID string
	deviceID     string
}

func (s managedWorkerLifecycleSource) WorkerRevision() uint64 {
	if s.host == nil {
		return 0
	}
	return s.host.WorkerRevision()
}

func (s managedWorkerLifecycleSource) WorkerLifecycleChanges() <-chan struct{} {
	if s.host == nil {
		return nil
	}
	return s.host.Changes()
}

func (s managedWorkerLifecycleSource) ListWorkerLifecycles(
	ctx context.Context,
) ([]protocol.WorkerLifecycleSnapshot, error) {
	if s.host == nil {
		return nil, errors.New("managed worker lifecycle host is unavailable")
	}
	workers, err := s.host.ListWorkers(ctx)
	if err != nil {
		return nil, err
	}
	snapshots := make([]protocol.WorkerLifecycleSnapshot, 0, len(workers))
	for index, worker := range workers {
		if worker.ControllerID != s.controllerID || worker.DeviceID != s.deviceID {
			return nil, errors.New("managed worker lifecycle is outside the configured peer identity")
		}
		phase, err := workerLifecyclePhase(worker.Status)
		if err != nil {
			return nil, fmt.Errorf("managed worker %d: %w", index, err)
		}
		failureCode := ""
		if worker.Status == store.WorkerFailed {
			failureCode = worker.FailureCode
		}
		snapshot := protocol.WorkerLifecycleSnapshot{
			TreeID: worker.TreeID, AgentID: worker.AgentID, Revision: worker.Revision,
			Phase: phase, FailureCode: failureCode,
		}
		if err := snapshot.Validate(); err != nil {
			return nil, fmt.Errorf("managed worker %d lifecycle: %w", index, err)
		}
		snapshots = append(snapshots, snapshot)
	}
	return snapshots, nil
}

func workerLifecyclePhase(status store.WorkerStatus) (protocol.WorkerLifecyclePhase, error) {
	switch status {
	case store.WorkerReserved:
		return protocol.WorkerLifecycleReserved, nil
	case store.WorkerPending:
		return protocol.WorkerLifecyclePending, nil
	case store.WorkerStarting:
		return protocol.WorkerLifecycleStarting, nil
	case store.WorkerPreflight:
		return protocol.WorkerLifecyclePreflight, nil
	case store.WorkerReady:
		return protocol.WorkerLifecycleReady, nil
	case store.WorkerRunning:
		return protocol.WorkerLifecycleRunning, nil
	case store.WorkerIdle:
		return protocol.WorkerLifecycleIdle, nil
	case store.WorkerInterrupted:
		return protocol.WorkerLifecycleInterrupted, nil
	case store.WorkerFailed:
		return protocol.WorkerLifecycleFailed, nil
	default:
		return "", fmt.Errorf("unsupported worker lifecycle status %q", status)
	}
}
