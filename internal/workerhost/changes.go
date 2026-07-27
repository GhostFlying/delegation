package workerhost

import (
	"context"

	"github.com/GhostFlying/delegation/internal/store"
)

// Changes is a coalesced signal that a ListWorkers snapshot may have changed.
func (h *Host) Changes() <-chan struct{} {
	return h.changes
}

// ListWorkers returns the current persisted worker lifecycle snapshot.
func (h *Host) ListWorkers(ctx context.Context) ([]store.WorkerReservation, error) {
	return h.state.ListWorkers(ctx)
}

// WorkerRevision returns the latest peer-wide worker change sequence observed
// by this host. The maximum Revision in a ListWorkers snapshot is the matching
// durable cursor when at least one worker exists.
func (h *Host) WorkerRevision() uint64 {
	h.changesMu.Lock()
	defer h.changesMu.Unlock()
	return h.workerRevision
}

// StartupWorkerRevision returns the durable worker cursor observed before
// startup recovery could allocate new revisions. The connector uses it for the
// first broker comparison so recovery work cannot conceal a restored database.
func (h *Host) StartupWorkerRevision() uint64 {
	h.changesMu.Lock()
	defer h.changesMu.Unlock()
	return h.startupWorkerRevision
}

func (h *Host) signalWorkerChange() {
	select {
	case h.changes <- struct{}{}:
	default:
	}
}

func (h *Host) recordWorkerChange(
	worker store.WorkerReservation,
	err error,
) (store.WorkerReservation, error) {
	if err == nil && h.advanceWorkerRevision(worker.Revision) {
		h.signalWorkerChange()
	}
	return worker, err
}

func (h *Host) recordWorkerRecovery(
	workers []store.WorkerReservation,
	err error,
) ([]store.WorkerReservation, error) {
	changed := false
	if err == nil {
		for _, worker := range workers {
			changed = h.advanceWorkerRevision(worker.Revision) || changed
		}
	}
	if changed {
		h.signalWorkerChange()
	}
	return workers, err
}

func (h *Host) seedWorkerRevision(ctx context.Context) error {
	snapshot, err := h.state.ReadPeerStatusSnapshot(ctx, h.controllerID, h.deviceID)
	if err != nil {
		return err
	}
	h.advanceWorkerRevision(snapshot.WorkerRevision)
	h.changesMu.Lock()
	h.startupWorkerRevision = h.workerRevision
	h.changesMu.Unlock()
	return nil
}

func (h *Host) advanceWorkerRevision(revision uint64) bool {
	h.changesMu.Lock()
	defer h.changesMu.Unlock()
	if revision <= h.workerRevision {
		return false
	}
	h.workerRevision = revision
	return true
}
