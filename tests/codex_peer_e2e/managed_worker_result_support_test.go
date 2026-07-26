//go:build integration

package codex_peer_e2e

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/GhostFlying/delegation/internal/connector"
	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/localbridge"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/resultpackagefiles"
	"github.com/GhostFlying/delegation/internal/store"
	"github.com/GhostFlying/delegation/internal/workerhost"
)

// managedTestHost mirrors the production connector ownership boundary: one
// result-package manager serves the worker finalizer, source publisher, relay
// endpoints, and root-local availability lookup for the lifetime of the host.
type managedTestHost struct {
	*workerhost.Host
	resultPackages *resultpackagefiles.Manager
	state          *store.PeerStore
	controllerID   string
	deviceID       string
	workspaceRoot  string
	closeOnce      sync.Once
	closeErr       error
}

func (h *managedTestHost) Close(ctx context.Context) error {
	h.closeOnce.Do(func() {
		h.closeErr = errors.Join(h.Host.Close(ctx), h.resultPackages.Close())
	})
	return h.closeErr
}

func (h *managedTestHost) ResultPackageChanges() <-chan struct{} {
	return h.resultPackages.ResultPackageChanges()
}

func (h *managedTestHost) WorkerLifecycleChanges() <-chan struct{} {
	return h.Host.Changes()
}

func (h *managedTestHost) ListWorkerLifecycles(
	ctx context.Context,
) ([]protocol.WorkerLifecycleSnapshot, error) {
	workers, err := h.Host.ListWorkers(ctx)
	if err != nil {
		return nil, err
	}
	snapshots := make([]protocol.WorkerLifecycleSnapshot, 0, len(workers))
	for index, worker := range workers {
		if worker.ControllerID != h.controllerID || worker.DeviceID != h.deviceID {
			return nil, errors.New("managed worker lifecycle is outside the test peer identity")
		}
		phase, err := managedTestWorkerLifecyclePhase(worker.Status)
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
			CodexThreadID: worker.CodexThreadID, ActiveTurnID: worker.ActiveTurnID,
		}
		if err := snapshot.Validate(); err != nil {
			return nil, fmt.Errorf("managed worker %d lifecycle: %w", index, err)
		}
		snapshots = append(snapshots, snapshot)
	}
	return snapshots, nil
}

func managedTestWorkerLifecyclePhase(
	status store.WorkerStatus,
) (protocol.WorkerLifecyclePhase, error) {
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
	case store.WorkerFinalizing:
		return protocol.WorkerLifecycleFinalizing, nil
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

func (h *managedTestHost) ListPendingResultPackagePublications(
	ctx context.Context,
) ([]connector.ResultPackagePublication, error) {
	outboxes, err := h.resultPackages.ListPendingResultPublications(ctx)
	if err != nil {
		return nil, err
	}
	publications := make([]connector.ResultPackagePublication, 0, len(outboxes))
	for index, outbox := range outboxes {
		publication, err := h.resultPackagePublication(ctx, outbox)
		if err != nil {
			return nil, fmt.Errorf(
				"%w: pending result package %d: %w",
				connector.ErrPermanentResultPackagePublication,
				index,
				err,
			)
		}
		publications = append(publications, publication)
	}
	return publications, nil
}

func (h *managedTestHost) resultPackagePublication(
	ctx context.Context,
	outbox store.ResultOutbox,
) (connector.ResultPackagePublication, error) {
	if err := outbox.Validate(); err != nil {
		return connector.ResultPackagePublication{}, err
	}
	worker, err := h.state.GetWorker(ctx, outbox.WorkerKey)
	if err != nil {
		return connector.ResultPackagePublication{}, err
	}
	if err := worker.Validate(); err != nil {
		return connector.ResultPackagePublication{}, fmt.Errorf("worker: %w", err)
	}
	manifest := outbox.Manifest
	if outbox.ControllerID != h.controllerID || outbox.SourceDeviceID != h.deviceID ||
		outbox.State != store.ResultOutboxPublishPending || worker.WorkerKey != outbox.WorkerKey ||
		worker.DeviceID != h.deviceID || worker.ParentAgentID == "" ||
		worker.Status != store.WorkerFinalizing || worker.ActiveTurnID != manifest.TurnID ||
		worker.CodexThreadID != manifest.ManagedThreadID || worker.Revision != manifest.LifecycleRevision ||
		manifest.ControllerID != outbox.ControllerID || manifest.TreeID != outbox.TreeID ||
		manifest.SourceAgentID != outbox.AgentID || manifest.SourceDeviceID != outbox.SourceDeviceID ||
		manifest.PackageID != outbox.PackageID {
		return connector.ResultPackagePublication{}, errors.New(
			"result package is outside the configured peer authority",
		)
	}
	publication := connector.ResultPackagePublication{
		Source: control.PrincipalIdentity{
			ControllerID: outbox.ControllerID, TreeID: outbox.TreeID,
			AgentID: outbox.AgentID, ParentAgentID: worker.ParentAgentID,
			DeviceID: outbox.SourceDeviceID,
		},
		Params: protocol.PublishResultPackageParams{Metadata: outbox.Metadata},
	}
	if err := publication.Source.Validate(); err != nil {
		return connector.ResultPackagePublication{}, err
	}
	return publication, publication.Params.Validate()
}

func (h *managedTestHost) AcknowledgeResultPackageMetadata(
	ctx context.Context,
	publication connector.ResultPackagePublication,
) error {
	manifest, err := publication.Params.Metadata.DecodeManifest()
	if err != nil {
		return err
	}
	key := store.ResultOutboxKey{
		WorkerKey: store.WorkerKey{
			ControllerID: publication.Source.ControllerID,
			TreeID:       publication.Source.TreeID,
			AgentID:      publication.Source.AgentID,
		},
		SourceDeviceID: publication.Source.DeviceID,
		PackageID:      manifest.PackageID,
	}
	finalization, err := h.resultPackages.AcknowledgeResultPackageMetadata(
		ctx, key, publication.Params.Metadata,
	)
	if err != nil {
		if managedTestPermanentResultPackageStoreError(err) {
			return fmt.Errorf("%w: %w", connector.ErrPermanentResultPackagePublication, err)
		}
		return err
	}
	outbox := finalization.Outbox
	if outbox.ResultOutboxKey != key || outbox.State != store.ResultOutboxDeliveryPending ||
		!protocol.SameResultPackageMetadata(outbox.Metadata, publication.Params.Metadata) {
		return fmt.Errorf(
			"%w: result package runtime returned a mismatched metadata acknowledgement",
			connector.ErrPermanentResultPackagePublication,
		)
	}
	return nil
}

func managedTestPermanentResultPackageStoreError(err error) bool {
	return errors.Is(err, store.ErrNotFound) ||
		errors.Is(err, store.ErrResultPackageAuthority) ||
		errors.Is(err, store.ErrResultPackageConflict) ||
		errors.Is(err, store.ErrResultPackageQuota) ||
		errors.Is(err, store.ErrResultPackageTransition)
}

func (h *managedTestHost) ReadResultPackagePart(
	ctx context.Context,
	request connector.ResultPackageReadRequest,
) (protocol.ReadResultPackagePartResult, error) {
	return h.resultPackages.ReadResultPackagePart(ctx, resultpackagefiles.ReadRequest{
		TreeID: request.TreeID, Source: request.Source, Params: request.Params,
	})
}

func (h *managedTestHost) BeginResultPackage(
	ctx context.Context,
	request connector.ResultPackageBeginRequest,
) (protocol.BeginResultPackageResult, error) {
	return h.resultPackages.BeginResultPackage(ctx, resultpackagefiles.BeginRequest{
		TreeID: request.TreeID, Source: request.Source, Params: request.Params,
	})
}

func (h *managedTestHost) WriteResultPackagePart(
	ctx context.Context,
	request connector.ResultPackageWriteRequest,
) (protocol.WriteResultPackagePartResult, error) {
	return h.resultPackages.WriteResultPackagePart(ctx, resultpackagefiles.WriteRequest{
		TreeID: request.TreeID, Source: request.Source, Params: request.Params,
	})
}

func (h *managedTestHost) FinishResultPackage(
	ctx context.Context,
	request connector.ResultPackageFinishRequest,
) (protocol.FinishResultPackageResult, error) {
	return h.resultPackages.FinishResultPackage(ctx, resultpackagefiles.FinishRequest{
		TreeID: request.TreeID, Source: request.Source, Params: request.Params,
	})
}

func (h *managedTestHost) CancelResultPackage(
	ctx context.Context,
	request connector.ResultPackageCancelRequest,
) (protocol.CancelResultPackageResult, error) {
	return h.resultPackages.CancelResultPackage(ctx, resultpackagefiles.CancelRequest{
		TreeID: request.TreeID, Source: request.Source, Params: request.Params,
	})
}

func (h *managedTestHost) AcknowledgeResultPackage(
	ctx context.Context,
	request connector.ResultPackageAcknowledgeRequest,
) (protocol.AcknowledgeResultPackageResult, error) {
	return h.resultPackages.AcknowledgeResultPackage(ctx, resultpackagefiles.AcknowledgeRequest{
		TreeID: request.TreeID, Source: request.Source, Params: request.Params,
	})
}

func (h *managedTestHost) ReleaseResultPackage(
	ctx context.Context,
	request connector.ResultPackageReleaseRequest,
) (protocol.ReleaseResultPackageResult, error) {
	return h.resultPackages.ReleaseResultPackage(ctx, resultpackagefiles.ReleaseRequest{
		TreeID: request.TreeID, Source: request.Source, Params: request.Params,
	})
}

func (h *managedTestHost) CleanupResultPackages(ctx context.Context) error {
	return h.resultPackages.CleanupResultPackages(ctx)
}

func (h *managedTestHost) LookupResultPackageAvailability(
	ctx context.Context,
	lookup localbridge.ResultPackageAvailabilityLookup,
) (protocol.ResultPackageAvailability, error) {
	result, err := h.resultPackages.LookupResultPackageAvailability(
		ctx,
		resultpackagefiles.LookupAvailabilityRequest{Root: lookup.Root, Manifest: lookup.Manifest},
	)
	if err != nil {
		return "", err
	}
	switch result.Availability {
	case resultpackagefiles.PackageAvailable:
		return protocol.ResultPackageAvailable, nil
	case resultpackagefiles.PackageEvicted:
		return protocol.ResultPackageEvicted, nil
	default:
		return "", errors.New("unsupported local result package availability")
	}
}

var (
	_ connector.WorkerLifecycleSource               = (*managedTestHost)(nil)
	_ connector.ResultPackageSource                 = (*managedTestHost)(nil)
	_ connector.ResultPackageManager                = (*managedTestHost)(nil)
	_ localbridge.ResultPackageAvailabilityProvider = (*managedTestHost)(nil)
)
