package cli

import (
	"context"
	"errors"
	"fmt"

	"github.com/GhostFlying/delegation/internal/connector"
	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/localbridge"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/resultpackagefiles"
	"github.com/GhostFlying/delegation/internal/store"
)

type managedResultPackageManager interface {
	ReadResultPackagePart(context.Context, resultpackagefiles.ReadRequest) (protocol.ReadResultPackagePartResult, error)
	BeginResultPackage(context.Context, resultpackagefiles.BeginRequest) (protocol.BeginResultPackageResult, error)
	WriteResultPackagePart(context.Context, resultpackagefiles.WriteRequest) (protocol.WriteResultPackagePartResult, error)
	FinishResultPackage(context.Context, resultpackagefiles.FinishRequest) (protocol.FinishResultPackageResult, error)
	CancelResultPackage(context.Context, resultpackagefiles.CancelRequest) (protocol.CancelResultPackageResult, error)
	AcknowledgeResultPackage(context.Context, resultpackagefiles.AcknowledgeRequest) (protocol.AcknowledgeResultPackageResult, error)
	ReleaseResultPackage(context.Context, resultpackagefiles.ReleaseRequest) (protocol.ReleaseResultPackageResult, error)
	CleanupResultPackages(context.Context) error
}

type managedResultPackagePublisher interface {
	ResultPackageChanges() <-chan struct{}
	ListPendingResultPublications(context.Context) ([]store.ResultOutbox, error)
	AcknowledgeResultPackageMetadata(
		context.Context,
		store.ResultOutboxKey,
		protocol.ResultPackageMetadata,
	) (store.WorkerResultFinalization, error)
}

type managedResultPackageSource struct {
	packages     managedResultPackagePublisher
	state        managedWorkerState
	controllerID string
	deviceID     string
}

type resultPackageAvailabilityLookup interface {
	LookupResultPackageAvailability(
		context.Context,
		resultpackagefiles.LookupAvailabilityRequest,
	) (resultpackagefiles.LookupAvailabilityResult, error)
}

type localResultPackageAvailabilityProvider struct {
	manager resultPackageAvailabilityLookup
}

func (p localResultPackageAvailabilityProvider) LookupResultPackageAvailability(
	ctx context.Context,
	lookup localbridge.ResultPackageAvailabilityLookup,
) (protocol.ResultPackageAvailability, error) {
	result, err := p.manager.LookupResultPackageAvailability(
		ctx,
		resultpackagefiles.LookupAvailabilityRequest{
			Root: lookup.Root, Manifest: lookup.Manifest,
		},
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

var _ connector.ResultPackageManager = managedWorkerSpawner{}
var _ connector.ResultPackageSource = managedResultPackageSource{}

func (s managedResultPackageSource) ResultPackageChanges() <-chan struct{} {
	if s.packages == nil {
		return nil
	}
	return s.packages.ResultPackageChanges()
}

func (s managedResultPackageSource) ListPendingResultPackagePublications(
	ctx context.Context,
) ([]connector.ResultPackagePublication, error) {
	if s.packages == nil || s.state == nil {
		return nil, errors.New("result package runtime is unavailable")
	}
	outboxes, err := s.packages.ListPendingResultPublications(ctx)
	if err != nil {
		return nil, err
	}
	publications := make([]connector.ResultPackagePublication, 0, len(outboxes))
	for index, outbox := range outboxes {
		publication, err := s.resultPackagePublication(ctx, outbox)
		if err != nil {
			return nil, permanentResultPackageSourceError(fmt.Errorf(
				"pending result package %d: %w", index, err,
			))
		}
		publications = append(publications, publication)
	}
	return publications, nil
}

func (s managedResultPackageSource) AcknowledgeResultPackageMetadata(
	ctx context.Context,
	publication connector.ResultPackagePublication,
) error {
	if s.packages == nil || s.state == nil {
		return errors.New("result package runtime is unavailable")
	}
	manifest, err := publication.Params.Metadata.DecodeManifest()
	if err != nil {
		return permanentResultPackageSourceError(err)
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
	finalization, err := s.packages.AcknowledgeResultPackageMetadata(
		ctx, key, publication.Params.Metadata,
	)
	if err != nil {
		if permanentResultPackageStoreError(err) {
			return permanentResultPackageSourceError(err)
		}
		return err
	}
	outbox := finalization.Outbox
	if outbox.ResultOutboxKey != key || outbox.State != store.ResultOutboxDeliveryPending ||
		!protocol.SameResultPackageMetadata(outbox.Metadata, publication.Params.Metadata) {
		return permanentResultPackageSourceError(errors.New(
			"result package runtime returned a mismatched metadata acknowledgement",
		))
	}
	return nil
}

func permanentResultPackageSourceError(err error) error {
	return fmt.Errorf("%w: %w", connector.ErrPermanentResultPackagePublication, err)
}

func permanentResultPackageStoreError(err error) bool {
	return errors.Is(err, store.ErrNotFound) ||
		errors.Is(err, store.ErrResultPackageAuthority) ||
		errors.Is(err, store.ErrResultPackageConflict) ||
		errors.Is(err, store.ErrResultPackageQuota) ||
		errors.Is(err, store.ErrResultPackageTransition)
}

func (s managedResultPackageSource) resultPackagePublication(
	ctx context.Context,
	outbox store.ResultOutbox,
) (connector.ResultPackagePublication, error) {
	if err := outbox.Validate(); err != nil {
		return connector.ResultPackagePublication{}, err
	}
	worker, err := s.state.GetWorker(ctx, outbox.WorkerKey)
	if err != nil {
		return connector.ResultPackagePublication{}, err
	}
	if err := worker.Validate(); err != nil {
		return connector.ResultPackagePublication{}, fmt.Errorf("worker: %w", err)
	}
	manifest := outbox.Manifest
	if outbox.ControllerID != s.controllerID || outbox.SourceDeviceID != s.deviceID ||
		outbox.State != store.ResultOutboxPublishPending || worker.WorkerKey != outbox.WorkerKey ||
		worker.DeviceID != s.deviceID || worker.ParentAgentID == "" ||
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
			ControllerID:  outbox.ControllerID,
			TreeID:        outbox.TreeID,
			AgentID:       outbox.AgentID,
			ParentAgentID: worker.ParentAgentID,
			DeviceID:      outbox.SourceDeviceID,
		},
		Params: protocol.PublishResultPackageParams{Metadata: outbox.Metadata},
	}
	if err := publication.Source.Validate(); err != nil {
		return connector.ResultPackagePublication{}, err
	}
	return publication, publication.Params.Validate()
}

func (s managedWorkerSpawner) ReadResultPackagePart(
	ctx context.Context,
	request connector.ResultPackageReadRequest,
) (protocol.ReadResultPackagePartResult, error) {
	if s.resultPackages == nil {
		return protocol.ReadResultPackagePartResult{}, errors.New("result package runtime is unavailable")
	}
	return s.resultPackages.ReadResultPackagePart(ctx, resultpackagefiles.ReadRequest{
		TreeID: request.TreeID, Source: request.Source, Params: request.Params,
	})
}

func (s managedWorkerSpawner) BeginResultPackage(
	ctx context.Context,
	request connector.ResultPackageBeginRequest,
) (protocol.BeginResultPackageResult, error) {
	if s.resultPackages == nil {
		return protocol.BeginResultPackageResult{}, errors.New("result package runtime is unavailable")
	}
	return s.resultPackages.BeginResultPackage(ctx, resultpackagefiles.BeginRequest{
		TreeID: request.TreeID, Source: request.Source, Params: request.Params,
	})
}

func (s managedWorkerSpawner) WriteResultPackagePart(
	ctx context.Context,
	request connector.ResultPackageWriteRequest,
) (protocol.WriteResultPackagePartResult, error) {
	if s.resultPackages == nil {
		return protocol.WriteResultPackagePartResult{}, errors.New("result package runtime is unavailable")
	}
	return s.resultPackages.WriteResultPackagePart(ctx, resultpackagefiles.WriteRequest{
		TreeID: request.TreeID, Source: request.Source, Params: request.Params,
	})
}

func (s managedWorkerSpawner) FinishResultPackage(
	ctx context.Context,
	request connector.ResultPackageFinishRequest,
) (protocol.FinishResultPackageResult, error) {
	if s.resultPackages == nil {
		return protocol.FinishResultPackageResult{}, errors.New("result package runtime is unavailable")
	}
	return s.resultPackages.FinishResultPackage(ctx, resultpackagefiles.FinishRequest{
		TreeID: request.TreeID, Source: request.Source, Params: request.Params,
	})
}

func (s managedWorkerSpawner) CancelResultPackage(
	ctx context.Context,
	request connector.ResultPackageCancelRequest,
) (protocol.CancelResultPackageResult, error) {
	if s.resultPackages == nil {
		return protocol.CancelResultPackageResult{}, errors.New("result package runtime is unavailable")
	}
	return s.resultPackages.CancelResultPackage(ctx, resultpackagefiles.CancelRequest{
		TreeID: request.TreeID, Source: request.Source, Params: request.Params,
	})
}

func (s managedWorkerSpawner) AcknowledgeResultPackage(
	ctx context.Context,
	request connector.ResultPackageAcknowledgeRequest,
) (protocol.AcknowledgeResultPackageResult, error) {
	if s.resultPackages == nil {
		return protocol.AcknowledgeResultPackageResult{}, errors.New("result package runtime is unavailable")
	}
	return s.resultPackages.AcknowledgeResultPackage(ctx, resultpackagefiles.AcknowledgeRequest{
		TreeID: request.TreeID, Source: request.Source, Params: request.Params,
	})
}

func (s managedWorkerSpawner) ReleaseResultPackage(
	ctx context.Context,
	request connector.ResultPackageReleaseRequest,
) (protocol.ReleaseResultPackageResult, error) {
	if s.resultPackages == nil {
		return protocol.ReleaseResultPackageResult{}, errors.New("result package runtime is unavailable")
	}
	return s.resultPackages.ReleaseResultPackage(ctx, resultpackagefiles.ReleaseRequest{
		TreeID: request.TreeID, Source: request.Source, Params: request.Params,
	})
}

func (s managedWorkerSpawner) CleanupResultPackages(ctx context.Context) error {
	if s.resultPackages == nil {
		return errors.New("result package runtime is unavailable")
	}
	return s.resultPackages.CleanupResultPackages(ctx)
}
