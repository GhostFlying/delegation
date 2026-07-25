package cli

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/GhostFlying/delegation/internal/connector"
	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
)

type managedChangesArtifactHost interface {
	ArtifactChanges() <-chan struct{}
	ListPendingChangesPublications(context.Context) ([]store.ChangesArtifact, error)
	AcknowledgeChangesArtifact(
		context.Context,
		store.WorkerKey,
		string,
		uint64,
	) (store.WorkerFinalization, error)
}

type managedChangesArtifactSource struct {
	host         managedChangesArtifactHost
	workers      managedWorkerLifecycleHost
	controllerID string
	deviceID     string
}

func (s managedChangesArtifactSource) ArtifactChanges() <-chan struct{} {
	if s.host == nil {
		return nil
	}
	return s.host.ArtifactChanges()
}

func (s managedChangesArtifactSource) ListPendingChangesPublications(
	ctx context.Context,
) ([]connector.ChangesArtifactPublication, error) {
	if s.host == nil || s.workers == nil {
		return nil, errors.New("managed changes artifact host is unavailable")
	}
	artifacts, err := s.host.ListPendingChangesPublications(ctx)
	if err != nil {
		return nil, err
	}
	workers, err := s.workers.ListWorkers(ctx)
	if err != nil {
		return nil, err
	}
	workersByKey := make(map[store.WorkerKey]store.WorkerReservation, len(workers))
	for _, worker := range workers {
		if _, duplicate := workersByKey[worker.WorkerKey]; duplicate {
			return nil, errors.New("managed worker source returned a duplicate worker")
		}
		workersByKey[worker.WorkerKey] = worker
	}
	publications := make([]connector.ChangesArtifactPublication, 0, len(artifacts))
	for index, artifact := range artifacts {
		if err := artifact.Validate(); err != nil {
			return nil, fmt.Errorf("managed changes artifact %d: %w", index, err)
		}
		worker, found := workersByKey[artifact.WorkerKey]
		if !found {
			return nil, fmt.Errorf("managed changes artifact %d has no worker reservation", index)
		}
		if err := worker.Validate(); err != nil {
			return nil, fmt.Errorf("managed changes artifact %d worker: %w", index, err)
		}
		if artifact.ControllerID != s.controllerID || worker.ControllerID != s.controllerID ||
			worker.DeviceID != s.deviceID || worker.ParentAgentID == "" ||
			artifact.WorkspaceTargetDeviceID != worker.DeviceID ||
			artifact.State != store.ChangesPublishPending ||
			worker.Status != store.WorkerFinalizing || worker.ActiveTurnID != artifact.TurnID ||
			worker.WorkspaceID != artifact.WorkspaceID {
			return nil, errors.New("managed changes artifact is outside the configured peer authority")
		}
		params, err := protocolChangesArtifactParams(artifact)
		if err != nil {
			return nil, fmt.Errorf("managed changes artifact %d: %w", index, err)
		}
		publication := connector.ChangesArtifactPublication{
			Source: control.PrincipalIdentity{
				ControllerID: artifact.ControllerID,
				TreeID:       artifact.TreeID, AgentID: artifact.AgentID,
				ParentAgentID: worker.ParentAgentID, DeviceID: worker.DeviceID,
			},
			Params: params,
		}
		if err := publication.Source.Validate(); err != nil {
			return nil, fmt.Errorf("managed changes artifact %d source: %w", index, err)
		}
		publications = append(publications, publication)
	}
	return publications, nil
}

func (s managedChangesArtifactSource) AcknowledgeChangesArtifact(
	ctx context.Context,
	publication connector.ChangesArtifactPublication,
	brokerSequence uint64,
) error {
	if s.host == nil {
		return errors.New("managed changes artifact host is unavailable")
	}
	result := protocol.PublishChangesArtifactResult{
		ArtifactID: publication.Params.ArtifactID,
		Sequence:   brokerSequence,
	}
	if err := publication.Params.Validate(); err != nil {
		return fmt.Errorf("managed changes artifact acknowledgement: %w", err)
	}
	if err := result.Validate(); err != nil {
		return fmt.Errorf("managed changes artifact acknowledgement: %w", err)
	}
	if publication.Source.ControllerID != s.controllerID ||
		publication.Source.DeviceID != s.deviceID || publication.Source.ParentAgentID == "" {
		return errors.New("managed changes artifact acknowledgement is outside the configured peer authority")
	}
	key := store.WorkerKey{
		ControllerID: publication.Source.ControllerID,
		TreeID:       publication.Source.TreeID,
		AgentID:      publication.Source.AgentID,
	}
	finalization, err := s.host.AcknowledgeChangesArtifact(
		ctx, key, publication.Params.ArtifactID, brokerSequence,
	)
	if err != nil {
		return err
	}
	if finalization.Worker.WorkerKey != key || finalization.Worker.Status == store.WorkerFinalizing ||
		finalization.Artifact.WorkerKey != key ||
		finalization.Artifact.ArtifactID != publication.Params.ArtifactID ||
		finalization.Artifact.State != store.ChangesPublished ||
		finalization.Artifact.BrokerSequence != brokerSequence {
		return errors.New("managed changes artifact host returned a mismatched acknowledgement")
	}
	return nil
}

func protocolChangesArtifactParams(
	artifact store.ChangesArtifact,
) (protocol.PublishChangesArtifactParams, error) {
	var status protocol.ChangesArtifactStatus
	switch artifact.Status {
	case store.ChangesAvailable:
		status = protocol.ChangesArtifactAvailable
	case store.ChangesUnchanged:
		status = protocol.ChangesArtifactUnchanged
	case store.ChangesCaptureFailed:
		status = protocol.ChangesArtifactCaptureFailed
	default:
		return protocol.PublishChangesArtifactParams{}, fmt.Errorf(
			"unsupported changes artifact status %q", artifact.Status,
		)
	}
	parts := make([]protocol.WorkspaceArtifactDescriptor, 0, len(artifact.Parts))
	for _, part := range artifact.Parts {
		var kind protocol.WorkspaceArtifactKind
		switch part.Kind {
		case store.ChangesArtifactBundle:
			kind = protocol.WorkspaceArtifactBundle
		case store.ChangesArtifactOverlay:
			kind = protocol.WorkspaceArtifactOverlay
		default:
			return protocol.PublishChangesArtifactParams{}, fmt.Errorf(
				"unsupported changes artifact part %q", part.Kind,
			)
		}
		parts = append(parts, protocol.WorkspaceArtifactDescriptor{
			Kind: kind, Size: part.SizeBytes, SHA256: part.SHA256,
		})
	}
	slices.SortFunc(parts, func(left, right protocol.WorkspaceArtifactDescriptor) int {
		return cmp.Compare(left.Kind, right.Kind)
	})
	params := protocol.PublishChangesArtifactParams{
		ArtifactID: artifact.ArtifactID, TurnID: artifact.TurnID,
		WorkspaceID: artifact.WorkspaceID, Status: status,
		WorkspaceSourceDeviceID: artifact.WorkspaceSourceDeviceID,
		WorkspaceTargetDeviceID: artifact.WorkspaceTargetDeviceID,
		BaseHeadOID:             artifact.BaseHeadOID, BaseManifestHash: artifact.BaseManifestHash,
		BaseSnapshotHash: artifact.BaseSnapshotHash, ResultHeadOID: artifact.ResultHeadOID,
		ResultSnapshotHash: artifact.ResultSnapshotHash, ResultClean: artifact.ResultClean,
		Parts: parts, BaseWarnings: slices.Clone(artifact.BaseWarnings),
		ResultWarnings: slices.Clone(artifact.ResultWarnings), FailureCode: artifact.FailureCode,
	}
	if err := params.Validate(); err != nil {
		return protocol.PublishChangesArtifactParams{}, err
	}
	return params, nil
}
