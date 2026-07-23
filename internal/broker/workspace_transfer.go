package broker

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/protocol"
)

const workspaceTransferCleanupTimeout = 30 * time.Second

var (
	errInvalidSourceWorkspaceTransfer = errors.New("source returned invalid workspace transfer data")
	errInvalidTargetWorkspaceTransfer = errors.New("target returned invalid workspace transfer data")
	errSourceWorkspaceTransferFenced  = errors.New("source workspace transfer peer was fenced after cleanup failure")
	errTargetWorkspaceTransferFenced  = errors.New("target workspace transfer peer was fenced after cleanup failure")
)

func (s *session) relayWorkspaceTransfer(
	ctx context.Context,
	target *session,
	treeID string,
	source control.PrincipalIdentity,
	params protocol.SyncWorkspaceParams,
	manifest protocol.WorkspaceManifest,
	preparation protocol.PrepareWorkspaceResult,
) (result protocol.PrepareWorkspaceResult, returnErr error) {
	transferID, err := s.server.newID()
	if err != nil {
		return protocol.PrepareWorkspaceResult{}, fmt.Errorf("create workspace transfer ID: %w", err)
	}
	controlParams := protocol.WorkspaceTransferControlParams{
		WorkspaceID: params.SyncID, TransferID: transferID,
		SourceAgentID: source.AgentID, SourceDeviceID: source.DeviceID,
	}
	targetStarted := false
	targetFinished := false
	defer func() {
		cleanupBase := context.WithoutCancel(ctx)
		cleanupContext, cancel := context.WithTimeout(cleanupBase, workspaceTransferCleanupTimeout)
		if _, cleanupErr := s.callPeer(
			cleanupContext, protocol.MethodCancelWorkspaceTransfer, treeID, source, controlParams,
		); cleanupErr != nil {
			s.server.reportError(&internalError{operation: "clean source workspace transfer", err: cleanupErr})
			if result.Outcome != protocol.WorkspacePrepareReady {
				_ = s.connection.CloseNow()
			}
			returnErr = errors.Join(returnErr, errSourceWorkspaceTransferFenced)
		}
		cancel()
		if targetStarted && !targetFinished {
			cleanupContext, cancel = context.WithTimeout(cleanupBase, workspaceTransferCleanupTimeout)
			if _, cleanupErr := target.callPeer(
				cleanupContext, protocol.MethodCancelWorkspaceTransfer, treeID, source, controlParams,
			); cleanupErr != nil {
				s.server.reportError(&internalError{operation: "cancel target workspace transfer", err: cleanupErr})
				_ = target.connection.CloseNow()
				returnErr = errors.Join(returnErr, errTargetWorkspaceTransferFenced)
			}
			cancel()
		}
	}()
	createPayload, err := s.callPeer(
		ctx, protocol.MethodCreateWorkspaceTransfer, treeID, source,
		protocol.CreateWorkspaceTransferParams{
			TransferID: transferID, WorkspaceID: params.SyncID,
			GitURL: params.GitURL, SourcePath: params.SourcePath, Manifest: manifest,
			BasisOIDs:      preparation.BasisOIDs,
			BundleRequired: preparation.BundleRequired, OverlayRequired: preparation.OverlayRequired,
		},
	)
	if err != nil {
		return protocol.PrepareWorkspaceResult{}, fmt.Errorf("create source workspace transfer: %w", err)
	}
	created, err := protocol.DecodePayload[protocol.CreateWorkspaceTransferResult](createPayload)
	if err != nil || created.Validate() != nil || created.Transfer.TransferID != transferID ||
		created.Transfer.WorkspaceID != params.SyncID {
		return protocol.PrepareWorkspaceResult{}, fmt.Errorf(
			"%w: transfer metadata is invalid", errInvalidSourceWorkspaceTransfer,
		)
	}
	if err := validateWorkspaceTransferPlan(created.Transfer, manifest, preparation); err != nil {
		return protocol.PrepareWorkspaceResult{}, fmt.Errorf("%w: %v", errInvalidSourceWorkspaceTransfer, err)
	}
	targetStarted = true
	beginPayload, err := target.callPeer(
		ctx, protocol.MethodBeginWorkspaceTransfer, treeID, source,
		protocol.BeginWorkspaceTransferParams{
			SourceAgentID: source.AgentID, SourceDeviceID: source.DeviceID,
			Manifest: manifest, Transfer: created.Transfer,
		},
	)
	if err != nil {
		return protocol.PrepareWorkspaceResult{}, fmt.Errorf("begin target workspace transfer: %w", err)
	}
	begun, err := protocol.DecodePayload[protocol.BeginWorkspaceTransferResult](beginPayload)
	if err != nil || begun.Validate() != nil || begun.TransferID != transferID {
		return protocol.PrepareWorkspaceResult{}, fmt.Errorf(
			"%w: transfer acknowledgement is invalid", errInvalidTargetWorkspaceTransfer,
		)
	}
	for _, artifact := range created.Transfer.Artifacts {
		if err := s.relayWorkspaceArtifact(
			ctx, target, treeID, source, params.SyncID, transferID, artifact,
		); err != nil {
			return protocol.PrepareWorkspaceResult{}, err
		}
	}
	finishPayload, err := target.callPeer(
		ctx, protocol.MethodFinishWorkspaceTransfer, treeID, source, controlParams,
	)
	if err != nil {
		return protocol.PrepareWorkspaceResult{}, fmt.Errorf("finish target workspace transfer: %w", err)
	}
	finished, err := protocol.DecodePayload[protocol.FinishWorkspaceTransferResult](finishPayload)
	if err != nil || finished.Validate() != nil || finished.Workspace.WorkspaceID != params.SyncID ||
		finished.Workspace.ManifestHash != created.Transfer.ManifestHash ||
		finished.Workspace.Strategy != created.Transfer.Strategy ||
		!slices.Equal(finished.Workspace.Warnings, created.Transfer.Warnings) {
		return protocol.PrepareWorkspaceResult{}, fmt.Errorf(
			"%w: completed workspace metadata is invalid", errInvalidTargetWorkspaceTransfer,
		)
	}
	targetFinished = true
	return finished.Workspace, nil
}

func (s *session) relayWorkspaceArtifact(
	ctx context.Context,
	target *session,
	treeID string,
	source control.PrincipalIdentity,
	workspaceID, transferID string,
	artifact protocol.WorkspaceArtifactDescriptor,
) error {
	for offset := int64(0); offset < artifact.Size; {
		limit := min(int64(protocol.WorkspaceArtifactChunkBytes), artifact.Size-offset)
		readPayload, err := s.callPeer(
			ctx, protocol.MethodReadWorkspaceArtifact, treeID, source,
			protocol.ReadWorkspaceArtifactParams{
				TransferID: transferID, Kind: artifact.Kind, Offset: offset, Limit: int(limit),
			},
		)
		if err != nil {
			return fmt.Errorf("read source workspace artifact: %w", err)
		}
		read, err := protocol.DecodePayload[protocol.ReadWorkspaceArtifactResult](readPayload)
		if err != nil || read.Validate() != nil || read.TransferID != transferID ||
			read.Kind != artifact.Kind || read.Offset != offset || len(read.Data) != int(limit) ||
			read.NextOffset != offset+limit {
			return fmt.Errorf("%w: artifact chunk does not match requested bounds", errInvalidSourceWorkspaceTransfer)
		}
		writePayload, err := target.callPeer(
			ctx, protocol.MethodWriteWorkspaceArtifact, treeID, source,
			protocol.WriteWorkspaceArtifactParams{
				WorkspaceID: workspaceID, TransferID: transferID,
				Kind: artifact.Kind, Offset: offset, Data: read.Data,
			},
		)
		if err != nil {
			return fmt.Errorf("write target workspace artifact: %w", err)
		}
		written, err := protocol.DecodePayload[protocol.WriteWorkspaceArtifactResult](writePayload)
		if err != nil || written.Validate() != nil || written.TransferID != transferID ||
			written.NextOffset != read.NextOffset {
			return fmt.Errorf("%w: artifact write acknowledgement does not match the chunk", errInvalidTargetWorkspaceTransfer)
		}
		offset = read.NextOffset
	}
	return nil
}

func validateWorkspaceTransferPlan(
	transfer protocol.WorkspaceTransferManifest,
	manifest protocol.WorkspaceManifest,
	preparation protocol.PrepareWorkspaceResult,
) error {
	manifestHash, err := protocol.WorkspaceManifestHash(manifest)
	if err != nil || transfer.ManifestHash != manifestHash {
		return errors.New("source workspace transfer does not match the pinned manifest")
	}
	hasBundle := slices.ContainsFunc(transfer.Artifacts, func(artifact protocol.WorkspaceArtifactDescriptor) bool {
		return artifact.Kind == protocol.WorkspaceArtifactBundle
	})
	hasOverlay := slices.ContainsFunc(transfer.Artifacts, func(artifact protocol.WorkspaceArtifactDescriptor) bool {
		return artifact.Kind == protocol.WorkspaceArtifactOverlay
	})
	if hasBundle != preparation.BundleRequired || hasOverlay != preparation.OverlayRequired {
		return errors.New("source workspace transfer does not match target requirements")
	}
	expectedWarnings, err := protocol.WorkspaceWarningsForStrategy(manifest.Warnings, transfer.Strategy)
	if err != nil || !slices.Equal(expectedWarnings, transfer.Warnings) {
		return errors.New("source workspace transfer warnings do not match its strategy")
	}
	return nil
}
