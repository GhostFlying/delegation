package cli

import (
	"context"
	"errors"

	"github.com/GhostFlying/delegation/internal/connector"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/workerhost"
)

func (s managedWorkerSpawner) InspectWorkspace(
	ctx context.Context,
	request connector.WorkspaceInspectRequest,
) (protocol.InspectWorkspaceResult, error) {
	if s.host == nil {
		return protocol.InspectWorkspaceResult{}, errors.New("managed workspace runtime is unavailable")
	}
	return s.host.InspectWorkspace(ctx, workerhost.WorkspaceInspectRequest{
		TreeID: request.TreeID, Source: request.Source, Params: request.Params,
	})
}

func (s managedWorkerSpawner) PrepareWorkspace(
	ctx context.Context,
	request connector.WorkspacePrepareRequest,
) (protocol.PrepareWorkspaceResult, error) {
	if s.host == nil {
		return protocol.PrepareWorkspaceResult{}, errors.New("managed workspace runtime is unavailable")
	}
	return s.host.PrepareWorkspace(ctx, workerhost.WorkspacePrepareRequest{
		TreeID: request.TreeID, Source: request.Source, Params: request.Params,
	})
}

func (s managedWorkerSpawner) CreateWorkspaceTransfer(
	ctx context.Context,
	request connector.WorkspaceCreateTransferRequest,
) (protocol.CreateWorkspaceTransferResult, error) {
	if s.host == nil {
		return protocol.CreateWorkspaceTransferResult{}, errors.New("managed workspace runtime is unavailable")
	}
	return s.host.CreateWorkspaceTransfer(ctx, workerhost.WorkspaceCreateTransferRequest{
		TreeID: request.TreeID, Source: request.Source, Params: request.Params,
	})
}

func (s managedWorkerSpawner) ReadWorkspaceArtifact(
	ctx context.Context,
	request connector.WorkspaceReadArtifactRequest,
) (protocol.ReadWorkspaceArtifactResult, error) {
	if s.host == nil {
		return protocol.ReadWorkspaceArtifactResult{}, errors.New("managed workspace runtime is unavailable")
	}
	return s.host.ReadWorkspaceArtifact(ctx, workerhost.WorkspaceReadArtifactRequest{
		TreeID: request.TreeID, Source: request.Source, Params: request.Params,
	})
}

func (s managedWorkerSpawner) BeginWorkspaceTransfer(
	ctx context.Context,
	request connector.WorkspaceBeginTransferRequest,
) (protocol.BeginWorkspaceTransferResult, error) {
	if s.host == nil {
		return protocol.BeginWorkspaceTransferResult{}, errors.New("managed workspace runtime is unavailable")
	}
	return s.host.BeginWorkspaceTransfer(ctx, workerhost.WorkspaceBeginTransferRequest{
		TreeID: request.TreeID, Source: request.Source, Params: request.Params,
	})
}

func (s managedWorkerSpawner) WriteWorkspaceArtifact(
	ctx context.Context,
	request connector.WorkspaceWriteArtifactRequest,
) (protocol.WriteWorkspaceArtifactResult, error) {
	if s.host == nil {
		return protocol.WriteWorkspaceArtifactResult{}, errors.New("managed workspace runtime is unavailable")
	}
	return s.host.WriteWorkspaceArtifact(ctx, workerhost.WorkspaceWriteArtifactRequest{
		TreeID: request.TreeID, Source: request.Source, Params: request.Params,
	})
}

func (s managedWorkerSpawner) FinishWorkspaceTransfer(
	ctx context.Context,
	request connector.WorkspaceTransferControlRequest,
) (protocol.FinishWorkspaceTransferResult, error) {
	if s.host == nil {
		return protocol.FinishWorkspaceTransferResult{}, errors.New("managed workspace runtime is unavailable")
	}
	return s.host.FinishWorkspaceTransfer(ctx, workerhost.WorkspaceTransferControlRequest{
		TreeID: request.TreeID, Source: request.Source, Params: request.Params,
	})
}

func (s managedWorkerSpawner) CancelWorkspaceTransfer(
	ctx context.Context,
	request connector.WorkspaceTransferControlRequest,
) (protocol.CancelWorkspaceTransferResult, error) {
	if s.host == nil {
		return protocol.CancelWorkspaceTransferResult{}, errors.New("managed workspace runtime is unavailable")
	}
	return s.host.CancelWorkspaceTransfer(ctx, workerhost.WorkspaceTransferControlRequest{
		TreeID: request.TreeID, Source: request.Source, Params: request.Params,
	})
}

func (s managedWorkerSpawner) CleanupWorkspaceTransfers(ctx context.Context) error {
	if s.host == nil {
		return errors.New("managed workspace runtime is unavailable")
	}
	return s.host.CleanupWorkspaceTransfers(ctx)
}
