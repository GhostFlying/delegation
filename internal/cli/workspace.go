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
