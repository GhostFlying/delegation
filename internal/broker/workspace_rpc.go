package broker

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"

	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
)

func (s *session) startWorkspaceSync(
	responseContext context.Context,
	sessionContext context.Context,
	request protocol.Envelope,
) error {
	select {
	case s.asyncSem <- struct{}{}:
	default:
		return s.writeError(responseContext, request, protocol.ErrorUnavailable, "too many pending workspace synchronizations")
	}
	operationContext, cancel := context.WithTimeout(sessionContext, workspaceSyncRequestTimeout)
	s.asyncMu.Lock()
	if _, exists := s.asyncCancels[request.RequestID]; exists {
		s.asyncMu.Unlock()
		cancel()
		<-s.asyncSem
		return errors.New("duplicate asynchronous requestId")
	}
	s.asyncCancels[request.RequestID] = cancel
	s.asyncMu.Unlock()
	s.async.Add(1)
	go func() {
		defer s.async.Done()
		defer func() {
			s.asyncMu.Lock()
			delete(s.asyncCancels, request.RequestID)
			s.asyncMu.Unlock()
			cancel()
			<-s.asyncSem
		}()
		if err := s.handleSyncWorkspace(operationContext, request); err != nil && !isContextError(err) {
			s.server.reportError(&internalError{operation: "synchronize Git workspace", err: err})
		}
	}()
	return nil
}

func (s *session) handleSyncWorkspace(ctx context.Context, request protocol.Envelope) error {
	if request.TreeID == "" || request.Source == nil || request.Source.ParentAgentID != "" {
		return s.writeError(ctx, request, protocol.ErrorInvalidRequest, "workspace sync requires a root principal")
	}
	if request.Source.DeviceID != s.deviceID {
		return s.writeError(ctx, request, protocol.ErrorForbidden, "workspace sync access denied")
	}
	params, err := protocol.DecodePayload[protocol.SyncWorkspaceParams](request.Payload)
	if err != nil || params.Validate() != nil {
		return s.writeError(ctx, request, protocol.ErrorInvalidParams, "invalid workspace sync payload")
	}
	key := store.WorkspaceSyncKey{
		ControllerID: request.ControllerID, TreeID: request.TreeID,
		SourceAgentID: request.Source.AgentID, SyncID: params.SyncID,
	}
	release, err := s.server.workspaceSyncs.acquire(ctx, key)
	if err != nil {
		return err
	}
	defer release()
	ctx = withCanceledWorkspacePeerCallDrain(ctx)
	receipt, err := s.server.registry.BeginWorkspaceSync(ctx, store.WorkspaceSyncIntent{
		Source: *request.Source, SyncID: params.SyncID,
		TargetDeviceID: params.TargetDeviceID, GitURL: params.GitURL,
		SourcePathHash: sha256.Sum256([]byte(params.SourcePath)),
	}, s.server.now())
	if err != nil {
		return s.handleWorkspaceStoreError(ctx, request, err)
	}
	if receipt.Status == store.WorkspaceSyncPrepared {
		summary := receipt.Summary()
		return s.writeResult(ctx, request, protocol.SyncWorkspaceResult{
			Workspace: &summary, Outcome: protocol.WorkspacePrepareReady,
			Warnings: append([]string(nil), summary.Warnings...),
		})
	}
	sourcePayload, callErr := s.callPeer(
		ctx, protocol.MethodInspectWorkspace, request.TreeID, *request.Source,
		protocol.InspectWorkspaceParams{
			SyncID: params.SyncID, GitURL: params.GitURL, SourcePath: params.SourcePath,
		},
	)
	if callErr != nil {
		if isContextError(callErr) {
			return callErr
		}
		return s.writeError(ctx, request, protocol.ErrorUnavailable, "source workspace inspection failed")
	}
	inspected, err := protocol.DecodePayload[protocol.InspectWorkspaceResult](sourcePayload)
	if err != nil || inspected.Validate() != nil || inspected.SyncID != params.SyncID ||
		inspected.Manifest.GitURL != params.GitURL {
		_ = s.connection.CloseNow()
		return s.writeError(ctx, request, protocol.ErrorUnavailable, "source returned an invalid workspace manifest")
	}
	receipt, err = s.server.registry.PinWorkspaceSyncManifest(
		ctx, key, inspected.Manifest, s.server.now(),
	)
	if err != nil {
		return s.handleWorkspaceStoreError(ctx, request, err)
	}
	manifest := receipt.Manifest()
	target := s.server.connection(params.TargetDeviceID)
	if target == nil {
		return s.writeError(ctx, request, protocol.ErrorUnavailable, "workspace target is unavailable")
	}
	cleanupProvisional := true
	defer func() {
		if !cleanupProvisional {
			return
		}
		select {
		case <-target.done:
			return
		default:
		}
		cleanupContext, cancel := context.WithTimeout(
			context.WithoutCancel(ctx), workspaceTransferCleanupTimeout,
		)
		defer cancel()
		_, cleanupErr := target.callPeer(
			cleanupContext, protocol.MethodCancelWorkspaceTransfer, request.TreeID, *request.Source,
			protocol.WorkspaceTransferControlParams{
				WorkspaceID: params.SyncID, TransferID: params.SyncID,
				SourceAgentID: request.Source.AgentID, SourceDeviceID: request.Source.DeviceID,
			},
		)
		if cleanupErr != nil {
			s.server.reportError(&internalError{operation: "clean provisional target workspace", err: cleanupErr})
			_ = target.connection.CloseNow()
		}
	}()
	targetPayload, callErr := target.callPeer(
		ctx, protocol.MethodPrepareWorkspace, request.TreeID, *request.Source,
		protocol.PrepareWorkspaceParams{
			WorkspaceID: params.SyncID, SourceAgentID: request.Source.AgentID,
			SourceDeviceID: request.Source.DeviceID, Manifest: manifest,
		},
	)
	if callErr != nil {
		if isContextError(callErr) {
			return callErr
		}
		return s.writeError(ctx, request, protocol.ErrorUnavailable, "target workspace preparation failed")
	}
	prepared, err := protocol.DecodePayload[protocol.PrepareWorkspaceResult](targetPayload)
	if err != nil || prepared.Validate() != nil || prepared.WorkspaceID != params.SyncID {
		_ = target.connection.CloseNow()
		return s.writeError(ctx, request, protocol.ErrorUnavailable, "target returned an invalid workspace result")
	}
	if prepared.Outcome == protocol.WorkspacePrepareTransferRequired {
		prepared, err = s.relayWorkspaceTransfer(
			ctx, target, request.TreeID, *request.Source, params, manifest, prepared,
		)
		if err != nil {
			targetFenced := errors.Is(err, errTargetWorkspaceTransferFenced)
			sourceFenced := errors.Is(err, errSourceWorkspaceTransferFenced)
			if targetFenced {
				cleanupProvisional = false
			}
			if sourceFenced && prepared.Outcome == protocol.WorkspacePrepareReady {
				defer s.connection.CloseNow()
			} else {
				if sourceFenced {
					if errors.Is(err, errInvalidTargetWorkspaceTransfer) {
						_ = target.connection.CloseNow()
					}
					return context.Canceled
				}
				if isContextError(err) {
					return err
				}
				writeErr := s.writeError(
					ctx, request, protocol.ErrorUnavailable, "workspace artifact transfer failed",
				)
				if errors.Is(err, errInvalidTargetWorkspaceTransfer) {
					_ = target.connection.CloseNow()
				}
				if errors.Is(err, errInvalidSourceWorkspaceTransfer) {
					_ = s.connection.CloseNow()
				}
				return writeErr
			}
		}
	}
	expectedWarnings, warningErr := protocol.WorkspaceWarningsForStrategy(manifest.Warnings, prepared.Strategy)
	if prepared.Outcome != protocol.WorkspacePrepareReady || warningErr != nil ||
		prepared.ManifestHash != receipt.ManifestHash ||
		!slices.Equal(prepared.Warnings, expectedWarnings) {
		_ = target.connection.CloseNow()
		return s.writeError(ctx, request, protocol.ErrorUnavailable, "target returned mismatched workspace metadata")
	}
	summary := protocol.WorkspaceSummary{
		WorkspaceID: params.SyncID, SourceDeviceID: request.Source.DeviceID,
		TargetDeviceID: params.TargetDeviceID, HeadOID: manifest.HeadOID,
		ObjectFormat:     manifest.ObjectFormat,
		WorkingDirectory: manifest.WorkingDirectory,
		Strategy:         prepared.Strategy, ManifestHash: prepared.ManifestHash,
		Warnings: append([]string(nil), prepared.Warnings...),
	}
	if err := summary.Validate(); err != nil {
		_ = target.connection.CloseNow()
		return s.writeError(ctx, request, protocol.ErrorUnavailable, "target returned invalid prepared workspace metadata")
	}
	receipt, err = s.server.registry.FinishWorkspaceSync(ctx, key, summary, s.server.now())
	if err != nil {
		return s.handleWorkspaceStoreError(ctx, request, err)
	}
	cleanupProvisional = false
	stored := receipt.Summary()
	return s.writeResult(ctx, request, protocol.SyncWorkspaceResult{
		Workspace: &stored, Outcome: protocol.WorkspacePrepareReady,
		Warnings: append([]string(nil), stored.Warnings...),
	})
}

func (s *session) handleWorkspaceStoreError(
	ctx context.Context,
	request protocol.Envelope,
	err error,
) error {
	if isContextError(err) {
		return err
	}
	if errors.Is(err, store.ErrAuthorizationDenied) {
		return s.writeError(ctx, request, protocol.ErrorForbidden, "workspace sync access denied")
	}
	if errors.Is(err, store.ErrNotFound) {
		return s.writeError(ctx, request, protocol.ErrorNotFound, "workspace resource not found")
	}
	if errors.Is(err, store.ErrConflict) {
		return s.writeError(ctx, request, protocol.ErrorConflict, "workspace request conflicts with existing state")
	}
	_ = s.writeError(ctx, request, protocol.ErrorUnavailable, "broker workspace state unavailable")
	return fmt.Errorf("workspace state: %w", err)
}
