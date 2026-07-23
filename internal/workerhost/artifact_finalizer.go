package workerhost

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/GhostFlying/delegation/internal/gitworkspace"
	"github.com/GhostFlying/delegation/internal/identity"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
)

const (
	changesArtifactRootName      = ".artifacts"
	changesArtifactPrefix        = "changes-"
	changesCaptureFailureCode    = "git_capture_failed"
	changesArtifactQuotaCode     = "artifact_quota_exceeded"
	pendingChangesArtifactSuffix = ".pending"
)

func openChangesArtifactRoot(workspaceRoot *os.Root) (*os.Root, error) {
	created := false
	if err := workspaceRoot.Mkdir(changesArtifactRootName, 0o700); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return nil, fmt.Errorf("create changes artifact root: %w", err)
		}
	} else {
		created = true
	}
	info, err := workspaceRoot.Lstat(changesArtifactRootName)
	if err != nil {
		return nil, fmt.Errorf("inspect changes artifact root: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("changes artifact root must be a directory, not a symbolic link")
	}
	if err := workspaceRoot.Chmod(changesArtifactRootName, 0o700); err != nil {
		return nil, fmt.Errorf("protect changes artifact root: %w", err)
	}
	root, err := workspaceRoot.OpenRoot(changesArtifactRootName)
	if err != nil {
		return nil, fmt.Errorf("open changes artifact root: %w", err)
	}
	anchored, err := workspaceRoot.Stat(changesArtifactRootName)
	if err != nil {
		_ = root.Close()
		return nil, fmt.Errorf("inspect anchored changes artifact root: %w", err)
	}
	visible, err := os.Stat(root.Name())
	if err != nil || !os.SameFile(anchored, visible) {
		_ = root.Close()
		return nil, errors.New("changes artifact root changed after it was opened")
	}
	if created {
		if err := syncDirectory(workspaceRoot); err != nil {
			_ = root.Close()
			return nil, fmt.Errorf("publish changes artifact root: %w", err)
		}
	}
	return root, nil
}

// ArtifactChanges is a coalesced signal that the durable publication outbox changed.
func (h *Host) ArtifactChanges() <-chan struct{} {
	return h.artifactChanges
}

// ListPendingChangesPublications returns this peer's changes artifacts awaiting broker ACK.
func (h *Host) ListPendingChangesPublications(ctx context.Context) ([]store.ChangesArtifact, error) {
	release, err := h.acquireOperation(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	return h.state.ListPendingChangesPublications(
		ctx, h.controllerID, h.deviceID, store.MaximumRetainedChangesArtifacts,
	)
}

// AcknowledgeChangesArtifact durably publishes an artifact and exposes the worker's terminal state.
func (h *Host) AcknowledgeChangesArtifact(
	ctx context.Context,
	key store.WorkerKey,
	artifactID string,
	sequence uint64,
) (store.WorkerFinalization, error) {
	release, err := h.acquireOperation(ctx)
	if err != nil {
		return store.WorkerFinalization{}, err
	}
	defer release()
	lock := h.lockFor(key)
	lock.Lock()
	defer lock.Unlock()
	finalization, err := h.state.AcknowledgeChangesArtifact(
		ctx, key, artifactID, sequence, time.Now(),
	)
	worker, err := h.recordWorkerChange(finalization.Worker, err)
	if err != nil {
		return store.WorkerFinalization{}, err
	}
	finalization.Worker = worker
	h.signalArtifactChange()
	return finalization, nil
}

func (h *Host) signalArtifactWork() {
	select {
	case h.artifactWake <- struct{}{}:
	default:
	}
}

func (h *Host) signalArtifactChange() {
	select {
	case h.artifactChanges <- struct{}{}:
	default:
	}
}

func (h *Host) processArtifactFinalizations(ctx context.Context) {
	defer h.background.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case <-h.artifactWake:
		}
		if err := h.processPendingArtifactFinalizations(ctx); err != nil {
			if errors.Is(err, context.Canceled) && ctx.Err() != nil {
				return
			}
			fatalErr := fmt.Errorf("finalize worker changes artifacts: %w", err)
			h.reportError(fatalErr)
			h.fail(fatalErr)
			return
		}
	}
}

func (h *Host) processPendingArtifactFinalizations(ctx context.Context) error {
	for {
		artifacts, err := h.state.ListPendingChangesCaptures(
			ctx, h.controllerID, h.deviceID, store.MaximumRetainedChangesArtifacts,
		)
		if err != nil {
			return err
		}
		if len(artifacts) == 0 {
			break
		}
		for _, artifact := range artifacts {
			if err := h.captureChangesArtifact(ctx, artifact); err != nil {
				return err
			}
		}
	}
	publications, err := h.state.ListPendingChangesPublications(
		ctx, h.controllerID, h.deviceID, store.MaximumRetainedChangesArtifacts,
	)
	if err != nil {
		return err
	}
	if len(publications) != 0 {
		h.signalArtifactChange()
	}
	return nil
}

func (h *Host) captureChangesArtifact(ctx context.Context, artifact store.ChangesArtifact) error {
	workspace, err := h.loadChangesWorkspace(ctx, artifact)
	if err != nil {
		return h.completeFailedChangesCapture(ctx, artifact, nil, changesCaptureFailureCode, err)
	}
	_, err = h.state.ReserveChangesArtifactPayload(
		ctx,
		artifact.WorkerKey,
		artifact.ArtifactID,
		store.MaximumChangesArtifactPayloadBytes,
		time.Now(),
	)
	if errors.Is(err, store.ErrChangesArtifactQuota) {
		return h.completeFailedChangesCapture(
			ctx, artifact, workspace.Warnings, changesArtifactQuotaCode, err,
		)
	}
	if err != nil {
		return err
	}
	pendingName, finalName, err := changesArtifactDirectoryNames(artifact.ArtifactID)
	if err != nil {
		return err
	}
	if err := h.removeChangesArtifactDirectories(ctx, pendingName, finalName); err != nil {
		return h.completeFailedChangesCapture(
			ctx, artifact, workspace.Warnings, changesCaptureFailureCode, err,
		)
	}
	manifest := protocol.WorkspaceManifest{
		GitURL: workspace.GitURL, HeadOID: workspace.HeadOID,
		ObjectFormat: workspace.ObjectFormat, WorkingDirectory: workspace.WorkingDirectory,
		Clean: workspace.Clean, SourceSnapshotHash: workspace.SourceSnapshotHash,
		Warnings: slices.Clone(workspace.SourceWarnings),
	}
	// CaptureResult requires an absolute path. The opened os.Root anchors its
	// parent, and pendingName is derived only from a validated artifact UUID.
	pendingPath := filepath.Join(h.artifactRoot.Name(), pendingName)
	capture, err := h.git.CaptureResult(ctx, workspace.WorkspacePath, pendingPath, manifest)
	if err != nil {
		_ = h.removeChangesArtifactDirectories(context.Background(), pendingName, finalName)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return h.completeFailedChangesCapture(
			ctx, artifact, workspace.Warnings, changesCaptureFailureCode, err,
		)
	}
	result, err := changesCaptureResult(capture, pendingPath, workspace.Warnings)
	if err != nil {
		_ = h.removeChangesArtifactDirectories(context.Background(), pendingName, finalName)
		return h.completeFailedChangesCapture(
			ctx, artifact, workspace.Warnings, changesCaptureFailureCode, err,
		)
	}
	if err := h.artifactRoot.Rename(pendingName, finalName); err != nil {
		_ = h.removeChangesArtifactDirectories(context.Background(), pendingName, finalName)
		return h.completeFailedChangesCapture(
			ctx, artifact, workspace.Warnings, changesCaptureFailureCode, err,
		)
	}
	if err := syncDirectory(h.artifactRoot); err != nil {
		_ = h.artifactRoot.RemoveAll(finalName)
		_ = syncDirectory(h.artifactRoot)
		return h.completeFailedChangesCapture(
			ctx, artifact, workspace.Warnings, changesCaptureFailureCode, err,
		)
	}
	if _, err := h.state.CompleteChangesArtifactCapture(
		ctx, artifact.WorkerKey, artifact.ArtifactID, result, time.Now(),
	); err != nil {
		return err
	}
	h.signalArtifactChange()
	return nil
}

func (h *Host) loadChangesWorkspace(
	ctx context.Context,
	artifact store.ChangesArtifact,
) (store.PreparedWorkspace, error) {
	worker, err := h.state.GetWorker(ctx, artifact.WorkerKey)
	if err != nil {
		return store.PreparedWorkspace{}, err
	}
	workspace, err := h.state.GetPreparedWorkspace(ctx, store.PreparedWorkspaceKey{
		ControllerID: artifact.ControllerID,
		TreeID:       artifact.TreeID,
		WorkspaceID:  artifact.WorkspaceID,
	})
	if err != nil {
		return store.PreparedWorkspace{}, err
	}
	manifest := protocol.WorkspaceManifest{
		GitURL: workspace.GitURL, HeadOID: workspace.HeadOID,
		ObjectFormat: workspace.ObjectFormat, WorkingDirectory: workspace.WorkingDirectory,
		Clean: workspace.Clean, SourceSnapshotHash: workspace.SourceSnapshotHash,
		Warnings: slices.Clone(workspace.SourceWarnings),
	}
	manifestHash, err := protocol.WorkspaceManifestHash(manifest)
	if err != nil {
		return store.PreparedWorkspace{}, err
	}
	if worker.Status != store.WorkerFinalizing || worker.ActiveTurnID != artifact.TurnID ||
		worker.WorkspaceID != artifact.WorkspaceID || worker.WorkspacePath != workspace.WorkspacePath ||
		worker.DeviceID != h.deviceID || workspace.Status != store.PreparedWorkspaceClaimed ||
		workspace.ClaimedAgentID != worker.AgentID || workspace.SourceAgentID != worker.ParentAgentID ||
		workspace.TargetDeviceID != h.deviceID || artifact.BaseHeadOID != workspace.HeadOID ||
		artifact.ObjectFormat != workspace.ObjectFormat || artifact.BaseClean != workspace.Clean ||
		artifact.BaseManifestHash != manifestHash || artifact.BaseManifestHash != workspace.ManifestHash ||
		artifact.BaseSnapshotHash != workspace.SourceSnapshotHash {
		return store.PreparedWorkspace{}, store.ErrChangesArtifactAuthority
	}
	if err := h.verifyPreparedWorkspace(ctx, workspace); err != nil {
		return store.PreparedWorkspace{}, err
	}
	return workspace, nil
}

func (h *Host) completeFailedChangesCapture(
	ctx context.Context,
	artifact store.ChangesArtifact,
	warnings []string,
	failureCode string,
	cause error,
) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if cause != nil {
		h.reportError(fmt.Errorf(
			"capture worker changes artifact %s: %w", artifact.ArtifactID, cause,
		))
	}
	_, err := h.state.CompleteChangesArtifactCapture(
		ctx,
		artifact.WorkerKey,
		artifact.ArtifactID,
		store.ChangesCaptureResult{
			Status:      store.ChangesCaptureFailed,
			Warnings:    slices.Clone(warnings),
			FailureCode: failureCode,
		},
		time.Now(),
	)
	if err == nil {
		h.signalArtifactChange()
	}
	return err
}

func (h *Host) removeChangesArtifactDirectories(ctx context.Context, names ...string) error {
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !isChangesArtifactDirectoryName(name) {
			return fmt.Errorf("refuse to remove unexpected changes artifact path %q", name)
		}
		if err := h.artifactRoot.RemoveAll(name); err != nil {
			return fmt.Errorf("remove changes artifact path %q: %w", name, err)
		}
	}
	if len(names) != 0 {
		return syncDirectory(h.artifactRoot)
	}
	return nil
}

func changesCaptureResult(
	capture gitworkspace.ResultCapture,
	expectedDirectory string,
	warnings []string,
) (store.ChangesCaptureResult, error) {
	if capture.ArtifactDirectory != expectedDirectory {
		return store.ChangesCaptureResult{}, errors.New("Git capture returned an unexpected artifact directory")
	}
	result := store.ChangesCaptureResult{
		Status:             store.ChangesAvailable,
		ResultHeadOID:      capture.ResultHeadOID,
		ResultSnapshotHash: capture.ResultSnapshotHash,
		ResultClean:        capture.ResultClean,
		Warnings:           slices.Clone(warnings),
	}
	if capture.Unchanged {
		result.Status = store.ChangesUnchanged
	}
	for _, part := range []*gitworkspace.ResultArtifact{capture.Bundle, capture.Overlay} {
		if part == nil {
			continue
		}
		expectedName := ""
		kind := store.ChangesArtifactPartKind("")
		switch part.Kind {
		case protocol.WorkspaceArtifactBundle:
			expectedName = store.ChangesBundlePartName
			kind = store.ChangesArtifactBundle
		case protocol.WorkspaceArtifactOverlay:
			expectedName = store.ChangesOverlayPartName
			kind = store.ChangesArtifactOverlay
		default:
			return store.ChangesCaptureResult{}, fmt.Errorf("unsupported Git capture part %q", part.Kind)
		}
		if part.Name != expectedName || part.Path != filepath.Join(expectedDirectory, expectedName) {
			return store.ChangesCaptureResult{}, errors.New("Git capture returned an unexpected artifact path")
		}
		result.Parts = append(result.Parts, store.ChangesArtifactPart{
			Kind: kind, Name: part.Name, SizeBytes: part.Size, SHA256: part.SHA256,
		})
	}
	return result, nil
}

func changesArtifactDirectoryNames(artifactID string) (string, string, error) {
	if err := identity.ValidateID(artifactID); err != nil {
		return "", "", fmt.Errorf("artifactId %w", err)
	}
	finalName := changesArtifactPrefix + artifactID
	return finalName + pendingChangesArtifactSuffix, finalName, nil
}

func isChangesArtifactDirectoryName(name string) bool {
	id := name
	if len(id) > len(pendingChangesArtifactSuffix) &&
		id[len(id)-len(pendingChangesArtifactSuffix):] == pendingChangesArtifactSuffix {
		id = id[:len(id)-len(pendingChangesArtifactSuffix)]
	}
	if len(id) <= len(changesArtifactPrefix) || id[:len(changesArtifactPrefix)] != changesArtifactPrefix {
		return false
	}
	return identity.ValidateID(id[len(changesArtifactPrefix):]) == nil
}
