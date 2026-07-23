package workerhost

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/gitworkspace"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
)

type WorkspaceInspectRequest struct {
	TreeID string
	Source control.PrincipalIdentity
	Params protocol.InspectWorkspaceParams
}

type WorkspacePrepareRequest struct {
	TreeID string
	Source control.PrincipalIdentity
	Params protocol.PrepareWorkspaceParams
}

func (h *Host) InspectWorkspace(
	ctx context.Context,
	request WorkspaceInspectRequest,
) (protocol.InspectWorkspaceResult, error) {
	release, err := h.acquireWorkspaceOperation(ctx)
	if err != nil {
		return protocol.InspectWorkspaceResult{}, err
	}
	defer release()
	if err := validateLocalWorkspaceRootRequest(
		request.TreeID, request.Source, h.controllerID, h.deviceID,
	); err != nil {
		return protocol.InspectWorkspaceResult{}, err
	}
	if err := request.Params.Validate(); err != nil {
		return protocol.InspectWorkspaceResult{}, err
	}
	repository, err := h.git.Inspect(ctx, request.Params.SourcePath, request.Params.GitURL)
	if err != nil {
		return protocol.InspectWorkspaceResult{}, err
	}
	return protocol.InspectWorkspaceResult{
		SyncID: request.Params.SyncID, Manifest: repository.Manifest,
	}, nil
}

func (h *Host) PrepareWorkspace(
	ctx context.Context,
	request WorkspacePrepareRequest,
) (protocol.PrepareWorkspaceResult, error) {
	release, err := h.acquireWorkspaceOperation(ctx)
	if err != nil {
		return protocol.PrepareWorkspaceResult{}, err
	}
	defer release()
	if err := validateWorkspaceRootRequest(request.TreeID, request.Source, h.controllerID); err != nil ||
		request.Params.SourceAgentID != request.Source.AgentID ||
		request.Params.SourceDeviceID != request.Source.DeviceID {
		return protocol.PrepareWorkspaceResult{}, errors.New("workspace prepare authority is invalid")
	}
	if err := request.Params.Validate(); err != nil {
		return protocol.PrepareWorkspaceResult{}, err
	}
	manifestHash, err := gitworkspace.ManifestHash(request.Params.Manifest)
	if err != nil {
		return protocol.PrepareWorkspaceResult{}, err
	}
	key := store.PreparedWorkspaceKey{
		ControllerID: h.controllerID, TreeID: request.TreeID,
		WorkspaceID: request.Params.WorkspaceID,
	}
	lock := h.lockFor(store.WorkerKey{
		ControllerID: h.controllerID, TreeID: request.TreeID, AgentID: request.Params.WorkspaceID,
	})
	lock.Lock()
	defer lock.Unlock()
	if existing, loadErr := h.state.GetPreparedWorkspace(ctx, key); loadErr == nil {
		if existing.SourceAgentID != request.Source.AgentID ||
			existing.SourceDeviceID != request.Source.DeviceID ||
			existing.TargetDeviceID != h.deviceID || existing.GitURL != request.Params.Manifest.GitURL ||
			existing.HeadOID != request.Params.Manifest.HeadOID ||
			existing.ObjectFormat != request.Params.Manifest.ObjectFormat ||
			existing.WorkingDirectory != request.Params.Manifest.WorkingDirectory ||
			existing.Clean != request.Params.Manifest.Clean ||
			existing.SourceSnapshotHash != request.Params.Manifest.SourceSnapshotHash ||
			existing.ManifestHash != manifestHash {
			return protocol.PrepareWorkspaceResult{}, store.ErrWorkerReservationConflict
		}
		if err := h.verifyPreparedWorkspace(ctx, existing); err != nil {
			return protocol.PrepareWorkspaceResult{}, err
		}
		return preparedWorkspaceResult(existing), nil
	} else if !errors.Is(loadErr, store.ErrNotFound) {
		return protocol.PrepareWorkspaceResult{}, loadErr
	}
	pendingKey := workspacePreparationKey(request.TreeID, request.Params.WorkspaceID)
	h.workspaceTransferMu.Lock()
	pending, alreadyPending := h.pendingWorkspaces[pendingKey]
	h.workspaceTransferMu.Unlock()
	if alreadyPending {
		if !pending.matches(request.Source, request.Params.Manifest, manifestHash) {
			return protocol.PrepareWorkspaceResult{}, store.ErrWorkerReservationConflict
		}
		return pending.result(), nil
	}
	finalName := workspaceSyncName(request.TreeID, request.Params.WorkspaceID)
	if _, err := h.workspaceRoot.Lstat(finalName); err == nil {
		if err := h.workspaceRoot.RemoveAll(finalName); err != nil {
			return protocol.PrepareWorkspaceResult{}, fmt.Errorf("remove orphaned prepared workspace: %w", err)
		}
		if err := h.syncWorkspaceDirectory(); err != nil {
			return protocol.PrepareWorkspaceResult{}, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return protocol.PrepareWorkspaceResult{}, fmt.Errorf("inspect target workspace destination: %w", err)
	}
	temporaryName := finalName + ".pending"
	if _, err := h.workspaceRoot.Lstat(temporaryName); err == nil {
		if err := h.workspaceRoot.RemoveAll(temporaryName); err != nil {
			return protocol.PrepareWorkspaceResult{}, fmt.Errorf("remove orphaned pending workspace: %w", err)
		}
		if err := h.syncWorkspaceDirectory(); err != nil {
			return protocol.PrepareWorkspaceResult{}, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return protocol.PrepareWorkspaceResult{}, fmt.Errorf("inspect pending workspace destination: %w", err)
	}
	temporaryPath := filepath.Join(h.workspaceRoot.Name(), temporaryName)
	base, err := h.git.PrepareBase(ctx, temporaryPath, request.Params.Manifest)
	if err != nil {
		return protocol.PrepareWorkspaceResult{}, err
	}
	if base.BundleRequired || base.OverlayRequired {
		if err := ctx.Err(); err != nil {
			_ = h.workspaceRoot.RemoveAll(temporaryName)
			return protocol.PrepareWorkspaceResult{}, err
		}
		pending = pendingWorkspacePreparation{
			TreeID: request.TreeID, Source: request.Source,
			WorkspaceID: request.Params.WorkspaceID, Manifest: request.Params.Manifest,
			TransferID:   request.Params.WorkspaceID,
			ManifestHash: manifestHash, TemporaryName: temporaryName, Base: base,
		}
		h.workspaceTransferMu.Lock()
		h.pendingWorkspaces[pendingKey] = pending
		h.workspaceTransferMu.Unlock()
		return pending.result(), nil
	}
	return h.publishPreparedWorkspace(
		ctx, request.TreeID, request.Source, request.Params.WorkspaceID,
		temporaryName, request.Params.Manifest, protocol.WorkspaceStrategyDirect,
		request.Params.Manifest.Warnings,
	)
}

func preparedWorkspaceResult(workspace store.PreparedWorkspace) protocol.PrepareWorkspaceResult {
	return protocol.PrepareWorkspaceResult{
		WorkspaceID: workspace.WorkspaceID, Outcome: protocol.WorkspacePrepareReady,
		Strategy: workspace.Strategy, ManifestHash: workspace.ManifestHash,
		Warnings: append([]string(nil), workspace.Warnings...),
	}
}

func validateLocalWorkspaceRootRequest(
	treeID string,
	source control.PrincipalIdentity,
	controllerID, deviceID string,
) error {
	if err := validateWorkspaceRootRequest(treeID, source, controllerID); err != nil {
		return err
	}
	if source.DeviceID != deviceID {
		return errors.New("workspace source is not a local tree root")
	}
	return nil
}

func (h *Host) prepareWorkerWorkspace(ctx context.Context, worker store.WorkerReservation) error {
	if worker.WorkspaceID == "" {
		return h.prepareWorkspace(worker.WorkerKey)
	}
	prepared, err := h.state.GetPreparedWorkspace(ctx, store.PreparedWorkspaceKey{
		ControllerID: worker.ControllerID, TreeID: worker.TreeID, WorkspaceID: worker.WorkspaceID,
	})
	if err != nil {
		return err
	}
	if prepared.WorkspacePath != worker.WorkspacePath ||
		prepared.WorkingDirectory != worker.WorkingDirectory ||
		prepared.TargetDeviceID != h.deviceID {
		return store.ErrWorkerReservationConflict
	}
	return h.verifyPreparedWorkspace(ctx, prepared)
}

func (h *Host) verifyPreparedWorkspace(ctx context.Context, workspace store.PreparedWorkspace) error {
	if err := h.verifyPreparedWorkspaceAuthority(workspace); err != nil {
		return err
	}
	repositoryRoot, err := h.workspaceRoot.OpenRoot(
		workspaceSyncName(workspace.TreeID, workspace.WorkspaceID),
	)
	if err != nil {
		return fmt.Errorf("open anchored prepared workspace: %w", err)
	}
	defer repositoryRoot.Close()
	partial := ""
	for _, component := range strings.Split(workspace.WorkingDirectory, "/") {
		if component == "" {
			continue
		}
		partial = filepath.Join(partial, component)
		info, err := repositoryRoot.Lstat(partial)
		if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return errors.New("prepared workspace working directory is unavailable")
		}
	}
	if workspace.Status == store.PreparedWorkspaceReady {
		manifest := protocol.WorkspaceManifest{
			GitURL: workspace.GitURL, HeadOID: workspace.HeadOID,
			ObjectFormat: workspace.ObjectFormat, WorkingDirectory: workspace.WorkingDirectory,
			Clean: workspace.Clean, SourceSnapshotHash: workspace.SourceSnapshotHash,
			Warnings: append([]string(nil), workspace.Warnings...),
		}
		if err := h.git.VerifySnapshot(ctx, workspace.WorkspacePath, manifest); err != nil {
			return err
		}
	}
	return nil
}

func (h *Host) verifyPreparedWorkspaceAuthority(workspace store.PreparedWorkspace) error {
	expected := filepath.Join(
		h.workspaceRoot.Name(), workspaceSyncName(workspace.TreeID, workspace.WorkspaceID),
	)
	if workspace.WorkspacePath != expected {
		return errors.New("prepared workspace is outside its configured location")
	}
	name := workspaceSyncName(workspace.TreeID, workspace.WorkspaceID)
	anchored, err := h.workspaceRoot.Lstat(name)
	if err != nil {
		return fmt.Errorf("inspect anchored prepared workspace: %w", err)
	}
	visible, err := os.Lstat(expected)
	if err != nil {
		return fmt.Errorf("inspect visible prepared workspace: %w", err)
	}
	if anchored.Mode()&os.ModeSymlink != 0 || visible.Mode()&os.ModeSymlink != 0 ||
		!anchored.IsDir() || !os.SameFile(anchored, visible) {
		return errors.New("prepared workspace directory changed after publication")
	}
	return nil
}

func (h *Host) syncWorkspaceDirectory() error {
	return syncDirectory(h.workspaceRoot)
}

func workspaceSyncName(treeID, workspaceID string) string {
	digest := sha256.Sum256([]byte(treeID + "\x00" + workspaceID))
	return "workspace-" + hex.EncodeToString(digest[:16])
}

func (h *Host) workerCWD(worker store.WorkerReservation) string {
	return filepath.Join(worker.WorkspacePath, filepath.FromSlash(worker.WorkingDirectory))
}

func (h *Host) validateThreadWorkspace(result threadResult, worker store.WorkerReservation) error {
	if !sameNativePath(result.CWD, h.workerCWD(worker)) {
		return errors.New("managed app-server returned an unexpected thread cwd")
	}
	if len(result.RuntimeWorkspaceRoots) != 1 ||
		!sameNativePath(result.RuntimeWorkspaceRoots[0], worker.WorkspacePath) {
		return errors.New("managed app-server returned unexpected runtime workspace roots")
	}
	expectedProfile := workerPermissionProfile
	if runtime.GOOS == "windows" {
		expectedProfile = windowsWorkerProfile
	}
	if result.ActivePermissionProfile == nil || result.ActivePermissionProfile.ID != expectedProfile {
		return errors.New("managed app-server returned an unexpected permission profile")
	}
	return nil
}

func sameNativePath(left, right string) bool {
	left = filepath.Clean(left)
	right = filepath.Clean(right)
	if runtime.GOOS == "windows" {
		return strings.EqualFold(left, right)
	}
	return left == right
}
