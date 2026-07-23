package workerhost

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"slices"

	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
)

func (h *Host) CreateWorkspaceTransfer(
	ctx context.Context,
	request WorkspaceCreateTransferRequest,
) (protocol.CreateWorkspaceTransferResult, error) {
	release, err := h.acquireWorkspaceOperation(ctx)
	if err != nil {
		return protocol.CreateWorkspaceTransferResult{}, err
	}
	defer release()
	if err := validateLocalWorkspaceRootRequest(
		request.TreeID, request.Source, h.controllerID, h.deviceID,
	); err != nil {
		return protocol.CreateWorkspaceTransferResult{}, err
	}
	if err := request.Params.Validate(); err != nil {
		return protocol.CreateWorkspaceTransferResult{}, err
	}
	if request.Params.OverlayRequired {
		return protocol.CreateWorkspaceTransferResult{}, errors.New("dirty workspace overlay transport is not implemented")
	}
	manifestHash, err := protocol.WorkspaceManifestHash(request.Params.Manifest)
	if err != nil {
		return protocol.CreateWorkspaceTransferResult{}, err
	}
	lock := h.lockFor(store.WorkerKey{
		ControllerID: h.controllerID, TreeID: request.TreeID, AgentID: request.Params.WorkspaceID,
	})
	lock.Lock()
	defer lock.Unlock()
	h.workspaceTransferMu.Lock()
	existing, exists := h.outboundTransfers[request.Params.TransferID]
	h.workspaceTransferMu.Unlock()
	if exists {
		if existing.TreeID != request.TreeID || existing.Source != request.Source ||
			existing.WorkspaceID != request.Params.WorkspaceID ||
			existing.Transfer.ManifestHash != manifestHash {
			return protocol.CreateWorkspaceTransferResult{}, store.ErrWorkerReservationConflict
		}
		return protocol.CreateWorkspaceTransferResult{Transfer: existing.Transfer}, nil
	}
	repository, err := h.git.Inspect(ctx, request.Params.SourcePath, request.Params.GitURL)
	if err != nil {
		return protocol.CreateWorkspaceTransferResult{}, err
	}
	actualHash, err := protocol.WorkspaceManifestHash(repository.Manifest)
	if err != nil || actualHash != manifestHash {
		return protocol.CreateWorkspaceTransferResult{}, errors.New("source workspace changed before transfer creation")
	}
	directoryName := sourceTransferDirectoryName(request.Params.TransferID)
	if err := h.workspaceRoot.RemoveAll(directoryName); err != nil {
		return protocol.CreateWorkspaceTransferResult{}, fmt.Errorf("remove orphaned source transfer: %w", err)
	}
	if err := h.workspaceRoot.Mkdir(directoryName, 0o700); err != nil {
		return protocol.CreateWorkspaceTransferResult{}, fmt.Errorf("create source transfer directory: %w", err)
	}
	keep := false
	defer func() {
		if !keep {
			_ = h.workspaceRoot.RemoveAll(directoryName)
			_ = h.syncWorkspaceDirectory()
		}
	}()
	artifactName := filepath.Join(directoryName, workspaceArtifactFileName(protocol.WorkspaceArtifactBundle))
	strategy, err := h.git.CreateBundle(
		ctx,
		repository.Root,
		filepath.Join(h.workspaceRoot.Name(), artifactName),
		request.Params.Manifest,
		request.Params.BasisOIDs,
	)
	if err != nil {
		return protocol.CreateWorkspaceTransferResult{}, err
	}
	repository, err = h.git.Inspect(ctx, request.Params.SourcePath, request.Params.GitURL)
	if err != nil {
		return protocol.CreateWorkspaceTransferResult{}, err
	}
	actualHash, err = protocol.WorkspaceManifestHash(repository.Manifest)
	if err != nil || actualHash != manifestHash {
		return protocol.CreateWorkspaceTransferResult{}, errors.New("source workspace changed during transfer creation")
	}
	descriptor, err := h.describeWorkspaceArtifact(artifactName, protocol.WorkspaceArtifactBundle)
	if err != nil {
		return protocol.CreateWorkspaceTransferResult{}, err
	}
	warnings, err := protocol.WorkspaceWarningsForStrategy(request.Params.Manifest.Warnings, strategy)
	if err != nil {
		return protocol.CreateWorkspaceTransferResult{}, err
	}
	transfer := protocol.WorkspaceTransferManifest{
		TransferID: request.Params.TransferID, WorkspaceID: request.Params.WorkspaceID,
		Strategy: strategy, ManifestHash: manifestHash,
		Artifacts: []protocol.WorkspaceArtifactDescriptor{descriptor}, Warnings: warnings,
	}
	if err := transfer.Validate(); err != nil {
		return protocol.CreateWorkspaceTransferResult{}, err
	}
	state := outboundWorkspaceTransfer{
		TreeID: request.TreeID, Source: request.Source, WorkspaceID: request.Params.WorkspaceID,
		DirectoryName: directoryName, Transfer: transfer,
		ArtifactNames: map[protocol.WorkspaceArtifactKind]string{
			protocol.WorkspaceArtifactBundle: artifactName,
		},
	}
	h.workspaceTransferMu.Lock()
	h.outboundTransfers[request.Params.TransferID] = state
	h.workspaceTransferMu.Unlock()
	keep = true
	return protocol.CreateWorkspaceTransferResult{Transfer: transfer}, nil
}

func (h *Host) ReadWorkspaceArtifact(
	ctx context.Context,
	request WorkspaceReadArtifactRequest,
) (protocol.ReadWorkspaceArtifactResult, error) {
	release, err := h.acquireWorkspaceOperation(ctx)
	if err != nil {
		return protocol.ReadWorkspaceArtifactResult{}, err
	}
	defer release()
	if err := validateLocalWorkspaceRootRequest(
		request.TreeID, request.Source, h.controllerID, h.deviceID,
	); err != nil {
		return protocol.ReadWorkspaceArtifactResult{}, err
	}
	if err := request.Params.Validate(); err != nil {
		return protocol.ReadWorkspaceArtifactResult{}, err
	}
	h.workspaceTransferMu.Lock()
	state, exists := h.outboundTransfers[request.Params.TransferID]
	h.workspaceTransferMu.Unlock()
	if !exists || state.TreeID != request.TreeID || state.Source != request.Source {
		return protocol.ReadWorkspaceArtifactResult{}, errors.New("source workspace transfer was not found")
	}
	lock := h.lockFor(store.WorkerKey{
		ControllerID: h.controllerID, TreeID: state.TreeID, AgentID: state.WorkspaceID,
	})
	lock.Lock()
	defer lock.Unlock()
	if err := ctx.Err(); err != nil {
		return protocol.ReadWorkspaceArtifactResult{}, err
	}
	descriptor, found := workspaceArtifactDescriptor(state.Transfer.Artifacts, request.Params.Kind)
	artifactName, named := state.ArtifactNames[request.Params.Kind]
	if !found || !named || request.Params.Offset >= descriptor.Size {
		return protocol.ReadWorkspaceArtifactResult{}, errors.New("source workspace artifact read is out of range")
	}
	file, err := h.workspaceRoot.Open(artifactName)
	if err != nil {
		return protocol.ReadWorkspaceArtifactResult{}, fmt.Errorf("open source workspace artifact: %w", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() != descriptor.Size {
		return protocol.ReadWorkspaceArtifactResult{}, errors.New("source workspace artifact changed after creation")
	}
	length := min(int64(request.Params.Limit), descriptor.Size-request.Params.Offset)
	data := make([]byte, int(length))
	if _, err := file.ReadAt(data, request.Params.Offset); err != nil {
		return protocol.ReadWorkspaceArtifactResult{}, fmt.Errorf("read source workspace artifact: %w", err)
	}
	return protocol.ReadWorkspaceArtifactResult{
		TransferID: request.Params.TransferID, Kind: request.Params.Kind,
		Offset: request.Params.Offset, Data: data, NextOffset: request.Params.Offset + length,
	}, nil
}

func (h *Host) BeginWorkspaceTransfer(
	ctx context.Context,
	request WorkspaceBeginTransferRequest,
) (protocol.BeginWorkspaceTransferResult, error) {
	release, err := h.acquireWorkspaceOperation(ctx)
	if err != nil {
		return protocol.BeginWorkspaceTransferResult{}, err
	}
	defer release()
	if err := validateWorkspaceRootRequest(request.TreeID, request.Source, h.controllerID); err != nil ||
		request.Params.SourceAgentID != request.Source.AgentID ||
		request.Params.SourceDeviceID != request.Source.DeviceID {
		return protocol.BeginWorkspaceTransferResult{}, errors.New("workspace transfer begin authority is invalid")
	}
	if err := request.Params.Validate(); err != nil {
		return protocol.BeginWorkspaceTransferResult{}, err
	}
	if _, found := workspaceArtifactDescriptor(
		request.Params.Transfer.Artifacts, protocol.WorkspaceArtifactOverlay,
	); found {
		return protocol.BeginWorkspaceTransferResult{}, errors.New("dirty workspace overlay transport is not implemented")
	}
	workspaceID := request.Params.Transfer.WorkspaceID
	lock := h.lockFor(store.WorkerKey{
		ControllerID: h.controllerID, TreeID: request.TreeID, AgentID: workspaceID,
	})
	lock.Lock()
	defer lock.Unlock()
	h.workspaceTransferMu.Lock()
	pending, pendingExists := h.pendingWorkspaces[workspacePreparationKey(request.TreeID, workspaceID)]
	existing, transferExists := h.inboundTransfers[request.Params.Transfer.TransferID]
	h.workspaceTransferMu.Unlock()
	if transferExists {
		if existing.TreeID != request.TreeID || existing.Source != request.Source ||
			existing.Transfer.WorkspaceID != workspaceID ||
			existing.Transfer.ManifestHash != request.Params.Transfer.ManifestHash {
			return protocol.BeginWorkspaceTransferResult{}, store.ErrWorkerReservationConflict
		}
		return protocol.BeginWorkspaceTransferResult{TransferID: existing.Transfer.TransferID}, nil
	}
	if !pendingExists || !pending.matches(request.Source, request.Params.Manifest, request.Params.Transfer.ManifestHash) {
		return protocol.BeginWorkspaceTransferResult{}, store.ErrWorkerReservationConflict
	}
	if pending.TransferID != request.Params.Transfer.TransferID && pending.TransferID != workspaceID {
		return protocol.BeginWorkspaceTransferResult{}, store.ErrWorkerReservationConflict
	}
	_, hasBundle := workspaceArtifactDescriptor(
		request.Params.Transfer.Artifacts, protocol.WorkspaceArtifactBundle,
	)
	if pending.Base.BundleRequired != hasBundle || pending.Base.OverlayRequired {
		return protocol.BeginWorkspaceTransferResult{}, errors.New("workspace transfer artifacts do not match target preparation")
	}
	directoryName := targetTransferDirectoryName(request.Params.Transfer.TransferID)
	if err := h.workspaceRoot.RemoveAll(directoryName); err != nil {
		return protocol.BeginWorkspaceTransferResult{}, fmt.Errorf("remove orphaned target transfer: %w", err)
	}
	if err := h.workspaceRoot.Mkdir(directoryName, 0o700); err != nil {
		return protocol.BeginWorkspaceTransferResult{}, fmt.Errorf("create target transfer directory: %w", err)
	}
	artifactNames := make(map[protocol.WorkspaceArtifactKind]string, len(request.Params.Transfer.Artifacts))
	for _, artifact := range request.Params.Transfer.Artifacts {
		name := filepath.Join(directoryName, workspaceArtifactFileName(artifact.Kind))
		file, err := h.workspaceRoot.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			_ = h.workspaceRoot.RemoveAll(directoryName)
			return protocol.BeginWorkspaceTransferResult{}, fmt.Errorf("create target workspace artifact: %w", err)
		}
		if err := file.Close(); err != nil {
			_ = h.workspaceRoot.RemoveAll(directoryName)
			return protocol.BeginWorkspaceTransferResult{}, fmt.Errorf("close target workspace artifact: %w", err)
		}
		artifactNames[artifact.Kind] = name
	}
	state := &inboundWorkspaceTransfer{
		TreeID: request.TreeID, Source: request.Source, Manifest: request.Params.Manifest,
		Transfer: request.Params.Transfer, DirectoryName: directoryName,
		PendingName: pending.TemporaryName, ArtifactNames: artifactNames,
		Offsets: make(map[protocol.WorkspaceArtifactKind]int64, len(artifactNames)),
		Hashes:  make(map[protocol.WorkspaceArtifactKind]hash.Hash, len(artifactNames)),
	}
	for kind := range artifactNames {
		state.Hashes[kind] = sha256.New()
	}
	if err := ctx.Err(); err != nil {
		_ = h.workspaceRoot.RemoveAll(directoryName)
		return protocol.BeginWorkspaceTransferResult{}, err
	}
	h.workspaceTransferMu.Lock()
	pending.TransferID = request.Params.Transfer.TransferID
	h.pendingWorkspaces[workspacePreparationKey(request.TreeID, workspaceID)] = pending
	h.inboundTransfers[request.Params.Transfer.TransferID] = state
	h.workspaceTransferMu.Unlock()
	return protocol.BeginWorkspaceTransferResult{TransferID: request.Params.Transfer.TransferID}, nil
}

func (h *Host) WriteWorkspaceArtifact(
	ctx context.Context,
	request WorkspaceWriteArtifactRequest,
) (protocol.WriteWorkspaceArtifactResult, error) {
	release, err := h.acquireWorkspaceOperation(ctx)
	if err != nil {
		return protocol.WriteWorkspaceArtifactResult{}, err
	}
	defer release()
	if err := validateWorkspaceRootRequest(request.TreeID, request.Source, h.controllerID); err != nil {
		return protocol.WriteWorkspaceArtifactResult{}, err
	}
	if err := request.Params.Validate(); err != nil {
		return protocol.WriteWorkspaceArtifactResult{}, err
	}
	h.workspaceTransferMu.Lock()
	state, exists := h.inboundTransfers[request.Params.TransferID]
	h.workspaceTransferMu.Unlock()
	if !exists || state.TreeID != request.TreeID || state.Source != request.Source ||
		state.Transfer.WorkspaceID != request.Params.WorkspaceID {
		return protocol.WriteWorkspaceArtifactResult{}, errors.New("target workspace transfer was not found")
	}
	lock := h.lockFor(store.WorkerKey{
		ControllerID: h.controllerID, TreeID: request.TreeID, AgentID: request.Params.WorkspaceID,
	})
	lock.Lock()
	defer lock.Unlock()
	if err := ctx.Err(); err != nil {
		return protocol.WriteWorkspaceArtifactResult{}, err
	}
	descriptor, found := workspaceArtifactDescriptor(state.Transfer.Artifacts, request.Params.Kind)
	name, named := state.ArtifactNames[request.Params.Kind]
	if !found || !named || state.Offsets[request.Params.Kind] != request.Params.Offset ||
		request.Params.Offset+int64(len(request.Params.Data)) > descriptor.Size {
		return protocol.WriteWorkspaceArtifactResult{}, errors.New("target workspace artifact write is out of sequence")
	}
	file, err := h.workspaceRoot.OpenFile(name, os.O_WRONLY, 0)
	if err != nil {
		return protocol.WriteWorkspaceArtifactResult{}, fmt.Errorf("open target workspace artifact: %w", err)
	}
	written, writeErr := file.WriteAt(request.Params.Data, request.Params.Offset)
	closeErr := file.Close()
	if writeErr != nil {
		return protocol.WriteWorkspaceArtifactResult{}, fmt.Errorf("write target workspace artifact: %w", writeErr)
	}
	if written != len(request.Params.Data) {
		return protocol.WriteWorkspaceArtifactResult{}, io.ErrShortWrite
	}
	if closeErr != nil {
		return protocol.WriteWorkspaceArtifactResult{}, fmt.Errorf("close target workspace artifact: %w", closeErr)
	}
	_, _ = state.Hashes[request.Params.Kind].Write(request.Params.Data)
	nextOffset := request.Params.Offset + int64(written)
	state.Offsets[request.Params.Kind] = nextOffset
	return protocol.WriteWorkspaceArtifactResult{
		TransferID: request.Params.TransferID, NextOffset: nextOffset,
	}, nil
}

func (h *Host) FinishWorkspaceTransfer(
	ctx context.Context,
	request WorkspaceTransferControlRequest,
) (protocol.FinishWorkspaceTransferResult, error) {
	release, err := h.acquireWorkspaceOperation(ctx)
	if err != nil {
		return protocol.FinishWorkspaceTransferResult{}, err
	}
	defer release()
	if err := validateWorkspaceTransferControl(request, h.controllerID); err != nil {
		return protocol.FinishWorkspaceTransferResult{}, err
	}
	lock := h.lockFor(store.WorkerKey{
		ControllerID: h.controllerID, TreeID: request.TreeID, AgentID: request.Params.WorkspaceID,
	})
	lock.Lock()
	defer lock.Unlock()
	h.workspaceTransferMu.Lock()
	state, exists := h.inboundTransfers[request.Params.TransferID]
	h.workspaceTransferMu.Unlock()
	if !exists {
		prepared, err := h.state.GetPreparedWorkspace(ctx, store.PreparedWorkspaceKey{
			ControllerID: h.controllerID, TreeID: request.TreeID, WorkspaceID: request.Params.WorkspaceID,
		})
		if err != nil || prepared.SourceAgentID != request.Source.AgentID ||
			prepared.SourceDeviceID != request.Source.DeviceID {
			return protocol.FinishWorkspaceTransferResult{}, errors.New("target workspace transfer was not found")
		}
		return protocol.FinishWorkspaceTransferResult{Workspace: preparedWorkspaceResult(prepared)}, nil
	}
	if state.TreeID != request.TreeID || state.Source != request.Source ||
		state.Transfer.WorkspaceID != request.Params.WorkspaceID {
		return protocol.FinishWorkspaceTransferResult{}, errors.New("target workspace transfer authority is invalid")
	}
	for _, artifact := range state.Transfer.Artifacts {
		if state.Offsets[artifact.Kind] != artifact.Size ||
			hex.EncodeToString(state.Hashes[artifact.Kind].Sum(nil)) != artifact.SHA256 {
			return protocol.FinishWorkspaceTransferResult{}, errors.New("target workspace artifact digest does not match its descriptor")
		}
		file, err := h.workspaceRoot.OpenFile(state.ArtifactNames[artifact.Kind], os.O_RDWR, 0)
		if err != nil {
			return protocol.FinishWorkspaceTransferResult{}, fmt.Errorf("open completed target artifact: %w", err)
		}
		syncErr := file.Sync()
		closeErr := file.Close()
		if syncErr != nil || closeErr != nil {
			return protocol.FinishWorkspaceTransferResult{}, errors.Join(syncErr, closeErr)
		}
	}
	bundleName, found := state.ArtifactNames[protocol.WorkspaceArtifactBundle]
	if !found {
		return protocol.FinishWorkspaceTransferResult{}, errors.New("clean workspace transfer is missing its Git bundle")
	}
	if err := h.git.ApplyBundle(
		ctx,
		filepath.Join(h.workspaceRoot.Name(), state.PendingName),
		filepath.Join(h.workspaceRoot.Name(), bundleName),
		state.Manifest,
	); err != nil {
		return protocol.FinishWorkspaceTransferResult{}, err
	}
	prepared, err := h.publishPreparedWorkspace(
		ctx, request.TreeID, request.Source, request.Params.WorkspaceID,
		state.PendingName, state.Manifest, state.Transfer.Strategy, state.Transfer.Warnings,
	)
	if err != nil {
		return protocol.FinishWorkspaceTransferResult{}, err
	}
	h.workspaceTransferMu.Lock()
	delete(h.inboundTransfers, request.Params.TransferID)
	pendingKey := workspacePreparationKey(request.TreeID, request.Params.WorkspaceID)
	if pending, found := h.pendingWorkspaces[pendingKey]; found &&
		pending.TransferID == request.Params.TransferID && pending.TemporaryName == state.PendingName {
		delete(h.pendingWorkspaces, pendingKey)
	}
	h.workspaceTransferMu.Unlock()
	if err := h.workspaceRoot.RemoveAll(state.DirectoryName); err != nil {
		h.reportError(fmt.Errorf("remove completed target workspace transfer: %w", err))
	} else if err := h.syncWorkspaceDirectory(); err != nil {
		h.reportError(err)
	}
	return protocol.FinishWorkspaceTransferResult{Workspace: prepared}, nil
}

func (h *Host) CancelWorkspaceTransfer(
	ctx context.Context,
	request WorkspaceTransferControlRequest,
) (protocol.CancelWorkspaceTransferResult, error) {
	release, err := h.acquireWorkspaceOperation(ctx)
	if err != nil {
		return protocol.CancelWorkspaceTransferResult{}, err
	}
	defer release()
	if err := validateWorkspaceTransferControl(request, h.controllerID); err != nil {
		return protocol.CancelWorkspaceTransferResult{}, err
	}
	lock := h.lockFor(store.WorkerKey{
		ControllerID: h.controllerID, TreeID: request.TreeID, AgentID: request.Params.WorkspaceID,
	})
	lock.Lock()
	defer lock.Unlock()
	h.workspaceTransferMu.Lock()
	outbound, hasOutbound := h.outboundTransfers[request.Params.TransferID]
	inbound, hasInbound := h.inboundTransfers[request.Params.TransferID]
	pendingKey := workspacePreparationKey(request.TreeID, request.Params.WorkspaceID)
	pending, hasPending := h.pendingWorkspaces[pendingKey]
	h.workspaceTransferMu.Unlock()
	names := make([]string, 0, 3)
	if hasOutbound && outbound.TreeID == request.TreeID && outbound.Source == request.Source &&
		outbound.WorkspaceID == request.Params.WorkspaceID {
		names = append(names, outbound.DirectoryName)
	}
	if hasInbound && inbound.TreeID == request.TreeID && inbound.Source == request.Source &&
		inbound.Transfer.WorkspaceID == request.Params.WorkspaceID {
		names = append(names, inbound.DirectoryName)
	}
	pendingMatches := hasPending && pending.Source == request.Source &&
		pending.WorkspaceID == request.Params.WorkspaceID && pending.TransferID == request.Params.TransferID
	ownsProvisionalPending := pendingMatches && !hasInbound && request.Params.TransferID == request.Params.WorkspaceID
	ownsInboundPending := pendingMatches && hasInbound && inbound.TreeID == request.TreeID &&
		inbound.Source == request.Source && inbound.Transfer.WorkspaceID == request.Params.WorkspaceID &&
		pending.TemporaryName == inbound.PendingName
	ownsPending := ownsProvisionalPending || ownsInboundPending
	if ownsPending {
		names = append(names, pending.TemporaryName)
	}
	slices.Sort(names)
	names = slices.Compact(names)
	for _, name := range names {
		if err := h.workspaceRoot.RemoveAll(name); err != nil {
			return protocol.CancelWorkspaceTransferResult{}, fmt.Errorf("remove canceled workspace transfer: %w", err)
		}
	}
	if len(names) != 0 {
		if err := h.syncWorkspaceDirectory(); err != nil {
			return protocol.CancelWorkspaceTransferResult{}, err
		}
	}
	h.workspaceTransferMu.Lock()
	if hasOutbound && outbound.TreeID == request.TreeID && outbound.Source == request.Source &&
		outbound.WorkspaceID == request.Params.WorkspaceID {
		delete(h.outboundTransfers, request.Params.TransferID)
	}
	if hasInbound && inbound.TreeID == request.TreeID && inbound.Source == request.Source &&
		inbound.Transfer.WorkspaceID == request.Params.WorkspaceID {
		delete(h.inboundTransfers, request.Params.TransferID)
	}
	if ownsPending {
		delete(h.pendingWorkspaces, pendingKey)
	}
	h.workspaceTransferMu.Unlock()
	return protocol.CancelWorkspaceTransferResult{TransferID: request.Params.TransferID}, nil
}
