package workerhost

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"path/filepath"
	"slices"
	"time"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/gitworkspace"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
)

type WorkspaceCreateTransferRequest struct {
	TreeID string
	Source control.PrincipalIdentity
	Params protocol.CreateWorkspaceTransferParams
}

type WorkspaceReadArtifactRequest struct {
	TreeID string
	Source control.PrincipalIdentity
	Params protocol.ReadWorkspaceArtifactParams
}

type WorkspaceBeginTransferRequest struct {
	TreeID string
	Source control.PrincipalIdentity
	Params protocol.BeginWorkspaceTransferParams
}

type WorkspaceWriteArtifactRequest struct {
	TreeID string
	Source control.PrincipalIdentity
	Params protocol.WriteWorkspaceArtifactParams
}

type WorkspaceTransferControlRequest struct {
	TreeID string
	Source control.PrincipalIdentity
	Params protocol.WorkspaceTransferControlParams
}

type pendingWorkspacePreparation struct {
	TreeID        string
	Source        control.PrincipalIdentity
	WorkspaceID   string
	TransferID    string
	Manifest      protocol.WorkspaceManifest
	ManifestHash  string
	TemporaryName string
	Base          gitworkspace.BasePreparation
}

type outboundWorkspaceTransfer struct {
	TreeID        string
	Source        control.PrincipalIdentity
	WorkspaceID   string
	DirectoryName string
	Transfer      protocol.WorkspaceTransferManifest
	ArtifactNames map[protocol.WorkspaceArtifactKind]string
}

type inboundWorkspaceTransfer struct {
	TreeID        string
	Source        control.PrincipalIdentity
	Manifest      protocol.WorkspaceManifest
	Transfer      protocol.WorkspaceTransferManifest
	DirectoryName string
	PendingName   string
	ArtifactNames map[protocol.WorkspaceArtifactKind]string
	Offsets       map[protocol.WorkspaceArtifactKind]int64
	Hashes        map[protocol.WorkspaceArtifactKind]hash.Hash
}

func (p pendingWorkspacePreparation) matches(
	source control.PrincipalIdentity,
	manifest protocol.WorkspaceManifest,
	manifestHash string,
) bool {
	return p.Source == source && p.ManifestHash == manifestHash &&
		p.Manifest.GitURL == manifest.GitURL && p.Manifest.HeadOID == manifest.HeadOID &&
		p.Manifest.ObjectFormat == manifest.ObjectFormat &&
		p.Manifest.WorkingDirectory == manifest.WorkingDirectory &&
		p.Manifest.Clean == manifest.Clean &&
		p.Manifest.SourceSnapshotHash == manifest.SourceSnapshotHash &&
		slices.Equal(p.Manifest.Warnings, manifest.Warnings)
}

func (p pendingWorkspacePreparation) result() protocol.PrepareWorkspaceResult {
	return protocol.PrepareWorkspaceResult{
		WorkspaceID: p.WorkspaceID, Outcome: protocol.WorkspacePrepareTransferRequired,
		ManifestHash: p.ManifestHash, Warnings: append([]string(nil), p.Manifest.Warnings...),
		BasisOIDs:      append([]string(nil), p.Base.BasisOIDs...),
		BundleRequired: p.Base.BundleRequired, OverlayRequired: p.Base.OverlayRequired,
	}
}

func validateWorkspaceRootRequest(
	treeID string,
	source control.PrincipalIdentity,
	controllerID string,
) error {
	if err := source.Validate(); err != nil {
		return err
	}
	if source.ControllerID != controllerID || source.TreeID != treeID || source.ParentAgentID != "" {
		return errors.New("workspace source is not a tree root")
	}
	return nil
}

func workspacePreparationKey(treeID, workspaceID string) string {
	return treeID + "\x00" + workspaceID
}

func (h *Host) publishPreparedWorkspace(
	ctx context.Context,
	treeID string,
	source control.PrincipalIdentity,
	workspaceID, temporaryName string,
	manifest protocol.WorkspaceManifest,
	strategy protocol.WorkspaceStrategy,
	warnings []string,
) (protocol.PrepareWorkspaceResult, error) {
	manifestHash, err := protocol.WorkspaceManifestHash(manifest)
	if err != nil {
		return protocol.PrepareWorkspaceResult{}, err
	}
	expectedWarnings, err := protocol.WorkspaceWarningsForStrategy(manifest.Warnings, strategy)
	if err != nil || !slices.Equal(expectedWarnings, warnings) {
		return protocol.PrepareWorkspaceResult{}, errors.New("prepared workspace warnings do not match its strategy")
	}
	finalName := workspaceSyncName(treeID, workspaceID)
	committed := false
	published := false
	defer func() {
		if committed {
			return
		}
		if published {
			_ = h.workspaceRoot.RemoveAll(finalName)
			_ = h.syncWorkspaceDirectory()
		} else {
			_ = h.workspaceRoot.RemoveAll(temporaryName)
		}
	}()
	if err := h.workspaceRoot.Rename(temporaryName, finalName); err != nil {
		return protocol.PrepareWorkspaceResult{}, fmt.Errorf("publish prepared workspace: %w", err)
	}
	published = true
	if err := h.syncWorkspaceDirectory(); err != nil {
		return protocol.PrepareWorkspaceResult{}, err
	}
	workspace := store.PreparedWorkspace{
		PreparedWorkspaceKey: store.PreparedWorkspaceKey{
			ControllerID: h.controllerID, TreeID: treeID, WorkspaceID: workspaceID,
		},
		SourceAgentID: source.AgentID, SourceDeviceID: source.DeviceID,
		TargetDeviceID: h.deviceID, GitURL: manifest.GitURL,
		HeadOID: manifest.HeadOID, ObjectFormat: manifest.ObjectFormat,
		WorkingDirectory: manifest.WorkingDirectory, Clean: manifest.Clean,
		SourceSnapshotHash: manifest.SourceSnapshotHash,
		WorkspacePath:      filepath.Join(h.workspaceRoot.Name(), finalName),
		Strategy:           strategy, ManifestHash: manifestHash,
		Warnings: append([]string(nil), warnings...),
	}
	stored, err := h.state.RecordPreparedWorkspace(ctx, workspace, time.Now())
	if err != nil {
		return protocol.PrepareWorkspaceResult{}, err
	}
	committed = true
	return preparedWorkspaceResult(stored), nil
}

func validateWorkspaceTransferControl(
	request WorkspaceTransferControlRequest,
	controllerID string,
) error {
	if err := validateWorkspaceRootRequest(request.TreeID, request.Source, controllerID); err != nil ||
		request.Params.SourceAgentID != request.Source.AgentID ||
		request.Params.SourceDeviceID != request.Source.DeviceID {
		return errors.New("workspace transfer control authority is invalid")
	}
	return request.Params.Validate()
}

func (h *Host) describeWorkspaceArtifact(
	name string,
	kind protocol.WorkspaceArtifactKind,
) (protocol.WorkspaceArtifactDescriptor, error) {
	file, err := h.workspaceRoot.Open(name)
	if err != nil {
		return protocol.WorkspaceArtifactDescriptor{}, fmt.Errorf("open workspace artifact: %w", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() < 1 ||
		info.Size() > protocol.MaximumWorkspaceArtifactBytes {
		return protocol.WorkspaceArtifactDescriptor{}, errors.New("workspace artifact has invalid size or type")
	}
	digest := sha256.New()
	written, err := io.Copy(digest, file)
	if err != nil || written != info.Size() {
		return protocol.WorkspaceArtifactDescriptor{}, errors.New("hash workspace artifact")
	}
	return protocol.WorkspaceArtifactDescriptor{
		Kind: kind, Size: info.Size(), SHA256: hex.EncodeToString(digest.Sum(nil)),
	}, nil
}

func workspaceArtifactDescriptor(
	descriptors []protocol.WorkspaceArtifactDescriptor,
	kind protocol.WorkspaceArtifactKind,
) (protocol.WorkspaceArtifactDescriptor, bool) {
	for _, descriptor := range descriptors {
		if descriptor.Kind == kind {
			return descriptor, true
		}
	}
	return protocol.WorkspaceArtifactDescriptor{}, false
}

func sourceTransferDirectoryName(transferID string) string {
	return "transfer-source-" + transferID
}

func targetTransferDirectoryName(transferID string) string {
	return "transfer-target-" + transferID
}

func workspaceArtifactFileName(kind protocol.WorkspaceArtifactKind) string {
	return "artifact-" + string(kind)
}
