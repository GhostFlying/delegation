package protocol

import (
	"errors"
	"fmt"
	"slices"

	"github.com/GhostFlying/delegation/internal/identity"
)

const (
	WorkspaceArtifactChunkBytes               = 128 * 1024
	MaximumWorkspaceArtifactBytes       int64 = 256 * 1024 * 1024
	MaximumWorkspaceTransferBytes       int64 = 512 * 1024 * 1024
	WorkspaceWarningFullHistoryFallback       = "remote_git_full_history_fallback"
)

type WorkspaceArtifactKind string

const (
	WorkspaceArtifactBundle  WorkspaceArtifactKind = "bundle"
	WorkspaceArtifactOverlay WorkspaceArtifactKind = "overlay"
)

func (k WorkspaceArtifactKind) Validate() error {
	switch k {
	case WorkspaceArtifactBundle, WorkspaceArtifactOverlay:
		return nil
	default:
		return fmt.Errorf("unsupported workspace artifact kind %q", k)
	}
}

type WorkspaceArtifactDescriptor struct {
	Kind   WorkspaceArtifactKind `json:"kind"`
	Size   int64                 `json:"size"`
	SHA256 string                `json:"sha256"`
}

func (d WorkspaceArtifactDescriptor) Validate() error {
	if err := d.Kind.Validate(); err != nil {
		return err
	}
	if d.Size < 1 || d.Size > MaximumWorkspaceArtifactBytes {
		return fmt.Errorf("workspace artifact size must be from 1 through %d bytes", MaximumWorkspaceArtifactBytes)
	}
	if !sha256DigestPattern.MatchString(d.SHA256) {
		return errors.New("workspace artifact sha256 must be a lowercase SHA-256 digest")
	}
	return nil
}

type WorkspaceTransferManifest struct {
	TransferID   string                        `json:"transferId"`
	WorkspaceID  string                        `json:"workspaceId"`
	Strategy     WorkspaceStrategy             `json:"strategy"`
	ManifestHash string                        `json:"manifestHash"`
	Artifacts    []WorkspaceArtifactDescriptor `json:"artifacts"`
	Warnings     []string                      `json:"warnings"`
}

func (m WorkspaceTransferManifest) Validate() error {
	for _, field := range []struct{ name, value string }{
		{name: "transferId", value: m.TransferID},
		{name: "workspaceId", value: m.WorkspaceID},
	} {
		if err := identity.ValidateID(field.value); err != nil {
			return fmt.Errorf("%s %w", field.name, err)
		}
	}
	if err := m.Strategy.Validate(); err != nil {
		return err
	}
	if !sha256DigestPattern.MatchString(m.ManifestHash) {
		return errors.New("manifestHash must be a lowercase SHA-256 digest")
	}
	if len(m.Artifacts) < 1 || len(m.Artifacts) > 2 {
		return errors.New("workspace transfer must contain from 1 through 2 artifacts")
	}
	var total int64
	previous := WorkspaceArtifactKind("")
	hasBundle := false
	for _, artifact := range m.Artifacts {
		if err := artifact.Validate(); err != nil {
			return err
		}
		if previous != "" && artifact.Kind <= previous {
			return errors.New("workspace artifacts must be sorted and unique")
		}
		previous = artifact.Kind
		total += artifact.Size
		hasBundle = hasBundle || artifact.Kind == WorkspaceArtifactBundle
	}
	if total > MaximumWorkspaceTransferBytes {
		return fmt.Errorf("workspace transfer exceeds %d-byte limit", MaximumWorkspaceTransferBytes)
	}
	if (m.Strategy == WorkspaceStrategyThin || m.Strategy == WorkspaceStrategyFull) != hasBundle {
		return errors.New("workspace transfer strategy does not match its bundle artifact")
	}
	if err := ValidateWorkspaceWarnings(m.Warnings); err != nil {
		return err
	}
	hasFullWarning := slices.Contains(m.Warnings, WorkspaceWarningFullHistoryFallback)
	if (m.Strategy == WorkspaceStrategyFull) != hasFullWarning {
		return errors.New("self-contained bundle warning does not match transfer strategy")
	}
	return nil
}

func WorkspaceWarningsForStrategy(source []string, strategy WorkspaceStrategy) ([]string, error) {
	if err := ValidateWorkspaceWarnings(source); err != nil {
		return nil, err
	}
	if err := strategy.Validate(); err != nil {
		return nil, err
	}
	warnings := append([]string(nil), source...)
	if strategy == WorkspaceStrategyFull {
		warnings = append(warnings, WorkspaceWarningFullHistoryFallback)
	}
	slices.Sort(warnings)
	warnings = slices.Compact(warnings)
	if err := ValidateWorkspaceWarnings(warnings); err != nil {
		return nil, err
	}
	return warnings, nil
}

type CreateWorkspaceTransferParams struct {
	TransferID      string            `json:"transferId"`
	WorkspaceID     string            `json:"workspaceId"`
	GitURL          string            `json:"gitUrl"`
	SourcePath      string            `json:"sourcePath"`
	Manifest        WorkspaceManifest `json:"manifest"`
	BasisOIDs       []string          `json:"basisOids"`
	BundleRequired  bool              `json:"bundleRequired"`
	OverlayRequired bool              `json:"overlayRequired"`
}

func (p CreateWorkspaceTransferParams) Validate() error {
	for _, field := range []struct{ name, value string }{
		{name: "transferId", value: p.TransferID},
		{name: "workspaceId", value: p.WorkspaceID},
	} {
		if err := identity.ValidateID(field.value); err != nil {
			return fmt.Errorf("%s %w", field.name, err)
		}
	}
	if err := validateWorkspaceSource(p.GitURL, p.SourcePath); err != nil {
		return err
	}
	if err := p.Manifest.Validate(); err != nil {
		return err
	}
	if p.Manifest.GitURL != p.GitURL {
		return errors.New("transfer Git URL does not match the source manifest")
	}
	if !p.BundleRequired && !p.OverlayRequired {
		return errors.New("workspace transfer must request an artifact")
	}
	if p.Manifest.Clean == p.OverlayRequired {
		return errors.New("overlay requirement does not match source cleanliness")
	}
	if len(p.BasisOIDs) > MaximumWorkspaceBasisOIDs {
		return fmt.Errorf("basisOids exceeds limit of %d", MaximumWorkspaceBasisOIDs)
	}
	for _, oid := range p.BasisOIDs {
		if !gitObjectIDPattern.MatchString(oid) {
			return errors.New("basisOids contains an invalid object ID")
		}
		if p.Manifest.ObjectFormat == "sha1" && len(oid) != 40 ||
			p.Manifest.ObjectFormat == "sha256" && len(oid) != 64 {
			return errors.New("basisOids object format does not match the source manifest")
		}
	}
	if !slices.IsSorted(p.BasisOIDs) || len(slices.Compact(slices.Clone(p.BasisOIDs))) != len(p.BasisOIDs) {
		return errors.New("basisOids must be sorted and unique")
	}
	if !p.BundleRequired && len(p.BasisOIDs) != 0 {
		return errors.New("basisOids require a bundle artifact")
	}
	return nil
}

type CreateWorkspaceTransferResult struct {
	Transfer WorkspaceTransferManifest `json:"transfer"`
}

func (r CreateWorkspaceTransferResult) Validate() error {
	return r.Transfer.Validate()
}

type ReadWorkspaceArtifactParams struct {
	TransferID string                `json:"transferId"`
	Kind       WorkspaceArtifactKind `json:"kind"`
	Offset     int64                 `json:"offset"`
	Limit      int                   `json:"limit"`
}

func (p ReadWorkspaceArtifactParams) Validate() error {
	if err := identity.ValidateID(p.TransferID); err != nil {
		return fmt.Errorf("transferId %w", err)
	}
	if err := p.Kind.Validate(); err != nil {
		return err
	}
	if p.Offset < 0 || p.Offset >= MaximumWorkspaceArtifactBytes {
		return errors.New("artifact offset is out of range")
	}
	if p.Limit < 1 || p.Limit > WorkspaceArtifactChunkBytes {
		return fmt.Errorf("artifact chunk limit must be from 1 through %d", WorkspaceArtifactChunkBytes)
	}
	return nil
}

type ReadWorkspaceArtifactResult struct {
	TransferID string                `json:"transferId"`
	Kind       WorkspaceArtifactKind `json:"kind"`
	Offset     int64                 `json:"offset"`
	Data       []byte                `json:"data"`
	NextOffset int64                 `json:"nextOffset"`
}

func (r ReadWorkspaceArtifactResult) Validate() error {
	if err := identity.ValidateID(r.TransferID); err != nil {
		return fmt.Errorf("transferId %w", err)
	}
	if err := r.Kind.Validate(); err != nil {
		return err
	}
	dataLength := int64(len(r.Data))
	if r.Offset < 0 || r.Offset >= MaximumWorkspaceArtifactBytes || dataLength < 1 ||
		dataLength > WorkspaceArtifactChunkBytes || dataLength > MaximumWorkspaceArtifactBytes-r.Offset ||
		r.NextOffset != r.Offset+dataLength {
		return errors.New("workspace artifact read result has invalid bounds")
	}
	return nil
}

type BeginWorkspaceTransferParams struct {
	SourceAgentID  string                    `json:"sourceAgentId"`
	SourceDeviceID string                    `json:"sourceDeviceId"`
	Manifest       WorkspaceManifest         `json:"manifest"`
	Transfer       WorkspaceTransferManifest `json:"transfer"`
}

func (p BeginWorkspaceTransferParams) Validate() error {
	for _, field := range []struct{ name, value string }{
		{name: "sourceAgentId", value: p.SourceAgentID},
		{name: "sourceDeviceId", value: p.SourceDeviceID},
	} {
		if err := identity.ValidateID(field.value); err != nil {
			return fmt.Errorf("%s %w", field.name, err)
		}
	}
	if err := p.Manifest.Validate(); err != nil {
		return err
	}
	if err := p.Transfer.Validate(); err != nil {
		return err
	}
	hash, err := WorkspaceManifestHash(p.Manifest)
	if err != nil || hash != p.Transfer.ManifestHash {
		return errors.New("workspace transfer does not match the source manifest")
	}
	expectedWarnings, err := WorkspaceWarningsForStrategy(p.Manifest.Warnings, p.Transfer.Strategy)
	if err != nil || !slices.Equal(expectedWarnings, p.Transfer.Warnings) {
		return errors.New("workspace transfer warnings do not match the source manifest")
	}
	hasOverlay := slices.ContainsFunc(p.Transfer.Artifacts, func(artifact WorkspaceArtifactDescriptor) bool {
		return artifact.Kind == WorkspaceArtifactOverlay
	})
	if hasOverlay == p.Manifest.Clean {
		return errors.New("workspace overlay does not match source cleanliness")
	}
	return nil
}

type BeginWorkspaceTransferResult struct {
	TransferID string `json:"transferId"`
}

func (r BeginWorkspaceTransferResult) Validate() error {
	return identity.ValidateID(r.TransferID)
}

type WriteWorkspaceArtifactParams struct {
	WorkspaceID string                `json:"workspaceId"`
	TransferID  string                `json:"transferId"`
	Kind        WorkspaceArtifactKind `json:"kind"`
	Offset      int64                 `json:"offset"`
	Data        []byte                `json:"data"`
}

func (p WriteWorkspaceArtifactParams) Validate() error {
	for _, field := range []struct{ name, value string }{
		{name: "workspaceId", value: p.WorkspaceID},
		{name: "transferId", value: p.TransferID},
	} {
		if err := identity.ValidateID(field.value); err != nil {
			return fmt.Errorf("%s %w", field.name, err)
		}
	}
	if err := p.Kind.Validate(); err != nil {
		return err
	}
	dataLength := int64(len(p.Data))
	if p.Offset < 0 || p.Offset >= MaximumWorkspaceArtifactBytes || dataLength < 1 ||
		dataLength > WorkspaceArtifactChunkBytes || dataLength > MaximumWorkspaceArtifactBytes-p.Offset {
		return errors.New("workspace artifact write has invalid bounds")
	}
	return nil
}

type WriteWorkspaceArtifactResult struct {
	TransferID string `json:"transferId"`
	NextOffset int64  `json:"nextOffset"`
}

func (r WriteWorkspaceArtifactResult) Validate() error {
	if err := identity.ValidateID(r.TransferID); err != nil {
		return fmt.Errorf("transferId %w", err)
	}
	if r.NextOffset < 1 || r.NextOffset > MaximumWorkspaceArtifactBytes {
		return errors.New("workspace artifact nextOffset is out of range")
	}
	return nil
}

type WorkspaceTransferControlParams struct {
	WorkspaceID    string `json:"workspaceId"`
	TransferID     string `json:"transferId"`
	SourceAgentID  string `json:"sourceAgentId"`
	SourceDeviceID string `json:"sourceDeviceId"`
}

func (p WorkspaceTransferControlParams) Validate() error {
	for _, field := range []struct{ name, value string }{
		{name: "workspaceId", value: p.WorkspaceID},
		{name: "transferId", value: p.TransferID},
		{name: "sourceAgentId", value: p.SourceAgentID},
		{name: "sourceDeviceId", value: p.SourceDeviceID},
	} {
		if err := identity.ValidateID(field.value); err != nil {
			return fmt.Errorf("%s %w", field.name, err)
		}
	}
	return nil
}

type FinishWorkspaceTransferResult struct {
	Workspace PrepareWorkspaceResult `json:"workspace"`
}

func (r FinishWorkspaceTransferResult) Validate() error {
	if err := r.Workspace.Validate(); err != nil {
		return err
	}
	if r.Workspace.Outcome != WorkspacePrepareReady {
		return errors.New("finished workspace transfer must return a ready workspace")
	}
	return nil
}

type CancelWorkspaceTransferResult struct {
	TransferID string `json:"transferId"`
}

func (r CancelWorkspaceTransferResult) Validate() error {
	return identity.ValidateID(r.TransferID)
}
