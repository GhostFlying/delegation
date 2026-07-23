package protocol

import (
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/GhostFlying/delegation/internal/identity"
)

type ChangesArtifactStatus string

const (
	ChangesArtifactAvailable     ChangesArtifactStatus = "available"
	ChangesArtifactUnchanged     ChangesArtifactStatus = "unchanged"
	ChangesArtifactCaptureFailed ChangesArtifactStatus = "captureFailed"
)

type PublishChangesArtifactParams struct {
	ArtifactID         string                        `json:"artifactId"`
	TurnID             string                        `json:"turnId"`
	WorkspaceID        string                        `json:"workspaceId"`
	Status             ChangesArtifactStatus         `json:"status"`
	BaseHeadOID        string                        `json:"baseHeadOid"`
	BaseManifestHash   string                        `json:"baseManifestHash"`
	BaseSnapshotHash   string                        `json:"baseSnapshotHash"`
	ResultHeadOID      string                        `json:"resultHeadOid"`
	ResultSnapshotHash string                        `json:"resultSnapshotHash"`
	ResultClean        bool                          `json:"resultClean"`
	Parts              []WorkspaceArtifactDescriptor `json:"parts"`
	BaseWarnings       []string                      `json:"baseWarnings"`
	ResultWarnings     []string                      `json:"resultWarnings"`
	FailureCode        string                        `json:"failureCode"`
}

func (p PublishChangesArtifactParams) Validate() error {
	for _, field := range []struct{ name, value string }{
		{name: "artifactId", value: p.ArtifactID},
		{name: "turnId", value: p.TurnID},
		{name: "workspaceId", value: p.WorkspaceID},
	} {
		if err := identity.ValidateID(field.value); err != nil {
			return fmt.Errorf("%s %w", field.name, err)
		}
	}
	if !gitObjectIDPattern.MatchString(p.BaseHeadOID) {
		return errors.New("baseHeadOid must be a lowercase SHA-1 or SHA-256 object ID")
	}
	if !sha256DigestPattern.MatchString(p.BaseManifestHash) {
		return errors.New("baseManifestHash must be a lowercase SHA-256 digest")
	}
	if !sha256DigestPattern.MatchString(p.BaseSnapshotHash) {
		return errors.New("baseSnapshotHash must be a lowercase SHA-256 digest")
	}
	if err := ValidateWorkspaceWarnings(p.BaseWarnings); err != nil {
		return fmt.Errorf("baseWarnings: %w", err)
	}
	if err := ValidateWorkspaceSourceWarnings(p.ResultWarnings); err != nil {
		return fmt.Errorf("resultWarnings: %w", err)
	}
	if p.Status == ChangesArtifactCaptureFailed && len(p.ResultWarnings) != 0 {
		return errors.New("failed changes artifact must not contain resultWarnings")
	}

	switch p.Status {
	case ChangesArtifactAvailable:
		if p.FailureCode != "" {
			return errors.New("available changes artifact must not contain failureCode")
		}
		if err := p.validateResult(); err != nil {
			return err
		}
		if p.ResultHeadOID == p.BaseHeadOID && p.ResultSnapshotHash == p.BaseSnapshotHash {
			return errors.New("available changes artifact must differ from its base")
		}
		return p.validateAvailableParts()
	case ChangesArtifactUnchanged:
		if p.FailureCode != "" || len(p.Parts) != 0 {
			return errors.New("unchanged changes artifact must not contain parts or failureCode")
		}
		if err := p.validateResult(); err != nil {
			return err
		}
		if p.ResultHeadOID != p.BaseHeadOID || p.ResultSnapshotHash != p.BaseSnapshotHash {
			return errors.New("unchanged changes artifact result must equal its base")
		}
		return nil
	case ChangesArtifactCaptureFailed:
		if err := ValidateFailureCode(p.FailureCode); err != nil {
			return err
		}
		if p.ResultHeadOID != "" || p.ResultSnapshotHash != "" || p.ResultClean || len(p.Parts) != 0 {
			return errors.New("failed changes artifact must not claim a result or parts")
		}
		return nil
	default:
		return fmt.Errorf("unsupported changes artifact status %q", p.Status)
	}
}

func (p PublishChangesArtifactParams) validateResult() error {
	if !gitObjectIDPattern.MatchString(p.ResultHeadOID) || len(p.ResultHeadOID) != len(p.BaseHeadOID) {
		return errors.New("resultHeadOid must use the base Git object format")
	}
	if !sha256DigestPattern.MatchString(p.ResultSnapshotHash) {
		return errors.New("resultSnapshotHash must be a lowercase SHA-256 digest")
	}
	return nil
}

func (p PublishChangesArtifactParams) validateAvailableParts() error {
	if len(p.Parts) > 2 {
		return errors.New("available changes artifact must contain at most two parts")
	}
	var total int64
	previous := WorkspaceArtifactKind("")
	hasBundle := false
	hasOverlay := false
	for _, part := range p.Parts {
		if err := part.Validate(); err != nil {
			return err
		}
		if previous != "" && part.Kind <= previous {
			return errors.New("changes artifact parts must be sorted and unique")
		}
		previous = part.Kind
		total += part.Size
		hasBundle = hasBundle || part.Kind == WorkspaceArtifactBundle
		hasOverlay = hasOverlay || part.Kind == WorkspaceArtifactOverlay
	}
	if total > MaximumWorkspaceTransferBytes {
		return fmt.Errorf("changes artifact exceeds %d-byte limit", MaximumWorkspaceTransferBytes)
	}
	if hasBundle != (p.ResultHeadOID != p.BaseHeadOID) {
		return errors.New("changes bundle does not match the result HEAD")
	}
	if hasOverlay != !p.ResultClean {
		return errors.New("changes overlay does not match result cleanliness")
	}
	if len(p.Parts) == 0 && (p.ResultHeadOID != p.BaseHeadOID || !p.ResultClean) {
		return errors.New("payload-free changes must retain the base HEAD and produce a clean result")
	}
	return nil
}

type PublishChangesArtifactResult struct {
	ArtifactID string `json:"artifactId"`
	Sequence   uint64 `json:"sequence"`
}

func (r PublishChangesArtifactResult) Validate() error {
	if err := identity.ValidateID(r.ArtifactID); err != nil {
		return fmt.Errorf("artifactId %w", err)
	}
	if r.Sequence == 0 || r.Sequence > math.MaxInt64 {
		return errors.New("changes artifact sequence is outside the supported range")
	}
	return nil
}

type ChangesArtifactMetadata struct {
	TreeID             string                        `json:"treeId"`
	ArtifactID         string                        `json:"artifactId"`
	TurnID             string                        `json:"turnId"`
	WorkspaceID        string                        `json:"workspaceId"`
	Status             ChangesArtifactStatus         `json:"status"`
	SourceAgentID      string                        `json:"sourceAgentId"`
	SourceDeviceID     string                        `json:"sourceDeviceId"`
	ObjectFormat       string                        `json:"objectFormat"`
	BaseHeadOID        string                        `json:"baseHeadOid"`
	BaseManifestHash   string                        `json:"baseManifestHash"`
	BaseSnapshotHash   string                        `json:"baseSnapshotHash"`
	BaseClean          bool                          `json:"baseClean"`
	ResultHeadOID      string                        `json:"resultHeadOid"`
	ResultSnapshotHash string                        `json:"resultSnapshotHash"`
	ResultClean        bool                          `json:"resultClean"`
	Parts              []WorkspaceArtifactDescriptor `json:"parts"`
	BaseWarnings       []string                      `json:"baseWarnings"`
	ResultWarnings     []string                      `json:"resultWarnings"`
	FailureCode        string                        `json:"failureCode"`
	Sequence           uint64                        `json:"sequence"`
	ObservedAt         int64                         `json:"observedAt"`
}

func (m ChangesArtifactMetadata) Validate() error {
	for _, field := range []struct{ name, value string }{
		{name: "treeId", value: m.TreeID},
		{name: "sourceAgentId", value: m.SourceAgentID},
		{name: "sourceDeviceId", value: m.SourceDeviceID},
	} {
		if err := identity.ValidateID(field.value); err != nil {
			return fmt.Errorf("%s %w", field.name, err)
		}
	}
	p := PublishChangesArtifactParams{
		ArtifactID: m.ArtifactID, TurnID: m.TurnID, WorkspaceID: m.WorkspaceID,
		Status: m.Status, BaseHeadOID: m.BaseHeadOID,
		BaseManifestHash: m.BaseManifestHash, BaseSnapshotHash: m.BaseSnapshotHash,
		ResultHeadOID: m.ResultHeadOID, ResultSnapshotHash: m.ResultSnapshotHash,
		ResultClean: m.ResultClean, Parts: m.Parts, BaseWarnings: m.BaseWarnings,
		ResultWarnings: m.ResultWarnings, FailureCode: m.FailureCode,
	}
	if err := p.Validate(); err != nil {
		return err
	}
	if (m.ObjectFormat == "sha1" && len(m.BaseHeadOID) != 40) ||
		(m.ObjectFormat == "sha256" && len(m.BaseHeadOID) != 64) ||
		(m.ObjectFormat != "sha1" && m.ObjectFormat != "sha256") {
		return errors.New("objectFormat does not match the base HEAD")
	}
	if m.Status == ChangesArtifactUnchanged && m.ResultClean != m.BaseClean {
		return errors.New("unchanged changes artifact cleanliness must equal its base")
	}
	if m.Status == ChangesArtifactAvailable && len(m.Parts) == 0 &&
		(m.BaseClean || !m.ResultClean || m.ResultHeadOID != m.BaseHeadOID) {
		return errors.New("payload-free changes artifact must reset a dirty base")
	}
	if m.Sequence == 0 || m.Sequence > math.MaxInt64 {
		return errors.New("changes artifact sequence is outside the supported range")
	}
	if m.ObservedAt < 0 {
		return errors.New("changes artifact observedAt must not be negative")
	}
	return nil
}

func SameChangesArtifactParams(left, right PublishChangesArtifactParams) bool {
	return left.ArtifactID == right.ArtifactID && left.TurnID == right.TurnID &&
		left.WorkspaceID == right.WorkspaceID && left.Status == right.Status &&
		left.BaseHeadOID == right.BaseHeadOID && left.BaseManifestHash == right.BaseManifestHash &&
		left.BaseSnapshotHash == right.BaseSnapshotHash && left.ResultHeadOID == right.ResultHeadOID &&
		left.ResultSnapshotHash == right.ResultSnapshotHash && left.ResultClean == right.ResultClean &&
		slices.Equal(left.Parts, right.Parts) && slices.Equal(left.BaseWarnings, right.BaseWarnings) &&
		slices.Equal(left.ResultWarnings, right.ResultWarnings) &&
		left.FailureCode == right.FailureCode
}
