package protocol

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"regexp"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/GhostFlying/delegation/internal/identity"
)

const (
	MaximumGitURLBytes            = 4 * 1024
	MaximumSourcePathBytes        = 32 * 1024
	MaximumWorkspaceRelativeBytes = 4 * 1024
	MaximumWorkspaceWarnings      = 16
	MaximumWorkspaceWarningBytes  = 64
	MaximumWorkspaceBasisOIDs     = 128
)

var (
	gitObjectIDPattern      = regexp.MustCompile(`^(?:[0-9a-f]{40}|[0-9a-f]{64})$`)
	sha256DigestPattern     = regexp.MustCompile(`^[0-9a-f]{64}$`)
	workspaceWarningPattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)
)

type WorkspaceStrategy string

const (
	WorkspaceStrategyDirect WorkspaceStrategy = "direct"
	WorkspaceStrategyThin   WorkspaceStrategy = "thinBundle"
	WorkspaceStrategyFull   WorkspaceStrategy = "selfContainedBundle"
)

func (s WorkspaceStrategy) Validate() error {
	switch s {
	case WorkspaceStrategyDirect, WorkspaceStrategyThin, WorkspaceStrategyFull:
		return nil
	default:
		return fmt.Errorf("unsupported workspace strategy %q", s)
	}
}

type WorkspacePrepareOutcome string

const (
	WorkspacePrepareReady            WorkspacePrepareOutcome = "ready"
	WorkspacePrepareTransferRequired WorkspacePrepareOutcome = "transferRequired"
)

func (o WorkspacePrepareOutcome) Validate() error {
	switch o {
	case WorkspacePrepareReady, WorkspacePrepareTransferRequired:
		return nil
	default:
		return fmt.Errorf("unsupported workspace prepare outcome %q", o)
	}
}

type SyncWorkspaceParams struct {
	SyncID         string `json:"syncId"`
	TargetDeviceID string `json:"targetDeviceId"`
	GitURL         string `json:"gitUrl"`
	SourcePath     string `json:"sourcePath"`
}

func (p SyncWorkspaceParams) Validate() error {
	if err := identity.ValidateID(p.SyncID); err != nil {
		return fmt.Errorf("syncId %w", err)
	}
	if err := identity.ValidateID(p.TargetDeviceID); err != nil {
		return fmt.Errorf("targetDeviceId %w", err)
	}
	return validateWorkspaceSource(p.GitURL, p.SourcePath)
}

type InspectWorkspaceParams struct {
	SyncID     string `json:"syncId"`
	GitURL     string `json:"gitUrl"`
	SourcePath string `json:"sourcePath"`
}

func (p InspectWorkspaceParams) Validate() error {
	if err := identity.ValidateID(p.SyncID); err != nil {
		return fmt.Errorf("syncId %w", err)
	}
	return validateWorkspaceSource(p.GitURL, p.SourcePath)
}

type WorkspaceManifest struct {
	GitURL             string   `json:"gitUrl"`
	HeadOID            string   `json:"headOid"`
	ObjectFormat       string   `json:"objectFormat"`
	WorkingDirectory   string   `json:"workingDirectory"`
	Clean              bool     `json:"clean"`
	SourceSnapshotHash string   `json:"sourceSnapshotHash"`
	Warnings           []string `json:"warnings"`
}

func (m WorkspaceManifest) Validate() error {
	if err := validateWorkspaceText("gitUrl", m.GitURL, MaximumGitURLBytes); err != nil {
		return err
	}
	if !gitObjectIDPattern.MatchString(m.HeadOID) {
		return errors.New("headOid must be a lowercase SHA-1 or SHA-256 object ID")
	}
	switch m.ObjectFormat {
	case "sha1":
		if len(m.HeadOID) != 40 {
			return errors.New("SHA-1 repository must use a 40-byte headOid")
		}
	case "sha256":
		if len(m.HeadOID) != 64 {
			return errors.New("SHA-256 repository must use a 64-byte headOid")
		}
	default:
		return fmt.Errorf("unsupported Git object format %q", m.ObjectFormat)
	}
	if len(m.WorkingDirectory) > MaximumWorkspaceRelativeBytes || !utf8.ValidString(m.WorkingDirectory) ||
		strings.ContainsRune(m.WorkingDirectory, '\x00') || path.IsAbs(m.WorkingDirectory) ||
		strings.Contains(m.WorkingDirectory, `\`) || strings.Contains(m.WorkingDirectory, ":") {
		return errors.New("workingDirectory is not a bounded relative path")
	}
	if m.WorkingDirectory == "." || m.WorkingDirectory == ".." ||
		strings.HasPrefix(path.Clean(m.WorkingDirectory), "../") {
		return errors.New("workingDirectory escapes the repository")
	}
	if !sha256DigestPattern.MatchString(m.SourceSnapshotHash) {
		return errors.New("sourceSnapshotHash must be a lowercase SHA-256 digest")
	}
	return ValidateWorkspaceWarnings(m.Warnings)
}

func WorkspaceManifestHash(manifest WorkspaceManifest) (string, error) {
	if err := manifest.Validate(); err != nil {
		return "", err
	}
	manifest.Warnings = append([]string{}, manifest.Warnings...)
	data, err := json.Marshal(manifest)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(data)
	return fmt.Sprintf("%x", digest), nil
}

type InspectWorkspaceResult struct {
	SyncID   string            `json:"syncId"`
	Manifest WorkspaceManifest `json:"manifest"`
}

func (r InspectWorkspaceResult) Validate() error {
	if err := identity.ValidateID(r.SyncID); err != nil {
		return fmt.Errorf("syncId %w", err)
	}
	return r.Manifest.Validate()
}

type PrepareWorkspaceParams struct {
	WorkspaceID    string            `json:"workspaceId"`
	SourceAgentID  string            `json:"sourceAgentId"`
	SourceDeviceID string            `json:"sourceDeviceId"`
	Manifest       WorkspaceManifest `json:"manifest"`
}

func (p PrepareWorkspaceParams) Validate() error {
	for _, field := range []struct {
		name, value string
	}{
		{name: "workspaceId", value: p.WorkspaceID},
		{name: "sourceAgentId", value: p.SourceAgentID},
		{name: "sourceDeviceId", value: p.SourceDeviceID},
	} {
		if err := identity.ValidateID(field.value); err != nil {
			return fmt.Errorf("%s %w", field.name, err)
		}
	}
	return p.Manifest.Validate()
}

type PrepareWorkspaceResult struct {
	WorkspaceID     string                  `json:"workspaceId"`
	Outcome         WorkspacePrepareOutcome `json:"outcome"`
	Strategy        WorkspaceStrategy       `json:"strategy,omitempty"`
	ManifestHash    string                  `json:"manifestHash,omitempty"`
	Warnings        []string                `json:"warnings"`
	BasisOIDs       []string                `json:"basisOids,omitempty"`
	BundleRequired  bool                    `json:"bundleRequired,omitempty"`
	OverlayRequired bool                    `json:"overlayRequired,omitempty"`
}

func (r PrepareWorkspaceResult) Validate() error {
	if err := identity.ValidateID(r.WorkspaceID); err != nil {
		return fmt.Errorf("workspaceId %w", err)
	}
	if err := r.Outcome.Validate(); err != nil {
		return err
	}
	if err := ValidateWorkspaceWarnings(r.Warnings); err != nil {
		return err
	}
	if len(r.BasisOIDs) > MaximumWorkspaceBasisOIDs {
		return fmt.Errorf("basisOids exceeds limit of %d", MaximumWorkspaceBasisOIDs)
	}
	for _, oid := range r.BasisOIDs {
		if !gitObjectIDPattern.MatchString(oid) {
			return errors.New("basisOids contains an invalid object ID")
		}
	}
	if !slices.IsSorted(r.BasisOIDs) || len(slices.Compact(slices.Clone(r.BasisOIDs))) != len(r.BasisOIDs) {
		return errors.New("basisOids must be sorted and unique")
	}
	if r.Outcome == WorkspacePrepareTransferRequired {
		if r.Strategy != "" || !sha256DigestPattern.MatchString(r.ManifestHash) {
			return errors.New("transfer-required result must bind the source manifest without claiming a strategy")
		}
		if !r.BundleRequired && !r.OverlayRequired {
			return errors.New("transfer-required result must request an artifact")
		}
		if !r.BundleRequired && len(r.BasisOIDs) != 0 {
			return errors.New("basisOids require a bundle transfer")
		}
		return nil
	}
	if err := r.Strategy.Validate(); err != nil {
		return err
	}
	if r.BundleRequired || r.OverlayRequired || len(r.BasisOIDs) != 0 {
		return errors.New("ready prepare result must not request transfer artifacts")
	}
	if !sha256DigestPattern.MatchString(r.ManifestHash) {
		return errors.New("manifestHash must be a lowercase SHA-256 digest")
	}
	return nil
}

type WorkspaceSummary struct {
	WorkspaceID      string            `json:"workspaceId"`
	SourceDeviceID   string            `json:"sourceDeviceId"`
	TargetDeviceID   string            `json:"targetDeviceId"`
	HeadOID          string            `json:"headOid"`
	ObjectFormat     string            `json:"objectFormat"`
	WorkingDirectory string            `json:"workingDirectory"`
	Strategy         WorkspaceStrategy `json:"strategy"`
	ManifestHash     string            `json:"manifestHash"`
	Warnings         []string          `json:"warnings"`
}

func (s WorkspaceSummary) Validate() error {
	for _, field := range []struct {
		name, value string
	}{
		{name: "workspaceId", value: s.WorkspaceID},
		{name: "sourceDeviceId", value: s.SourceDeviceID},
		{name: "targetDeviceId", value: s.TargetDeviceID},
	} {
		if err := identity.ValidateID(field.value); err != nil {
			return fmt.Errorf("%s %w", field.name, err)
		}
	}
	manifest := WorkspaceManifest{
		GitURL: "ssh://validated.invalid/repository", HeadOID: s.HeadOID,
		ObjectFormat: s.ObjectFormat, WorkingDirectory: s.WorkingDirectory,
		Clean: true, SourceSnapshotHash: strings.Repeat("0", sha256.Size*2), Warnings: s.Warnings,
	}
	if err := manifest.Validate(); err != nil {
		return err
	}
	if err := s.Strategy.Validate(); err != nil {
		return err
	}
	if !sha256DigestPattern.MatchString(s.ManifestHash) {
		return errors.New("manifestHash must be a lowercase SHA-256 digest")
	}
	return nil
}

type SyncWorkspaceResult struct {
	Workspace *WorkspaceSummary       `json:"workspace,omitempty"`
	Outcome   WorkspacePrepareOutcome `json:"outcome"`
	Warnings  []string                `json:"warnings"`
}

func (r SyncWorkspaceResult) Validate() error {
	if err := r.Outcome.Validate(); err != nil {
		return err
	}
	if err := ValidateWorkspaceWarnings(r.Warnings); err != nil {
		return err
	}
	if r.Outcome != WorkspacePrepareReady {
		return errors.New("workspace sync must not expose an intermediate preparation state")
	}
	if r.Workspace == nil {
		return errors.New("ready sync must return a workspace")
	}
	if err := r.Workspace.Validate(); err != nil {
		return err
	}
	if !slices.Equal(r.Warnings, r.Workspace.Warnings) {
		return errors.New("workspace sync warnings do not match its workspace")
	}
	return nil
}

func ValidateWorkspaceWarnings(warnings []string) error {
	if len(warnings) > MaximumWorkspaceWarnings {
		return fmt.Errorf("workspace warnings exceed limit of %d", MaximumWorkspaceWarnings)
	}
	previous := ""
	for _, warning := range warnings {
		if len(warning) > MaximumWorkspaceWarningBytes || !workspaceWarningPattern.MatchString(warning) {
			return fmt.Errorf("invalid workspace warning %q", warning)
		}
		if previous != "" && warning <= previous {
			return errors.New("workspace warnings must be sorted and unique")
		}
		previous = warning
	}
	return nil
}

func validateWorkspaceText(name, value string, limit int) error {
	if strings.TrimSpace(value) == "" || len(value) > limit || !utf8.ValidString(value) ||
		strings.ContainsRune(value, '\x00') {
		return fmt.Errorf("%s must contain from 1 through %d bytes of valid text", name, limit)
	}
	return nil
}

func validateWorkspaceSource(gitURL, sourcePath string) error {
	if err := validateWorkspaceText("gitUrl", gitURL, MaximumGitURLBytes); err != nil {
		return err
	}
	return validateWorkspaceText("sourcePath", sourcePath, MaximumSourcePathBytes)
}
