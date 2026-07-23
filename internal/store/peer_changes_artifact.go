package store

import (
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"

	"github.com/GhostFlying/delegation/internal/identity"
	"github.com/GhostFlying/delegation/internal/protocol"
)

const (
	// MaximumRetainedChangesArtifacts is the published-artifact target for GC callers.
	// Pending artifact records are governed by worker-slot admission instead.
	MaximumRetainedChangesArtifacts    = 64
	MaximumRetainedChangesPayloadBytes = int64(2 * 1024 * 1024 * 1024)
	MaximumChangesArtifactPayloadBytes = protocol.MaximumWorkspaceTransferBytes
	maximumChangesArtifactPage         = 256
	changesCaptureFailureCode          = "changes_capture_failed"
)

const (
	ChangesBundlePartName  = "changes.bundle"
	ChangesOverlayPartName = "changes-overlay.tar.zst"
)

var (
	ErrChangesArtifactConflict   = errors.New("changes artifact conflicts with existing state")
	ErrChangesArtifactTransition = errors.New("invalid changes artifact transition")
	ErrChangesArtifactQuota      = errors.New("changes artifact retention quota exceeded")
	ErrChangesArtifactAuthority  = errors.New("changes artifact is outside the worker workspace authority")
)

type ChangesArtifactState string

const (
	ChangesCapturePending ChangesArtifactState = "capturePending"
	ChangesPublishPending ChangesArtifactState = "publishPending"
	ChangesPublished      ChangesArtifactState = "published"
)

type ChangesCaptureStatus string

const (
	ChangesAvailable     ChangesCaptureStatus = "available"
	ChangesUnchanged     ChangesCaptureStatus = "unchanged"
	ChangesCaptureFailed ChangesCaptureStatus = "captureFailed"
)

type ChangesArtifactPartKind string

const (
	ChangesArtifactBundle  ChangesArtifactPartKind = "bundle"
	ChangesArtifactOverlay ChangesArtifactPartKind = "overlay"
)

type ChangesArtifactPart struct {
	Kind      ChangesArtifactPartKind
	Name      string
	SizeBytes int64
	SHA256    string
}

// ChangesArtifact is the peer-local durable outbox record. Part names are
// fixed relative names under an artifact-owned directory; arbitrary paths are
// intentionally not representable here or in the database.
type ChangesArtifact struct {
	WorkerKey
	ArtifactID            string
	TurnID                string
	WorkspaceID           string
	CompletionTarget      WorkerStatus
	CompletionFailureCode string
	State                 ChangesArtifactState
	Status                ChangesCaptureStatus
	ObjectFormat          string
	BaseHeadOID           string
	BaseClean             bool
	BaseManifestHash      string
	BaseSnapshotHash      string
	BaseWarnings          []string
	ResultHeadOID         string
	ResultSnapshotHash    string
	ResultClean           bool
	Parts                 []ChangesArtifactPart
	ResultWarnings        []string
	FailureCode           string
	RetentionReserved     bool
	ReservedBytes         int64
	PayloadBytes          int64
	BrokerSequence        uint64
	CreatedAt             int64
	UpdatedAt             int64
}

type ChangesArtifactRetention struct {
	Count         int
	ReservedBytes int64
}

type WorkerFinalization struct {
	Worker   WorkerReservation
	Artifact ChangesArtifact
}

type ChangesCaptureResult struct {
	Status             ChangesCaptureStatus
	ResultHeadOID      string
	ResultSnapshotHash string
	ResultClean        bool
	Parts              []ChangesArtifactPart
	ResultWarnings     []string
	FailureCode        string
}

func scanPeerChangesArtifact(scanner rowScanner) (ChangesArtifact, error) {
	var artifact ChangesArtifact
	var bundle, overlay ChangesArtifactPart
	var baseWarningsJSON, resultWarningsJSON string
	if err := scanner.Scan(
		&artifact.ControllerID, &artifact.TreeID, &artifact.AgentID,
		&artifact.TurnID, &artifact.ArtifactID, &artifact.WorkspaceID,
		&artifact.CompletionTarget, &artifact.CompletionFailureCode,
		&artifact.State, &artifact.Status, &artifact.ObjectFormat,
		&artifact.BaseHeadOID, &artifact.BaseClean, &artifact.BaseManifestHash, &artifact.BaseSnapshotHash,
		&baseWarningsJSON,
		&artifact.ResultHeadOID, &artifact.ResultSnapshotHash, &artifact.ResultClean,
		&bundle.Name, &bundle.SizeBytes, &bundle.SHA256,
		&overlay.Name, &overlay.SizeBytes, &overlay.SHA256,
		&resultWarningsJSON, &artifact.FailureCode, &artifact.RetentionReserved,
		&artifact.ReservedBytes, &artifact.PayloadBytes, &artifact.BrokerSequence,
		&artifact.CreatedAt, &artifact.UpdatedAt,
	); errors.Is(err, sql.ErrNoRows) {
		return ChangesArtifact{}, ErrNotFound
	} else if err != nil {
		return ChangesArtifact{}, fmt.Errorf("load changes artifact: %w", err)
	}
	if bundle.Name != "" {
		bundle.Kind = ChangesArtifactBundle
		artifact.Parts = append(artifact.Parts, bundle)
	}
	if overlay.Name != "" {
		overlay.Kind = ChangesArtifactOverlay
		artifact.Parts = append(artifact.Parts, overlay)
	}
	if err := json.Unmarshal([]byte(baseWarningsJSON), &artifact.BaseWarnings); err != nil {
		return ChangesArtifact{}, errors.New("stored changes artifact base warnings are invalid")
	}
	if err := json.Unmarshal([]byte(resultWarningsJSON), &artifact.ResultWarnings); err != nil {
		return ChangesArtifact{}, errors.New("stored changes artifact result warnings are invalid")
	}
	if err := artifact.Validate(); err != nil {
		return ChangesArtifact{}, fmt.Errorf("stored changes artifact is invalid: %w", err)
	}
	return artifact, nil
}

func (a ChangesArtifact) Validate() error {
	if err := validateChangesArtifactIdentity(a.WorkerKey, a.ArtifactID); err != nil {
		return err
	}
	if err := identity.ValidateID(a.TurnID); err != nil {
		return fmt.Errorf("turnId %w", err)
	}
	if err := identity.ValidateID(a.WorkspaceID); err != nil {
		return fmt.Errorf("workspaceId %w", err)
	}
	if err := validateFinalTarget(a.CompletionTarget, a.CompletionFailureCode); err != nil {
		return err
	}
	if !a.State.valid() {
		return fmt.Errorf("unsupported changes artifact state %q", a.State)
	}
	if err := validateObjectID(a.ObjectFormat, a.BaseHeadOID); err != nil {
		return fmt.Errorf("baseHeadOid: %w", err)
	}
	if err := validateDigest("baseManifestHash", a.BaseManifestHash); err != nil {
		return err
	}
	if err := validateDigest("baseSnapshotHash", a.BaseSnapshotHash); err != nil {
		return err
	}
	if err := protocol.ValidateWorkspaceWarnings(a.BaseWarnings); err != nil {
		return fmt.Errorf("base warnings: %w", err)
	}
	if a.CreatedAt < 0 || a.UpdatedAt < a.CreatedAt {
		return errors.New("changes artifact timestamps are invalid")
	}
	if a.ReservedBytes < 0 || a.ReservedBytes > MaximumChangesArtifactPayloadBytes ||
		a.PayloadBytes < 0 || a.PayloadBytes > MaximumChangesArtifactPayloadBytes {
		return errors.New("changes artifact byte count is invalid")
	}
	if a.RetentionReserved != (a.ReservedBytes > 0) {
		return errors.New("changes artifact retention reservation is invalid")
	}
	switch a.State {
	case ChangesCapturePending:
		if a.Status != "" || a.ResultHeadOID != "" || a.ResultSnapshotHash != "" ||
			a.ResultClean || len(a.Parts) != 0 || len(a.ResultWarnings) != 0 || a.FailureCode != "" ||
			a.PayloadBytes != 0 || a.BrokerSequence != 0 {
			return errors.New("capture-pending changes artifact contains capture output")
		}
	case ChangesPublishPending, ChangesPublished:
		result := ChangesCaptureResult{
			Status: a.Status, ResultHeadOID: a.ResultHeadOID,
			ResultSnapshotHash: a.ResultSnapshotHash, ResultClean: a.ResultClean,
			Parts: a.Parts, ResultWarnings: a.ResultWarnings, FailureCode: a.FailureCode,
		}
		if err := validateChangesCaptureForArtifact(a, result); err != nil {
			return err
		}
		if a.State == ChangesPublishPending && a.BrokerSequence != 0 {
			return errors.New("publish-pending changes artifact contains broker sequence")
		}
		if a.State == ChangesPublished && (a.BrokerSequence == 0 || a.BrokerSequence > math.MaxInt64) {
			return errors.New("published changes artifact has invalid broker sequence")
		}
	}
	return nil
}

func validateFinalizationRequest(
	key WorkerKey,
	turnID string,
	target WorkerStatus,
	failureCode string,
) error {
	if err := key.Validate(); err != nil {
		return err
	}
	if err := identity.ValidateID(turnID); err != nil {
		return fmt.Errorf("turnId %w", err)
	}
	return validateFinalTarget(target, failureCode)
}

func validateFinalTarget(target WorkerStatus, failureCode string) error {
	if !target.finalTarget() {
		return fmt.Errorf("unsupported final worker target %q", target)
	}
	if err := validateFailureCode(failureCode); err != nil {
		return err
	}
	if target == WorkerIdle && failureCode != "" {
		return errors.New("idle final target cannot contain failureCode")
	}
	if target != WorkerIdle && failureCode == "" {
		return errors.New("non-idle final target requires failureCode")
	}
	return nil
}

func validateChangesArtifactIdentity(key WorkerKey, artifactID string) error {
	if err := key.Validate(); err != nil {
		return err
	}
	if err := identity.ValidateID(artifactID); err != nil {
		return fmt.Errorf("artifactId %w", err)
	}
	return nil
}

func validateChangesCaptureResult(result ChangesCaptureResult) error {
	if !result.Status.valid() {
		return fmt.Errorf("unsupported changes capture status %q", result.Status)
	}
	if err := protocol.ValidateWorkspaceSourceWarnings(result.ResultWarnings); err != nil {
		return fmt.Errorf("result warnings: %w", err)
	}
	if err := validateFailureCode(result.FailureCode); err != nil {
		return err
	}
	seen := make(map[ChangesArtifactPartKind]struct{}, len(result.Parts))
	for _, part := range result.Parts {
		if err := part.Validate(); err != nil {
			return err
		}
		if _, exists := seen[part.Kind]; exists {
			return fmt.Errorf("duplicate changes artifact part %q", part.Kind)
		}
		seen[part.Kind] = struct{}{}
	}
	if changesPayloadBytes(result.Parts) > MaximumChangesArtifactPayloadBytes {
		return errors.New("changes artifact payload exceeds transfer limit")
	}
	switch result.Status {
	case ChangesAvailable:
		if result.ResultHeadOID == "" || result.ResultSnapshotHash == "" || result.FailureCode != "" {
			return errors.New("available changes capture details are incomplete")
		}
	case ChangesUnchanged:
		if result.ResultHeadOID == "" || result.ResultSnapshotHash == "" ||
			len(result.Parts) != 0 || result.FailureCode != "" {
			return errors.New("unchanged changes capture details are invalid")
		}
	case ChangesCaptureFailed:
		if result.ResultHeadOID != "" || result.ResultSnapshotHash != "" || result.ResultClean ||
			len(result.Parts) != 0 || len(result.ResultWarnings) != 0 || result.FailureCode == "" {
			return errors.New("failed changes capture details are invalid")
		}
	}
	return nil
}

func validateChangesCaptureForArtifact(
	artifact ChangesArtifact,
	result ChangesCaptureResult,
) error {
	if err := validateChangesCaptureResult(result); err != nil {
		return err
	}
	if result.Status == ChangesCaptureFailed {
		return nil
	}
	if err := validateObjectID(artifact.ObjectFormat, result.ResultHeadOID); err != nil {
		return fmt.Errorf("resultHeadOid: %w", err)
	}
	if err := validateDigest("resultSnapshotHash", result.ResultSnapshotHash); err != nil {
		return err
	}
	if result.Status == ChangesUnchanged {
		if result.ResultHeadOID != artifact.BaseHeadOID ||
			result.ResultSnapshotHash != artifact.BaseSnapshotHash ||
			result.ResultClean != artifact.BaseClean {
			return errors.New("unchanged capture does not match the prepared workspace base")
		}
		return nil
	}
	payloadBytes := changesPayloadBytes(result.Parts)
	if payloadBytes == 0 {
		if artifact.BaseClean || result.ResultHeadOID != artifact.BaseHeadOID || !result.ResultClean {
			return errors.New("zero-payload available capture must clean an unchanged dirty base")
		}
		return nil
	}
	if !artifact.RetentionReserved || payloadBytes > artifact.ReservedBytes {
		return ErrChangesArtifactQuota
	}
	return nil
}

func (p ChangesArtifactPart) Validate() error {
	switch p.Kind {
	case ChangesArtifactBundle:
		if p.Name != ChangesBundlePartName {
			return errors.New("bundle part must use its controlled relative name")
		}
	case ChangesArtifactOverlay:
		if p.Name != ChangesOverlayPartName {
			return errors.New("overlay part must use its controlled relative name")
		}
	default:
		return fmt.Errorf("unsupported changes artifact part kind %q", p.Kind)
	}
	if p.SizeBytes < 1 || p.SizeBytes > protocol.MaximumWorkspaceArtifactBytes {
		return fmt.Errorf(
			"changes artifact part size must be from 1 through %d",
			protocol.MaximumWorkspaceArtifactBytes,
		)
	}
	return validateDigest("changes artifact part sha256", p.SHA256)
}

func validateObjectID(objectFormat, oid string) error {
	want := 0
	switch objectFormat {
	case "sha1":
		want = 40
	case "sha256":
		want = 64
	default:
		return fmt.Errorf("unsupported Git object format %q", objectFormat)
	}
	if len(oid) != want || strings.ToLower(oid) != oid {
		return fmt.Errorf("must be a lowercase %s object ID", objectFormat)
	}
	if _, err := hex.DecodeString(oid); err != nil {
		return fmt.Errorf("must be a lowercase %s object ID", objectFormat)
	}
	return nil
}

func validateDigest(name, value string) error {
	if len(value) != 64 || strings.ToLower(value) != value {
		return fmt.Errorf("%s must be a lowercase SHA-256 digest", name)
	}
	if _, err := hex.DecodeString(value); err != nil {
		return fmt.Errorf("%s must be a lowercase SHA-256 digest", name)
	}
	return nil
}

func canonicalChangesParts(parts []ChangesArtifactPart) []ChangesArtifactPart {
	result := slices.Clone(parts)
	slices.SortFunc(result, func(left, right ChangesArtifactPart) int {
		return strings.Compare(string(left.Kind), string(right.Kind))
	})
	return result
}

func splitChangesParts(parts []ChangesArtifactPart) (ChangesArtifactPart, ChangesArtifactPart) {
	var bundle, overlay ChangesArtifactPart
	for _, part := range parts {
		switch part.Kind {
		case ChangesArtifactBundle:
			bundle = part
		case ChangesArtifactOverlay:
			overlay = part
		}
	}
	return bundle, overlay
}

func changesPayloadBytes(parts []ChangesArtifactPart) int64 {
	var total int64
	for _, part := range parts {
		total += part.SizeBytes
	}
	return total
}

func sameChangesCapture(artifact ChangesArtifact, result ChangesCaptureResult) bool {
	return artifact.Status == result.Status &&
		artifact.ResultHeadOID == result.ResultHeadOID &&
		artifact.ResultSnapshotHash == result.ResultSnapshotHash &&
		artifact.ResultClean == result.ResultClean &&
		slices.Equal(artifact.Parts, result.Parts) &&
		slices.Equal(artifact.ResultWarnings, result.ResultWarnings) &&
		artifact.FailureCode == result.FailureCode
}

func (s ChangesArtifactState) valid() bool {
	switch s {
	case ChangesCapturePending, ChangesPublishPending, ChangesPublished:
		return true
	default:
		return false
	}
}

func (s ChangesCaptureStatus) valid() bool {
	switch s {
	case ChangesAvailable, ChangesUnchanged, ChangesCaptureFailed:
		return true
	default:
		return false
	}
}
