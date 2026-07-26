package protocol

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/GhostFlying/delegation/internal/identity"
)

const (
	ResultManifestVersion = 2

	ResultManifestFileName       = "result-manifest.json"
	ResultRolloutFileName        = "rollout.jsonl.zst"
	ResultChangesBundleFileName  = "changes.bundle"
	ResultChangesOverlayFileName = "changes-overlay.tar.zst"

	ResultPackageChunkBytes                = 128 * 1024
	MaximumResultManifestBytes       int64 = 64 * 1024
	MaximumResultRolloutBytes        int64 = 64 * 1024 * 1024
	MaximumResultRolloutRawBytes     int64 = 64 * 1024 * 1024
	MaximumResultChangesBundleBytes  int64 = 256 * 1024 * 1024
	MaximumResultChangesOverlayBytes int64 = 256 * 1024 * 1024
	MaximumResultPackageBytes              = MaximumResultManifestBytes + MaximumResultRolloutBytes +
		MaximumResultChangesBundleBytes + MaximumResultChangesOverlayBytes
	MaximumResultPackagePayloadParts = 3
)

type ResultPackagePartKind string

const (
	ResultPackagePartChangesBundle  ResultPackagePartKind = "changesBundle"
	ResultPackagePartChangesOverlay ResultPackagePartKind = "changesOverlay"
	ResultPackagePartManifest       ResultPackagePartKind = "manifest"
	ResultPackagePartRollout        ResultPackagePartKind = "rollout"
)

func (k ResultPackagePartKind) Validate() error {
	switch k {
	case ResultPackagePartChangesBundle,
		ResultPackagePartChangesOverlay,
		ResultPackagePartManifest,
		ResultPackagePartRollout:
		return nil
	default:
		return fmt.Errorf("unsupported result package part kind %q", k)
	}
}

func (k ResultPackagePartKind) FileName() (string, error) {
	switch k {
	case ResultPackagePartChangesBundle:
		return ResultChangesBundleFileName, nil
	case ResultPackagePartChangesOverlay:
		return ResultChangesOverlayFileName, nil
	case ResultPackagePartManifest:
		return ResultManifestFileName, nil
	case ResultPackagePartRollout:
		return ResultRolloutFileName, nil
	default:
		return "", fmt.Errorf("unsupported result package part kind %q", k)
	}
}

func (k ResultPackagePartKind) MaximumBytes() (int64, error) {
	switch k {
	case ResultPackagePartChangesBundle:
		return MaximumResultChangesBundleBytes, nil
	case ResultPackagePartChangesOverlay:
		return MaximumResultChangesOverlayBytes, nil
	case ResultPackagePartManifest:
		return MaximumResultManifestBytes, nil
	case ResultPackagePartRollout:
		return MaximumResultRolloutBytes, nil
	default:
		return 0, fmt.Errorf("unsupported result package part kind %q", k)
	}
}

func (k ResultPackagePartKind) validatePayloadKind() error {
	if err := k.Validate(); err != nil {
		return err
	}
	if k == ResultPackagePartManifest {
		return errors.New("result package payload must not use the manifest part")
	}
	return nil
}

type ResultPackagePartDescriptor struct {
	Kind   ResultPackagePartKind `json:"kind"`
	Size   int64                 `json:"size"`
	SHA256 string                `json:"sha256"`
}

func (d ResultPackagePartDescriptor) Validate() error {
	maximum, err := d.Kind.MaximumBytes()
	if err != nil {
		return err
	}
	if d.Size < 1 || d.Size > maximum {
		return fmt.Errorf("result package %s size must be from 1 through %d bytes", d.Kind, maximum)
	}
	if !sha256DigestPattern.MatchString(d.SHA256) {
		return errors.New("result package part sha256 must be a lowercase SHA-256 digest")
	}
	return nil
}

type ResultTerminalOutcome string

const (
	ResultTerminalCompleted   ResultTerminalOutcome = "completed"
	ResultTerminalFailed      ResultTerminalOutcome = "failed"
	ResultTerminalInterrupted ResultTerminalOutcome = "interrupted"
)

type ResultTerminal struct {
	Outcome     ResultTerminalOutcome `json:"outcome"`
	FailureCode string                `json:"failureCode"`
}

func (t ResultTerminal) Validate() error {
	switch t.Outcome {
	case ResultTerminalCompleted:
		if t.FailureCode != "" {
			return errors.New("completed result terminal must not contain failureCode")
		}
		return nil
	case ResultTerminalFailed, ResultTerminalInterrupted:
		return ValidateFailureCode(t.FailureCode)
	default:
		return fmt.Errorf("unsupported result terminal outcome %q", t.Outcome)
	}
}

type ResultRolloutStatus string

const (
	ResultRolloutAvailable     ResultRolloutStatus = "available"
	ResultRolloutCaptureFailed ResultRolloutStatus = "captureFailed"
)

type ResultRolloutComponent struct {
	Status      ResultRolloutStatus `json:"status"`
	RawSize     int64               `json:"rawSize"`
	RawSHA256   string              `json:"rawSha256"`
	FailureCode string              `json:"failureCode"`
}

func (r ResultRolloutComponent) Validate() error {
	switch r.Status {
	case ResultRolloutAvailable:
		if r.RawSize < 1 || r.RawSize > MaximumResultRolloutRawBytes {
			return fmt.Errorf("rollout rawSize must be from 1 through %d bytes", MaximumResultRolloutRawBytes)
		}
		if !sha256DigestPattern.MatchString(r.RawSHA256) {
			return errors.New("rollout rawSha256 must be a lowercase SHA-256 digest")
		}
		if r.FailureCode != "" {
			return errors.New("available rollout must not contain failureCode")
		}
		return nil
	case ResultRolloutCaptureFailed:
		if r.RawSize != 0 || r.RawSHA256 != "" {
			return errors.New("failed rollout capture must not claim raw bytes")
		}
		return ValidateFailureCode(r.FailureCode)
	default:
		return fmt.Errorf("unsupported rollout status %q", r.Status)
	}
}

type ResultWorkspaceStatus string

const (
	ResultWorkspaceNotManaged    ResultWorkspaceStatus = "notManaged"
	ResultWorkspaceUnchanged     ResultWorkspaceStatus = "unchanged"
	ResultWorkspaceChanged       ResultWorkspaceStatus = "changed"
	ResultWorkspaceCaptureFailed ResultWorkspaceStatus = "captureFailed"
)

type ResultWorkspaceComponent struct {
	Status             ResultWorkspaceStatus `json:"status"`
	WorkspaceID        string                `json:"workspaceId"`
	SourceDeviceID     string                `json:"sourceDeviceId"`
	TargetDeviceID     string                `json:"targetDeviceId"`
	ObjectFormat       string                `json:"objectFormat"`
	BaseHeadOID        string                `json:"baseHeadOid"`
	BaseManifestHash   string                `json:"baseManifestHash"`
	BaseSnapshotHash   string                `json:"baseSnapshotHash"`
	BaseClean          bool                  `json:"baseClean"`
	ResultHeadOID      string                `json:"resultHeadOid"`
	ResultSnapshotHash string                `json:"resultSnapshotHash"`
	ResultClean        bool                  `json:"resultClean"`
	BaseWarnings       []string              `json:"baseWarnings"`
	ResultWarnings     []string              `json:"resultWarnings"`
	FailureCode        string                `json:"failureCode"`
}

func (w ResultWorkspaceComponent) Validate() error {
	if w.BaseWarnings == nil || w.ResultWarnings == nil {
		return errors.New("workspace warning lists must be present")
	}
	if w.Status == ResultWorkspaceNotManaged {
		if w.WorkspaceID != "" || w.SourceDeviceID != "" || w.TargetDeviceID != "" ||
			w.ObjectFormat != "" || w.BaseHeadOID != "" || w.BaseManifestHash != "" ||
			w.BaseSnapshotHash != "" || w.BaseClean || w.ResultHeadOID != "" ||
			w.ResultSnapshotHash != "" || w.ResultClean || len(w.BaseWarnings) != 0 ||
			len(w.ResultWarnings) != 0 || w.FailureCode != "" {
			return errors.New("unmanaged workspace result must not contain workspace state")
		}
		return nil
	}

	for _, field := range []struct{ name, value string }{
		{name: "workspaceId", value: w.WorkspaceID},
		{name: "workspace sourceDeviceId", value: w.SourceDeviceID},
		{name: "workspace targetDeviceId", value: w.TargetDeviceID},
	} {
		if err := identity.ValidateID(field.value); err != nil {
			return fmt.Errorf("%s %w", field.name, err)
		}
	}
	if err := validateResultGitBase(w); err != nil {
		return err
	}
	if err := ValidateWorkspaceWarnings(w.BaseWarnings); err != nil {
		return fmt.Errorf("baseWarnings: %w", err)
	}
	if err := ValidateWorkspaceSourceWarnings(w.ResultWarnings); err != nil {
		return fmt.Errorf("resultWarnings: %w", err)
	}

	switch w.Status {
	case ResultWorkspaceUnchanged:
		if w.FailureCode != "" {
			return errors.New("unchanged workspace result must not contain failureCode")
		}
		if err := validateResultGitResult(w); err != nil {
			return err
		}
		if w.ResultHeadOID != w.BaseHeadOID || w.ResultSnapshotHash != w.BaseSnapshotHash ||
			w.ResultClean != w.BaseClean {
			return errors.New("unchanged workspace result must equal its base")
		}
		return nil
	case ResultWorkspaceChanged:
		if w.FailureCode != "" {
			return errors.New("changed workspace result must not contain failureCode")
		}
		if err := validateResultGitResult(w); err != nil {
			return err
		}
		if w.ResultHeadOID == w.BaseHeadOID && w.ResultSnapshotHash == w.BaseSnapshotHash &&
			w.ResultClean == w.BaseClean {
			return errors.New("changed workspace result must differ from its base")
		}
		return nil
	case ResultWorkspaceCaptureFailed:
		if w.ResultHeadOID != "" || w.ResultSnapshotHash != "" || w.ResultClean ||
			len(w.ResultWarnings) != 0 {
			return errors.New("failed workspace capture must not claim result state")
		}
		return ValidateFailureCode(w.FailureCode)
	default:
		return fmt.Errorf("unsupported workspace status %q", w.Status)
	}
}

func validateResultGitBase(workspace ResultWorkspaceComponent) error {
	if !gitObjectIDPattern.MatchString(workspace.BaseHeadOID) {
		return errors.New("baseHeadOid must be a lowercase SHA-1 or SHA-256 object ID")
	}
	switch workspace.ObjectFormat {
	case "sha1":
		if len(workspace.BaseHeadOID) != 40 {
			return errors.New("SHA-1 workspace must use a 40-byte baseHeadOid")
		}
	case "sha256":
		if len(workspace.BaseHeadOID) != 64 {
			return errors.New("SHA-256 workspace must use a 64-byte baseHeadOid")
		}
	default:
		return fmt.Errorf("unsupported Git object format %q", workspace.ObjectFormat)
	}
	if !sha256DigestPattern.MatchString(workspace.BaseManifestHash) {
		return errors.New("baseManifestHash must be a lowercase SHA-256 digest")
	}
	if !sha256DigestPattern.MatchString(workspace.BaseSnapshotHash) {
		return errors.New("baseSnapshotHash must be a lowercase SHA-256 digest")
	}
	return nil
}

func validateResultGitResult(workspace ResultWorkspaceComponent) error {
	if !gitObjectIDPattern.MatchString(workspace.ResultHeadOID) ||
		len(workspace.ResultHeadOID) != len(workspace.BaseHeadOID) {
		return errors.New("resultHeadOid must use the base Git object format")
	}
	if !sha256DigestPattern.MatchString(workspace.ResultSnapshotHash) {
		return errors.New("resultSnapshotHash must be a lowercase SHA-256 digest")
	}
	return nil
}

type ResultManifest struct {
	Version           int                           `json:"version"`
	PackageID         string                        `json:"packageId"`
	ControllerID      string                        `json:"controllerId"`
	TreeID            string                        `json:"treeId"`
	SourceAgentID     string                        `json:"sourceAgentId"`
	SourceDeviceID    string                        `json:"sourceDeviceId"`
	ManagedThreadID   string                        `json:"managedThreadId"`
	TurnID            string                        `json:"turnId"`
	LifecycleRevision uint64                        `json:"lifecycleRevision"`
	Terminal          ResultTerminal                `json:"terminal"`
	CapturedAt        int64                         `json:"capturedAt"`
	Rollout           ResultRolloutComponent        `json:"rollout"`
	Workspace         ResultWorkspaceComponent      `json:"workspace"`
	Parts             []ResultPackagePartDescriptor `json:"parts"`
}

func (m ResultManifest) Validate() error {
	if m.Version != ResultManifestVersion {
		return fmt.Errorf("unsupported result manifest version %d", m.Version)
	}
	for _, field := range []struct{ name, value string }{
		{name: "packageId", value: m.PackageID},
		{name: "controllerId", value: m.ControllerID},
		{name: "treeId", value: m.TreeID},
		{name: "sourceAgentId", value: m.SourceAgentID},
		{name: "sourceDeviceId", value: m.SourceDeviceID},
		{name: "managedThreadId", value: m.ManagedThreadID},
		{name: "turnId", value: m.TurnID},
	} {
		if err := identity.ValidateID(field.value); err != nil {
			return fmt.Errorf("%s %w", field.name, err)
		}
	}
	if m.LifecycleRevision == 0 || m.LifecycleRevision > math.MaxInt64 {
		return errors.New("result lifecycleRevision is outside the supported range")
	}
	if m.CapturedAt < 0 {
		return errors.New("result capturedAt must not be negative")
	}
	if err := m.Terminal.Validate(); err != nil {
		return err
	}
	if err := m.Rollout.Validate(); err != nil {
		return err
	}
	if err := m.Workspace.Validate(); err != nil {
		return err
	}
	if m.Workspace.Status != ResultWorkspaceNotManaged &&
		m.Workspace.TargetDeviceID != m.SourceDeviceID {
		return errors.New("workspace targetDeviceId must match result sourceDeviceId")
	}
	return m.validateParts()
}

func (m ResultManifest) validateParts() error {
	if m.Parts == nil {
		return errors.New("result manifest parts must be present")
	}
	if len(m.Parts) > MaximumResultPackagePayloadParts {
		return fmt.Errorf("result manifest must contain at most %d payload parts", MaximumResultPackagePayloadParts)
	}
	var total int64
	previous := ResultPackagePartKind("")
	hasRollout := false
	hasBundle := false
	hasOverlay := false
	for index, part := range m.Parts {
		if err := part.Validate(); err != nil {
			return fmt.Errorf("parts[%d]: %w", index, err)
		}
		if err := part.Kind.validatePayloadKind(); err != nil {
			return fmt.Errorf("parts[%d]: %w", index, err)
		}
		if previous != "" && part.Kind <= previous {
			return errors.New("result package parts must be sorted and unique")
		}
		previous = part.Kind
		if part.Size > MaximumResultPackageBytes-total {
			return fmt.Errorf("result package exceeds %d-byte limit", MaximumResultPackageBytes)
		}
		total += part.Size
		hasRollout = hasRollout || part.Kind == ResultPackagePartRollout
		hasBundle = hasBundle || part.Kind == ResultPackagePartChangesBundle
		hasOverlay = hasOverlay || part.Kind == ResultPackagePartChangesOverlay
	}
	if hasRollout != (m.Rollout.Status == ResultRolloutAvailable) {
		return errors.New("rollout part does not match rollout component status")
	}
	switch m.Workspace.Status {
	case ResultWorkspaceChanged:
		if hasBundle != (m.Workspace.ResultHeadOID != m.Workspace.BaseHeadOID) {
			return errors.New("changes bundle does not match the result HEAD")
		}
		if hasOverlay != !m.Workspace.ResultClean {
			return errors.New("changes overlay does not match result cleanliness")
		}
		if !hasBundle && !hasOverlay && m.Workspace.BaseClean {
			return errors.New("payload-free workspace changes must reset a dirty base")
		}
	case ResultWorkspaceNotManaged, ResultWorkspaceUnchanged, ResultWorkspaceCaptureFailed:
		if hasBundle || hasOverlay {
			return errors.New("workspace status must not contain changes payloads")
		}
	default:
		return fmt.Errorf("unsupported workspace status %q", m.Workspace.Status)
	}
	return nil
}

func EncodeResultManifest(manifest ResultManifest) ([]byte, ResultPackagePartDescriptor, error) {
	if err := manifest.Validate(); err != nil {
		return nil, ResultPackagePartDescriptor{}, err
	}
	data, err := json.Marshal(manifest)
	if err != nil {
		return nil, ResultPackagePartDescriptor{}, fmt.Errorf("encode result manifest: %w", err)
	}
	data = append(data, '\n')
	if int64(len(data)) > MaximumResultManifestBytes {
		return nil, ResultPackagePartDescriptor{}, fmt.Errorf(
			"result manifest exceeds %d-byte limit", MaximumResultManifestBytes,
		)
	}
	digest := sha256.Sum256(data)
	descriptor := ResultPackagePartDescriptor{
		Kind: ResultPackagePartManifest, Size: int64(len(data)), SHA256: fmt.Sprintf("%x", digest),
	}
	return data, descriptor, nil
}

func DecodeResultManifest(data []byte) (ResultManifest, error) {
	var manifest ResultManifest
	if len(data) < 1 || int64(len(data)) > MaximumResultManifestBytes {
		return manifest, fmt.Errorf("result manifest must contain from 1 through %d bytes", MaximumResultManifestBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return manifest, fmt.Errorf("decode result manifest: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return manifest, errors.New("result manifest must contain exactly one JSON value")
		}
		return manifest, fmt.Errorf("decode trailing result manifest: %w", err)
	}
	if err := validateResultManifestFields(data); err != nil {
		return manifest, err
	}
	if err := manifest.Validate(); err != nil {
		return manifest, err
	}
	return manifest, nil
}

func validateResultManifestFields(data []byte) error {
	top, err := requiredJSONObject(data, "result manifest", []string{
		"version", "packageId", "controllerId", "treeId", "sourceAgentId", "sourceDeviceId",
		"managedThreadId", "turnId", "lifecycleRevision", "terminal", "capturedAt", "rollout",
		"workspace", "parts",
	})
	if err != nil {
		return err
	}
	if _, err := requiredJSONObject(top["terminal"], "result terminal", []string{
		"outcome", "failureCode",
	}); err != nil {
		return err
	}
	if _, err := requiredJSONObject(top["rollout"], "result rollout", []string{
		"status", "rawSize", "rawSha256", "failureCode",
	}); err != nil {
		return err
	}
	if _, err := requiredJSONObject(top["workspace"], "result workspace", []string{
		"status", "workspaceId", "sourceDeviceId", "targetDeviceId", "objectFormat", "baseHeadOid",
		"baseManifestHash", "baseSnapshotHash", "baseClean", "resultHeadOid", "resultSnapshotHash",
		"resultClean", "baseWarnings", "resultWarnings", "failureCode",
	}); err != nil {
		return err
	}
	var parts []json.RawMessage
	if err := json.Unmarshal(top["parts"], &parts); err != nil {
		return fmt.Errorf("decode result manifest parts: %w", err)
	}
	for index, part := range parts {
		if _, err := requiredJSONObject(part, fmt.Sprintf("result part %d", index), []string{
			"kind", "size", "sha256",
		}); err != nil {
			return err
		}
	}
	return nil
}

func requiredJSONObject(data []byte, name string, fields []string) (map[string]json.RawMessage, error) {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(data, &object); err != nil {
		return nil, fmt.Errorf("decode %s fields: %w", name, err)
	}
	if len(object) != len(fields) {
		return nil, fmt.Errorf("%s must contain every required field", name)
	}
	for _, field := range fields {
		value, ok := object[field]
		if !ok || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return nil, fmt.Errorf("%s field %s must be present and non-null", name, field)
		}
	}
	return object, nil
}

type ResultPackageMetadata struct {
	Manifest           []byte                      `json:"manifest"`
	ManifestDescriptor ResultPackagePartDescriptor `json:"manifestDescriptor"`
}

func (m ResultPackageMetadata) Validate() error {
	_, err := m.DecodeManifest()
	return err
}

func (m ResultPackageMetadata) DecodeManifest() (ResultManifest, error) {
	if err := m.ManifestDescriptor.Validate(); err != nil {
		return ResultManifest{}, err
	}
	if m.ManifestDescriptor.Kind != ResultPackagePartManifest {
		return ResultManifest{}, errors.New("result package metadata descriptor must describe the manifest")
	}
	if int64(len(m.Manifest)) != m.ManifestDescriptor.Size {
		return ResultManifest{}, errors.New("result manifest bytes do not match their descriptor size")
	}
	digest := sha256.Sum256(m.Manifest)
	if fmt.Sprintf("%x", digest) != m.ManifestDescriptor.SHA256 {
		return ResultManifest{}, errors.New("result manifest bytes do not match their descriptor digest")
	}
	return DecodeResultManifest(m.Manifest)
}

func SameResultPackageMetadata(left, right ResultPackageMetadata) bool {
	return left.ManifestDescriptor == right.ManifestDescriptor && bytes.Equal(left.Manifest, right.Manifest)
}

type PublishResultPackageParams struct {
	Metadata ResultPackageMetadata `json:"metadata"`
}

func (p PublishResultPackageParams) Validate() error {
	return p.Metadata.Validate()
}

func SamePublishResultPackageParams(left, right PublishResultPackageParams) bool {
	return SameResultPackageMetadata(left.Metadata, right.Metadata)
}

type PublishResultPackageResult struct {
	PackageID string `json:"packageId"`
}

func (r PublishResultPackageResult) Validate() error {
	if err := identity.ValidateID(r.PackageID); err != nil {
		return fmt.Errorf("packageId %w", err)
	}
	return nil
}

type BeginResultPackageParams struct {
	AttemptID      string                `json:"attemptId"`
	PackageID      string                `json:"packageId"`
	LeaseExpiresAt int64                 `json:"leaseExpiresAt"`
	Metadata       ResultPackageMetadata `json:"metadata"`
}

func (p BeginResultPackageParams) Validate() error {
	for _, field := range []struct{ name, value string }{
		{name: "attemptId", value: p.AttemptID},
		{name: "packageId", value: p.PackageID},
	} {
		if err := identity.ValidateID(field.value); err != nil {
			return fmt.Errorf("%s %w", field.name, err)
		}
	}
	if p.LeaseExpiresAt < 1 {
		return errors.New("result package leaseExpiresAt must be positive")
	}
	manifest, err := p.Metadata.DecodeManifest()
	if err != nil {
		return err
	}
	if manifest.PackageID != p.PackageID {
		return errors.New("begin packageId does not match the result manifest")
	}
	return nil
}

func SameBeginResultPackageParams(left, right BeginResultPackageParams) bool {
	return left.AttemptID == right.AttemptID && left.PackageID == right.PackageID &&
		left.LeaseExpiresAt == right.LeaseExpiresAt && SameResultPackageMetadata(left.Metadata, right.Metadata)
}

type ResultPackageBeginOutcome string

const (
	ResultPackageReceiving        ResultPackageBeginOutcome = "receiving"
	ResultPackageAlreadyAvailable ResultPackageBeginOutcome = "alreadyAvailable"
)

type ResultPackagePartOffset struct {
	Kind       ResultPackagePartKind `json:"kind"`
	NextOffset int64                 `json:"nextOffset"`
}

func (o ResultPackagePartOffset) Validate() error {
	if err := o.Kind.validatePayloadKind(); err != nil {
		return err
	}
	maximum, _ := o.Kind.MaximumBytes()
	if o.NextOffset < 0 || o.NextOffset > maximum {
		return fmt.Errorf("result package %s nextOffset is out of range", o.Kind)
	}
	return nil
}

type BeginResultPackageResult struct {
	AttemptID string                    `json:"attemptId"`
	PackageID string                    `json:"packageId"`
	Outcome   ResultPackageBeginOutcome `json:"outcome"`
	Offsets   []ResultPackagePartOffset `json:"offsets"`
}

func (r BeginResultPackageResult) Validate() error {
	for _, field := range []struct{ name, value string }{
		{name: "attemptId", value: r.AttemptID},
		{name: "packageId", value: r.PackageID},
	} {
		if err := identity.ValidateID(field.value); err != nil {
			return fmt.Errorf("%s %w", field.name, err)
		}
	}
	if r.Offsets == nil {
		return errors.New("result package offsets must be present")
	}
	switch r.Outcome {
	case ResultPackageReceiving:
	case ResultPackageAlreadyAvailable:
		if len(r.Offsets) != 0 {
			return errors.New("already available result package must not contain offsets")
		}
	default:
		return fmt.Errorf("unsupported result package begin outcome %q", r.Outcome)
	}
	if len(r.Offsets) > MaximumResultPackagePayloadParts {
		return fmt.Errorf("result package must contain at most %d offsets", MaximumResultPackagePayloadParts)
	}
	previous := ResultPackagePartKind("")
	for index, offset := range r.Offsets {
		if err := offset.Validate(); err != nil {
			return fmt.Errorf("offsets[%d]: %w", index, err)
		}
		if previous != "" && offset.Kind <= previous {
			return errors.New("result package offsets must be sorted and unique")
		}
		previous = offset.Kind
	}
	return nil
}

type ReadResultPackagePartParams struct {
	PackageID string                `json:"packageId"`
	Kind      ResultPackagePartKind `json:"kind"`
	Offset    int64                 `json:"offset"`
	Limit     int                   `json:"limit"`
}

func (p ReadResultPackagePartParams) Validate() error {
	if err := identity.ValidateID(p.PackageID); err != nil {
		return fmt.Errorf("packageId %w", err)
	}
	if err := p.Kind.validatePayloadKind(); err != nil {
		return err
	}
	maximum, _ := p.Kind.MaximumBytes()
	if p.Offset < 0 || p.Offset >= maximum || p.Limit < 1 || p.Limit > ResultPackageChunkBytes ||
		int64(p.Limit) > maximum-p.Offset {
		return errors.New("result package part read has invalid bounds")
	}
	return nil
}

func SameReadResultPackagePartParams(left, right ReadResultPackagePartParams) bool {
	return left == right
}

type ReadResultPackagePartResult struct {
	PackageID  string                `json:"packageId"`
	Kind       ResultPackagePartKind `json:"kind"`
	Offset     int64                 `json:"offset"`
	Data       []byte                `json:"data"`
	NextOffset int64                 `json:"nextOffset"`
}

func (r ReadResultPackagePartResult) Validate() error {
	if err := identity.ValidateID(r.PackageID); err != nil {
		return fmt.Errorf("packageId %w", err)
	}
	if err := r.Kind.validatePayloadKind(); err != nil {
		return err
	}
	maximum, _ := r.Kind.MaximumBytes()
	dataLength := int64(len(r.Data))
	if r.Offset < 0 || r.Offset >= maximum || dataLength < 1 || dataLength > ResultPackageChunkBytes ||
		dataLength > maximum-r.Offset || r.NextOffset != r.Offset+dataLength {
		return errors.New("result package part read result has invalid bounds")
	}
	return nil
}

type WriteResultPackagePartParams struct {
	AttemptID string                `json:"attemptId"`
	PackageID string                `json:"packageId"`
	Kind      ResultPackagePartKind `json:"kind"`
	Offset    int64                 `json:"offset"`
	Data      []byte                `json:"data"`
}

func (p WriteResultPackagePartParams) Validate() error {
	for _, field := range []struct{ name, value string }{
		{name: "attemptId", value: p.AttemptID},
		{name: "packageId", value: p.PackageID},
	} {
		if err := identity.ValidateID(field.value); err != nil {
			return fmt.Errorf("%s %w", field.name, err)
		}
	}
	if err := p.Kind.validatePayloadKind(); err != nil {
		return err
	}
	maximum, _ := p.Kind.MaximumBytes()
	dataLength := int64(len(p.Data))
	if p.Offset < 0 || p.Offset >= maximum || dataLength < 1 || dataLength > ResultPackageChunkBytes ||
		dataLength > maximum-p.Offset {
		return errors.New("result package part write has invalid bounds")
	}
	return nil
}

func SameWriteResultPackagePartParams(left, right WriteResultPackagePartParams) bool {
	return left.AttemptID == right.AttemptID && left.PackageID == right.PackageID &&
		left.Kind == right.Kind && left.Offset == right.Offset && bytes.Equal(left.Data, right.Data)
}

type WriteResultPackagePartResult struct {
	AttemptID  string                `json:"attemptId"`
	PackageID  string                `json:"packageId"`
	Kind       ResultPackagePartKind `json:"kind"`
	NextOffset int64                 `json:"nextOffset"`
}

func (r WriteResultPackagePartResult) Validate() error {
	for _, field := range []struct{ name, value string }{
		{name: "attemptId", value: r.AttemptID},
		{name: "packageId", value: r.PackageID},
	} {
		if err := identity.ValidateID(field.value); err != nil {
			return fmt.Errorf("%s %w", field.name, err)
		}
	}
	if err := r.Kind.validatePayloadKind(); err != nil {
		return err
	}
	maximum, _ := r.Kind.MaximumBytes()
	if r.NextOffset < 1 || r.NextOffset > maximum {
		return errors.New("result package part nextOffset is out of range")
	}
	return nil
}

type FinishResultPackageParams struct {
	AttemptID string `json:"attemptId"`
	PackageID string `json:"packageId"`
}

func (p FinishResultPackageParams) Validate() error {
	return validateResultPackageControl(p.AttemptID, p.PackageID)
}

func SameFinishResultPackageParams(left, right FinishResultPackageParams) bool {
	return left == right
}

type FinishResultPackageResult struct {
	AttemptID string `json:"attemptId"`
	PackageID string `json:"packageId"`
}

func (r FinishResultPackageResult) Validate() error {
	return validateResultPackageControl(r.AttemptID, r.PackageID)
}

type CancelResultPackageParams struct {
	AttemptID string `json:"attemptId"`
	PackageID string `json:"packageId"`
}

func (p CancelResultPackageParams) Validate() error {
	return validateResultPackageControl(p.AttemptID, p.PackageID)
}

func SameCancelResultPackageParams(left, right CancelResultPackageParams) bool {
	return left == right
}

type CancelResultPackageResult struct {
	AttemptID string `json:"attemptId"`
	PackageID string `json:"packageId"`
}

func (r CancelResultPackageResult) Validate() error {
	return validateResultPackageControl(r.AttemptID, r.PackageID)
}

func validateResultPackageControl(attemptID, packageID string) error {
	if err := identity.ValidateID(attemptID); err != nil {
		return fmt.Errorf("attemptId %w", err)
	}
	if err := identity.ValidateID(packageID); err != nil {
		return fmt.Errorf("packageId %w", err)
	}
	return nil
}

type AcknowledgeResultPackageParams struct {
	PackageID string `json:"packageId"`
	Sequence  uint64 `json:"sequence"`
}

func (p AcknowledgeResultPackageParams) Validate() error {
	if err := identity.ValidateID(p.PackageID); err != nil {
		return fmt.Errorf("packageId %w", err)
	}
	if p.Sequence == 0 || p.Sequence > math.MaxInt64 {
		return errors.New("result package sequence is outside the supported range")
	}
	return nil
}

func SameAcknowledgeResultPackageParams(left, right AcknowledgeResultPackageParams) bool {
	return left == right
}

type AcknowledgeResultPackageResult struct {
	PackageID string `json:"packageId"`
	Sequence  uint64 `json:"sequence"`
}

func (r AcknowledgeResultPackageResult) Validate() error {
	return AcknowledgeResultPackageParams(r).Validate()
}

// ReleaseResultPackageParams authorizes the source peer to remove its
// authoritative copy after the broker durably records the source
// acknowledgement. Keeping this separate from delivery acknowledgement closes
// the broker-commit crash window around source payload deletion.
type ReleaseResultPackageParams struct {
	PackageID string `json:"packageId"`
	Sequence  uint64 `json:"sequence"`
}

func (p ReleaseResultPackageParams) Validate() error {
	return AcknowledgeResultPackageParams(p).Validate()
}

func SameReleaseResultPackageParams(left, right ReleaseResultPackageParams) bool {
	return left == right
}

type ReleaseResultPackageResult struct {
	PackageID string `json:"packageId"`
	Sequence  uint64 `json:"sequence"`
}

func (r ReleaseResultPackageResult) Validate() error {
	return ReleaseResultPackageParams(r).Validate()
}
