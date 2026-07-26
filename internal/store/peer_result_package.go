package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"time"

	"github.com/GhostFlying/delegation/internal/identity"
	"github.com/GhostFlying/delegation/internal/protocol"
)

const (
	MaximumPeerResultPackages         = 64
	MaximumPeerResultStoreBytes       = int64(2 * 1024 * 1024 * 1024)
	MaximumResultInboxRemovalReceipts = 256
	maximumPeerResultPage             = 256
)

var (
	ErrResultPackageAuthority  = errors.New("result package is outside the peer authority")
	ErrResultPackageConflict   = errors.New("result package conflicts with existing state")
	ErrResultPackageQuota      = errors.New("result package retention quota exceeded")
	ErrResultPackageTransition = errors.New("invalid result package transition")
)

type ResultOutboxState string

const (
	ResultOutboxCapturePending  ResultOutboxState = "capturePending"
	ResultOutboxPublishPending  ResultOutboxState = "publishPending"
	ResultOutboxDeliveryPending ResultOutboxState = "deliveryPending"
	ResultOutboxDelivered       ResultOutboxState = "delivered"
	ResultOutboxReleasePending  ResultOutboxState = "releasePending"
)

type ResultInboxState string

const (
	ResultInboxReceiving         ResultInboxState = "receiving"
	ResultInboxAvailable         ResultInboxState = "available"
	ResultInboxEvictionTombstone ResultInboxState = "evictionTombstone"
)

type ResultInboxAvailability string

const (
	ResultInboxAvailabilityReceiving ResultInboxAvailability = "receiving"
	ResultInboxAvailabilityAvailable ResultInboxAvailability = "available"
	ResultInboxAvailabilityEvicted   ResultInboxAvailability = "evicted"
)

type ResultInboxRemovalOutcome string

const (
	ResultInboxRemovalCancelled ResultInboxRemovalOutcome = "cancelled"
	ResultInboxRemovalReclaimed ResultInboxRemovalOutcome = "reclaimed"
)

type ResultInboxRemovalPhase string

const (
	ResultInboxRemovalPrepared  ResultInboxRemovalPhase = "prepared"
	ResultInboxRemovalCompleted ResultInboxRemovalPhase = "completed"
)

type ResultInboxRemoval struct {
	Authority ResultInboxAuthority
	PackageID string
	AttemptID string
	Outcome   ResultInboxRemovalOutcome
	Phase     ResultInboxRemovalPhase
	CreatedAt int64
	UpdatedAt int64
}

func (r ResultInboxRemoval) Validate() error {
	if err := r.Authority.Validate(); err != nil {
		return err
	}
	if err := validateResultPackageAttempt(r.AttemptID, r.PackageID); err != nil {
		return err
	}
	switch r.Outcome {
	case ResultInboxRemovalCancelled, ResultInboxRemovalReclaimed:
	default:
		return fmt.Errorf("unsupported result inbox removal outcome %q", r.Outcome)
	}
	switch r.Phase {
	case ResultInboxRemovalPrepared, ResultInboxRemovalCompleted:
	default:
		return fmt.Errorf("unsupported result inbox removal phase %q", r.Phase)
	}
	if r.CreatedAt < 0 || r.UpdatedAt < r.CreatedAt {
		return errors.New("result inbox removal timestamps are invalid")
	}
	return nil
}

type ResultOutboxKey struct {
	WorkerKey
	SourceDeviceID string
	PackageID      string
}

func (k ResultOutboxKey) Validate() error {
	if err := k.WorkerKey.Validate(); err != nil {
		return err
	}
	if err := identity.ValidateID(k.SourceDeviceID); err != nil {
		return fmt.Errorf("sourceDeviceId %w", err)
	}
	if err := identity.ValidateID(k.PackageID); err != nil {
		return fmt.Errorf("packageId %w", err)
	}
	return nil
}

type ResultInboxAuthority struct {
	ControllerID string
	TreeID       string
	RootAgentID  string
	RootDeviceID string
}

func (a ResultInboxAuthority) Validate() error {
	for _, field := range []struct{ name, value string }{
		{name: "controllerId", value: a.ControllerID},
		{name: "treeId", value: a.TreeID},
		{name: "rootAgentId", value: a.RootAgentID},
		{name: "rootDeviceId", value: a.RootDeviceID},
	} {
		if err := identity.ValidateID(field.value); err != nil {
			return fmt.Errorf("%s %w", field.name, err)
		}
	}
	return nil
}

type ResultOutbox struct {
	ResultOutboxKey
	State                 ResultOutboxState
	Metadata              protocol.ResultPackageMetadata
	Manifest              protocol.ResultManifest
	ReservationLimitBytes int64
	ReservedBytes         int64
	PackageBytes          int64
	DeliverySequence      uint64
	CreatedAt             int64
	UpdatedAt             int64
}

func (r ResultOutbox) Validate() error {
	if err := r.ResultOutboxKey.Validate(); err != nil {
		return err
	}
	if r.ReservationLimitBytes < 1 || r.ReservationLimitBytes > protocol.MaximumResultPackageBytes ||
		r.ReservedBytes < 1 || r.ReservedBytes > r.ReservationLimitBytes {
		return errors.New("result outbox reservation is outside the package bound")
	}
	if r.CreatedAt < 0 || r.UpdatedAt < r.CreatedAt {
		return errors.New("result outbox timestamps are invalid")
	}
	switch r.State {
	case ResultOutboxCapturePending:
		if len(r.Metadata.Manifest) != 0 || r.Metadata.ManifestDescriptor != (protocol.ResultPackagePartDescriptor{}) ||
			!reflect.DeepEqual(r.Manifest, protocol.ResultManifest{}) ||
			r.ReservedBytes != r.ReservationLimitBytes || r.PackageBytes != 0 || r.DeliverySequence != 0 {
			return errors.New("capture-pending result outbox contains captured metadata")
		}
	case ResultOutboxPublishPending, ResultOutboxDeliveryPending, ResultOutboxDelivered,
		ResultOutboxReleasePending:
		if err := validateStoredResultMetadata(r.ResultOutboxKey, r.Metadata, r.Manifest); err != nil {
			return err
		}
		packageBytes, err := resultPackageBytes(r.Metadata)
		if err != nil {
			return err
		}
		if r.PackageBytes != packageBytes || r.ReservedBytes != packageBytes ||
			r.ReservationLimitBytes < packageBytes {
			return errors.New("result outbox retained bytes do not match its metadata")
		}
		if r.State == ResultOutboxDelivered || r.State == ResultOutboxReleasePending {
			if r.DeliverySequence == 0 || r.DeliverySequence > math.MaxInt64 {
				return errors.New("delivered result outbox has invalid sequence")
			}
		} else if r.DeliverySequence != 0 {
			return errors.New("undelivered result outbox contains a sequence")
		}
	default:
		return fmt.Errorf("unsupported result outbox state %q", r.State)
	}
	return nil
}

type ResultInbox struct {
	Authority      ResultInboxAuthority
	PackageID      string
	State          ResultInboxState
	AttemptID      string
	LeaseExpiresAt int64
	Metadata       protocol.ResultPackageMetadata
	Manifest       protocol.ResultManifest
	Offsets        []protocol.ResultPackagePartOffset
	PackageBytes   int64
	CreatedAt      int64
	UpdatedAt      int64
}

func (r ResultInbox) Validate() error {
	if err := r.Authority.Validate(); err != nil {
		return err
	}
	if err := identity.ValidateID(r.PackageID); err != nil {
		return fmt.Errorf("packageId %w", err)
	}
	if err := identity.ValidateID(r.AttemptID); err != nil {
		return fmt.Errorf("attemptId %w", err)
	}
	if r.LeaseExpiresAt < 1 {
		return errors.New("result inbox lease must be positive")
	}
	if r.CreatedAt < 0 || r.UpdatedAt < r.CreatedAt {
		return errors.New("result inbox timestamps are invalid")
	}
	if err := validateInboxMetadata(r.Authority, r.PackageID, r.Metadata, r.Manifest); err != nil {
		return err
	}
	packageBytes, err := resultPackageBytes(r.Metadata)
	if err != nil {
		return err
	}
	if r.PackageBytes != packageBytes {
		return errors.New("result inbox retained bytes do not match its metadata")
	}
	switch r.State {
	case ResultInboxReceiving, ResultInboxAvailable, ResultInboxEvictionTombstone:
	default:
		return fmt.Errorf("unsupported result inbox state %q", r.State)
	}
	if r.Offsets == nil || len(r.Offsets) != len(r.Manifest.Parts) {
		return errors.New("result inbox offsets do not match its payload descriptors")
	}
	for index, offset := range r.Offsets {
		if err := offset.Validate(); err != nil {
			return fmt.Errorf("offsets[%d]: %w", index, err)
		}
		part := r.Manifest.Parts[index]
		if offset.Kind != part.Kind || offset.NextOffset > part.Size {
			return errors.New("result inbox offset is outside its descriptor")
		}
		if r.State != ResultInboxReceiving && offset.NextOffset != part.Size {
			return errors.New("published result inbox contains an incomplete part")
		}
	}
	return nil
}

type ResultPackageRetention struct {
	Count int
	Bytes int64
}

type ResultInboxChunkCommit struct {
	Kind   protocol.ResultPackagePartKind
	Offset int64
	Size   int64
	SHA256 string
}

func (c ResultInboxChunkCommit) Validate() error {
	if err := c.Kind.Validate(); err != nil {
		return err
	}
	if c.Kind == protocol.ResultPackagePartManifest {
		return errors.New("manifest bytes are not transferred as a result payload chunk")
	}
	if c.Offset < 0 || c.Size < 1 || c.Size > protocol.ResultPackageChunkBytes {
		return errors.New("result inbox chunk bounds are invalid")
	}
	return validateDigest("result inbox chunk sha256", c.SHA256)
}

type ResultInboxChunkCommitResult struct {
	NextOffset int64
	Replay     bool
}

func cloneResultMetadata(metadata protocol.ResultPackageMetadata) protocol.ResultPackageMetadata {
	metadata.Manifest = slices.Clone(metadata.Manifest)
	return metadata
}

func resultPackageBytes(metadata protocol.ResultPackageMetadata) (int64, error) {
	manifest, err := metadata.DecodeManifest()
	if err != nil {
		return 0, err
	}
	total := metadata.ManifestDescriptor.Size
	for _, part := range manifest.Parts {
		if part.Size > protocol.MaximumResultPackageBytes-total {
			return 0, errors.New("result package byte count overflows its bound")
		}
		total += part.Size
	}
	if total < 1 || total > protocol.MaximumResultPackageBytes {
		return 0, errors.New("result package is outside its aggregate bound")
	}
	return total, nil
}

func validateStoredResultMetadata(
	key ResultOutboxKey,
	metadata protocol.ResultPackageMetadata,
	manifest protocol.ResultManifest,
) error {
	decoded, err := metadata.DecodeManifest()
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(decoded, manifest) {
		return errors.New("stored result manifest columns do not match the exact metadata bytes")
	}
	if manifest.PackageID != key.PackageID || manifest.ControllerID != key.ControllerID ||
		manifest.TreeID != key.TreeID || manifest.SourceAgentID != key.AgentID ||
		manifest.SourceDeviceID != key.SourceDeviceID {
		return ErrResultPackageAuthority
	}
	return nil
}

func validateInboxMetadata(
	authority ResultInboxAuthority,
	packageID string,
	metadata protocol.ResultPackageMetadata,
	manifest protocol.ResultManifest,
) error {
	decoded, err := metadata.DecodeManifest()
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(decoded, manifest) {
		return errors.New("stored result inbox manifest does not match the exact metadata bytes")
	}
	if manifest.PackageID != packageID || manifest.ControllerID != authority.ControllerID ||
		manifest.TreeID != authority.TreeID {
		return ErrResultPackageAuthority
	}
	return nil
}

func encodeResultParts(parts []protocol.ResultPackagePartDescriptor) (string, error) {
	encoded, err := json.Marshal(parts)
	if err != nil {
		return "", fmt.Errorf("encode result package descriptors: %w", err)
	}
	return string(encoded), nil
}

func decodeResultMetadata(
	manifestBytes []byte,
	manifestSize int64,
	manifestSHA256, partsJSON string,
) (protocol.ResultPackageMetadata, protocol.ResultManifest, error) {
	metadata := protocol.ResultPackageMetadata{
		Manifest: slices.Clone(manifestBytes),
		ManifestDescriptor: protocol.ResultPackagePartDescriptor{
			Kind: protocol.ResultPackagePartManifest, Size: manifestSize, SHA256: manifestSHA256,
		},
	}
	manifest, err := metadata.DecodeManifest()
	if err != nil {
		return protocol.ResultPackageMetadata{}, protocol.ResultManifest{}, fmt.Errorf(
			"stored result package metadata is invalid: %w", err,
		)
	}
	var parts []protocol.ResultPackagePartDescriptor
	if err := json.Unmarshal([]byte(partsJSON), &parts); err != nil {
		return protocol.ResultPackageMetadata{}, protocol.ResultManifest{},
			errors.New("stored result package descriptors are invalid")
	}
	if !slices.Equal(parts, manifest.Parts) {
		return protocol.ResultPackageMetadata{}, protocol.ResultManifest{},
			errors.New("stored result package descriptors do not match the manifest")
	}
	return metadata, manifest, nil
}

func scanResultOutbox(scanner rowScanner) (ResultOutbox, error) {
	var result ResultOutbox
	var manifestBytes []byte
	var manifestSize int64
	var manifestSHA256, partsJSON string
	var managedThreadID, turnID string
	var lifecycleRevision uint64
	if err := scanner.Scan(
		&result.ControllerID, &result.TreeID, &result.AgentID, &result.SourceDeviceID,
		&result.PackageID, &result.State, &result.ReservationLimitBytes,
		&result.ReservedBytes, &result.PackageBytes,
		&result.DeliverySequence, &managedThreadID, &turnID, &lifecycleRevision,
		&manifestBytes, &manifestSize, &manifestSHA256, &partsJSON,
		&result.CreatedAt, &result.UpdatedAt,
	); errors.Is(err, sql.ErrNoRows) {
		return ResultOutbox{}, ErrNotFound
	} else if err != nil {
		return ResultOutbox{}, fmt.Errorf("load result outbox: %w", err)
	}
	if result.State != ResultOutboxCapturePending {
		metadata, manifest, err := decodeResultMetadata(manifestBytes, manifestSize, manifestSHA256, partsJSON)
		if err != nil {
			return ResultOutbox{}, err
		}
		result.Metadata = metadata
		result.Manifest = manifest
		if managedThreadID != manifest.ManagedThreadID || turnID != manifest.TurnID ||
			lifecycleRevision != manifest.LifecycleRevision {
			return ResultOutbox{}, errors.New("stored result outbox identity columns do not match its manifest")
		}
	} else if managedThreadID != "" || turnID != "" || lifecycleRevision != 0 {
		return ResultOutbox{}, errors.New("capture-pending result outbox contains turn identity")
	}
	if err := result.Validate(); err != nil {
		return ResultOutbox{}, fmt.Errorf("stored result outbox is invalid: %w", err)
	}
	return result, nil
}

func scanResultInbox(scanner rowScanner) (ResultInbox, error) {
	var result ResultInbox
	var manifestBytes []byte
	var manifestSize int64
	var manifestSHA256, partsJSON string
	var sourceAgentID, sourceDeviceID, managedThreadID, turnID string
	var lifecycleRevision uint64
	if err := scanner.Scan(
		&result.Authority.ControllerID, &result.Authority.TreeID,
		&result.Authority.RootAgentID, &result.Authority.RootDeviceID,
		&sourceAgentID, &sourceDeviceID, &managedThreadID, &turnID,
		&result.PackageID, &result.State, &result.AttemptID, &result.LeaseExpiresAt, &lifecycleRevision,
		&manifestBytes, &manifestSize, &manifestSHA256, &partsJSON, &result.PackageBytes,
		&result.CreatedAt, &result.UpdatedAt,
	); errors.Is(err, sql.ErrNoRows) {
		return ResultInbox{}, ErrNotFound
	} else if err != nil {
		return ResultInbox{}, fmt.Errorf("load result inbox: %w", err)
	}
	metadata, manifest, err := decodeResultMetadata(manifestBytes, manifestSize, manifestSHA256, partsJSON)
	if err != nil {
		return ResultInbox{}, err
	}
	result.Metadata = metadata
	result.Manifest = manifest
	if sourceAgentID != manifest.SourceAgentID || sourceDeviceID != manifest.SourceDeviceID ||
		managedThreadID != manifest.ManagedThreadID || turnID != manifest.TurnID ||
		lifecycleRevision != manifest.LifecycleRevision {
		return ResultInbox{}, errors.New("stored result inbox identity columns do not match its manifest")
	}
	return result, nil
}

type resultRowsQueryer interface {
	rowQueryer
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func loadResultInboxOffsets(ctx context.Context, queryer resultRowsQueryer, result *ResultInbox) error {
	rows, err := queryer.QueryContext(ctx, `
SELECT kind, next_offset
FROM peer_result_inbox_parts
WHERE package_id = ?
ORDER BY kind
`, result.PackageID)
	if err != nil {
		return fmt.Errorf("list result inbox offsets: %w", err)
	}
	defer rows.Close()
	result.Offsets = make([]protocol.ResultPackagePartOffset, 0, len(result.Manifest.Parts))
	for rows.Next() {
		var offset protocol.ResultPackagePartOffset
		if err := rows.Scan(&offset.Kind, &offset.NextOffset); err != nil {
			return fmt.Errorf("load result inbox offset: %w", err)
		}
		result.Offsets = append(result.Offsets, offset)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("list result inbox offsets: %w", err)
	}
	if err := result.Validate(); err != nil {
		return fmt.Errorf("stored result inbox is invalid: %w", err)
	}
	return nil
}

func resultObservedAt(observedAt time.Time) (int64, error) {
	return unixTime(observedAt, "observedAt")
}

func validateResultPage(limit int) error {
	if limit < 1 || limit > maximumPeerResultPage {
		return fmt.Errorf("limit must be from 1 through %d", maximumPeerResultPage)
	}
	return nil
}

const resultOutboxSelect = `
SELECT controller_id, tree_id, source_agent_id, source_device_id,
	package_id, state, reservation_limit_bytes, reserved_bytes, package_bytes, delivery_sequence,
	managed_thread_id, turn_id, lifecycle_revision,
	manifest_bytes, manifest_size_bytes, manifest_sha256, parts_json,
	created_at, updated_at
FROM peer_result_outbox
`

const resultInboxSelect = `
SELECT controller_id, tree_id, root_agent_id, root_device_id,
	source_agent_id, source_device_id, managed_thread_id, turn_id,
	package_id, state, attempt_id, lease_expires_at, lifecycle_revision,
	manifest_bytes, manifest_size_bytes, manifest_sha256, parts_json,
	package_bytes, created_at, updated_at
FROM peer_result_inbox
`
