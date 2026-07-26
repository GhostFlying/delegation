package resultpackagefiles

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"slices"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/rolloutcapture"
	"github.com/GhostFlying/delegation/internal/store"
)

func (m *Manager) ReadResultPackagePart(
	ctx context.Context,
	request ReadRequest,
) (protocol.ReadResultPackagePartResult, error) {
	if err := request.Params.Validate(); err != nil {
		return protocol.ReadResultPackagePartResult{}, err
	}
	lock := m.lock(request.Params.PackageID)
	lock.Lock()
	defer lock.Unlock()
	outbox, err := m.authorizeSource(ctx, request.TreeID, request.Source, request.Params.PackageID)
	if err != nil {
		return protocol.ReadResultPackagePartResult{}, err
	}
	if outbox.State != store.ResultOutboxPublishPending &&
		outbox.State != store.ResultOutboxDeliveryPending && outbox.State != store.ResultOutboxDelivered {
		return protocol.ReadResultPackagePartResult{}, store.ErrResultPackageTransition
	}
	descriptor, ok := descriptorFor(outbox.Manifest, request.Params.Kind)
	if !ok {
		return protocol.ReadResultPackagePartResult{}, store.ErrResultPackageConflict
	}
	remaining := descriptor.Size - request.Params.Offset
	expected := min(int64(protocol.ResultPackageChunkBytes), remaining)
	if request.Params.Offset < 0 || request.Params.Offset%protocol.ResultPackageChunkBytes != 0 ||
		remaining <= 0 || int64(request.Params.Limit) != expected {
		return protocol.ReadResultPackagePartResult{}, store.ErrResultPackageConflict
	}
	directory, err := openPrivateDirectory(m.outbox, request.Params.PackageID)
	if err != nil {
		return protocol.ReadResultPackagePartResult{}, fmt.Errorf("open result package outbox entry: %w", err)
	}
	defer directory.Close()
	if err := validateDirectoryEntries(directory, outbox.Manifest, true, false); err != nil {
		return protocol.ReadResultPackagePartResult{}, err
	}
	if err := verifyManifestFile(directory, outbox.Metadata); err != nil {
		return protocol.ReadResultPackagePartResult{}, err
	}
	fileName, _ := request.Params.Kind.FileName()
	file, info, err := openRegularFile(directory, fileName)
	if err != nil {
		return protocol.ReadResultPackagePartResult{}, err
	}
	defer file.Close()
	if info.Size() != descriptor.Size {
		return protocol.ReadResultPackagePartResult{}, errors.New("result package outbox part size does not match its descriptor")
	}
	data := make([]byte, request.Params.Limit)
	count, err := file.ReadAt(data, request.Params.Offset)
	if err != nil && !(errors.Is(err, io.EOF) && count == len(data)) {
		return protocol.ReadResultPackagePartResult{}, fmt.Errorf("read result package outbox part: %w", err)
	}
	if count != len(data) {
		return protocol.ReadResultPackagePartResult{}, io.ErrUnexpectedEOF
	}
	return protocol.ReadResultPackagePartResult{
		PackageID:  request.Params.PackageID,
		Kind:       request.Params.Kind,
		Offset:     request.Params.Offset,
		Data:       data,
		NextOffset: request.Params.Offset + int64(len(data)),
	}, nil
}

func (m *Manager) BeginResultPackage(
	ctx context.Context,
	request BeginRequest,
) (protocol.BeginResultPackageResult, error) {
	authority, err := m.sinkAuthority(request.TreeID, request.Source)
	if err != nil {
		return protocol.BeginResultPackageResult{}, err
	}
	if err := request.Params.Validate(); err != nil {
		return protocol.BeginResultPackageResult{}, err
	}
	if err := m.reclaimExpired(ctx); err != nil {
		return protocol.BeginResultPackageResult{}, err
	}
	lock := m.lock(request.Params.PackageID)
	lock.Lock()
	defer lock.Unlock()
	result, err := m.state.BeginResultInbox(ctx, authority, request.Params, m.now())
	if err != nil {
		return protocol.BeginResultPackageResult{}, err
	}
	if result.Outcome == protocol.ResultPackageAlreadyAvailable {
		inbox, err := m.state.GetResultInbox(ctx, authority, request.Params.PackageID)
		if err != nil {
			return protocol.BeginResultPackageResult{}, err
		}
		if err := m.verifyAvailable(ctx, inbox); err != nil {
			return protocol.BeginResultPackageResult{}, err
		}
		return result, nil
	}
	inbox, err := m.state.GetResultInbox(ctx, authority, request.Params.PackageID)
	if err != nil {
		return protocol.BeginResultPackageResult{}, err
	}
	published, err := m.reconcileReceiving(ctx, inbox)
	if err != nil {
		return protocol.BeginResultPackageResult{}, err
	}
	if published {
		return protocol.BeginResultPackageResult{
			AttemptID: request.Params.AttemptID,
			PackageID: request.Params.PackageID,
			Outcome:   protocol.ResultPackageAlreadyAvailable,
			Offsets:   []protocol.ResultPackagePartOffset{},
		}, nil
	}
	return result, nil
}

func (m *Manager) WriteResultPackagePart(
	ctx context.Context,
	request WriteRequest,
) (protocol.WriteResultPackagePartResult, error) {
	authority, err := m.sinkAuthority(request.TreeID, request.Source)
	if err != nil {
		return protocol.WriteResultPackagePartResult{}, err
	}
	if err := request.Params.Validate(); err != nil {
		return protocol.WriteResultPackagePartResult{}, err
	}
	lock := m.lock(request.Params.PackageID)
	lock.Lock()
	defer lock.Unlock()
	inbox, err := m.state.GetResultInbox(ctx, authority, request.Params.PackageID)
	if err != nil {
		return protocol.WriteResultPackagePartResult{}, err
	}
	if inbox.State != store.ResultInboxReceiving || inbox.AttemptID != request.Params.AttemptID {
		return protocol.WriteResultPackagePartResult{}, store.ErrResultPackageTransition
	}
	descriptor, offset, ok := descriptorAndOffset(inbox, request.Params.Kind)
	if !ok {
		return protocol.WriteResultPackagePartResult{}, store.ErrResultPackageConflict
	}
	dataSize := int64(len(request.Params.Data))
	if request.Params.Offset%protocol.ResultPackageChunkBytes != 0 ||
		dataSize != min(int64(protocol.ResultPackageChunkBytes), descriptor.Size-request.Params.Offset) ||
		request.Params.Offset > offset.NextOffset ||
		request.Params.Offset < offset.NextOffset && request.Params.Offset+dataSize > offset.NextOffset {
		return protocol.WriteResultPackagePartResult{}, store.ErrResultPackageConflict
	}
	directory, err := m.openReceiving(inbox)
	if err != nil {
		return protocol.WriteResultPackagePartResult{}, err
	}
	defer directory.Close()
	fileName, _ := request.Params.Kind.FileName()
	file, info, err := openRegularFile(directory, fileName)
	if err != nil {
		return protocol.WriteResultPackagePartResult{}, err
	}
	defer file.Close()
	if info.Size() < offset.NextOffset {
		return protocol.WriteResultPackagePartResult{}, errors.New("result package file is shorter than its committed offset")
	}
	if info.Size() > offset.NextOffset {
		if err := file.Truncate(offset.NextOffset); err != nil {
			return protocol.WriteResultPackagePartResult{}, fmt.Errorf("truncate uncommitted result package bytes: %w", err)
		}
		if err := file.Sync(); err != nil {
			return protocol.WriteResultPackagePartResult{}, fmt.Errorf("sync truncated result package part: %w", err)
		}
	}
	chunkDigest := sha256.Sum256(request.Params.Data)
	commit := store.ResultInboxChunkCommit{
		Kind:   request.Params.Kind,
		Offset: request.Params.Offset,
		Size:   dataSize,
		SHA256: hex.EncodeToString(chunkDigest[:]),
	}
	if request.Params.Offset < offset.NextOffset {
		stored := make([]byte, len(request.Params.Data))
		count, readErr := file.ReadAt(stored, request.Params.Offset)
		if readErr != nil && !(errors.Is(readErr, io.EOF) && count == len(stored)) {
			return protocol.WriteResultPackagePartResult{}, readErr
		}
		if count != len(stored) || !slices.Equal(stored, request.Params.Data) {
			return protocol.WriteResultPackagePartResult{}, store.ErrResultPackageConflict
		}
	} else {
		count, writeErr := file.WriteAt(request.Params.Data, request.Params.Offset)
		if writeErr == nil && count != len(request.Params.Data) {
			writeErr = io.ErrShortWrite
		}
		if writeErr != nil {
			return protocol.WriteResultPackagePartResult{}, writeErr
		}
		if err := file.Sync(); err != nil {
			return protocol.WriteResultPackagePartResult{}, fmt.Errorf("sync result package chunk: %w", err)
		}
	}
	committed, err := m.state.CommitResultInboxChunk(
		ctx, authority, request.Params.AttemptID, request.Params.PackageID, commit, m.now(),
	)
	if err != nil {
		return protocol.WriteResultPackagePartResult{}, err
	}
	return protocol.WriteResultPackagePartResult{
		AttemptID:  request.Params.AttemptID,
		PackageID:  request.Params.PackageID,
		Kind:       request.Params.Kind,
		NextOffset: committed.NextOffset,
	}, nil
}

func (m *Manager) FinishResultPackage(
	ctx context.Context,
	request FinishRequest,
) (protocol.FinishResultPackageResult, error) {
	authority, err := m.sinkAuthority(request.TreeID, request.Source)
	if err != nil {
		return protocol.FinishResultPackageResult{}, err
	}
	if err := request.Params.Validate(); err != nil {
		return protocol.FinishResultPackageResult{}, err
	}
	lock := m.lock(request.Params.PackageID)
	lock.Lock()
	defer lock.Unlock()
	inbox, err := m.state.GetResultInbox(ctx, authority, request.Params.PackageID)
	if err != nil {
		return protocol.FinishResultPackageResult{}, err
	}
	if inbox.AttemptID != request.Params.AttemptID {
		return protocol.FinishResultPackageResult{}, store.ErrResultPackageConflict
	}
	if inbox.State == store.ResultInboxAvailable {
		if err := m.verifyAvailable(ctx, inbox); err != nil {
			return protocol.FinishResultPackageResult{}, err
		}
		return protocol.FinishResultPackageResult(request.Params), nil
	}
	if inbox.State != store.ResultInboxReceiving || !inboxComplete(inbox) {
		return protocol.FinishResultPackageResult{}, store.ErrResultPackageTransition
	}
	if exists, err := rootEntryExists(m.inbox, inbox.PackageID); err != nil {
		return protocol.FinishResultPackageResult{}, err
	} else if exists {
		if exists, err := rootEntryExists(m.inbox, receivingDirectoryName(inbox.PackageID, inbox.AttemptID)); err != nil {
			return protocol.FinishResultPackageResult{}, err
		} else if exists {
			return protocol.FinishResultPackageResult{}, errors.New("result package has both receiving and published directories")
		}
		if err := m.verifyAvailable(ctx, inbox); err != nil {
			return protocol.FinishResultPackageResult{}, err
		}
		if err := m.syncRoot(m.inbox); err != nil {
			return protocol.FinishResultPackageResult{}, err
		}
		if _, err := m.state.CommitResultInboxAvailable(
			ctx, authority, inbox.AttemptID, inbox.PackageID, m.now(),
		); err != nil {
			return protocol.FinishResultPackageResult{}, err
		}
		return protocol.FinishResultPackageResult(request.Params), nil
	}
	directory, err := m.openReceiving(inbox)
	if err != nil {
		return protocol.FinishResultPackageResult{}, err
	}
	if err := validateDirectoryEntries(directory, inbox.Manifest, false, true); err != nil {
		_ = directory.Close()
		return protocol.FinishResultPackageResult{}, err
	}
	for _, descriptor := range inbox.Manifest.Parts {
		fileName, _ := descriptor.Kind.FileName()
		file, info, err := openRegularFile(directory, fileName)
		if err != nil {
			_ = directory.Close()
			return protocol.FinishResultPackageResult{}, err
		}
		if info.Size() != descriptor.Size {
			_ = file.Close()
			_ = directory.Close()
			return protocol.FinishResultPackageResult{}, errors.New("result package part size does not match its descriptor")
		}
		digest, verifyErr := hashRegularFile(ctx, file, descriptor.Size)
		if verifyErr == nil && digest != descriptor.SHA256 {
			verifyErr = errors.New("result package part digest does not match its descriptor")
		}
		if verifyErr == nil && descriptor.Kind == protocol.ResultPackagePartRollout {
			_, verifyErr = file.Seek(0, io.SeekStart)
			if verifyErr == nil {
				verifyErr = rolloutcapture.VerifyCompressedSegment(
					ctx, file, inbox.Manifest.Rollout.RawSize, inbox.Manifest.Rollout.RawSHA256,
				)
			}
		}
		verifyErr = errors.Join(verifyErr, file.Sync(), file.Close())
		if verifyErr != nil {
			_ = directory.Close()
			return protocol.FinishResultPackageResult{}, verifyErr
		}
	}
	if err := writeManifestFile(directory, inbox.Metadata); err != nil {
		_ = directory.Close()
		return protocol.FinishResultPackageResult{}, err
	}
	if err := verifyManifestFile(directory, inbox.Metadata); err != nil {
		_ = directory.Close()
		return protocol.FinishResultPackageResult{}, err
	}
	if err := syncDirectory(directory); err != nil {
		_ = directory.Close()
		return protocol.FinishResultPackageResult{}, err
	}
	if err := directory.Close(); err != nil {
		return protocol.FinishResultPackageResult{}, err
	}
	temporary := receivingDirectoryName(inbox.PackageID, inbox.AttemptID)
	if err := m.inbox.Rename(temporary, inbox.PackageID); err != nil {
		return protocol.FinishResultPackageResult{}, fmt.Errorf("publish result package inbox: %w", err)
	}
	if err := m.syncRoot(m.inbox); err != nil {
		return protocol.FinishResultPackageResult{}, err
	}
	if _, err := m.state.CommitResultInboxAvailable(
		ctx, authority, inbox.AttemptID, inbox.PackageID, m.now(),
	); err != nil {
		return protocol.FinishResultPackageResult{}, err
	}
	return protocol.FinishResultPackageResult(request.Params), nil
}

func (m *Manager) CancelResultPackage(
	ctx context.Context,
	request CancelRequest,
) (protocol.CancelResultPackageResult, error) {
	authority, err := m.sinkAuthority(request.TreeID, request.Source)
	if err != nil {
		return protocol.CancelResultPackageResult{}, err
	}
	if err := request.Params.Validate(); err != nil {
		return protocol.CancelResultPackageResult{}, err
	}
	lock := m.lock(request.Params.PackageID)
	lock.Lock()
	defer lock.Unlock()
	removal, err := m.state.PrepareResultInboxCancel(
		ctx, authority, request.Params.AttemptID, request.Params.PackageID, m.now(),
	)
	if err != nil {
		return protocol.CancelResultPackageResult{}, err
	}
	if err := m.finishRemoval(ctx, removal); err != nil {
		return protocol.CancelResultPackageResult{}, err
	}
	return protocol.CancelResultPackageResult(request.Params), nil
}

func (m *Manager) AcknowledgeResultPackage(
	ctx context.Context,
	request AcknowledgeRequest,
) (protocol.AcknowledgeResultPackageResult, error) {
	if err := request.Params.Validate(); err != nil {
		return protocol.AcknowledgeResultPackageResult{}, err
	}
	lock := m.lock(request.Params.PackageID)
	lock.Lock()
	defer lock.Unlock()
	outbox, err := m.authorizeSource(ctx, request.TreeID, request.Source, request.Params.PackageID)
	if err != nil {
		return protocol.AcknowledgeResultPackageResult{}, err
	}
	if outbox.State != store.ResultOutboxDeliveryPending && outbox.State != store.ResultOutboxDelivered {
		return protocol.AcknowledgeResultPackageResult{}, store.ErrResultPackageTransition
	}
	if _, err := m.state.AcknowledgeResultOutboxDelivery(
		ctx, outbox.ResultOutboxKey, request.Params.Sequence, m.now(),
	); err != nil {
		return protocol.AcknowledgeResultPackageResult{}, err
	}
	return protocol.AcknowledgeResultPackageResult(request.Params), nil
}

func (m *Manager) authorizeSource(
	ctx context.Context,
	treeID string,
	source control.PrincipalIdentity,
	packageID string,
) (store.ResultOutbox, error) {
	if err := source.Validate(); err != nil {
		return store.ResultOutbox{}, err
	}
	if source.ControllerID != m.controllerID || source.TreeID != treeID ||
		source.DeviceID != m.deviceID || source.ParentAgentID == "" {
		return store.ResultOutbox{}, store.ErrResultPackageAuthority
	}
	key := store.ResultOutboxKey{
		WorkerKey: store.WorkerKey{
			ControllerID: source.ControllerID,
			TreeID:       source.TreeID,
			AgentID:      source.AgentID,
		},
		SourceDeviceID: source.DeviceID,
		PackageID:      packageID,
	}
	worker, err := m.state.GetWorker(ctx, key.WorkerKey)
	if err != nil {
		return store.ResultOutbox{}, err
	}
	if worker.ParentAgentID != source.ParentAgentID || worker.DeviceID != source.DeviceID {
		return store.ResultOutbox{}, store.ErrResultPackageAuthority
	}
	outbox, err := m.state.GetResultOutbox(ctx, key)
	if err != nil {
		return store.ResultOutbox{}, err
	}
	return outbox, nil
}

func (m *Manager) sinkAuthority(
	treeID string,
	source control.PrincipalIdentity,
) (store.ResultInboxAuthority, error) {
	if err := source.Validate(); err != nil {
		return store.ResultInboxAuthority{}, err
	}
	if source.ControllerID != m.controllerID || source.TreeID != treeID ||
		source.DeviceID != m.deviceID || source.ParentAgentID != "" {
		return store.ResultInboxAuthority{}, store.ErrResultPackageAuthority
	}
	return store.ResultInboxAuthority{
		ControllerID: source.ControllerID,
		TreeID:       source.TreeID,
		RootAgentID:  source.AgentID,
		RootDeviceID: source.DeviceID,
	}, nil
}

func descriptorFor(
	manifest protocol.ResultManifest,
	kind protocol.ResultPackagePartKind,
) (protocol.ResultPackagePartDescriptor, bool) {
	for _, descriptor := range manifest.Parts {
		if descriptor.Kind == kind {
			return descriptor, true
		}
	}
	return protocol.ResultPackagePartDescriptor{}, false
}

func descriptorAndOffset(
	inbox store.ResultInbox,
	kind protocol.ResultPackagePartKind,
) (protocol.ResultPackagePartDescriptor, protocol.ResultPackagePartOffset, bool) {
	for index, descriptor := range inbox.Manifest.Parts {
		if descriptor.Kind == kind {
			return descriptor, inbox.Offsets[index], true
		}
	}
	return protocol.ResultPackagePartDescriptor{}, protocol.ResultPackagePartOffset{}, false
}

func inboxComplete(inbox store.ResultInbox) bool {
	for index, offset := range inbox.Offsets {
		if offset.NextOffset != inbox.Manifest.Parts[index].Size {
			return false
		}
	}
	return true
}

func rootEntryExists(root *os.Root, name string) (bool, error) {
	_, err := root.Lstat(name)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	return err == nil, err
}

func (m *Manager) openReceiving(inbox store.ResultInbox) (*os.Root, error) {
	return openPrivateDirectory(m.inbox, receivingDirectoryName(inbox.PackageID, inbox.AttemptID))
}

func (m *Manager) verifyAvailable(ctx context.Context, inbox store.ResultInbox) error {
	directory, err := openPrivateDirectory(m.inbox, inbox.PackageID)
	if err != nil {
		return err
	}
	defer directory.Close()
	if err := validateDirectoryEntries(directory, inbox.Manifest, true, false); err != nil {
		return err
	}
	if err := verifyManifestFile(directory, inbox.Metadata); err != nil {
		return err
	}
	for _, descriptor := range inbox.Manifest.Parts {
		fileName, _ := descriptor.Kind.FileName()
		file, info, err := openRegularFile(directory, fileName)
		if err != nil {
			return err
		}
		if info.Size() != descriptor.Size {
			_ = file.Close()
			return errors.New("available result package part size does not match its descriptor")
		}
		digest, verifyErr := hashRegularFile(ctx, file, descriptor.Size)
		verifyErr = errors.Join(verifyErr, file.Close())
		if verifyErr != nil {
			return verifyErr
		}
		if digest != descriptor.SHA256 {
			return errors.New("available result package part digest does not match its descriptor")
		}
		if descriptor.Kind == protocol.ResultPackagePartRollout {
			file, _, err := openRegularFile(directory, fileName)
			if err != nil {
				return err
			}
			verifyErr := rolloutcapture.VerifyCompressedSegment(
				ctx, file, inbox.Manifest.Rollout.RawSize, inbox.Manifest.Rollout.RawSHA256,
			)
			verifyErr = errors.Join(verifyErr, file.Close())
			if verifyErr != nil {
				return verifyErr
			}
		}
	}
	return nil
}

func (m *Manager) reclaimExpired(ctx context.Context) error {
	expired, err := m.state.ListExpiredResultInboxes(
		ctx, m.controllerID, m.deviceID, m.now(), store.MaximumPeerResultPackages,
	)
	if err != nil {
		return err
	}
	for _, inbox := range expired {
		lock := m.lock(inbox.PackageID)
		lock.Lock()
		removal, prepareErr := m.state.PrepareExpiredResultInboxReclaim(
			ctx, inbox.Authority, inbox.AttemptID, inbox.PackageID, m.now(),
		)
		if prepareErr == nil {
			prepareErr = m.finishRemoval(ctx, removal)
		}
		lock.Unlock()
		if prepareErr != nil && !errors.Is(prepareErr, store.ErrResultPackageTransition) &&
			!errors.Is(prepareErr, store.ErrResultPackageConflict) {
			return prepareErr
		}
	}
	return nil
}

func (m *Manager) finishRemoval(ctx context.Context, removal store.ResultInboxRemoval) error {
	if removal.Phase == store.ResultInboxRemovalCompleted {
		return nil
	}
	if exists, err := rootEntryExists(m.inbox, removal.PackageID); err != nil {
		return err
	} else if exists {
		return errors.New("cannot remove receiving result package with a published directory")
	}
	if err := removePackageDirectory(
		m.inbox, receivingDirectoryName(removal.PackageID, removal.AttemptID),
	); err != nil {
		return err
	}
	_, err := m.state.CommitResultInboxRemoval(ctx, removal, m.now())
	return err
}
