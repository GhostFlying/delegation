package resultpackagefiles

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"

	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/rolloutcapture"
	"github.com/GhostFlying/delegation/internal/store"
)

func (m *Manager) recover(ctx context.Context) error {
	removals, err := m.state.ListPreparedResultInboxRemovals(
		ctx, m.controllerID, m.deviceID, store.MaximumResultInboxRemovalReceipts,
	)
	if err != nil {
		return err
	}
	for _, removal := range removals {
		lock := m.lock(removal.PackageID)
		lock.Lock()
		err := m.finishRemoval(ctx, removal)
		lock.Unlock()
		if err != nil {
			return fmt.Errorf("complete prepared result inbox removal: %w", err)
		}
	}
	receiving, err := m.state.ListReceivingResultInboxes(
		ctx, m.controllerID, m.deviceID, store.MaximumPeerResultPackages,
	)
	if err != nil {
		return err
	}
	for _, inbox := range receiving {
		lock := m.lock(inbox.PackageID)
		lock.Lock()
		_, recoverErr := m.reconcileReceiving(ctx, inbox)
		lock.Unlock()
		if recoverErr != nil {
			return fmt.Errorf("recover receiving result package: %w", recoverErr)
		}
	}
	if err := m.reclaimExpired(ctx); err != nil {
		return fmt.Errorf("reclaim expired result inbox: %w", err)
	}

	tombstones, err := m.state.ListResultInboxEvictionTombstones(
		ctx, m.controllerID, m.deviceID, store.MaximumPeerResultPackages,
	)
	if err != nil {
		return err
	}
	for _, inbox := range tombstones {
		lock := m.lock(inbox.PackageID)
		lock.Lock()
		removeErr := removePackageDirectory(m.inbox, inbox.PackageID)
		if removeErr == nil {
			removeErr = m.state.CompactResultInboxEviction(ctx, inbox.Authority, inbox.PackageID)
		}
		lock.Unlock()
		if removeErr != nil {
			return fmt.Errorf("complete result inbox eviction: %w", removeErr)
		}
	}

	if err := m.verifyStoredInboxes(ctx); err != nil {
		return err
	}
	return m.verifyStoredOutboxes(ctx)
}

func (m *Manager) reconcileReceiving(ctx context.Context, inbox store.ResultInbox) (bool, error) {
	finalExists, err := rootEntryExists(m.inbox, inbox.PackageID)
	if err != nil {
		return false, err
	}
	temporary := receivingDirectoryName(inbox.PackageID, inbox.AttemptID)
	temporaryExists, err := rootEntryExists(m.inbox, temporary)
	if err != nil {
		return false, err
	}
	if finalExists {
		if temporaryExists {
			return false, errors.New("result package has both receiving and published directories")
		}
		if !inboxComplete(inbox) {
			return false, errors.New("incomplete result inbox has a published directory")
		}
		if err := m.verifyAvailable(ctx, inbox); err != nil {
			return false, err
		}
		if err := m.syncRoot(m.inbox); err != nil {
			return false, err
		}
		if _, err := m.state.CommitResultInboxAvailable(
			ctx, inbox.Authority, inbox.AttemptID, inbox.PackageID, m.now(),
		); err != nil {
			return false, err
		}
		return true, nil
	}
	if !temporaryExists {
		for _, offset := range inbox.Offsets {
			if offset.NextOffset != 0 {
				return false, errors.New("receiving result package directory is missing committed bytes")
			}
		}
		if err := createReceivingDirectory(m.inbox, inbox); err != nil {
			return false, err
		}
		return false, nil
	}
	directory, err := m.openReceiving(inbox)
	if err != nil {
		return false, err
	}
	defer directory.Close()
	if err := repairReceivingFiles(directory, inbox); err != nil {
		return false, err
	}
	manifestExists, err := rootEntryExists(directory, protocol.ResultManifestFileName)
	if err != nil {
		return false, err
	}
	if manifestExists {
		if !inboxComplete(inbox) {
			return false, errors.New("incomplete result package contains a manifest file")
		}
		if err := writeManifestFile(directory, inbox.Metadata); err != nil {
			return false, err
		}
		if err := syncDirectory(directory); err != nil {
			return false, err
		}
	}
	changed := false
	for index, descriptor := range inbox.Manifest.Parts {
		fileName, _ := descriptor.Kind.FileName()
		file, info, err := openRegularFile(directory, fileName)
		if err != nil {
			return false, err
		}
		offset := inbox.Offsets[index].NextOffset
		if info.Size() < offset {
			_ = file.Close()
			return false, errors.New("result package file is shorter than its committed offset")
		}
		if info.Size() > offset {
			if err := file.Truncate(offset); err != nil {
				_ = file.Close()
				return false, err
			}
			changed = true
		}
		closeErr := errors.Join(file.Sync(), file.Close())
		if closeErr != nil {
			return false, closeErr
		}
	}
	if changed {
		if err := syncDirectory(directory); err != nil {
			return false, err
		}
	}
	return false, nil
}

func repairReceivingFiles(directory *os.Root, inbox store.ResultInbox) error {
	expected := expectedFiles(inbox.Manifest, false)
	entries, err := fs.ReadDir(directory.FS(), ".")
	if err != nil {
		return err
	}
	seen := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if name != protocol.ResultManifestFileName {
			if _, ok := expected[name]; !ok {
				return fmt.Errorf("unexpected result package entry %q", name)
			}
		}
		seen[name] = struct{}{}
	}
	for index, descriptor := range inbox.Manifest.Parts {
		name, _ := descriptor.Kind.FileName()
		if _, ok := seen[name]; ok {
			continue
		}
		if inbox.Offsets[index].NextOffset != 0 {
			return fmt.Errorf("result package entry %q is missing committed bytes", name)
		}
		file, err := directory.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
		if err != nil {
			return err
		}
		if err := errors.Join(file.Sync(), file.Close()); err != nil {
			return err
		}
	}
	return syncDirectory(directory)
}

func (m *Manager) verifyStoredInboxes(ctx context.Context) error {
	available, err := m.state.ListAvailableResultInboxes(
		ctx, m.controllerID, m.deviceID, store.MaximumPeerResultPackages,
	)
	if err != nil {
		return err
	}
	receiving, err := m.state.ListReceivingResultInboxes(
		ctx, m.controllerID, m.deviceID, store.MaximumPeerResultPackages,
	)
	if err != nil {
		return err
	}
	expected := make(map[string]struct{}, len(available)+len(receiving))
	for _, inbox := range available {
		if err := m.verifyAvailable(ctx, inbox); err != nil {
			return fmt.Errorf("verify available result package: %w", err)
		}
		expected[inbox.PackageID] = struct{}{}
	}
	for _, inbox := range receiving {
		expected[receivingDirectoryName(inbox.PackageID, inbox.AttemptID)] = struct{}{}
	}
	return verifyTopLevelDirectories(m.inbox, expected)
}

func (m *Manager) verifyStoredOutboxes(ctx context.Context) error {
	var outboxes []store.ResultOutbox
	for _, list := range []func(context.Context, string, string, int) ([]store.ResultOutbox, error){
		m.state.ListPendingResultPublications,
		m.state.ListPendingResultDeliveries,
		m.state.ListDeliveredResultOutboxes,
	} {
		results, err := list(ctx, m.controllerID, m.deviceID, store.MaximumPeerResultPackages)
		if err != nil {
			return err
		}
		outboxes = append(outboxes, results...)
	}
	expected := make(map[string]struct{}, len(outboxes))
	for _, outbox := range outboxes {
		if err := m.verifyOutbox(ctx, outbox); err != nil {
			return fmt.Errorf("verify result package outbox: %w", err)
		}
		expected[outbox.PackageID] = struct{}{}
	}
	return verifyTopLevelDirectories(m.outbox, expected)
}

func (m *Manager) verifyOutbox(ctx context.Context, outbox store.ResultOutbox) error {
	directory, err := openPrivateDirectory(m.outbox, outbox.PackageID)
	if err != nil {
		return err
	}
	defer directory.Close()
	if err := validateDirectoryEntries(directory, outbox.Manifest, true, false); err != nil {
		return err
	}
	if err := verifyManifestFile(directory, outbox.Metadata); err != nil {
		return err
	}
	for _, descriptor := range outbox.Manifest.Parts {
		fileName, _ := descriptor.Kind.FileName()
		file, info, err := openRegularFile(directory, fileName)
		if err != nil {
			return err
		}
		if info.Size() != descriptor.Size {
			_ = file.Close()
			return errors.New("result package outbox part size does not match its descriptor")
		}
		digest, verifyErr := hashRegularFile(ctx, file, descriptor.Size)
		verifyErr = errors.Join(verifyErr, file.Close())
		if verifyErr != nil {
			return verifyErr
		}
		if digest != descriptor.SHA256 {
			return errors.New("result package outbox part digest does not match its descriptor")
		}
		if descriptor.Kind == protocol.ResultPackagePartRollout {
			file, _, err := openRegularFile(directory, fileName)
			if err != nil {
				return err
			}
			verifyErr := rolloutcapture.VerifyCompressedSegment(
				ctx, file, outbox.Manifest.Rollout.RawSize, outbox.Manifest.Rollout.RawSHA256,
			)
			verifyErr = errors.Join(verifyErr, file.Close())
			if verifyErr != nil {
				return verifyErr
			}
		}
	}
	return nil
}

func verifyTopLevelDirectories(root *os.Root, expected map[string]struct{}) error {
	entries, err := fs.ReadDir(root.FS(), ".")
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if _, ok := expected[entry.Name()]; !ok {
			return fmt.Errorf("unexpected result package directory %q", entry.Name())
		}
		directory, err := openPrivateDirectory(root, entry.Name())
		if err != nil {
			return err
		}
		if err := directory.Close(); err != nil {
			return err
		}
		delete(expected, entry.Name())
	}
	for name := range expected {
		return fmt.Errorf("result package directory %q is missing", name)
	}
	return nil
}
