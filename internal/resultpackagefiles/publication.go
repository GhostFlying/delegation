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
	"path/filepath"

	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
)

const publishingDirectoryPrefix = ".publishing-"

// ErrPublicationIntegrity marks capture input or an already-published package
// that no longer matches its authoritative metadata. Retrying the same input
// cannot repair these failures.
var ErrPublicationIntegrity = errors.New("result package publication integrity violation")

type ResultPackagePartSource struct {
	Kind protocol.ResultPackagePartKind
	Path string
}

type PublishResultPackageRequest struct {
	Key      store.ResultOutboxKey
	Metadata protocol.ResultPackageMetadata
	Parts    []ResultPackagePartSource
}

// PublishResultPackage durably copies trusted capture outputs into the fixed
// outbox layout before committing their authoritative metadata in SQLite.
func (m *Manager) PublishResultPackage(
	ctx context.Context,
	request PublishResultPackageRequest,
) (store.ResultOutbox, error) {
	manifest, err := validatePublishRequest(request)
	if err != nil {
		return store.ResultOutbox{}, fmt.Errorf("%w: %w", ErrPublicationIntegrity, err)
	}
	lock := m.lock(request.Key.PackageID)
	lock.Lock()
	defer lock.Unlock()

	stored, err := m.state.GetResultOutbox(ctx, request.Key)
	if err != nil {
		return store.ResultOutbox{}, err
	}
	if stored.State != store.ResultOutboxCapturePending {
		if !protocol.SameResultPackageMetadata(stored.Metadata, request.Metadata) {
			return store.ResultOutbox{}, store.ErrResultPackageConflict
		}
		if err := m.verifyOutbox(ctx, stored); err != nil {
			return store.ResultOutbox{}, fmt.Errorf("%w: %w", ErrPublicationIntegrity, err)
		}
		return stored, nil
	}

	if exists, err := rootEntryExists(m.outbox, request.Key.PackageID); err != nil {
		return store.ResultOutbox{}, err
	} else if exists {
		published, err := m.commitExistingCapture(ctx, stored)
		if err != nil {
			return store.ResultOutbox{}, err
		}
		return published, nil
	}
	temporary := publishingDirectoryName(request.Key.PackageID)
	if err := removePackageDirectory(m.outbox, temporary); err != nil {
		return store.ResultOutbox{}, fmt.Errorf("remove stale result package capture: %w", err)
	}
	if err := m.outbox.Mkdir(temporary, 0o700); err != nil {
		return store.ResultOutbox{}, fmt.Errorf("create result package capture: %w", err)
	}
	directory, err := openPrivateDirectory(m.outbox, temporary)
	if err != nil {
		return store.ResultOutbox{}, err
	}
	cleanup := func(cause error) (store.ResultOutbox, error) {
		closeErr := directory.Close()
		removeErr := removePackageDirectory(m.outbox, temporary)
		return store.ResultOutbox{}, errors.Join(cause, closeErr, removeErr)
	}
	for _, descriptor := range manifest.Parts {
		source := sourceForKind(request.Parts, descriptor.Kind)
		if err := copyResultPart(ctx, directory, source.Path, descriptor); err != nil {
			return cleanup(fmt.Errorf("copy result package part %s: %w", descriptor.Kind, err))
		}
	}
	if err := writeManifestFile(directory, request.Metadata); err != nil {
		return cleanup(fmt.Errorf("write result package manifest: %w", err))
	}
	if err := syncDirectory(directory); err != nil {
		return cleanup(fmt.Errorf("sync result package capture: %w", err))
	}
	if err := directory.Close(); err != nil {
		_ = removePackageDirectory(m.outbox, temporary)
		return store.ResultOutbox{}, err
	}
	if err := m.outbox.Rename(temporary, request.Key.PackageID); err != nil {
		_ = removePackageDirectory(m.outbox, temporary)
		return store.ResultOutbox{}, fmt.Errorf("publish result package directory: %w", err)
	}
	if err := m.syncRoot(m.outbox); err != nil {
		return store.ResultOutbox{}, fmt.Errorf("sync result package outbox: %w", err)
	}
	return m.CommitResultOutboxCapture(ctx, request.Key, request.Metadata)
}

func (m *Manager) CommitResultOutboxCapture(
	ctx context.Context,
	key store.ResultOutboxKey,
	metadata protocol.ResultPackageMetadata,
) (store.ResultOutbox, error) {
	outbox, err := m.commitOutbox(ctx, key, metadata, m.now())
	if err == nil {
		m.signalOutboxChange()
	}
	return outbox, err
}

func (m *Manager) ListPendingResultPublications(ctx context.Context) ([]store.ResultOutbox, error) {
	return m.state.ListPendingResultPublications(
		ctx, m.controllerID, m.deviceID, store.MaximumPeerResultPackages,
	)
}

func (m *Manager) AcknowledgeResultPackageMetadata(
	ctx context.Context,
	key store.ResultOutboxKey,
	metadata protocol.ResultPackageMetadata,
) (store.WorkerResultFinalization, error) {
	lock := m.lock(key.PackageID)
	lock.Lock()
	defer lock.Unlock()
	result, err := m.state.AcknowledgeResultOutboxMetadata(ctx, key, metadata, m.now())
	if err == nil {
		m.signalOutboxChange()
		err = m.notifyWorkerFinalized(result)
	}
	return result, err
}

func validatePublishRequest(request PublishResultPackageRequest) (protocol.ResultManifest, error) {
	if err := request.Key.Validate(); err != nil {
		return protocol.ResultManifest{}, err
	}
	manifest, err := request.Metadata.DecodeManifest()
	if err != nil {
		return protocol.ResultManifest{}, err
	}
	if manifest.PackageID != request.Key.PackageID || manifest.ControllerID != request.Key.ControllerID ||
		manifest.TreeID != request.Key.TreeID || manifest.SourceAgentID != request.Key.AgentID ||
		manifest.SourceDeviceID != request.Key.SourceDeviceID {
		return protocol.ResultManifest{}, store.ErrResultPackageAuthority
	}
	if len(request.Parts) != len(manifest.Parts) {
		return protocol.ResultManifest{}, store.ErrResultPackageConflict
	}
	seen := make(map[protocol.ResultPackagePartKind]struct{}, len(request.Parts))
	for _, source := range request.Parts {
		if _, ok := seen[source.Kind]; ok {
			return protocol.ResultManifest{}, store.ErrResultPackageConflict
		}
		seen[source.Kind] = struct{}{}
		if !filepath.IsAbs(source.Path) {
			return protocol.ResultManifest{}, errors.New("result package source path must be absolute")
		}
		if _, ok := descriptorFor(manifest, source.Kind); !ok {
			return protocol.ResultManifest{}, store.ErrResultPackageConflict
		}
	}
	return manifest, nil
}

func sourceForKind(
	parts []ResultPackagePartSource,
	kind protocol.ResultPackagePartKind,
) ResultPackagePartSource {
	for _, part := range parts {
		if part.Kind == kind {
			return part
		}
	}
	return ResultPackagePartSource{}
}

func copyResultPart(
	ctx context.Context,
	directory *os.Root,
	sourcePath string,
	descriptor protocol.ResultPackagePartDescriptor,
) error {
	before, err := os.Lstat(sourcePath)
	if err != nil {
		return err
	}
	if !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%w: result package source is not a regular file", ErrPublicationIntegrity)
	}
	source, err := os.Open(sourcePath)
	if err != nil {
		return err
	}
	opened, statErr := source.Stat()
	if statErr != nil || !os.SameFile(before, opened) || !opened.Mode().IsRegular() {
		_ = source.Close()
		if statErr == nil {
			statErr = fmt.Errorf(
				"%w: result package source changed while it was opened",
				ErrPublicationIntegrity,
			)
		}
		return statErr
	}
	if opened.Size() != descriptor.Size {
		_ = source.Close()
		return fmt.Errorf(
			"%w: result package source size does not match its descriptor",
			ErrPublicationIntegrity,
		)
	}
	name, _ := descriptor.Kind.FileName()
	destination, err := directory.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		_ = source.Close()
		return err
	}
	digest := sha256.New()
	reader := &contextReader{ctx: ctx, source: io.LimitReader(source, descriptor.Size+1)}
	written, copyErr := io.CopyBuffer(io.MultiWriter(destination, digest), reader, make([]byte, 128*1024))
	if copyErr == nil && written != descriptor.Size {
		copyErr = fmt.Errorf(
			"%w: result package source size changed during copy",
			ErrPublicationIntegrity,
		)
	}
	if copyErr == nil && hex.EncodeToString(digest.Sum(nil)) != descriptor.SHA256 {
		copyErr = fmt.Errorf(
			"%w: result package source digest does not match its descriptor",
			ErrPublicationIntegrity,
		)
	}
	return errors.Join(copyErr, destination.Sync(), destination.Close(), source.Close())
}

func publishingDirectoryName(packageID string) string {
	return publishingDirectoryPrefix + packageID
}

func (m *Manager) commitExistingCapture(
	ctx context.Context,
	outbox store.ResultOutbox,
) (store.ResultOutbox, error) {
	directory, err := openPrivateDirectory(m.outbox, outbox.PackageID)
	if err != nil {
		return store.ResultOutbox{}, fmt.Errorf(
			"%w: open existing result package capture: %w", ErrPublicationIntegrity, err,
		)
	}
	metadata, readErr := readResultMetadata(directory)
	closeErr := directory.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return store.ResultOutbox{}, fmt.Errorf(
			"%w: read existing result package capture: %w", ErrPublicationIntegrity, err,
		)
	}
	manifest, err := metadata.DecodeManifest()
	if err != nil {
		return store.ResultOutbox{}, fmt.Errorf(
			"%w: decode existing result package capture: %w", ErrPublicationIntegrity, err,
		)
	}
	if manifest.PackageID != outbox.PackageID || manifest.ControllerID != outbox.ControllerID ||
		manifest.TreeID != outbox.TreeID || manifest.SourceAgentID != outbox.AgentID ||
		manifest.SourceDeviceID != outbox.SourceDeviceID {
		return store.ResultOutbox{}, fmt.Errorf("%w: %w", ErrPublicationIntegrity, store.ErrResultPackageAuthority)
	}
	prospective := outbox
	prospective.State = store.ResultOutboxPublishPending
	prospective.Metadata = metadata
	prospective.Manifest = manifest
	if err := m.verifyOutbox(ctx, prospective); err != nil {
		return store.ResultOutbox{}, fmt.Errorf("%w: %w", ErrPublicationIntegrity, err)
	}
	if err := m.syncRoot(m.outbox); err != nil {
		return store.ResultOutbox{}, fmt.Errorf("sync result package outbox: %w", err)
	}
	return m.CommitResultOutboxCapture(ctx, outbox.ResultOutboxKey, metadata)
}

func readResultMetadata(directory *os.Root) (protocol.ResultPackageMetadata, error) {
	file, info, err := openRegularFile(directory, protocol.ResultManifestFileName)
	if err != nil {
		return protocol.ResultPackageMetadata{}, err
	}
	defer file.Close()
	if info.Size() < 1 || info.Size() > protocol.MaximumResultManifestBytes {
		return protocol.ResultPackageMetadata{}, errors.New("result manifest file has invalid size")
	}
	data, err := io.ReadAll(io.LimitReader(file, protocol.MaximumResultManifestBytes+1))
	if err != nil {
		return protocol.ResultPackageMetadata{}, err
	}
	manifest, err := protocol.DecodeResultManifest(data)
	if err != nil {
		return protocol.ResultPackageMetadata{}, err
	}
	canonical, descriptor, err := protocol.EncodeResultManifest(manifest)
	if err != nil {
		return protocol.ResultPackageMetadata{}, err
	}
	if string(canonical) != string(data) {
		return protocol.ResultPackageMetadata{}, errors.New("result manifest file is not canonical")
	}
	return protocol.ResultPackageMetadata{Manifest: data, ManifestDescriptor: descriptor}, nil
}

func removePublishingDirectory(root *os.Root, name string) error {
	if err := removePackageDirectory(root, name); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}
