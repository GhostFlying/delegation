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

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
)

var (
	ErrApplyPackageEvicted     = errors.New("result package was evicted from the root peer")
	ErrApplyPackageUnavailable = errors.New("result package is not available in the root peer")
)

type ApplyPackageRequest struct {
	Root      control.PrincipalIdentity
	PackageID string
}

type MaterializeApplyPackageRequest struct {
	ApplyPackageRequest
	Authorization protocol.AuthorizeResultApplyResult
}

// LookupApplyManifest returns only verified local metadata. It never returns a
// payload path or rollout content.
func (m *Manager) LookupApplyManifest(
	ctx context.Context,
	request ApplyPackageRequest,
) (protocol.ResultManifest, error) {
	authority, err := m.sinkAuthority(request.Root.TreeID, request.Root)
	if err != nil {
		return protocol.ResultManifest{}, err
	}
	if err := request.Root.Validate(); err != nil || request.Root.ParentAgentID != "" {
		return protocol.ResultManifest{}, store.ErrResultPackageAuthority
	}
	lock := m.lock(request.PackageID)
	lock.Lock()
	defer lock.Unlock()
	inbox, err := m.state.GetResultInbox(ctx, authority, request.PackageID)
	if errors.Is(err, store.ErrNotFound) {
		return protocol.ResultManifest{}, ErrApplyPackageEvicted
	}
	if err != nil {
		return protocol.ResultManifest{}, err
	}
	switch inbox.State {
	case store.ResultInboxEvictionTombstone:
		return protocol.ResultManifest{}, ErrApplyPackageEvicted
	case store.ResultInboxReceiving:
		return protocol.ResultManifest{}, ErrApplyPackageUnavailable
	case store.ResultInboxAvailable:
	default:
		return protocol.ResultManifest{}, ErrApplyPackageUnavailable
	}
	if err := m.verifyAvailable(ctx, inbox); err != nil {
		return protocol.ResultManifest{}, err
	}
	if inbox.Manifest.PackageID != request.PackageID ||
		inbox.Manifest.ControllerID != request.Root.ControllerID ||
		inbox.Manifest.TreeID != request.Root.TreeID {
		return protocol.ResultManifest{}, store.ErrResultPackageAuthority
	}
	return inbox.Manifest, nil
}

// MaterializeApplyWorkspace copies only verified Git result parts into an
// already-open private directory. Rollout bytes remain in the result inbox.
func (m *Manager) MaterializeApplyWorkspace(
	ctx context.Context,
	request MaterializeApplyPackageRequest,
	destination *os.Root,
) (protocol.ResultManifest, error) {
	if destination == nil {
		return protocol.ResultManifest{}, errors.New("result apply destination is required")
	}
	if err := request.Authorization.Validate(); err != nil {
		return protocol.ResultManifest{}, err
	}
	if request.Authorization.PackageID != request.PackageID {
		return protocol.ResultManifest{}, store.ErrResultPackageConflict
	}
	authority, err := m.sinkAuthority(request.Root.TreeID, request.Root)
	if err != nil {
		return protocol.ResultManifest{}, err
	}
	lock := m.lock(request.PackageID)
	lock.Lock()
	defer lock.Unlock()
	inbox, err := m.state.GetResultInbox(ctx, authority, request.PackageID)
	if errors.Is(err, store.ErrNotFound) {
		return protocol.ResultManifest{}, ErrApplyPackageEvicted
	}
	if err != nil {
		return protocol.ResultManifest{}, err
	}
	if inbox.State == store.ResultInboxEvictionTombstone {
		return protocol.ResultManifest{}, ErrApplyPackageEvicted
	}
	if inbox.State != store.ResultInboxAvailable {
		return protocol.ResultManifest{}, ErrApplyPackageUnavailable
	}
	if err := m.verifyAvailable(ctx, inbox); err != nil {
		return protocol.ResultManifest{}, err
	}
	manifest := inbox.Manifest
	if manifest.PackageID != request.PackageID ||
		manifest.ControllerID != request.Root.ControllerID || manifest.TreeID != request.Root.TreeID ||
		inbox.Metadata.ManifestDescriptor.SHA256 != request.Authorization.ManifestSHA256 ||
		manifest.Workspace.WorkspaceID != request.Authorization.WorkspaceID ||
		manifest.Workspace.BaseManifestHash != request.Authorization.BaseManifestHash {
		return protocol.ResultManifest{}, store.ErrResultPackageAuthority
	}
	entries, err := fs.ReadDir(destination.FS(), ".")
	if err != nil {
		return protocol.ResultManifest{}, fmt.Errorf("inspect result apply destination: %w", err)
	}
	if len(entries) != 0 {
		return protocol.ResultManifest{}, errors.New("result apply destination must be empty")
	}
	source, err := openPrivateDirectory(m.inbox, request.PackageID)
	if err != nil {
		return protocol.ResultManifest{}, err
	}
	defer source.Close()
	created := make([]string, 0, 2)
	keep := false
	defer func() {
		if keep {
			return
		}
		for _, name := range created {
			_ = destination.Remove(name)
		}
	}()
	for _, descriptor := range manifest.Parts {
		if descriptor.Kind != protocol.ResultPackagePartChangesBundle &&
			descriptor.Kind != protocol.ResultPackagePartChangesOverlay {
			continue
		}
		name, _ := descriptor.Kind.FileName()
		input, info, err := openRegularFile(source, name)
		if err != nil {
			return protocol.ResultManifest{}, err
		}
		if info.Size() != descriptor.Size {
			_ = input.Close()
			return protocol.ResultManifest{}, errors.New("result apply part size changed")
		}
		output, err := destination.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			_ = input.Close()
			return protocol.ResultManifest{}, err
		}
		created = append(created, name)
		digest := sha256.New()
		written, copyErr := io.Copy(
			io.MultiWriter(output, digest),
			&contextReader{ctx: ctx, source: io.LimitReader(input, descriptor.Size+1)},
		)
		closeErr := errors.Join(input.Close(), output.Sync(), output.Close())
		if copyErr != nil || closeErr != nil {
			return protocol.ResultManifest{}, errors.Join(copyErr, closeErr)
		}
		if written != descriptor.Size || hex.EncodeToString(digest.Sum(nil)) != descriptor.SHA256 {
			return protocol.ResultManifest{}, errors.New("result apply part does not match its descriptor")
		}
	}
	if err := syncDirectory(destination); err != nil {
		return protocol.ResultManifest{}, fmt.Errorf("sync materialized result workspace: %w", err)
	}
	keep = true
	return manifest, nil
}
