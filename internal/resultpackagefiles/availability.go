package resultpackagefiles

import (
	"bytes"
	"context"
	"errors"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
)

type PackageAvailability string

const (
	PackageAvailable PackageAvailability = "available"
	PackageEvicted   PackageAvailability = "evicted"
)

type LookupAvailabilityRequest struct {
	Root     control.PrincipalIdentity
	Manifest protocol.ResultManifest
}

type LookupAvailabilityResult struct {
	PackageID    string
	Availability PackageAvailability
}

// LookupResultPackageAvailability verifies broker metadata against this root
// peer's durable inbox. Broker delivery state alone never proves that payload
// bytes are locally available.
func (m *Manager) LookupResultPackageAvailability(
	ctx context.Context,
	request LookupAvailabilityRequest,
) (LookupAvailabilityResult, error) {
	authority, err := m.sinkAuthority(request.Root.TreeID, request.Root)
	if err != nil {
		return LookupAvailabilityResult{}, err
	}
	requestedManifest, _, err := protocol.EncodeResultManifest(request.Manifest)
	if err != nil {
		return LookupAvailabilityResult{}, err
	}
	if request.Manifest.ControllerID != authority.ControllerID ||
		request.Manifest.TreeID != authority.TreeID {
		return LookupAvailabilityResult{}, store.ErrResultPackageAuthority
	}
	lock := m.lock(request.Manifest.PackageID)
	lock.Lock()
	defer lock.Unlock()
	inbox, err := m.state.GetResultInbox(ctx, authority, request.Manifest.PackageID)
	if errors.Is(err, store.ErrNotFound) {
		return LookupAvailabilityResult{
			PackageID: request.Manifest.PackageID, Availability: PackageEvicted,
		}, nil
	}
	if err != nil {
		return LookupAvailabilityResult{}, err
	}
	durableManifest, _, err := protocol.EncodeResultManifest(inbox.Manifest)
	if err != nil {
		return LookupAvailabilityResult{}, err
	}
	if !bytes.Equal(requestedManifest, durableManifest) {
		return LookupAvailabilityResult{}, store.ErrResultPackageConflict
	}
	switch inbox.State {
	case store.ResultInboxAvailable:
		if err := m.verifyAvailable(ctx, inbox); err != nil {
			return LookupAvailabilityResult{}, err
		}
		return LookupAvailabilityResult{
			PackageID: inbox.PackageID, Availability: PackageAvailable,
		}, nil
	case store.ResultInboxEvictionTombstone:
		return LookupAvailabilityResult{
			PackageID: inbox.PackageID, Availability: PackageEvicted,
		}, nil
	case store.ResultInboxReceiving:
		return LookupAvailabilityResult{}, store.ErrResultPackageTransition
	default:
		return LookupAvailabilityResult{}, store.ErrResultPackageTransition
	}
}
