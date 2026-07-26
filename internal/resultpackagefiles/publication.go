package resultpackagefiles

import (
	"context"

	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
)

func (m *Manager) ListPendingResultPublications(
	ctx context.Context,
) ([]store.ResultOutbox, error) {
	return m.state.ListPendingResultPublications(
		ctx, m.controllerID, m.deviceID, store.MaximumPeerResultPackages,
	)
}

func (m *Manager) AcknowledgeResultPackageMetadata(
	ctx context.Context,
	key store.ResultOutboxKey,
	metadata protocol.ResultPackageMetadata,
) (store.ResultOutbox, error) {
	lock := m.lock(key.PackageID)
	lock.Lock()
	defer lock.Unlock()
	result, err := m.state.AcknowledgeResultOutboxMetadata(ctx, key, metadata, m.now())
	if err == nil {
		m.signalOutboxChange()
	}
	return result, err
}
