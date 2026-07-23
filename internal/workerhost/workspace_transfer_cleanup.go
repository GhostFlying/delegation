package workerhost

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/GhostFlying/delegation/internal/identity"
)

const (
	workspaceDirectoryPrefix = "workspace-"
	pendingDirectorySuffix   = ".pending"
)

func (h *Host) acquireWorkspaceOperation(ctx context.Context) (func(), error) {
	releaseHost, err := h.acquireOperation(ctx)
	if err != nil {
		return nil, err
	}
	h.workspaceOperations.RLock()
	if err := ctx.Err(); err != nil {
		h.workspaceOperations.RUnlock()
		releaseHost()
		return nil, err
	}
	return func() {
		h.workspaceOperations.RUnlock()
		releaseHost()
	}, nil
}

// CleanupWorkspaceTransfers discards all session-scoped workspace transfer state.
func (h *Host) CleanupWorkspaceTransfers(ctx context.Context) error {
	releaseHost, err := h.acquireOperation(ctx)
	if err != nil {
		return err
	}
	defer releaseHost()
	h.workspaceOperations.Lock()
	defer h.workspaceOperations.Unlock()
	return h.cleanupWorkspaceTransfers(ctx)
}

func (h *Host) cleanupWorkspaceTransfers(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	h.workspaceTransferMu.Lock()
	names := make([]string, 0, len(h.pendingWorkspaces)+len(h.outboundTransfers)+len(h.inboundTransfers))
	for _, pending := range h.pendingWorkspaces {
		expected := workspaceSyncName(pending.TreeID, pending.WorkspaceID) + pendingDirectorySuffix
		if pending.TemporaryName != expected || !isPendingWorkspaceDirectoryName(expected) {
			h.workspaceTransferMu.Unlock()
			return errors.New("pending workspace has an invalid cleanup path")
		}
		names = append(names, pending.TemporaryName)
	}
	for transferID, transfer := range h.outboundTransfers {
		expected := sourceTransferDirectoryName(transferID)
		if transfer.Transfer.TransferID != transferID || transfer.DirectoryName != expected ||
			!isTransferDirectoryName(expected) {
			h.workspaceTransferMu.Unlock()
			return errors.New("outbound workspace transfer has an invalid cleanup path")
		}
		names = append(names, transfer.DirectoryName)
	}
	for transferID, transfer := range h.inboundTransfers {
		expected := targetTransferDirectoryName(transferID)
		if transfer.Transfer.TransferID != transferID || transfer.DirectoryName != expected ||
			!isTransferDirectoryName(expected) {
			h.workspaceTransferMu.Unlock()
			return errors.New("inbound workspace transfer has an invalid cleanup path")
		}
		names = append(names, transfer.DirectoryName)
	}
	h.workspaceTransferMu.Unlock()
	if err := removeWorkspaceOrphans(ctx, h.workspaceRoot, names); err != nil {
		return err
	}
	h.workspaceTransferMu.Lock()
	clear(h.pendingWorkspaces)
	clear(h.outboundTransfers)
	clear(h.inboundTransfers)
	h.workspaceTransferMu.Unlock()
	return nil
}

func cleanupStartupWorkspaceOrphans(ctx context.Context, root *os.Root) error {
	directory, err := root.Open(".")
	if err != nil {
		return fmt.Errorf("open worker workspace root for orphan cleanup: %w", err)
	}
	entries, readErr := directory.ReadDir(-1)
	closeErr := directory.Close()
	if readErr != nil || closeErr != nil {
		return fmt.Errorf("read worker workspace root for orphan cleanup: %w", errors.Join(readErr, closeErr))
	}
	names := make([]string, 0)
	for _, entry := range entries {
		if isTransferDirectoryName(entry.Name()) || isPendingWorkspaceDirectoryName(entry.Name()) {
			names = append(names, entry.Name())
		}
	}
	if err := removeWorkspaceOrphans(ctx, root, names); err != nil {
		return fmt.Errorf("clean startup workspace orphans: %w", err)
	}
	return nil
}

func removeWorkspaceOrphans(ctx context.Context, root *os.Root, names []string) error {
	slices.Sort(names)
	names = slices.Compact(names)
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !isTransferDirectoryName(name) && !isPendingWorkspaceDirectoryName(name) {
			return fmt.Errorf("refuse to remove unexpected workspace path %q", name)
		}
		if err := root.RemoveAll(name); err != nil {
			return fmt.Errorf("remove workspace transfer orphan %q: %w", name, err)
		}
	}
	if len(names) != 0 {
		return syncDirectory(root)
	}
	return nil
}

func isTransferDirectoryName(name string) bool {
	for _, prefix := range []string{"transfer-source-", "transfer-target-"} {
		if strings.HasPrefix(name, prefix) && identity.ValidateID(strings.TrimPrefix(name, prefix)) == nil {
			return true
		}
	}
	return false
}

func isPendingWorkspaceDirectoryName(name string) bool {
	if !strings.HasPrefix(name, workspaceDirectoryPrefix) || !strings.HasSuffix(name, pendingDirectorySuffix) {
		return false
	}
	digest := strings.TrimSuffix(strings.TrimPrefix(name, workspaceDirectoryPrefix), pendingDirectorySuffix)
	if len(digest) != 32 {
		return false
	}
	for _, character := range digest {
		if character < '0' || character > '9' && character < 'a' || character > 'f' {
			return false
		}
	}
	return true
}
