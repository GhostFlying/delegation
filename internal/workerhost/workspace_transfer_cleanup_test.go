package workerhost

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
)

func TestHostCleansOnlyStrictlyNamedStartupWorkspaceOrphans(t *testing.T) {
	workspaceID := newTestID()
	sourceTransferID := newTestID()
	targetTransferID := newTestID()
	exact := []string{
		sourceTransferDirectoryName(sourceTransferID),
		targetTransferDirectoryName(targetTransferID),
		workspaceSyncName(testTreeID, workspaceID) + pendingDirectorySuffix,
	}
	preserved := []string{
		sourceTransferDirectoryName(sourceTransferID) + ".keep",
		"transfer-source-not-a-uuid",
		workspaceSyncName(testTreeID, workspaceID),
		workspaceSyncName(testTreeID, workspaceID) + pendingDirectorySuffix + ".keep",
	}
	_, _, paths := newTestHostWithStateSetup(t, 1, "", func(_ *store.PeerStore, root string) {
		for _, name := range append(append([]string(nil), exact...), preserved...) {
			if err := os.MkdirAll(filepath.Join(root, name, "nested"), 0o700); err != nil {
				t.Fatal(err)
			}
		}
	})
	workspaceRoot := filepath.Join(filepath.Dir(paths.configPath), "workspaces")
	for _, name := range exact {
		if _, err := os.Lstat(filepath.Join(workspaceRoot, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("startup orphan %q remains: %v", name, err)
		}
	}
	for _, name := range preserved {
		if _, err := os.Lstat(filepath.Join(workspaceRoot, name)); err != nil {
			t.Fatalf("non-orphan %q was removed: %v", name, err)
		}
	}
}

func TestHostCloseCleansInMemoryWorkspaceTransfers(t *testing.T) {
	host, _, _ := newTestHost(t, 1)
	root := host.workspaceRoot.Name()
	source := control.NewRootPrincipal(
		testControllerID, testTreeID, testParentID, testDeviceID,
	).Identity()
	workspaceID := newTestID()
	transferID := newTestID()
	pendingName := workspaceSyncName(testTreeID, workspaceID) + pendingDirectorySuffix
	sourceName := sourceTransferDirectoryName(transferID)
	targetName := targetTransferDirectoryName(transferID)
	for _, name := range []string{pendingName, sourceName, targetName} {
		if err := os.MkdirAll(filepath.Join(root, name), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	transfer := protocol.WorkspaceTransferManifest{TransferID: transferID, WorkspaceID: workspaceID}
	host.workspaceTransferMu.Lock()
	host.pendingWorkspaces[workspacePreparationKey(testTreeID, workspaceID)] = pendingWorkspacePreparation{
		TreeID: testTreeID, Source: source, WorkspaceID: workspaceID,
		TransferID: transferID, TemporaryName: pendingName,
	}
	host.outboundTransfers[transferID] = outboundWorkspaceTransfer{
		TreeID: testTreeID, Source: source, WorkspaceID: workspaceID,
		DirectoryName: sourceName, Transfer: transfer,
	}
	host.inboundTransfers[transferID] = &inboundWorkspaceTransfer{
		TreeID: testTreeID, Source: source, DirectoryName: targetName,
		PendingName: pendingName, Transfer: transfer,
	}
	host.workspaceTransferMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := host.Close(ctx); err != nil {
		cancel()
		t.Fatal(err)
	}
	cancel()
	for _, name := range []string{pendingName, sourceName, targetName} {
		if _, err := os.Lstat(filepath.Join(root, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("Close() left workspace transfer path %q: %v", name, err)
		}
	}
	host.workspaceTransferMu.Lock()
	defer host.workspaceTransferMu.Unlock()
	if len(host.pendingWorkspaces) != 0 || len(host.outboundTransfers) != 0 || len(host.inboundTransfers) != 0 {
		t.Fatalf(
			"Close() left workspace transfer state: pending=%d outbound=%d inbound=%d",
			len(host.pendingWorkspaces), len(host.outboundTransfers), len(host.inboundTransfers),
		)
	}
}

func TestWorkspaceTransferPendingOwnership(t *testing.T) {
	host, state, _ := newTestHost(t, 1)
	gitURL, sourcePath := createHostedTestRepository(t)
	if err := os.WriteFile(filepath.Join(sourcePath, "unpublished.txt"), []byte("unpublished\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runTestGit(t, sourcePath, "add", "unpublished.txt")
	runTestGit(
		t, sourcePath,
		"-c", "user.name=Delegation Test", "-c", "user.email=test@example.invalid",
		"commit", "-m", "unpublished",
	)
	source := control.NewRootPrincipal(
		testControllerID, testTreeID, testParentID, testDeviceID,
	).Identity()
	workspaceID := newTestID()
	inspected, err := host.InspectWorkspace(context.Background(), WorkspaceInspectRequest{
		TreeID: testTreeID, Source: source,
		Params: protocol.InspectWorkspaceParams{
			SyncID: workspaceID, GitURL: gitURL, SourcePath: sourcePath,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := host.PrepareWorkspace(context.Background(), WorkspacePrepareRequest{
		TreeID: testTreeID, Source: source,
		Params: protocol.PrepareWorkspaceParams{
			WorkspaceID: workspaceID, SourceAgentID: source.AgentID,
			SourceDeviceID: source.DeviceID, Manifest: inspected.Manifest,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	otherTransferID := newTestID()
	controlRequest := func(id string) WorkspaceTransferControlRequest {
		return WorkspaceTransferControlRequest{
			TreeID: testTreeID, Source: source,
			Params: protocol.WorkspaceTransferControlParams{
				WorkspaceID: workspaceID, TransferID: id,
				SourceAgentID: source.AgentID, SourceDeviceID: source.DeviceID,
			},
		}
	}
	if _, err := host.CancelWorkspaceTransfer(context.Background(), controlRequest(otherTransferID)); err != nil {
		t.Fatal(err)
	}
	pendingKey := workspacePreparationKey(testTreeID, workspaceID)
	host.workspaceTransferMu.Lock()
	pending, found := host.pendingWorkspaces[pendingKey]
	host.workspaceTransferMu.Unlock()
	if !found || pending.TransferID != workspaceID {
		t.Fatalf("provisional pending workspace after unrelated cancel = %#v, found %v", pending, found)
	}
	if _, err := host.workspaceRoot.Lstat(pending.TemporaryName); err != nil {
		t.Fatalf("unrelated cancel removed provisional pending path %q: %v", pending.TemporaryName, err)
	}
	if _, err := host.CancelWorkspaceTransfer(context.Background(), controlRequest(workspaceID)); err != nil {
		t.Fatal(err)
	}
	host.workspaceTransferMu.Lock()
	_, found = host.pendingWorkspaces[pendingKey]
	host.workspaceTransferMu.Unlock()
	if found {
		t.Fatal("provisional cancel left pending workspace state")
	}
	if _, err := host.workspaceRoot.Lstat(pending.TemporaryName); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("provisional cancel left pending path %q: %v", pending.TemporaryName, err)
	}
	prepared, err = host.PrepareWorkspace(context.Background(), WorkspacePrepareRequest{
		TreeID: testTreeID, Source: source,
		Params: protocol.PrepareWorkspaceParams{
			WorkspaceID: workspaceID, SourceAgentID: source.AgentID,
			SourceDeviceID: source.DeviceID, Manifest: inspected.Manifest,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	transferID := newTestID()
	created, err := host.CreateWorkspaceTransfer(context.Background(), WorkspaceCreateTransferRequest{
		TreeID: testTreeID, Source: source,
		Params: protocol.CreateWorkspaceTransferParams{
			TransferID: transferID, WorkspaceID: workspaceID,
			GitURL: gitURL, SourcePath: sourcePath, Manifest: inspected.Manifest,
			BasisOIDs: prepared.BasisOIDs, BundleRequired: true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.BeginWorkspaceTransfer(context.Background(), WorkspaceBeginTransferRequest{
		TreeID: testTreeID, Source: source,
		Params: protocol.BeginWorkspaceTransferParams{
			SourceAgentID: source.AgentID, SourceDeviceID: source.DeviceID,
			Manifest: inspected.Manifest, Transfer: created.Transfer,
		},
	}); err != nil {
		t.Fatal(err)
	}
	host.workspaceTransferMu.Lock()
	pending, found = host.pendingWorkspaces[pendingKey]
	inbound, inboundFound := host.inboundTransfers[transferID]
	host.workspaceTransferMu.Unlock()
	if !found || pending.TransferID != transferID || !inboundFound ||
		inbound.Transfer.TransferID != transferID || inbound.PendingName != pending.TemporaryName {
		t.Fatalf(
			"Begin did not atomically bind pending ownership: pending=%#v found=%v inbound=%#v found=%v",
			pending, found, inbound, inboundFound,
		)
	}
	conflicting := created.Transfer
	conflicting.TransferID = otherTransferID
	if _, err := host.BeginWorkspaceTransfer(context.Background(), WorkspaceBeginTransferRequest{
		TreeID: testTreeID, Source: source,
		Params: protocol.BeginWorkspaceTransferParams{
			SourceAgentID: source.AgentID, SourceDeviceID: source.DeviceID,
			Manifest: inspected.Manifest, Transfer: conflicting,
		},
	}); !errors.Is(err, store.ErrWorkerReservationConflict) {
		t.Fatalf("second transfer bound the same pending workspace: %v", err)
	}
	if _, err := host.CancelWorkspaceTransfer(context.Background(), controlRequest(otherTransferID)); err != nil {
		t.Fatal(err)
	}
	host.workspaceTransferMu.Lock()
	pending, pendingFound := host.pendingWorkspaces[pendingKey]
	_, outboundFound := host.outboundTransfers[transferID]
	_, inboundFound = host.inboundTransfers[transferID]
	host.workspaceTransferMu.Unlock()
	if !pendingFound || pending.TransferID != transferID || !outboundFound || !inboundFound {
		t.Fatalf(
			"unrelated cancel changed owned transfer: pending=%#v found=%v outbound=%v inbound=%v",
			pending, pendingFound, outboundFound, inboundFound,
		)
	}
	for _, name := range []string{
		pending.TemporaryName, sourceTransferDirectoryName(transferID), targetTransferDirectoryName(transferID),
	} {
		if _, err := host.workspaceRoot.Lstat(name); err != nil {
			t.Fatalf("unrelated cancel removed owned path %q: %v", name, err)
		}
	}
	if _, err := host.CancelWorkspaceTransfer(context.Background(), controlRequest(transferID)); err != nil {
		t.Fatal(err)
	}
	if _, err := state.GetPreparedWorkspace(context.Background(), store.PreparedWorkspaceKey{
		ControllerID: testControllerID, TreeID: testTreeID, WorkspaceID: workspaceID,
	}); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("canceled owned transfer published a workspace: %v", err)
	}
}
