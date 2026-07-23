package workerhost

import (
	"context"
	"crypto/rand"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
)

func TestHostTransfersBundleWorkspace(t *testing.T) {
	for _, test := range []struct {
		name         string
		full         bool
		dirty        bool
		strategy     protocol.WorkspaceStrategy
		wantWarning  bool
		cleanupRetry bool
	}{
		{name: "thin", strategy: protocol.WorkspaceStrategyThin},
		{name: "thin with overlay", dirty: true, strategy: protocol.WorkspaceStrategyThin},
		{name: "thin cleanup retry", strategy: protocol.WorkspaceStrategyThin, cleanupRetry: true},
		{
			name: "self contained", full: true, strategy: protocol.WorkspaceStrategyFull,
			wantWarning: true,
		},
		{
			name: "self contained with overlay", full: true, dirty: true,
			strategy: protocol.WorkspaceStrategyFull, wantWarning: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			host, state, _ := newTestHost(t, 1)
			gitURL, sourcePath := createHostedTestRepository(t)
			payload := make([]byte, 3*protocol.WorkspaceArtifactChunkBytes)
			if _, err := rand.Read(payload); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(sourcePath, "unpublished.bin"), payload, 0o600); err != nil {
				t.Fatal(err)
			}
			runTestGit(t, sourcePath, "add", "unpublished.bin")
			runTestGit(
				t, sourcePath,
				"-c", "user.name=Delegation Test", "-c", "user.email=test@example.invalid",
				"commit", "-m", "unpublished",
			)
			if test.full {
				gitURL = "https://127.0.0.1:1/unavailable.git"
			}
			if test.dirty {
				if err := os.WriteFile(
					filepath.Join(sourcePath, "nested", "dirty-untracked.txt"),
					[]byte("dirty overlay\n"), 0o600,
				); err != nil {
					t.Fatal(err)
				}
			}
			source := control.NewRootPrincipal(
				testControllerID, testTreeID, testParentID, testDeviceID,
			).Identity()
			workspaceID := newTestID()
			inspected, err := host.InspectWorkspace(context.Background(), WorkspaceInspectRequest{
				TreeID: testTreeID, Source: source,
				Params: protocol.InspectWorkspaceParams{
					SyncID: workspaceID, GitURL: gitURL,
					SourcePath: filepath.Join(sourcePath, "nested"),
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			if inspected.Manifest.Clean == test.dirty {
				t.Fatalf("source clean = %t, want %t", inspected.Manifest.Clean, !test.dirty)
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
			if prepared.Outcome != protocol.WorkspacePrepareTransferRequired ||
				!prepared.BundleRequired || prepared.OverlayRequired != test.dirty ||
				test.full != (len(prepared.BasisOIDs) == 0) {
				t.Fatalf("target preparation = %#v", prepared)
			}
			transferID := newTestID()
			created, err := host.CreateWorkspaceTransfer(context.Background(), WorkspaceCreateTransferRequest{
				TreeID: testTreeID, Source: source,
				Params: protocol.CreateWorkspaceTransferParams{
					TransferID: transferID, WorkspaceID: workspaceID,
					GitURL: gitURL, SourcePath: filepath.Join(sourcePath, "nested"),
					Manifest: inspected.Manifest, BasisOIDs: prepared.BasisOIDs,
					BundleRequired: true, OverlayRequired: test.dirty,
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			if created.Transfer.Strategy != test.strategy ||
				slices.Contains(created.Transfer.Warnings, protocol.WorkspaceWarningFullHistoryFallback) != test.wantWarning {
				t.Fatalf("source transfer = %#v", created.Transfer)
			}
			wantKinds := []protocol.WorkspaceArtifactKind{protocol.WorkspaceArtifactBundle}
			if test.dirty {
				wantKinds = append(wantKinds, protocol.WorkspaceArtifactOverlay)
			}
			gotKinds := make([]protocol.WorkspaceArtifactKind, 0, len(created.Transfer.Artifacts))
			for _, artifact := range created.Transfer.Artifacts {
				gotKinds = append(gotKinds, artifact.Kind)
			}
			if !slices.Equal(gotKinds, wantKinds) {
				t.Fatalf("source transfer artifacts = %v, want %v", gotKinds, wantKinds)
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
			for _, artifact := range created.Transfer.Artifacts {
				for offset := int64(0); offset < artifact.Size; {
					limit := min(int64(protocol.WorkspaceArtifactChunkBytes), artifact.Size-offset)
					chunk, err := host.ReadWorkspaceArtifact(context.Background(), WorkspaceReadArtifactRequest{
						TreeID: testTreeID, Source: source,
						Params: protocol.ReadWorkspaceArtifactParams{
							TransferID: transferID, Kind: artifact.Kind, Offset: offset, Limit: int(limit),
						},
					})
					if err != nil {
						t.Fatal(err)
					}
					written, err := host.WriteWorkspaceArtifact(context.Background(), WorkspaceWriteArtifactRequest{
						TreeID: testTreeID, Source: source,
						Params: protocol.WriteWorkspaceArtifactParams{
							WorkspaceID: workspaceID, TransferID: transferID,
							Kind: artifact.Kind, Offset: offset, Data: chunk.Data,
						},
					})
					if err != nil {
						t.Fatal(err)
					}
					offset = written.NextOffset
				}
			}
			controlRequest := WorkspaceTransferControlRequest{
				TreeID: testTreeID, Source: source,
				Params: protocol.WorkspaceTransferControlParams{
					WorkspaceID: workspaceID, TransferID: transferID,
					SourceAgentID: source.AgentID, SourceDeviceID: source.DeviceID,
				},
			}
			if test.cleanupRetry {
				removeWorkspaceTransfer := host.removeWorkspaceTransfer
				failed := false
				host.removeWorkspaceTransfer = func(name string) error {
					if !failed && name == targetTransferDirectoryName(transferID) {
						failed = true
						return errors.New("simulated Windows file sharing violation")
					}
					return removeWorkspaceTransfer(name)
				}
				if _, err := host.FinishWorkspaceTransfer(context.Background(), controlRequest); err == nil ||
					!strings.Contains(err.Error(), "remove completed target workspace transfer") {
					t.Fatalf("first finish cleanup = %v, want retryable removal error", err)
				}
				host.workspaceTransferMu.Lock()
				_, inboundFound := host.inboundTransfers[transferID]
				_, pendingFound := host.pendingWorkspaces[workspacePreparationKey(testTreeID, workspaceID)]
				host.workspaceTransferMu.Unlock()
				if !inboundFound || !pendingFound {
					t.Fatal("failed finish cleanup discarded retry ownership")
				}
				if _, err := host.workspaceRoot.Lstat(targetTransferDirectoryName(transferID)); err != nil {
					t.Fatalf("failed finish cleanup lost transfer directory: %v", err)
				}
				for name, mutate := range map[string]func(*control.PrincipalIdentity){
					"agent":  func(identity *control.PrincipalIdentity) { identity.AgentID = newTestID() },
					"device": func(identity *control.PrincipalIdentity) { identity.DeviceID = newTestID() },
				} {
					t.Run("prepared authority drift "+name, func(t *testing.T) {
						spoofedSource := source
						mutate(&spoofedSource)
						host.workspaceTransferMu.Lock()
						host.inboundTransfers[transferID].Source = spoofedSource
						host.workspaceTransferMu.Unlock()
						spoofedControl := controlRequest
						spoofedControl.Source = spoofedSource
						spoofedControl.Params.SourceAgentID = spoofedSource.AgentID
						spoofedControl.Params.SourceDeviceID = spoofedSource.DeviceID
						if _, err := host.FinishWorkspaceTransfer(context.Background(), spoofedControl); !errors.Is(err, store.ErrWorkerReservationConflict) {
							t.Fatalf("finish with drifted prepared-workspace authority = %v", err)
						}
						host.workspaceTransferMu.Lock()
						host.inboundTransfers[transferID].Source = source
						host.workspaceTransferMu.Unlock()
					})
				}
			}
			finished, err := host.FinishWorkspaceTransfer(context.Background(), controlRequest)
			if err != nil {
				t.Fatal(err)
			}
			if finished.Workspace.Outcome != protocol.WorkspacePrepareReady ||
				finished.Workspace.Strategy != test.strategy {
				t.Fatalf("finished workspace = %#v", finished)
			}
			repeated, err := host.FinishWorkspaceTransfer(context.Background(), controlRequest)
			if err != nil || repeated.Workspace.ManifestHash != finished.Workspace.ManifestHash {
				t.Fatalf("idempotent finish = %#v, %v", repeated, err)
			}
			stored, err := state.GetPreparedWorkspace(context.Background(), store.PreparedWorkspaceKey{
				ControllerID: testControllerID, TreeID: testTreeID, WorkspaceID: workspaceID,
			})
			if err != nil {
				t.Fatal(err)
			}
			if data, err := os.ReadFile(filepath.Join(stored.WorkspacePath, "unpublished.bin")); err != nil ||
				!slices.Equal(data, payload) {
				t.Fatalf("prepared bundle payload length = %d, %v", len(data), err)
			}
			if stored.Clean == test.dirty || stored.SourceSnapshotHash != inspected.Manifest.SourceSnapshotHash {
				t.Fatalf("stored workspace snapshot = %#v", stored)
			}
			if test.dirty {
				data, err := os.ReadFile(filepath.Join(stored.WorkspacePath, "nested", "dirty-untracked.txt"))
				if err != nil || string(data) != "dirty overlay\n" {
					t.Fatalf("prepared bundle overlay = %q, %v", data, err)
				}
			}
			if _, err := host.CancelWorkspaceTransfer(context.Background(), controlRequest); err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{
				sourceTransferDirectoryName(transferID), targetTransferDirectoryName(transferID),
			} {
				if _, err := host.workspaceRoot.Lstat(name); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("transfer directory %q still exists: %v", name, err)
				}
			}
		})
	}
}

func TestWorkspaceOperationsRejectNonRootAndNonlocalAuthorities(t *testing.T) {
	host, _, _ := newTestHost(t, 1)
	root := control.NewRootPrincipal(
		testControllerID, testTreeID, testParentID, testDeviceID,
	).Identity()
	remoteRoot := root
	remoteRoot.DeviceID = newTestID()
	if err := validateLocalWorkspaceRootRequest(
		testTreeID, remoteRoot, testControllerID, testDeviceID,
	); err == nil || err.Error() != "workspace source is not a local tree root" {
		t.Fatalf("local workspace validation from another device = %v", err)
	}
	if _, err := host.InspectWorkspace(context.Background(), WorkspaceInspectRequest{
		TreeID: testTreeID, Source: remoteRoot,
	}); err == nil || err.Error() != "workspace source is not a local tree root" {
		t.Fatalf("InspectWorkspace from another device = %v", err)
	}
	worker := control.NewWorkerPrincipal(
		testControllerID, testTreeID, newTestID(), root.AgentID, testDeviceID,
	).Identity()
	if err := validateWorkspaceRootRequest(testTreeID, worker, testControllerID); err == nil ||
		err.Error() != "workspace source is not a tree root" {
		t.Fatalf("workspace root validation for worker = %v", err)
	}
	if _, err := host.BeginWorkspaceTransfer(context.Background(), WorkspaceBeginTransferRequest{
		TreeID: testTreeID, Source: worker,
	}); err == nil || err.Error() != "workspace transfer begin authority is invalid" {
		t.Fatalf("BeginWorkspaceTransfer for worker = %v", err)
	}

	for name, mutate := range map[string]func(*protocol.BeginWorkspaceTransferParams, *protocol.WorkspaceTransferControlParams){
		"agent": func(
			begin *protocol.BeginWorkspaceTransferParams,
			controlParams *protocol.WorkspaceTransferControlParams,
		) {
			begin.SourceAgentID = newTestID()
			controlParams.SourceAgentID = newTestID()
		},
		"device": func(
			begin *protocol.BeginWorkspaceTransferParams,
			controlParams *protocol.WorkspaceTransferControlParams,
		) {
			begin.SourceDeviceID = newTestID()
			controlParams.SourceDeviceID = newTestID()
		},
	} {
		t.Run(name, func(t *testing.T) {
			source := root
			beginParams := protocol.BeginWorkspaceTransferParams{
				SourceAgentID: source.AgentID, SourceDeviceID: source.DeviceID,
			}
			controlParams := protocol.WorkspaceTransferControlParams{
				WorkspaceID: newTestID(), TransferID: newTestID(),
				SourceAgentID: source.AgentID, SourceDeviceID: source.DeviceID,
			}
			mutate(&beginParams, &controlParams)
			if _, err := host.BeginWorkspaceTransfer(context.Background(), WorkspaceBeginTransferRequest{
				TreeID: testTreeID, Source: source, Params: beginParams,
			}); err == nil || err.Error() != "workspace transfer begin authority is invalid" {
				t.Fatalf("BeginWorkspaceTransfer with mismatched source identity parameters = %v", err)
			}
			if _, err := host.FinishWorkspaceTransfer(context.Background(), WorkspaceTransferControlRequest{
				TreeID: testTreeID, Source: source, Params: controlParams,
			}); err == nil || err.Error() != "workspace transfer control authority is invalid" {
				t.Fatalf("FinishWorkspaceTransfer with mismatched source identity parameters = %v", err)
			}
		})
	}
	host.workspaceTransferMu.Lock()
	pendingCount := len(host.pendingWorkspaces)
	outboundCount := len(host.outboundTransfers)
	inboundCount := len(host.inboundTransfers)
	host.workspaceTransferMu.Unlock()
	entries, err := os.ReadDir(host.workspaceRoot.Name())
	if err != nil {
		t.Fatal(err)
	}
	if pendingCount != 0 || outboundCount != 0 || inboundCount != 0 || len(entries) != 0 {
		t.Fatalf(
			"rejected authority changed workspace state: pending %d outbound %d inbound %d files %d",
			pendingCount, outboundCount, inboundCount, len(entries),
		)
	}
}

func TestFinishedWorkspaceTransferRejectsDifferentSource(t *testing.T) {
	host, state, _ := newTestHost(t, 1)
	workspaceID := newTestID()
	workspacePath := filepath.Join(host.workspaceRoot.Name(), workspaceSyncName(testTreeID, workspaceID))
	head := initializeTestRepository(t, workspacePath)
	recordPreparedWorkspace(t, state, workspaceID, workspacePath, head)
	owner := control.NewRootPrincipal(
		testControllerID, testTreeID, testParentID, testParentID,
	).Identity()
	for name, mutate := range map[string]func(*control.PrincipalIdentity){
		"agent":  func(identity *control.PrincipalIdentity) { identity.AgentID = newTestID() },
		"device": func(identity *control.PrincipalIdentity) { identity.DeviceID = newTestID() },
	} {
		t.Run(name, func(t *testing.T) {
			source := owner
			mutate(&source)
			request := WorkspaceTransferControlRequest{
				TreeID: testTreeID, Source: source,
				Params: protocol.WorkspaceTransferControlParams{
					WorkspaceID: workspaceID, TransferID: newTestID(),
					SourceAgentID: source.AgentID, SourceDeviceID: source.DeviceID,
				},
			}
			if _, err := host.FinishWorkspaceTransfer(context.Background(), request); err == nil ||
				!strings.Contains(err.Error(), "not found") {
				t.Fatalf("finished workspace transfer from a different source = %v", err)
			}
		})
	}
}

func TestHostRejectsOutOfSequenceWorkspaceArtifact(t *testing.T) {
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
	if _, err := host.WriteWorkspaceArtifact(context.Background(), WorkspaceWriteArtifactRequest{
		TreeID: testTreeID, Source: source,
		Params: protocol.WriteWorkspaceArtifactParams{
			WorkspaceID: workspaceID, TransferID: transferID,
			Kind: protocol.WorkspaceArtifactBundle, Offset: 1, Data: []byte{1},
		},
	}); err == nil {
		t.Fatal("out-of-sequence artifact write succeeded")
	}
	controlRequest := WorkspaceTransferControlRequest{
		TreeID: testTreeID, Source: source,
		Params: protocol.WorkspaceTransferControlParams{
			WorkspaceID: workspaceID, TransferID: transferID,
			SourceAgentID: source.AgentID, SourceDeviceID: source.DeviceID,
		},
	}
	if _, err := host.CancelWorkspaceTransfer(context.Background(), controlRequest); err != nil {
		t.Fatal(err)
	}
	if _, err := state.GetPreparedWorkspace(context.Background(), store.PreparedWorkspaceKey{
		ControllerID: testControllerID, TreeID: testTreeID, WorkspaceID: workspaceID,
	}); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("canceled transfer prepared workspace: %v", err)
	}
}

func TestHostTransfersDirtyDirectWorkspaceOverlay(t *testing.T) {
	host, state, _ := newTestHost(t, 1)
	gitURL, sourcePath := createHostedTestRepository(t)
	if err := os.WriteFile(filepath.Join(sourcePath, "dirty-untracked.txt"), []byte("dirty\n"), 0o600); err != nil {
		t.Fatal(err)
	}
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
	if inspected.Manifest.Clean {
		t.Fatal("dirty source was reported clean")
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
	if prepared.Outcome != protocol.WorkspacePrepareTransferRequired ||
		prepared.BundleRequired || !prepared.OverlayRequired {
		t.Fatalf("dirty target preparation = %#v", prepared)
	}
	transferID := newTestID()
	created, err := host.CreateWorkspaceTransfer(context.Background(), WorkspaceCreateTransferRequest{
		TreeID: testTreeID, Source: source,
		Params: protocol.CreateWorkspaceTransferParams{
			TransferID: transferID, WorkspaceID: workspaceID,
			GitURL: gitURL, SourcePath: sourcePath, Manifest: inspected.Manifest,
			OverlayRequired: true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.Transfer.Strategy != protocol.WorkspaceStrategyDirect ||
		len(created.Transfer.Artifacts) != 1 ||
		created.Transfer.Artifacts[0].Kind != protocol.WorkspaceArtifactOverlay {
		t.Fatalf("dirty overlay transfer = %#v", created.Transfer)
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
	for _, artifact := range created.Transfer.Artifacts {
		for offset := int64(0); offset < artifact.Size; {
			chunk, err := host.ReadWorkspaceArtifact(context.Background(), WorkspaceReadArtifactRequest{
				TreeID: testTreeID, Source: source,
				Params: protocol.ReadWorkspaceArtifactParams{
					TransferID: transferID, Kind: artifact.Kind, Offset: offset,
					Limit: protocol.WorkspaceArtifactChunkBytes,
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			written, err := host.WriteWorkspaceArtifact(context.Background(), WorkspaceWriteArtifactRequest{
				TreeID: testTreeID, Source: source,
				Params: protocol.WriteWorkspaceArtifactParams{
					WorkspaceID: workspaceID, TransferID: transferID,
					Kind: artifact.Kind, Offset: offset, Data: chunk.Data,
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			offset = written.NextOffset
		}
	}
	controlRequest := WorkspaceTransferControlRequest{
		TreeID: testTreeID, Source: source,
		Params: protocol.WorkspaceTransferControlParams{
			WorkspaceID: workspaceID, TransferID: transferID,
			SourceAgentID: source.AgentID, SourceDeviceID: source.DeviceID,
		},
	}
	finished, err := host.FinishWorkspaceTransfer(context.Background(), controlRequest)
	if err != nil {
		t.Fatal(err)
	}
	if finished.Workspace.Outcome != protocol.WorkspacePrepareReady ||
		finished.Workspace.Strategy != protocol.WorkspaceStrategyDirect {
		t.Fatalf("finished dirty workspace = %#v", finished)
	}
	stored, err := state.GetPreparedWorkspace(context.Background(), store.PreparedWorkspaceKey{
		ControllerID: testControllerID, TreeID: testTreeID, WorkspaceID: workspaceID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if stored.Clean || stored.SourceSnapshotHash != inspected.Manifest.SourceSnapshotHash {
		t.Fatalf("stored dirty workspace = %#v", stored)
	}
	if data, err := os.ReadFile(filepath.Join(stored.WorkspacePath, "dirty-untracked.txt")); err != nil ||
		string(data) != "dirty\n" {
		t.Fatalf("prepared dirty file = %q, %v", data, err)
	}
	if _, err := host.CancelWorkspaceTransfer(context.Background(), controlRequest); err != nil {
		t.Fatal(err)
	}
	host.workspaceTransferMu.Lock()
	pendingCount := len(host.pendingWorkspaces)
	outboundCount := len(host.outboundTransfers)
	inboundCount := len(host.inboundTransfers)
	host.workspaceTransferMu.Unlock()
	if pendingCount != 0 || outboundCount != 0 || inboundCount != 0 {
		t.Fatalf("dirty transfer retained state = pending %d outbound %d inbound %d", pendingCount, outboundCount, inboundCount)
	}
	for _, name := range []string{
		sourceTransferDirectoryName(transferID), targetTransferDirectoryName(transferID),
		workspaceSyncName(testTreeID, workspaceID) + pendingDirectorySuffix,
	} {
		if _, err := host.workspaceRoot.Lstat(name); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("completed dirty transfer path %q still exists: %v", name, err)
		}
	}
}

func TestHostRejectsWorkspaceArtifactDigestMismatchAndCleansTransfer(t *testing.T) {
	host, state, source, workspaceID, transferID, transfer := prepareCleanWorkspaceTransfer(t)
	corrupted := false
	for _, artifact := range transfer.Artifacts {
		for offset := int64(0); offset < artifact.Size; {
			limit := min(int64(protocol.WorkspaceArtifactChunkBytes), artifact.Size-offset)
			chunk, err := host.ReadWorkspaceArtifact(context.Background(), WorkspaceReadArtifactRequest{
				TreeID: testTreeID, Source: source,
				Params: protocol.ReadWorkspaceArtifactParams{
					TransferID: transferID, Kind: artifact.Kind, Offset: offset, Limit: int(limit),
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			if !corrupted {
				chunk.Data[0] ^= 0xff
				corrupted = true
			}
			written, err := host.WriteWorkspaceArtifact(context.Background(), WorkspaceWriteArtifactRequest{
				TreeID: testTreeID, Source: source,
				Params: protocol.WriteWorkspaceArtifactParams{
					WorkspaceID: workspaceID, TransferID: transferID,
					Kind: artifact.Kind, Offset: offset, Data: chunk.Data,
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			offset = written.NextOffset
		}
	}
	controlRequest := WorkspaceTransferControlRequest{
		TreeID: testTreeID, Source: source,
		Params: protocol.WorkspaceTransferControlParams{
			WorkspaceID: workspaceID, TransferID: transferID,
			SourceAgentID: source.AgentID, SourceDeviceID: source.DeviceID,
		},
	}
	if _, err := host.FinishWorkspaceTransfer(context.Background(), controlRequest); err == nil ||
		!strings.Contains(err.Error(), "digest") {
		t.Fatalf("finish corrupted transport = %v, want digest rejection", err)
	}
	if _, err := state.GetPreparedWorkspace(context.Background(), store.PreparedWorkspaceKey{
		ControllerID: testControllerID, TreeID: testTreeID, WorkspaceID: workspaceID,
	}); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("corrupted transport published workspace: %v", err)
	}
	if _, err := host.CancelWorkspaceTransfer(context.Background(), controlRequest); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{
		targetTransferDirectoryName(transferID), workspaceSyncName(testTreeID, workspaceID) + pendingDirectorySuffix,
	} {
		if _, err := host.workspaceRoot.Lstat(name); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("corrupted transfer path %q still exists: %v", name, err)
		}
	}
}

func prepareCleanWorkspaceTransfer(
	t *testing.T,
) (*Host, *store.PeerStore, control.PrincipalIdentity, string, string, protocol.WorkspaceTransferManifest) {
	t.Helper()
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
	return host, state, source, workspaceID, transferID, created.Transfer
}
