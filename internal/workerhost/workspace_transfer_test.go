package workerhost

import (
	"context"
	"crypto/rand"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
)

func TestHostTransfersCleanBundleWorkspace(t *testing.T) {
	for _, test := range []struct {
		name        string
		full        bool
		strategy    protocol.WorkspaceStrategy
		wantWarning bool
	}{
		{name: "thin", strategy: protocol.WorkspaceStrategyThin},
		{
			name: "self contained", full: true, strategy: protocol.WorkspaceStrategyFull,
			wantWarning: true,
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
				!prepared.BundleRequired || prepared.OverlayRequired ||
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
					BundleRequired: true,
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			if created.Transfer.Strategy != test.strategy ||
				slices.Contains(created.Transfer.Warnings, protocol.WorkspaceWarningFullHistoryFallback) != test.wantWarning {
				t.Fatalf("source transfer = %#v", created.Transfer)
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
