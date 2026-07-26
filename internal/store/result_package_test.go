package store

import (
	"context"
	"crypto/sha256"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/protocol"
)

const (
	resultPackageOne  = "123e4567-e89b-42d3-a456-426614175130"
	resultPackageTwo  = "123e4567-e89b-42d3-a456-426614175131"
	resultTurnTwo     = "123e4567-e89b-42d3-a456-426614175132"
	resultThreadTwo   = "123e4567-e89b-42d3-a456-426614175133"
	resultRelayParent = "123e4567-e89b-42d3-a456-426614175134"
)

type resultPackageFixture struct {
	Registry *Store
	Root     control.Principal
	Worker   control.PrincipalIdentity
	Session  WorkerLifecycleSession
	Manifest protocol.ResultManifest
	Metadata protocol.ResultPackageMetadata
}

func TestResultPackagePublishIsMetadataOnlyUnsequencedAndReplayable(t *testing.T) {
	fixture := prepareResultPackageFixture(t, false, true)
	ctx := context.Background()
	params := protocol.PublishResultPackageParams{Metadata: fixture.Metadata}
	created, err := fixture.Registry.PublishResultPackage(
		ctx, fixture.Worker.DeviceID, fixture.Worker, params, time.Unix(40, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	wantResult := protocol.PublishResultPackageResult{PackageID: resultPackageOne}
	if created != wantResult {
		t.Fatalf("publish result = %#v, want %#v", created, wantResult)
	}

	applyLifecyclePage(t, fixture.Registry, fixture.Session, 1, 2, protocol.WorkerLifecycleSnapshot{
		TreeID: fixture.Worker.TreeID, AgentID: fixture.Worker.AgentID, Revision: 2,
		Phase: protocol.WorkerLifecycleIdle, CodexThreadID: lifecycleCodexThreadOne,
	})
	replayed, err := fixture.Registry.PublishResultPackage(
		ctx, fixture.Worker.DeviceID, fixture.Worker, params, time.Unix(50, 0),
	)
	if err != nil || replayed != created {
		t.Fatalf("publish replay = %#v, error %v", replayed, err)
	}
	record, err := fixture.Registry.GetResultPackageForDelivery(
		ctx, fixture.Worker.DeviceID, fixture.Worker, resultPackageOne,
	)
	if err != nil {
		t.Fatal(err)
	}
	if record.State != ResultPackageDeliveryPending || record.Sequence != 0 ||
		record.RootDeviceID != fixture.Root.DeviceID || record.PublishedAt != 40 ||
		record.SourceAcknowledgedAt != 0 ||
		record.SourcePrincipal != fixture.Worker || record.RootPrincipal != fixture.Root.Identity() ||
		!protocol.SameResultPackageMetadata(record.Metadata, fixture.Metadata) ||
		!reflect.DeepEqual(record.Manifest, fixture.Manifest) {
		t.Fatalf("stored result package = %#v", record)
	}
	page, err := fixture.Registry.ListDeliveredResultPackages(
		ctx, fixture.Root.Identity(), ResultPackagePageRequest{Limit: 1},
	)
	if err != nil || len(page.Packages) != 0 || page.Highwater != 0 {
		t.Fatalf("pending result package page = %#v, error %v", page, err)
	}
	var count, sequence int
	if err := fixture.Registry.db.QueryRowContext(ctx, `SELECT count(*) FROM result_packages`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if err := fixture.Registry.db.QueryRowContext(ctx, `SELECT last_result_sequence FROM trees`).Scan(&sequence); err != nil {
		t.Fatal(err)
	}
	if count != 1 || sequence != 0 {
		t.Fatalf("published metadata count=%d, result sequence=%d", count, sequence)
	}
	assertResultPackageTableHasNoPayloadColumns(t, fixture.Registry)
}

func TestPendingResultPackageRelaysAreBoundedAndPeerScoped(t *testing.T) {
	fixture := prepareResultPackageFixture(t, false, true)
	ctx := context.Background()
	publishResultManifest(t, fixture, fixture.Manifest, 40)
	for name, deviceID := range map[string]string{
		"source": fixture.Worker.DeviceID,
		"root":   fixture.Root.DeviceID,
	} {
		t.Run(name, func(t *testing.T) {
			page, err := fixture.Registry.ListPendingResultPackageRelaysForPeer(
				ctx, fixture.Worker.ControllerID, deviceID, ResultPackageRelayPageRequest{Limit: 1},
			)
			if err != nil || len(page.Packages) != 1 {
				t.Fatalf("pending relays = %#v, error %v", page, err)
			}
			if page.Packages[0].SourcePrincipal != fixture.Worker ||
				page.Packages[0].RootPrincipal != fixture.Root.Identity() ||
				page.Packages[0].Manifest.PackageID != resultPackageOne {
				t.Fatalf("pending relay authority = %#v", page.Packages[0])
			}
		})
	}
	firstPage, err := fixture.Registry.ListPendingResultPackageRelaysForPeer(
		ctx,
		fixture.Worker.ControllerID,
		fixture.Worker.DeviceID,
		ResultPackageRelayPageRequest{Limit: 1},
	)
	if err != nil || firstPage.NextAfter == nil {
		t.Fatalf("exact-full pending relay page = %#v, error %v", firstPage, err)
	}
	tailPage, err := fixture.Registry.ListPendingResultPackageRelaysForPeer(
		ctx,
		fixture.Worker.ControllerID,
		fixture.Worker.DeviceID,
		ResultPackageRelayPageRequest{After: firstPage.NextAfter, Limit: 1},
	)
	if err != nil || len(tailPage.Packages) != 0 || tailPage.NextAfter != nil {
		t.Fatalf("pending relay page after exact limit = %#v, error %v", tailPage, err)
	}
	otherDeviceID := "123e4567-e89b-42d3-a456-426614175134"
	page, err := fixture.Registry.ListPendingResultPackageRelaysForPeer(
		ctx, fixture.Worker.ControllerID, otherDeviceID, ResultPackageRelayPageRequest{Limit: 1},
	)
	if err != nil || len(page.Packages) != 0 {
		t.Fatalf("unrelated peer relays = %#v, error %v", page, err)
	}
	if _, err := fixture.Registry.ListPendingResultPackageRelaysForPeer(
		ctx,
		fixture.Worker.ControllerID,
		fixture.Worker.DeviceID,
		ResultPackageRelayPageRequest{Limit: maximumPendingResultRelays + 1},
	); err == nil {
		t.Fatal("pending relay lookup accepted an unbounded limit")
	}

	if _, err := fixture.Registry.MarkResultPackageDelivered(
		ctx, fixture.Root.DeviceID, fixture.Root.Identity(), resultPackageOne, time.Unix(50, 0),
	); err != nil {
		t.Fatal(err)
	}
	page, err = fixture.Registry.ListPendingResultPackageRelaysForPeer(
		ctx,
		fixture.Worker.ControllerID,
		fixture.Worker.DeviceID,
		ResultPackageRelayPageRequest{Limit: 1},
	)
	if err != nil || len(page.Packages) != 1 || page.Packages[0].State != ResultPackageDelivered {
		t.Fatalf("delivered source acknowledgement = %#v, error %v", page, err)
	}
	page, err = fixture.Registry.ListPendingResultPackageRelaysForPeer(
		ctx,
		fixture.Worker.ControllerID,
		fixture.Root.DeviceID,
		ResultPackageRelayPageRequest{Limit: 1},
	)
	if err != nil || len(page.Packages) != 0 {
		t.Fatalf("delivered root relays = %#v, error %v", page, err)
	}
}

func TestResultPackageSourceAcknowledgementAndReleaseAreAuthorizedDurableAndIdempotent(t *testing.T) {
	fixture := prepareResultPackageFixture(t, false, true)
	ctx := context.Background()
	publishResultManifest(t, fixture, fixture.Manifest, 40)
	if _, err := fixture.Registry.MarkResultPackageSourceAcknowledged(
		ctx,
		fixture.Worker.DeviceID,
		fixture.Worker,
		resultPackageOne,
		time.Unix(50, 0),
	); !errors.Is(err, ErrConflict) {
		t.Fatalf("pending source acknowledgement error = %v, want ErrConflict", err)
	}
	if _, err := fixture.Registry.MarkResultPackageSourceReleased(
		ctx,
		fixture.Worker.DeviceID,
		fixture.Worker,
		resultPackageOne,
		time.Unix(50, 0),
	); !errors.Is(err, ErrConflict) {
		t.Fatalf("pending source release error = %v, want ErrConflict", err)
	}
	if _, err := fixture.Registry.MarkResultPackageDelivered(
		ctx, fixture.Root.DeviceID, fixture.Root.Identity(), resultPackageOne, time.Unix(50, 0),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.Registry.MarkResultPackageSourceAcknowledged(
		ctx,
		fixture.Root.DeviceID,
		fixture.Root.Identity(),
		resultPackageOne,
		time.Unix(60, 0),
	); !errors.Is(err, ErrAuthorizationDenied) {
		t.Fatalf("root source acknowledgement error = %v, want authorization denied", err)
	}
	acknowledged, err := fixture.Registry.MarkResultPackageSourceAcknowledged(
		ctx,
		fixture.Worker.DeviceID,
		fixture.Worker,
		resultPackageOne,
		time.Unix(45, 0),
	)
	if err != nil || acknowledged.SourceAcknowledgedAt != 50 {
		t.Fatalf("source acknowledgement = %#v, error %v", acknowledged, err)
	}
	replayed, err := fixture.Registry.MarkResultPackageSourceAcknowledged(
		ctx,
		fixture.Worker.DeviceID,
		fixture.Worker,
		resultPackageOne,
		time.Unix(70, 0),
	)
	if err != nil || replayed.SourceAcknowledgedAt != 50 {
		t.Fatalf("source acknowledgement replay = %#v, error %v", replayed, err)
	}
	page, err := fixture.Registry.ListPendingResultPackageRelaysForPeer(
		ctx,
		fixture.Worker.ControllerID,
		fixture.Worker.DeviceID,
		ResultPackageRelayPageRequest{Limit: 1},
	)
	if err != nil || len(page.Packages) != 1 || page.NextAfter == nil ||
		page.Packages[0].SourceAcknowledgedAt != 50 || page.Packages[0].SourceReleasedAt != 0 {
		t.Fatalf("acknowledged but unreleased source reconnect page = %#v, error %v", page, err)
	}
	if _, err := fixture.Registry.MarkResultPackageSourceReleased(
		ctx,
		fixture.Root.DeviceID,
		fixture.Root.Identity(),
		resultPackageOne,
		time.Unix(60, 0),
	); !errors.Is(err, ErrAuthorizationDenied) {
		t.Fatalf("root source release error = %v, want authorization denied", err)
	}
	released, err := fixture.Registry.MarkResultPackageSourceReleased(
		ctx,
		fixture.Worker.DeviceID,
		fixture.Worker,
		resultPackageOne,
		time.Unix(45, 0),
	)
	if err != nil || released.SourceReleasedAt != 50 {
		t.Fatalf("source release = %#v, error %v", released, err)
	}
	replayedRelease, err := fixture.Registry.MarkResultPackageSourceReleased(
		ctx,
		fixture.Worker.DeviceID,
		fixture.Worker,
		resultPackageOne,
		time.Unix(80, 0),
	)
	if err != nil || replayedRelease.SourceReleasedAt != 50 {
		t.Fatalf("source release replay = %#v, error %v", replayedRelease, err)
	}
	page, err = fixture.Registry.ListPendingResultPackageRelaysForPeer(
		ctx,
		fixture.Worker.ControllerID,
		fixture.Worker.DeviceID,
		ResultPackageRelayPageRequest{Limit: 1},
	)
	if err != nil || len(page.Packages) != 0 || page.NextAfter != nil {
		t.Fatalf("released source reconnect page = %#v, error %v", page, err)
	}
	rootPage, err := fixture.Registry.ListDeliveredResultPackages(
		ctx, fixture.Root.Identity(), ResultPackagePageRequest{Limit: 1},
	)
	if err != nil || len(rootPage.Packages) != 1 ||
		rootPage.Packages[0].SourceAcknowledgedAt != 50 ||
		rootPage.Packages[0].SourceReleasedAt != 50 {
		t.Fatalf("root delivered package after source release = %#v, error %v", rootPage, err)
	}
}

func TestPendingResultPackageRelayDerivesTreeRootInsteadOfSourceParent(t *testing.T) {
	fixture := prepareResultPackageFixture(t, false, true)
	ctx := context.Background()
	publishResultManifest(t, fixture, fixture.Manifest, 40)
	if _, err := fixture.Registry.CreateWorkerPrincipal(
		ctx,
		fixture.Worker.ControllerID,
		fixture.Worker.TreeID,
		resultRelayParent,
		fixture.Root.AgentID,
		fixture.Worker.DeviceID,
		time.Unix(41, 0),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.Registry.db.ExecContext(ctx, `
UPDATE principals
SET parent_agent_id = ?
WHERE controller_id = ? AND tree_id = ? AND agent_id = ?
`, resultRelayParent, fixture.Worker.ControllerID, fixture.Worker.TreeID, fixture.Worker.AgentID); err != nil {
		t.Fatal(err)
	}
	page, err := fixture.Registry.ListPendingResultPackageRelaysForPeer(
		ctx,
		fixture.Worker.ControllerID,
		fixture.Worker.DeviceID,
		ResultPackageRelayPageRequest{Limit: 1},
	)
	if err != nil || len(page.Packages) != 1 {
		t.Fatalf("pending relays = %#v, error %v", page, err)
	}
	if page.Packages[0].SourcePrincipal.ParentAgentID != resultRelayParent ||
		page.Packages[0].RootPrincipal != fixture.Root.Identity() {
		t.Fatalf("derived relay principals = %#v", page.Packages[0])
	}
}

func TestResultPackagePublishConflictsAreAtomicByPackageAndSourceTurn(t *testing.T) {
	fixture := prepareResultPackageFixture(t, false, true)
	ctx := context.Background()
	params := protocol.PublishResultPackageParams{Metadata: fixture.Metadata}
	if _, err := fixture.Registry.PublishResultPackage(
		ctx, fixture.Worker.DeviceID, fixture.Worker, params, time.Unix(40, 0),
	); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(*protocol.ResultManifest)
	}{
		{name: "same package changed turn", mutate: func(value *protocol.ResultManifest) {
			value.TurnID = resultTurnTwo
		}},
		{name: "same source turn changed package", mutate: func(value *protocol.ResultManifest) {
			value.PackageID = resultPackageTwo
		}},
		{name: "same identity changed metadata", mutate: func(value *protocol.ResultManifest) {
			value.CapturedAt++
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			changed := fixture.Manifest
			test.mutate(&changed)
			metadata := encodeResultMetadata(t, changed)
			_, err := fixture.Registry.PublishResultPackage(
				ctx, fixture.Worker.DeviceID, fixture.Worker,
				protocol.PublishResultPackageParams{Metadata: metadata}, time.Unix(41, 0),
			)
			if !errors.Is(err, ErrConflict) {
				t.Fatalf("conflicting publish error = %v, want ErrConflict", err)
			}
		})
	}
	var count, sequence int
	if err := fixture.Registry.db.QueryRowContext(ctx, `SELECT count(*) FROM result_packages`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if err := fixture.Registry.db.QueryRowContext(ctx, `SELECT last_result_sequence FROM trees`).Scan(&sequence); err != nil {
		t.Fatal(err)
	}
	if count != 1 || sequence != 0 {
		t.Fatalf("conflicts changed metadata: count=%d sequence=%d", count, sequence)
	}
}

func TestResultPackagePublishBindsIntroducedLifecycleAuthorityRevision(t *testing.T) {
	t.Run("peer cursor behind is retryable and creates no package", func(t *testing.T) {
		fixture := prepareResultPackageFixture(t, false, true)
		applyLifecyclePage(t, fixture.Registry, fixture.Session, 1, 2, protocol.WorkerLifecycleSnapshot{
			TreeID: fixture.Worker.TreeID, AgentID: fixture.Worker.AgentID, Revision: 2,
			Phase: protocol.WorkerLifecycleIdle, CodexThreadID: lifecycleCodexThreadOne,
		})
		future := fixture.Manifest
		future.LifecycleRevision = 3
		_, err := fixture.Registry.PublishResultPackage(
			context.Background(), fixture.Worker.DeviceID, fixture.Worker,
			protocol.PublishResultPackageParams{Metadata: encodeResultMetadata(t, future)}, time.Unix(40, 0),
		)
		if !errors.Is(err, ErrResultPackageLifecycleNotReady) {
			t.Fatalf("early lifecycle publish error = %v, want lifecycle_not_ready", err)
		}
		assertResultPackageCount(t, fixture.Registry, 0)
		applyLifecyclePage(t, fixture.Registry, fixture.Session, 2, 3, protocol.WorkerLifecycleSnapshot{
			TreeID: fixture.Worker.TreeID, AgentID: fixture.Worker.AgentID, Revision: 3,
			Phase: protocol.WorkerLifecycleFinalizing, CodexThreadID: lifecycleCodexThreadOne,
			ActiveTurnID: lifecycleTurnOne,
		})
		if _, err := fixture.Registry.PublishResultPackage(
			context.Background(), fixture.Worker.DeviceID, fixture.Worker,
			protocol.PublishResultPackageParams{Metadata: encodeResultMetadata(t, future)}, time.Unix(41, 0),
		); err != nil {
			t.Fatalf("publish after lifecycle sync: %v", err)
		}
	})

	t.Run("same authority refresh preserves introduced revision", func(t *testing.T) {
		fixture := prepareResultPackageFixture(t, false, true)
		refresh := protocol.WorkerLifecycleSnapshot{
			TreeID: fixture.Worker.TreeID, AgentID: fixture.Worker.AgentID, Revision: 2,
			Phase: protocol.WorkerLifecycleFinalizing, CodexThreadID: lifecycleCodexThreadOne,
			ActiveTurnID: lifecycleTurnOne,
		}
		applyLifecyclePage(t, fixture.Registry, fixture.Session, 1, 2, refresh)
		authority, err := queryWorkerLifecycleAuthority(
			context.Background(), fixture.Registry.db, fixture.Worker.ControllerID,
			fixture.Worker.TreeID, fixture.Worker.AgentID,
		)
		if err != nil || authority.Snapshot.Revision != 2 || authority.AuthorityRevision != 1 {
			t.Fatalf("refreshed lifecycle authority = %#v, error %v", authority, err)
		}
		if _, err := fixture.Registry.PublishResultPackage(
			context.Background(), fixture.Worker.DeviceID, fixture.Worker,
			protocol.PublishResultPackageParams{Metadata: fixture.Metadata}, time.Unix(40, 0),
		); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("identity change fences old manifest and accepts new authority", func(t *testing.T) {
		fixture := prepareResultPackageFixture(t, false, true)
		changed := protocol.WorkerLifecycleSnapshot{
			TreeID: fixture.Worker.TreeID, AgentID: fixture.Worker.AgentID, Revision: 2,
			Phase: protocol.WorkerLifecycleFinalizing, CodexThreadID: resultThreadTwo,
			ActiveTurnID: resultTurnTwo,
		}
		applyLifecyclePage(t, fixture.Registry, fixture.Session, 1, 2, changed)
		_, err := fixture.Registry.PublishResultPackage(
			context.Background(), fixture.Worker.DeviceID, fixture.Worker,
			protocol.PublishResultPackageParams{Metadata: fixture.Metadata}, time.Unix(40, 0),
		)
		if !errors.Is(err, ErrConflict) {
			t.Fatalf("stale authority publish error = %v, want ErrConflict", err)
		}
		current := fixture.Manifest
		current.PackageID = resultPackageTwo
		current.ManagedThreadID = resultThreadTwo
		current.TurnID = resultTurnTwo
		current.LifecycleRevision = 2
		if _, err := fixture.Registry.PublishResultPackage(
			context.Background(), fixture.Worker.DeviceID, fixture.Worker,
			protocol.PublishResultPackageParams{Metadata: encodeResultMetadata(t, current)}, time.Unix(41, 0),
		); err != nil {
			t.Fatal(err)
		}
	})
}

func TestResultPackagePublishEnforcesConnectionSpawnAndWorkspaceAuthority(t *testing.T) {
	t.Run("connection and principal", func(t *testing.T) {
		fixture := prepareResultPackageFixture(t, false, true)
		for name, invoke := range map[string]func() error{
			"wrong connection": func() error {
				_, err := fixture.Registry.PublishResultPackage(
					context.Background(), fixture.Root.DeviceID, fixture.Worker,
					protocol.PublishResultPackageParams{Metadata: fixture.Metadata}, time.Unix(40, 0),
				)
				return err
			},
			"root principal": func() error {
				_, err := fixture.Registry.PublishResultPackage(
					context.Background(), fixture.Root.DeviceID, fixture.Root.Identity(),
					protocol.PublishResultPackageParams{Metadata: fixture.Metadata}, time.Unix(40, 0),
				)
				return err
			},
		} {
			t.Run(name, func(t *testing.T) {
				if err := invoke(); !errors.Is(err, ErrAuthorizationDenied) {
					t.Fatalf("publish error = %v, want authorization denial", err)
				}
			})
		}
		assertResultPackageCount(t, fixture.Registry, 0)
	})

	t.Run("unstarted spawn", func(t *testing.T) {
		fixture := prepareResultPackageFixture(t, false, false)
		_, err := fixture.Registry.PublishResultPackage(
			context.Background(), fixture.Worker.DeviceID, fixture.Worker,
			protocol.PublishResultPackageParams{Metadata: fixture.Metadata}, time.Unix(40, 0),
		)
		if !errors.Is(err, ErrAuthorizationDenied) {
			t.Fatalf("unstarted spawn publish error = %v", err)
		}
	})

	t.Run("workspace base and consumption", func(t *testing.T) {
		mutations := []struct {
			name      string
			mutate    func(*protocol.ResultManifest)
			wantError error
		}{
			{name: "wrong workspace", mutate: func(value *protocol.ResultManifest) {
				value.Workspace.WorkspaceID = workspaceSyncID
			}, wantError: ErrAuthorizationDenied},
			{name: "wrong source peer", mutate: func(value *protocol.ResultManifest) {
				value.Workspace.SourceDeviceID = deviceSecondID
			}, wantError: ErrAuthorizationDenied},
			{name: "changed base head", mutate: func(value *protocol.ResultManifest) {
				value.Workspace.BaseHeadOID = strings.Repeat("9", len(value.Workspace.BaseHeadOID))
				value.Workspace.ResultHeadOID = value.Workspace.BaseHeadOID
			}, wantError: ErrConflict},
			{name: "changed object format", mutate: func(value *protocol.ResultManifest) {
				value.Workspace.ObjectFormat = "sha256"
				value.Workspace.BaseHeadOID = strings.Repeat("9", 64)
				value.Workspace.ResultHeadOID = value.Workspace.BaseHeadOID
			}, wantError: ErrConflict},
			{name: "changed base manifest", mutate: func(value *protocol.ResultManifest) {
				value.Workspace.BaseManifestHash = strings.Repeat("9", 64)
			}, wantError: ErrConflict},
			{name: "changed base snapshot", mutate: func(value *protocol.ResultManifest) {
				value.Workspace.BaseSnapshotHash = strings.Repeat("9", 64)
				value.Workspace.ResultSnapshotHash = value.Workspace.BaseSnapshotHash
			}, wantError: ErrConflict},
			{name: "changed base cleanliness", mutate: func(value *protocol.ResultManifest) {
				value.Workspace.BaseClean = false
				value.Workspace.ResultClean = false
			}, wantError: ErrConflict},
			{name: "changed base warnings", mutate: func(value *protocol.ResultManifest) {
				value.Workspace.BaseWarnings = []string{"submodule_repository_not_transferred"}
			}, wantError: ErrConflict},
		}
		for _, test := range mutations {
			t.Run(test.name, func(t *testing.T) {
				fixture := prepareResultPackageFixture(t, true, true)
				changed := fixture.Manifest
				test.mutate(&changed)
				_, err := fixture.Registry.PublishResultPackage(
					context.Background(), fixture.Worker.DeviceID, fixture.Worker,
					protocol.PublishResultPackageParams{Metadata: encodeResultMetadata(t, changed)},
					time.Unix(40, 0),
				)
				if !errors.Is(err, test.wantError) {
					t.Fatalf("workspace authority error = %v, want %v", err, test.wantError)
				}
				assertResultPackageCount(t, fixture.Registry, 0)
			})
		}
	})

	t.Run("workspace mode cannot be substituted", func(t *testing.T) {
		managed := prepareResultPackageFixture(t, true, true)
		changed := managed.Manifest
		changed.Workspace = unmanagedResultWorkspace()
		_, err := managed.Registry.PublishResultPackage(
			context.Background(), managed.Worker.DeviceID, managed.Worker,
			protocol.PublishResultPackageParams{Metadata: encodeResultMetadata(t, changed)}, time.Unix(40, 0),
		)
		if !errors.Is(err, ErrAuthorizationDenied) {
			t.Fatalf("managed as unmanaged error = %v", err)
		}

		unmanaged := prepareResultPackageFixture(t, false, true)
		changed = unmanaged.Manifest
		changed.Workspace = managed.Manifest.Workspace
		_, err = unmanaged.Registry.PublishResultPackage(
			context.Background(), unmanaged.Worker.DeviceID, unmanaged.Worker,
			protocol.PublishResultPackageParams{Metadata: encodeResultMetadata(t, changed)}, time.Unix(40, 0),
		)
		if !errors.Is(err, ErrAuthorizationDenied) {
			t.Fatalf("unmanaged as managed error = %v", err)
		}
	})
}

func TestResultPackageDeliveryAllocatesAvailabilityOrderAndIsRootScoped(t *testing.T) {
	fixture := prepareResultPackageFixture(t, false, true)
	ctx := context.Background()
	publishResultManifest(t, fixture, fixture.Manifest, 40)

	idle := protocol.WorkerLifecycleSnapshot{
		TreeID: fixture.Worker.TreeID, AgentID: fixture.Worker.AgentID, Revision: 2,
		Phase: protocol.WorkerLifecycleIdle, CodexThreadID: lifecycleCodexThreadOne,
	}
	applyLifecyclePage(t, fixture.Registry, fixture.Session, 1, 2, idle)
	secondAuthority := protocol.WorkerLifecycleSnapshot{
		TreeID: fixture.Worker.TreeID, AgentID: fixture.Worker.AgentID, Revision: 3,
		Phase: protocol.WorkerLifecycleFinalizing, CodexThreadID: lifecycleCodexThreadOne,
		ActiveTurnID: resultTurnTwo,
	}
	applyLifecyclePage(t, fixture.Registry, fixture.Session, 2, 3, secondAuthority)
	second := fixture.Manifest
	second.PackageID = resultPackageTwo
	second.TurnID = resultTurnTwo
	second.LifecycleRevision = 3
	publishResultManifest(t, fixture, second, 41)

	secondDelivered, err := fixture.Registry.MarkResultPackageDelivered(
		ctx, fixture.Root.DeviceID, fixture.Root.Identity(), resultPackageTwo, time.Unix(50, 0),
	)
	if err != nil || secondDelivered.Sequence != 1 || secondDelivered.State != ResultPackageDelivered {
		t.Fatalf("second package delivery = %#v, error %v", secondDelivered, err)
	}
	firstDelivered, err := fixture.Registry.MarkResultPackageDelivered(
		ctx, fixture.Root.DeviceID, fixture.Root.Identity(), resultPackageOne, time.Unix(51, 0),
	)
	if err != nil || firstDelivered.Sequence != 2 {
		t.Fatalf("first package delivery = %#v, error %v", firstDelivered, err)
	}
	replay, err := fixture.Registry.MarkResultPackageDelivered(
		ctx, fixture.Root.DeviceID, fixture.Root.Identity(), resultPackageOne, time.Unix(80, 0),
	)
	if err != nil || replay.Sequence != 2 || replay.DeliveredAt != 51 {
		t.Fatalf("delivery replay = %#v, error %v", replay, err)
	}

	page, err := fixture.Registry.ListDeliveredResultPackages(
		ctx, fixture.Root.Identity(), ResultPackagePageRequest{Limit: 2},
	)
	if err != nil || page.Highwater != 2 || page.NextSequence != 2 ||
		len(page.Packages) != 2 || page.Packages[0].Manifest.PackageID != resultPackageTwo ||
		page.Packages[1].Manifest.PackageID != resultPackageOne {
		t.Fatalf("delivered package page = %#v, error %v", page, err)
	}

	_, otherRoot, err := fixture.Registry.EnsureRootTree(
		ctx, testControllerID, treeSecondThreadID, fixture.Root.DeviceID, time.Unix(60, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	otherPage, err := fixture.Registry.ListDeliveredResultPackages(
		ctx, otherRoot.Identity(), ResultPackagePageRequest{Limit: 1},
	)
	if err != nil || len(otherPage.Packages) != 0 || otherPage.Highwater != 0 {
		t.Fatalf("other tree result page = %#v, error %v", otherPage, err)
	}
	if _, err := fixture.Registry.MarkResultPackageDelivered(
		ctx, otherRoot.DeviceID, otherRoot.Identity(), resultPackageOne, time.Unix(90, 0),
	); !errors.Is(err, ErrNotFound) {
		t.Fatalf("other tree delivery error = %v, want ErrNotFound", err)
	}
	if _, err := fixture.Registry.MarkResultPackageDelivered(
		ctx, fixture.Worker.DeviceID, fixture.Root.Identity(), resultPackageOne, time.Unix(90, 0),
	); !errors.Is(err, ErrAuthorizationDenied) {
		t.Fatalf("wrong root connection error = %v", err)
	}
	if _, err := fixture.Registry.ListDeliveredResultPackages(
		ctx, fixture.Worker, ResultPackagePageRequest{Limit: 1},
	); !errors.Is(err, ErrAuthorizationDenied) {
		t.Fatalf("worker list error = %v", err)
	}
	if _, err := fixture.Registry.ListDeliveredResultPackages(
		ctx, fixture.Root.Identity(), ResultPackagePageRequest{AfterSequence: 3, Limit: 1},
	); !errors.Is(err, ErrResultPackageCursorAhead) {
		t.Fatalf("ahead result cursor error = %v", err)
	}
}

func TestResultPackageDeliveryLookupIsSourceAndTreeScoped(t *testing.T) {
	fixture := prepareResultPackageFixture(t, false, true)
	publishResultManifest(t, fixture, fixture.Manifest, 40)
	ctx := context.Background()
	if _, err := fixture.Registry.GetResultPackageForDelivery(
		ctx, fixture.Root.DeviceID, fixture.Worker, resultPackageOne,
	); !errors.Is(err, ErrAuthorizationDenied) {
		t.Fatalf("wrong source connection lookup error = %v", err)
	}
	if _, err := fixture.Registry.GetResultPackageForDelivery(
		ctx, fixture.Root.DeviceID, fixture.Root.Identity(), resultPackageOne,
	); !errors.Is(err, ErrAuthorizationDenied) {
		t.Fatalf("root source lookup error = %v", err)
	}
	other := fixture.Worker
	other.TreeID = treeSecondThreadID
	if _, err := fixture.Registry.GetResultPackageForDelivery(
		ctx, fixture.Worker.DeviceID, other, resultPackageOne,
	); !errors.Is(err, ErrAuthorizationDenied) {
		t.Fatalf("cross-tree source lookup error = %v", err)
	}
}

func prepareResultPackageFixture(
	t *testing.T,
	workspace, started bool,
) resultPackageFixture {
	t.Helper()
	var (
		registry     *Store
		root         control.Principal
		worker       control.PrincipalIdentity
		workspaceOut protocol.ResultWorkspaceComponent
	)
	if workspace {
		var manifest protocol.WorkspaceManifest
		var manifestHash string
		registry, root, worker, manifest, manifestHash = prepareChangesArtifactStore(t, true, started)
		workspaceOut = protocol.ResultWorkspaceComponent{
			Status: protocol.ResultWorkspaceUnchanged, WorkspaceID: changesWorkspaceID,
			SourceDeviceID: root.DeviceID, TargetDeviceID: worker.DeviceID,
			ObjectFormat: manifest.ObjectFormat, BaseHeadOID: manifest.HeadOID,
			BaseManifestHash: manifestHash, BaseSnapshotHash: manifest.SourceSnapshotHash,
			BaseClean: manifest.Clean, ResultHeadOID: manifest.HeadOID,
			ResultSnapshotHash: manifest.SourceSnapshotHash, ResultClean: manifest.Clean,
			BaseWarnings: append([]string{}, manifest.Warnings...), ResultWarnings: []string{},
		}
	} else {
		registry, root = prepareAgentSpawnStore(t)
		spawn, err := registry.BeginAgentSpawn(context.Background(), AgentSpawnIntent{
			Source: root.Identity(), SpawnID: agentSpawnID, AgentID: agentSpawnAgentID,
			TargetDeviceID: agentSpawnTargetID, TaskName: "result_worker",
			PromptDigest: sha256.Sum256([]byte("return result package")),
		}, time.Unix(6, 0))
		if err != nil {
			t.Fatal(err)
		}
		if started {
			if _, err := registry.MarkAgentSpawnStarted(
				context.Background(), keyForReceipt(spawn), time.Unix(7, 0),
			); err != nil {
				t.Fatal(err)
			}
		}
		worker = spawn.Agent.Principal
		workspaceOut = unmanagedResultWorkspace()
	}

	session := lifecycleSession(t, registry, lifecycleConnectionOne)
	claimLifecycleSession(t, registry, session, 0)
	applyLifecyclePage(t, registry, session, 0, 1, protocol.WorkerLifecycleSnapshot{
		TreeID: worker.TreeID, AgentID: worker.AgentID, Revision: 1,
		Phase: protocol.WorkerLifecycleFinalizing, CodexThreadID: lifecycleCodexThreadOne,
		ActiveTurnID: lifecycleTurnOne,
	})
	manifest := protocol.ResultManifest{
		Version: protocol.ResultManifestVersion, PackageID: resultPackageOne,
		ControllerID: worker.ControllerID, TreeID: worker.TreeID,
		SourceAgentID: worker.AgentID, SourceDeviceID: worker.DeviceID,
		ManagedThreadID: lifecycleCodexThreadOne, TurnID: lifecycleTurnOne,
		LifecycleRevision: 1,
		Terminal:          protocol.ResultTerminal{Outcome: protocol.ResultTerminalCompleted},
		CapturedAt:        30,
		Rollout: protocol.ResultRolloutComponent{
			Status: protocol.ResultRolloutCaptureFailed, FailureCode: "rollout_unavailable",
		},
		Workspace: workspaceOut,
		Parts:     []protocol.ResultPackagePartDescriptor{},
	}
	return resultPackageFixture{
		Registry: registry, Root: root, Worker: worker, Session: session,
		Manifest: manifest, Metadata: encodeResultMetadata(t, manifest),
	}
}

func unmanagedResultWorkspace() protocol.ResultWorkspaceComponent {
	return protocol.ResultWorkspaceComponent{
		Status:       protocol.ResultWorkspaceNotManaged,
		BaseWarnings: []string{}, ResultWarnings: []string{},
	}
}

func encodeResultMetadata(t *testing.T, manifest protocol.ResultManifest) protocol.ResultPackageMetadata {
	t.Helper()
	data, descriptor, err := protocol.EncodeResultManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	return protocol.ResultPackageMetadata{Manifest: data, ManifestDescriptor: descriptor}
}

func publishResultManifest(
	t *testing.T,
	fixture resultPackageFixture,
	manifest protocol.ResultManifest,
	publishedAt int64,
) {
	t.Helper()
	if _, err := fixture.Registry.PublishResultPackage(
		context.Background(), fixture.Worker.DeviceID, fixture.Worker,
		protocol.PublishResultPackageParams{Metadata: encodeResultMetadata(t, manifest)},
		time.Unix(publishedAt, 0),
	); err != nil {
		t.Fatal(err)
	}
}

func assertResultPackageCount(t *testing.T, registry *Store, want int) {
	t.Helper()
	var count int
	if err := registry.db.QueryRowContext(
		context.Background(), `SELECT count(*) FROM result_packages`,
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != want {
		t.Fatalf("result package count = %d, want %d", count, want)
	}
}

func assertResultPackageTableHasNoPayloadColumns(t *testing.T, registry *Store) {
	t.Helper()
	rows, err := registry.db.QueryContext(
		context.Background(), `SELECT name FROM pragma_table_info('result_packages')`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	forbidden := []string{"payload", "chunk", "rollout", "bundle", "overlay", "path"}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		lower := strings.ToLower(name)
		if slices.ContainsFunc(forbidden, func(part string) bool { return strings.Contains(lower, part) }) {
			t.Fatalf("broker result package table contains payload-bearing column %q", name)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}
