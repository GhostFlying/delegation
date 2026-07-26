package resultpackagefiles

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/rolloutcapture"
	"github.com/GhostFlying/delegation/internal/store"
)

const (
	testControllerID = "00000000-0000-4000-8000-000000000001"
	testTreeID       = "00000000-0000-4000-8000-000000000002"
	testRootAgentID  = "00000000-0000-4000-8000-000000000003"
	testDeviceID     = "00000000-0000-4000-8000-000000000004"
	testWorkerID     = "00000000-0000-4000-8000-000000000005"
	testWorkspaceID  = "00000000-0000-4000-8000-000000000006"
	testThreadID     = "00000000-0000-4000-8000-000000000007"
	testTurnID       = "00000000-0000-4000-8000-000000000008"
)

func TestSelfTargetResultPackageTransferKeepsOutboxAndInboxIsolated(t *testing.T) {
	fixture := newManagerFixture(t)
	defer fixture.close()
	start := time.Unix(1_700_000_000, 0)
	fixture.manager.now = func() time.Time { return start }
	bundle := make([]byte, protocol.ResultPackageChunkBytes+17)
	for index := range bundle {
		bundle[index] = byte(index * 31)
	}
	packageID := testID(100)
	metadata, worker := fixture.publishOutbox(t, packageID, bundle, start)
	root := control.NewRootPrincipal(testControllerID, testTreeID, testRootAgentID, testDeviceID).Identity()
	begin := protocol.BeginResultPackageParams{
		AttemptID: testID(101), PackageID: packageID,
		LeaseExpiresAt: start.Add(5 * time.Minute).Unix(), Metadata: metadata,
	}
	result, err := fixture.manager.BeginResultPackage(context.Background(), BeginRequest{
		TreeID: testTreeID, Source: root, Params: begin,
	})
	if err != nil || result.Outcome != protocol.ResultPackageReceiving {
		t.Fatalf("begin result package = %#v, %v", result, err)
	}
	workerPrincipal := control.NewWorkerPrincipal(
		testControllerID, testTreeID, testWorkerID, testRootAgentID, testDeviceID,
	).Identity()
	metadataRaceObserved := false
	for offset := int64(0); offset < int64(len(bundle)); {
		limit := min(protocol.ResultPackageChunkBytes, len(bundle)-int(offset))
		read, err := fixture.manager.ReadResultPackagePart(context.Background(), ReadRequest{
			TreeID: testTreeID, Source: workerPrincipal,
			Params: protocol.ReadResultPackagePartParams{
				PackageID: packageID, Kind: protocol.ResultPackagePartChangesBundle,
				Offset: offset, Limit: limit,
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		if !metadataRaceObserved {
			stored, err := fixture.state.GetResultOutbox(context.Background(), store.ResultOutboxKey{
				WorkerKey: worker.WorkerKey, SourceDeviceID: testDeviceID, PackageID: packageID,
			})
			if err != nil || stored.State != store.ResultOutboxPublishPending {
				t.Fatalf("source read race outbox = %#v, %v", stored, err)
			}
			if _, err := fixture.state.AcknowledgeResultOutboxMetadata(
				context.Background(), stored.ResultOutboxKey, stored.Metadata, start.Add(2*time.Second),
			); err != nil {
				t.Fatal(err)
			}
			metadataRaceObserved = true
		}
		written, err := fixture.manager.WriteResultPackagePart(context.Background(), WriteRequest{
			TreeID: testTreeID, Source: root,
			Params: protocol.WriteResultPackagePartParams{
				AttemptID: begin.AttemptID, PackageID: packageID,
				Kind: read.Kind, Offset: read.Offset, Data: read.Data,
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		offset = written.NextOffset
	}
	replayed, err := fixture.manager.WriteResultPackagePart(context.Background(), WriteRequest{
		TreeID: testTreeID, Source: root,
		Params: protocol.WriteResultPackagePartParams{
			AttemptID: begin.AttemptID, PackageID: packageID,
			Kind: protocol.ResultPackagePartChangesBundle, Offset: 0,
			Data: bundle[:protocol.ResultPackageChunkBytes],
		},
	})
	if err != nil || replayed.NextOffset != int64(len(bundle)) {
		t.Fatalf("chunk replay = %#v, %v", replayed, err)
	}
	if _, err := fixture.manager.FinishResultPackage(context.Background(), FinishRequest{
		TreeID: testTreeID, Source: root,
		Params: protocol.FinishResultPackageParams{AttemptID: begin.AttemptID, PackageID: packageID},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.manager.AcknowledgeResultPackage(context.Background(), AcknowledgeRequest{
		TreeID: testTreeID, Source: workerPrincipal,
		Params: protocol.AcknowledgeResultPackageParams{PackageID: packageID, Sequence: 9},
	}); err != nil {
		t.Fatal(err)
	}
	outbox, err := fixture.state.GetResultOutbox(context.Background(), store.ResultOutboxKey{
		WorkerKey: worker.WorkerKey, SourceDeviceID: testDeviceID, PackageID: packageID,
	})
	if err != nil || outbox.State != store.ResultOutboxDelivered || outbox.DeliverySequence != 9 {
		t.Fatalf("delivered outbox = %#v, %v", outbox, err)
	}
	for _, root := range []*os.Root{fixture.manager.outbox, fixture.manager.inbox} {
		if info, err := root.Lstat(packageID); err != nil || !info.IsDir() {
			t.Fatalf("self-target package directory = %#v, %v", info, err)
		}
	}
	if _, err := fixture.manager.AcknowledgeResultPackage(context.Background(), AcknowledgeRequest{
		TreeID: testTreeID, Source: workerPrincipal,
		Params: protocol.AcknowledgeResultPackageParams{PackageID: packageID, Sequence: 10},
	}); !errors.Is(err, store.ErrResultPackageConflict) {
		t.Fatalf("changed delivery sequence error = %v", err)
	}
}

func TestBrokerReleaseDurablyRemovesOnlyDeliveredOutbox(t *testing.T) {
	fixture := newManagerFixture(t)
	defer fixture.close()
	start := time.Unix(1_700_000_000, 0)
	fixture.manager.now = func() time.Time { return start }
	packageID := testID(150)
	metadata, worker := fixture.publishOutbox(t, packageID, []byte("result"), start)
	key := store.ResultOutboxKey{
		WorkerKey: worker.WorkerKey, SourceDeviceID: testDeviceID, PackageID: packageID,
	}
	if _, err := fixture.state.AcknowledgeResultOutboxMetadata(
		context.Background(), key, metadata, start.Add(time.Second),
	); err != nil {
		t.Fatal(err)
	}
	workerPrincipal := control.NewWorkerPrincipal(
		testControllerID, testTreeID, testWorkerID, testRootAgentID, testDeviceID,
	).Identity()
	release := ReleaseRequest{
		TreeID: testTreeID, Source: workerPrincipal,
		Params: protocol.ReleaseResultPackageParams{PackageID: packageID, Sequence: 9},
	}
	if _, err := fixture.manager.ReleaseResultPackage(context.Background(), release); !errors.Is(
		err, store.ErrResultPackageTransition,
	) {
		t.Fatalf("release before delivery error = %v", err)
	}
	if _, err := fixture.manager.AcknowledgeResultPackage(context.Background(), AcknowledgeRequest{
		TreeID: testTreeID, Source: workerPrincipal,
		Params: protocol.AcknowledgeResultPackageParams{PackageID: packageID, Sequence: 9},
	}); err != nil {
		t.Fatal(err)
	}
	result, err := fixture.manager.ReleaseResultPackage(context.Background(), release)
	if err != nil || result != protocol.ReleaseResultPackageResult(release.Params) {
		t.Fatalf("release = %#v, %v", result, err)
	}
	if _, err := fixture.state.GetResultOutbox(context.Background(), key); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("released outbox lookup error = %v", err)
	}
	if _, err := fixture.manager.outbox.Lstat(packageID); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("released outbox directory error = %v", err)
	}
	if replay, err := fixture.manager.ReleaseResultPackage(context.Background(), release); err != nil ||
		replay != result {
		t.Fatalf("release replay = %#v, %v", replay, err)
	}
	acknowledgement := AcknowledgeRequest{
		TreeID: testTreeID, Source: workerPrincipal,
		Params: protocol.AcknowledgeResultPackageParams{PackageID: packageID, Sequence: 9},
	}
	if replay, err := fixture.manager.AcknowledgeResultPackage(
		context.Background(), acknowledgement,
	); err != nil || replay != protocol.AcknowledgeResultPackageResult(acknowledgement.Params) {
		t.Fatalf("acknowledgement after release replay = %#v, %v", replay, err)
	}
	wrong := release
	wrong.Source = control.NewWorkerPrincipal(
		testControllerID, testTreeID, testID(151), testRootAgentID, testDeviceID,
	).Identity()
	if _, err := fixture.manager.ReleaseResultPackage(context.Background(), wrong); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("unknown worker release error = %v", err)
	}
	wrongAcknowledgement := acknowledgement
	wrongAcknowledgement.Source = wrong.Source
	if _, err := fixture.manager.AcknowledgeResultPackage(
		context.Background(), wrongAcknowledgement,
	); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("unknown worker acknowledgement error = %v", err)
	}
}

func TestStartupCompletesOutboxReleaseOnBothCrashSides(t *testing.T) {
	for _, removeBeforeRestart := range []bool{false, true} {
		t.Run(fmt.Sprintf("removed=%t", removeBeforeRestart), func(t *testing.T) {
			fixture := newManagerFixture(t)
			start := time.Unix(1_700_000_000, 0)
			fixture.manager.now = func() time.Time { return start }
			packageID := testID(160)
			metadata, worker := fixture.publishOutbox(t, packageID, []byte("result"), start)
			key := store.ResultOutboxKey{
				WorkerKey: worker.WorkerKey, SourceDeviceID: testDeviceID, PackageID: packageID,
			}
			if _, err := fixture.state.AcknowledgeResultOutboxMetadata(
				context.Background(), key, metadata, start.Add(time.Second),
			); err != nil {
				t.Fatal(err)
			}
			if _, err := fixture.state.AcknowledgeResultOutboxDelivery(
				context.Background(), key, 9, start.Add(2*time.Second),
			); err != nil {
				t.Fatal(err)
			}
			if _, err := fixture.state.PrepareResultOutboxRelease(
				context.Background(), key, 9, start.Add(3*time.Second),
			); err != nil {
				t.Fatal(err)
			}
			if removeBeforeRestart {
				if err := removePackageDirectory(fixture.manager.outbox, packageID); err != nil {
					t.Fatal(err)
				}
			}
			if err := fixture.manager.Close(); err != nil {
				t.Fatal(err)
			}
			fixture.manager = fixture.reopen(t)
			if _, err := fixture.state.GetResultOutbox(context.Background(), key); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("recovered release lookup error = %v", err)
			}
			if _, err := fixture.manager.outbox.Lstat(packageID); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("recovered release directory error = %v", err)
			}
			fixture.close()
		})
	}
}

func TestRecoveryToleratesOnePendingDeletionAndMaintenanceRetriesIt(t *testing.T) {
	fixture := newManagerFixture(t)
	defer fixture.close()
	ctx := context.Background()
	start := time.Unix(1_700_000_000, 0)
	fixture.manager.now = func() time.Time { return start }
	outboxPackageID := testID(170)
	metadata, worker := fixture.publishOutbox(t, outboxPackageID, []byte("result"), start)
	outboxKey := store.ResultOutboxKey{
		WorkerKey: worker.WorkerKey, SourceDeviceID: testDeviceID, PackageID: outboxPackageID,
	}
	if _, err := fixture.state.AcknowledgeResultOutboxMetadata(
		ctx, outboxKey, metadata, start.Add(time.Second),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.state.AcknowledgeResultOutboxDelivery(
		ctx, outboxKey, 9, start.Add(2*time.Second),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.state.PrepareResultOutboxRelease(
		ctx, outboxKey, 9, start.Add(3*time.Second),
	); err != nil {
		t.Fatal(err)
	}

	inboxPackageID := testID(171)
	inboxAttemptID := testID(172)
	root := control.NewRootPrincipal(
		testControllerID, testTreeID, testRootAgentID, testDeviceID,
	).Identity()
	if _, err := fixture.manager.BeginResultPackage(ctx, BeginRequest{
		TreeID: testTreeID, Source: root,
		Params: protocol.BeginResultPackageParams{
			AttemptID: inboxAttemptID, PackageID: inboxPackageID,
			LeaseExpiresAt: start.Add(5 * time.Minute).Unix(),
			Metadata:       emptyMetadata(t, inboxPackageID),
		},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.manager.FinishResultPackage(ctx, FinishRequest{
		TreeID: testTreeID, Source: root,
		Params: protocol.FinishResultPackageParams{
			AttemptID: inboxAttemptID, PackageID: inboxPackageID,
		},
	}); err != nil {
		t.Fatal(err)
	}
	inboxAuthority := store.ResultInboxAuthority{
		ControllerID: testControllerID, TreeID: testTreeID,
		RootAgentID: testRootAgentID, RootDeviceID: testDeviceID,
	}
	if _, err := fixture.state.PrepareResultInboxEviction(
		ctx, inboxAuthority, inboxPackageID, start.Add(4*time.Second),
	); err != nil {
		t.Fatal(err)
	}

	remove := fixture.manager.removePackage
	wantRemovalError := errors.New("transient package lock")
	fixture.manager.removePackage = func(root *os.Root, packageID string) error {
		if packageID == outboxPackageID {
			return wantRemovalError
		}
		return remove(root, packageID)
	}
	if err := fixture.manager.recover(ctx); err != nil {
		t.Fatalf("recovery failed on retryable terminal deletion: %v", err)
	}
	if _, err := fixture.state.GetResultInbox(
		ctx, inboxAuthority, inboxPackageID,
	); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("later inbox eviction was not compacted: %v", err)
	}
	if err := fixture.manager.CleanupResultPackages(ctx); !errors.Is(err, wantRemovalError) {
		t.Fatalf("maintenance error = %v, want transient removal error", err)
	}
	fixture.manager.removePackage = remove
	if err := fixture.manager.CleanupResultPackages(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.state.GetResultOutbox(ctx, outboxKey); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("retried outbox release lookup error = %v", err)
	}
}

func TestInboxAdmissionEvictsAvailableButNeverReceiving(t *testing.T) {
	fixture := newManagerFixture(t)
	defer fixture.close()
	start := time.Unix(1_700_000_000, 0)
	current := start
	fixture.manager.now = func() time.Time { return current }
	root := control.NewRootPrincipal(testControllerID, testTreeID, testRootAgentID, testDeviceID).Identity()
	firstAvailable := testID(2000)
	for index := 0; index < store.MaximumPeerResultPackages-1; index++ {
		packageID := testID(2000 + index)
		attemptID := testID(2100 + index)
		current = start.Add(time.Duration(index) * time.Second)
		if _, err := fixture.manager.BeginResultPackage(context.Background(), BeginRequest{
			TreeID: testTreeID, Source: root,
			Params: protocol.BeginResultPackageParams{
				AttemptID: attemptID, PackageID: packageID,
				LeaseExpiresAt: current.Add(store.MaximumResultInboxLease).Unix(), Metadata: emptyMetadata(t, packageID),
			},
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.manager.FinishResultPackage(context.Background(), FinishRequest{
			TreeID: testTreeID, Source: root,
			Params: protocol.FinishResultPackageParams{AttemptID: attemptID, PackageID: packageID},
		}); err != nil {
			t.Fatal(err)
		}
	}
	receivingID := testID(2200)
	receivingAttempt := testID(2201)
	current = start.Add(10 * time.Minute)
	if _, err := fixture.manager.BeginResultPackage(context.Background(), BeginRequest{
		TreeID: testTreeID, Source: root,
		Params: protocol.BeginResultPackageParams{
			AttemptID: receivingAttempt, PackageID: receivingID,
			LeaseExpiresAt: current.Add(store.MaximumResultInboxLease).Unix(), Metadata: emptyMetadata(t, receivingID),
		},
	}); err != nil {
		t.Fatal(err)
	}
	newPackageID := testID(2202)
	newAttemptID := testID(2203)
	current = current.Add(time.Second)
	if _, err := fixture.manager.BeginResultPackage(context.Background(), BeginRequest{
		TreeID: testTreeID, Source: root,
		Params: protocol.BeginResultPackageParams{
			AttemptID: newAttemptID, PackageID: newPackageID,
			LeaseExpiresAt: current.Add(store.MaximumResultInboxLease).Unix(), Metadata: emptyMetadata(t, newPackageID),
		},
	}); err != nil {
		t.Fatal(err)
	}
	authority := store.ResultInboxAuthority{
		ControllerID: testControllerID, TreeID: testTreeID,
		RootAgentID: testRootAgentID, RootDeviceID: testDeviceID,
	}
	if _, err := fixture.state.GetResultInbox(context.Background(), authority, firstAvailable); !errors.Is(
		err, store.ErrNotFound,
	) {
		t.Fatalf("oldest available inbox lookup error = %v", err)
	}
	for _, packageID := range []string{receivingID, newPackageID} {
		inbox, err := fixture.state.GetResultInbox(context.Background(), authority, packageID)
		if err != nil || inbox.State != store.ResultInboxReceiving {
			t.Fatalf("protected receiving inbox %s = %#v, %v", packageID, inbox, err)
		}
	}
}

func TestInboxAdmissionRetriesLockedTombstoneBeforeChoosingAnotherVictim(t *testing.T) {
	fixture := newManagerFixture(t)
	defer fixture.close()
	ctx := context.Background()
	start := time.Unix(1_700_000_000, 0)
	current := start
	fixture.manager.now = func() time.Time { return current }
	root := control.NewRootPrincipal(
		testControllerID, testTreeID, testRootAgentID, testDeviceID,
	).Identity()
	for index := 0; index < store.MaximumPeerResultPackages; index++ {
		packageID := testID(2300 + index)
		attemptID := testID(2400 + index)
		current = start.Add(time.Duration(index) * time.Second)
		if _, err := fixture.manager.BeginResultPackage(ctx, BeginRequest{
			TreeID: testTreeID, Source: root,
			Params: protocol.BeginResultPackageParams{
				AttemptID: attemptID, PackageID: packageID,
				LeaseExpiresAt: current.Add(store.MaximumResultInboxLease).Unix(),
				Metadata:       emptyMetadata(t, packageID),
			},
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.manager.FinishResultPackage(ctx, FinishRequest{
			TreeID: testTreeID, Source: root,
			Params: protocol.FinishResultPackageParams{
				AttemptID: attemptID, PackageID: packageID,
			},
		}); err != nil {
			t.Fatal(err)
		}
	}

	lockedPackageID := testID(2300)
	incomingPackageID := testID(2500)
	incomingAttemptID := testID(2501)
	wantLockError := errors.New("result package is temporarily locked")
	remove := fixture.manager.removePackage
	fixture.manager.removePackage = func(root *os.Root, packageID string) error {
		if packageID == lockedPackageID {
			return wantLockError
		}
		return remove(root, packageID)
	}
	begin := BeginRequest{
		TreeID: testTreeID, Source: root,
		Params: protocol.BeginResultPackageParams{
			AttemptID: incomingAttemptID, PackageID: incomingPackageID,
			LeaseExpiresAt: current.Add(store.MaximumResultInboxLease).Unix(),
			Metadata:       emptyMetadata(t, incomingPackageID),
		},
	}
	for attempt := 1; attempt <= 2; attempt++ {
		if _, err := fixture.manager.BeginResultPackage(ctx, begin); !errors.Is(err, wantLockError) {
			t.Fatalf("locked admission attempt %d error = %v", attempt, err)
		}
		available, err := fixture.state.ListAvailableResultInboxes(
			ctx, testControllerID, testDeviceID, store.MaximumPeerResultPackages,
		)
		if err != nil || len(available) != store.MaximumPeerResultPackages-1 {
			t.Fatalf("available results after attempt %d = %d, %v", attempt, len(available), err)
		}
		tombstones, err := fixture.state.ListResultInboxEvictionTombstones(
			ctx, testControllerID, testDeviceID, store.MaximumPeerResultPackages,
		)
		if err != nil || len(tombstones) != 1 || tombstones[0].PackageID != lockedPackageID {
			t.Fatalf("eviction tombstones after attempt %d = %#v, %v", attempt, tombstones, err)
		}
	}

	fixture.manager.removePackage = remove
	if _, err := fixture.manager.BeginResultPackage(ctx, begin); err != nil {
		t.Fatal(err)
	}
	status, err := fixture.state.ReadPeerStatusSnapshot(ctx, testControllerID, testDeviceID)
	if err != nil || status.Results.InboxEvicted != 1 || status.Results.InboxReceiving != 1 ||
		status.Results.InboxAvailable != store.MaximumPeerResultPackages-1 {
		t.Fatalf("post-retry inbox status = %#v, %v", status.Results, err)
	}
}

func TestReceivingRecoveryTruncatesUncommittedBytesAndCompletesPreparedRemoval(t *testing.T) {
	fixture := newManagerFixture(t)
	start := time.Now().Truncate(time.Second)
	fixture.manager.now = func() time.Time { return start }
	packageID := testID(200)
	attemptID := testID(201)
	payload := bytes.Repeat([]byte("x"), protocol.ResultPackageChunkBytes+1)
	metadata := workspaceMetadata(t, packageID, payload)
	root := control.NewRootPrincipal(testControllerID, testTreeID, testRootAgentID, testDeviceID).Identity()
	begin := protocol.BeginResultPackageParams{
		AttemptID: attemptID, PackageID: packageID,
		LeaseExpiresAt: start.Add(5 * time.Minute).Unix(), Metadata: metadata,
	}
	if _, err := fixture.manager.BeginResultPackage(context.Background(), BeginRequest{
		TreeID: testTreeID, Source: root, Params: begin,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.manager.WriteResultPackagePart(context.Background(), WriteRequest{
		TreeID: testTreeID, Source: root,
		Params: protocol.WriteResultPackagePartParams{
			AttemptID: attemptID, PackageID: packageID,
			Kind: protocol.ResultPackagePartChangesBundle, Data: payload[:protocol.ResultPackageChunkBytes],
		},
	}); err != nil {
		t.Fatal(err)
	}
	partPath := filepath.Join(
		fixture.workspace, inboxDirectoryName, receivingDirectoryName(packageID, attemptID),
		protocol.ResultChangesBundleFileName,
	)
	file, err := os.OpenFile(partPath, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte("uncommitted")); err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(file.Sync(), file.Close(), fixture.manager.Close()); err != nil {
		t.Fatal(err)
	}
	fixture.manager = fixture.reopen(t)
	info, err := os.Stat(partPath)
	if err != nil || info.Size() != protocol.ResultPackageChunkBytes {
		t.Fatalf("recovered part size = %v, %v", info, err)
	}
	result, err := fixture.manager.BeginResultPackage(context.Background(), BeginRequest{
		TreeID: testTreeID, Source: root, Params: begin,
	})
	if err != nil || result.Offsets[0].NextOffset != protocol.ResultPackageChunkBytes {
		t.Fatalf("replayed begin = %#v, %v", result, err)
	}
	authority := store.ResultInboxAuthority{
		ControllerID: testControllerID, TreeID: testTreeID,
		RootAgentID: testRootAgentID, RootDeviceID: testDeviceID,
	}
	if _, err := fixture.state.PrepareResultInboxCancel(
		context.Background(), authority, attemptID, packageID, start.Add(time.Second),
	); err != nil {
		t.Fatal(err)
	}
	if err := fixture.manager.Close(); err != nil {
		t.Fatal(err)
	}
	fixture.manager = fixture.reopen(t)
	if _, err := fixture.state.GetResultInbox(context.Background(), authority, packageID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("removed inbox lookup error = %v", err)
	}
	if _, err := os.Stat(filepath.Dir(partPath)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("prepared removal directory error = %v", err)
	}
	fixture.close()
}

func TestStartupReconcilesPublishedDirectoryAndFailsClosedWhenItDisappears(t *testing.T) {
	fixture := newManagerFixture(t)
	start := time.Now().Truncate(time.Second)
	fixture.manager.now = func() time.Time { return start }
	packageID := testID(300)
	attemptID := testID(301)
	metadata := emptyMetadata(t, packageID)
	root := control.NewRootPrincipal(testControllerID, testTreeID, testRootAgentID, testDeviceID).Identity()
	begin := protocol.BeginResultPackageParams{
		AttemptID: attemptID, PackageID: packageID,
		LeaseExpiresAt: start.Add(5 * time.Minute).Unix(), Metadata: metadata,
	}
	if _, err := fixture.manager.BeginResultPackage(context.Background(), BeginRequest{
		TreeID: testTreeID, Source: root, Params: begin,
	}); err != nil {
		t.Fatal(err)
	}
	temporary := receivingDirectoryName(packageID, attemptID)
	directory, err := fixture.manager.inbox.OpenRoot(temporary)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeManifestFile(directory, metadata); err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(syncDirectory(directory), directory.Close()); err != nil {
		t.Fatal(err)
	}
	if err := fixture.manager.inbox.Rename(temporary, packageID); err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(syncDirectory(fixture.manager.inbox), fixture.manager.Close()); err != nil {
		t.Fatal(err)
	}
	fixture.manager = fixture.reopen(t)
	authority := store.ResultInboxAuthority{
		ControllerID: testControllerID, TreeID: testTreeID,
		RootAgentID: testRootAgentID, RootDeviceID: testDeviceID,
	}
	inbox, err := fixture.state.GetResultInbox(context.Background(), authority, packageID)
	if err != nil || inbox.State != store.ResultInboxAvailable {
		t.Fatalf("recovered available inbox = %#v, %v", inbox, err)
	}
	if err := fixture.manager.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(fixture.workspace, inboxDirectoryName, packageID)); err != nil {
		t.Fatal(err)
	}
	if manager, err := New(context.Background(), fixture.options()); err == nil {
		_ = manager.Close()
		t.Fatal("startup accepted an available row without its package directory")
	}
	fixture.manager = nil
	fixture.close()
}

func TestStartupCompletesResultInboxEviction(t *testing.T) {
	fixture := newManagerFixture(t)
	start := time.Now().Truncate(time.Second)
	fixture.manager.now = func() time.Time { return start }
	packageID := testID(320)
	attemptID := testID(321)
	root := control.NewRootPrincipal(testControllerID, testTreeID, testRootAgentID, testDeviceID).Identity()
	params := protocol.BeginResultPackageParams{
		AttemptID: attemptID, PackageID: packageID,
		LeaseExpiresAt: start.Add(5 * time.Minute).Unix(), Metadata: emptyMetadata(t, packageID),
	}
	if _, err := fixture.manager.BeginResultPackage(context.Background(), BeginRequest{
		TreeID: testTreeID, Source: root, Params: params,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.manager.FinishResultPackage(context.Background(), FinishRequest{
		TreeID: testTreeID, Source: root,
		Params: protocol.FinishResultPackageParams{AttemptID: attemptID, PackageID: packageID},
	}); err != nil {
		t.Fatal(err)
	}
	authority := store.ResultInboxAuthority{
		ControllerID: testControllerID, TreeID: testTreeID,
		RootAgentID: testRootAgentID, RootDeviceID: testDeviceID,
	}
	if _, err := fixture.state.PrepareResultInboxEviction(
		context.Background(), authority, packageID, start.Add(time.Minute),
	); err != nil {
		t.Fatal(err)
	}
	if err := fixture.manager.Close(); err != nil {
		t.Fatal(err)
	}
	fixture.manager = fixture.reopen(t)
	if _, err := fixture.state.GetResultInbox(context.Background(), authority, packageID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("compacted inbox lookup error = %v", err)
	}
	if _, err := fixture.manager.inbox.Lstat(packageID); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("evicted package directory error = %v", err)
	}
	tombstones, err := fixture.state.ListResultInboxEvictionTombstones(
		context.Background(), testControllerID, testDeviceID, store.MaximumPeerResultPackages,
	)
	if err != nil || len(tombstones) != 0 {
		t.Fatalf("remaining eviction tombstones = %#v, %v", tombstones, err)
	}
	status, err := fixture.state.ReadPeerStatusSnapshot(
		context.Background(), testControllerID, testDeviceID,
	)
	if err != nil || status.Results.InboxEvicted != 1 {
		t.Fatalf("inbox eviction lifetime count = %d, %v", status.Results.InboxEvicted, err)
	}
	if err := fixture.manager.CleanupResultPackages(context.Background()); err != nil {
		t.Fatal(err)
	}
	status, err = fixture.state.ReadPeerStatusSnapshot(
		context.Background(), testControllerID, testDeviceID,
	)
	if err != nil || status.Results.InboxEvicted != 1 {
		t.Fatalf("replayed inbox eviction lifetime count = %d, %v", status.Results.InboxEvicted, err)
	}
	fixture.close()
}

func TestFinishRetryResyncsPublishedDirectoryBeforeDatabaseCommit(t *testing.T) {
	fixture := newManagerFixture(t)
	defer fixture.close()
	start := time.Now().Truncate(time.Second)
	fixture.manager.now = func() time.Time { return start }
	packageID := testID(330)
	attemptID := testID(331)
	root := control.NewRootPrincipal(testControllerID, testTreeID, testRootAgentID, testDeviceID).Identity()
	params := protocol.BeginResultPackageParams{
		AttemptID: attemptID, PackageID: packageID,
		LeaseExpiresAt: start.Add(5 * time.Minute).Unix(), Metadata: emptyMetadata(t, packageID),
	}
	if _, err := fixture.manager.BeginResultPackage(context.Background(), BeginRequest{
		TreeID: testTreeID, Source: root, Params: params,
	}); err != nil {
		t.Fatal(err)
	}
	wantSyncError := errors.New("injected inbox sync failure")
	failed := false
	fixture.manager.syncRoot = func(root *os.Root) error {
		if root == fixture.manager.inbox && !failed {
			failed = true
			return wantSyncError
		}
		return syncDirectory(root)
	}
	finish := FinishRequest{
		TreeID: testTreeID, Source: root,
		Params: protocol.FinishResultPackageParams{AttemptID: attemptID, PackageID: packageID},
	}
	if _, err := fixture.manager.FinishResultPackage(context.Background(), finish); !errors.Is(err, wantSyncError) {
		t.Fatalf("finish after rename error = %v", err)
	}
	authority := store.ResultInboxAuthority{
		ControllerID: testControllerID, TreeID: testTreeID,
		RootAgentID: testRootAgentID, RootDeviceID: testDeviceID,
	}
	inbox, err := fixture.state.GetResultInbox(context.Background(), authority, packageID)
	if err != nil || inbox.State != store.ResultInboxReceiving {
		t.Fatalf("inbox after failed parent sync = %#v, %v", inbox, err)
	}
	if _, err := fixture.manager.inbox.Lstat(packageID); err != nil {
		t.Fatalf("published directory after failed parent sync: %v", err)
	}
	resynced := false
	fixture.manager.syncRoot = func(root *os.Root) error {
		if root == fixture.manager.inbox {
			resynced = true
		}
		return syncDirectory(root)
	}
	if _, err := fixture.manager.FinishResultPackage(context.Background(), finish); err != nil {
		t.Fatal(err)
	}
	inbox, err = fixture.state.GetResultInbox(context.Background(), authority, packageID)
	if err != nil || inbox.State != store.ResultInboxAvailable || !resynced {
		t.Fatalf("retried available inbox = %#v, resynced=%t, %v", inbox, resynced, err)
	}
}

func TestStartupRepairsPartialReceivingManifest(t *testing.T) {
	fixture := newManagerFixture(t)
	start := time.Now().Truncate(time.Second)
	fixture.manager.now = func() time.Time { return start }
	packageID := testID(340)
	attemptID := testID(341)
	metadata := emptyMetadata(t, packageID)
	root := control.NewRootPrincipal(testControllerID, testTreeID, testRootAgentID, testDeviceID).Identity()
	if _, err := fixture.manager.BeginResultPackage(context.Background(), BeginRequest{
		TreeID: testTreeID, Source: root,
		Params: protocol.BeginResultPackageParams{
			AttemptID: attemptID, PackageID: packageID,
			LeaseExpiresAt: start.Add(5 * time.Minute).Unix(), Metadata: metadata,
		},
	}); err != nil {
		t.Fatal(err)
	}
	directory, err := fixture.manager.inbox.OpenRoot(receivingDirectoryName(packageID, attemptID))
	if err != nil {
		t.Fatal(err)
	}
	partial := metadata.Manifest[:len(metadata.Manifest)/2]
	file, err := directory.OpenFile(
		protocol.ResultManifestFileName, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(partial); err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(file.Sync(), file.Close(), syncDirectory(directory), directory.Close()); err != nil {
		t.Fatal(err)
	}
	if err := fixture.manager.Close(); err != nil {
		t.Fatal(err)
	}
	fixture.manager = fixture.reopen(t)
	directory, err = fixture.manager.inbox.OpenRoot(receivingDirectoryName(packageID, attemptID))
	if err != nil {
		t.Fatal(err)
	}
	got, err := directory.ReadFile(protocol.ResultManifestFileName)
	if closeErr := directory.Close(); err == nil {
		err = closeErr
	}
	if err != nil || !bytes.Equal(got, metadata.Manifest) {
		t.Fatalf("repaired manifest = %q, %v", got, err)
	}
	fixture.close()
}

func TestConcurrentBeginDoesNotReclaimTransferWithRecentChunkActivity(t *testing.T) {
	fixture := newManagerFixture(t)
	defer fixture.close()
	ctx := context.Background()
	start := time.Now().Truncate(time.Second)
	current := start
	fixture.manager.now = func() time.Time { return current }
	root := control.NewRootPrincipal(testControllerID, testTreeID, testRootAgentID, testDeviceID).Identity()
	packageID := testID(350)
	attemptID := testID(351)
	payload := bytes.Repeat([]byte("a"), protocol.ResultPackageChunkBytes+1)
	metadata := workspaceMetadata(t, packageID, payload)
	begin := protocol.BeginResultPackageParams{
		AttemptID: attemptID, PackageID: packageID,
		LeaseExpiresAt: start.Add(time.Minute).Unix(), Metadata: metadata,
	}
	if _, err := fixture.manager.BeginResultPackage(ctx, BeginRequest{
		TreeID: testTreeID, Source: root, Params: begin,
	}); err != nil {
		t.Fatal(err)
	}
	current = start.Add(9 * time.Minute)
	if _, err := fixture.manager.WriteResultPackagePart(ctx, WriteRequest{
		TreeID: testTreeID, Source: root,
		Params: protocol.WriteResultPackagePartParams{
			AttemptID: attemptID, PackageID: packageID,
			Kind: protocol.ResultPackagePartChangesBundle,
			Data: payload[:protocol.ResultPackageChunkBytes],
		},
	}); err != nil {
		t.Fatal(err)
	}
	current = start.Add(11 * time.Minute)
	otherPackageID := testID(352)
	if _, err := fixture.manager.BeginResultPackage(ctx, BeginRequest{
		TreeID: testTreeID, Source: root,
		Params: protocol.BeginResultPackageParams{
			AttemptID: testID(353), PackageID: otherPackageID,
			LeaseExpiresAt: current.Add(time.Minute).Unix(),
			Metadata:       emptyMetadata(t, otherPackageID),
		},
	}); err != nil {
		t.Fatal(err)
	}
	authority := store.ResultInboxAuthority{
		ControllerID: testControllerID, TreeID: testTreeID,
		RootAgentID: testRootAgentID, RootDeviceID: testDeviceID,
	}
	stored, err := fixture.state.GetResultInbox(ctx, authority, packageID)
	if err != nil || stored.State != store.ResultInboxReceiving ||
		stored.LeaseExpiresAt != start.Add(19*time.Minute).Unix() {
		t.Fatalf("active transfer after concurrent begin = %#v, %v", stored, err)
	}
}

func TestExpiredReclaimWaitsForPackageWriterLock(t *testing.T) {
	fixture := newManagerFixture(t)
	defer fixture.close()
	ctx := context.Background()
	start := time.Now().Truncate(time.Second)
	current := start
	fixture.manager.now = func() time.Time { return current }
	root := control.NewRootPrincipal(testControllerID, testTreeID, testRootAgentID, testDeviceID).Identity()
	packageID := testID(360)
	attemptID := testID(361)
	if _, err := fixture.manager.BeginResultPackage(ctx, BeginRequest{
		TreeID: testTreeID, Source: root,
		Params: protocol.BeginResultPackageParams{
			AttemptID: attemptID, PackageID: packageID,
			LeaseExpiresAt: start.Add(time.Minute).Unix(), Metadata: emptyMetadata(t, packageID),
		},
	}); err != nil {
		t.Fatal(err)
	}
	current = start.Add(store.MaximumResultInboxLease + time.Second)
	lock := fixture.manager.lock(packageID)
	lock.Lock()
	done := make(chan error, 1)
	go func() {
		done <- fixture.manager.reclaimExpired(ctx)
	}()
	select {
	case err := <-done:
		lock.Unlock()
		t.Fatalf("reclaim completed while package writer lock was held: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if _, err := fixture.manager.inbox.Lstat(receivingDirectoryName(packageID, attemptID)); err != nil {
		lock.Unlock()
		t.Fatalf("active receiving directory error = %v", err)
	}
	lock.Unlock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	authority := store.ResultInboxAuthority{
		ControllerID: testControllerID, TreeID: testTreeID,
		RootAgentID: testRootAgentID, RootDeviceID: testDeviceID,
	}
	if _, err := fixture.state.GetResultInbox(ctx, authority, packageID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("reclaimed inbox lookup error = %v", err)
	}
}

func TestCompletedCancelReplayDoesNotRemoveNewAvailableAttempt(t *testing.T) {
	fixture := newManagerFixture(t)
	defer fixture.close()
	start := time.Now().Truncate(time.Second)
	fixture.manager.now = func() time.Time { return start }
	packageID := testID(370)
	firstAttemptID := testID(371)
	secondAttemptID := testID(372)
	metadata := emptyMetadata(t, packageID)
	root := control.NewRootPrincipal(testControllerID, testTreeID, testRootAgentID, testDeviceID).Identity()
	begin := func(attemptID string) {
		t.Helper()
		if _, err := fixture.manager.BeginResultPackage(context.Background(), BeginRequest{
			TreeID: testTreeID, Source: root,
			Params: protocol.BeginResultPackageParams{
				AttemptID: attemptID, PackageID: packageID,
				LeaseExpiresAt: start.Add(5 * time.Minute).Unix(), Metadata: metadata,
			},
		}); err != nil {
			t.Fatal(err)
		}
	}
	begin(firstAttemptID)
	firstCancel := CancelRequest{
		TreeID: testTreeID, Source: root,
		Params: protocol.CancelResultPackageParams{AttemptID: firstAttemptID, PackageID: packageID},
	}
	if _, err := fixture.manager.CancelResultPackage(context.Background(), firstCancel); err != nil {
		t.Fatal(err)
	}
	begin(secondAttemptID)
	if _, err := fixture.manager.FinishResultPackage(context.Background(), FinishRequest{
		TreeID: testTreeID, Source: root,
		Params: protocol.FinishResultPackageParams{AttemptID: secondAttemptID, PackageID: packageID},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.manager.CancelResultPackage(context.Background(), firstCancel); err != nil {
		t.Fatalf("completed cancel replay: %v", err)
	}
	authority := store.ResultInboxAuthority{
		ControllerID: testControllerID, TreeID: testTreeID,
		RootAgentID: testRootAgentID, RootDeviceID: testDeviceID,
	}
	inbox, err := fixture.state.GetResultInbox(context.Background(), authority, packageID)
	if err != nil || inbox.State != store.ResultInboxAvailable || inbox.AttemptID != secondAttemptID {
		t.Fatalf("new available attempt after old cancel replay = %#v, %v", inbox, err)
	}
	if _, err := fixture.manager.inbox.Lstat(packageID); err != nil {
		t.Fatalf("new available directory after old cancel replay: %v", err)
	}
}

func TestFinishRejectsDigestAndRolloutCorruption(t *testing.T) {
	for _, test := range []struct {
		name     string
		metadata func(*testing.T, string) (protocol.ResultPackageMetadata, []byte)
		want     error
	}{
		{
			name: "payload digest",
			metadata: func(t *testing.T, packageID string) (protocol.ResultPackageMetadata, []byte) {
				good := []byte("good")
				return workspaceMetadata(t, packageID, good), []byte("baad")
			},
		},
		{
			name:     "rollout frame",
			metadata: invalidRolloutMetadata,
			want:     rolloutcapture.ErrInvalidFrame,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newManagerFixture(t)
			defer fixture.close()
			start := time.Unix(1_700_003_000, 0)
			fixture.manager.now = func() time.Time { return start }
			packageID := testID(400 + len(test.name))
			attemptID := testID(500 + len(test.name))
			metadata, payload := test.metadata(t, packageID)
			manifest, err := metadata.DecodeManifest()
			if err != nil {
				t.Fatal(err)
			}
			root := control.NewRootPrincipal(testControllerID, testTreeID, testRootAgentID, testDeviceID).Identity()
			begin := protocol.BeginResultPackageParams{
				AttemptID: attemptID, PackageID: packageID,
				LeaseExpiresAt: start.Add(5 * time.Minute).Unix(), Metadata: metadata,
			}
			if _, err := fixture.manager.BeginResultPackage(context.Background(), BeginRequest{
				TreeID: testTreeID, Source: root, Params: begin,
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := fixture.manager.WriteResultPackagePart(context.Background(), WriteRequest{
				TreeID: testTreeID, Source: root,
				Params: protocol.WriteResultPackagePartParams{
					AttemptID: attemptID, PackageID: packageID,
					Kind: manifest.Parts[0].Kind, Data: payload,
				},
			}); err != nil {
				t.Fatal(err)
			}
			_, err = fixture.manager.FinishResultPackage(context.Background(), FinishRequest{
				TreeID: testTreeID, Source: root,
				Params: protocol.FinishResultPackageParams{AttemptID: attemptID, PackageID: packageID},
			})
			if err == nil || test.want != nil && !errors.Is(err, test.want) {
				t.Fatalf("finish corruption error = %v, want %v", err, test.want)
			}
			if _, err := fixture.manager.CancelResultPackage(context.Background(), CancelRequest{
				TreeID: testTreeID, Source: root,
				Params: protocol.CancelResultPackageParams{AttemptID: attemptID, PackageID: packageID},
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestManagerRejectsUnexpectedSymlinkAndWrongAuthority(t *testing.T) {
	if runtime.GOOS != "windows" {
		workspace := t.TempDir()
		inboxPath := filepath.Join(workspace, inboxDirectoryName)
		if err := os.Mkdir(inboxPath, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(t.TempDir(), filepath.Join(inboxPath, "unexpected")); err != nil {
			t.Fatal(err)
		}
		state := openTestStore(t)
		defer state.Close()
		manager, err := New(context.Background(), Options{
			ControllerID: testControllerID, DeviceID: testDeviceID,
			WorkspaceRoot: workspace, Store: state,
		})
		if err == nil {
			_ = manager.Close()
			t.Fatal("manager accepted an unexpected inbox symlink")
		}

		workspace = t.TempDir()
		outboxPath := filepath.Join(workspace, outboxDirectoryName)
		if err := os.Mkdir(outboxPath, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(outboxPath, 0o750); err != nil {
			t.Fatal(err)
		}
		state = openTestStore(t)
		defer state.Close()
		manager, err = New(context.Background(), Options{
			ControllerID: testControllerID, DeviceID: testDeviceID,
			WorkspaceRoot: workspace, Store: state,
		})
		if err == nil {
			_ = manager.Close()
			t.Fatal("manager accepted a result package directory without mode 0700")
		}
	}

	fixture := newManagerFixture(t)
	defer fixture.close()
	start := time.Unix(1_700_004_000, 0)
	fixture.manager.now = func() time.Time { return start }
	wrong := control.NewRootPrincipal(testControllerID, testTreeID, testRootAgentID, testID(999)).Identity()
	metadata := emptyMetadata(t, testID(600))
	_, err := fixture.manager.BeginResultPackage(context.Background(), BeginRequest{
		TreeID: testTreeID, Source: wrong,
		Params: protocol.BeginResultPackageParams{
			AttemptID: testID(601), PackageID: testID(600),
			LeaseExpiresAt: start.Add(time.Minute).Unix(), Metadata: metadata,
		},
	})
	if !errors.Is(err, store.ErrResultPackageAuthority) {
		t.Fatalf("wrong root authority error = %v", err)
	}
}

type managerFixture struct {
	workspace string
	state     *store.PeerStore
	manager   *Manager
}

func newManagerFixture(t *testing.T) *managerFixture {
	t.Helper()
	fixture := &managerFixture{workspace: t.TempDir(), state: openTestStore(t)}
	fixture.manager = fixture.reopen(t)
	return fixture
}

func (f *managerFixture) options() Options {
	return Options{
		ControllerID:  testControllerID,
		DeviceID:      testDeviceID,
		WorkspaceRoot: f.workspace,
		Store:         f.state,
	}
}

func (f *managerFixture) reopen(t *testing.T) *Manager {
	t.Helper()
	manager, err := New(context.Background(), f.options())
	if err != nil {
		t.Fatal(err)
	}
	return manager
}

func (f *managerFixture) close() {
	if f.manager != nil {
		_ = f.manager.Close()
		f.manager = nil
	}
	if f.state != nil {
		_ = f.state.Close()
		f.state = nil
	}
}

func (f *managerFixture) publishOutbox(
	t *testing.T,
	packageID string,
	bundle []byte,
	start time.Time,
) (protocol.ResultPackageMetadata, store.WorkerReservation) {
	t.Helper()
	ctx := context.Background()
	workspacePath := filepath.Join(f.workspace, "managed-workspace")
	if err := os.Mkdir(workspacePath, 0o700); err != nil {
		t.Fatal(err)
	}
	manifest := protocol.WorkspaceManifest{
		GitURL:  "https://example.invalid/repository.git",
		HeadOID: strings.Repeat("a", 40), ObjectFormat: "sha1",
		WorkingDirectory: "", Clean: true,
		SourceSnapshotHash: strings.Repeat("b", 64), Warnings: []string{},
	}
	manifestHash, err := protocol.WorkspaceManifestHash(manifest)
	if err != nil {
		t.Fatal(err)
	}
	workspace, err := f.state.RecordPreparedWorkspace(ctx, store.PreparedWorkspace{
		PreparedWorkspaceKey: store.PreparedWorkspaceKey{
			ControllerID: testControllerID, TreeID: testTreeID, WorkspaceID: testWorkspaceID,
		},
		SourceAgentID: testRootAgentID, SourceDeviceID: testDeviceID, TargetDeviceID: testDeviceID,
		GitURL: manifest.GitURL, HeadOID: manifest.HeadOID, ObjectFormat: manifest.ObjectFormat,
		Clean: manifest.Clean, SourceSnapshotHash: manifest.SourceSnapshotHash,
		WorkspacePath: workspacePath, Strategy: protocol.WorkspaceStrategyDirect,
		ManifestHash: manifestHash, SourceWarnings: []string{}, Warnings: []string{},
	}, start)
	if err != nil {
		t.Fatal(err)
	}
	worker, err := f.state.ReserveWorkerStartWithWorkspace(ctx, store.WorkerReservation{
		WorkerKey: store.WorkerKey{
			ControllerID: testControllerID, TreeID: testTreeID, AgentID: testWorkerID,
		},
		ParentAgentID: testRootAgentID, DeviceID: testDeviceID,
		TaskName: "self target", PromptDigest: strings.Repeat("c", 64),
		WorkspaceID: testWorkspaceID, WorkspacePath: workspacePath, ProfileVersion: 1,
	}, 1, start.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	worker, err = f.state.AttachWorkerThread(ctx, worker.WorkerKey, testThreadID, start.Add(2*time.Second))
	if err == nil {
		worker, err = f.state.MarkWorkerReady(ctx, worker.WorkerKey, start.Add(3*time.Second))
	}
	if err == nil {
		_, _, err = f.state.PrepareWorkerTurnStartIntent(ctx, store.PrepareWorkerTurnStartIntentRequest{
			WorkerKey: worker.WorkerKey, IntentID: testID(98), DeviceID: testDeviceID,
			ManagedThreadID: testThreadID, PackageID: packageID,
			Rollout: store.WorkerRolloutLocator{
				Status: store.WorkerRolloutUnavailable, FailureCode: "rollout_unavailable",
			},
			ReservationLimitBytes: protocol.MaximumResultPackageBytes,
		}, start.Add(4*time.Second))
	}
	if err == nil {
		var resolution store.WorkerTurnStartResolution
		resolution, err = f.state.BindWorkerTurnStartIntent(
			ctx, worker.WorkerKey, testID(98), testTurnID, start.Add(5*time.Second),
		)
		worker = resolution.Worker
	}
	if err != nil {
		t.Fatal(err)
	}
	finalization, err := f.state.BeginWorkerFinalization(
		ctx, worker.WorkerKey, testTurnID, store.WorkerIdle, "", start.Add(6*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	worker = finalization.Worker
	descriptor := protocol.ResultPackagePartDescriptor{
		Kind: protocol.ResultPackagePartChangesBundle,
		Size: int64(len(bundle)), SHA256: sha256Hex(bundle),
	}
	resultManifest := protocol.ResultManifest{
		Version: protocol.ResultManifestVersion, PackageID: packageID,
		ControllerID: testControllerID, TreeID: testTreeID,
		SourceAgentID: testWorkerID, SourceDeviceID: testDeviceID,
		ManagedThreadID: testThreadID, TurnID: testTurnID,
		LifecycleRevision: worker.Revision,
		Terminal:          protocol.ResultTerminal{Outcome: protocol.ResultTerminalCompleted},
		CapturedAt:        start.Add(6 * time.Second).Unix(),
		Rollout: protocol.ResultRolloutComponent{
			Status: protocol.ResultRolloutCaptureFailed, FailureCode: "rollout_unavailable",
		},
		Workspace: protocol.ResultWorkspaceComponent{
			Status: protocol.ResultWorkspaceChanged, WorkspaceID: testWorkspaceID,
			SourceDeviceID: testDeviceID, TargetDeviceID: testDeviceID,
			ObjectFormat: "sha1", BaseHeadOID: workspace.HeadOID,
			BaseManifestHash: workspace.ManifestHash, BaseSnapshotHash: workspace.SourceSnapshotHash,
			BaseClean: true, ResultHeadOID: strings.Repeat("d", 40),
			ResultSnapshotHash: strings.Repeat("e", 64), ResultClean: true,
			BaseWarnings: []string{}, ResultWarnings: []string{},
		},
		Parts: []protocol.ResultPackagePartDescriptor{descriptor},
	}
	metadata := encodeMetadata(t, resultManifest)
	key := store.ResultOutboxKey{
		WorkerKey: worker.WorkerKey, SourceDeviceID: testDeviceID, PackageID: packageID,
	}
	if err := writePackageDirectory(f.manager.outbox, packageID, metadata, map[string][]byte{
		protocol.ResultChangesBundleFileName: bundle,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.state.CommitResultOutboxCapture(ctx, key, metadata, start.Add(7*time.Second)); err != nil {
		t.Fatal(err)
	}
	return metadata, worker
}

func openTestStore(t *testing.T) *store.PeerStore {
	t.Helper()
	directory := t.TempDir()
	state, err := store.OpenPeer(context.Background(), filepath.Join(directory, "peer.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func workspaceMetadata(t *testing.T, packageID string, payload []byte) protocol.ResultPackageMetadata {
	t.Helper()
	return encodeMetadata(t, protocol.ResultManifest{
		Version: protocol.ResultManifestVersion, PackageID: packageID,
		ControllerID: testControllerID, TreeID: testTreeID,
		SourceAgentID: testWorkerID, SourceDeviceID: testDeviceID,
		ManagedThreadID: testThreadID, TurnID: testTurnID, LifecycleRevision: 7,
		Terminal:   protocol.ResultTerminal{Outcome: protocol.ResultTerminalCompleted},
		CapturedAt: 1_700_000_000,
		Rollout: protocol.ResultRolloutComponent{
			Status: protocol.ResultRolloutCaptureFailed, FailureCode: "rollout_unavailable",
		},
		Workspace: protocol.ResultWorkspaceComponent{
			Status: protocol.ResultWorkspaceChanged, WorkspaceID: testWorkspaceID,
			SourceDeviceID: testDeviceID, TargetDeviceID: testDeviceID,
			ObjectFormat: "sha1", BaseHeadOID: strings.Repeat("a", 40),
			BaseManifestHash: strings.Repeat("b", 64), BaseSnapshotHash: strings.Repeat("c", 64),
			BaseClean: true, ResultHeadOID: strings.Repeat("d", 40),
			ResultSnapshotHash: strings.Repeat("e", 64), ResultClean: true,
			BaseWarnings: []string{}, ResultWarnings: []string{},
		},
		Parts: []protocol.ResultPackagePartDescriptor{{
			Kind: protocol.ResultPackagePartChangesBundle,
			Size: int64(len(payload)), SHA256: sha256Hex(payload),
		}},
	})
}

func emptyMetadata(t *testing.T, packageID string) protocol.ResultPackageMetadata {
	t.Helper()
	return encodeMetadata(t, protocol.ResultManifest{
		Version: protocol.ResultManifestVersion, PackageID: packageID,
		ControllerID: testControllerID, TreeID: testTreeID,
		SourceAgentID: testWorkerID, SourceDeviceID: testDeviceID,
		ManagedThreadID: testThreadID, TurnID: testTurnID, LifecycleRevision: 7,
		Terminal:   protocol.ResultTerminal{Outcome: protocol.ResultTerminalCompleted},
		CapturedAt: 1_700_000_000,
		Rollout: protocol.ResultRolloutComponent{
			Status: protocol.ResultRolloutCaptureFailed, FailureCode: "rollout_unavailable",
		},
		Workspace: protocol.ResultWorkspaceComponent{
			Status:       protocol.ResultWorkspaceNotManaged,
			BaseWarnings: []string{}, ResultWarnings: []string{},
		},
		Parts: []protocol.ResultPackagePartDescriptor{},
	})
}

func invalidRolloutMetadata(t *testing.T, packageID string) (protocol.ResultPackageMetadata, []byte) {
	t.Helper()
	payload := []byte("not-zstd")
	raw := []byte("rollout")
	return encodeMetadata(t, protocol.ResultManifest{
		Version: protocol.ResultManifestVersion, PackageID: packageID,
		ControllerID: testControllerID, TreeID: testTreeID,
		SourceAgentID: testWorkerID, SourceDeviceID: testDeviceID,
		ManagedThreadID: testThreadID, TurnID: testTurnID, LifecycleRevision: 7,
		Terminal:   protocol.ResultTerminal{Outcome: protocol.ResultTerminalCompleted},
		CapturedAt: 1_700_000_000,
		Rollout: protocol.ResultRolloutComponent{
			Status:  protocol.ResultRolloutAvailable,
			RawSize: int64(len(raw)), RawSHA256: sha256Hex(raw),
		},
		Workspace: protocol.ResultWorkspaceComponent{
			Status:       protocol.ResultWorkspaceNotManaged,
			BaseWarnings: []string{}, ResultWarnings: []string{},
		},
		Parts: []protocol.ResultPackagePartDescriptor{{
			Kind: protocol.ResultPackagePartRollout,
			Size: int64(len(payload)), SHA256: sha256Hex(payload),
		}},
	}), payload
}

func encodeMetadata(t *testing.T, manifest protocol.ResultManifest) protocol.ResultPackageMetadata {
	t.Helper()
	data, descriptor, err := protocol.EncodeResultManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	return protocol.ResultPackageMetadata{Manifest: data, ManifestDescriptor: descriptor}
}

func writePackageDirectory(
	root *os.Root,
	packageID string,
	metadata protocol.ResultPackageMetadata,
	files map[string][]byte,
) error {
	if err := root.Mkdir(packageID, 0o700); err != nil {
		return err
	}
	directory, err := root.OpenRoot(packageID)
	if err != nil {
		return err
	}
	defer directory.Close()
	if err := directory.WriteFile(protocol.ResultManifestFileName, metadata.Manifest, 0o600); err != nil {
		return err
	}
	for name, data := range files {
		if err := directory.WriteFile(name, data, 0o600); err != nil {
			return err
		}
	}
	if err := syncDirectory(directory); err != nil {
		return err
	}
	return syncDirectory(root)
}

func sha256Hex(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func testID(value int) string {
	return fmt.Sprintf("00000000-0000-4000-8000-%012x", value)
}
