package store

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/protocol"
)

func TestResultOutboxLifecycleIsIdempotentAndReleasesUnusedReservation(t *testing.T) {
	ctx := context.Background()
	packageID := changesTestID(5001)
	state, worker, threadID, turnID := newFinalizingResultWorker(t, packageID)
	defer state.Close()
	key := ResultOutboxKey{
		WorkerKey: worker.WorkerKey, SourceDeviceID: worker.DeviceID, PackageID: packageID,
	}
	now := time.Unix(1_700_000_100, 0)
	reserved, err := state.ReserveResultOutbox(
		ctx, key, protocol.MaximumResultPackageBytes, now,
	)
	if err != nil {
		t.Fatal(err)
	}
	replayedReservation, err := state.ReserveResultOutbox(
		ctx, key, protocol.MaximumResultPackageBytes, now.Add(time.Second),
	)
	if err != nil || !reflect.DeepEqual(replayedReservation, reserved) {
		t.Fatalf("reservation replay = %#v, %v; want %#v", replayedReservation, err, reserved)
	}
	metadata := resultMetadata(t, key, threadID, turnID, 21)
	packageBytes, err := resultPackageBytes(metadata)
	if err != nil {
		t.Fatal(err)
	}
	published, err := state.CommitResultOutboxCapture(ctx, key, metadata, now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if published.State != ResultOutboxPublishPending || published.PackageBytes != packageBytes ||
		published.ReservedBytes != packageBytes ||
		published.ReservationLimitBytes != protocol.MaximumResultPackageBytes {
		t.Fatalf("published result outbox = %#v", published)
	}
	retention, err := state.GetResultOutboxRetention(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if retention != (ResultPackageRetention{Count: 1, Bytes: packageBytes}) {
		t.Fatalf("retention = %#v, want exact package bytes %d", retention, packageBytes)
	}
	publishedReplay, err := state.CommitResultOutboxCapture(ctx, key, metadata, now.Add(3*time.Second))
	if err != nil || !reflect.DeepEqual(publishedReplay, published) {
		t.Fatalf("capture replay = %#v, %v; want %#v", publishedReplay, err, published)
	}
	conflict := resultMetadata(t, key, threadID, turnID, 22)
	if _, err := state.CommitResultOutboxCapture(ctx, key, conflict, now.Add(3*time.Second)); !errors.Is(
		err, ErrResultPackageConflict,
	) {
		t.Fatalf("conflicting capture error = %v, want ErrResultPackageConflict", err)
	}

	publications, err := state.ListPendingResultPublications(ctx, key.ControllerID, key.SourceDeviceID, 10)
	if err != nil || len(publications) != 1 || publications[0].PackageID != key.PackageID {
		t.Fatalf("pending publications = %#v, %v", publications, err)
	}
	delivery, err := state.AcknowledgeResultOutboxMetadata(ctx, key, metadata, now.Add(4*time.Second))
	if err != nil || delivery.State != ResultOutboxDeliveryPending {
		t.Fatalf("metadata acknowledgement = %#v, %v", delivery, err)
	}
	if _, err := state.AcknowledgeResultOutboxMetadata(ctx, key, metadata, now.Add(5*time.Second)); err != nil {
		t.Fatalf("metadata acknowledgement replay: %v", err)
	}
	if delivered, err := state.ListDeliveredResultOutboxes(ctx, key.ControllerID, key.SourceDeviceID, 10); err != nil || len(delivered) != 0 {
		t.Fatalf("delivery-pending package was GC eligible: %#v, %v", delivered, err)
	}
	delivered, err := state.AcknowledgeResultOutboxDelivery(ctx, key, 9, now.Add(6*time.Second))
	if err != nil || delivered.State != ResultOutboxDelivered || delivered.DeliverySequence != 9 {
		t.Fatalf("delivery acknowledgement = %#v, %v", delivered, err)
	}
	if _, err := state.AcknowledgeResultOutboxDelivery(ctx, key, 9, now.Add(7*time.Second)); err != nil {
		t.Fatalf("delivery acknowledgement replay: %v", err)
	}
	if _, err := state.AcknowledgeResultOutboxDelivery(ctx, key, 10, now.Add(7*time.Second)); !errors.Is(
		err, ErrResultPackageConflict,
	) {
		t.Fatalf("different delivery sequence error = %v, want ErrResultPackageConflict", err)
	}
	eligible, err := state.ListDeliveredResultOutboxes(ctx, key.ControllerID, key.SourceDeviceID, 10)
	if err != nil || len(eligible) != 1 || eligible[0].PackageID != key.PackageID {
		t.Fatalf("delivered GC list = %#v, %v", eligible, err)
	}
	if err := state.DeleteDeliveredResultOutbox(ctx, key); err != nil {
		t.Fatal(err)
	}
	if err := state.DeleteDeliveredResultOutbox(ctx, key); err != nil {
		t.Fatalf("delete replay: %v", err)
	}
}

func TestResultOutboxEnforcesWorkerAuthorityAndIdentity(t *testing.T) {
	ctx := context.Background()
	packageID := changesTestID(5010)
	state, worker, threadID, turnID := newFinalizingResultWorker(t, packageID)
	defer state.Close()
	key := ResultOutboxKey{
		WorkerKey: worker.WorkerKey, SourceDeviceID: worker.DeviceID, PackageID: packageID,
	}
	now := time.Unix(1_700_001_000, 0)
	wrongDevice := key
	wrongDevice.SourceDeviceID = changesTestID(5011)
	wrongDevice.PackageID = changesTestID(5015)
	if _, err := state.ReserveResultOutbox(ctx, wrongDevice, 1024, now); !errors.Is(
		err, ErrResultPackageAuthority,
	) {
		t.Fatalf("wrong device reservation error = %v", err)
	}
	if _, err := state.ReserveResultOutbox(ctx, key, protocol.MaximumResultPackageBytes, now); err != nil {
		t.Fatal(err)
	}
	metadata := resultMetadata(t, key, threadID, turnID, 21)
	wrongPackage := key
	wrongPackage.PackageID = changesTestID(5014)
	if _, err := state.ReserveResultOutbox(
		ctx, wrongPackage, protocol.MaximumResultPackageBytes, now,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := state.CommitResultOutboxCapture(
		ctx, wrongPackage, resultMetadata(t, wrongPackage, threadID, turnID, 21), now,
	); !errors.Is(err, ErrResultPackageAuthority) {
		t.Fatalf("unbound package capture error = %v, want ErrResultPackageAuthority", err)
	}
	wrongThread := resultMetadata(t, key, changesTestID(5012), turnID, 21)
	if _, err := state.CommitResultOutboxCapture(ctx, key, wrongThread, now); !errors.Is(
		err, ErrResultPackageAuthority,
	) {
		t.Fatalf("wrong thread capture error = %v", err)
	}
	wrongRevision := rewriteResultMetadata(t, metadata, func(manifest *protocol.ResultManifest) {
		manifest.LifecycleRevision++
	})
	if _, err := state.CommitResultOutboxCapture(ctx, key, wrongRevision, now); !errors.Is(
		err, ErrResultPackageAuthority,
	) {
		t.Fatalf("wrong lifecycle revision error = %v", err)
	}
	wrongTerminal := rewriteResultMetadata(t, metadata, func(manifest *protocol.ResultManifest) {
		manifest.Terminal = protocol.ResultTerminal{
			Outcome: protocol.ResultTerminalFailed, FailureCode: "worker_failed",
		}
	})
	if _, err := state.CommitResultOutboxCapture(ctx, key, wrongTerminal, now); !errors.Is(
		err, ErrResultPackageAuthority,
	) {
		t.Fatalf("wrong terminal authority error = %v", err)
	}
	wrongIdentity := key
	wrongIdentity.AgentID = changesTestID(5013)
	if _, err := state.ReserveResultOutbox(
		ctx, wrongIdentity, protocol.MaximumResultPackageBytes, now,
	); !errors.Is(err, ErrResultPackageConflict) {
		t.Fatalf("reused package identity error = %v, want ErrResultPackageConflict", err)
	}
	if _, err := state.CommitResultOutboxCapture(ctx, key, metadata, now); err != nil {
		t.Fatal(err)
	}
	stored, err := state.GetResultOutbox(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	metadata.Manifest[0] ^= 1
	if reflect.DeepEqual(stored.Metadata.Manifest, metadata.Manifest) {
		t.Fatal("stored manifest aliased caller bytes")
	}
}

func TestResultOutboxAdmissionEnforcesCountAndByteBudgets(t *testing.T) {
	t.Run("count", func(t *testing.T) {
		ctx := context.Background()
		state, worker, _, _ := newFinalizingResultWorker(t, "")
		defer state.Close()
		now := time.Unix(1_700_002_000, 0)
		for index := 0; index < MaximumPeerResultPackages; index++ {
			key := ResultOutboxKey{
				WorkerKey: worker.WorkerKey, SourceDeviceID: worker.DeviceID,
				PackageID: changesTestID(5100 + index),
			}
			if _, err := state.ReserveResultOutbox(ctx, key, 1, now); err != nil {
				t.Fatalf("reserve package %d: %v", index, err)
			}
		}
		overflow := ResultOutboxKey{
			WorkerKey: worker.WorkerKey, SourceDeviceID: worker.DeviceID,
			PackageID: changesTestID(5200),
		}
		if _, err := state.ReserveResultOutbox(ctx, overflow, 1, now); !errors.Is(
			err, ErrResultPackageQuota,
		) {
			t.Fatalf("count overflow error = %v, want ErrResultPackageQuota", err)
		}
	})

	t.Run("bytes", func(t *testing.T) {
		ctx := context.Background()
		state, worker, _, _ := newFinalizingResultWorker(t, "")
		defer state.Close()
		now := time.Unix(1_700_003_000, 0)
		for index := 0; index < 3; index++ {
			key := ResultOutboxKey{
				WorkerKey: worker.WorkerKey, SourceDeviceID: worker.DeviceID,
				PackageID: changesTestID(5300 + index),
			}
			if _, err := state.ReserveResultOutbox(
				ctx, key, protocol.MaximumResultPackageBytes, now,
			); err != nil {
				t.Fatalf("reserve maximum package %d: %v", index, err)
			}
		}
		overflow := ResultOutboxKey{
			WorkerKey: worker.WorkerKey, SourceDeviceID: worker.DeviceID,
			PackageID: changesTestID(5310),
		}
		if _, err := state.ReserveResultOutbox(
			ctx, overflow, protocol.MaximumResultPackageBytes, now,
		); !errors.Is(err, ErrResultPackageQuota) {
			t.Fatalf("byte overflow error = %v, want ErrResultPackageQuota", err)
		}
	})
}

func newFinalizingResultWorker(
	t *testing.T,
	packageID string,
) (*PeerStore, WorkerReservation, string, string) {
	t.Helper()
	ctx := context.Background()
	state, err := OpenPeer(ctx, filepath.Join(t.TempDir(), "state", "peer.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	worker := workerReservation(t, changesTestID(5401), "result")
	now := time.Unix(1_700_000_000, 0)
	worker, err = state.ReserveWorkerStart(ctx, worker, 1, now)
	if err != nil {
		state.Close()
		t.Fatal(err)
	}
	threadID := changesTestID(5402)
	worker, err = state.AttachWorkerThread(ctx, worker.WorkerKey, threadID, now.Add(time.Second))
	if err != nil {
		state.Close()
		t.Fatal(err)
	}
	worker, err = state.MarkWorkerReady(ctx, worker.WorkerKey, now.Add(2*time.Second))
	if err != nil {
		state.Close()
		t.Fatal(err)
	}
	turnID := changesTestID(5403)
	if packageID == "" {
		worker, err = state.MarkWorkerRunning(ctx, worker.WorkerKey, turnID, now.Add(3*time.Second))
		if err != nil {
			state.Close()
			t.Fatal(err)
		}
	} else {
		request := unavailableTurnIntentRequest(worker, changesTestID(5404), packageID, "")
		if _, _, err := state.PrepareWorkerTurnStartIntent(ctx, request, now.Add(3*time.Second)); err != nil {
			state.Close()
			t.Fatal(err)
		}
		resolution, err := state.BindWorkerTurnStartIntent(
			ctx, worker.WorkerKey, request.IntentID, turnID, now.Add(4*time.Second),
		)
		if err != nil {
			state.Close()
			t.Fatal(err)
		}
		worker = resolution.Worker
	}
	if _, err := state.db.ExecContext(ctx, `
UPDATE worker_reservations SET
	status = 'finalizing', final_target_status = 'idle', final_failure_code = '', revision = 7, updated_at = ?
WHERE controller_id = ? AND tree_id = ? AND agent_id = ? AND status = 'running'
`, now.Add(4*time.Second).Unix(), worker.ControllerID, worker.TreeID, worker.AgentID); err != nil {
		state.Close()
		t.Fatal(err)
	}
	if _, err := state.db.ExecContext(ctx, `UPDATE peer_metadata SET worker_revision = 7`); err != nil {
		state.Close()
		t.Fatal(err)
	}
	worker, err = state.GetWorker(ctx, worker.WorkerKey)
	if err != nil {
		state.Close()
		t.Fatal(err)
	}
	return state, worker, threadID, turnID
}

func resultMetadata(
	t *testing.T,
	key ResultOutboxKey,
	threadID, turnID string,
	rolloutSize int64,
) protocol.ResultPackageMetadata {
	t.Helper()
	manifest := protocol.ResultManifest{
		Version: protocol.ResultManifestVersion, PackageID: key.PackageID,
		ControllerID: key.ControllerID, TreeID: key.TreeID,
		SourceAgentID: key.AgentID, SourceDeviceID: key.SourceDeviceID,
		ManagedThreadID: threadID, TurnID: turnID, LifecycleRevision: 7,
		Terminal:   protocol.ResultTerminal{Outcome: protocol.ResultTerminalCompleted},
		CapturedAt: 1_700_000_000,
		Workspace: protocol.ResultWorkspaceComponent{
			Status: protocol.ResultWorkspaceNotManaged, BaseWarnings: []string{}, ResultWarnings: []string{},
		},
		Parts: []protocol.ResultPackagePartDescriptor{},
	}
	if rolloutSize > 0 {
		manifest.Rollout = protocol.ResultRolloutComponent{
			Status: protocol.ResultRolloutAvailable, RawSize: 1,
			RawSHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		}
		manifest.Parts = []protocol.ResultPackagePartDescriptor{{
			Kind: protocol.ResultPackagePartRollout, Size: rolloutSize,
			SHA256: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		}}
	} else {
		manifest.Rollout = protocol.ResultRolloutComponent{
			Status: protocol.ResultRolloutCaptureFailed, FailureCode: "rollout_unavailable",
		}
	}
	data, descriptor, err := protocol.EncodeResultManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	return protocol.ResultPackageMetadata{Manifest: data, ManifestDescriptor: descriptor}
}

func rewriteResultMetadata(
	t *testing.T,
	metadata protocol.ResultPackageMetadata,
	rewrite func(*protocol.ResultManifest),
) protocol.ResultPackageMetadata {
	t.Helper()
	manifest, err := metadata.DecodeManifest()
	if err != nil {
		t.Fatal(err)
	}
	rewrite(&manifest)
	data, descriptor, err := protocol.EncodeResultManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	return protocol.ResultPackageMetadata{Manifest: data, ManifestDescriptor: descriptor}
}
