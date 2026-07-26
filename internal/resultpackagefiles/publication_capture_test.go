package resultpackagefiles

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
)

func TestPublishResultPackageCommitsDurableOutboxAndSignals(t *testing.T) {
	fixture := newManagerFixture(t)
	defer fixture.close()
	start := time.Unix(1_700_100_000, 0)
	fixture.manager.now = func() time.Time { return start }
	key, metadata, finalizing := prepareNoWorkspaceResult(t, fixture, testID(700), start)

	published, err := fixture.manager.PublishResultPackage(context.Background(), PublishResultPackageRequest{
		Key: key, Metadata: metadata, Parts: []ResultPackagePartSource{},
	})
	if err != nil || published.State != store.ResultOutboxPublishPending {
		t.Fatalf("PublishResultPackage() = %#v, %v", published, err)
	}
	select {
	case <-fixture.manager.ResultPackageChanges():
	default:
		t.Fatal("result package publication did not signal connector work")
	}
	beforeACK, err := fixture.state.GetWorker(context.Background(), key.WorkerKey)
	if err != nil || !reflect.DeepEqual(beforeACK, finalizing) {
		t.Fatalf("worker before metadata ACK = %#v, %v; want %#v", beforeACK, err, finalizing)
	}
	acknowledged, err := fixture.manager.AcknowledgeResultPackageMetadata(
		context.Background(), key, metadata,
	)
	if err != nil || acknowledged.Outbox.State != store.ResultOutboxDeliveryPending ||
		acknowledged.Worker.Status != store.WorkerIdle {
		t.Fatalf("metadata acknowledgement = %#v, %v", acknowledged, err)
	}
}

func TestManagerRecoversPublishedDirectoryBeforeDatabaseCommit(t *testing.T) {
	fixture := newManagerFixture(t)
	defer fixture.close()
	start := time.Unix(1_700_101_000, 0)
	key, metadata, _ := prepareNoWorkspaceResult(t, fixture, testID(710), start)
	if err := writePackageDirectory(
		fixture.manager.outbox, key.PackageID, metadata, map[string][]byte{},
	); err != nil {
		t.Fatal(err)
	}
	if err := fixture.manager.Close(); err != nil {
		t.Fatal(err)
	}
	fixture.manager = fixture.reopen(t)
	stored, err := fixture.state.GetResultOutbox(context.Background(), key)
	if err != nil || stored.State != store.ResultOutboxPublishPending ||
		!protocol.SameResultPackageMetadata(stored.Metadata, metadata) {
		t.Fatalf("recovered result outbox = %#v, %v", stored, err)
	}
}

func TestPublishRejectsMalformedExistingCaptureAsIntegrityFailure(t *testing.T) {
	fixture := newManagerFixture(t)
	defer fixture.close()
	key, metadata, _ := prepareNoWorkspaceResult(
		t, fixture, testID(713), time.Unix(1_700_101_050, 0),
	)
	if err := fixture.manager.outbox.Mkdir(key.PackageID, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := syncDirectory(fixture.manager.outbox); err != nil {
		t.Fatal(err)
	}
	_, err := fixture.manager.PublishResultPackage(
		context.Background(),
		PublishResultPackageRequest{
			Key: key, Metadata: metadata, Parts: []ResultPackagePartSource{},
		},
	)
	if !errors.Is(err, ErrPublicationIntegrity) {
		t.Fatalf("malformed existing capture error = %v, want ErrPublicationIntegrity", err)
	}
}

func TestPublishRetryResyncsAndCommitsCanonicalDiskManifest(t *testing.T) {
	fixture := newManagerFixture(t)
	defer fixture.close()
	start := time.Unix(1_700_101_100, 0)
	key, metadata, _ := prepareNoWorkspaceResult(t, fixture, testID(711), start)
	wantSyncError := errors.New("injected result outbox sync failure")
	syncCalls := 0
	fixture.manager.syncRoot = func(root *os.Root) error {
		if root == fixture.manager.outbox {
			syncCalls++
			if syncCalls == 1 {
				return wantSyncError
			}
		}
		return syncDirectory(root)
	}
	request := PublishResultPackageRequest{
		Key: key, Metadata: metadata, Parts: []ResultPackagePartSource{},
	}
	if _, err := fixture.manager.PublishResultPackage(
		context.Background(), request,
	); !errors.Is(err, wantSyncError) {
		t.Fatalf("publication after rename error = %v", err)
	}
	stored, err := fixture.state.GetResultOutbox(context.Background(), key)
	if err != nil || stored.State != store.ResultOutboxCapturePending {
		t.Fatalf("outbox after failed parent sync = %#v, %v", stored, err)
	}
	request.Metadata = rewriteCapturedAt(t, metadata, start.Add(time.Minute).Unix())
	published, err := fixture.manager.PublishResultPackage(context.Background(), request)
	if err != nil || published.State != store.ResultOutboxPublishPending || syncCalls != 2 ||
		!protocol.SameResultPackageMetadata(published.Metadata, metadata) {
		t.Fatalf("canonical publication retry = %#v, syncCalls=%d, %v", published, syncCalls, err)
	}
}

func TestPublishRetryCommitsCanonicalDiskManifestAfterDatabaseFailure(t *testing.T) {
	fixture := newManagerFixture(t)
	defer fixture.close()
	start := time.Unix(1_700_101_200, 0)
	key, metadata, _ := prepareNoWorkspaceResult(t, fixture, testID(712), start)
	wantCommitError := errors.New("injected result outbox commit failure")
	commit := fixture.manager.commitOutbox
	fixture.manager.commitOutbox = func(
		context.Context,
		store.ResultOutboxKey,
		protocol.ResultPackageMetadata,
		time.Time,
	) (store.ResultOutbox, error) {
		return store.ResultOutbox{}, wantCommitError
	}
	request := PublishResultPackageRequest{
		Key: key, Metadata: metadata, Parts: []ResultPackagePartSource{},
	}
	if _, err := fixture.manager.PublishResultPackage(
		context.Background(), request,
	); !errors.Is(err, wantCommitError) {
		t.Fatalf("publication before database commit error = %v", err)
	}
	fixture.manager.commitOutbox = commit
	request.Metadata = rewriteCapturedAt(t, metadata, start.Add(time.Minute).Unix())
	published, err := fixture.manager.PublishResultPackage(context.Background(), request)
	if err != nil || published.State != store.ResultOutboxPublishPending ||
		!protocol.SameResultPackageMetadata(published.Metadata, metadata) {
		t.Fatalf("database publication retry = %#v, %v", published, err)
	}
}

func TestPublishResultPackageRejectsSymlinkedCaptureSource(t *testing.T) {
	fixture := newManagerFixture(t)
	defer fixture.close()
	start := time.Unix(1_700_102_000, 0)
	key, metadata, worker := prepareNoWorkspaceResult(t, fixture, testID(720), start)
	payload := []byte("not a zstd frame")
	realPath := filepath.Join(fixture.workspace, "rollout-source")
	linkPath := filepath.Join(fixture.workspace, "rollout-link")
	if err := os.WriteFile(realPath, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(realPath, linkPath); err != nil {
		t.Skipf("create symlink: %v", err)
	}
	metadata = metadataWithRolloutPart(t, metadata, int64(len(payload)), sha256Hex(payload))
	_, err := fixture.manager.PublishResultPackage(context.Background(), PublishResultPackageRequest{
		Key: key, Metadata: metadata,
		Parts: []ResultPackagePartSource{{Kind: protocol.ResultPackagePartRollout, Path: linkPath}},
	})
	if err == nil || !strings.Contains(err.Error(), "not a regular file") {
		t.Fatalf("symlinked PublishResultPackage() error = %v", err)
	}
	stored, loadErr := fixture.state.GetWorker(context.Background(), worker.WorkerKey)
	if loadErr != nil || stored.Status != store.WorkerFinalizing {
		t.Fatalf("worker after rejected publish = %#v, %v", stored, loadErr)
	}
}

func prepareNoWorkspaceResult(
	t *testing.T,
	fixture *managerFixture,
	packageID string,
	start time.Time,
) (store.ResultOutboxKey, protocol.ResultPackageMetadata, store.WorkerReservation) {
	t.Helper()
	ctx := context.Background()
	worker, err := fixture.state.ReserveWorkerStart(ctx, store.WorkerReservation{
		WorkerKey: store.WorkerKey{
			ControllerID: testControllerID, TreeID: testTreeID, AgentID: testWorkerID,
		},
		ParentAgentID: testRootAgentID, DeviceID: testDeviceID,
		TaskName: "result", PromptDigest: strings.Repeat("a", 64),
		WorkspacePath: filepath.Join(fixture.workspace, "worker"), ProfileVersion: 1,
	}, 1, start)
	if err != nil {
		t.Fatal(err)
	}
	worker, err = fixture.state.AttachWorkerThread(ctx, worker.WorkerKey, testThreadID, start.Add(time.Second))
	if err == nil {
		worker, err = fixture.state.MarkWorkerReady(ctx, worker.WorkerKey, start.Add(2*time.Second))
	}
	if err != nil {
		t.Fatal(err)
	}
	intent, _, err := fixture.state.PrepareWorkerTurnStartIntent(ctx, store.PrepareWorkerTurnStartIntentRequest{
		WorkerKey: worker.WorkerKey, IntentID: testID(799), DeviceID: testDeviceID,
		ManagedThreadID: testThreadID, PackageID: packageID,
		Rollout: store.WorkerRolloutLocator{
			Status: store.WorkerRolloutUnavailable, FailureCode: "rollout_unavailable",
		},
		ReservationLimitBytes: protocol.MaximumResultManifestBytes + protocol.MaximumResultRolloutBytes,
	}, start.Add(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	resolution, err := fixture.state.BindWorkerTurnStartIntent(
		ctx, worker.WorkerKey, intent.IntentID, testTurnID, start.Add(4*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	finalization, err := fixture.state.BeginWorkerResultFinalization(
		ctx, worker.WorkerKey, testTurnID, store.WorkerIdle, "", start.Add(5*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	key := store.ResultOutboxKey{
		WorkerKey: worker.WorkerKey, SourceDeviceID: testDeviceID, PackageID: packageID,
	}
	manifest := protocol.ResultManifest{
		Version: protocol.ResultManifestVersion, PackageID: packageID,
		ControllerID: testControllerID, TreeID: testTreeID,
		SourceAgentID: testWorkerID, SourceDeviceID: testDeviceID,
		ManagedThreadID: testThreadID, TurnID: testTurnID,
		LifecycleRevision: finalization.Worker.Revision,
		Terminal:          protocol.ResultTerminal{Outcome: protocol.ResultTerminalCompleted},
		CapturedAt:        start.Add(5 * time.Second).Unix(),
		Rollout: protocol.ResultRolloutComponent{
			Status: protocol.ResultRolloutCaptureFailed, FailureCode: "rollout_unavailable",
		},
		Workspace: protocol.ResultWorkspaceComponent{
			Status:       protocol.ResultWorkspaceNotManaged,
			BaseWarnings: []string{}, ResultWarnings: []string{},
		},
		Parts: []protocol.ResultPackagePartDescriptor{},
	}
	data, descriptor, err := protocol.EncodeResultManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if resolution.Intent.PackageID != packageID {
		t.Fatal("bound result package identity changed")
	}
	return key, protocol.ResultPackageMetadata{
		Manifest: data, ManifestDescriptor: descriptor,
	}, finalization.Worker
}

func metadataWithRolloutPart(
	t *testing.T,
	metadata protocol.ResultPackageMetadata,
	size int64,
	digest string,
) protocol.ResultPackageMetadata {
	t.Helper()
	manifest, err := metadata.DecodeManifest()
	if err != nil {
		t.Fatal(err)
	}
	manifest.Rollout = protocol.ResultRolloutComponent{
		Status: protocol.ResultRolloutAvailable, RawSize: 1,
		RawSHA256: strings.Repeat("b", 64),
	}
	manifest.Parts = []protocol.ResultPackagePartDescriptor{{
		Kind: protocol.ResultPackagePartRollout, Size: size, SHA256: digest,
	}}
	data, descriptor, err := protocol.EncodeResultManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	return protocol.ResultPackageMetadata{Manifest: data, ManifestDescriptor: descriptor}
}

func rewriteCapturedAt(
	t *testing.T,
	metadata protocol.ResultPackageMetadata,
	capturedAt int64,
) protocol.ResultPackageMetadata {
	t.Helper()
	manifest, err := metadata.DecodeManifest()
	if err != nil {
		t.Fatal(err)
	}
	manifest.CapturedAt = capturedAt
	data, descriptor, err := protocol.EncodeResultManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	return protocol.ResultPackageMetadata{Manifest: data, ManifestDescriptor: descriptor}
}

func TestMetadataAcknowledgementRejectsChangedMetadataWithoutReleasingWorker(t *testing.T) {
	fixture := newManagerFixture(t)
	defer fixture.close()
	start := time.Unix(1_700_103_000, 0)
	key, metadata, finalizing := prepareNoWorkspaceResult(t, fixture, testID(730), start)
	if _, err := fixture.manager.PublishResultPackage(context.Background(), PublishResultPackageRequest{
		Key: key, Metadata: metadata, Parts: []ResultPackagePartSource{},
	}); err != nil {
		t.Fatal(err)
	}
	changed := append([]byte(nil), metadata.Manifest...)
	changed[len(changed)-2] ^= 1
	wrong := protocol.ResultPackageMetadata{
		Manifest: changed, ManifestDescriptor: metadata.ManifestDescriptor,
	}
	if _, err := fixture.manager.AcknowledgeResultPackageMetadata(
		context.Background(), key, wrong,
	); err == nil {
		t.Fatal("metadata acknowledgement accepted changed bytes")
	}
	worker, err := fixture.state.GetWorker(context.Background(), key.WorkerKey)
	if err != nil || !reflect.DeepEqual(worker, finalizing) {
		t.Fatalf("worker after rejected metadata = %#v, %v; want %#v", worker, err, finalizing)
	}
}
