package resultpackagefiles

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
)

func TestResultPackagePublicationRequiresExactMetadataBeforeAdvancing(t *testing.T) {
	fixture := newManagerFixture(t)
	defer fixture.close()
	start := time.Unix(1_700_000_000, 0)
	fixture.manager.now = func() time.Time { return start.Add(10 * time.Second) }
	packageID := testID(320)
	metadata, worker := fixture.publishOutbox(t, packageID, []byte("result bundle"), start)
	key := store.ResultOutboxKey{
		WorkerKey: worker.WorkerKey, SourceDeviceID: testDeviceID, PackageID: packageID,
	}

	publications, err := fixture.manager.ListPendingResultPublications(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(publications) != 1 || publications[0].ResultOutboxKey != key ||
		publications[0].State != store.ResultOutboxPublishPending ||
		!protocol.SameResultPackageMetadata(publications[0].Metadata, metadata) {
		t.Fatalf("pending publications = %#v", publications)
	}

	manifest, err := metadata.DecodeManifest()
	if err != nil {
		t.Fatal(err)
	}
	manifest.CapturedAt++
	wrongData, wrongDescriptor, err := protocol.EncodeResultManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	wrongMetadata := protocol.ResultPackageMetadata{
		Manifest: wrongData, ManifestDescriptor: wrongDescriptor,
	}
	if _, err := fixture.manager.AcknowledgeResultPackageMetadata(
		context.Background(), key, wrongMetadata,
	); !errors.Is(err, store.ErrResultPackageConflict) {
		t.Fatalf("wrong metadata acknowledgement error = %v", err)
	}
	stored, err := fixture.state.GetResultOutbox(context.Background(), key)
	if err != nil || stored.State != store.ResultOutboxPublishPending {
		t.Fatalf("outbox after wrong metadata = %#v, %v", stored, err)
	}
	select {
	case <-fixture.manager.ResultPackageChanges():
		t.Fatal("wrong metadata acknowledgement signaled a publication change")
	default:
	}

	acknowledged, err := fixture.manager.AcknowledgeResultPackageMetadata(
		context.Background(), key, metadata,
	)
	if err != nil || acknowledged.State != store.ResultOutboxDeliveryPending ||
		!protocol.SameResultPackageMetadata(acknowledged.Metadata, metadata) {
		t.Fatalf("metadata acknowledgement = %#v, %v", acknowledged, err)
	}
	select {
	case <-fixture.manager.ResultPackageChanges():
	default:
		t.Fatal("metadata acknowledgement did not signal a publication change")
	}

	replayed, err := fixture.manager.AcknowledgeResultPackageMetadata(
		context.Background(), key, metadata,
	)
	if err != nil || replayed.State != store.ResultOutboxDeliveryPending {
		t.Fatalf("metadata acknowledgement replay = %#v, %v", replayed, err)
	}
}
