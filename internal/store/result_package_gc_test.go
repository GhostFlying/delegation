package store

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/protocol"
)

type resultPackageGCSeedAuthority struct {
	ControllerID string
	TreeID       string
	RootDeviceID string
	Source       control.PrincipalIdentity
}

func TestResultPackageRetentionOrderMatchesRootInboxEviction(t *testing.T) {
	ctx := context.Background()
	fixture := prepareResultPackageFixture(t, false, true)
	peer := openResultPeer(t)
	defer peer.Close()
	authority := ResultInboxAuthority{
		ControllerID: fixture.Root.ControllerID,
		TreeID:       fixture.Root.TreeID,
		RootAgentID:  fixture.Root.AgentID,
		RootDeviceID: fixture.Root.DeviceID,
	}
	const publishedAt = int64(5_000)
	packageIDs := make([]string, 0, MaximumPeerResultPackages+1)
	for index := range MaximumPeerResultPackages + 1 {
		packageID := changesTestID(290_000 + index)
		managedThreadID := changesTestID(291_000 + index)
		turnID := changesTestID(292_000 + index)
		metadata := statusResultMetadata(
			t, authority.ControllerID, authority.TreeID, fixture.Worker.AgentID,
			fixture.Worker.DeviceID, packageID, managedThreadID, turnID, false,
		)
		insertRetentionAlignmentPackage(
			t, fixture.Registry, peer, authority, fixture.Worker, metadata,
			packageID, managedThreadID, turnID, publishedAt, index,
		)
		packageIDs = append(packageIDs, packageID)
	}

	oldest, err := peer.ListAvailableResultInboxes(
		ctx, authority.ControllerID, authority.RootDeviceID, 1,
	)
	if err != nil || len(oldest) != 1 || oldest[0].PackageID != packageIDs[0] {
		t.Fatalf("root eviction candidate = %#v, error %v; want %s", oldest, err, packageIDs[0])
	}
	compaction, err := fixture.Registry.CompactReleasedResultPackageDetails(
		ctx, authority.ControllerID, MaximumResultPackageDetailCompactionBatch,
	)
	if err != nil || compaction != (ResultPackageDetailCompaction{Compacted: 1}) {
		t.Fatalf("broker compaction = %#v, error %v", compaction, err)
	}
	assertResultPackageGCRowExists(
		t,
		fixture.Registry,
		resultPackageGCSeedAuthority{
			ControllerID: authority.ControllerID,
			TreeID:       authority.TreeID,
			RootDeviceID: authority.RootDeviceID,
			Source:       fixture.Worker,
		},
		packageIDs[0],
		false,
	)
}

func TestCompactReleasedResultPackageDetailsRetainsNewestPerRootAndController(t *testing.T) {
	ctx := context.Background()
	fixture := prepareResultPackageFixture(t, false, true)
	firstAuthority := resultPackageGCSeedAuthority{
		ControllerID: fixture.Root.ControllerID,
		TreeID:       fixture.Root.TreeID,
		RootDeviceID: fixture.Root.DeviceID,
		Source:       fixture.Worker,
	}
	firstPackages := seedDeliveredResultPackages(
		t, fixture.Registry, firstAuthority, 300_000, 1, 1, 65,
		func(int) bool { return true },
	)

	_, secondRoot, err := fixture.Registry.EnsureRootTree(
		ctx, fixture.Root.ControllerID, changesTestID(301_000), agentSpawnTargetID,
		time.Unix(100, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	secondWorker := control.NewWorkerPrincipal(
		fixture.Root.ControllerID,
		secondRoot.TreeID,
		changesTestID(301_001),
		secondRoot.AgentID,
		fixture.Root.DeviceID,
	)
	insertStoredWorker(t, fixture.Registry, secondWorker)
	secondAuthority := resultPackageGCSeedAuthority{
		ControllerID: secondRoot.ControllerID,
		TreeID:       secondRoot.TreeID,
		RootDeviceID: secondRoot.DeviceID,
		Source:       secondWorker.Identity(),
	}
	secondPackages := seedDeliveredResultPackages(
		t, fixture.Registry, secondAuthority, 310_000, 1, 66, 64,
		func(int) bool { return true },
	)

	for _, deviceID := range []string{testDeviceID, agentSpawnTargetID} {
		if _, err := fixture.Registry.RegisterTrustedDevice(
			ctx,
			deviceDescriptor(deviceSecondControllerID, deviceID),
			time.Unix(110, 0),
		); err != nil {
			t.Fatal(err)
		}
	}
	_, otherControllerRoot, err := fixture.Registry.EnsureRootTree(
		ctx,
		deviceSecondControllerID,
		changesTestID(302_000),
		testDeviceID,
		time.Unix(111, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	otherControllerWorker := control.NewWorkerPrincipal(
		deviceSecondControllerID,
		otherControllerRoot.TreeID,
		changesTestID(302_001),
		otherControllerRoot.AgentID,
		agentSpawnTargetID,
	)
	insertStoredWorker(t, fixture.Registry, otherControllerWorker)
	otherControllerAuthority := resultPackageGCSeedAuthority{
		ControllerID: otherControllerRoot.ControllerID,
		TreeID:       otherControllerRoot.TreeID,
		RootDeviceID: otherControllerRoot.DeviceID,
		Source:       otherControllerWorker.Identity(),
	}
	otherControllerPackages := seedDeliveredResultPackages(
		t, fixture.Registry, otherControllerAuthority, 320_000, 1, 1, 65,
		func(int) bool { return true },
	)

	compaction, err := fixture.Registry.CompactReleasedResultPackageDetails(
		ctx, fixture.Root.ControllerID, 128,
	)
	if err != nil {
		t.Fatal(err)
	}
	if want := (ResultPackageDetailCompaction{Compacted: 1}); compaction != want {
		t.Fatalf("compaction = %#v, want %#v", compaction, want)
	}
	assertResultPackageGCWindow(
		t, fixture.Registry, fixture.Root.ControllerID, fixture.Root.DeviceID, 64, 2, 65,
	)
	assertResultPackageGCWindow(
		t, fixture.Registry, fixture.Root.ControllerID, secondRoot.DeviceID, 64, 66, 129,
	)
	assertResultPackageGCWindow(
		t, fixture.Registry, deviceSecondControllerID, otherControllerRoot.DeviceID, 65, 1, 65,
	)
	assertResultPackageGCRowExists(t, fixture.Registry, firstAuthority, firstPackages[0], false)
	assertResultPackageGCRowExists(t, fixture.Registry, secondAuthority, secondPackages[0], true)
	assertResultPackageGCRowExists(
		t, fixture.Registry, otherControllerAuthority, otherControllerPackages[0], true,
	)
	assertResultPackageGCCompactedLifetime(t, fixture.Registry, fixture.Root.ControllerID, 1)
	assertResultPackageGCCompactedLifetime(t, fixture.Registry, deviceSecondControllerID, 0)

	replayed, err := fixture.Registry.CompactReleasedResultPackageDetails(
		ctx, fixture.Root.ControllerID, 128,
	)
	if err != nil || replayed != (ResultPackageDetailCompaction{}) {
		t.Fatalf("compaction replay = %#v, error %v", replayed, err)
	}
	assertResultPackageGCCompactedLifetime(t, fixture.Registry, fixture.Root.ControllerID, 1)

	otherControllerCompaction, err := fixture.Registry.CompactReleasedResultPackageDetails(
		ctx, deviceSecondControllerID, 128,
	)
	if err != nil || otherControllerCompaction != (ResultPackageDetailCompaction{Compacted: 1}) {
		t.Fatalf("other controller compaction = %#v, error %v", otherControllerCompaction, err)
	}
	assertResultPackageGCWindow(
		t, fixture.Registry, deviceSecondControllerID, otherControllerRoot.DeviceID, 64, 2, 65,
	)
	assertResultPackageGCCompactedLifetime(t, fixture.Registry, deviceSecondControllerID, 1)
}

func TestCompactReleasedResultPackageDetailsProtectsUnreleasedRows(t *testing.T) {
	ctx := context.Background()
	fixture := prepareResultPackageFixture(t, false, true)
	authority := resultPackageGCSeedAuthority{
		ControllerID: fixture.Root.ControllerID,
		TreeID:       fixture.Root.TreeID,
		RootDeviceID: fixture.Root.DeviceID,
		Source:       fixture.Worker,
	}
	packages := seedDeliveredResultPackages(
		t, fixture.Registry, authority, 400_000, 1, 1, 66,
		func(index int) bool { return index != 0 },
	)

	compaction, err := fixture.Registry.CompactReleasedResultPackageDetails(
		ctx, fixture.Root.ControllerID, 128,
	)
	if err != nil || compaction != (ResultPackageDetailCompaction{Compacted: 1}) {
		t.Fatalf("protected compaction = %#v, error %v", compaction, err)
	}
	assertResultPackageGCRowExists(t, fixture.Registry, authority, packages[0], true)
	assertResultPackageGCRowExists(t, fixture.Registry, authority, packages[1], false)
	assertResultPackageGCWindow(
		t, fixture.Registry, fixture.Root.ControllerID, fixture.Root.DeviceID, 65, 1, 66,
	)

	if _, err := fixture.Registry.MarkResultPackageSourceReleased(
		ctx,
		fixture.Worker.DeviceID,
		fixture.Worker,
		packages[0],
		time.Unix(10_000, 0),
	); err != nil {
		t.Fatal(err)
	}
	compaction, err = fixture.Registry.CompactReleasedResultPackageDetails(
		ctx, fixture.Root.ControllerID, 128,
	)
	if err != nil || compaction != (ResultPackageDetailCompaction{Compacted: 1}) {
		t.Fatalf("released compaction = %#v, error %v", compaction, err)
	}
	assertResultPackageGCRowExists(t, fixture.Registry, authority, packages[0], false)
	assertResultPackageGCWindow(
		t, fixture.Registry, fixture.Root.ControllerID, fixture.Root.DeviceID, 64, 3, 66,
	)
	assertResultPackageGCCompactedLifetime(t, fixture.Registry, fixture.Root.ControllerID, 2)
}

func TestCompactReleasedResultPackageDetailsBatchesAndReportsMore(t *testing.T) {
	ctx := context.Background()
	fixture := prepareResultPackageFixture(t, false, true)
	authority := resultPackageGCSeedAuthority{
		ControllerID: fixture.Root.ControllerID,
		TreeID:       fixture.Root.TreeID,
		RootDeviceID: fixture.Root.DeviceID,
		Source:       fixture.Worker,
	}
	seedDeliveredResultPackages(
		t, fixture.Registry, authority, 500_000, 1, 1, 200,
		func(int) bool { return true },
	)

	first, err := fixture.Registry.CompactReleasedResultPackageDetails(
		ctx, fixture.Root.ControllerID, 128,
	)
	if err != nil {
		t.Fatal(err)
	}
	if want := (ResultPackageDetailCompaction{Compacted: 128, More: true}); first != want {
		t.Fatalf("first compaction = %#v, want %#v", first, want)
	}
	assertResultPackageGCWindow(
		t, fixture.Registry, fixture.Root.ControllerID, fixture.Root.DeviceID, 72, 129, 200,
	)

	second, err := fixture.Registry.CompactReleasedResultPackageDetails(
		ctx, fixture.Root.ControllerID, 128,
	)
	if err != nil {
		t.Fatal(err)
	}
	if want := (ResultPackageDetailCompaction{Compacted: 8}); second != want {
		t.Fatalf("second compaction = %#v, want %#v", second, want)
	}
	assertResultPackageGCWindow(
		t, fixture.Registry, fixture.Root.ControllerID, fixture.Root.DeviceID, 64, 137, 200,
	)
	assertResultPackageGCCompactedLifetime(t, fixture.Registry, fixture.Root.ControllerID, 136)
}

func TestCompactReleasedResultPackageDetailsValidatesScopeAndLimit(t *testing.T) {
	fixture := prepareResultPackageFixture(t, false, true)
	tests := []struct {
		name         string
		controllerID string
		limit        int
	}{
		{name: "invalid controller", controllerID: "not-an-id", limit: 1},
		{name: "zero limit", controllerID: fixture.Root.ControllerID, limit: 0},
		{name: "negative limit", controllerID: fixture.Root.ControllerID, limit: -1},
		{name: "over maximum limit", controllerID: fixture.Root.ControllerID, limit: 129},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := fixture.Registry.CompactReleasedResultPackageDetails(
				context.Background(), test.controllerID, test.limit,
			); err == nil {
				t.Fatal("compaction accepted invalid input")
			}
		})
	}

	compaction, err := fixture.Registry.CompactReleasedResultPackageDetails(
		context.Background(), fixture.Root.ControllerID, 128,
	)
	if err != nil || compaction != (ResultPackageDetailCompaction{}) {
		t.Fatalf("maximum compaction limit = %#v, error %v", compaction, err)
	}
	assertResultPackageGCCompactedLifetime(t, fixture.Registry, fixture.Root.ControllerID, 0)
}

func seedDeliveredResultPackages(
	t *testing.T,
	registry *Store,
	authority resultPackageGCSeedAuthority,
	idBase, sequenceStart, ordinalStart, count int,
	isReleased func(index int) bool,
) []string {
	t.Helper()
	ctx := context.Background()
	transaction, err := registry.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer transaction.Rollback()

	packageIDs := make([]string, 0, count)
	releasedCount := 0
	for index := range count {
		packageID := changesTestID(idBase + index)
		managedThreadID := changesTestID(idBase + 10_000 + index)
		turnID := changesTestID(idBase + 20_000 + index)
		metadata := statusResultMetadata(
			t,
			authority.ControllerID,
			authority.TreeID,
			authority.Source.AgentID,
			authority.Source.DeviceID,
			packageID,
			managedThreadID,
			turnID,
			false,
		)
		ordinal := ordinalStart + index
		sequence := sequenceStart + index
		publishedAt := int64(1_000 + ordinal)
		deliveredAt := publishedAt + 1
		acknowledgedAt := deliveredAt + 1
		releasedAt := int64(0)
		if isReleased(index) {
			releasedAt = acknowledgedAt + 1
			releasedCount++
		}
		if _, err := transaction.ExecContext(ctx, `
INSERT INTO result_packages(
	controller_id, tree_id, package_id, source_agent_id, source_device_id,
	managed_thread_id, turn_id, lifecycle_revision, root_device_id,
	manifest_bytes, manifest_size, manifest_sha256, state, result_sequence,
	root_retention_ordinal, published_at, delivered_at, source_acknowledged_at,
	source_released_at
) VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, 'delivered', ?, ?, ?, ?, ?, ?)
`, authority.ControllerID, authority.TreeID, packageID,
			authority.Source.AgentID, authority.Source.DeviceID,
			managedThreadID, turnID, authority.RootDeviceID,
			metadata.Manifest, metadata.ManifestDescriptor.Size,
			metadata.ManifestDescriptor.SHA256, sequence, ordinal, publishedAt,
			deliveredAt, acknowledgedAt, releasedAt); err != nil {
			t.Fatal(err)
		}
		packageIDs = append(packageIDs, packageID)
	}
	if _, err := transaction.ExecContext(ctx, `
UPDATE trees
SET last_result_sequence = max(last_result_sequence, ?)
WHERE controller_id = ? AND tree_id = ?
`, sequenceStart+count-1, authority.ControllerID, authority.TreeID); err != nil {
		t.Fatal(err)
	}
	if _, err := transaction.ExecContext(ctx, `
INSERT INTO controller_lifetime_counters(
	controller_id, result_packages_delivered,
	result_packages_source_acknowledged, result_packages_source_released
) VALUES (?, ?, ?, ?)
ON CONFLICT(controller_id) DO UPDATE SET
	result_packages_delivered = result_packages_delivered + excluded.result_packages_delivered,
	result_packages_source_acknowledged = result_packages_source_acknowledged +
		excluded.result_packages_source_acknowledged,
	result_packages_source_released = result_packages_source_released +
		excluded.result_packages_source_released
`, authority.ControllerID, count, count, releasedCount); err != nil {
		t.Fatal(err)
	}
	if err := transaction.Commit(); err != nil {
		t.Fatal(err)
	}
	return packageIDs
}

func insertRetentionAlignmentPackage(
	t *testing.T,
	registry *Store,
	peer *PeerStore,
	authority ResultInboxAuthority,
	source control.PrincipalIdentity,
	metadata protocol.ResultPackageMetadata,
	packageID, managedThreadID, turnID string,
	publishedAt int64,
	index int,
) {
	t.Helper()
	ctx := context.Background()
	rootRetentionOrdinal := index + 1
	sequence := index + 1
	packagePublishedAt := publishedAt + int64(MaximumPeerResultPackages-index)
	if _, err := registry.db.ExecContext(ctx, `
INSERT INTO result_packages(
	controller_id, tree_id, package_id, source_agent_id, source_device_id,
	managed_thread_id, turn_id, lifecycle_revision, root_device_id,
	manifest_bytes, manifest_size, manifest_sha256, state, result_sequence,
	root_retention_ordinal, published_at, delivered_at, source_acknowledged_at, source_released_at
) VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, 'delivered', ?, ?, ?, ?, ?, ?)
`, authority.ControllerID, authority.TreeID, packageID, source.AgentID, source.DeviceID,
		managedThreadID, turnID, authority.RootDeviceID, metadata.Manifest,
		metadata.ManifestDescriptor.Size, metadata.ManifestDescriptor.SHA256,
		sequence, rootRetentionOrdinal, packagePublishedAt, packagePublishedAt+1,
		packagePublishedAt+2, packagePublishedAt+3); err != nil {
		t.Fatal(err)
	}
	if _, err := peer.db.ExecContext(ctx, `
INSERT INTO peer_result_inbox(
	controller_id, tree_id, root_agent_id, root_device_id,
	source_agent_id, source_device_id, managed_thread_id, turn_id,
	package_id, state, attempt_id, retention_ordinal, lease_expires_at, lifecycle_revision,
	manifest_bytes, manifest_size_bytes, manifest_sha256, parts_json,
	package_bytes, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'available', ?, ?, ?, 1, ?, ?, ?, '[]', ?, 1, ?)
`, authority.ControllerID, authority.TreeID, authority.RootAgentID, authority.RootDeviceID,
		source.AgentID, source.DeviceID, managedThreadID, turnID, packageID,
		changesTestID(293_000+index), rootRetentionOrdinal, packagePublishedAt+60,
		metadata.Manifest, metadata.ManifestDescriptor.Size, metadata.ManifestDescriptor.SHA256,
		metadata.ManifestDescriptor.Size, 1_000-index); err != nil {
		t.Fatal(err)
	}
}

func assertResultPackageGCWindow(
	t *testing.T,
	registry *Store,
	controllerID, rootDeviceID string,
	wantCount, wantMinimumOrdinal, wantMaximumOrdinal int,
) {
	t.Helper()
	var count, minimumOrdinal, maximumOrdinal int
	if err := registry.db.QueryRowContext(context.Background(), `
SELECT count(*), COALESCE(min(root_retention_ordinal), 0),
	COALESCE(max(root_retention_ordinal), 0)
FROM result_packages
WHERE controller_id = ? AND root_device_id = ? AND state = 'delivered'
`, controllerID, rootDeviceID).Scan(&count, &minimumOrdinal, &maximumOrdinal); err != nil {
		t.Fatal(err)
	}
	if count != wantCount || minimumOrdinal != wantMinimumOrdinal || maximumOrdinal != wantMaximumOrdinal {
		t.Fatalf(
			"result package window = count %d, ordinals %d..%d; want count %d, ordinals %d..%d",
			count, minimumOrdinal, maximumOrdinal,
			wantCount, wantMinimumOrdinal, wantMaximumOrdinal,
		)
	}
}

func assertResultPackageGCRowExists(
	t *testing.T,
	registry *Store,
	authority resultPackageGCSeedAuthority,
	packageID string,
	want bool,
) {
	t.Helper()
	var exists bool
	if err := registry.db.QueryRowContext(context.Background(), `
SELECT EXISTS(
	SELECT 1 FROM result_packages
	WHERE controller_id = ? AND tree_id = ? AND package_id = ?
)
`, authority.ControllerID, authority.TreeID, packageID).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists != want {
		t.Fatalf("result package %s exists = %t, want %t", packageID, exists, want)
	}
}

func assertResultPackageGCCompactedLifetime(
	t *testing.T,
	registry *Store,
	controllerID string,
	want uint64,
) {
	t.Helper()
	var got uint64
	err := registry.db.QueryRowContext(context.Background(), `
SELECT result_package_details_compacted
FROM controller_lifetime_counters
WHERE controller_id = ?
`, controllerID).Scan(&got)
	if err == sql.ErrNoRows {
		got = 0
	} else if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("compacted result package details = %d, want %d", got, want)
	}
}
