package resultpackagefiles

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
)

func TestLookupResultPackageAvailabilityVerifiesAvailablePackage(t *testing.T) {
	fixture := newManagerFixture(t)
	defer fixture.close()
	root, manifest, _ := fixture.makeAvailablePackage(t, testID(800), []byte("payload"))
	result, err := fixture.manager.LookupResultPackageAvailability(
		context.Background(), LookupAvailabilityRequest{Root: root, Manifest: manifest},
	)
	if err != nil || result != (LookupAvailabilityResult{
		PackageID: manifest.PackageID, Availability: PackageAvailable,
	}) {
		t.Fatalf("available lookup = %#v, %v", result, err)
	}
}

func TestLookupResultPackageAvailabilityRejectsMismatchedAuthorityAndManifest(t *testing.T) {
	fixture := newManagerFixture(t)
	defer fixture.close()
	root, manifest, _ := fixture.makeAvailablePackage(t, testID(810), nil)
	otherID := testID(819)
	for _, test := range []struct {
		name   string
		root   control.PrincipalIdentity
		mutate func(*protocol.ResultManifest)
		want   error
	}{
		{
			name: "semantic manifest", root: root,
			mutate: func(value *protocol.ResultManifest) { value.CapturedAt++ },
			want:   store.ErrResultPackageConflict,
		},
		{
			name:   "root device",
			root:   control.NewRootPrincipal(testControllerID, testTreeID, testRootAgentID, otherID).Identity(),
			mutate: func(*protocol.ResultManifest) {}, want: store.ErrResultPackageAuthority,
		},
		{
			name: "manifest tree", root: root,
			mutate: func(value *protocol.ResultManifest) { value.TreeID = otherID },
			want:   store.ErrResultPackageAuthority,
		},
		{
			name:   "root tree",
			root:   control.NewRootPrincipal(testControllerID, otherID, testRootAgentID, testDeviceID).Identity(),
			mutate: func(*protocol.ResultManifest) {}, want: store.ErrResultPackageAuthority,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			requested := manifest
			test.mutate(&requested)
			_, err := fixture.manager.LookupResultPackageAvailability(
				context.Background(), LookupAvailabilityRequest{Root: test.root, Manifest: requested},
			)
			if !errors.Is(err, test.want) {
				t.Fatalf("lookup error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestLookupResultPackageAvailabilityDistinguishesReceivingEvictedAndAbsent(t *testing.T) {
	fixture := newManagerFixture(t)
	defer fixture.close()
	ctx := context.Background()
	start := time.Now().Truncate(time.Second)
	fixture.manager.now = func() time.Time { return start }
	root := control.NewRootPrincipal(testControllerID, testTreeID, testRootAgentID, testDeviceID).Identity()
	receivingID := testID(820)
	receivingMetadata := emptyMetadata(t, receivingID)
	receivingManifest, err := receivingMetadata.DecodeManifest()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.manager.BeginResultPackage(ctx, BeginRequest{
		TreeID: testTreeID, Source: root,
		Params: protocol.BeginResultPackageParams{
			AttemptID: testID(821), PackageID: receivingID,
			LeaseExpiresAt: start.Add(time.Minute).Unix(), Metadata: receivingMetadata,
		},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.manager.LookupResultPackageAvailability(
		ctx, LookupAvailabilityRequest{Root: root, Manifest: receivingManifest},
	); !errors.Is(err, store.ErrResultPackageTransition) {
		t.Fatalf("receiving lookup error = %v", err)
	}

	_, evictedManifest, authority := fixture.makeAvailablePackage(t, testID(822), nil)
	if _, err := fixture.state.PrepareResultInboxEviction(
		ctx, authority, evictedManifest.PackageID, start.Add(time.Minute),
	); err != nil {
		t.Fatal(err)
	}
	evicted, err := fixture.manager.LookupResultPackageAvailability(
		ctx, LookupAvailabilityRequest{Root: root, Manifest: evictedManifest},
	)
	if err != nil || evicted.Availability != PackageEvicted {
		t.Fatalf("tombstone lookup = %#v, %v", evicted, err)
	}

	absentMetadata := emptyMetadata(t, testID(823))
	absentManifest, err := absentMetadata.DecodeManifest()
	if err != nil {
		t.Fatal(err)
	}
	absent, err := fixture.manager.LookupResultPackageAvailability(
		ctx, LookupAvailabilityRequest{Root: root, Manifest: absentManifest},
	)
	if err != nil || absent.Availability != PackageEvicted {
		t.Fatalf("absent lookup = %#v, %v", absent, err)
	}
}

func TestLookupResultPackageAvailabilityFailsClosedOnMissingOrCorruptBytes(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, *managerFixture, string)
	}{
		{
			name: "missing",
			mutate: func(t *testing.T, fixture *managerFixture, packageID string) {
				t.Helper()
				if err := fixture.manager.inbox.Remove(packageID + "/" + protocol.ResultChangesBundleFileName); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "corrupt",
			mutate: func(t *testing.T, fixture *managerFixture, packageID string) {
				t.Helper()
				directory, err := fixture.manager.inbox.OpenRoot(packageID)
				if err != nil {
					t.Fatal(err)
				}
				file, err := directory.OpenFile(protocol.ResultChangesBundleFileName, os.O_WRONLY, 0)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := file.WriteAt([]byte("PAYLOAD"), 0); err != nil {
					t.Fatal(err)
				}
				if err := errors.Join(file.Sync(), file.Close(), directory.Close()); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newManagerFixture(t)
			defer fixture.close()
			root, manifest, _ := fixture.makeAvailablePackage(t, testID(830), []byte("payload"))
			test.mutate(t, fixture, manifest.PackageID)
			if _, err := fixture.manager.LookupResultPackageAvailability(
				context.Background(), LookupAvailabilityRequest{Root: root, Manifest: manifest},
			); err == nil {
				t.Fatal("lookup accepted missing or corrupt result package bytes")
			}
		})
	}
}

func (f *managerFixture) makeAvailablePackage(
	t *testing.T,
	packageID string,
	payload []byte,
) (control.PrincipalIdentity, protocol.ResultManifest, store.ResultInboxAuthority) {
	t.Helper()
	start := time.Now().Truncate(time.Second)
	f.manager.now = func() time.Time { return start }
	root := control.NewRootPrincipal(testControllerID, testTreeID, testRootAgentID, testDeviceID).Identity()
	metadata := emptyMetadata(t, packageID)
	if payload != nil {
		metadata = workspaceMetadata(t, packageID, payload)
	}
	attemptID := testID(899)
	if _, err := f.manager.BeginResultPackage(context.Background(), BeginRequest{
		TreeID: testTreeID, Source: root,
		Params: protocol.BeginResultPackageParams{
			AttemptID: attemptID, PackageID: packageID,
			LeaseExpiresAt: start.Add(time.Minute).Unix(), Metadata: metadata,
		},
	}); err != nil {
		t.Fatal(err)
	}
	if payload != nil {
		if _, err := f.manager.WriteResultPackagePart(context.Background(), WriteRequest{
			TreeID: testTreeID, Source: root,
			Params: protocol.WriteResultPackagePartParams{
				AttemptID: attemptID, PackageID: packageID,
				Kind: protocol.ResultPackagePartChangesBundle, Data: payload,
			},
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.manager.FinishResultPackage(context.Background(), FinishRequest{
		TreeID: testTreeID, Source: root,
		Params: protocol.FinishResultPackageParams{AttemptID: attemptID, PackageID: packageID},
	}); err != nil {
		t.Fatal(err)
	}
	manifest, err := metadata.DecodeManifest()
	if err != nil {
		t.Fatal(err)
	}
	return root, manifest, store.ResultInboxAuthority{
		ControllerID: testControllerID, TreeID: testTreeID,
		RootAgentID: testRootAgentID, RootDeviceID: testDeviceID,
	}
}
