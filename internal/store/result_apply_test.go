package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/protocol"
)

const (
	resultApplyOne = "123e4567-e89b-42d3-a456-426614175140"
	resultApplyTwo = "123e4567-e89b-42d3-a456-426614175141"
)

func TestResultApplyAuthorizationIsRootScopedDurableAndExactlyReplayable(t *testing.T) {
	fixture := deliveredResultApplyFixture(t)
	params := resultApplyParams(fixture, resultApplyOne)
	ctx := context.Background()
	created, err := fixture.Registry.AuthorizeResultApply(
		ctx, fixture.Root.DeviceID, fixture.Root.Identity(), params, time.Unix(60, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	want := protocol.AuthorizeResultApplyResult{
		ApplyID: params.ApplyID, PackageID: params.PackageID,
		ManifestSHA256:   fixture.Metadata.ManifestDescriptor.SHA256,
		WorkspaceID:      fixture.Manifest.Workspace.WorkspaceID,
		BaseManifestHash: fixture.Manifest.Workspace.BaseManifestHash,
	}
	if created != want {
		t.Fatalf("result apply authorization = %#v, want %#v", created, want)
	}
	replayed, err := fixture.Registry.AuthorizeResultApply(
		ctx, fixture.Root.DeviceID, fixture.Root.Identity(), params, time.Unix(70, 0),
	)
	if err != nil || replayed != created {
		t.Fatalf("result apply authorization replay = %#v, error %v", replayed, err)
	}

	for name, mutate := range map[string]func(*protocol.AuthorizeResultApplyParams){
		"package": func(value *protocol.AuthorizeResultApplyParams) { value.PackageID = resultPackageTwo },
		"Git URL": func(value *protocol.AuthorizeResultApplyParams) {
			value.GitURL = "https://example.invalid/other.git"
		},
		"source path": func(value *protocol.AuthorizeResultApplyParams) {
			value.SourcePathSHA256 = strings.Repeat("f", 64)
		},
	} {
		t.Run(name, func(t *testing.T) {
			changed := params
			mutate(&changed)
			if _, err := fixture.Registry.AuthorizeResultApply(
				ctx, fixture.Root.DeviceID, fixture.Root.Identity(), changed, time.Unix(80, 0),
			); !errors.Is(err, ErrConflict) {
				t.Fatalf("changed applyId replay error = %v, want ErrConflict", err)
			}
		})
	}
	var count int
	if err := fixture.Registry.db.QueryRowContext(
		ctx, `SELECT count(*) FROM result_apply_authorizations`,
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("result apply authorization count = %d, want 1", count)
	}
}

func TestResultApplyAuthorizationRejectsPrincipalAndPackageBypass(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(resultPackageFixture, *string, *control.PrincipalIdentity, *protocol.AuthorizeResultApplyParams)
		wantError error
	}{
		{name: "worker principal", mutate: func(fixture resultPackageFixture, _ *string, root *control.PrincipalIdentity, _ *protocol.AuthorizeResultApplyParams) {
			*root = fixture.Worker
		}, wantError: ErrAuthorizationDenied},
		{name: "wrong connection device", mutate: func(_ resultPackageFixture, deviceID *string, _ *control.PrincipalIdentity, _ *protocol.AuthorizeResultApplyParams) {
			*deviceID = agentSpawnTargetID
		}, wantError: ErrAuthorizationDenied},
		{name: "forged root device", mutate: func(_ resultPackageFixture, _ *string, root *control.PrincipalIdentity, _ *protocol.AuthorizeResultApplyParams) {
			root.DeviceID = agentSpawnTargetID
		}, wantError: ErrAuthorizationDenied},
		{name: "forged root agent", mutate: func(_ resultPackageFixture, _ *string, root *control.PrincipalIdentity, _ *protocol.AuthorizeResultApplyParams) {
			root.AgentID = resultApplyTwo
		}, wantError: ErrAuthorizationDenied},
		{name: "cross tree", mutate: func(_ resultPackageFixture, _ *string, root *control.PrincipalIdentity, _ *protocol.AuthorizeResultApplyParams) {
			root.TreeID = treeSecondThreadID
		}, wantError: ErrAuthorizationDenied},
		{name: "unknown package", mutate: func(_ resultPackageFixture, _ *string, _ *control.PrincipalIdentity, params *protocol.AuthorizeResultApplyParams) {
			params.PackageID = resultPackageTwo
		}, wantError: ErrNotFound},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := deliveredResultApplyFixture(t)
			params := resultApplyParams(fixture, resultApplyOne)
			deviceID := fixture.Root.DeviceID
			root := fixture.Root.Identity()
			test.mutate(fixture, &deviceID, &root, &params)
			if _, err := fixture.Registry.AuthorizeResultApply(
				context.Background(), deviceID, root, params, time.Unix(60, 0),
			); !errors.Is(err, test.wantError) {
				t.Fatalf("result apply bypass error = %v, want %v", err, test.wantError)
			}
		})
	}
}

func TestResultApplyAuthorizationRejectsWorkspaceURLAndPathDigestMismatch(t *testing.T) {
	fixture := deliveredResultApplyFixture(t)
	for name, mutate := range map[string]func(*protocol.AuthorizeResultApplyParams){
		"Git URL": func(value *protocol.AuthorizeResultApplyParams) {
			value.GitURL = "https://example.invalid/wrong.git"
		},
		"source path": func(value *protocol.AuthorizeResultApplyParams) {
			value.SourcePathSHA256 = strings.Repeat("e", 64)
		},
	} {
		t.Run(name, func(t *testing.T) {
			params := resultApplyParams(fixture, resultApplyTwo)
			mutate(&params)
			if _, err := fixture.Registry.AuthorizeResultApply(
				context.Background(), fixture.Root.DeviceID, fixture.Root.Identity(), params, time.Unix(60, 0),
			); !errors.Is(err, ErrConflict) {
				t.Fatalf("workspace mismatch error = %v, want ErrConflict", err)
			}
		})
	}
}

func TestResultApplyAuthorizationRejectsUndeliveredPackage(t *testing.T) {
	fixture := prepareResultPackageFixture(t, true, true)
	publishResultManifest(t, fixture, fixture.Manifest, 40)
	params := resultApplyParams(fixture, resultApplyOne)
	if _, err := fixture.Registry.AuthorizeResultApply(
		context.Background(), fixture.Root.DeviceID, fixture.Root.Identity(), params, time.Unix(50, 0),
	); !errors.Is(err, ErrAuthorizationDenied) {
		t.Fatalf("undelivered result package error = %v, want ErrAuthorizationDenied", err)
	}
}

func deliveredResultApplyFixture(t *testing.T) resultPackageFixture {
	t.Helper()
	fixture := prepareResultPackageFixture(t, true, true)
	publishResultManifest(t, fixture, fixture.Manifest, 40)
	delivered, err := fixture.Registry.MarkResultPackageDelivered(
		context.Background(), fixture.Root.DeviceID, fixture.Root.Identity(),
		fixture.Manifest.PackageID, 1, time.Unix(50, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	if delivered.State != ResultPackageDelivered || !reflect.DeepEqual(delivered.Manifest, fixture.Manifest) {
		t.Fatalf("delivered result package = %#v", delivered)
	}
	return fixture
}

func resultApplyParams(
	fixture resultPackageFixture,
	applyID string,
) protocol.AuthorizeResultApplyParams {
	pathDigest := sha256.Sum256([]byte("/trusted/changes-source"))
	return protocol.AuthorizeResultApplyParams{
		ApplyID: applyID, PackageID: fixture.Manifest.PackageID,
		SourcePathSHA256: hex.EncodeToString(pathDigest[:]),
		GitURL:           "ssh://git@example.invalid/changes.git",
	}
}
