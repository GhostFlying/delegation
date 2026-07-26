package cli

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/GhostFlying/delegation/internal/connector"
	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/localbridge"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/resultpackagefiles"
	"github.com/GhostFlying/delegation/internal/store"
)

type staticResultPackageAvailabilityLookup struct {
	result  resultpackagefiles.LookupAvailabilityResult
	err     error
	request resultpackagefiles.LookupAvailabilityRequest
}

func (s *staticResultPackageAvailabilityLookup) LookupResultPackageAvailability(
	_ context.Context,
	request resultpackagefiles.LookupAvailabilityRequest,
) (resultpackagefiles.LookupAvailabilityResult, error) {
	s.request = request
	return s.result, s.err
}

func TestLocalResultPackageAvailabilityProvider(t *testing.T) {
	root := control.PrincipalIdentity{
		ControllerID: "123e4567-e89b-42d3-a456-426614174180",
		TreeID:       "123e4567-e89b-42d3-a456-426614174181",
		AgentID:      "123e4567-e89b-42d3-a456-426614174182",
		DeviceID:     "123e4567-e89b-42d3-a456-426614174183",
	}
	manifest := protocol.ResultManifest{PackageID: "123e4567-e89b-42d3-a456-426614174184"}
	for _, test := range []struct {
		name      string
		result    resultpackagefiles.PackageAvailability
		err       error
		want      protocol.ResultPackageAvailability
		wantError bool
	}{
		{name: "available", result: resultpackagefiles.PackageAvailable, want: protocol.ResultPackageAvailable},
		{name: "evicted", result: resultpackagefiles.PackageEvicted, want: protocol.ResultPackageEvicted},
		{name: "lookup failure", err: errors.New("lookup failed"), wantError: true},
		{name: "invalid manager result", result: "unexpected", wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			lookup := &staticResultPackageAvailabilityLookup{
				result: resultpackagefiles.LookupAvailabilityResult{
					PackageID: manifest.PackageID, Availability: test.result,
				},
				err: test.err,
			}
			got, err := (localResultPackageAvailabilityProvider{manager: lookup}).
				LookupResultPackageAvailability(
					context.Background(),
					localbridge.ResultPackageAvailabilityLookup{Root: root, Manifest: manifest},
				)
			if (err != nil) != test.wantError || got != test.want {
				t.Fatalf("availability = %q, error %v", got, err)
			}
			wantRequest := resultpackagefiles.LookupAvailabilityRequest{Root: root, Manifest: manifest}
			if !reflect.DeepEqual(lookup.request, wantRequest) {
				t.Fatalf("lookup request = %#v, want %#v", lookup.request, wantRequest)
			}
		})
	}
}

type staticResultPackagePublisher struct {
	changes  <-chan struct{}
	outboxes []store.ResultOutbox
	acked    store.ResultOutboxKey
}

func (p *staticResultPackagePublisher) ResultPackageChanges() <-chan struct{} {
	return p.changes
}

func (p *staticResultPackagePublisher) ListPendingResultPublications(
	context.Context,
) ([]store.ResultOutbox, error) {
	return append([]store.ResultOutbox(nil), p.outboxes...), nil
}

func (p *staticResultPackagePublisher) AcknowledgeResultPackageMetadata(
	_ context.Context,
	key store.ResultOutboxKey,
	metadata protocol.ResultPackageMetadata,
) (store.WorkerResultFinalization, error) {
	if !protocol.SameResultPackageMetadata(p.outboxes[0].Metadata, metadata) {
		return store.WorkerResultFinalization{}, store.ErrResultPackageConflict
	}
	p.acked = key
	result := p.outboxes[0]
	result.State = store.ResultOutboxDeliveryPending
	return store.WorkerResultFinalization{Outbox: result}, nil
}

type resultPackageManagedWorkerState struct {
	worker store.WorkerReservation
}

func (s resultPackageManagedWorkerState) GetWorker(
	context.Context,
	store.WorkerKey,
) (store.WorkerReservation, error) {
	return s.worker, nil
}

func TestManagedResultPackageSourceMapsAndAcknowledgesMetadata(t *testing.T) {
	const (
		controllerID = "123e4567-e89b-42d3-a456-426614174190"
		deviceID     = "123e4567-e89b-42d3-a456-426614174191"
		treeID       = "123e4567-e89b-42d3-a456-426614174192"
		rootAgentID  = "123e4567-e89b-42d3-a456-426614174193"
		workerID     = "123e4567-e89b-42d3-a456-426614174194"
		threadID     = "123e4567-e89b-42d3-a456-426614174195"
		turnID       = "123e4567-e89b-42d3-a456-426614174196"
		packageID    = "123e4567-e89b-42d3-a456-426614174197"
	)
	manifest := protocol.ResultManifest{
		Version: protocol.ResultManifestVersion, PackageID: packageID,
		ControllerID: controllerID, TreeID: treeID,
		SourceAgentID: workerID, SourceDeviceID: deviceID,
		ManagedThreadID: threadID, TurnID: turnID, LifecycleRevision: 7,
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
	}
	manifestBytes, descriptor, err := protocol.EncodeResultManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	metadata := protocol.ResultPackageMetadata{Manifest: manifestBytes, ManifestDescriptor: descriptor}
	key := store.ResultOutboxKey{
		WorkerKey:      store.WorkerKey{ControllerID: controllerID, TreeID: treeID, AgentID: workerID},
		SourceDeviceID: deviceID, PackageID: packageID,
	}
	outbox := store.ResultOutbox{
		ResultOutboxKey: key, State: store.ResultOutboxPublishPending,
		Metadata: metadata, Manifest: manifest,
		ReservationLimitBytes: protocol.MaximumResultPackageBytes,
		ReservedBytes:         int64(len(manifestBytes)), PackageBytes: int64(len(manifestBytes)),
		CreatedAt: 1_700_000_000, UpdatedAt: 1_700_000_001,
	}
	worker := store.WorkerReservation{
		WorkerKey: key.WorkerKey, ParentAgentID: rootAgentID, DeviceID: deviceID,
		TaskName: "result_worker", PromptDigest: strings.Repeat("a", 64),
		WorkspacePath: t.TempDir(), CodexThreadID: threadID, ProfileVersion: 1,
		Status: store.WorkerFinalizing, ActiveTurnID: turnID, LastBoundTurnID: turnID,
		FinalTarget: store.WorkerIdle, Revision: 7,
		CreatedAt: 1_700_000_000, UpdatedAt: 1_700_000_001,
	}
	changes := make(chan struct{}, 1)
	publisher := &staticResultPackagePublisher{changes: changes, outboxes: []store.ResultOutbox{outbox}}
	source := managedResultPackageSource{
		packages: publisher, state: resultPackageManagedWorkerState{worker: worker},
		controllerID: controllerID, deviceID: deviceID,
	}
	if source.ResultPackageChanges() != (<-chan struct{})(changes) {
		t.Fatal("managed result source did not expose the publisher signal")
	}
	publications, err := source.ListPendingResultPackagePublications(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := connector.ResultPackagePublication{
		Source: control.PrincipalIdentity{
			ControllerID: controllerID, TreeID: treeID, AgentID: workerID,
			ParentAgentID: rootAgentID, DeviceID: deviceID,
		},
		Params: protocol.PublishResultPackageParams{Metadata: metadata},
	}
	if len(publications) != 1 || publications[0].Source != want.Source ||
		!protocol.SamePublishResultPackageParams(publications[0].Params, want.Params) {
		t.Fatalf("publications = %#v, want %#v", publications, want)
	}
	if err := source.AcknowledgeResultPackageMetadata(
		context.Background(), publications[0],
	); err != nil {
		t.Fatal(err)
	}
	if publisher.acked != key {
		t.Fatalf("acknowledged key = %#v, want %#v", publisher.acked, key)
	}
}
