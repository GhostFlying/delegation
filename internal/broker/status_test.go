package broker

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/buildinfo"
	"github.com/GhostFlying/delegation/internal/statuspage"
	"github.com/GhostFlying/delegation/internal/store"
)

type observedStatusReader struct {
	snapshot  store.StatusSnapshot
	devices   []string
	err       error
	calls     int
	afterRead func(int)
}

func (r *observedStatusReader) ReadStatusSnapshot(
	_ context.Context,
	_ string,
	deviceIDs []string,
) (store.StatusSnapshot, error) {
	r.calls++
	r.devices = append([]string(nil), deviceIDs...)
	if r.afterRead != nil {
		r.afterRead(r.calls)
	}
	return r.snapshot, r.err
}

func TestStatusCombinesDurableStateWithLiveSynchronizedConnections(t *testing.T) {
	const (
		readyDevice   = "123e4567-e89b-42d3-a456-426614174101"
		syncingDevice = "123e4567-e89b-42d3-a456-426614174102"
	)
	reader := &observedStatusReader{snapshot: store.StatusSnapshot{
		Devices:    store.StatusDeviceCounts{Total: 3, Online: 2},
		Trees:      4,
		Dispatches: store.StatusDispatchCounts{Pending: 5, Started: 6, Failed: 7},
		Workers:    store.StatusWorkerCounts{Running: 8, Occupied: 9},
		Artifacts:  store.StatusArtifactCounts{Available: 10, Unchanged: 11, CaptureFailed: 12},
		Results: store.StatusResultCounts{
			DeliveryPending: 13, DetailsRetained: 15, Delivered: 14,
			SourceAcknowledged: 12, SourceReleased: 11, DetailsCompacted: 2,
		},
		Lifetime: store.StatusLifetimeCounters{DispatchesStarted: 16, TurnsStarted: 17},
	}}
	ready := &session{deviceID: readyDevice}
	ready.revision.Store(2)
	ready.workerReady.Store(true)
	syncing := &session{deviceID: syncingDevice}
	syncing.revision.Store(2)
	server := &Server{
		controllerID: brokerTestControllerID,
		statusReader: reader,
		connections:  map[string]*session{readyDevice: ready, syncingDevice: syncing},
		latestRevisions: map[string]uint64{
			readyDevice:   2,
			syncingDevice: 2,
		},
		startedAt: time.Unix(100, 0),
		now:       func() time.Time { return time.Unix(223, 0) },
	}

	got, err := server.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := statuspage.Snapshot{
		Version: buildinfo.Version, UptimeSeconds: 123, ControllerID: brokerTestControllerID,
		Devices:      statuspage.DeviceCounts{Registered: 3, Online: 2, Connected: 2, SyncReady: 1},
		Dispatch:     statuspage.DispatchCounts{Pending: 5, Started: 6, Failed: 7, LifetimeStarted: 16},
		RunningTurns: 8, OccupiedSlots: 9, LifetimeTurns: 17, Trees: 4,
		Artifacts: statuspage.ArtifactCounts{Available: 10, Unchanged: 11, CaptureFailed: 12},
		Results: statuspage.ResultCounts{
			DeliveryPending: 13, DetailsRetained: 15, Delivered: 14,
			SourceAcknowledged: 12, SourceReleased: 11, DetailsCompacted: 2,
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("status = %#v, want %#v", got, want)
	}
	if !reflect.DeepEqual(reader.devices, []string{readyDevice}) {
		t.Fatalf("sync-ready device filter = %#v", reader.devices)
	}
}

func TestStatusAllowsConnectedSessionAfterDurableCredentialRevocation(t *testing.T) {
	const deviceID = "123e4567-e89b-42d3-a456-426614174101"
	reader := &observedStatusReader{snapshot: store.StatusSnapshot{
		Devices: store.StatusDeviceCounts{Total: 1, Online: 0},
	}}
	connected := &session{deviceID: deviceID}
	connected.revision.Store(1)
	connected.workerReady.Store(true)
	server := &Server{
		controllerID: brokerTestControllerID,
		statusReader: reader,
		connections:  map[string]*session{deviceID: connected},
		latestRevisions: map[string]uint64{
			deviceID: 1,
		},
		startedAt: time.Unix(100, 0),
		now:       func() time.Time { return time.Unix(100, 0) },
	}

	snapshot, err := server.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Validate(); err != nil {
		t.Fatalf("status rejected transient connected/offline session: %v", err)
	}
	want := statuspage.DeviceCounts{Registered: 1, Online: 0, Connected: 1, SyncReady: 1}
	if snapshot.Devices != want {
		t.Fatalf("device status = %#v, want %#v", snapshot.Devices, want)
	}
}

func TestStatusFailsClosedWithoutDurableSnapshot(t *testing.T) {
	server := &Server{}
	if _, err := server.Status(context.Background()); err == nil {
		t.Fatal("Status() succeeded without a status reader")
	}
	injected := errors.New("status store unavailable")
	server.statusReader = &observedStatusReader{err: injected}
	server.connections = map[string]*session{}
	server.latestRevisions = map[string]uint64{}
	if _, err := server.Status(context.Background()); !errors.Is(err, injected) {
		t.Fatalf("Status() error = %v, want %v", err, injected)
	}
}

func TestStatusRetriesConnectionChurnAndStopsAtBound(t *testing.T) {
	reader := &observedStatusReader{}
	server := &Server{
		controllerID:    brokerTestControllerID,
		statusReader:    reader,
		connections:     map[string]*session{},
		latestRevisions: map[string]uint64{},
		startedAt:       time.Unix(100, 0),
		now:             func() time.Time { return time.Unix(100, 0) },
	}
	reader.afterRead = func(call int) {
		if call != 1 {
			return
		}
		server.mu.Lock()
		server.statusGeneration++
		server.mu.Unlock()
	}
	if _, err := server.Status(context.Background()); err != nil {
		t.Fatalf("Status() did not recover from one connection change: %v", err)
	}
	if reader.calls != 2 {
		t.Fatalf("status read calls = %d, want 2", reader.calls)
	}

	reader.calls = 0
	reader.afterRead = func(int) {
		server.mu.Lock()
		server.statusGeneration++
		server.mu.Unlock()
	}
	if _, err := server.Status(context.Background()); err == nil {
		t.Fatal("Status() succeeded during continuous connection churn")
	}
	if reader.calls != 3 {
		t.Fatalf("bounded status read calls = %d, want 3", reader.calls)
	}
}
