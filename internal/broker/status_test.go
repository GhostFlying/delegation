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
	snapshot store.StatusSnapshot
	devices  []string
	err      error
}

func (r *observedStatusReader) ReadStatusSnapshot(
	_ context.Context,
	_ string,
	deviceIDs []string,
) (store.StatusSnapshot, error) {
	r.devices = append([]string(nil), deviceIDs...)
	return r.snapshot, r.err
}

func TestStatusCombinesDurableStateWithLiveSynchronizedConnections(t *testing.T) {
	const (
		readyDevice = "123e4567-e89b-42d3-a456-426614174101"
		staleDevice = "123e4567-e89b-42d3-a456-426614174102"
	)
	reader := &observedStatusReader{snapshot: store.StatusSnapshot{
		Devices:    store.StatusDeviceCounts{Total: 3, Online: 2},
		Trees:      4,
		Dispatches: store.StatusDispatchCounts{Pending: 5, Started: 6, Failed: 7},
		Workers:    store.StatusWorkerCounts{Running: 8, Occupied: 9},
		Artifacts:  store.StatusArtifactCounts{Available: 10, Unchanged: 11, CaptureFailed: 12},
		Lifetime:   store.StatusLifetimeCounters{DispatchesStarted: 13, TurnsStarted: 14},
	}}
	ready := &session{deviceID: readyDevice}
	ready.revision.Store(2)
	ready.workerReady.Store(true)
	stale := &session{deviceID: staleDevice}
	stale.revision.Store(1)
	stale.workerReady.Store(true)
	server := &Server{
		controllerID: brokerTestControllerID,
		statusReader: reader,
		connections:  map[string]*session{readyDevice: ready, staleDevice: stale},
		latestRevisions: map[string]uint64{
			readyDevice: 2,
			staleDevice: 2,
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
		Dispatch:     statuspage.DispatchCounts{Pending: 5, Started: 6, Failed: 7, LifetimeStarted: 13},
		RunningTurns: 8, OccupiedSlots: 9, LifetimeTurns: 14, Trees: 4,
		Artifacts: statuspage.ArtifactCounts{Available: 10, Unchanged: 11, CaptureFailed: 12},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("status = %#v, want %#v", got, want)
	}
	if !reflect.DeepEqual(reader.devices, []string{readyDevice}) {
		t.Fatalf("sync-ready device filter = %#v", reader.devices)
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
