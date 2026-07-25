package broker

import (
	"context"
	"errors"
	"time"

	"github.com/GhostFlying/delegation/internal/buildinfo"
	"github.com/GhostFlying/delegation/internal/statuspage"
	"github.com/GhostFlying/delegation/internal/store"
)

// StatusReader supplies durable aggregates while the broker owns the exact
// live-connection view used to select synchronized worker devices.
type StatusReader interface {
	ReadStatusSnapshot(context.Context, string, []string) (store.StatusSnapshot, error)
}

// Status returns a bounded aggregate snapshot for the broker status surfaces.
func (s *Server) Status(ctx context.Context) (statuspage.Snapshot, error) {
	if s.statusReader == nil {
		return statuspage.Snapshot{}, errors.New("broker status reader is unavailable")
	}
	connected, syncReadyDeviceIDs := s.connectionStatus()
	durable, err := s.statusReader.ReadStatusSnapshot(
		ctx, s.controllerID, syncReadyDeviceIDs,
	)
	if err != nil {
		return statuspage.Snapshot{}, err
	}
	uptime := s.now().Sub(s.startedAt)
	if uptime < 0 {
		uptime = 0
	}
	return statuspage.Snapshot{
		Version:       buildinfo.Version,
		UptimeSeconds: uint64(uptime / time.Second),
		ControllerID:  s.controllerID,
		Devices: statuspage.DeviceCounts{
			Registered: uint64(durable.Devices.Total),
			Online:     uint64(durable.Devices.Online),
			Connected:  uint64(connected),
			SyncReady:  uint64(len(syncReadyDeviceIDs)),
		},
		Dispatch: statuspage.DispatchCounts{
			Pending:         uint64(durable.Dispatches.Pending),
			Started:         uint64(durable.Dispatches.Started),
			Failed:          uint64(durable.Dispatches.Failed),
			LifetimeStarted: durable.Lifetime.DispatchesStarted,
		},
		RunningTurns:  uint64(durable.Workers.Running),
		OccupiedSlots: uint64(durable.Workers.Occupied),
		LifetimeTurns: durable.Lifetime.TurnsStarted,
		Trees:         uint64(durable.Trees),
		Artifacts: statuspage.ArtifactCounts{
			Available:     uint64(durable.Artifacts.Available),
			Unchanged:     uint64(durable.Artifacts.Unchanged),
			CaptureFailed: uint64(durable.Artifacts.CaptureFailed),
		},
	}, nil
}

func (s *Server) connectionStatus() (int, []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	connected := len(s.connections)
	syncReadyDeviceIDs := make([]string, 0, connected)
	for deviceID, current := range s.connections {
		if current.revision.Load() < s.latestRevisions[deviceID] || !current.workerReady.Load() {
			continue
		}
		syncReadyDeviceIDs = append(syncReadyDeviceIDs, deviceID)
	}
	return connected, syncReadyDeviceIDs
}
