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
	const maximumSnapshotAttempts = 3
	for range maximumSnapshotAttempts {
		connections := s.captureConnectionStatus()
		durable, err := s.statusReader.ReadStatusSnapshot(
			ctx, s.controllerID, connections.syncReadyDeviceIDs,
		)
		if err != nil {
			return statuspage.Snapshot{}, err
		}
		if s.connectionStatusGenerationMatches(connections.generation) {
			return s.buildStatusSnapshot(durable, connections), nil
		}
		if err := ctx.Err(); err != nil {
			return statuspage.Snapshot{}, err
		}
	}
	return statuspage.Snapshot{}, errors.New("broker connections changed during status snapshot")
}

type connectionStatusSnapshot struct {
	generation         uint64
	connected          int
	syncReadyDeviceIDs []string
}

func (s *Server) buildStatusSnapshot(
	durable store.StatusSnapshot,
	connections connectionStatusSnapshot,
) statuspage.Snapshot {
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
			Connected:  uint64(connections.connected),
			SyncReady:  uint64(len(connections.syncReadyDeviceIDs)),
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
		Results: statuspage.ResultCounts{
			DeliveryPending:    uint64(durable.Results.DeliveryPending),
			Delivered:          uint64(durable.Results.Delivered),
			SourceAcknowledged: uint64(durable.Results.SourceAcknowledged),
			SourceReleased:     uint64(durable.Results.SourceReleased),
		},
	}
}

func (s *Server) captureConnectionStatus() connectionStatusSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	snapshot := connectionStatusSnapshot{
		generation:         s.statusGeneration,
		syncReadyDeviceIDs: make([]string, 0, len(s.connections)),
	}
	for deviceID := range s.connections {
		if s.currentConnectionLocked(deviceID) == nil {
			continue
		}
		snapshot.connected++
		if s.workerReadyConnectionLocked(deviceID) != nil {
			snapshot.syncReadyDeviceIDs = append(snapshot.syncReadyDeviceIDs, deviceID)
		}
	}
	return snapshot
}

func (s *Server) connectionStatusGenerationMatches(generation uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.statusGeneration == generation
}
