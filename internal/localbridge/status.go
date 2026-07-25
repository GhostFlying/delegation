package localbridge

import (
	"context"
	"errors"
	"fmt"

	"github.com/GhostFlying/delegation/internal/identity"
)

const methodStatus = "bridge.status"

type WorkerCounts struct {
	Total       int64 `json:"total"`
	Pending     int64 `json:"pending"`
	Running     int64 `json:"running"`
	Idle        int64 `json:"idle"`
	Interrupted int64 `json:"interrupted"`
	Failed      int64 `json:"failed"`
	Occupied    int64 `json:"occupied"`
}

type ArtifactCounts struct {
	Available      int64 `json:"available"`
	CapturePending int64 `json:"capturePending"`
	PublishPending int64 `json:"publishPending"`
	CaptureFailed  int64 `json:"captureFailed"`
	RetainedBytes  int64 `json:"retainedBytes"`
}

type StatusSnapshot struct {
	Version          string         `json:"version"`
	ControllerID     string         `json:"controllerId"`
	DeviceID         string         `json:"deviceId"`
	DeviceName       string         `json:"deviceName"`
	Connected        bool           `json:"connected"`
	RegistryRevision uint64         `json:"registryRevision"`
	WorkerRevision   uint64         `json:"workerRevision"`
	MaxWorkerSlots   int            `json:"maxWorkerSlots"`
	Workers          WorkerCounts   `json:"workers"`
	Artifacts        ArtifactCounts `json:"artifacts"`
}

func (s StatusSnapshot) Validate() error {
	if s.Version == "" {
		return errors.New("version is required")
	}
	if err := identity.ValidateID(s.ControllerID); err != nil {
		return fmt.Errorf("controllerId %w", err)
	}
	if err := identity.ValidateID(s.DeviceID); err != nil {
		return fmt.Errorf("deviceId %w", err)
	}
	if s.DeviceName == "" {
		return errors.New("deviceName is required")
	}
	if s.MaxWorkerSlots < 1 || s.Workers.Occupied > int64(s.MaxWorkerSlots) {
		return errors.New("worker slot counts are invalid")
	}
	counts := []int64{
		s.Workers.Total, s.Workers.Pending, s.Workers.Running, s.Workers.Idle,
		s.Workers.Interrupted, s.Workers.Failed, s.Workers.Occupied,
		s.Artifacts.Available, s.Artifacts.CapturePending, s.Artifacts.PublishPending,
		s.Artifacts.CaptureFailed, s.Artifacts.RetainedBytes,
	}
	for _, count := range counts {
		if count < 0 {
			return errors.New("status counts must not be negative")
		}
	}
	if s.Workers.Running > s.Workers.Occupied || s.Workers.Occupied > s.Workers.Total {
		return errors.New("worker counts are inconsistent")
	}
	return nil
}

func ReadStatus(ctx context.Context, endpoint string) (StatusSnapshot, error) {
	client, err := NewClient(endpoint)
	if err != nil {
		return StatusSnapshot{}, err
	}
	var status StatusSnapshot
	if err := client.Call(ctx, methodStatus, "", nil, struct{}{}, &status); err != nil {
		return StatusSnapshot{}, fmt.Errorf("read local delegation status: %w", err)
	}
	if err := status.Validate(); err != nil {
		return StatusSnapshot{}, fmt.Errorf("invalid local delegation status: %w", err)
	}
	return status, nil
}
