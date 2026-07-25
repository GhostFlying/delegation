package localbridge

import (
	"context"
	"errors"
	"fmt"
	"unicode"
	"unicode/utf8"

	"github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/identity"
)

const methodStatus = "bridge.status"

const maximumStatusVersionBytes = 128

type WorkerCounts struct {
	Total       int64 `json:"total"`
	Reserved    int64 `json:"reserved"`
	Pending     int64 `json:"pending"`
	Starting    int64 `json:"starting"`
	Preflight   int64 `json:"preflight"`
	Ready       int64 `json:"ready"`
	Running     int64 `json:"running"`
	Finalizing  int64 `json:"finalizing"`
	Idle        int64 `json:"idle"`
	Interrupted int64 `json:"interrupted"`
	Failed      int64 `json:"failed"`
	Occupied    int64 `json:"occupied"`
}

type ArtifactCounts struct {
	CapturePending int64 `json:"capturePending"`
	PublishPending int64 `json:"publishPending"`
	Retained       int64 `json:"retained"`
	RetainedBytes  int64 `json:"retainedBytes"`
}

type StatusSnapshot struct {
	Version              string         `json:"version"`
	ControllerID         string         `json:"controllerId"`
	DeviceID             string         `json:"deviceId"`
	DeviceName           string         `json:"deviceName"`
	Connected            bool           `json:"connected"`
	RegistryRevision     uint64         `json:"registryRevision"`
	WorkerRevision       uint64         `json:"workerRevision"`
	BrokerWorkerRevision uint64         `json:"brokerWorkerRevision"`
	WorkerSyncReady      bool           `json:"workerSyncReady"`
	MaxWorkerSlots       int            `json:"maxWorkerSlots"`
	Workers              WorkerCounts   `json:"workers"`
	Artifacts            ArtifactCounts `json:"artifacts"`
}

func (s StatusSnapshot) Validate() error {
	if len(s.Version) == 0 || len(s.Version) > maximumStatusVersionBytes || !utf8.ValidString(s.Version) {
		return errors.New("version must be bounded UTF-8 text")
	}
	for _, character := range s.Version {
		if !unicode.IsPrint(character) {
			return errors.New("version must be bounded UTF-8 text")
		}
	}
	if err := identity.ValidateID(s.ControllerID); err != nil {
		return fmt.Errorf("controllerId %w", err)
	}
	if err := identity.ValidateID(s.DeviceID); err != nil {
		return fmt.Errorf("deviceId %w", err)
	}
	if err := control.ValidateDeviceName(s.DeviceName); err != nil {
		return fmt.Errorf("deviceName: %w", err)
	}
	if s.MaxWorkerSlots < 1 || s.MaxWorkerSlots > config.MaximumWorkerSlots ||
		s.Workers.Occupied > int64(s.MaxWorkerSlots) {
		return errors.New("worker slot counts are invalid")
	}
	counts := []int64{
		s.Workers.Total, s.Workers.Reserved, s.Workers.Pending, s.Workers.Starting,
		s.Workers.Preflight, s.Workers.Ready, s.Workers.Running, s.Workers.Finalizing,
		s.Workers.Idle, s.Workers.Interrupted, s.Workers.Failed, s.Workers.Occupied,
		s.Artifacts.CapturePending, s.Artifacts.PublishPending, s.Artifacts.Retained,
		s.Artifacts.RetainedBytes,
	}
	for _, count := range counts {
		if count < 0 {
			return errors.New("status counts must not be negative")
		}
	}
	phaseTotal, ok := sumCounts(
		s.Workers.Reserved, s.Workers.Pending, s.Workers.Starting, s.Workers.Preflight,
		s.Workers.Ready, s.Workers.Running, s.Workers.Finalizing, s.Workers.Idle,
		s.Workers.Interrupted, s.Workers.Failed,
	)
	if !ok || phaseTotal != s.Workers.Total {
		return errors.New("worker phase counts are inconsistent")
	}
	occupied, ok := sumCounts(
		s.Workers.Reserved, s.Workers.Starting, s.Workers.Preflight,
		s.Workers.Ready, s.Workers.Running, s.Workers.Finalizing,
	)
	if !ok || occupied != s.Workers.Occupied || s.Workers.Occupied > s.Workers.Total {
		return errors.New("worker counts are inconsistent")
	}
	if s.WorkerSyncReady &&
		(!s.Connected || s.BrokerWorkerRevision != s.WorkerRevision) {
		return errors.New("worker synchronization state is inconsistent")
	}
	return nil
}

func sumCounts(counts ...int64) (int64, bool) {
	const maximumInt64 = int64(^uint64(0) >> 1)
	var total int64
	for _, count := range counts {
		if count < 0 || count > maximumInt64-total {
			return 0, false
		}
		total += count
	}
	return total, true
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
