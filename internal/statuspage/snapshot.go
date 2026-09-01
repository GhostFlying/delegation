package statuspage

import (
	"context"
	"errors"
	"fmt"
	"unicode"
	"unicode/utf8"

	"github.com/GhostFlying/delegation/internal/config"
)

const maximumTextBytes = 256

// Provider returns one internally consistent broker status snapshot.
type Provider func(context.Context) (Snapshot, error)

// Snapshot is the aggregate broker status presented by the status endpoints.
// It intentionally contains no credentials, local paths, or per-device data.
type Snapshot struct {
	config.TransportStatus
	Version           string             `json:"version,omitempty"`
	ServiceRunning    bool               `json:"serviceRunning"`
	UptimeSeconds     uint64             `json:"uptimeSeconds"`
	ControllerID      string             `json:"controllerId,omitempty"`
	InstanceID        string             `json:"instanceId,omitempty"`
	Devices           DeviceCounts       `json:"devices"`
	Dispatch          DispatchCounts     `json:"dispatch"`
	RunningTurns      uint64             `json:"runningTurns"`
	OccupiedSlots     uint64             `json:"occupiedSlots"`
	LifetimeTurns     uint64             `json:"lifetimeTurns"`
	Trees             uint64             `json:"trees"`
	Artifacts         ArtifactCounts     `json:"artifacts"`
	Results           ResultCounts       `json:"results"`
	Upgrade           *Upgrade           `json:"upgrade,omitempty"`
	ControllerUpgrade *ControllerUpgrade `json:"controllerUpgrade,omitempty"`
}

type Upgrade struct {
	TransactionID    string `json:"transactionId"`
	State            string `json:"state"`
	SourceVersion    string `json:"sourceVersion"`
	TargetVersion    string `json:"targetVersion"`
	CommitAuthorized bool   `json:"commitAuthorized"`
	FailureCode      string `json:"failureCode,omitempty"`
	UpdatedAt        int64  `json:"updatedAt"`
}

type ControllerUpgradeCounts struct {
	Total                int `json:"total"`
	Pending              int `json:"pending"`
	Prepared             int `json:"prepared"`
	Armed                int `json:"armed"`
	ActivationRequested  int `json:"activationRequested"`
	Qualified            int `json:"qualified"`
	Canceled             int `json:"canceled"`
	InterventionRequired int `json:"interventionRequired"`
}

type ControllerUpgrade struct {
	TransactionID      string                  `json:"transactionId"`
	State              string                  `json:"state"`
	SourceVersion      string                  `json:"sourceVersion"`
	TargetVersion      string                  `json:"targetVersion"`
	GlobalCommit       bool                    `json:"globalCommit"`
	Deadline           int64                   `json:"deadline"`
	CompletionDeadline int64                   `json:"completionDeadline,omitempty"`
	Participants       ControllerUpgradeCounts `json:"participants"`
	BrokerState        string                  `json:"brokerState,omitempty"`
	BrokerFailureCode  string                  `json:"brokerFailureCode,omitempty"`
	FailureCode        string                  `json:"failureCode,omitempty"`
	UpdatedAt          int64                   `json:"updatedAt"`
}

// DeviceCounts summarizes registered and usable devices without identifying
// any individual device.
type DeviceCounts struct {
	Registered   uint64 `json:"registered"`
	Online       uint64 `json:"online"`
	Connected    uint64 `json:"connected"`
	SyncReady    uint64 `json:"syncReady"`
	WorkerReady  uint64 `json:"workerReady"`
	Dispatchable uint64 `json:"dispatchable"`
}

// DispatchCounts summarizes current dispatch states and lifetime starts.
type DispatchCounts struct {
	Pending         uint64 `json:"pending"`
	Started         uint64 `json:"started"`
	Failed          uint64 `json:"failed"`
	LifetimeStarted uint64 `json:"lifetimeStarted"`
}

// ArtifactCounts summarizes retained artifact availability and publication.
type ArtifactCounts struct {
	Available     uint64 `json:"available"`
	Unchanged     uint64 `json:"unchanged"`
	CaptureFailed uint64 `json:"captureFailed"`
}

// ResultCounts summarizes broker-side result package delivery progress.
type ResultCounts struct {
	DeliveryPending    uint64 `json:"deliveryPending"`
	DetailsRetained    uint64 `json:"detailsRetained"`
	Delivered          uint64 `json:"delivered"`
	SourceAcknowledged uint64 `json:"sourceAcknowledged"`
	SourceReleased     uint64 `json:"sourceReleased"`
	DetailsCompacted   uint64 `json:"detailsCompacted"`
}

// Validate checks that a snapshot is safe for bounded status presentation.
func (s Snapshot) Validate() error {
	if err := s.TransportStatus.Validate(); err != nil {
		return fmt.Errorf("transport status: %w", err)
	}
	if !validOptionalText(s.Version) {
		return errors.New("version is not bounded display text")
	}
	if !validOptionalText(s.ControllerID) {
		return errors.New("controller ID is not bounded display text")
	}
	if !validOptionalText(s.InstanceID) {
		return errors.New("instance ID is not bounded display text")
	}
	if s.Devices.Online > s.Devices.Registered ||
		s.Devices.Connected > s.Devices.Registered ||
		s.Devices.SyncReady > s.Devices.Connected ||
		s.Devices.WorkerReady > s.Devices.Connected ||
		s.Devices.Dispatchable > s.Devices.SyncReady ||
		s.Devices.Dispatchable > s.Devices.WorkerReady {
		return errors.New("device counts are inconsistent")
	}
	if s.RunningTurns > s.OccupiedSlots {
		return errors.New("worker counts are inconsistent")
	}
	if s.Results.SourceAcknowledged > s.Results.Delivered ||
		s.Results.SourceReleased > s.Results.SourceAcknowledged ||
		s.Results.DetailsCompacted > s.Results.SourceReleased ||
		s.Results.DeliveryPending > s.Results.DetailsRetained {
		return errors.New("result package counts are inconsistent")
	}
	if s.Upgrade != nil && (!validOptionalText(s.Upgrade.TransactionID) ||
		!validOptionalText(s.Upgrade.State) || !validOptionalText(s.Upgrade.SourceVersion) ||
		!validOptionalText(s.Upgrade.TargetVersion) ||
		!validOptionalText(s.Upgrade.FailureCode) || s.Upgrade.UpdatedAt <= 0) {
		return errors.New("upgrade status is invalid")
	}
	if s.ControllerUpgrade != nil {
		u := s.ControllerUpgrade
		if !validOptionalText(u.TransactionID) || !validOptionalText(u.State) ||
			!validOptionalText(u.SourceVersion) || !validOptionalText(u.TargetVersion) ||
			!validOptionalText(u.BrokerState) || !validOptionalText(u.BrokerFailureCode) ||
			!validOptionalText(u.FailureCode) || u.Deadline <= 0 || u.CompletionDeadline < 0 ||
			u.UpdatedAt <= 0 || !validControllerUpgradeCounts(u.Participants) {
			return errors.New("controller upgrade status is invalid")
		}
	}
	return nil
}

func validControllerUpgradeCounts(counts ControllerUpgradeCounts) bool {
	values := []int{
		counts.Total, counts.Pending, counts.Prepared, counts.Armed, counts.ActivationRequested,
		counts.Qualified, counts.Canceled, counts.InterventionRequired,
	}
	for _, value := range values {
		if value < 0 || value > counts.Total {
			return false
		}
	}
	return counts.Pending+counts.Prepared+counts.Armed+counts.ActivationRequested+
		counts.Qualified+counts.Canceled+counts.InterventionRequired == counts.Total
}

func validOptionalText(value string) bool {
	if len(value) > maximumTextBytes || !utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if !unicode.IsPrint(character) {
			return false
		}
	}
	return true
}
