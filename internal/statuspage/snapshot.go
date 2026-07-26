package statuspage

import (
	"context"
	"errors"
	"unicode"
	"unicode/utf8"
)

const maximumTextBytes = 256

// Provider returns one internally consistent broker status snapshot.
type Provider func(context.Context) (Snapshot, error)

// Snapshot is the aggregate, transport-neutral broker status presented by the
// status endpoints. It intentionally contains no per-device or per-task data.
type Snapshot struct {
	Version       string         `json:"version,omitempty"`
	UptimeSeconds uint64         `json:"uptimeSeconds"`
	ControllerID  string         `json:"controllerId,omitempty"`
	Devices       DeviceCounts   `json:"devices"`
	Dispatch      DispatchCounts `json:"dispatch"`
	RunningTurns  uint64         `json:"runningTurns"`
	OccupiedSlots uint64         `json:"occupiedSlots"`
	LifetimeTurns uint64         `json:"lifetimeTurns"`
	Trees         uint64         `json:"trees"`
	Artifacts     ArtifactCounts `json:"artifacts"`
	Results       ResultCounts   `json:"results"`
}

// DeviceCounts summarizes registered and usable devices without identifying
// any individual device.
type DeviceCounts struct {
	Registered uint64 `json:"registered"`
	Online     uint64 `json:"online"`
	Connected  uint64 `json:"connected"`
	SyncReady  uint64 `json:"syncReady"`
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
	Delivered          uint64 `json:"delivered"`
	SourceAcknowledged uint64 `json:"sourceAcknowledged"`
	SourceReleased     uint64 `json:"sourceReleased"`
}

// Validate checks that a snapshot is safe for bounded status presentation.
func (s Snapshot) Validate() error {
	if !validOptionalText(s.Version) {
		return errors.New("version is not bounded display text")
	}
	if !validOptionalText(s.ControllerID) {
		return errors.New("controller ID is not bounded display text")
	}
	if s.Devices.Online > s.Devices.Registered ||
		s.Devices.Connected > s.Devices.Registered ||
		s.Devices.SyncReady > s.Devices.Connected {
		return errors.New("device counts are inconsistent")
	}
	if s.RunningTurns > s.OccupiedSlots {
		return errors.New("worker counts are inconsistent")
	}
	if s.Results.SourceAcknowledged > s.Results.Delivered ||
		s.Results.SourceReleased > s.Results.SourceAcknowledged {
		return errors.New("result package counts are inconsistent")
	}
	return nil
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
