package protocol

import (
	"errors"
	"fmt"
	"math"

	"github.com/GhostFlying/delegation/internal/identity"
)

const (
	MethodWaitAgent            = "agent.wait"
	MaximumAgentWaitMessages   = 2
	MaximumAgentWaitActivities = 16
	MaximumAgentWaitArtifacts  = 1
	MaximumAgentWaitResults    = 1
	MaximumAgentWaitMillis     = 25_000
)

type WaitAgentParams struct {
	MailboxCursor   uint64 `json:"mailboxCursor,omitempty"`
	LifecycleCursor uint64 `json:"lifecycleCursor,omitempty"`
	ArtifactCursor  uint64 `json:"artifactCursor,omitempty"`
	ResultCursor    uint64 `json:"resultCursor,omitempty"`
	TimeoutMillis   int    `json:"timeoutMillis"`
	MessageLimit    int    `json:"messageLimit"`
	ActivityLimit   int    `json:"activityLimit"`
	ArtifactLimit   int    `json:"artifactLimit"`
	ResultLimit     int    `json:"resultLimit"`
}

func (p WaitAgentParams) Validate() error {
	if p.MailboxCursor > math.MaxInt64 || p.LifecycleCursor > math.MaxInt64 ||
		p.ArtifactCursor > math.MaxInt64 || p.ResultCursor > math.MaxInt64 {
		return errors.New("agent wait cursor exceeds the supported range")
	}
	if p.TimeoutMillis < 0 || p.TimeoutMillis > MaximumAgentWaitMillis {
		return fmt.Errorf("timeoutMillis must be from 0 through %d", MaximumAgentWaitMillis)
	}
	if p.MessageLimit < 1 || p.MessageLimit > MaximumAgentWaitMessages {
		return fmt.Errorf("messageLimit must be from 1 through %d", MaximumAgentWaitMessages)
	}
	if p.ActivityLimit < 1 || p.ActivityLimit > MaximumAgentWaitActivities {
		return fmt.Errorf("activityLimit must be from 1 through %d", MaximumAgentWaitActivities)
	}
	if p.ArtifactLimit < 1 || p.ArtifactLimit > MaximumAgentWaitArtifacts {
		return fmt.Errorf("artifactLimit must be from 1 through %d", MaximumAgentWaitArtifacts)
	}
	if p.ResultLimit < 1 || p.ResultLimit > MaximumAgentWaitResults {
		return fmt.Errorf("resultLimit must be from 1 through %d", MaximumAgentWaitResults)
	}
	return nil
}

type ResultPackageAvailability string

const (
	ResultPackageUnverified ResultPackageAvailability = "unverified"
	ResultPackageAvailable  ResultPackageAvailability = "available"
	ResultPackageEvicted    ResultPackageAvailability = "evicted"
)

func (a ResultPackageAvailability) Validate() error {
	switch a {
	case ResultPackageUnverified, ResultPackageAvailable, ResultPackageEvicted:
		return nil
	default:
		return fmt.Errorf("unsupported result package availability %q", a)
	}
}

// ResultPackageHandle is bounded metadata for a root-local result package. The
// broker can prove delivery ordering but not local byte availability, so only
// the root peer's local bridge may replace unverified with available or evicted.
type ResultPackageHandle struct {
	Manifest     ResultManifest            `json:"manifest"`
	Availability ResultPackageAvailability `json:"availability"`
	Sequence     uint64                    `json:"sequence"`
	DeliveredAt  int64                     `json:"deliveredAt"`
}

func (h ResultPackageHandle) Validate() error {
	if err := h.Manifest.Validate(); err != nil {
		return fmt.Errorf("manifest: %w", err)
	}
	if err := h.Availability.Validate(); err != nil {
		return err
	}
	if h.Sequence == 0 || h.Sequence > math.MaxInt64 {
		return errors.New("result package sequence is outside the supported range")
	}
	if h.DeliveredAt < 0 {
		return errors.New("result package deliveredAt must not be negative")
	}
	return nil
}

type AgentLifecycleActivity struct {
	AgentID        string               `json:"agentId"`
	TargetDeviceID string               `json:"targetDeviceId"`
	TargetRevision uint64               `json:"targetRevision"`
	Phase          WorkerLifecyclePhase `json:"phase"`
	FailureCode    string               `json:"failureCode"`
	Sequence       uint64               `json:"sequence"`
	ObservedAt     int64                `json:"observedAt"`
}

func (a AgentLifecycleActivity) Validate() error {
	if err := identity.ValidateID(a.AgentID); err != nil {
		return fmt.Errorf("agentId %w", err)
	}
	if err := identity.ValidateID(a.TargetDeviceID); err != nil {
		return fmt.Errorf("targetDeviceId %w", err)
	}
	if a.TargetRevision == 0 || a.TargetRevision > math.MaxInt64 ||
		a.Sequence == 0 || a.Sequence > math.MaxInt64 {
		return errors.New("agent lifecycle activity revision or sequence is outside the supported range")
	}
	if a.ObservedAt < 0 {
		return errors.New("agent lifecycle activity observedAt must not be negative")
	}
	return a.Phase.Validate(a.FailureCode)
}

type WaitAgentResult struct {
	Messages            []MailboxMessage          `json:"messages"`
	Activities          []AgentLifecycleActivity  `json:"activities"`
	Artifacts           []ChangesArtifactMetadata `json:"artifacts"`
	Results             []ResultPackageHandle     `json:"results"`
	NextMailboxCursor   uint64                    `json:"nextMailboxCursor"`
	NextLifecycleCursor uint64                    `json:"nextLifecycleCursor"`
	NextArtifactCursor  uint64                    `json:"nextArtifactCursor"`
	NextResultCursor    uint64                    `json:"nextResultCursor"`
	MoreMessages        bool                      `json:"moreMessages"`
	MoreActivities      bool                      `json:"moreActivities"`
	MoreArtifacts       bool                      `json:"moreArtifacts"`
	MoreResults         bool                      `json:"moreResults"`
}
