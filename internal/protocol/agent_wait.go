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
	MaximumAgentWaitMillis     = 25_000
)

type WaitAgentParams struct {
	MailboxCursor   uint64 `json:"mailboxCursor,omitempty"`
	LifecycleCursor uint64 `json:"lifecycleCursor,omitempty"`
	TimeoutMillis   int    `json:"timeoutMillis"`
	MessageLimit    int    `json:"messageLimit"`
	ActivityLimit   int    `json:"activityLimit"`
}

func (p WaitAgentParams) Validate() error {
	if p.MailboxCursor > math.MaxInt64 || p.LifecycleCursor > math.MaxInt64 {
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
	Messages            []MailboxMessage         `json:"messages"`
	Activities          []AgentLifecycleActivity `json:"activities"`
	NextMailboxCursor   uint64                   `json:"nextMailboxCursor"`
	NextLifecycleCursor uint64                   `json:"nextLifecycleCursor"`
	MoreMessages        bool                     `json:"moreMessages"`
	MoreActivities      bool                     `json:"moreActivities"`
}
