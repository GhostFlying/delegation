package protocol

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/identity"
)

const (
	MaximumAgentPage          = 32
	MaximumAgentPromptBytes   = 8 * 1024
	MaximumAgentTaskNameBytes = 64
	MaximumFailureCodeBytes   = 64
	MaximumAgentsPerTree      = 256
)

var (
	agentTaskNamePattern = regexp.MustCompile(`^[a-z0-9_]+$`)
	failureCodePattern   = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
)

type AgentSpawnStatus string

const (
	AgentSpawnPending AgentSpawnStatus = "pending"
	AgentSpawnStarted AgentSpawnStatus = "started"
	AgentSpawnFailed  AgentSpawnStatus = "failed"
)

func (s AgentSpawnStatus) Validate(failureCode string) error {
	switch s {
	case AgentSpawnPending, AgentSpawnStarted:
		if failureCode != "" {
			return errors.New("non-failed agent must not contain failureCode")
		}
	case AgentSpawnFailed:
		if err := ValidateFailureCode(failureCode); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported agent spawn status %q", s)
	}
	return nil
}

type AgentSpawnOutcome string

const (
	AgentSpawnOutcomeIndeterminate AgentSpawnOutcome = "indeterminate"
	AgentSpawnOutcomeBusy          AgentSpawnOutcome = "busy"
	AgentSpawnOutcomeStarted       AgentSpawnOutcome = "started"
	AgentSpawnOutcomeFailed        AgentSpawnOutcome = "failed"
)

func (o AgentSpawnOutcome) Validate(failureCode string) error {
	switch o {
	case AgentSpawnOutcomeIndeterminate, AgentSpawnOutcomeBusy, AgentSpawnOutcomeStarted:
		if failureCode != "" {
			return errors.New("non-failed agent spawn outcome must not contain failureCode")
		}
	case AgentSpawnOutcomeFailed:
		if err := ValidateFailureCode(failureCode); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported agent spawn outcome %q", o)
	}
	return nil
}

type SpawnAgentParams struct {
	SpawnID        string `json:"spawnId"`
	TargetDeviceID string `json:"targetDeviceId"`
	TaskName       string `json:"taskName"`
	Message        string `json:"message"`
}

func (p SpawnAgentParams) Validate() error {
	if err := identity.ValidateID(p.SpawnID); err != nil {
		return fmt.Errorf("spawnId %w", err)
	}
	if err := identity.ValidateID(p.TargetDeviceID); err != nil {
		return fmt.Errorf("targetDeviceId %w", err)
	}
	if err := ValidateAgentTaskName(p.TaskName); err != nil {
		return err
	}
	return ValidateAgentMessage(p.Message)
}

type SpawnWorkerParams struct {
	SpawnID  string `json:"spawnId"`
	AgentID  string `json:"agentId"`
	TaskName string `json:"taskName"`
	Message  string `json:"message"`
}

func (p SpawnWorkerParams) Validate() error {
	if err := identity.ValidateID(p.SpawnID); err != nil {
		return fmt.Errorf("spawnId %w", err)
	}
	if err := identity.ValidateID(p.AgentID); err != nil {
		return fmt.Errorf("agentId %w", err)
	}
	if err := ValidateAgentTaskName(p.TaskName); err != nil {
		return err
	}
	return ValidateAgentMessage(p.Message)
}

type AgentSummary struct {
	SpawnID     string                    `json:"spawnId"`
	Principal   control.PrincipalIdentity `json:"principal"`
	TaskName    string                    `json:"taskName"`
	Status      AgentSpawnStatus          `json:"status"`
	FailureCode string                    `json:"failureCode"`
	Sequence    uint64                    `json:"sequence"`
}

func (a AgentSummary) Validate() error {
	if err := identity.ValidateID(a.SpawnID); err != nil {
		return fmt.Errorf("spawnId %w", err)
	}
	if err := a.Principal.Validate(); err != nil {
		return fmt.Errorf("agent principal: %w", err)
	}
	if a.Principal.ParentAgentID == "" {
		return errors.New("agent principal must be a managed worker")
	}
	if err := ValidateAgentTaskName(a.TaskName); err != nil {
		return err
	}
	if err := a.Status.Validate(a.FailureCode); err != nil {
		return err
	}
	if a.Sequence < 1 || a.Sequence > MaximumAgentsPerTree {
		return errors.New("agent sequence is outside the supported range")
	}
	return nil
}

type SpawnAgentResult struct {
	Agent   AgentSummary      `json:"agent"`
	Outcome AgentSpawnOutcome `json:"outcome"`
}

func (r SpawnAgentResult) Validate() error {
	if err := r.Agent.Validate(); err != nil {
		return err
	}
	if err := r.Outcome.Validate(r.Agent.FailureCode); err != nil {
		return err
	}
	var expectedStatus AgentSpawnStatus
	switch r.Outcome {
	case AgentSpawnOutcomeIndeterminate, AgentSpawnOutcomeBusy:
		expectedStatus = AgentSpawnPending
	case AgentSpawnOutcomeStarted:
		expectedStatus = AgentSpawnStarted
	case AgentSpawnOutcomeFailed:
		expectedStatus = AgentSpawnFailed
	default:
		panic("validated agent spawn result has an unknown outcome")
	}
	if r.Agent.Status != expectedStatus {
		return errors.New("agent spawn outcome does not match durable status")
	}
	return nil
}

type SpawnWorkerResult struct {
	SpawnID     string                    `json:"spawnId"`
	Principal   control.PrincipalIdentity `json:"principal"`
	Outcome     AgentSpawnOutcome         `json:"outcome"`
	FailureCode string                    `json:"failureCode"`
}

func (r SpawnWorkerResult) Validate() error {
	if err := identity.ValidateID(r.SpawnID); err != nil {
		return fmt.Errorf("spawnId %w", err)
	}
	if err := r.Principal.Validate(); err != nil {
		return fmt.Errorf("worker principal: %w", err)
	}
	if r.Principal.ParentAgentID == "" {
		return errors.New("worker principal must contain parentAgentId")
	}
	return r.Outcome.Validate(r.FailureCode)
}

type ListAgentsParams struct {
	AfterSequence uint64 `json:"afterSequence,omitempty"`
	Limit         int    `json:"limit"`
}

func (p ListAgentsParams) Validate() error {
	if p.AfterSequence > MaximumAgentsPerTree {
		return errors.New("agent cursor exceeds the supported range")
	}
	if p.Limit < 1 || p.Limit > MaximumAgentPage {
		return fmt.Errorf("agent page limit must be from 1 through %d", MaximumAgentPage)
	}
	return nil
}

type ListAgentsResult struct {
	Agents       []AgentSummary `json:"agents"`
	NextSequence uint64         `json:"nextSequence,omitempty"`
}

func ValidateAgentTaskName(taskName string) error {
	if len(taskName) < 1 || len(taskName) > MaximumAgentTaskNameBytes ||
		!agentTaskNamePattern.MatchString(taskName) {
		return fmt.Errorf(
			"taskName must contain from 1 through %d lowercase letters, digits, or underscores",
			MaximumAgentTaskNameBytes,
		)
	}
	return nil
}

func ValidateAgentMessage(message string) error {
	if strings.TrimSpace(message) == "" || len(message) > MaximumAgentPromptBytes ||
		!utf8.ValidString(message) || strings.ContainsRune(message, '\x00') {
		return fmt.Errorf(
			"message must contain from 1 through %d bytes of valid text",
			MaximumAgentPromptBytes,
		)
	}
	return nil
}

func ValidateFailureCode(failureCode string) error {
	if len(failureCode) < 1 || len(failureCode) > MaximumFailureCodeBytes ||
		!failureCodePattern.MatchString(failureCode) {
		return fmt.Errorf(
			"failureCode must contain from 1 through %d lowercase letters, digits, or underscores",
			MaximumFailureCodeBytes,
		)
	}
	return nil
}
