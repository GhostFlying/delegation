package protocol

import (
	"errors"
	"fmt"
	"math"
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
	WorkspaceID    string `json:"workspaceId,omitempty"`
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
	if p.WorkspaceID != "" {
		if err := identity.ValidateID(p.WorkspaceID); err != nil {
			return fmt.Errorf("workspaceId %w", err)
		}
	}
	return ValidateAgentMessage(p.Message)
}

type SpawnWorkerParams struct {
	SpawnID     string `json:"spawnId"`
	AgentID     string `json:"agentId"`
	TaskName    string `json:"taskName"`
	Message     string `json:"message"`
	WorkspaceID string `json:"workspaceId,omitempty"`
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
	if p.WorkspaceID != "" {
		if err := identity.ValidateID(p.WorkspaceID); err != nil {
			return fmt.Errorf("workspaceId %w", err)
		}
	}
	return ValidateAgentMessage(p.Message)
}

type AgentSummary struct {
	SpawnID          string                    `json:"spawnId"`
	Principal        control.PrincipalIdentity `json:"principal"`
	TaskName         string                    `json:"taskName"`
	SpawnStatus      AgentSpawnStatus          `json:"spawnStatus"`
	SpawnFailureCode string                    `json:"spawnFailureCode"`
	WorkspaceID      string                    `json:"workspaceId,omitempty"`
	Sequence         uint64                    `json:"sequence"`
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
	if a.WorkspaceID != "" {
		if err := identity.ValidateID(a.WorkspaceID); err != nil {
			return fmt.Errorf("workspaceId %w", err)
		}
	}
	if err := a.SpawnStatus.Validate(a.SpawnFailureCode); err != nil {
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
	if err := r.Outcome.Validate(r.Agent.SpawnFailureCode); err != nil {
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
	if r.Agent.SpawnStatus != expectedStatus {
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
	Agents       []AgentState `json:"agents"`
	NextSequence uint64       `json:"nextSequence,omitempty"`
}

type AgentLifecycleFreshness string

const (
	AgentLifecycleCurrent AgentLifecycleFreshness = "current"
	AgentLifecycleSyncing AgentLifecycleFreshness = "syncing"
	AgentLifecycleOffline AgentLifecycleFreshness = "offline"
	AgentLifecycleMissing AgentLifecycleFreshness = "missing"
)

type AgentEffectiveStatus string

const (
	AgentEffectiveIndeterminate AgentEffectiveStatus = "indeterminate"
	AgentEffectiveStarting      AgentEffectiveStatus = "starting"
	AgentEffectiveRunning       AgentEffectiveStatus = "running"
	AgentEffectiveFinalizing    AgentEffectiveStatus = "finalizing"
	AgentEffectiveIdle          AgentEffectiveStatus = "idle"
	AgentEffectiveInterrupted   AgentEffectiveStatus = "interrupted"
	AgentEffectiveFailed        AgentEffectiveStatus = "failed"
)

type AgentFailureSource string

const (
	AgentFailureSourceSpawn     AgentFailureSource = "spawn"
	AgentFailureSourceLifecycle AgentFailureSource = "lifecycle"
)

// AgentState combines an immutable spawn receipt with the most recent
// lifecycle observation and a generation-consistent target connection view.
type AgentState struct {
	SpawnID                 string                    `json:"spawnId"`
	Principal               control.PrincipalIdentity `json:"principal"`
	TaskName                string                    `json:"taskName"`
	SpawnStatus             AgentSpawnStatus          `json:"spawnStatus"`
	SpawnFailureCode        string                    `json:"spawnFailureCode"`
	WorkspaceID             string                    `json:"workspaceId,omitempty"`
	Sequence                uint64                    `json:"sequence"`
	LifecyclePhase          WorkerLifecyclePhase      `json:"lifecyclePhase"`
	LifecycleFailureCode    string                    `json:"lifecycleFailureCode"`
	LifecycleTargetRevision uint64                    `json:"lifecycleTargetRevision"`
	LifecycleObservedAt     int64                     `json:"lifecycleObservedAt"`
	LifecycleFreshness      AgentLifecycleFreshness   `json:"lifecycleFreshness"`
	EffectiveStatus         AgentEffectiveStatus      `json:"effectiveStatus"`
	EffectiveFailureCode    string                    `json:"effectiveFailureCode"`
	FailureSource           AgentFailureSource        `json:"failureSource"`
	TargetDispatchable      bool                      `json:"targetDispatchable"`
}

func (a AgentState) SpawnReceipt() AgentSummary {
	return AgentSummary{
		SpawnID: a.SpawnID, Principal: a.Principal, TaskName: a.TaskName,
		SpawnStatus: a.SpawnStatus, SpawnFailureCode: a.SpawnFailureCode,
		WorkspaceID: a.WorkspaceID, Sequence: a.Sequence,
	}
}

func (a AgentState) Validate() error {
	if err := a.SpawnReceipt().Validate(); err != nil {
		return err
	}
	switch a.LifecycleFreshness {
	case AgentLifecycleMissing:
		if a.LifecyclePhase != "" || a.LifecycleFailureCode != "" ||
			a.LifecycleTargetRevision != 0 || a.LifecycleObservedAt != 0 {
			return errors.New("missing lifecycle must not contain lifecycle state")
		}
	case AgentLifecycleCurrent, AgentLifecycleSyncing, AgentLifecycleOffline:
		if a.LifecycleTargetRevision == 0 || a.LifecycleTargetRevision > math.MaxInt64 ||
			a.LifecycleObservedAt < 0 {
			return errors.New("agent lifecycle metadata is invalid")
		}
		if err := a.LifecyclePhase.Validate(a.LifecycleFailureCode); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported agent lifecycle freshness %q", a.LifecycleFreshness)
	}
	if a.TargetDispatchable && (a.LifecycleFreshness == AgentLifecycleSyncing ||
		a.LifecycleFreshness == AgentLifecycleOffline) {
		return errors.New("stale agent lifecycle target must not be dispatchable")
	}
	effective, failureCode, failureSource := ProjectAgentEffectiveState(
		a.SpawnStatus, a.SpawnFailureCode, a.LifecyclePhase,
		a.LifecycleFailureCode, a.LifecycleFreshness,
	)
	if a.EffectiveStatus != effective || a.EffectiveFailureCode != failureCode ||
		a.FailureSource != failureSource {
		return errors.New("agent effective state does not match its authorities")
	}
	return nil
}

func ProjectAgentEffectiveState(
	spawnStatus AgentSpawnStatus,
	spawnFailureCode string,
	lifecyclePhase WorkerLifecyclePhase,
	lifecycleFailureCode string,
	freshness AgentLifecycleFreshness,
) (AgentEffectiveStatus, string, AgentFailureSource) {
	if spawnStatus == AgentSpawnFailed {
		return AgentEffectiveFailed, spawnFailureCode, AgentFailureSourceSpawn
	}
	if lifecyclePhase == WorkerLifecycleFailed && freshness != AgentLifecycleMissing {
		return AgentEffectiveFailed, lifecycleFailureCode, AgentFailureSourceLifecycle
	}
	if freshness != AgentLifecycleCurrent {
		return AgentEffectiveIndeterminate, "", ""
	}
	switch lifecyclePhase {
	case WorkerLifecycleReserved, WorkerLifecyclePending, WorkerLifecycleStarting,
		WorkerLifecyclePreflight, WorkerLifecycleReady:
		return AgentEffectiveStarting, "", ""
	case WorkerLifecycleRunning:
		return AgentEffectiveRunning, "", ""
	case WorkerLifecycleFinalizing:
		return AgentEffectiveFinalizing, "", ""
	case WorkerLifecycleIdle:
		return AgentEffectiveIdle, "", ""
	case WorkerLifecycleInterrupted:
		return AgentEffectiveInterrupted, "", ""
	default:
		return AgentEffectiveIndeterminate, "", ""
	}
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
