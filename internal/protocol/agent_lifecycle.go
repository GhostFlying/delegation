package protocol

import (
	"errors"
	"fmt"
	"math"

	"github.com/GhostFlying/delegation/internal/identity"
)

const MaximumWorkerLifecyclePage = 32

type WorkerLifecyclePhase string

const (
	WorkerLifecycleReserved    WorkerLifecyclePhase = "reserved"
	WorkerLifecyclePending     WorkerLifecyclePhase = "pending"
	WorkerLifecycleStarting    WorkerLifecyclePhase = "starting"
	WorkerLifecyclePreflight   WorkerLifecyclePhase = "preflight"
	WorkerLifecycleReady       WorkerLifecyclePhase = "ready"
	WorkerLifecycleRunning     WorkerLifecyclePhase = "running"
	WorkerLifecycleFinalizing  WorkerLifecyclePhase = "finalizing"
	WorkerLifecycleIdle        WorkerLifecyclePhase = "idle"
	WorkerLifecycleInterrupted WorkerLifecyclePhase = "interrupted"
	WorkerLifecycleFailed      WorkerLifecyclePhase = "failed"
)

func (p WorkerLifecyclePhase) Validate(failureCode string) error {
	switch p {
	case WorkerLifecycleReserved,
		WorkerLifecyclePending,
		WorkerLifecycleStarting,
		WorkerLifecyclePreflight,
		WorkerLifecycleReady,
		WorkerLifecycleRunning,
		WorkerLifecycleFinalizing,
		WorkerLifecycleIdle,
		WorkerLifecycleInterrupted:
		if failureCode != "" {
			return errors.New("non-failed worker lifecycle must not contain failureCode")
		}
	case WorkerLifecycleFailed:
		if err := ValidateFailureCode(failureCode); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported worker lifecycle phase %q", p)
	}
	return nil
}

type WorkerLifecycleSnapshot struct {
	TreeID        string               `json:"treeId"`
	AgentID       string               `json:"agentId"`
	Revision      uint64               `json:"revision"`
	Phase         WorkerLifecyclePhase `json:"phase"`
	FailureCode   string               `json:"failureCode"`
	CodexThreadID string               `json:"codexThreadId"`
	ActiveTurnID  string               `json:"activeTurnId"`
}

func (s WorkerLifecycleSnapshot) Validate() error {
	if err := identity.ValidateID(s.TreeID); err != nil {
		return fmt.Errorf("treeId %w", err)
	}
	if err := identity.ValidateID(s.AgentID); err != nil {
		return fmt.Errorf("agentId %w", err)
	}
	if s.Revision == 0 || s.Revision > math.MaxInt64 {
		return errors.New("worker lifecycle revision is outside the supported range")
	}
	if err := s.Phase.Validate(s.FailureCode); err != nil {
		return err
	}
	if s.CodexThreadID != "" {
		if err := identity.ValidateID(s.CodexThreadID); err != nil {
			return fmt.Errorf("codexThreadId %w", err)
		}
	}
	if s.ActiveTurnID != "" {
		if err := identity.ValidateID(s.ActiveTurnID); err != nil {
			return fmt.Errorf("activeTurnId %w", err)
		}
	}
	switch s.Phase {
	case WorkerLifecycleReserved:
		if s.CodexThreadID != "" || s.ActiveTurnID != "" {
			return errors.New("reserved worker lifecycle must not contain thread or turn identity")
		}
	case WorkerLifecyclePending, WorkerLifecycleStarting:
		if s.ActiveTurnID != "" {
			return errors.New("pending or starting worker lifecycle must not contain activeTurnId")
		}
	case WorkerLifecyclePreflight, WorkerLifecycleReady, WorkerLifecycleIdle:
		if s.CodexThreadID == "" || s.ActiveTurnID != "" {
			return errors.New("worker lifecycle phase requires a thread and no active turn")
		}
	case WorkerLifecycleRunning, WorkerLifecycleFinalizing:
		if s.CodexThreadID == "" || s.ActiveTurnID == "" {
			return errors.New("running or finalizing worker lifecycle requires thread and turn identity")
		}
	case WorkerLifecycleInterrupted:
		if s.CodexThreadID == "" {
			return errors.New("interrupted worker lifecycle requires codexThreadId")
		}
	case WorkerLifecycleFailed:
		if s.ActiveTurnID != "" {
			return errors.New("failed worker lifecycle must not contain activeTurnId")
		}
	}
	return nil
}

type SyncWorkerLifecycleParams struct {
	BaseRevision    uint64                    `json:"baseRevision"`
	ThroughRevision uint64                    `json:"throughRevision"`
	Complete        bool                      `json:"complete"`
	Workers         []WorkerLifecycleSnapshot `json:"workers"`
}

func (p SyncWorkerLifecycleParams) Validate() error {
	if p.BaseRevision > math.MaxInt64 || p.ThroughRevision > math.MaxInt64 {
		return errors.New("worker lifecycle page revision exceeds the supported range")
	}
	if p.ThroughRevision < p.BaseRevision {
		return errors.New("throughRevision must not precede baseRevision")
	}
	if len(p.Workers) > MaximumWorkerLifecyclePage {
		return fmt.Errorf("worker lifecycle page must contain at most %d workers", MaximumWorkerLifecyclePage)
	}
	if !p.Complete {
		if len(p.Workers) != MaximumWorkerLifecyclePage {
			return fmt.Errorf("incomplete worker lifecycle page must contain %d workers", MaximumWorkerLifecyclePage)
		}
		if p.BaseRevision == p.ThroughRevision {
			return errors.New("incomplete worker lifecycle page must advance toward throughRevision")
		}
	}

	previous := p.BaseRevision
	seen := make(map[string]struct{}, len(p.Workers))
	for index, worker := range p.Workers {
		if err := worker.Validate(); err != nil {
			return fmt.Errorf("workers[%d]: %w", index, err)
		}
		if worker.Revision <= previous {
			return errors.New("worker lifecycle revisions must increase strictly after baseRevision")
		}
		if worker.Revision > p.ThroughRevision {
			return errors.New("worker lifecycle revision exceeds throughRevision")
		}
		key := worker.TreeID + "\x00" + worker.AgentID
		if _, exists := seen[key]; exists {
			return errors.New("worker lifecycle page contains a duplicate agent")
		}
		seen[key] = struct{}{}
		previous = worker.Revision
	}
	if !p.Complete && previous == p.ThroughRevision {
		return errors.New("page ending at throughRevision must be complete")
	}
	return nil
}

func (p SyncWorkerLifecycleParams) AppliedRevision() (uint64, error) {
	if err := p.Validate(); err != nil {
		return 0, err
	}
	if p.Complete {
		return p.ThroughRevision, nil
	}
	return p.Workers[len(p.Workers)-1].Revision, nil
}

type SyncWorkerLifecycleResult struct {
	AppliedRevision uint64 `json:"appliedRevision"`
}

func (r SyncWorkerLifecycleResult) Validate() error {
	if r.AppliedRevision > math.MaxInt64 {
		return errors.New("appliedRevision exceeds the supported range")
	}
	return nil
}
