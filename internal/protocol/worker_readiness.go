package protocol

import (
	"errors"
	"fmt"
	"regexp"
	"time"
)

const MaximumReadinessAttempts = 5

var workerReadinessAttemptOffsets = [...]time.Duration{
	0,
	10 * time.Second,
	30 * time.Second,
	2 * time.Minute,
	7 * time.Minute,
}

func WorkerReadinessAttemptOffset(attempt int) (time.Duration, error) {
	if attempt < 0 || attempt >= len(workerReadinessAttemptOffsets) {
		return 0, fmt.Errorf(
			"worker readiness attempt must be from 0 through %d",
			len(workerReadinessAttemptOffsets)-1,
		)
	}
	return workerReadinessAttemptOffsets[attempt], nil
}

type WorkerReadinessState string

const (
	WorkerReadinessPending              WorkerReadinessState = "pending"
	WorkerReadinessReady                WorkerReadinessState = "ready"
	WorkerReadinessInterventionRequired WorkerReadinessState = "intervention_required"
)

const (
	WorkerRequalificationExhausted = "worker_requalification_exhausted"
	WorkerManagedHomeInvalid       = "managed_home_invalid"
	WorkerProfileUnsupported       = "profile_arguments_unsupported"
	WorkerHostUnsupported          = "unsupported_host"
	WorkerServiceIdentityInvalid   = "service_identity_invalid"
	WorkerRepairRollbackFailed     = "rollback_failed"
	WorkerInterventionRequiredCode = "intervention_required"
)

type WorkerInterventionRequiredErrorData struct {
	Code        string `json:"code"`
	FailureCode string `json:"failureCode"`
}

func (d WorkerInterventionRequiredErrorData) Validate() error {
	if d.Code != WorkerInterventionRequiredCode {
		return errors.New("worker intervention error code is invalid")
	}
	if !readinessFailurePattern.MatchString(d.FailureCode) {
		return errors.New("worker intervention failureCode is invalid")
	}
	return nil
}

var readinessDigestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
var readinessFailurePattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)

// WorkerReadiness is the complete durable execution-qualification snapshot.
// Times are Unix milliseconds so the same value can be persisted and sent on
// the wire without platform-dependent time encodings.
type WorkerReadiness struct {
	Epoch          uint64               `json:"epoch"`
	State          WorkerReadinessState `json:"state"`
	AttemptCount   int                  `json:"attemptCount"`
	RuntimeDigest  string               `json:"runtimeDigest"`
	ConfigDigest   string               `json:"configDigest"`
	EpochStartedAt int64                `json:"epochStartedAt"`
	NextAttemptAt  int64                `json:"nextAttemptAt"`
	LastAttemptAt  int64                `json:"lastAttemptAt"`
	FailureCode    string               `json:"failureCode"`
	UpdatedAt      int64                `json:"updatedAt"`
}

// WorkerReadinessCursor identifies one durable readiness revision without
// exposing the runtime or configuration digests to local subscribers.
type WorkerReadinessCursor struct {
	Epoch     uint64 `json:"epoch"`
	UpdatedAt int64  `json:"updatedAt"`
}

func (c WorkerReadinessCursor) Validate() error {
	if c.Epoch == 0 || c.UpdatedAt <= 0 {
		return errors.New("worker readiness cursor must be positive")
	}
	return nil
}

func (r WorkerReadiness) Cursor() WorkerReadinessCursor {
	return WorkerReadinessCursor{Epoch: r.Epoch, UpdatedAt: r.UpdatedAt}
}

func (c WorkerReadinessCursor) Before(readiness WorkerReadiness) bool {
	return c.Epoch < readiness.Epoch ||
		c.Epoch == readiness.Epoch && c.UpdatedAt < readiness.UpdatedAt
}

func NewPendingWorkerReadiness(runtimeDigest, configDigest string, nowMillis int64) WorkerReadiness {
	return WorkerReadiness{
		Epoch: 1, State: WorkerReadinessPending, RuntimeDigest: runtimeDigest,
		ConfigDigest: configDigest, EpochStartedAt: nowMillis, NextAttemptAt: nowMillis,
		UpdatedAt: nowMillis,
	}
}

func (r WorkerReadiness) Validate() error {
	if r.Epoch == 0 {
		return errors.New("worker readiness epoch must be positive")
	}
	if !readinessDigestPattern.MatchString(r.RuntimeDigest) ||
		!readinessDigestPattern.MatchString(r.ConfigDigest) {
		return errors.New("worker readiness digests must be lowercase SHA-256")
	}
	if r.AttemptCount < 0 || r.AttemptCount > MaximumReadinessAttempts {
		return fmt.Errorf("worker readiness attemptCount must be from 0 through %d", MaximumReadinessAttempts)
	}
	if r.EpochStartedAt <= 0 || r.NextAttemptAt < 0 || r.LastAttemptAt < 0 || r.UpdatedAt <= 0 {
		return errors.New("worker readiness epoch and update timestamps must be positive")
	}
	if r.UpdatedAt < r.EpochStartedAt ||
		r.LastAttemptAt != 0 && r.LastAttemptAt < r.EpochStartedAt ||
		r.NextAttemptAt != 0 && r.NextAttemptAt < r.EpochStartedAt {
		return errors.New("worker readiness timestamps precede the epoch")
	}
	if r.FailureCode != "" && !readinessFailurePattern.MatchString(r.FailureCode) {
		return errors.New("worker readiness failureCode is invalid")
	}
	switch r.State {
	case WorkerReadinessPending:
		if r.AttemptCount < MaximumReadinessAttempts && r.NextAttemptAt == 0 {
			return errors.New("pending worker readiness must have a scheduled remaining attempt")
		}
		if r.AttemptCount == MaximumReadinessAttempts && r.NextAttemptAt != 0 {
			return errors.New("pending worker readiness with a consumed attempt budget must not be scheduled")
		}
	case WorkerReadinessReady:
		if r.AttemptCount < 1 || r.NextAttemptAt != 0 || r.FailureCode != "" {
			return errors.New("ready worker readiness is inconsistent")
		}
	case WorkerReadinessInterventionRequired:
		if r.NextAttemptAt != 0 || r.FailureCode == "" {
			return errors.New("intervention-required worker readiness is inconsistent")
		}
	default:
		return fmt.Errorf("unsupported worker readiness state %q", r.State)
	}
	if r.AttemptCount == 0 && r.LastAttemptAt != 0 {
		return errors.New("worker readiness without attempts must not have lastAttemptAt")
	}
	if r.AttemptCount > 0 && r.LastAttemptAt == 0 {
		return errors.New("worker readiness attempts require lastAttemptAt")
	}
	return nil
}

func (r WorkerReadiness) IsReady() bool {
	return r.State == WorkerReadinessReady
}

type UpdateWorkerReadinessParams struct {
	Readiness WorkerReadiness `json:"readiness"`
}

func (p UpdateWorkerReadinessParams) Validate() error {
	return p.Readiness.Validate()
}

type UpdateWorkerReadinessResult struct {
	Readiness WorkerReadiness `json:"readiness"`
}

func (r UpdateWorkerReadinessResult) Validate() error {
	return r.Readiness.Validate()
}
