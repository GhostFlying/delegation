package protocol

import (
	"errors"
	"fmt"

	"github.com/GhostFlying/delegation/internal/identity"
)

type AgentOperationAction string

const (
	AgentOperationSend      AgentOperationAction = "send"
	AgentOperationFollowup  AgentOperationAction = "followup"
	AgentOperationInterrupt AgentOperationAction = "interrupt"
)

type AgentOperationOutcome string

const (
	AgentOperationOutcomePending AgentOperationOutcome = "pending"
	AgentOperationOutcomeQueued  AgentOperationOutcome = "queued"
	AgentOperationOutcomeSteered AgentOperationOutcome = "steered"
	AgentOperationOutcomeStarted AgentOperationOutcome = "started"
	// Interrupted means the target app-server acknowledged turn/interrupt. The
	// later turn/completed notification remains authoritative for idle state.
	AgentOperationOutcomeInterrupted AgentOperationOutcome = "interrupted"
	AgentOperationOutcomeFailed      AgentOperationOutcome = "failed"
)

type SendAgentParams struct {
	AgentID   string `json:"agentId"`
	MessageID string `json:"messageId"`
	Message   string `json:"message"`
}

func (p SendAgentParams) Validate() error {
	return validateSendOperation(p.AgentID, p.MessageID, p.Message)
}

type FollowupAgentParams struct {
	OperationID string `json:"operationId"`
	AgentID     string `json:"agentId"`
	Message     string `json:"message"`
}

func (p FollowupAgentParams) Validate() error {
	if err := validateAgentOperationTarget(p.OperationID, p.AgentID); err != nil {
		return err
	}
	return ValidateAgentMessage(p.Message)
}

type InterruptAgentParams struct {
	OperationID string `json:"operationId"`
	AgentID     string `json:"agentId"`
}

func (p InterruptAgentParams) Validate() error {
	return validateAgentOperationTarget(p.OperationID, p.AgentID)
}

type SendWorkerParams struct {
	AgentID   string `json:"agentId"`
	MessageID string `json:"messageId"`
	Message   string `json:"message"`
}

func (p SendWorkerParams) Validate() error {
	return validateSendOperation(p.AgentID, p.MessageID, p.Message)
}

type FollowupWorkerParams struct {
	OperationID string `json:"operationId"`
	AgentID     string `json:"agentId"`
	Message     string `json:"message"`
}

func (p FollowupWorkerParams) Validate() error {
	if err := validateAgentOperationTarget(p.OperationID, p.AgentID); err != nil {
		return err
	}
	return ValidateAgentMessage(p.Message)
}

type InterruptWorkerParams struct {
	OperationID string `json:"operationId"`
	AgentID     string `json:"agentId"`
}

func (p InterruptWorkerParams) Validate() error {
	return validateAgentOperationTarget(p.OperationID, p.AgentID)
}

type AgentOperationResult struct {
	OperationID string                `json:"operationId"`
	AgentID     string                `json:"agentId"`
	Action      AgentOperationAction  `json:"action"`
	Outcome     AgentOperationOutcome `json:"outcome"`
	FailureCode string                `json:"failureCode"`
}

func (r AgentOperationResult) Validate() error {
	if err := validateAgentOperationTarget(r.OperationID, r.AgentID); err != nil {
		return err
	}
	if err := r.Action.Validate(); err != nil {
		return err
	}
	return r.Outcome.Validate(r.Action, r.FailureCode)
}

type WorkerOperationResult = AgentOperationResult

func (a AgentOperationAction) Validate() error {
	switch a {
	case AgentOperationSend, AgentOperationFollowup, AgentOperationInterrupt:
		return nil
	default:
		return fmt.Errorf("unsupported agent operation action %q", a)
	}
}

func (o AgentOperationOutcome) Validate(action AgentOperationAction, failureCode string) error {
	if err := action.Validate(); err != nil {
		return err
	}
	switch o {
	case AgentOperationOutcomePending,
		AgentOperationOutcomeQueued,
		AgentOperationOutcomeSteered,
		AgentOperationOutcomeStarted,
		AgentOperationOutcomeInterrupted:
		if failureCode != "" {
			return errors.New("non-failed agent operation must not contain failureCode")
		}
		if !action.accepts(o) {
			return fmt.Errorf("agent operation action %q does not support outcome %q", action, o)
		}
	case AgentOperationOutcomeFailed:
		if err := ValidateFailureCode(failureCode); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported agent operation outcome %q", o)
	}
	return nil
}

func (a AgentOperationAction) accepts(outcome AgentOperationOutcome) bool {
	if outcome == AgentOperationOutcomePending {
		return true
	}
	switch a {
	case AgentOperationSend:
		return outcome == AgentOperationOutcomeQueued || outcome == AgentOperationOutcomeSteered
	case AgentOperationFollowup:
		return outcome == AgentOperationOutcomeStarted
	case AgentOperationInterrupt:
		return outcome == AgentOperationOutcomeInterrupted
	default:
		return false
	}
}

func validateSendOperation(agentID, messageID, message string) error {
	if err := identity.ValidateID(agentID); err != nil {
		return fmt.Errorf("agentId %w", err)
	}
	if err := identity.ValidateID(messageID); err != nil {
		return fmt.Errorf("messageId %w", err)
	}
	return ValidateMailboxMessage(message)
}

func validateAgentOperationTarget(operationID, agentID string) error {
	if err := identity.ValidateID(operationID); err != nil {
		return fmt.Errorf("operationId %w", err)
	}
	if err := identity.ValidateID(agentID); err != nil {
		return fmt.Errorf("agentId %w", err)
	}
	return nil
}
