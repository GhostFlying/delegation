package protocol

import (
	"encoding/json"
	"strings"
	"testing"
)

const (
	protocolOperationID        = "123e4567-e89b-42d3-a456-426614174320"
	protocolOperationAgentID   = "123e4567-e89b-42d3-a456-426614174321"
	protocolOperationMessageID = "123e4567-e89b-42d3-a456-426614174322"
)

func TestAgentOperationParamsValidateBoundaries(t *testing.T) {
	send := SendAgentParams{
		AgentID:   protocolOperationAgentID,
		MessageID: protocolOperationMessageID,
		Message:   strings.Repeat("m", MaximumMailboxMessageBytes),
	}
	followup := FollowupAgentParams{
		OperationID: protocolOperationID,
		AgentID:     protocolOperationAgentID,
		Message:     strings.Repeat("f", MaximumAgentPromptBytes),
	}
	interrupt := InterruptAgentParams{OperationID: protocolOperationID, AgentID: protocolOperationAgentID}
	for name, input := range map[string]interface{ Validate() error }{
		"send agent":       send,
		"followup agent":   followup,
		"interrupt agent":  interrupt,
		"send worker":      SendWorkerParams(send),
		"followup worker":  FollowupWorkerParams(followup),
		"interrupt worker": InterruptWorkerParams(interrupt),
	} {
		t.Run(name, func(t *testing.T) {
			if err := input.Validate(); err != nil {
				t.Fatalf("valid parameters: %v", err)
			}
		})
	}

	invalidSend := []SendAgentParams{send, send, send}
	invalidSend[0].AgentID = "invalid"
	invalidSend[1].MessageID = "invalid"
	invalidSend[2].Message += "m"
	for index, input := range invalidSend {
		if err := input.Validate(); err == nil {
			t.Fatalf("invalid send parameters %d were accepted", index)
		}
	}
	followup.Message += "f"
	if err := followup.Validate(); err == nil {
		t.Fatal("oversized follow-up was accepted")
	}
}

func TestAgentOperationResultValidation(t *testing.T) {
	valid := []AgentOperationResult{
		{OperationID: protocolOperationID, AgentID: protocolOperationAgentID, Action: AgentOperationSend, Outcome: AgentOperationOutcomePending},
		{OperationID: protocolOperationID, AgentID: protocolOperationAgentID, Action: AgentOperationSend, Outcome: AgentOperationOutcomeQueued},
		{OperationID: protocolOperationID, AgentID: protocolOperationAgentID, Action: AgentOperationSend, Outcome: AgentOperationOutcomeSteered},
		{OperationID: protocolOperationID, AgentID: protocolOperationAgentID, Action: AgentOperationFollowup, Outcome: AgentOperationOutcomeStarted},
		{OperationID: protocolOperationID, AgentID: protocolOperationAgentID, Action: AgentOperationInterrupt, Outcome: AgentOperationOutcomeInterrupted},
		{OperationID: protocolOperationID, AgentID: protocolOperationAgentID, Action: AgentOperationInterrupt, Outcome: AgentOperationOutcomeFailed, FailureCode: "not_running"},
	}
	for index, result := range valid {
		if err := result.Validate(); err != nil {
			t.Fatalf("valid result %d: %v", index, err)
		}
	}

	invalid := []AgentOperationResult{valid[0], valid[1], valid[3], valid[4], valid[5]}
	invalid[0].Outcome = "unknown"
	invalid[1].FailureCode = "unexpected"
	invalid[2].Outcome = AgentOperationOutcomeSteered
	invalid[3].Outcome = ""
	invalid[4].FailureCode = ""
	for index, result := range invalid {
		if err := result.Validate(); err == nil {
			t.Fatalf("invalid result %d was accepted", index)
		}
	}
}

func TestAgentOperationPayloadDecodeRejectsUnknownFields(t *testing.T) {
	payload := json.RawMessage(`{
		"operationId":"123e4567-e89b-42d3-a456-426614174320",
		"agentId":"123e4567-e89b-42d3-a456-426614174321",
		"unknown":true
	}`)
	if _, err := DecodePayload[InterruptAgentParams](payload); err == nil {
		t.Fatal("agent operation payload accepted an unknown field")
	}
}
