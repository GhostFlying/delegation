package protocol

import (
	"math"
	"strings"
	"testing"
)

func TestSendMessageParamsEnforcesWireMessageContract(t *testing.T) {
	valid := SendMessageParams{
		MessageID: "123e4567-e89b-42d3-a456-426614174001",
		Target:    MessageTarget{Kind: MessageTargetParent},
		Message:   strings.Repeat("x", MaximumMailboxMessageBytes),
	}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	for name, params := range map[string]SendMessageParams{
		"oversized": {
			MessageID: "123e4567-e89b-42d3-a456-426614174001",
			Target:    MessageTarget{Kind: MessageTargetParent},
			Message:   strings.Repeat("x", MaximumMailboxMessageBytes+1),
		},
		"empty": {
			MessageID: "123e4567-e89b-42d3-a456-426614174001",
			Target:    MessageTarget{Kind: MessageTargetParent},
		},
		"missing message ID": {
			Target: MessageTarget{Kind: MessageTargetParent}, Message: "status",
		},
		"uppercase message ID": {
			MessageID: "123E4567-E89B-42D3-A456-426614174001",
			Target:    MessageTarget{Kind: MessageTargetParent},
			Message:   "status",
		},
		"invalid target": {
			MessageID: "123e4567-e89b-42d3-a456-426614174001",
			Target:    MessageTarget{Kind: MessageTargetAgent},
			Message:   "status",
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := params.Validate(); err == nil {
				t.Fatal("SendMessageParams.Validate accepted invalid input")
			}
		})
	}
}

func TestWaitMailboxParamsEnforcesBoundedLongPoll(t *testing.T) {
	valid := WaitMailboxParams{
		Cursor: math.MaxInt64, TimeoutMillis: MaximumMailboxWaitMillis, Limit: MaximumMailboxPage,
	}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	for name, params := range map[string]WaitMailboxParams{
		"cursor":  {Cursor: math.MaxInt64 + 1, Limit: 1},
		"timeout": {TimeoutMillis: MaximumMailboxWaitMillis + 1, Limit: 1},
		"limit":   {Limit: MaximumMailboxPage + 1},
	} {
		t.Run(name, func(t *testing.T) {
			if err := params.Validate(); err == nil {
				t.Fatal("WaitMailboxParams.Validate accepted invalid input")
			}
		})
	}
}
