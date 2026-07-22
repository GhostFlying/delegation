package protocol

import (
	"math"
	"testing"
)

func TestWaitAgentParamsEnforceBoundedCursorsPagesAndTimeout(t *testing.T) {
	valid := WaitAgentParams{
		TimeoutMillis: MaximumAgentWaitMillis,
		MessageLimit:  MaximumAgentWaitMessages,
		ActivityLimit: MaximumAgentWaitActivities,
	}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	invalid := []WaitAgentParams{
		{MailboxCursor: math.MaxInt64 + 1, MessageLimit: 1, ActivityLimit: 1},
		{LifecycleCursor: math.MaxInt64 + 1, MessageLimit: 1, ActivityLimit: 1},
		{TimeoutMillis: -1, MessageLimit: 1, ActivityLimit: 1},
		{TimeoutMillis: MaximumAgentWaitMillis + 1, MessageLimit: 1, ActivityLimit: 1},
		{MessageLimit: 0, ActivityLimit: 1},
		{MessageLimit: MaximumAgentWaitMessages + 1, ActivityLimit: 1},
		{MessageLimit: 1, ActivityLimit: MaximumAgentWaitActivities + 1},
	}
	for index, params := range invalid {
		if err := params.Validate(); err == nil {
			t.Fatalf("invalid params %d were accepted: %#v", index, params)
		}
	}
}

func TestAgentLifecycleActivityValidation(t *testing.T) {
	activity := AgentLifecycleActivity{
		AgentID:        "123e4567-e89b-42d3-a456-426614174100",
		TargetDeviceID: "123e4567-e89b-42d3-a456-426614174101",
		TargetRevision: 1,
		Phase:          WorkerLifecycleRunning,
		Sequence:       1,
	}
	if err := activity.Validate(); err != nil {
		t.Fatal(err)
	}
	activity.Phase = WorkerLifecycleFailed
	if err := activity.Validate(); err == nil {
		t.Fatal("failed lifecycle without failureCode was accepted")
	}
}
