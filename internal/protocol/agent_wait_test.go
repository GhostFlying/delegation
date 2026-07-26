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
		ArtifactLimit: MaximumAgentWaitArtifacts,
		ResultLimit:   MaximumAgentWaitResults,
	}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	invalid := []WaitAgentParams{
		{MailboxCursor: math.MaxInt64 + 1, MessageLimit: 1, ActivityLimit: 1, ArtifactLimit: 1, ResultLimit: 1},
		{LifecycleCursor: math.MaxInt64 + 1, MessageLimit: 1, ActivityLimit: 1, ArtifactLimit: 1, ResultLimit: 1},
		{ArtifactCursor: math.MaxInt64 + 1, MessageLimit: 1, ActivityLimit: 1, ArtifactLimit: 1, ResultLimit: 1},
		{ResultCursor: math.MaxInt64 + 1, MessageLimit: 1, ActivityLimit: 1, ArtifactLimit: 1, ResultLimit: 1},
		{TimeoutMillis: -1, MessageLimit: 1, ActivityLimit: 1, ArtifactLimit: 1, ResultLimit: 1},
		{TimeoutMillis: MaximumAgentWaitMillis + 1, MessageLimit: 1, ActivityLimit: 1, ArtifactLimit: 1, ResultLimit: 1},
		{MessageLimit: 0, ActivityLimit: 1, ArtifactLimit: 1, ResultLimit: 1},
		{MessageLimit: MaximumAgentWaitMessages + 1, ActivityLimit: 1, ArtifactLimit: 1, ResultLimit: 1},
		{MessageLimit: 1, ActivityLimit: MaximumAgentWaitActivities + 1, ArtifactLimit: 1, ResultLimit: 1},
		{MessageLimit: 1, ActivityLimit: 1, ArtifactLimit: 0, ResultLimit: 1},
		{MessageLimit: 1, ActivityLimit: 1, ArtifactLimit: MaximumAgentWaitArtifacts + 1, ResultLimit: 1},
		{MessageLimit: 1, ActivityLimit: 1, ArtifactLimit: 1, ResultLimit: 0},
		{MessageLimit: 1, ActivityLimit: 1, ArtifactLimit: 1, ResultLimit: MaximumAgentWaitResults + 1},
	}
	for index, params := range invalid {
		if err := params.Validate(); err == nil {
			t.Fatalf("invalid params %d were accepted: %#v", index, params)
		}
	}
}

func TestResultPackageHandleRequiresBoundedDeliveryMetadata(t *testing.T) {
	handle := ResultPackageHandle{
		Manifest: validResultManifest(), Availability: ResultPackageUnverified,
		Sequence: 1, DeliveredAt: 1,
	}
	if err := handle.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*ResultPackageHandle){
		func(value *ResultPackageHandle) { value.Availability = "unknown" },
		func(value *ResultPackageHandle) { value.Sequence = 0 },
		func(value *ResultPackageHandle) { value.DeliveredAt = -1 },
		func(value *ResultPackageHandle) { value.Manifest.PackageID = "wrong" },
	} {
		invalid := handle
		mutate(&invalid)
		if err := invalid.Validate(); err == nil {
			t.Fatalf("invalid result package handle was accepted: %#v", invalid)
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
