package protocol

import (
	"strings"
	"testing"
)

const (
	testArtifactID         = "123e4567-e89b-42d3-a456-426614174110"
	testTurnID             = "123e4567-e89b-42d3-a456-426614174111"
	testChangesWorkspaceID = "123e4567-e89b-42d3-a456-426614174112"
)

func TestChangesArtifactStatusesValidateTheirPayloadShape(t *testing.T) {
	available := validChangesArtifactParams()
	available.BaseWarnings = []string{WorkspaceWarningFullHistoryFallback}
	available.ResultWarnings = []string{"lfs_payload_not_transferred"}
	if err := available.Validate(); err != nil {
		t.Fatal(err)
	}
	unchanged := available
	unchanged.Status = ChangesArtifactUnchanged
	unchanged.ResultHeadOID = unchanged.BaseHeadOID
	unchanged.ResultSnapshotHash = unchanged.BaseSnapshotHash
	unchanged.ResultClean = true
	unchanged.Parts = []WorkspaceArtifactDescriptor{}
	if err := unchanged.Validate(); err != nil {
		t.Fatal(err)
	}
	failed := available
	failed.Status = ChangesArtifactCaptureFailed
	failed.ResultHeadOID = ""
	failed.ResultSnapshotHash = ""
	failed.ResultClean = false
	failed.Parts = []WorkspaceArtifactDescriptor{}
	failed.ResultWarnings = []string{}
	failed.FailureCode = "changes_capture_failed"
	if err := failed.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestChangesArtifactSeparatesBaseAndResultWarnings(t *testing.T) {
	params := validChangesArtifactParams()
	params.BaseWarnings = []string{WorkspaceWarningFullHistoryFallback}
	params.ResultWarnings = []string{"submodule_repository_not_transferred"}
	if err := params.Validate(); err != nil {
		t.Fatal(err)
	}

	invalidResult := params
	invalidResult.ResultWarnings = []string{WorkspaceWarningFullHistoryFallback}
	if err := invalidResult.Validate(); err == nil {
		t.Fatal("result warnings accepted a workspace transport fallback warning")
	}

	failed := params
	failed.Status = ChangesArtifactCaptureFailed
	failed.ResultHeadOID = ""
	failed.ResultSnapshotHash = ""
	failed.ResultClean = false
	failed.Parts = []WorkspaceArtifactDescriptor{}
	failed.FailureCode = "changes_capture_failed"
	if err := failed.Validate(); err == nil {
		t.Fatal("failed changes artifact accepted result warnings")
	}
}

func TestChangesArtifactRejectsInconsistentResultsAndParts(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*PublishChangesArtifactParams)
	}{
		{name: "same result", mutate: func(value *PublishChangesArtifactParams) {
			value.ResultHeadOID = value.BaseHeadOID
			value.ResultSnapshotHash = value.BaseSnapshotHash
			value.Parts = nil
		}},
		{name: "missing bundle", mutate: func(value *PublishChangesArtifactParams) { value.Parts = nil }},
		{name: "unexpected overlay", mutate: func(value *PublishChangesArtifactParams) {
			value.Parts = append(value.Parts, WorkspaceArtifactDescriptor{
				Kind: WorkspaceArtifactOverlay, Size: 1, SHA256: strings.Repeat("d", 64),
			})
		}},
		{name: "dirty without overlay", mutate: func(value *PublishChangesArtifactParams) {
			value.ResultClean = false
		}},
		{name: "unsorted parts", mutate: func(value *PublishChangesArtifactParams) {
			value.ResultClean = false
			value.Parts = []WorkspaceArtifactDescriptor{
				{Kind: WorkspaceArtifactOverlay, Size: 1, SHA256: strings.Repeat("d", 64)},
				{Kind: WorkspaceArtifactBundle, Size: 1, SHA256: strings.Repeat("e", 64)},
			}
		}},
		{name: "failed claims result", mutate: func(value *PublishChangesArtifactParams) {
			value.Status = ChangesArtifactCaptureFailed
			value.FailureCode = "capture_failed"
		}},
		{name: "missing workspace source device", mutate: func(value *PublishChangesArtifactParams) {
			value.WorkspaceSourceDeviceID = ""
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			params := validChangesArtifactParams()
			test.mutate(&params)
			if err := params.Validate(); err == nil {
				t.Fatalf("Validate accepted %#v", params)
			}
		})
	}
}

func TestChangesArtifactMetadataBindsDerivedBaseState(t *testing.T) {
	params := validChangesArtifactParams()
	params.ResultHeadOID = params.BaseHeadOID
	params.ResultSnapshotHash = strings.Repeat("f", 64)
	params.Parts = []WorkspaceArtifactDescriptor{}
	metadata := changesArtifactMetadata(params)
	metadata.BaseClean = false
	if err := metadata.Validate(); err != nil {
		t.Fatal(err)
	}
	metadata.BaseClean = true
	if err := metadata.Validate(); err == nil {
		t.Fatal("clean base accepted a payload-free reset artifact")
	}

	params.Status = ChangesArtifactUnchanged
	params.ResultSnapshotHash = params.BaseSnapshotHash
	metadata = changesArtifactMetadata(params)
	metadata.BaseClean = false
	metadata.ResultClean = true
	if err := metadata.Validate(); err == nil {
		t.Fatal("unchanged artifact changed base cleanliness")
	}
}

func TestChangesArtifactMetadataBindsPublisherToWorkspaceTarget(t *testing.T) {
	metadata := changesArtifactMetadata(validChangesArtifactParams())
	metadata.WorkspaceTargetDeviceID = testControllerID
	if err := metadata.Validate(); err == nil {
		t.Fatal("changes artifact accepted a publisher outside the workspace target device")
	}
}

func validChangesArtifactParams() PublishChangesArtifactParams {
	return PublishChangesArtifactParams{
		ArtifactID: testArtifactID, TurnID: testTurnID, WorkspaceID: testChangesWorkspaceID,
		WorkspaceSourceDeviceID: testControllerID, WorkspaceTargetDeviceID: testDeviceID,
		Status: ChangesArtifactAvailable, BaseHeadOID: strings.Repeat("a", 40),
		BaseManifestHash: strings.Repeat("b", 64), BaseSnapshotHash: strings.Repeat("c", 64),
		ResultHeadOID: strings.Repeat("d", 40), ResultSnapshotHash: strings.Repeat("e", 64),
		ResultClean: true,
		Parts: []WorkspaceArtifactDescriptor{{
			Kind: WorkspaceArtifactBundle, Size: 32, SHA256: strings.Repeat("f", 64),
		}},
		BaseWarnings: []string{}, ResultWarnings: []string{},
	}
}

func changesArtifactMetadata(params PublishChangesArtifactParams) ChangesArtifactMetadata {
	return ChangesArtifactMetadata{
		TreeID: testTreeID, ArtifactID: params.ArtifactID, TurnID: params.TurnID,
		WorkspaceID: params.WorkspaceID, Status: params.Status, SourceAgentID: testAgentID,
		SourceDeviceID:          testDeviceID,
		WorkspaceSourceDeviceID: params.WorkspaceSourceDeviceID,
		WorkspaceTargetDeviceID: params.WorkspaceTargetDeviceID,
		ObjectFormat:            "sha1", BaseHeadOID: params.BaseHeadOID,
		BaseManifestHash: params.BaseManifestHash, BaseSnapshotHash: params.BaseSnapshotHash,
		BaseClean: true, ResultHeadOID: params.ResultHeadOID,
		ResultSnapshotHash: params.ResultSnapshotHash, ResultClean: params.ResultClean,
		Parts: params.Parts, BaseWarnings: params.BaseWarnings,
		ResultWarnings: params.ResultWarnings, FailureCode: params.FailureCode,
		Sequence: 1, ObservedAt: 1,
	}
}
