package protocol

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
)

const (
	testResultPackageID   = "123e4567-e89b-42d3-a456-426614174120"
	testManagedThreadID   = "123e4567-e89b-42d3-a456-426614174121"
	testResultTurnID      = "123e4567-e89b-42d3-a456-426614174122"
	testResultWorkspaceID = "123e4567-e89b-42d3-a456-426614174123"
	testWorkspaceSourceID = "123e4567-e89b-42d3-a456-426614174124"
	testResultAttemptID   = "123e4567-e89b-42d3-a456-426614174125"
)

func TestResultManifestCanonicalEncodingAndDigest(t *testing.T) {
	manifest := validResultManifest()
	data, descriptor, err := EncodeResultManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"version":2,"packageId":"123e4567-e89b-42d3-a456-426614174120","controllerId":"123e4567-e89b-42d3-a456-426614174000","treeId":"123e4567-e89b-42d3-a456-426614174001","sourceAgentId":"123e4567-e89b-42d3-a456-426614174002","sourceDeviceId":"123e4567-e89b-42d3-a456-426614174003","managedThreadId":"123e4567-e89b-42d3-a456-426614174121","turnId":"123e4567-e89b-42d3-a456-426614174122","lifecycleRevision":7,"terminal":{"outcome":"completed","failureCode":""},"capturedAt":1700000000,"rollout":{"status":"available","rawSize":42,"rawSha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","failureCode":""},"workspace":{"status":"notManaged","workspaceId":"","sourceDeviceId":"","targetDeviceId":"","objectFormat":"","baseHeadOid":"","baseManifestHash":"","baseSnapshotHash":"","baseClean":false,"resultHeadOid":"","resultSnapshotHash":"","resultClean":false,"baseWarnings":[],"resultWarnings":[],"failureCode":""},"parts":[{"kind":"rollout","size":21,"sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}]}
`
	if string(data) != want {
		t.Fatalf("canonical manifest = %q, want %q", data, want)
	}
	wantDescriptor := ResultPackagePartDescriptor{
		Kind: ResultPackagePartManifest, Size: int64(len(want)),
		SHA256: "80747d2034e367f22c237fbb3310bca3146930b03c6ee5d20f2ef3addfdf3684",
	}
	if descriptor != wantDescriptor {
		t.Fatalf("manifest descriptor = %#v, want %#v", descriptor, wantDescriptor)
	}
	decoded, err := DecodeResultManifest(data)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, manifest) {
		t.Fatalf("decoded manifest = %#v, want %#v", decoded, manifest)
	}
	metadata := ResultPackageMetadata{Manifest: data, ManifestDescriptor: descriptor}
	if err := metadata.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestResultManifestRejectsPayloadThatCannotLeaveMaximumManifestHeadroom(t *testing.T) {
	manifest := validChangedResultManifest()
	manifest.Rollout = ResultRolloutComponent{
		Status: ResultRolloutCaptureFailed, FailureCode: "rollout_unavailable",
	}
	manifest.Parts = []ResultPackagePartDescriptor{
		{
			Kind: ResultPackagePartChangesBundle, Size: MaximumResultChangesBundleBytes,
			SHA256: strings.Repeat("c", 64),
		},
		{
			Kind: ResultPackagePartChangesOverlay, Size: MaximumResultChangesOverlayBytes,
			SHA256: strings.Repeat("d", 64),
		},
	}
	if _, _, err := EncodeResultManifest(manifest); err == nil ||
		!strings.Contains(err.Error(), "payload exceeds") {
		t.Fatalf("payload-headroom error = %v", err)
	}
}

func TestDecodeResultManifestRejectsNonStrictShapes(t *testing.T) {
	data, _, err := EncodeResultManifest(validResultManifest())
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "unknown field",
			data: bytes.Replace(data, []byte(`{"version":2`), []byte(`{"unknown":true,"version":2`), 1),
		},
		{name: "trailing value", data: append(append([]byte{}, data...), []byte(`{}
`)...)},
		{
			name: "missing required field",
			data: bytes.Replace(data, []byte(`"capturedAt":1700000000,`), nil, 1),
		},
		{
			name: "missing nested field",
			data: bytes.Replace(data, []byte(`"failureCode":""`), nil, 1),
		},
		{
			name: "null required array",
			data: bytes.Replace(data, []byte(`"baseWarnings":[]`), []byte(`"baseWarnings":null`), 1),
		},
		{name: "oversized", data: bytes.Repeat([]byte{' '}, int(MaximumResultManifestBytes)+1)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := DecodeResultManifest(test.data); err == nil {
				t.Fatal("DecodeResultManifest accepted invalid bytes")
			}
		})
	}
}

func TestResultPackagePartDescriptorBoundsAndNames(t *testing.T) {
	tests := []struct {
		kind     ResultPackagePartKind
		maximum  int64
		fileName string
	}{
		{kind: ResultPackagePartManifest, maximum: MaximumResultManifestBytes, fileName: ResultManifestFileName},
		{kind: ResultPackagePartRollout, maximum: MaximumResultRolloutBytes, fileName: ResultRolloutFileName},
		{
			kind: ResultPackagePartChangesBundle, maximum: MaximumResultChangesBundleBytes,
			fileName: ResultChangesBundleFileName,
		},
		{
			kind: ResultPackagePartChangesOverlay, maximum: MaximumResultChangesOverlayBytes,
			fileName: ResultChangesOverlayFileName,
		},
	}
	for _, test := range tests {
		t.Run(string(test.kind), func(t *testing.T) {
			descriptor := ResultPackagePartDescriptor{
				Kind: test.kind, Size: test.maximum, SHA256: strings.Repeat("a", 64),
			}
			if err := descriptor.Validate(); err != nil {
				t.Fatal(err)
			}
			name, err := test.kind.FileName()
			if err != nil || name != test.fileName {
				t.Fatalf("FileName() = %q, %v, want %q", name, err, test.fileName)
			}
			descriptor.Size++
			if err := descriptor.Validate(); err == nil {
				t.Fatal("descriptor accepted an oversized part")
			}
		})
	}
	if _, err := ResultPackagePartKind("unknown").FileName(); err == nil {
		t.Fatal("unknown part kind returned a file name")
	}
}

func TestResultManifestBindsComponentsToPayloadParts(t *testing.T) {
	changed := validChangedResultManifest()
	if err := changed.Validate(); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(*ResultManifest)
	}{
		{name: "missing rollout", mutate: func(value *ResultManifest) { value.Parts = value.Parts[:2] }},
		{
			name:   "missing bundle",
			mutate: func(value *ResultManifest) { value.Parts = value.Parts[1:] },
		},
		{
			name:   "clean with overlay",
			mutate: func(value *ResultManifest) { value.Workspace.ResultClean = true },
		},
		{
			name:   "unsorted parts",
			mutate: func(value *ResultManifest) { value.Parts[0], value.Parts[1] = value.Parts[1], value.Parts[0] },
		},
		{
			name:   "recursive manifest part",
			mutate: func(value *ResultManifest) { value.Parts[0].Kind = ResultPackagePartManifest },
		},
		{
			name:   "cross-device workspace",
			mutate: func(value *ResultManifest) { value.Workspace.TargetDeviceID = testWorkspaceSourceID },
		},
		{
			name:   "nil warning list",
			mutate: func(value *ResultManifest) { value.Workspace.ResultWarnings = nil },
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			manifest := cloneResultManifest(changed)
			test.mutate(&manifest)
			if err := manifest.Validate(); err == nil {
				t.Fatal("Validate accepted an inconsistent result manifest")
			}
		})
	}
}

func TestResultManifestSupportsIndependentCaptureFailures(t *testing.T) {
	manifest := validResultManifest()
	manifest.Rollout = ResultRolloutComponent{
		Status: ResultRolloutCaptureFailed, FailureCode: "rollout_unavailable",
	}
	manifest.Parts = []ResultPackagePartDescriptor{}
	manifest.Workspace = resultWorkspaceBase()
	manifest.Workspace.Status = ResultWorkspaceCaptureFailed
	manifest.Workspace.FailureCode = "changes_capture_failed"
	if err := manifest.Validate(); err != nil {
		t.Fatal(err)
	}
	manifest.Workspace.ResultHeadOID = manifest.Workspace.BaseHeadOID
	if err := manifest.Validate(); err == nil {
		t.Fatal("failed workspace capture claimed a result")
	}
}

func TestResultManifestAllowsOnlyRepresentablePayloadFreeChanges(t *testing.T) {
	manifest := validResultManifest()
	manifest.Parts = manifest.Parts[:1]
	manifest.Workspace = resultWorkspaceBase()
	manifest.Workspace.Status = ResultWorkspaceChanged
	manifest.Workspace.BaseClean = false
	manifest.Workspace.ResultHeadOID = manifest.Workspace.BaseHeadOID
	manifest.Workspace.ResultSnapshotHash = strings.Repeat("4", 64)
	manifest.Workspace.ResultClean = true
	if err := manifest.Validate(); err != nil {
		t.Fatal(err)
	}
	manifest.Workspace.BaseClean = true
	if err := manifest.Validate(); err == nil {
		t.Fatal("payload-free changes accepted a clean base")
	}
}

func validResultManifest() ResultManifest {
	return ResultManifest{
		Version: ResultManifestVersion, PackageID: testResultPackageID,
		ControllerID: testControllerID, TreeID: testTreeID, SourceAgentID: testAgentID,
		SourceDeviceID: testDeviceID, ManagedThreadID: testManagedThreadID, TurnID: testResultTurnID,
		LifecycleRevision: 7, Terminal: ResultTerminal{Outcome: ResultTerminalCompleted},
		CapturedAt: 1_700_000_000,
		Rollout: ResultRolloutComponent{
			Status: ResultRolloutAvailable, RawSize: 42, RawSHA256: strings.Repeat("a", 64),
		},
		Workspace: ResultWorkspaceComponent{
			Status: ResultWorkspaceNotManaged, BaseWarnings: []string{}, ResultWarnings: []string{},
		},
		Parts: []ResultPackagePartDescriptor{{
			Kind: ResultPackagePartRollout, Size: 21, SHA256: strings.Repeat("b", 64),
		}},
	}
}

func validChangedResultManifest() ResultManifest {
	manifest := validResultManifest()
	manifest.Workspace = resultWorkspaceBase()
	manifest.Workspace.Status = ResultWorkspaceChanged
	manifest.Workspace.ResultHeadOID = strings.Repeat("d", 40)
	manifest.Workspace.ResultSnapshotHash = strings.Repeat("e", 64)
	manifest.Workspace.ResultClean = false
	manifest.Workspace.ResultWarnings = []string{"lfs_payload_not_transferred"}
	manifest.Parts = []ResultPackagePartDescriptor{
		{Kind: ResultPackagePartChangesBundle, Size: 31, SHA256: strings.Repeat("c", 64)},
		{Kind: ResultPackagePartChangesOverlay, Size: 32, SHA256: strings.Repeat("d", 64)},
		{Kind: ResultPackagePartRollout, Size: 21, SHA256: strings.Repeat("b", 64)},
	}
	return manifest
}

func resultWorkspaceBase() ResultWorkspaceComponent {
	return ResultWorkspaceComponent{
		WorkspaceID: testResultWorkspaceID, SourceDeviceID: testWorkspaceSourceID,
		TargetDeviceID: testDeviceID, ObjectFormat: "sha1", BaseHeadOID: strings.Repeat("1", 40),
		BaseManifestHash: strings.Repeat("2", 64), BaseSnapshotHash: strings.Repeat("3", 64),
		BaseClean: true, BaseWarnings: []string{}, ResultWarnings: []string{},
	}
}

func cloneResultManifest(manifest ResultManifest) ResultManifest {
	manifest.Parts = append([]ResultPackagePartDescriptor{}, manifest.Parts...)
	manifest.Workspace.BaseWarnings = append([]string{}, manifest.Workspace.BaseWarnings...)
	manifest.Workspace.ResultWarnings = append([]string{}, manifest.Workspace.ResultWarnings...)
	return manifest
}
