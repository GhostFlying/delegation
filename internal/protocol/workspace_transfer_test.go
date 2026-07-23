package protocol

import (
	"strings"
	"testing"
)

const testWorkspaceID = "123e4567-e89b-42d3-a456-426614174005"
const testTransferID = "123e4567-e89b-42d3-a456-426614174006"

func TestWorkspaceTransferManifestValidatesCleanBundleStrategies(t *testing.T) {
	manifest := testWorkspaceManifest(t, true)
	manifestHash, err := WorkspaceManifestHash(manifest)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name     string
		strategy WorkspaceStrategy
		warnings []string
	}{
		{name: "thin", strategy: WorkspaceStrategyThin},
		{
			name: "self contained", strategy: WorkspaceStrategyFull,
			warnings: []string{WorkspaceWarningFullHistoryFallback},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			transfer := WorkspaceTransferManifest{
				TransferID: testTransferID, WorkspaceID: testWorkspaceID,
				Strategy: test.strategy, ManifestHash: manifestHash,
				Artifacts: []WorkspaceArtifactDescriptor{{
					Kind: WorkspaceArtifactBundle, Size: 1024, SHA256: strings.Repeat("b", 64),
				}},
				Warnings: test.warnings,
			}
			if err := transfer.Validate(); err != nil {
				t.Fatal(err)
			}
			if err := (BeginWorkspaceTransferParams{
				SourceAgentID: testAgentID, SourceDeviceID: testDeviceID,
				Manifest: manifest, Transfer: transfer,
			}).Validate(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestWorkspaceTransferManifestRejectsMismatchedStrategyAndArtifacts(t *testing.T) {
	base := WorkspaceTransferManifest{
		TransferID: testTransferID, WorkspaceID: testWorkspaceID,
		Strategy: WorkspaceStrategyThin, ManifestHash: strings.Repeat("a", 64),
		Artifacts: []WorkspaceArtifactDescriptor{{
			Kind: WorkspaceArtifactBundle, Size: 1, SHA256: strings.Repeat("b", 64),
		}},
	}
	for _, test := range []struct {
		name   string
		mutate func(*WorkspaceTransferManifest)
	}{
		{
			name: "missing bundle",
			mutate: func(value *WorkspaceTransferManifest) {
				value.Artifacts[0].Kind = WorkspaceArtifactOverlay
			},
		},
		{
			name: "full without warning",
			mutate: func(value *WorkspaceTransferManifest) {
				value.Strategy = WorkspaceStrategyFull
			},
		},
		{
			name: "thin with full warning",
			mutate: func(value *WorkspaceTransferManifest) {
				value.Warnings = []string{WorkspaceWarningFullHistoryFallback}
			},
		},
		{
			name: "duplicate artifact",
			mutate: func(value *WorkspaceTransferManifest) {
				value.Artifacts = append(value.Artifacts, value.Artifacts[0])
			},
		},
		{
			name: "oversized artifact",
			mutate: func(value *WorkspaceTransferManifest) {
				value.Artifacts[0].Size = MaximumWorkspaceArtifactBytes + 1
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			value := base
			value.Artifacts = append([]WorkspaceArtifactDescriptor(nil), base.Artifacts...)
			test.mutate(&value)
			if err := value.Validate(); err == nil {
				t.Fatal("Validate() succeeded")
			}
		})
	}
}

func TestBeginWorkspaceTransferBindsWarningsAndOverlay(t *testing.T) {
	manifest := testWorkspaceManifest(t, true)
	manifestHash, err := WorkspaceManifestHash(manifest)
	if err != nil {
		t.Fatal(err)
	}
	params := BeginWorkspaceTransferParams{
		SourceAgentID: testAgentID, SourceDeviceID: testDeviceID, Manifest: manifest,
		Transfer: WorkspaceTransferManifest{
			TransferID: testTransferID, WorkspaceID: testWorkspaceID,
			Strategy: WorkspaceStrategyThin, ManifestHash: manifestHash,
			Artifacts: []WorkspaceArtifactDescriptor{{
				Kind: WorkspaceArtifactBundle, Size: 1, SHA256: strings.Repeat("b", 64),
			}},
			Warnings: []string{},
		},
	}
	if err := params.Validate(); err != nil {
		t.Fatal(err)
	}
	params.Transfer.Warnings = []string{WorkspaceWarningFullHistoryFallback}
	if err := params.Validate(); err == nil {
		t.Fatal("mismatched transfer warnings were accepted")
	}
	params.Transfer.Warnings = []string{}
	params.Transfer.Artifacts = append(params.Transfer.Artifacts, WorkspaceArtifactDescriptor{
		Kind: WorkspaceArtifactOverlay, Size: 1, SHA256: strings.Repeat("c", 64),
	})
	if err := params.Validate(); err == nil {
		t.Fatal("overlay for a clean source was accepted")
	}
}

func TestWorkspaceTransferChunkBounds(t *testing.T) {
	validRead := ReadWorkspaceArtifactParams{
		TransferID: testTransferID, Kind: WorkspaceArtifactBundle,
		Offset: MaximumWorkspaceArtifactBytes - 1, Limit: 1,
	}
	if err := validRead.Validate(); err != nil {
		t.Fatal(err)
	}
	invalidRead := validRead
	invalidRead.Limit = WorkspaceArtifactChunkBytes + 1
	if err := invalidRead.Validate(); err == nil {
		t.Fatal("oversized read chunk was accepted")
	}
	validWrite := WriteWorkspaceArtifactParams{
		WorkspaceID: testWorkspaceID, TransferID: testTransferID, Kind: WorkspaceArtifactBundle,
		Offset: MaximumWorkspaceArtifactBytes - 1, Data: []byte{1},
	}
	if err := validWrite.Validate(); err != nil {
		t.Fatal(err)
	}
	invalidWrite := validWrite
	invalidWrite.Data = []byte{1, 2}
	if err := invalidWrite.Validate(); err == nil {
		t.Fatal("out-of-range write chunk was accepted")
	}
}

func TestCreateWorkspaceTransferBindsSourceState(t *testing.T) {
	manifest := testWorkspaceManifest(t, true)
	params := CreateWorkspaceTransferParams{
		TransferID: testTransferID, WorkspaceID: testWorkspaceID,
		GitURL: manifest.GitURL, SourcePath: "/source", Manifest: manifest,
		BasisOIDs: []string{strings.Repeat("1", 40)}, BundleRequired: true,
	}
	if err := params.Validate(); err != nil {
		t.Fatal(err)
	}
	params.Manifest.Clean = false
	if err := params.Validate(); err == nil {
		t.Fatal("dirty source without overlay was accepted")
	}
	params.Manifest.Clean = true
	params.BasisOIDs = []string{strings.Repeat("2", 64)}
	if err := params.Validate(); err == nil {
		t.Fatal("basis object format mismatch was accepted")
	}
}

func TestWorkspaceWarningsForStrategyAddsOnlyFullFallback(t *testing.T) {
	for _, test := range []struct {
		strategy WorkspaceStrategy
		want     []string
	}{
		{strategy: WorkspaceStrategyDirect, want: []string{"lfs_payload_not_transferred"}},
		{strategy: WorkspaceStrategyThin, want: []string{"lfs_payload_not_transferred"}},
		{
			strategy: WorkspaceStrategyFull,
			want:     []string{"lfs_payload_not_transferred", WorkspaceWarningFullHistoryFallback},
		},
	} {
		warnings, err := WorkspaceWarningsForStrategy([]string{"lfs_payload_not_transferred"}, test.strategy)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Join(warnings, ",") != strings.Join(test.want, ",") {
			t.Fatalf("warnings for %q = %#v, want %#v", test.strategy, warnings, test.want)
		}
	}
}

func TestSyncWorkspaceResultRejectsIntermediateTransferState(t *testing.T) {
	result := SyncWorkspaceResult{
		Outcome:  WorkspacePrepareTransferRequired,
		Warnings: []string{},
	}
	if err := result.Validate(); err == nil {
		t.Fatal("intermediate workspace transfer state was accepted as a root result")
	}
}

func testWorkspaceManifest(t *testing.T, clean bool) WorkspaceManifest {
	t.Helper()
	manifest := WorkspaceManifest{
		GitURL:  "https://example.invalid/repository.git",
		HeadOID: strings.Repeat("1", 40), ObjectFormat: "sha1",
		WorkingDirectory: "nested", Clean: clean,
		SourceSnapshotHash: strings.Repeat("a", 64), Warnings: []string{},
	}
	if err := manifest.Validate(); err != nil {
		t.Fatal(err)
	}
	return manifest
}
