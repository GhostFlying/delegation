package protocol

import (
	"math"
	"testing"
)

func TestResultPackagePublishAndBeginValidateManifestIdentity(t *testing.T) {
	metadata := validResultPackageMetadata(t)
	publish := PublishResultPackageParams{Metadata: metadata}
	if err := publish.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := (PublishResultPackageResult{PackageID: testResultPackageID}).Validate(); err != nil {
		t.Fatal(err)
	}
	begin := BeginResultPackageParams{
		AttemptID: testResultAttemptID, PackageID: testResultPackageID,
		LeaseExpiresAt: 1_700_000_060, Metadata: metadata,
	}
	if err := begin.Validate(); err != nil {
		t.Fatal(err)
	}

	invalid := begin
	invalid.PackageID = testResultWorkspaceID
	if err := invalid.Validate(); err == nil {
		t.Fatal("begin accepted a package ID outside its manifest")
	}
	invalid = begin
	invalid.LeaseExpiresAt = 0
	if err := invalid.Validate(); err == nil {
		t.Fatal("begin accepted an expired lease boundary")
	}

	corrupt := cloneResultPackageMetadata(metadata)
	corrupt.Manifest[len(corrupt.Manifest)-2] ^= 1
	if err := corrupt.Validate(); err == nil {
		t.Fatal("metadata accepted bytes outside its digest")
	}
	wrongSize := metadata
	wrongSize.ManifestDescriptor.Size++
	if err := wrongSize.Validate(); err == nil {
		t.Fatal("metadata accepted bytes outside its declared size")
	}
}

func TestBeginResultPackageResultValidatesOutcomeAndOffsets(t *testing.T) {
	receiving := BeginResultPackageResult{
		AttemptID: testResultAttemptID, PackageID: testResultPackageID,
		Outcome: ResultPackageReceiving,
		Offsets: []ResultPackagePartOffset{
			{Kind: ResultPackagePartChangesBundle, NextOffset: 0},
			{Kind: ResultPackagePartRollout, NextOffset: 21},
		},
	}
	if err := receiving.Validate(); err != nil {
		t.Fatal(err)
	}
	already := receiving
	already.Outcome = ResultPackageAlreadyAvailable
	already.Offsets = []ResultPackagePartOffset{}
	if err := already.Validate(); err != nil {
		t.Fatal(err)
	}

	invalid := receiving
	invalid.Offsets = append([]ResultPackagePartOffset{}, receiving.Offsets...)
	invalid.Offsets[0].Kind = ResultPackagePartManifest
	if err := invalid.Validate(); err == nil {
		t.Fatal("begin result accepted a manifest payload offset")
	}
	invalid = already
	invalid.Offsets = []ResultPackagePartOffset{{Kind: ResultPackagePartRollout}}
	if err := invalid.Validate(); err == nil {
		t.Fatal("already available result accepted offsets")
	}
}

func TestResultPackageChunkTypesValidateBounds(t *testing.T) {
	read := ReadResultPackagePartParams{
		PackageID: testResultPackageID, Kind: ResultPackagePartRollout,
		Offset: MaximumResultRolloutBytes - 1, Limit: 1,
	}
	if err := read.Validate(); err != nil {
		t.Fatal(err)
	}
	readResult := ReadResultPackagePartResult{
		PackageID: testResultPackageID, Kind: ResultPackagePartRollout,
		Offset: MaximumResultRolloutBytes - 1, Data: []byte{1}, NextOffset: MaximumResultRolloutBytes,
	}
	if err := readResult.Validate(); err != nil {
		t.Fatal(err)
	}
	write := WriteResultPackagePartParams{
		AttemptID: testResultAttemptID, PackageID: testResultPackageID,
		Kind: ResultPackagePartChangesBundle, Offset: MaximumResultChangesBundleBytes - 1,
		Data: []byte{1},
	}
	if err := write.Validate(); err != nil {
		t.Fatal(err)
	}
	writeResult := WriteResultPackagePartResult{
		AttemptID: testResultAttemptID, PackageID: testResultPackageID,
		Kind: ResultPackagePartChangesBundle, NextOffset: MaximumResultChangesBundleBytes,
	}
	if err := writeResult.Validate(); err != nil {
		t.Fatal(err)
	}

	invalidRead := read
	invalidRead.Limit = 2
	if err := invalidRead.Validate(); err == nil {
		t.Fatal("read accepted bytes beyond the part bound")
	}
	invalidRead = read
	invalidRead.Kind = ResultPackagePartManifest
	if err := invalidRead.Validate(); err == nil {
		t.Fatal("read accepted the in-metadata manifest as a payload")
	}
	invalidReadResult := readResult
	invalidReadResult.Offset = math.MaxInt64
	invalidReadResult.NextOffset = math.MinInt64
	if err := invalidReadResult.Validate(); err == nil {
		t.Fatal("read result accepted overflowing offsets")
	}
	invalidWrite := write
	invalidWrite.Data = []byte{1, 2}
	if err := invalidWrite.Validate(); err == nil {
		t.Fatal("write accepted bytes beyond the part bound")
	}
}

func TestResultPackageControlAndAcknowledgementTypes(t *testing.T) {
	finish := FinishResultPackageParams{AttemptID: testResultAttemptID, PackageID: testResultPackageID}
	cancel := CancelResultPackageParams(finish)
	for _, validate := range []func() error{
		finish.Validate,
		FinishResultPackageResult(finish).Validate,
		cancel.Validate,
		CancelResultPackageResult(cancel).Validate,
	} {
		if err := validate(); err != nil {
			t.Fatal(err)
		}
	}
	acknowledgement := AcknowledgeResultPackageParams{PackageID: testResultPackageID, Sequence: 9}
	if err := acknowledgement.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := (AcknowledgeResultPackageResult(acknowledgement)).Validate(); err != nil {
		t.Fatal(err)
	}
	release := ReleaseResultPackageParams(acknowledgement)
	if err := release.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := (ReleaseResultPackageResult(release)).Validate(); err != nil {
		t.Fatal(err)
	}
	acknowledgement.Sequence = 0
	if err := acknowledgement.Validate(); err == nil {
		t.Fatal("acknowledgement accepted sequence zero")
	}
}

func TestResultPackageIdempotencyComparisonsCoverRequestBytes(t *testing.T) {
	metadata := validResultPackageMetadata(t)
	publish := PublishResultPackageParams{Metadata: metadata}
	publishCopy := PublishResultPackageParams{Metadata: cloneResultPackageMetadata(metadata)}
	if !SamePublishResultPackageParams(publish, publishCopy) ||
		!SameResultPackageMetadata(publish.Metadata, publishCopy.Metadata) {
		t.Fatal("identical publish metadata did not compare equal")
	}
	publishCopy.Metadata.Manifest[0] ^= 1
	if SamePublishResultPackageParams(publish, publishCopy) {
		t.Fatal("publish comparison ignored manifest bytes")
	}

	begin := BeginResultPackageParams{
		AttemptID: testResultAttemptID, PackageID: testResultPackageID,
		LeaseExpiresAt: 1_700_000_060, Metadata: metadata,
	}
	beginCopy := begin
	beginCopy.Metadata = cloneResultPackageMetadata(metadata)
	if !SameBeginResultPackageParams(begin, beginCopy) {
		t.Fatal("identical begin parameters did not compare equal")
	}
	beginCopy.LeaseExpiresAt++
	if SameBeginResultPackageParams(begin, beginCopy) {
		t.Fatal("begin comparison ignored the lease")
	}

	read := ReadResultPackagePartParams{
		PackageID: testResultPackageID, Kind: ResultPackagePartRollout, Limit: ResultPackageChunkBytes,
	}
	if !SameReadResultPackagePartParams(read, read) {
		t.Fatal("identical read parameters did not compare equal")
	}
	changedRead := read
	changedRead.Offset++
	if SameReadResultPackagePartParams(read, changedRead) {
		t.Fatal("read comparison ignored the offset")
	}

	write := WriteResultPackagePartParams{
		AttemptID: testResultAttemptID, PackageID: testResultPackageID,
		Kind: ResultPackagePartRollout, Data: []byte{1, 2, 3},
	}
	writeCopy := write
	writeCopy.Data = append([]byte{}, write.Data...)
	if !SameWriteResultPackagePartParams(write, writeCopy) {
		t.Fatal("identical write parameters did not compare equal")
	}
	writeCopy.Data[0]++
	if SameWriteResultPackagePartParams(write, writeCopy) {
		t.Fatal("write comparison ignored chunk bytes")
	}

	finish := FinishResultPackageParams{AttemptID: testResultAttemptID, PackageID: testResultPackageID}
	if !SameFinishResultPackageParams(finish, finish) {
		t.Fatal("identical finish parameters did not compare equal")
	}
	changedFinish := finish
	changedFinish.PackageID = testResultWorkspaceID
	if SameFinishResultPackageParams(finish, changedFinish) {
		t.Fatal("finish comparison ignored package identity")
	}
	cancel := CancelResultPackageParams(finish)
	if !SameCancelResultPackageParams(cancel, cancel) {
		t.Fatal("identical cancel parameters did not compare equal")
	}
	changedCancel := cancel
	changedCancel.AttemptID = testResultWorkspaceID
	if SameCancelResultPackageParams(cancel, changedCancel) {
		t.Fatal("cancel comparison ignored attempt identity")
	}
	acknowledgement := AcknowledgeResultPackageParams{PackageID: testResultPackageID, Sequence: 9}
	if !SameAcknowledgeResultPackageParams(acknowledgement, acknowledgement) {
		t.Fatal("identical acknowledgement did not compare equal")
	}
	changedAcknowledgement := acknowledgement
	changedAcknowledgement.Sequence++
	if SameAcknowledgeResultPackageParams(acknowledgement, changedAcknowledgement) {
		t.Fatal("acknowledgement comparison ignored sequence")
	}
	release := ReleaseResultPackageParams(acknowledgement)
	if !SameReleaseResultPackageParams(release, release) {
		t.Fatal("identical release parameters did not compare equal")
	}
	changedRelease := release
	changedRelease.Sequence++
	if SameReleaseResultPackageParams(release, changedRelease) {
		t.Fatal("release comparison ignored sequence")
	}
}

func validResultPackageMetadata(t *testing.T) ResultPackageMetadata {
	t.Helper()
	data, descriptor, err := EncodeResultManifest(validResultManifest())
	if err != nil {
		t.Fatal(err)
	}
	return ResultPackageMetadata{Manifest: data, ManifestDescriptor: descriptor}
}

func cloneResultPackageMetadata(metadata ResultPackageMetadata) ResultPackageMetadata {
	metadata.Manifest = append([]byte{}, metadata.Manifest...)
	return metadata
}
