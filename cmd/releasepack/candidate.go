package main

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	candidateSchemaVersion     = 1
	evidenceSchemaVersion      = 1
	candidateDescriptorName    = "release-candidate.json"
	candidateProvenanceName    = "candidate-provenance.sigstore.json"
	candidateManifestName      = "release-artifacts.sha256"
	candidatePayloadDomain     = "delegation-release-candidate-payload-v1\n"
	maxCandidateDescriptorSize = 256 << 10
	maxEvidenceSize            = 16 << 10
	maxProvenanceSize          = 16 << 20
	maxArchiveSize             = 512 << 20
)

var (
	commitPattern          = regexp.MustCompile(`^(?:[0-9a-f]{40}|[0-9a-f]{64})$`)
	repositoryPattern      = regexp.MustCompile(`^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$`)
	runIDPattern           = regexp.MustCompile(`^[1-9][0-9]*$`)
	notarizationIDPattern  = regexp.MustCompile(`^[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}$`)
	lowercaseDigestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
	canonicalGzipHeader    = [...]byte{0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff}
)

type signatureEvidence struct {
	SchemaVersion         int      `json:"schemaVersion"`
	Target                string   `json:"target"`
	SignatureKind         string   `json:"signatureKind"`
	Status                string   `json:"status"`
	Verifications         []string `json:"verifications"`
	NotarizationRequestID string   `json:"notarizationRequestId"`
}

type candidateDescriptor struct {
	SchemaVersion     int                         `json:"schemaVersion"`
	Repository        string                      `json:"repository"`
	RuntimeVersion    string                      `json:"runtimeVersion"`
	SourceCommit      string                      `json:"sourceCommit"`
	WorkflowRunID     string                      `json:"workflowRunId"`
	CandidateArtifact candidateArtifactIdentity   `json:"candidateArtifact"`
	Artifacts         []candidateArtifactMetadata `json:"artifacts"`
}

type candidateArtifactIdentity struct {
	Name          string `json:"name"`
	PayloadSHA256 string `json:"payloadSha256"`
}

type candidateArtifactMetadata struct {
	Name      string                     `json:"name"`
	OS        string                     `json:"os"`
	Arch      string                     `json:"arch"`
	SizeBytes int64                      `json:"sizeBytes"`
	SHA256    string                     `json:"sha256"`
	Evidence  candidateEvidenceReference `json:"evidence"`
}

type candidateEvidenceReference struct {
	Name                  string `json:"name"`
	SizeBytes             int64  `json:"sizeBytes"`
	SHA256                string `json:"sha256"`
	SignatureKind         string `json:"signatureKind"`
	Status                string `json:"status"`
	NotarizationRequestID string `json:"notarizationRequestId"`
}

type candidateExpectations struct {
	Repository        string
	SourceCommit      string
	WorkflowRunID     string
	CandidateName     string
	RequireProvenance bool
}

type candidatePayloadEntry struct {
	Name      string
	SizeBytes int64
	SHA256    string
}

func parseTarget(value string) (target, error) {
	for _, candidate := range releaseTargets {
		if candidate.id() == value {
			return candidate, nil
		}
	}
	return target{}, fmt.Errorf("unknown target %q", value)
}

func (t target) id() string {
	return t.os + "-" + t.arch
}

func (t target) binaryName() string {
	if t.os == "windows" {
		return "delegation.exe"
	}
	return "delegation"
}

func (t target) archiveName(version string) string {
	return fmt.Sprintf("delegation_%s_%s_%s.%s", version, t.os, t.arch, t.archive)
}

func (t target) evidenceName() string {
	return fmt.Sprintf("evidence_%s_%s.json", t.os, t.arch)
}

func expectedEvidence(t target, notarizationRequestID string) (signatureEvidence, error) {
	evidence := signatureEvidence{
		SchemaVersion: evidenceSchemaVersion,
		Target:        t.id(),
		Verifications: []string{},
	}
	switch t.os {
	case "linux":
		if notarizationRequestID != "" {
			return signatureEvidence{}, errors.New("Linux evidence cannot contain a notarization request ID")
		}
		evidence.SignatureKind = "none"
		evidence.Status = "notRequired"
		evidence.Verifications = []string{"unsigned-platform"}
	case "darwin":
		if !notarizationIDPattern.MatchString(notarizationRequestID) {
			return signatureEvidence{}, errors.New("macOS evidence requires a notarization request UUID")
		}
		evidence.SignatureKind = "appleDeveloperId"
		evidence.Status = "verified"
		evidence.Verifications = []string{
			"codesign-strict",
			"hardened-runtime",
			"notarization-accepted",
			"secure-timestamp",
		}
		evidence.NotarizationRequestID = strings.ToLower(notarizationRequestID)
	case "windows":
		if notarizationRequestID != "" {
			return signatureEvidence{}, errors.New("Windows evidence cannot contain a notarization request ID")
		}
		evidence.SignatureKind = "authenticode"
		evidence.Status = "verified"
		evidence.Verifications = []string{
			"authenticode-chain",
			"rfc3161-timestamp",
			"sha256-digest",
		}
	default:
		return signatureEvidence{}, fmt.Errorf("unsupported target operating system %q", t.os)
	}
	return evidence, nil
}

func writeEvidence(path string, t target, notarizationRequestID string) error {
	evidence, err := expectedEvidence(t, notarizationRequestID)
	if err != nil {
		return err
	}
	data, err := canonicalJSON(evidence)
	if err != nil {
		return err
	}
	if err := writeNewFile(path, data, 0o644); err != nil {
		return fmt.Errorf("write signing evidence: %w", err)
	}
	return nil
}

func assembleCandidate(
	input,
	output,
	repository,
	version,
	sourceCommit,
	workflowRunID,
	candidateName string,
	notice []byte,
) error {
	if err := validateCandidateIdentity(repository, sourceCommit, workflowRunID, candidateName); err != nil {
		return err
	}
	if !versionPattern.MatchString(version) {
		return fmt.Errorf("invalid runtime version %q", version)
	}
	input, err := filepath.Abs(input)
	if err != nil {
		return fmt.Errorf("resolve candidate input: %w", err)
	}
	output, err = filepath.Abs(output)
	if err != nil {
		return fmt.Errorf("resolve candidate output: %w", err)
	}
	if err := requireNewPath(output); err != nil {
		return err
	}
	if err := requireExactRegularFiles(input, expectedPartNames(version)); err != nil {
		return fmt.Errorf("validate candidate inputs: %w", err)
	}
	parent := filepath.Dir(output)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return fmt.Errorf("create candidate output parent: %w", err)
	}
	staging, err := os.MkdirTemp(parent, ".delegation-candidate-*")
	if err != nil {
		return fmt.Errorf("create candidate staging directory: %w", err)
	}
	defer os.RemoveAll(staging)

	entries := make([]candidatePayloadEntry, 0, len(releaseTargets)*2+1)
	artifacts := make([]candidateArtifactMetadata, 0, len(releaseTargets))
	checksums := make(map[string]string, len(releaseTargets))
	for _, t := range releaseTargets {
		archiveName := t.archiveName(version)
		archiveEntry, err := copyCandidateFile(input, staging, archiveName, maxArchiveSize)
		if err != nil {
			return err
		}
		if err := verifyRuntimeArchive(filepath.Join(staging, archiveName), t, notice); err != nil {
			return fmt.Errorf("verify %s: %w", archiveName, err)
		}
		evidenceName := t.evidenceName()
		evidenceEntry, err := copyCandidateFile(input, staging, evidenceName, maxEvidenceSize)
		if err != nil {
			return err
		}
		evidence, err := readEvidence(filepath.Join(staging, evidenceName), t)
		if err != nil {
			return err
		}
		checksums[archiveName] = archiveEntry.SHA256
		entries = append(entries, archiveEntry, evidenceEntry)
		artifacts = append(artifacts, candidateArtifactMetadata{
			Name:      archiveName,
			OS:        t.os,
			Arch:      t.arch,
			SizeBytes: archiveEntry.SizeBytes,
			SHA256:    archiveEntry.SHA256,
			Evidence: candidateEvidenceReference{
				Name:                  evidenceName,
				SizeBytes:             evidenceEntry.SizeBytes,
				SHA256:                evidenceEntry.SHA256,
				SignatureKind:         evidence.SignatureKind,
				Status:                evidence.Status,
				NotarizationRequestID: evidence.NotarizationRequestID,
			},
		})
	}
	manifestPath := filepath.Join(staging, candidateManifestName)
	if err := writeChecksumManifest(manifestPath, checksums); err != nil {
		return err
	}
	manifestEntry, err := inspectCandidateFile(manifestPath, candidateManifestName, maxEvidenceSize)
	if err != nil {
		return err
	}
	entries = append(entries, manifestEntry)
	descriptor := candidateDescriptor{
		SchemaVersion:  candidateSchemaVersion,
		Repository:     repository,
		RuntimeVersion: version,
		SourceCommit:   sourceCommit,
		WorkflowRunID:  workflowRunID,
		CandidateArtifact: candidateArtifactIdentity{
			Name:          candidateName,
			PayloadSHA256: candidatePayloadSHA256(entries),
		},
		Artifacts: artifacts,
	}
	descriptorData, err := canonicalJSON(descriptor)
	if err != nil {
		return err
	}
	if err := writeNewFile(filepath.Join(staging, candidateDescriptorName), descriptorData, 0o644); err != nil {
		return fmt.Errorf("write candidate descriptor: %w", err)
	}
	if _, err := verifyCandidate(staging, candidateExpectations{
		Repository:    repository,
		SourceCommit:  sourceCommit,
		WorkflowRunID: workflowRunID,
		CandidateName: candidateName,
	}, notice); err != nil {
		return fmt.Errorf("verify assembled candidate: %w", err)
	}
	if err := commitReleaseDirectory(staging, output); err != nil {
		return fmt.Errorf("commit candidate directory: %w", err)
	}
	return nil
}

func verifyCandidate(root string, expected candidateExpectations, notice []byte) (candidateDescriptor, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return candidateDescriptor{}, fmt.Errorf("resolve candidate directory: %w", err)
	}
	descriptorPath := filepath.Join(root, candidateDescriptorName)
	descriptorData, err := readBoundedRegularFile(descriptorPath, maxCandidateDescriptorSize)
	if err != nil {
		return candidateDescriptor{}, fmt.Errorf("read candidate descriptor: %w", err)
	}
	var descriptor candidateDescriptor
	if err := decodeCanonicalJSON(descriptorData, &descriptor); err != nil {
		return candidateDescriptor{}, fmt.Errorf("decode candidate descriptor: %w", err)
	}
	if descriptor.SchemaVersion != candidateSchemaVersion {
		return candidateDescriptor{}, fmt.Errorf("unsupported candidate schema version %d", descriptor.SchemaVersion)
	}
	if err := validateCandidateIdentity(
		descriptor.Repository,
		descriptor.SourceCommit,
		descriptor.WorkflowRunID,
		descriptor.CandidateArtifact.Name,
	); err != nil {
		return candidateDescriptor{}, err
	}
	if !versionPattern.MatchString(descriptor.RuntimeVersion) {
		return candidateDescriptor{}, fmt.Errorf("invalid descriptor runtime version %q", descriptor.RuntimeVersion)
	}
	if expected.Repository != "" && descriptor.Repository != expected.Repository {
		return candidateDescriptor{}, fmt.Errorf("candidate repository %q does not match %q", descriptor.Repository, expected.Repository)
	}
	if expected.SourceCommit != "" && descriptor.SourceCommit != expected.SourceCommit {
		return candidateDescriptor{}, fmt.Errorf("candidate source commit %q does not match %q", descriptor.SourceCommit, expected.SourceCommit)
	}
	if expected.WorkflowRunID != "" && descriptor.WorkflowRunID != expected.WorkflowRunID {
		return candidateDescriptor{}, fmt.Errorf("candidate workflow run %q does not match %q", descriptor.WorkflowRunID, expected.WorkflowRunID)
	}
	if expected.CandidateName != "" && descriptor.CandidateArtifact.Name != expected.CandidateName {
		return candidateDescriptor{}, fmt.Errorf("candidate artifact %q does not match %q", descriptor.CandidateArtifact.Name, expected.CandidateName)
	}

	expectedNames := expectedCandidateNames(descriptor.RuntimeVersion, expected.RequireProvenance)
	if err := requireExactRegularFiles(root, expectedNames); err != nil {
		return candidateDescriptor{}, err
	}
	if len(descriptor.Artifacts) != len(releaseTargets) {
		return candidateDescriptor{}, fmt.Errorf("candidate artifacts = %d, want %d", len(descriptor.Artifacts), len(releaseTargets))
	}
	entries := make([]candidatePayloadEntry, 0, len(releaseTargets)*2+1)
	checksums := make(map[string]string, len(releaseTargets))
	for index, t := range releaseTargets {
		artifact := descriptor.Artifacts[index]
		archiveName := t.archiveName(descriptor.RuntimeVersion)
		if artifact.Name != archiveName || artifact.OS != t.os || artifact.Arch != t.arch {
			return candidateDescriptor{}, fmt.Errorf("candidate artifact %d has unexpected target metadata", index)
		}
		archiveEntry, err := inspectCandidateFile(filepath.Join(root, archiveName), archiveName, maxArchiveSize)
		if err != nil {
			return candidateDescriptor{}, err
		}
		if archiveEntry.SizeBytes != artifact.SizeBytes || archiveEntry.SHA256 != artifact.SHA256 {
			return candidateDescriptor{}, fmt.Errorf("candidate archive %s does not match its descriptor", archiveName)
		}
		if err := verifyRuntimeArchive(filepath.Join(root, archiveName), t, notice); err != nil {
			return candidateDescriptor{}, fmt.Errorf("verify %s: %w", archiveName, err)
		}
		evidenceName := t.evidenceName()
		evidenceEntry, err := inspectCandidateFile(filepath.Join(root, evidenceName), evidenceName, maxEvidenceSize)
		if err != nil {
			return candidateDescriptor{}, err
		}
		evidence, err := readEvidence(filepath.Join(root, evidenceName), t)
		if err != nil {
			return candidateDescriptor{}, err
		}
		reference := artifact.Evidence
		if reference.Name != evidenceName || reference.SizeBytes != evidenceEntry.SizeBytes ||
			reference.SHA256 != evidenceEntry.SHA256 || reference.SignatureKind != evidence.SignatureKind ||
			reference.Status != evidence.Status || reference.NotarizationRequestID != evidence.NotarizationRequestID {
			return candidateDescriptor{}, fmt.Errorf("candidate evidence %s does not match its descriptor", evidenceName)
		}
		checksums[archiveName] = archiveEntry.SHA256
		entries = append(entries, archiveEntry, evidenceEntry)
	}
	manifestPath := filepath.Join(root, candidateManifestName)
	if err := verifyChecksumManifest(manifestPath, checksums); err != nil {
		return candidateDescriptor{}, err
	}
	manifestEntry, err := inspectCandidateFile(manifestPath, candidateManifestName, maxEvidenceSize)
	if err != nil {
		return candidateDescriptor{}, err
	}
	entries = append(entries, manifestEntry)
	if got := candidatePayloadSHA256(entries); got != descriptor.CandidateArtifact.PayloadSHA256 {
		return candidateDescriptor{}, fmt.Errorf("candidate payload digest %s does not match descriptor %s", got, descriptor.CandidateArtifact.PayloadSHA256)
	}
	if expected.RequireProvenance {
		if err := verifyJSONFile(filepath.Join(root, candidateProvenanceName), maxProvenanceSize); err != nil {
			return candidateDescriptor{}, fmt.Errorf("verify candidate provenance bundle: %w", err)
		}
	}
	return descriptor, nil
}

func validateCandidateIdentity(repository, sourceCommit, workflowRunID, candidateName string) error {
	if !repositoryPattern.MatchString(repository) {
		return fmt.Errorf("invalid GitHub repository %q", repository)
	}
	if !commitPattern.MatchString(sourceCommit) {
		return fmt.Errorf("invalid source commit %q", sourceCommit)
	}
	if !runIDPattern.MatchString(workflowRunID) {
		return fmt.Errorf("invalid workflow run ID %q", workflowRunID)
	}
	expectedName := "delegation-release-candidate-" + workflowRunID
	if candidateName != expectedName {
		return fmt.Errorf("candidate artifact name %q does not match %q", candidateName, expectedName)
	}
	return nil
}

func expectedPartNames(version string) map[string]struct{} {
	names := make(map[string]struct{}, len(releaseTargets)*2)
	for _, t := range releaseTargets {
		names[t.archiveName(version)] = struct{}{}
		names[t.evidenceName()] = struct{}{}
	}
	return names
}

func expectedCandidateNames(version string, requireProvenance bool) map[string]struct{} {
	names := expectedPartNames(version)
	names[candidateManifestName] = struct{}{}
	names[candidateDescriptorName] = struct{}{}
	if requireProvenance {
		names[candidateProvenanceName] = struct{}{}
	}
	return names
}

func copyCandidateFile(sourceRoot, destinationRoot, name string, limit int64) (candidatePayloadEntry, error) {
	source := filepath.Join(sourceRoot, name)
	destination := filepath.Join(destinationRoot, name)
	input, info, err := openRegularFile(source, limit)
	if err != nil {
		return candidatePayloadEntry{}, fmt.Errorf("open candidate input %s: %w", name, err)
	}
	defer input.Close()
	output, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return candidatePayloadEntry{}, fmt.Errorf("create candidate file %s: %w", name, err)
	}
	digest := sha256.New()
	copied, copyErr := io.Copy(io.MultiWriter(output, digest), input)
	closeErr := output.Close()
	if err := errors.Join(copyErr, closeErr); err != nil {
		return candidatePayloadEntry{}, fmt.Errorf("copy candidate file %s: %w", name, err)
	}
	if copied != info.Size() {
		return candidatePayloadEntry{}, fmt.Errorf("candidate input %s changed size while copying", name)
	}
	return candidatePayloadEntry{Name: name, SizeBytes: info.Size(), SHA256: hex.EncodeToString(digest.Sum(nil))}, nil
}

func inspectCandidateFile(path, name string, limit int64) (candidatePayloadEntry, error) {
	file, info, err := openRegularFile(path, limit)
	if err != nil {
		return candidatePayloadEntry{}, fmt.Errorf("open candidate file %s: %w", name, err)
	}
	digest := sha256.New()
	copied, copyErr := io.Copy(digest, file)
	closeErr := file.Close()
	if err := errors.Join(copyErr, closeErr); err != nil {
		return candidatePayloadEntry{}, fmt.Errorf("hash candidate file %s: %w", name, err)
	}
	if copied != info.Size() {
		return candidatePayloadEntry{}, fmt.Errorf("candidate file %s changed size while hashing", name)
	}
	return candidatePayloadEntry{Name: name, SizeBytes: info.Size(), SHA256: hex.EncodeToString(digest.Sum(nil))}, nil
}

func openRegularFile(path string, limit int64) (*os.File, os.FileInfo, error) {
	before, err := os.Lstat(path)
	if err != nil {
		return nil, nil, err
	}
	if !before.Mode().IsRegular() || before.Size() < 0 || before.Size() > limit {
		return nil, nil, fmt.Errorf("must be a regular file no larger than %d bytes", limit)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	after, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, nil, err
	}
	if !after.Mode().IsRegular() || !os.SameFile(before, after) || after.Size() != before.Size() {
		file.Close()
		return nil, nil, errors.New("file identity changed while opening")
	}
	return file, after, nil
}

func readBoundedRegularFile(path string, limit int64) ([]byte, error) {
	file, _, err := openRegularFile(path, limit)
	if err != nil {
		return nil, err
	}
	data, readErr := io.ReadAll(io.LimitReader(file, limit+1))
	closeErr := file.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("file grew beyond %d bytes while reading", limit)
	}
	return data, nil
}

func requireExactRegularFiles(root string, expected map[string]struct{}) error {
	rootInfo, err := os.Lstat(root)
	if err != nil {
		return err
	}
	if !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 {
		return errors.New("candidate root must be a directory, not a link")
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	if len(entries) != len(expected) {
		return fmt.Errorf("candidate contains %d entries, want %d", len(entries), len(expected))
	}
	for _, entry := range entries {
		if _, ok := expected[entry.Name()]; !ok {
			return fmt.Errorf("candidate contains unexpected entry %q", entry.Name())
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("candidate entry %q is not a regular file", entry.Name())
		}
	}
	return nil
}

func readEvidence(path string, t target) (signatureEvidence, error) {
	data, err := readBoundedRegularFile(path, maxEvidenceSize)
	if err != nil {
		return signatureEvidence{}, fmt.Errorf("read evidence %s: %w", t.evidenceName(), err)
	}
	var evidence signatureEvidence
	if err := decodeCanonicalJSON(data, &evidence); err != nil {
		return signatureEvidence{}, fmt.Errorf("decode evidence %s: %w", t.evidenceName(), err)
	}
	expected, err := expectedEvidence(t, evidence.NotarizationRequestID)
	if err != nil {
		return signatureEvidence{}, fmt.Errorf("validate evidence %s: %w", t.evidenceName(), err)
	}
	if !evidenceEqual(evidence, expected) {
		return signatureEvidence{}, fmt.Errorf("evidence %s does not contain the required verification result", t.evidenceName())
	}
	return evidence, nil
}

func evidenceEqual(left, right signatureEvidence) bool {
	if left.SchemaVersion != right.SchemaVersion || left.Target != right.Target ||
		left.SignatureKind != right.SignatureKind || left.Status != right.Status ||
		left.NotarizationRequestID != right.NotarizationRequestID || len(left.Verifications) != len(right.Verifications) {
		return false
	}
	for index := range left.Verifications {
		if left.Verifications[index] != right.Verifications[index] {
			return false
		}
	}
	return true
}

func verifyChecksumManifest(path string, checksums map[string]string) error {
	data, err := readBoundedRegularFile(path, maxEvidenceSize)
	if err != nil {
		return fmt.Errorf("read candidate manifest: %w", err)
	}
	names := make([]string, 0, len(checksums))
	for name := range checksums {
		names = append(names, name)
	}
	sort.Strings(names)
	var expected strings.Builder
	for _, name := range names {
		fmt.Fprintf(&expected, "%s  %s\n", checksums[name], name)
	}
	if string(data) != expected.String() {
		return errors.New("candidate manifest does not exactly match archive digests")
	}
	return nil
}

func verifyRelease(repoRoot, releaseRoot string) error {
	repoRoot, err := filepath.Abs(repoRoot)
	if err != nil {
		return fmt.Errorf("resolve repository root: %w", err)
	}
	releaseRoot, err = filepath.Abs(releaseRoot)
	if err != nil {
		return fmt.Errorf("resolve release directory: %w", err)
	}
	version, err := readVersion(repoRoot)
	if err != nil {
		return err
	}
	notice, err := readReleaseNotice(repoRoot)
	if err != nil {
		return err
	}
	expectedNames := make(map[string]struct{}, len(releaseTargets)+1)
	expectedNames[candidateManifestName] = struct{}{}
	for _, target := range releaseTargets {
		expectedNames[target.archiveName(version)] = struct{}{}
	}
	if err := requireExactRegularFiles(releaseRoot, expectedNames); err != nil {
		return err
	}
	checksums := make(map[string]string, len(releaseTargets))
	for _, target := range releaseTargets {
		archiveName := target.archiveName(version)
		archivePath := filepath.Join(releaseRoot, archiveName)
		if err := verifyRuntimeArchive(archivePath, target, notice); err != nil {
			return fmt.Errorf("verify %s: %w", archiveName, err)
		}
		digest, err := fileSHA256(archivePath)
		if err != nil {
			return fmt.Errorf("hash %s: %w", archiveName, err)
		}
		checksums[archiveName] = digest
	}
	if err := verifyChecksumManifest(
		filepath.Join(releaseRoot, candidateManifestName),
		checksums,
	); err != nil {
		return err
	}
	return nil
}

func verifyRuntimeArchive(path string, t target, notice []byte) error {
	if t.archive == "zip" {
		reader, err := zip.OpenReader(path)
		if err != nil {
			return err
		}
		defer reader.Close()
		if len(reader.File) != 2 {
			return fmt.Errorf("zip contains %d entries, want 2", len(reader.File))
		}
		if reader.Comment != "" {
			return errors.New("zip archive comment is not canonical")
		}
		binaryEntry := reader.File[0]
		if binaryEntry.Name != t.binaryName() || !binaryEntry.Mode().IsRegular() ||
			binaryEntry.Mode() != 0o755 || binaryEntry.UncompressedSize64 == 0 ||
			binaryEntry.UncompressedSize64 > maxArchiveSize || !zipMetadataIsFixed(binaryEntry, 0o755) {
			return errors.New("zip does not contain the expected executable")
		}
		noticeEntry := reader.File[1]
		if noticeEntry.Name != releaseNoticeName || !noticeEntry.Mode().IsRegular() ||
			noticeEntry.Mode() != 0o644 || noticeEntry.UncompressedSize64 != uint64(len(notice)) ||
			!zipMetadataIsFixed(noticeEntry, 0o644) {
			return errors.New("zip does not contain the expected release notice")
		}
		if err := consumeZipEntry(binaryEntry, nil); err != nil {
			return err
		}
		return consumeZipEntry(noticeEntry, notice)
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	var rawGzipHeader [len(canonicalGzipHeader)]byte
	if _, err := file.ReadAt(rawGzipHeader[:], 0); err != nil {
		return fmt.Errorf("read gzip header: %w", err)
	}
	if rawGzipHeader != canonicalGzipHeader {
		return errors.New("gzip archive header is not canonical")
	}
	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		return err
	}
	defer gzipReader.Close()
	if !gzipReader.ModTime.IsZero() || gzipReader.Name != "" ||
		gzipReader.Comment != "" || len(gzipReader.Extra) != 0 || gzipReader.OS != 255 {
		return errors.New("gzip archive metadata is not canonical")
	}
	tarReader := tar.NewReader(gzipReader)
	binaryHeader, err := tarReader.Next()
	if err != nil {
		return err
	}
	if binaryHeader.Name != t.binaryName() || binaryHeader.Typeflag != tar.TypeReg ||
		binaryHeader.Mode != 0o755 || binaryHeader.Size <= 0 ||
		binaryHeader.Size > maxArchiveSize || !tarMetadataIsFixed(binaryHeader) {
		return errors.New("tar archive does not contain the expected executable")
	}
	if _, err := io.Copy(io.Discard, tarReader); err != nil {
		return err
	}
	noticeHeader, err := tarReader.Next()
	if err != nil {
		return fmt.Errorf("read tar release notice: %w", err)
	}
	if noticeHeader.Name != releaseNoticeName || noticeHeader.Typeflag != tar.TypeReg ||
		noticeHeader.Mode != 0o644 || noticeHeader.Size != int64(len(notice)) ||
		!tarMetadataIsFixed(noticeHeader) {
		return errors.New("tar archive does not contain the expected release notice")
	}
	archivedNotice, err := io.ReadAll(io.LimitReader(tarReader, int64(len(notice))+1))
	if err != nil {
		return err
	}
	if !bytes.Equal(archivedNotice, notice) {
		return errors.New("tar release notice does not match the audited notice")
	}
	if _, err := tarReader.Next(); err != io.EOF {
		return fmt.Errorf("tar contains an unexpected third entry: %w", err)
	}
	return nil
}

func zipMetadataIsFixed(entry *zip.File, mode os.FileMode) bool {
	const unixRegularFile = 0o100000

	return entry.Modified.Equal(archiveTime) && entry.Method == zip.Deflate &&
		entry.ModifiedDate == 33 && entry.ModifiedTime == 0 &&
		entry.Comment == "" && bytes.Equal(entry.Extra, []byte{
		0x55, 0x54, 0x05, 0x00, 0x01, 0x00, 0xa6, 0xce, 0x12,
	}) && !entry.NonUTF8 && entry.CreatorVersion == 0x314 &&
		entry.ReaderVersion == 0x14 && entry.Flags == 0x8 &&
		entry.ExternalAttrs == uint32(unixRegularFile|mode)<<16
}

func consumeZipEntry(entry *zip.File, expected []byte) error {
	opened, err := entry.Open()
	if err != nil {
		return err
	}
	var destination io.Writer = io.Discard
	var archived bytes.Buffer
	if expected != nil {
		destination = &archived
	}
	_, copyErr := io.Copy(destination, opened)
	if err := errors.Join(copyErr, opened.Close()); err != nil {
		return err
	}
	if expected != nil && !bytes.Equal(archived.Bytes(), expected) {
		return errors.New("zip release notice does not match the audited notice")
	}
	return nil
}

func tarMetadataIsFixed(header *tar.Header) bool {
	return header.ModTime.Equal(time.Unix(0, 0).UTC()) && header.Uid == 0 && header.Gid == 0 &&
		header.Uname == "" && header.Gname == "" && header.Linkname == "" &&
		header.AccessTime.IsZero() && header.ChangeTime.IsZero() &&
		header.Devmajor == 0 && header.Devminor == 0 && len(header.PAXRecords) == 0 &&
		len(header.Xattrs) == 0 && header.Format == tar.FormatUSTAR
}

func candidatePayloadSHA256(entries []candidatePayloadEntry) string {
	sorted := append([]candidatePayloadEntry(nil), entries...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })
	digest := sha256.New()
	io.WriteString(digest, candidatePayloadDomain)
	for _, entry := range sorted {
		fmt.Fprintf(digest, "%s  %d  %s\n", entry.SHA256, entry.SizeBytes, entry.Name)
	}
	return hex.EncodeToString(digest.Sum(nil))
}

func canonicalJSON(value any) ([]byte, error) {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

func decodeCanonicalJSON(data []byte, value any) error {
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	if err := requireJSONEOF(decoder); err != nil {
		return err
	}
	canonical, err := canonicalJSON(value)
	if err != nil {
		return err
	}
	if string(canonical) != string(data) {
		return errors.New("JSON is not in canonical Delegation form")
	}
	return nil
}

func requireJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("JSON contains more than one value")
		}
		return err
	}
	return nil
}

func verifyJSONFile(path string, limit int64) error {
	data, err := readBoundedRegularFile(path, limit)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	var value any
	if err := decoder.Decode(&value); err != nil {
		return err
	}
	return requireJSONEOF(decoder)
}

func writeNewFile(path string, data []byte, mode os.FileMode) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(data)
	return errors.Join(writeErr, file.Close())
}

func requireNewPath(path string) error {
	if _, err := os.Lstat(path); err == nil {
		return fmt.Errorf("output already exists: %s", path)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect output: %w", err)
	}
	return nil
}

func parsePositiveID(value, field string) (int64, error) {
	if !runIDPattern.MatchString(value) {
		return 0, fmt.Errorf("invalid %s %q", field, value)
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("invalid %s %q", field, value)
	}
	return parsed, nil
}
