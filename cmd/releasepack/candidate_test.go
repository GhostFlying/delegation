package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const testNotarizationRequestID = "d5c99425-16c7-47ab-9e96-8f65f018111f"

type candidateFixture struct {
	root          string
	input         string
	candidate     string
	version       string
	repository    string
	sourceCommit  string
	workflowRunID string
	candidateName string
}

func TestCandidateDescriptorAndPayloadVerify(t *testing.T) {
	fixture := makeCandidateFixture(t, "1.2.3-rc.1", strings.Repeat("a", 40))
	descriptor, err := verifyCandidate(fixture.candidate, fixture.expectations())
	if err != nil {
		t.Fatal(err)
	}
	if descriptor.RuntimeVersion != fixture.version || len(descriptor.Artifacts) != len(releaseTargets) {
		t.Fatalf("descriptor = %#v", descriptor)
	}
	if !lowercaseDigestPattern.MatchString(descriptor.CandidateArtifact.PayloadSHA256) {
		t.Fatalf("payload digest = %q", descriptor.CandidateArtifact.PayloadSHA256)
	}
	for index, target := range releaseTargets {
		artifact := descriptor.Artifacts[index]
		if artifact.Name != target.archiveName(fixture.version) || artifact.Evidence.Name != target.evidenceName() {
			t.Fatalf("artifact %d = %#v", index, artifact)
		}
	}
}

func TestCandidateVerifierRejectsTamperingAndUnexpectedEntries(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, candidateFixture)
		want   string
	}{
		{
			name: "archive bytes",
			mutate: func(t *testing.T, fixture candidateFixture) {
				t.Helper()
				path := filepath.Join(fixture.candidate, releaseTargets[0].archiveName(fixture.version))
				file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := file.WriteString("tampered"); err != nil {
					t.Fatal(err)
				}
				if err := file.Close(); err != nil {
					t.Fatal(err)
				}
			},
			want: "does not match its descriptor",
		},
		{
			name: "unexpected entry",
			mutate: func(t *testing.T, fixture candidateFixture) {
				t.Helper()
				if err := os.WriteFile(filepath.Join(fixture.candidate, "unlisted"), []byte("extra"), 0o644); err != nil {
					t.Fatal(err)
				}
			},
			want: "contains 16 entries, want 15",
		},
		{
			name: "noncanonical descriptor",
			mutate: func(t *testing.T, fixture candidateFixture) {
				t.Helper()
				path := filepath.Join(fixture.candidate, candidateDescriptorName)
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				var compact any
				if err := json.Unmarshal(data, &compact); err != nil {
					t.Fatal(err)
				}
				data, err = json.Marshal(compact)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, data, 0o644); err != nil {
					t.Fatal(err)
				}
			},
			want: "not in canonical Delegation form",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := makeCandidateFixture(t, "1.2.3", strings.Repeat("b", 40))
			test.mutate(t, fixture)
			_, err := verifyCandidate(fixture.candidate, fixture.expectations())
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("verifyCandidate() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestCandidateAssemblerRejectsUnverifiedEvidence(t *testing.T) {
	fixture := makeCandidateParts(t, "1.2.3", strings.Repeat("c", 40))
	path := filepath.Join(fixture.input, releaseTargets[2].evidenceName())
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var evidence signatureEvidence
	if err := json.Unmarshal(data, &evidence); err != nil {
		t.Fatal(err)
	}
	evidence.Status = "claimed"
	data, err = canonicalJSON(evidence)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	err = assembleCandidate(
		fixture.input,
		fixture.candidate,
		fixture.repository,
		fixture.version,
		fixture.sourceCommit,
		fixture.workflowRunID,
		fixture.candidateName,
	)
	if err == nil || !strings.Contains(err.Error(), "required verification result") {
		t.Fatalf("assembleCandidate() error = %v", err)
	}
}

func makeCandidateFixture(t *testing.T, version, sourceCommit string) candidateFixture {
	t.Helper()
	fixture := makeCandidateParts(t, version, sourceCommit)
	if err := assembleCandidate(
		fixture.input,
		fixture.candidate,
		fixture.repository,
		fixture.version,
		fixture.sourceCommit,
		fixture.workflowRunID,
		fixture.candidateName,
	); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(fixture.candidate, candidateProvenanceName), []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	return fixture
}

func makeCandidateParts(t *testing.T, version, sourceCommit string) candidateFixture {
	t.Helper()
	root := t.TempDir()
	input := filepath.Join(root, "input")
	if err := os.Mkdir(input, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, target := range releaseTargets {
		binary := filepath.Join(root, "binary-"+target.id())
		if err := os.WriteFile(binary, []byte("signed "+target.id()+"\n"), 0o755); err != nil {
			t.Fatal(err)
		}
		archive := filepath.Join(input, target.archiveName(version))
		var err error
		if target.archive == "zip" {
			err = writeZip(archive, binary, target.binaryName())
		} else {
			err = writeTarGzip(archive, binary, target.binaryName())
		}
		if err != nil {
			t.Fatal(err)
		}
		notarizationID := ""
		if target.os == "darwin" {
			notarizationID = testNotarizationRequestID
		}
		if err := writeEvidence(filepath.Join(input, target.evidenceName()), target, notarizationID); err != nil {
			t.Fatal(err)
		}
	}
	workflowRunID := "123456"
	return candidateFixture{
		root:          root,
		input:         input,
		candidate:     filepath.Join(root, "candidate"),
		version:       version,
		repository:    "GhostFlying/delegation",
		sourceCommit:  sourceCommit,
		workflowRunID: workflowRunID,
		candidateName: "delegation-release-candidate-" + workflowRunID,
	}
}

func (f candidateFixture) expectations() candidateExpectations {
	return candidateExpectations{
		Repository:        f.repository,
		SourceCommit:      f.sourceCommit,
		WorkflowRunID:     f.workflowRunID,
		CandidateName:     f.candidateName,
		RequireProvenance: true,
	}
}
