package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

type promotionFixture struct {
	repoRoot       string
	candidateRoot  string
	repository     string
	tag            string
	sourceCommit   string
	workflowRunID  string
	candidateName  string
	artifactID     string
	artifactDigest string
}

func TestPromotionRequiresManifestOnlyChildCommit(t *testing.T) {
	fixture := makePromotionFixture(t, false)
	contract, err := verifyPromotion(
		fixture.repoRoot,
		fixture.candidateRoot,
		fixture.repository,
		fixture.tag,
		fixture.sourceCommit,
		fixture.workflowRunID,
		fixture.candidateName,
	)
	if err != nil {
		t.Fatal(err)
	}
	if contract.SourceCommit != fixture.sourceCommit || contract.Tag != fixture.tag ||
		contract.ArtifactName != fixture.candidateName || !lowercaseDigestPattern.MatchString(contract.PayloadSHA256) {
		t.Fatalf("contract = %#v", contract)
	}
	predicatePath := filepath.Join(t.TempDir(), "promotion.json")
	if err := writePromotionPredicate(predicatePath, fixture.artifactID, fixture.artifactDigest, contract); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(predicatePath)
	if err != nil {
		t.Fatal(err)
	}
	var predicate promotionPredicate
	if err := decodeCanonicalJSON(data, &predicate); err != nil {
		t.Fatal(err)
	}
	if predicate.Candidate.ArtifactID != fixture.artifactID ||
		predicate.Candidate.ArtifactSHA256 != fixture.artifactDigest || predicate.TagCommit != contract.TagCommit {
		t.Fatalf("predicate = %#v", predicate)
	}
}

func TestPromotionRejectsAdditionalSourceChange(t *testing.T) {
	fixture := makePromotionFixture(t, true)
	_, err := verifyPromotion(
		fixture.repoRoot,
		fixture.candidateRoot,
		fixture.repository,
		fixture.tag,
		fixture.sourceCommit,
		fixture.workflowRunID,
		fixture.candidateName,
	)
	if err == nil || !strings.Contains(err.Error(), "must change only the checksum manifest") {
		t.Fatalf("verifyPromotion() error = %v", err)
	}
}

func TestPromotionRejectsMergeCommit(t *testing.T) {
	fixture := makePromotionFixture(t, false)
	runGitTest(t, fixture.repoRoot, "tag", "-d", fixture.tag)
	runGitTest(t, fixture.repoRoot, "branch", "release-side", fixture.sourceCommit)
	runGitTest(t, fixture.repoRoot, "checkout", "release-side")
	runGitTest(
		t,
		fixture.repoRoot,
		"-c", "user.name=Release Test",
		"-c", "user.email=release-test@example.invalid",
		"commit", "--allow-empty", "-m", "independent release parent",
	)
	runGitTest(t, fixture.repoRoot, "checkout", "main")
	runGitTest(
		t,
		fixture.repoRoot,
		"-c", "user.name=Release Test",
		"-c", "user.email=release-test@example.invalid",
		"merge", "--no-ff", "release-side", "-m", "merge release parent",
	)
	runGitTest(t, fixture.repoRoot, "tag", fixture.tag)

	_, err := verifyPromotion(
		fixture.repoRoot,
		fixture.candidateRoot,
		fixture.repository,
		fixture.tag,
		fixture.sourceCommit,
		fixture.workflowRunID,
		fixture.candidateName,
	)
	if err == nil || !strings.Contains(err.Error(), "must have the candidate source as its only parent") {
		t.Fatalf("verifyPromotion() error = %v", err)
	}
}

func TestPromotionRejectsTrackedManifestDifferentFromCandidate(t *testing.T) {
	fixture := makePromotionFixture(t, false)
	runGitTest(t, fixture.repoRoot, "tag", "-d", fixture.tag)
	manifestPath := filepath.Join(fixture.repoRoot, "plugins", "delegation", candidateManifestName)
	manifest, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	manifest = append(manifest, "# reviewed manifest changed\n"...)
	if err := os.WriteFile(manifestPath, manifest, 0o644); err != nil {
		t.Fatal(err)
	}
	runGitTest(t, fixture.repoRoot, "add", manifestPath)
	runGitTest(
		t,
		fixture.repoRoot,
		"-c", "user.name=Release Test",
		"-c", "user.email=release-test@example.invalid",
		"commit", "--amend", "--no-edit",
	)
	runGitTest(t, fixture.repoRoot, "tag", fixture.tag)

	_, err = verifyPromotion(
		fixture.repoRoot,
		fixture.candidateRoot,
		fixture.repository,
		fixture.tag,
		fixture.sourceCommit,
		fixture.workflowRunID,
		fixture.candidateName,
	)
	if err == nil || !strings.Contains(err.Error(), "tracked checksum manifest does not match") {
		t.Fatalf("verifyPromotion() error = %v", err)
	}
}

func makePromotionFixture(t *testing.T, addSourceChange bool) promotionFixture {
	t.Helper()
	repoRoot := filepath.Join(t.TempDir(), "repo")
	if err := os.MkdirAll(filepath.Join(repoRoot, "plugins", "delegation"), 0o755); err != nil {
		t.Fatal(err)
	}
	version := "1.2.3-rc.1"
	if err := os.WriteFile(filepath.Join(repoRoot, "plugins", "delegation", "VERSION"), []byte(version+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(repoRoot, "plugins", "delegation", candidateManifestName)
	if err := os.WriteFile(manifestPath, []byte("# candidate checksum is committed in the next change\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repoRoot, "README.md"), []byte("release fixture\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(repoRoot, releaseNoticeName),
		testReleaseNotice(t),
		0o644,
	); err != nil {
		t.Fatal(err)
	}
	runGitTest(t, repoRoot, "init", "-b", "main")
	runGitTest(t, repoRoot, "add", ".")
	runGitTest(t, repoRoot, "-c", "user.name=Release Test", "-c", "user.email=release-test@example.invalid", "commit", "-m", "candidate source")
	sourceCommit := runGitTest(t, repoRoot, "rev-parse", "HEAD")
	candidate := makeCandidateParts(t, version, sourceCommit)
	if err := assembleCandidate(
		candidate.input,
		candidate.candidate,
		candidate.repository,
		candidate.version,
		candidate.sourceCommit,
		candidate.workflowRunID,
		candidate.candidateName,
		testReleaseNotice(t),
	); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(candidate.candidate, candidateProvenanceName), []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	manifest, err := os.ReadFile(filepath.Join(candidate.candidate, candidateManifestName))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifestPath, manifest, 0o644); err != nil {
		t.Fatal(err)
	}
	if addSourceChange {
		if err := os.WriteFile(filepath.Join(repoRoot, "README.md"), []byte("unexpected release change\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	runGitTest(t, repoRoot, "add", ".")
	runGitTest(t, repoRoot, "-c", "user.name=Release Test", "-c", "user.email=release-test@example.invalid", "commit", "-m", "release manifest")
	tag := "v" + version
	runGitTest(t, repoRoot, "tag", tag)
	return promotionFixture{
		repoRoot:       repoRoot,
		candidateRoot:  candidate.candidate,
		repository:     candidate.repository,
		tag:            tag,
		sourceCommit:   sourceCommit,
		workflowRunID:  candidate.workflowRunID,
		candidateName:  candidate.candidateName,
		artifactID:     "654321",
		artifactDigest: strings.Repeat("f", 64),
	}
}

func runGitTest(t *testing.T, root string, args ...string) string {
	t.Helper()
	command := exec.Command("git", append([]string{"-C", root}, args...)...)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v: %s", strings.Join(args, " "), err, output)
	}
	return strings.TrimSpace(string(output))
}

func TestPromotionPredicateRejectsUnknownFields(t *testing.T) {
	data, err := canonicalJSON(promotionPredicate{})
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatal(err)
	}
	raw["unexpected"] = true
	data, err = json.MarshalIndent(raw, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	data = append(data, '\n')
	var predicate promotionPredicate
	if err := decodeCanonicalJSON(data, &predicate); err == nil {
		t.Fatal("decodeCanonicalJSON() accepted an unknown predicate field")
	}
}
