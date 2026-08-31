package releaseverify

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"github.com/sigstore/sigstore-go/pkg/testing/data"
	sigstoreverify "github.com/sigstore/sigstore-go/pkg/verify"
)

const (
	fixtureDigest = "46d4e2f74c4877316640000a6fdf8a8b59f1e0847667973e9859f774dd31b8f1e0937813b777fb66a2ac67d50540fe34640966eee9fc2ccca387082b4c85cd3c"
	fixtureCommit = "f0b49a04e5a62250e0f60fb128004a73110fe311"
)

func TestSigstoreVerifierCryptographicallyVerifiesOfflineFixture(t *testing.T) {
	verifier := fixtureSigstoreVerifier(t)
	bundleJSON := fixtureBundleJSON(t)
	digest, err := hex.DecodeString(fixtureDigest)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := githubCertificateIdentity(
		"sigstore/sigstore-js",
		".github/workflows/release.yml",
		CanonicalFulcioIssuer,
		"refs/heads/main",
		fixtureCommit,
	)
	if err != nil {
		t.Fatal(err)
	}
	result, err := verifier.verifyBundleDigest(bundleJSON, "sha512", digest, identity)
	if err != nil {
		t.Fatal(err)
	}
	if result == nil || result.Statement == nil || result.Signature == nil || result.Signature.Certificate == nil {
		t.Fatalf("offline verification returned incomplete result: %+v", result)
	}
}

func TestSigstoreVerifierRejectsTamperingAndIdentityDrift(t *testing.T) {
	bundleJSON := fixtureBundleJSON(t)
	digest, err := hex.DecodeString(fixtureDigest)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name       string
		mutate     func([]byte) []byte
		repository string
		workflow   string
		ref        string
		commit     string
	}{
		{name: "bundle", mutate: tamperFixturePayload},
		{name: "subject digest", mutate: func(data []byte) []byte { return data }},
		{name: "repository", repository: "attacker/sigstore-js", mutate: func(data []byte) []byte { return data }},
		{name: "workflow", workflow: ".github/workflows/other.yml", mutate: func(data []byte) []byte { return data }},
		{name: "workflow ref", ref: "refs/tags/v2.0.0", mutate: func(data []byte) []byte { return data }},
		{name: "source commit", commit: strings.Repeat("a", 40), mutate: func(data []byte) []byte { return data }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			algorithm := "sha512"
			actualDigest := digest
			if test.name == "subject digest" {
				actualDigest = append([]byte(nil), digest...)
				actualDigest[0] ^= 1
			}
			repository := "sigstore/sigstore-js"
			if test.repository != "" {
				repository = test.repository
			}
			workflow := ".github/workflows/release.yml"
			if test.workflow != "" {
				workflow = test.workflow
			}
			workflowRef := "refs/heads/main"
			if test.ref != "" {
				workflowRef = test.ref
			}
			commit := fixtureCommit
			if test.commit != "" {
				commit = test.commit
			}
			identity, err := githubCertificateIdentity(repository, workflow, CanonicalFulcioIssuer, workflowRef, commit)
			if err != nil {
				t.Fatal(err)
			}
			_, err = fixtureSigstoreVerifier(t).verifyBundleDigest(test.mutate(append([]byte(nil), bundleJSON...)), algorithm, actualDigest, identity)
			if err == nil {
				t.Fatal("verification unexpectedly accepted tampered or mismatched input")
			}
		})
	}
}

func TestReleaseStatementBindsTagCommitAndCanonicalFields(t *testing.T) {
	manifest := []byte("canonical manifest\n")
	digest := sha256.Sum256(manifest)
	identity := Identity{
		Repository: CanonicalRepository, Workflow: CanonicalWorkflow, Issuer: CanonicalFulcioIssuer,
		Tag: "v0.1.0-alpha.8", TagCommit: testTagCommit,
	}
	result := releaseStatementResult(t, digest, identity)
	if err := verifyReleaseStatement(result, digest, identity); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name   string
		mutate func(*Identity)
	}{
		{name: "repository", mutate: func(i *Identity) { i.Repository = "attacker/delegation" }},
		{name: "workflow", mutate: func(i *Identity) { i.Workflow = ".github/workflows/other.yml" }},
		{name: "tag", mutate: func(i *Identity) { i.Tag = "v0.1.0-alpha.9" }},
		{name: "tag commit", mutate: func(i *Identity) { i.TagCommit = strings.Repeat("f", 40) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			changed := identity
			test.mutate(&changed)
			if err := verifyReleaseStatement(result, digest, changed); err == nil {
				t.Fatal("statement unexpectedly accepted mismatched identity")
			}
		})
	}
}

func TestReleaseCertificateIdentityPinsTagRefAndCommit(t *testing.T) {
	identity := Identity{
		Repository: CanonicalRepository, Workflow: CanonicalWorkflow, Issuer: CanonicalFulcioIssuer,
		Tag: "v0.1.0-alpha.8", TagCommit: testTagCommit,
	}
	certificateIdentity, err := releaseCertificateIdentity(identity)
	if err != nil {
		t.Fatal(err)
	}
	wantRef := "refs/tags/" + identity.Tag
	wantSigner := "https://github.com/" + identity.Repository + "/" + identity.Workflow + "@" + wantRef
	if certificateIdentity.SubjectAlternativeName.SubjectAlternativeName != wantSigner ||
		certificateIdentity.Issuer.Issuer != CanonicalFulcioIssuer ||
		certificateIdentity.GithubWorkflowRepository != CanonicalRepository ||
		certificateIdentity.GithubWorkflowRef != wantRef ||
		certificateIdentity.GithubWorkflowSHA != testTagCommit ||
		certificateIdentity.BuildSignerURI != wantSigner ||
		certificateIdentity.BuildSignerDigest != testTagCommit ||
		certificateIdentity.SourceRepositoryRef != wantRef ||
		certificateIdentity.SourceRepositoryDigest != testTagCommit ||
		certificateIdentity.BuildConfigURI != wantSigner ||
		certificateIdentity.BuildConfigDigest != testTagCommit {
		t.Fatalf("certificate identity does not pin tag ref and commit: %+v", certificateIdentity)
	}
}

func fixtureSigstoreVerifier(t *testing.T) *sigstoreProvenanceVerifier {
	t.Helper()
	verifier, err := newSigstoreProvenanceVerifier(data.TrustedRoot(t, "public-good.json"))
	if err != nil {
		t.Fatal(err)
	}
	return verifier
}

func releaseStatementResult(t *testing.T, digest [sha256.Size]byte, identity Identity) *sigstoreverify.VerificationResult {
	t.Helper()
	statement := map[string]any{
		"mediaType": "application/vnd.dev.sigstore.verificationresult+json;version=0.1",
		"statement": map[string]any{
			"_type":         inTotoStatementType,
			"predicateType": ReleasePredicateType,
			"subject": []any{map[string]any{
				"name":   ReleaseManifestName,
				"digest": map[string]string{"sha256": hex.EncodeToString(digest[:])},
			}},
			"predicate": map[string]any{
				"schemaVersion": 1,
				"repository":    identity.Repository,
				"workflow":      identity.Workflow,
				"tag":           identity.Tag,
				"tagCommit":     identity.TagCommit,
			},
		},
	}
	data, err := json.Marshal(statement)
	if err != nil {
		t.Fatal(err)
	}
	result := sigstoreverify.NewVerificationResult()
	if err := json.Unmarshal(data, result); err != nil {
		t.Fatal(err)
	}
	return result
}

func tamperFixturePayload(data []byte) []byte {
	var document map[string]any
	if err := json.Unmarshal(data, &document); err != nil {
		panic(err)
	}
	envelope := document["dsseEnvelope"].(map[string]any)
	payload := []byte(envelope["payload"].(string))
	if payload[0] == 'A' {
		payload[0] = 'B'
	} else {
		payload[0] = 'A'
	}
	envelope["payload"] = string(payload)
	result, err := json.Marshal(document)
	if err != nil {
		panic(err)
	}
	return result
}

func fixtureBundleJSON(t *testing.T) []byte {
	t.Helper()
	fixture := data.Bundle(t, "sigstore.js@2.0.0-provenance.sigstore.json")
	bundleJSON, err := fixture.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	return bundleJSON
}
