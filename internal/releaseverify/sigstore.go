package releaseverify

import (
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/sigstore/sigstore-go/pkg/bundle"
	"github.com/sigstore/sigstore-go/pkg/fulcio/certificate"
	"github.com/sigstore/sigstore-go/pkg/root"
	sigstoreverify "github.com/sigstore/sigstore-go/pkg/verify"
)

const (
	inTotoStatementType = "https://in-toto.io/Statement/v1"
)

type sigstoreProvenanceVerifier struct {
	verifier *sigstoreverify.Verifier
}

// NewVerifier constructs a release verifier using caller-provided, trusted
// Sigstore material. Callers can use root.FetchTrustedRoot to obtain the
// public-good trust root through Sigstore's TUF client, or provide a pinned
// root for offline verification.
func NewVerifier(trustedMaterial root.TrustedMaterial) (*Verifier, error) {
	provenance, err := newSigstoreProvenanceVerifier(trustedMaterial)
	if err != nil {
		return nil, err
	}
	return newVerifier(provenance), nil
}

func newSigstoreProvenanceVerifier(trustedMaterial root.TrustedMaterial) (*sigstoreProvenanceVerifier, error) {
	if trustedMaterial == nil {
		return nil, errors.New("Sigstore trusted material is required")
	}
	sigstoreVerifier, err := sigstoreverify.NewVerifier(
		trustedMaterial,
		sigstoreverify.WithSignedCertificateTimestamps(1),
		sigstoreverify.WithTransparencyLog(1),
		sigstoreverify.WithObserverTimestamps(1),
	)
	if err != nil {
		return nil, fmt.Errorf("create Sigstore verifier: %w", err)
	}
	return &sigstoreProvenanceVerifier{verifier: sigstoreVerifier}, nil
}

func (v *sigstoreProvenanceVerifier) Verify(bundleJSON, subject []byte, identity Identity) error {
	subjectDigest := sha256.Sum256(subject)
	certificateIdentity, err := releaseCertificateIdentity(identity)
	if err != nil {
		return err
	}
	result, err := v.verifyBundleDigest(bundleJSON, "sha256", subjectDigest[:], certificateIdentity)
	if err != nil {
		return err
	}
	if err := verifyReleaseStatement(result, subjectDigest, identity); err != nil {
		return fmt.Errorf("verify signed release predicate: %w", err)
	}
	return nil
}

func (v *sigstoreProvenanceVerifier) verifyBundleDigest(bundleJSON []byte, algorithm string, digest []byte, certificateIdentity sigstoreverify.CertificateIdentity) (*sigstoreverify.VerificationResult, error) {
	if v == nil || v.verifier == nil {
		return nil, errors.New("Sigstore verifier is not configured")
	}
	if len(bundleJSON) == 0 || len(bundleJSON) > maxProvenanceBundleSize {
		return nil, errors.New("Sigstore bundle is empty or oversized")
	}

	entity := &bundle.Bundle{}
	if err := entity.UnmarshalJSON(bundleJSON); err != nil {
		return nil, fmt.Errorf("parse Sigstore bundle: %w", err)
	}
	return v.verifyEntityDigest(entity, algorithm, digest, certificateIdentity)
}

func (v *sigstoreProvenanceVerifier) verifyEntityDigest(entity sigstoreverify.SignedEntity, algorithm string, digest []byte, certificateIdentity sigstoreverify.CertificateIdentity) (*sigstoreverify.VerificationResult, error) {
	result, err := v.verifier.Verify(
		entity,
		sigstoreverify.NewPolicy(
			sigstoreverify.WithArtifactDigest(algorithm, digest),
			sigstoreverify.WithCertificateIdentity(certificateIdentity),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("verify Sigstore bundle: %w", err)
	}
	return result, nil
}

func releaseCertificateIdentity(identity Identity) (sigstoreverify.CertificateIdentity, error) {
	if identity.Repository == "" || identity.Workflow == "" || identity.Issuer == "" ||
		identity.Tag == "" || !isCommit(identity.TagCommit) {
		return sigstoreverify.CertificateIdentity{}, errors.New("release certificate identity is incomplete")
	}
	workflowRef := "refs/tags/" + identity.Tag
	return githubCertificateIdentity(
		identity.Repository, identity.Workflow, identity.Issuer, workflowRef, identity.TagCommit,
	)
}

func githubCertificateIdentity(repository, workflow, issuer, workflowRef, sourceCommit string) (sigstoreverify.CertificateIdentity, error) {
	repositoryURI := "https://github.com/" + repository
	signerURI := repositoryURI + "/" + workflow + "@" + workflowRef
	sanMatcher, err := sigstoreverify.NewSANMatcher(signerURI, "")
	if err != nil {
		return sigstoreverify.CertificateIdentity{}, fmt.Errorf("create signer identity matcher: %w", err)
	}
	issuerMatcher, err := sigstoreverify.NewIssuerMatcher(issuer, "")
	if err != nil {
		return sigstoreverify.CertificateIdentity{}, fmt.Errorf("create issuer matcher: %w", err)
	}
	certificateIdentity, err := sigstoreverify.NewCertificateIdentity(
		sanMatcher,
		issuerMatcher,
		certificate.Extensions{
			GithubWorkflowSHA:        sourceCommit,
			GithubWorkflowRepository: repository,
			GithubWorkflowRef:        workflowRef,
			BuildSignerURI:           signerURI,
			BuildSignerDigest:        sourceCommit,
			RunnerEnvironment:        "github-hosted",
			SourceRepositoryURI:      repositoryURI,
			SourceRepositoryDigest:   sourceCommit,
			SourceRepositoryRef:      workflowRef,
			BuildConfigURI:           signerURI,
			BuildConfigDigest:        sourceCommit,
		},
	)
	if err != nil {
		return sigstoreverify.CertificateIdentity{}, fmt.Errorf("create release certificate identity: %w", err)
	}
	return certificateIdentity, nil
}

func verifyReleaseStatement(result *sigstoreverify.VerificationResult, subjectDigest [sha256.Size]byte, identity Identity) error {
	if result == nil || result.Statement == nil {
		return errors.New("attestation does not contain an in-toto statement")
	}
	statement := result.Statement
	if err := statement.Validate(); err != nil {
		return fmt.Errorf("invalid in-toto statement: %w", err)
	}
	if statement.GetType() != inTotoStatementType {
		return fmt.Errorf("unexpected statement type %q", statement.GetType())
	}
	if statement.GetPredicateType() != ReleasePredicateType {
		return fmt.Errorf("unexpected predicate type %q", statement.GetPredicateType())
	}
	if len(statement.GetSubject()) != 1 {
		return fmt.Errorf("attestation has %d subjects, expected 1", len(statement.GetSubject()))
	}
	subject := statement.GetSubject()[0]
	digests := subject.GetDigest()
	if subject.GetName() != ReleaseManifestName || subject.GetUri() != "" || len(subject.GetContent()) != 0 ||
		subject.GetDownloadLocation() != "" || subject.GetMediaType() != "" || subject.GetAnnotations() != nil ||
		len(digests) != 1 || digests["sha256"] != fmt.Sprintf("%x", subjectDigest) {
		return errors.New("attestation subject is not the exact release manifest")
	}

	predicate := statement.GetPredicate()
	if predicate == nil || len(predicate.GetFields()) != 5 {
		return errors.New("release predicate does not have the canonical fields")
	}
	if value, ok := predicate.GetFields()["schemaVersion"]; !ok || value.GetNumberValue() != 1 {
		return errors.New("release predicate has an invalid schema version")
	}
	expected := map[string]string{
		"repository": identity.Repository,
		"workflow":   identity.Workflow,
		"tag":        identity.Tag,
		"tagCommit":  identity.TagCommit,
	}
	for field, want := range expected {
		value, ok := predicate.GetFields()[field]
		if !ok || value.GetStringValue() != want {
			return fmt.Errorf("release predicate %s does not match %q", field, want)
		}
	}
	return nil
}
