package releaseverify

import (
	"fmt"
	"sort"
	"strings"
	"testing"
)

const testTagCommit = "0123456789abcdef0123456789abcdef01234567"

type recordingProvenanceVerifier struct {
	err      error
	bundle   []byte
	subject  []byte
	identity Identity
}

func (v *recordingProvenanceVerifier) Verify(bundle, subject []byte, identity Identity) error {
	v.bundle = append([]byte(nil), bundle...)
	v.subject = append([]byte(nil), subject...)
	v.identity = identity
	return v.err
}

func TestVerifierAcceptsCanonicalRelease(t *testing.T) {
	provenance := &recordingProvenanceVerifier{}
	request := validRequest(t)
	result, err := newVerifier(provenance).Verify(request)
	if err != nil {
		t.Fatal(err)
	}
	expectedArtifact, err := artifactName(request.TargetVersion, CurrentPlatform())
	if err != nil {
		t.Fatal(err)
	}
	if result.ReleaseID != request.Release.ID || result.Version != request.TargetVersion || result.Tag != request.Release.TagName ||
		result.TagCommit != testTagCommit || result.Artifact.Name != expectedArtifact {
		t.Fatalf("unexpected verification result: %+v", result)
	}
	if result.ArtifactSHA256 != sha256Hex(result.Artifact.Content) ||
		result.ManifestSHA256 != sha256Hex(provenance.subject) {
		t.Fatalf("verification result has incorrect digests: %+v", result)
	}
	if string(provenance.bundle) != "signed provenance" || provenance.identity != (Identity{
		Repository: CanonicalRepository,
		Workflow:   CanonicalWorkflow,
		Issuer:     CanonicalFulcioIssuer,
		Tag:        "v0.1.0-alpha.8",
		TagCommit:  testTagCommit,
	}) {
		t.Fatalf("unexpected provenance inputs: bundle=%q identity=%+v", provenance.bundle, provenance.identity)
	}
}

func TestVerifierRejectsUntrustedReleaseState(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Request)
		want   string
	}{
		{name: "repository", mutate: func(r *Request) { r.Release.Repository = "attacker/delegation" }, want: "not canonical"},
		{name: "release ID", mutate: func(r *Request) { r.Release.ID = 0 }, want: "release ID must be positive"},
		{name: "non-forward version", mutate: func(r *Request) { r.CurrentVersion = r.TargetVersion }, want: "not strictly newer"},
		{name: "non-canonical version", mutate: func(r *Request) { r.TargetVersion = "01.1.0" }, want: "strict semantic versions"},
		{name: "stable version", mutate: func(r *Request) { r.TargetVersion = "0.1.1" }, want: "must be a semantic-version prerelease"},
		{name: "tag", mutate: func(r *Request) { r.Release.TagName = "v0.1.0-alpha.9" }, want: "does not match target"},
		{name: "tag commit", mutate: func(r *Request) { r.Release.TagCommit = strings.Repeat("f", 40) }, want: "expected commit"},
		{name: "mutable", mutate: func(r *Request) { r.Release.Immutable = false }, want: "immutable prerelease"},
		{name: "extra asset", mutate: func(r *Request) {
			r.Release.Assets = append(r.Release.Assets, Asset{Name: "extra", Size: 1, Digest: "sha256:" + strings.Repeat("0", 64)})
		}, want: "expected 8"},
		{name: "asset ID", mutate: func(r *Request) { r.Release.Assets[0].ID = 0 }, want: "invalid GitHub asset ID"},
		{name: "duplicate asset ID", mutate: func(r *Request) { r.Release.Assets[1].ID = r.Release.Assets[0].ID }, want: "duplicate GitHub asset ID"},
		{name: "archive metadata", mutate: func(r *Request) { r.Release.Assets[0].Digest = "sha256:" + strings.Repeat("0", 64) }, want: "GitHub digest"},
		{name: "artifact bytes", mutate: func(r *Request) {
			name, err := artifactName(r.TargetVersion, CurrentPlatform())
			if err != nil {
				t.Fatal(err)
			}
			for index := range r.Release.Assets {
				if r.Release.Assets[index].Name == name {
					r.Release.Assets[index].Content = []byte("tampered")
					return
				}
			}
			t.Fatalf("current-platform asset %q is missing from fixture", name)
		}, want: "asset size"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := validRequest(t)
			test.mutate(&request)
			_, err := newVerifier(&recordingProvenanceVerifier{}).Verify(request)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Verify() error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestVerifierRejectsProvenanceFailure(t *testing.T) {
	want := "signature rejected"
	_, err := newVerifier(&recordingProvenanceVerifier{err: fmt.Errorf("%s", want)}).Verify(validRequest(t))
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("Verify() error = %v, want substring %q", err, want)
	}
}

func validRequest(t *testing.T) Request {
	t.Helper()
	version := "0.1.0-alpha.8"
	contents := map[string][]byte{}
	for _, platform := range supportedPlatforms {
		name, err := artifactName(version, platform)
		if err != nil {
			t.Fatal(err)
		}
		contents[name] = []byte("archive:" + name)
	}
	names := make([]string, 0, len(contents))
	for name := range contents {
		names = append(names, name)
	}
	sort.Strings(names)
	var manifest strings.Builder
	for _, name := range names {
		fmt.Fprintf(&manifest, "%s  %s\n", sha256Hex(contents[name]), name)
	}

	assets := make([]Asset, 0, 8)
	assetID := int64(100)
	for _, name := range names {
		content := contents[name]
		asset := Asset{ID: assetID, Name: name, Size: int64(len(content)), Digest: "sha256:" + sha256Hex(content)}
		assetID++
		currentArtifact, err := artifactName(version, CurrentPlatform())
		if err != nil {
			t.Fatal(err)
		}
		if name == currentArtifact {
			asset.Content = content
		}
		assets = append(assets, asset)
	}
	manifestBytes := []byte(manifest.String())
	assets = append(assets, Asset{
		ID: assetID, Name: ReleaseManifestName, Size: int64(len(manifestBytes)),
		Digest: "sha256:" + sha256Hex(manifestBytes), Content: manifestBytes,
	})
	assetID++
	provenance := []byte("signed provenance")
	assets = append(assets, Asset{
		ID: assetID, Name: ReleaseProvenanceName, Size: int64(len(provenance)),
		Digest: "sha256:" + sha256Hex(provenance), Content: provenance,
	})

	return Request{
		CurrentVersion: "0.1.0-alpha.7",
		TargetVersion:  version,
		TagCommit:      testTagCommit,
		Release: Release{
			ID: 42, Repository: CanonicalRepository, TagName: "v" + version, TagCommit: testTagCommit,
			Prerelease: true, Immutable: true, Assets: assets,
		},
	}
}
