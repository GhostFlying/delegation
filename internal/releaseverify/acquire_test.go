package releaseverify

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
)

type fixtureReleaseSource struct {
	release   Release
	commit    string
	contents  map[int64][]byte
	downloads []int64
}

func (s *fixtureReleaseSource) Release(context.Context, string) (Release, error) {
	return s.release, nil
}

func (s *fixtureReleaseSource) ResolveTagCommit(context.Context, string) (string, error) {
	return s.commit, nil
}

func (s *fixtureReleaseSource) DownloadAsset(_ context.Context, id, maximum int64) ([]byte, error) {
	s.downloads = append(s.downloads, id)
	content, ok := s.contents[id]
	if !ok {
		return nil, errors.New("missing fixture asset")
	}
	if int64(len(content)) > maximum {
		return nil, errors.New("fixture exceeded bound")
	}
	return append([]byte(nil), content...), nil
}

func TestAcquireCanonicalReleaseDownloadsOnlyTrustBearingPlatformAssets(t *testing.T) {
	request := validRequest(t)
	source := fixtureSourceForRequest(request)
	result, err := AcquireCanonicalRelease(context.Background(), request.CurrentVersion, request.TargetVersion, AcquisitionDependencies{
		Source: source, Verifier: newVerifier(&recordingProvenanceVerifier{}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Version != request.TargetVersion || len(source.downloads) != 3 {
		t.Fatalf("result = %#v, downloads = %v", result, source.downloads)
	}
}

func TestAcquireCanonicalReleaseFailsClosedBeforeVerification(t *testing.T) {
	request := validRequest(t)
	source := fixtureSourceForRequest(request)
	currentArtifact, err := artifactName(request.TargetVersion, CurrentPlatform())
	if err != nil {
		t.Fatal(err)
	}
	for index := range source.release.Assets {
		if source.release.Assets[index].Name == currentArtifact {
			source.release.Assets[index].Size = maxRuntimeArtifactSize + 1
		}
	}
	_, err = AcquireCanonicalRelease(context.Background(), request.CurrentVersion, request.TargetVersion, AcquisitionDependencies{
		Source: source, Verifier: newVerifier(&recordingProvenanceVerifier{}),
	})
	if err == nil || !strings.Contains(err.Error(), "size contract") || len(source.downloads) != 0 {
		t.Fatalf("AcquireCanonicalRelease() error = %v, downloads = %v", err, source.downloads)
	}
}

func fixtureSourceForRequest(request Request) *fixtureReleaseSource {
	source := &fixtureReleaseSource{
		release: request.Release, commit: request.TagCommit, contents: map[int64][]byte{},
	}
	for index := range source.release.Assets {
		asset := &source.release.Assets[index]
		if len(asset.Content) != 0 {
			source.contents[asset.ID] = append([]byte(nil), asset.Content...)
			asset.Content = nil
		}
	}
	return source
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestGitHubSourceUsesOnlyCanonicalEndpointsAndBoundsResponses(t *testing.T) {
	requests := 0
	client := &http.Client{Transport: roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		requests++
		if request.URL.Scheme != "https" || request.URL.Host != "api.github.com" ||
			!strings.HasPrefix(request.URL.Path, "/repos/GhostFlying/delegation/") {
			return nil, fmt.Errorf("non-canonical request: %s", request.URL)
		}
		body := "{\"id\":42,\"tag_name\":\"v0.1.0-alpha.8\",\"draft\":false,\"prerelease\":true,\"immutable\":true,\"assets\":[]}"
		return &http.Response{
			StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body)), Header: http.Header{},
		}, nil
	})}
	release, err := NewGitHubSource(client).Release(context.Background(), "v0.1.0-alpha.8")
	if err != nil || release.Repository != CanonicalRepository || requests != 1 {
		t.Fatalf("Release() = %#v, %v; requests = %d", release, err, requests)
	}
	oversized := &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(strings.Repeat(" ", maximumReleaseJSONSize+1))),
			Header: http.Header{},
		}, nil
	})}
	if _, err := NewGitHubSource(oversized).Release(context.Background(), "v0.1.0-alpha.8"); err == nil || !strings.Contains(err.Error(), "size limit") {
		t.Fatalf("oversized Release() error = %v", err)
	}
}

func TestCanonicalRedirectRejectsUntrustedHost(t *testing.T) {
	request, err := http.NewRequest(http.MethodGet, "https://attacker.invalid/archive", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := canonicalRedirect(request, []*http.Request{{}}); err == nil {
		t.Fatal("canonicalRedirect accepted an untrusted host")
	}
}
