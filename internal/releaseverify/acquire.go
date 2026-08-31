package releaseverify

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/sigstore/sigstore-go/pkg/root"
)

const (
	canonicalGitHubAPI     = "https://api.github.com"
	maximumReleaseJSONSize = 256 << 10
)

// ReleaseSource supplies metadata and bytes from the canonical GitHub
// repository. The interface keeps acquisition tests offline; production code
// uses GitHubSource, whose repository and endpoints are not caller-selectable.
type ReleaseSource interface {
	Release(context.Context, string) (Release, error)
	ResolveTagCommit(context.Context, string) (string, error)
	DownloadAsset(context.Context, int64, int64) ([]byte, error)
}

type TrustedRootFetcher func() (root.TrustedMaterial, error)

// AcquisitionDependencies are injectable trust and transport dependencies.
// A zero value uses the canonical unauthenticated GitHub API and Sigstore TUF
// trusted root.
type AcquisitionDependencies struct {
	Source           ReleaseSource
	Verifier         *Verifier
	FetchTrustedRoot TrustedRootFetcher
}

// AcquireCanonicalRelease downloads and verifies exactly the trust-bearing
// assets required for the current platform. Metadata for every release asset
// is still checked so an extra, missing, duplicated, or renamed asset fails
// closed.
func AcquireCanonicalRelease(
	ctx context.Context, currentVersion, targetVersion string, dependencies AcquisitionDependencies,
) (Result, error) {
	if err := requireForwardVersion(currentVersion, targetVersion); err != nil {
		return Result{}, err
	}
	source := dependencies.Source
	if source == nil {
		source = NewGitHubSource(nil)
	}
	verifier := dependencies.Verifier
	if verifier == nil {
		fetch := dependencies.FetchTrustedRoot
		if fetch == nil {
			fetch = func() (root.TrustedMaterial, error) {
				return root.FetchTrustedRoot()
			}
		}
		trustedMaterial, err := fetch()
		if err != nil {
			return Result{}, fmt.Errorf("fetch Sigstore trusted root: %w", err)
		}
		verifier, err = NewVerifier(trustedMaterial)
		if err != nil {
			return Result{}, fmt.Errorf("create release verifier: %w", err)
		}
	}
	tag := "v" + targetVersion
	release, err := source.Release(ctx, tag)
	if err != nil {
		return Result{}, fmt.Errorf("fetch canonical GitHub release: %w", err)
	}
	commit, err := source.ResolveTagCommit(ctx, tag)
	if err != nil {
		return Result{}, fmt.Errorf("resolve canonical release tag: %w", err)
	}
	release.TagCommit = commit
	artifact, err := artifactName(targetVersion, CurrentPlatform())
	if err != nil {
		return Result{}, err
	}
	required := map[string]bool{
		artifact:              true,
		ReleaseManifestName:   true,
		ReleaseProvenanceName: true,
	}
	for index := range release.Assets {
		asset := &release.Assets[index]
		limit, limitErr := assetLimit(asset.Name, targetVersion)
		if limitErr != nil {
			return Result{}, limitErr
		}
		if asset.Size <= 0 || asset.Size > limit {
			return Result{}, fmt.Errorf("release asset %q exceeds its size contract", asset.Name)
		}
		if !required[asset.Name] {
			continue
		}
		content, downloadErr := source.DownloadAsset(ctx, asset.ID, limit)
		if downloadErr != nil {
			return Result{}, fmt.Errorf("download canonical release asset %s: %w", asset.Name, downloadErr)
		}
		asset.Content = content
		delete(required, asset.Name)
	}
	if len(required) != 0 {
		return Result{}, errors.New("canonical release is missing a required current-platform asset")
	}
	return verifier.Verify(Request{
		CurrentVersion: currentVersion, TargetVersion: targetVersion, TagCommit: commit, Release: release,
	})
}

func assetLimit(name, version string) (int64, error) {
	switch name {
	case ReleaseManifestName:
		return maxManifestSize, nil
	case ReleaseProvenanceName:
		return maxProvenanceBundleSize, nil
	}
	expected, err := expectedAssetNames(version)
	if err != nil {
		return 0, err
	}
	if _, ok := expected[name]; !ok {
		return 0, fmt.Errorf("unexpected release asset %q", name)
	}
	return maxRuntimeArtifactSize, nil
}

// GitHubSource reads the fixed public GhostFlying/delegation release API. It
// never consumes a release URL or repository supplied by a broker.
type GitHubSource struct {
	client *http.Client
}

func NewGitHubSource(client *http.Client) *GitHubSource {
	if client == nil {
		client = &http.Client{}
	}
	copy := *client
	if copy.Timeout == 0 {
		copy.Timeout = 2 * time.Minute
	}
	copy.CheckRedirect = canonicalRedirect
	return &GitHubSource{client: &copy}
}

func (s *GitHubSource) Release(ctx context.Context, tag string) (Release, error) {
	if !validReleaseTag(tag) {
		return Release{}, errors.New("canonical release tag is invalid")
	}
	var response struct {
		ID         int64  `json:"id"`
		TagName    string `json:"tag_name"`
		Draft      bool   `json:"draft"`
		Prerelease bool   `json:"prerelease"`
		Immutable  bool   `json:"immutable"`
		Assets     []struct {
			ID     int64  `json:"id"`
			Name   string `json:"name"`
			Size   int64  `json:"size"`
			Digest string `json:"digest"`
		} `json:"assets"`
	}
	endpoint := canonicalGitHubAPI + "/repos/" + CanonicalRepository + "/releases/tags/" + url.PathEscape(tag)
	if err := s.getJSON(ctx, endpoint, &response); err != nil {
		return Release{}, err
	}
	if len(response.Assets) > 32 {
		return Release{}, errors.New("canonical release has too many assets")
	}
	release := Release{
		ID: response.ID, Repository: CanonicalRepository, TagName: response.TagName,
		Draft: response.Draft, Prerelease: response.Prerelease, Immutable: response.Immutable,
		Assets: make([]Asset, 0, len(response.Assets)),
	}
	for _, asset := range response.Assets {
		release.Assets = append(release.Assets, Asset{
			ID: asset.ID, Name: asset.Name, Size: asset.Size, Digest: asset.Digest,
		})
	}
	return release, nil
}

func (s *GitHubSource) ResolveTagCommit(ctx context.Context, tag string) (string, error) {
	if !validReleaseTag(tag) {
		return "", errors.New("canonical release tag is invalid")
	}
	var response struct {
		SHA string `json:"sha"`
	}
	endpoint := canonicalGitHubAPI + "/repos/" + CanonicalRepository + "/commits/" + url.PathEscape(tag)
	if err := s.getJSON(ctx, endpoint, &response); err != nil {
		return "", err
	}
	if !isCommit(response.SHA) {
		return "", errors.New("canonical release tag did not resolve to a commit")
	}
	return response.SHA, nil
}

func (s *GitHubSource) DownloadAsset(ctx context.Context, assetID, maximum int64) ([]byte, error) {
	if assetID <= 0 || maximum <= 0 || maximum > maxRuntimeArtifactSize {
		return nil, errors.New("release asset download request is invalid")
	}
	endpoint := canonicalGitHubAPI + "/repos/" + CanonicalRepository + "/releases/assets/" + strconv.FormatInt(assetID, 10)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	setGitHubHeaders(request, "application/octet-stream")
	response, err := s.client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GitHub asset API returned HTTP %d", response.StatusCode)
	}
	content, err := io.ReadAll(io.LimitReader(response.Body, maximum+1))
	if err != nil {
		return nil, err
	}
	if int64(len(content)) > maximum {
		return nil, fmt.Errorf("GitHub asset exceeds the %d-byte limit", maximum)
	}
	return content, nil
}

func (s *GitHubSource) getJSON(ctx context.Context, endpoint string, destination any) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	setGitHubHeaders(request, "application/vnd.github+json")
	response, err := s.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("GitHub API returned HTTP %d", response.StatusCode)
	}
	content, err := io.ReadAll(io.LimitReader(response.Body, maximumReleaseJSONSize+1))
	if err != nil {
		return fmt.Errorf("read GitHub response: %w", err)
	}
	if len(content) > maximumReleaseJSONSize {
		return errors.New("GitHub response exceeds its size limit")
	}
	decoder := json.NewDecoder(strings.NewReader(string(content)))
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("decode GitHub response: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("GitHub response contains trailing data")
	}
	return nil
}

func setGitHubHeaders(request *http.Request, accept string) {
	request.Header.Set("Accept", accept)
	request.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	request.Header.Set("User-Agent", "delegation-release-upgrade")
}

func canonicalRedirect(request *http.Request, via []*http.Request) error {
	if len(via) >= 10 {
		return errors.New("too many GitHub release redirects")
	}
	host := strings.ToLower(request.URL.Hostname())
	if request.URL.Scheme != "https" || (host != "github.com" && host != "api.github.com" &&
		!strings.HasSuffix(host, ".githubusercontent.com")) {
		return errors.New("GitHub release redirect left the canonical host set")
	}
	return nil
}

func validReleaseTag(tag string) bool {
	version, ok := strings.CutPrefix(tag, "v")
	return ok && validVersion(version)
}
