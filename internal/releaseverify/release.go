// Package releaseverify verifies Delegation runtime releases before they are
// eligible for an upgrade. It deliberately does not download assets or switch
// services; callers provide a complete release snapshot and the downloaded
// bytes for the trust-bearing assets.
package releaseverify

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"runtime"
	"strings"

	"golang.org/x/mod/semver"
)

const (
	CanonicalRepository     = "GhostFlying/delegation"
	CanonicalWorkflow       = ".github/workflows/release.yml"
	CanonicalFulcioIssuer   = "https://token.actions.githubusercontent.com"
	ReleasePredicateType    = "https://github.com/GhostFlying/delegation/attestations/release-manifest/v1"
	ReleaseManifestName     = "release-artifacts.sha256"
	ReleaseProvenanceName   = "release-provenance.sigstore.json"
	maxManifestSize         = 16 << 10
	maxProvenanceBundleSize = 1 << 20
	maxRuntimeArtifactSize  = 256 << 20
)

var supportedPlatforms = []Platform{
	{OS: "darwin", Architecture: "amd64"},
	{OS: "darwin", Architecture: "arm64"},
	{OS: "linux", Architecture: "amd64"},
	{OS: "linux", Architecture: "arm64"},
	{OS: "windows", Architecture: "amd64"},
	{OS: "windows", Architecture: "arm64"},
}

var versionPattern = regexp.MustCompile(`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:-(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)(?:\.(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*))*)?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$`)

type Platform struct {
	OS           string
	Architecture string
}

func CurrentPlatform() Platform {
	return Platform{OS: runtime.GOOS, Architecture: runtime.GOARCH}
}

type Asset struct {
	ID      int64
	Name    string
	Size    int64
	Digest  string
	Content []byte
}

type Release struct {
	ID         int64
	Repository string
	TagName    string
	TagCommit  string
	Draft      bool
	Prerelease bool
	Immutable  bool
	Assets     []Asset
}

type Request struct {
	CurrentVersion string
	TargetVersion  string
	TagCommit      string
	Release        Release
}

type Result struct {
	ReleaseID      int64
	Version        string
	Tag            string
	TagCommit      string
	Artifact       Asset
	ArtifactSHA256 string
	ManifestSHA256 string
}

type provenanceVerifier interface {
	Verify(bundle, subject []byte, identity Identity) error
}

type Identity struct {
	Repository string
	Workflow   string
	Issuer     string
	Tag        string
	TagCommit  string
}

type Verifier struct {
	provenance provenanceVerifier
}

func newVerifier(provenance provenanceVerifier) *Verifier {
	return &Verifier{provenance: provenance}
}

func (v *Verifier) Verify(request Request) (Result, error) {
	if v == nil || v.provenance == nil {
		return Result{}, errors.New("release provenance verifier is not configured")
	}
	if request.Release.Repository != CanonicalRepository {
		return Result{}, fmt.Errorf("release repository %q is not canonical", request.Release.Repository)
	}
	if request.Release.ID <= 0 {
		return Result{}, errors.New("GitHub release ID must be positive")
	}
	if err := requireForwardVersion(request.CurrentVersion, request.TargetVersion); err != nil {
		return Result{}, err
	}
	if semver.Prerelease("v"+request.TargetVersion) == "" {
		return Result{}, errors.New("target version must be a semantic-version prerelease")
	}
	tag := "v" + request.TargetVersion
	if request.Release.TagName != tag {
		return Result{}, fmt.Errorf("release tag %q does not match target %q", request.Release.TagName, tag)
	}
	if !isCommit(request.TagCommit) || request.Release.TagCommit != request.TagCommit {
		return Result{}, errors.New("release tag does not resolve to the expected commit")
	}
	if request.Release.Draft || !request.Release.Prerelease || !request.Release.Immutable {
		return Result{}, errors.New("release must be a published immutable prerelease")
	}

	expectedNames, err := expectedAssetNames(request.TargetVersion)
	if err != nil {
		return Result{}, err
	}
	assets, err := indexAssets(request.Release.Assets, expectedNames)
	if err != nil {
		return Result{}, err
	}
	manifest := assets[ReleaseManifestName]
	if err := validateAssetContent(manifest, maxManifestSize); err != nil {
		return Result{}, fmt.Errorf("verify release manifest asset: %w", err)
	}
	manifestDigests, err := parseManifest(manifest.Content, request.TargetVersion)
	if err != nil {
		return Result{}, err
	}
	for name, digest := range manifestDigests {
		if assets[name].Digest != "sha256:"+digest {
			return Result{}, fmt.Errorf("GitHub digest for %s does not match the release manifest", name)
		}
	}

	artifactName, err := artifactName(request.TargetVersion, CurrentPlatform())
	if err != nil {
		return Result{}, err
	}
	artifact := assets[artifactName]
	if err := validateAssetContent(artifact, maxRuntimeArtifactSize); err != nil {
		return Result{}, fmt.Errorf("verify current platform artifact: %w", err)
	}
	artifactDigest := sha256Hex(artifact.Content)
	if artifactDigest != manifestDigests[artifactName] {
		return Result{}, errors.New("current platform artifact digest does not match the release manifest")
	}

	provenance := assets[ReleaseProvenanceName]
	if err := validateAssetContent(provenance, maxProvenanceBundleSize); err != nil {
		return Result{}, fmt.Errorf("verify release provenance asset: %w", err)
	}
	identity := Identity{
		Repository: request.Release.Repository,
		Workflow:   CanonicalWorkflow,
		Issuer:     CanonicalFulcioIssuer,
		Tag:        tag,
		TagCommit:  request.TagCommit,
	}
	if err := v.provenance.Verify(provenance.Content, manifest.Content, identity); err != nil {
		return Result{}, fmt.Errorf("verify release manifest provenance: %w", err)
	}

	return Result{
		ReleaseID:      request.Release.ID,
		Version:        request.TargetVersion,
		Tag:            tag,
		TagCommit:      request.TagCommit,
		Artifact:       artifact,
		ArtifactSHA256: artifactDigest,
		ManifestSHA256: sha256Hex(manifest.Content),
	}, nil
}

func requireForwardVersion(current, target string) error {
	if !validVersion(current) || !validVersion(target) {
		return fmt.Errorf("current and target versions must be strict semantic versions: %q -> %q", current, target)
	}
	if semver.Compare("v"+target, "v"+current) <= 0 {
		return fmt.Errorf("target version %s is not strictly newer than %s", target, current)
	}
	return nil
}

func validVersion(value string) bool {
	return versionPattern.MatchString(value) && semver.IsValid("v"+value)
}

func expectedAssetNames(version string) (map[string]struct{}, error) {
	if !validVersion(version) {
		return nil, fmt.Errorf("invalid target version %q", version)
	}
	names := map[string]struct{}{
		ReleaseManifestName:   {},
		ReleaseProvenanceName: {},
	}
	for _, platform := range supportedPlatforms {
		name, _ := artifactName(version, platform)
		names[name] = struct{}{}
	}
	return names, nil
}

func indexAssets(assets []Asset, expected map[string]struct{}) (map[string]Asset, error) {
	if len(assets) != len(expected) {
		return nil, fmt.Errorf("release has %d assets, expected %d", len(assets), len(expected))
	}
	indexed := make(map[string]Asset, len(assets))
	ids := make(map[int64]struct{}, len(assets))
	for _, asset := range assets {
		if _, ok := expected[asset.Name]; !ok {
			return nil, fmt.Errorf("unexpected release asset %q", asset.Name)
		}
		if _, duplicate := indexed[asset.Name]; duplicate {
			return nil, fmt.Errorf("duplicate release asset %q", asset.Name)
		}
		if asset.ID <= 0 {
			return nil, fmt.Errorf("release asset %q has an invalid GitHub asset ID", asset.Name)
		}
		if _, duplicate := ids[asset.ID]; duplicate {
			return nil, fmt.Errorf("duplicate GitHub asset ID %d", asset.ID)
		}
		digest, ok := strings.CutPrefix(asset.Digest, "sha256:")
		if asset.Size <= 0 || !ok || len(digest) != sha256.Size*2 || digest != strings.ToLower(digest) {
			return nil, fmt.Errorf("release asset %q has invalid metadata", asset.Name)
		}
		if _, err := hex.DecodeString(digest); err != nil {
			return nil, fmt.Errorf("release asset %q has invalid SHA-256 digest", asset.Name)
		}
		indexed[asset.Name] = asset
		ids[asset.ID] = struct{}{}
	}
	return indexed, nil
}

func validateAssetContent(asset Asset, maximum int64) error {
	if len(asset.Content) == 0 || int64(len(asset.Content)) != asset.Size {
		return errors.New("downloaded content does not match the GitHub asset size")
	}
	if asset.Size > maximum {
		return fmt.Errorf("asset exceeds the %d-byte limit", maximum)
	}
	if "sha256:"+sha256Hex(asset.Content) != asset.Digest {
		return errors.New("downloaded content does not match the GitHub asset digest")
	}
	return nil
}

func parseManifest(data []byte, version string) (map[string]string, error) {
	if len(data) == 0 || len(data) > maxManifestSize || data[len(data)-1] != '\n' || strings.Contains(string(data), "\r") {
		return nil, errors.New("release manifest is empty, oversized, or non-canonical")
	}
	digests := make(map[string]string, len(supportedPlatforms))
	lines := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
	if len(lines) != len(supportedPlatforms) {
		return nil, fmt.Errorf("release manifest has %d entries, expected %d", len(lines), len(supportedPlatforms))
	}
	previous := ""
	for _, line := range lines {
		if len(line) < sha256.Size*2+2 || line[sha256.Size*2:sha256.Size*2+2] != "  " {
			return nil, errors.New("release manifest has an invalid line")
		}
		digest, name := line[:sha256.Size*2], line[sha256.Size*2+2:]
		decoded, err := hex.DecodeString(digest)
		if err != nil || len(decoded) != sha256.Size || digest != strings.ToLower(digest) {
			return nil, fmt.Errorf("release manifest has an invalid digest for %q", name)
		}
		if previous != "" && name <= previous {
			return nil, errors.New("release manifest entries are duplicated or not sorted")
		}
		digests[name] = digest
		previous = name
	}
	expected, _ := expectedAssetNames(version)
	delete(expected, ReleaseManifestName)
	delete(expected, ReleaseProvenanceName)
	for name := range expected {
		if _, ok := digests[name]; !ok {
			return nil, fmt.Errorf("release manifest does not contain %s", name)
		}
	}
	return digests, nil
}

func artifactName(version string, platform Platform) (string, error) {
	archive := "tar.gz"
	if platform.OS == "windows" {
		archive = "zip"
	}
	for _, supported := range supportedPlatforms {
		if platform == supported {
			return fmt.Sprintf("delegation_%s_%s_%s.%s", version, platform.OS, platform.Architecture, archive), nil
		}
	}
	return "", fmt.Errorf("unsupported release platform %s-%s", platform.OS, platform.Architecture)
}

func isCommit(value string) bool {
	if len(value) != 40 || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func sha256Hex(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}
