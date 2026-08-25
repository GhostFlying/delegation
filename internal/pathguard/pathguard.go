package pathguard

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type namedPath struct {
	name string
	path string
}

var platformPathAliasTarget = func(string, os.FileInfo) (string, bool, error) {
	return "", false, nil
}

var platformCanonicalExistingPath = func(path string) (string, error) {
	return filepath.Clean(path), nil
}

// ValidatePeerAuthority rejects aliases between the configuration, token, and
// reservation database files that make up a peer authority.
func ValidatePeerAuthority(configPath, statePath, tokenPath string) error {
	files := peerAuthorityFiles(configPath, statePath, tokenPath)
	for firstIndex, first := range files {
		for _, second := range files[firstIndex+1:] {
			conflicts, err := equivalent(first.path, second.path)
			if err != nil {
				return err
			}
			if conflicts {
				return fmt.Errorf("%s path conflicts with %s", second.name, first.name)
			}
		}
	}
	for _, file := range files {
		if err := requireSingleLink(file); err != nil {
			return err
		}
	}
	return nil
}

// ValidatePeerRuntimeAuthority extends the file authority check to managed
// directories whose aliases or containment would expose peer authority or
// merge independent runtime namespaces.
func ValidatePeerRuntimeAuthority(
	configPath, statePath, tokenPath, codexHome, workspaceRoot string,
) error {
	if err := ValidatePeerAuthority(configPath, statePath, tokenPath); err != nil {
		return err
	}
	files := peerAuthorityFiles(configPath, statePath, tokenPath)
	directories := []namedPath{
		{name: "worker CODEX_HOME", path: codexHome},
		{name: "worker workspace root", path: workspaceRoot},
	}
	for _, directory := range directories {
		for _, file := range files {
			conflicts, err := pathWithin(file.path, directory.path)
			if err != nil {
				return err
			}
			if conflicts {
				return fmt.Errorf("%s must not be inside %s", file.name, directory.name)
			}
		}
	}
	workspaceInsideHome, err := pathWithin(workspaceRoot, codexHome)
	if err != nil {
		return err
	}
	homeInsideWorkspace, err := pathWithin(codexHome, workspaceRoot)
	if err != nil {
		return err
	}
	if workspaceInsideHome || homeInsideWorkspace {
		return errors.New("worker workspace root and worker CODEX_HOME must not contain one another")
	}
	return nil
}

// ValidatePeerTailscaleAuthority extends the peer runtime authority check to
// the embedded Tailscale state directory and enrollment key.
func ValidatePeerTailscaleAuthority(
	configPath, statePath, tokenPath, codexHome, workspaceRoot,
	tailscaleStateDir, tailscaleAuthKeyPath string,
) error {
	if err := ValidatePeerRuntimeAuthority(
		configPath,
		statePath,
		tokenPath,
		codexHome,
		workspaceRoot,
	); err != nil {
		return err
	}
	if err := validateTailscaleAuthority(
		peerAuthorityFiles(configPath, statePath, tokenPath),
		tailscaleStateDir,
		tailscaleAuthKeyPath,
	); err != nil {
		return err
	}
	tailscaleState := namedPath{name: "Tailscale state directory", path: tailscaleStateDir}
	tailscaleLease := tailscaleStateLease(tailscaleStateDir)
	tailscaleAuthKey := namedPath{name: "Tailscale enrollment key", path: tailscaleAuthKeyPath}
	for _, directory := range []namedPath{
		{name: "worker CODEX_HOME", path: codexHome},
		{name: "worker workspace root", path: workspaceRoot},
	} {
		if err := rejectMutualContainment(tailscaleState, directory); err != nil {
			return err
		}
		if err := rejectMutualContainment(tailscaleLease, directory); err != nil {
			return err
		}
		contained, err := pathWithin(tailscaleAuthKey.path, directory.path)
		if err != nil {
			return err
		}
		if contained {
			return fmt.Errorf("%s must not be inside %s", tailscaleAuthKey.name, directory.name)
		}
	}
	return nil
}

// ValidateManagedExecutable rejects an executable that managed Codex can
// overwrite through either of its writable runtime directories.
func ValidateManagedExecutable(name, executablePath, codexHome, workspaceRoot string) error {
	if strings.TrimSpace(name) == "" {
		return errors.New("managed executable name is required")
	}
	for _, directory := range []namedPath{
		{name: "worker CODEX_HOME", path: codexHome},
		{name: "worker workspace root", path: workspaceRoot},
	} {
		contained, err := pathWithin(executablePath, directory.path)
		if err != nil {
			return err
		}
		if contained {
			return fmt.Errorf("%s must not be inside %s", name, directory.name)
		}
	}
	return nil
}

// ValidatePeerServiceEnvironment prevents provider credentials from aliasing
// peer authority files or living inside a directory exposed to managed Codex.
func ValidatePeerServiceEnvironment(
	environmentPath, configPath, statePath, tokenPath, codexHome, workspaceRoot string,
) error {
	if !filepath.IsAbs(environmentPath) {
		return errors.New("peer service environment path must be absolute")
	}
	if err := ValidatePeerRuntimeAuthority(
		configPath,
		statePath,
		tokenPath,
		codexHome,
		workspaceRoot,
	); err != nil {
		return err
	}
	return validatePeerServiceEnvironment(
		environmentPath,
		peerAuthorityFiles(configPath, statePath, tokenPath),
		codexHome,
		workspaceRoot,
		nil,
	)
}

// ValidatePeerTailscaleServiceEnvironment extends peer service environment
// authority validation to embedded Tailscale state, lease, and enrollment key
// paths.
func ValidatePeerTailscaleServiceEnvironment(
	environmentPath, configPath, statePath, tokenPath, codexHome, workspaceRoot,
	tailscaleStateDir, tailscaleAuthKeyPath string,
) error {
	if !filepath.IsAbs(environmentPath) {
		return errors.New("peer service environment path must be absolute")
	}
	if err := ValidatePeerTailscaleAuthority(
		configPath,
		statePath,
		tokenPath,
		codexHome,
		workspaceRoot,
		tailscaleStateDir,
		tailscaleAuthKeyPath,
	); err != nil {
		return err
	}
	authorities := append(
		peerAuthorityFiles(configPath, statePath, tokenPath),
		tailscaleStateLease(tailscaleStateDir),
		namedPath{name: "Tailscale enrollment key", path: tailscaleAuthKeyPath},
	)
	tailscaleState := namedPath{name: "Tailscale state directory", path: tailscaleStateDir}
	return validatePeerServiceEnvironment(
		environmentPath,
		authorities,
		codexHome,
		workspaceRoot,
		[]namedPath{tailscaleState, tailscaleStateLease(tailscaleStateDir)},
	)
}

func validatePeerServiceEnvironment(
	environmentPath string,
	authorities []namedPath,
	codexHome, workspaceRoot string,
	isolatedPaths []namedPath,
) error {
	environment := namedPath{name: "peer service environment", path: environmentPath}
	for _, authority := range authorities {
		conflicts, err := equivalent(environmentPath, authority.path)
		if err != nil {
			return err
		}
		if conflicts {
			return fmt.Errorf("peer service environment path conflicts with %s", authority.name)
		}
	}
	for _, isolated := range isolatedPaths {
		if err := rejectMutualContainment(environment, isolated); err != nil {
			return err
		}
	}
	for _, directory := range []namedPath{
		{name: "worker CODEX_HOME", path: codexHome},
		{name: "worker workspace root", path: workspaceRoot},
	} {
		contained, err := pathWithin(environmentPath, directory.path)
		if err != nil {
			return err
		}
		if contained {
			return fmt.Errorf("peer service environment must not be inside %s", directory.name)
		}
	}
	if err := requireSingleLink(environment); err != nil {
		return err
	}
	return nil
}

// ValidateBrokerAuthority rejects aliases between the configuration, master
// token, and SQLite database files that make up a broker authority.
func ValidateBrokerAuthority(configPath, statePath, masterPath string) error {
	files := brokerAuthorityFiles(configPath, statePath, masterPath)
	for firstIndex, first := range files {
		for _, second := range files[firstIndex+1:] {
			conflicts, err := equivalent(first.path, second.path)
			if err != nil {
				return err
			}
			if conflicts {
				return fmt.Errorf("%s path conflicts with %s", second.name, first.name)
			}
		}
	}
	return nil
}

// ValidateBrokerTailscaleAuthority extends the broker file authority check to
// the embedded Tailscale state directory and enrollment key.
func ValidateBrokerTailscaleAuthority(
	configPath, statePath, tokenPath, tailscaleStateDir, tailscaleAuthKeyPath string,
) error {
	if err := ValidateBrokerAuthority(configPath, statePath, tokenPath); err != nil {
		return err
	}
	return validateTailscaleAuthority(
		brokerAuthorityFiles(configPath, statePath, tokenPath),
		tailscaleStateDir,
		tailscaleAuthKeyPath,
	)
}

// ValidateCredentialOutput rejects an output token path that aliases any file
// belonging to the broker authority.
func ValidateCredentialOutput(tokenPath, configPath, statePath, masterPath string) error {
	for _, candidate := range brokerAuthorityFiles(configPath, statePath, masterPath) {
		conflicts, err := equivalent(tokenPath, candidate.path)
		if err != nil {
			return err
		}
		if conflicts {
			return fmt.Errorf("output token path conflicts with %s", candidate.name)
		}
	}
	return nil
}

func brokerAuthorityFiles(configPath, statePath, masterPath string) []namedPath {
	files := []namedPath{{name: "broker configuration", path: configPath}}
	if masterPath != "" {
		files = append(files, namedPath{name: "broker master token", path: masterPath})
	}
	return append(files,
		namedPath{name: "broker state", path: statePath},
		namedPath{name: "broker rollback journal", path: statePath + "-journal"},
		namedPath{name: "broker WAL", path: statePath + "-wal"},
		namedPath{name: "broker shared memory", path: statePath + "-shm"},
		namedPath{name: "broker instance lease", path: statePath + ".broker.lock"},
	)
}

func peerAuthorityFiles(configPath, statePath, tokenPath string) []namedPath {
	files := []namedPath{{name: "peer configuration", path: configPath}}
	if tokenPath != "" {
		files = append(files, namedPath{name: "peer token", path: tokenPath})
	}
	return append(files,
		namedPath{name: "peer state", path: statePath},
		namedPath{name: "peer rollback journal", path: statePath + "-journal"},
		namedPath{name: "peer WAL", path: statePath + "-wal"},
		namedPath{name: "peer shared memory", path: statePath + "-shm"},
		namedPath{name: "peer instance lease", path: statePath + ".peer.lock"},
	)
}

func validateTailscaleAuthority(
	files []namedPath, tailscaleStateDir, tailscaleAuthKeyPath string,
) error {
	tailscaleState := namedPath{name: "Tailscale state directory", path: tailscaleStateDir}
	tailscaleLease := tailscaleStateLease(tailscaleStateDir)
	tailscaleAuthKey := namedPath{name: "Tailscale enrollment key", path: tailscaleAuthKeyPath}
	for _, file := range files {
		conflicts, err := equivalent(tailscaleAuthKey.path, file.path)
		if err != nil {
			return err
		}
		if conflicts {
			return fmt.Errorf("%s path conflicts with %s", tailscaleAuthKey.name, file.name)
		}
	}
	for _, file := range append(append([]namedPath{}, files...), tailscaleAuthKey) {
		conflicts, err := equivalent(tailscaleLease.path, file.path)
		if err != nil {
			return err
		}
		if conflicts {
			return fmt.Errorf("%s path conflicts with %s", tailscaleLease.name, file.name)
		}
	}
	protectedFiles := append(append([]namedPath{}, files...), tailscaleAuthKey)
	for _, file := range protectedFiles {
		if err := rejectMutualContainment(tailscaleState, file); err != nil {
			return err
		}
		if err := rejectMutualContainment(tailscaleLease, file); err != nil {
			return err
		}
	}
	return requireSingleLink(tailscaleAuthKey)
}

func tailscaleStateLease(stateDir string) namedPath {
	return namedPath{
		name: "Tailscale state directory lease",
		path: stateDir + ".tailscale.lock",
	}
}

func rejectMutualContainment(first, second namedPath) error {
	firstInsideSecond, err := pathWithin(first.path, second.path)
	if err != nil {
		return err
	}
	if firstInsideSecond {
		return fmt.Errorf("%s must not be inside %s", first.name, second.name)
	}
	secondInsideFirst, err := pathWithin(second.path, first.path)
	if err != nil {
		return err
	}
	if secondInsideFirst {
		return fmt.Errorf("%s must not be inside %s", second.name, first.name)
	}
	conflicts, err := equivalent(first.path, second.path)
	if err != nil {
		return err
	}
	if conflicts {
		return fmt.Errorf("%s path conflicts with %s", first.name, second.name)
	}
	return nil
}

func equivalent(first, second string) (bool, error) {
	firstCanonical, err := canonicalFuturePath(first)
	if err != nil {
		return false, err
	}
	secondCanonical, err := canonicalFuturePath(second)
	if err != nil {
		return false, err
	}
	// Conservatively cover case-insensitive macOS and removable filesystems.
	if strings.EqualFold(firstCanonical, secondCanonical) {
		return true, nil
	}
	firstInfo, firstErr := os.Stat(firstCanonical)
	if firstErr != nil && !errors.Is(firstErr, os.ErrNotExist) {
		return false, fmt.Errorf("inspect path identity for %s: %w", firstCanonical, firstErr)
	}
	secondInfo, secondErr := os.Stat(secondCanonical)
	if secondErr != nil && !errors.Is(secondErr, os.ErrNotExist) {
		return false, fmt.Errorf("inspect path identity for %s: %w", secondCanonical, secondErr)
	}
	return firstErr == nil && secondErr == nil && os.SameFile(firstInfo, secondInfo), nil
}

func requireSingleLink(file namedPath) error {
	canonical, err := canonicalFuturePath(file.path)
	if err != nil {
		return err
	}
	opened, err := os.Open(canonical)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect hard-link count for %s: %w", file.name, err)
	}
	defer opened.Close()
	info, err := opened.Stat()
	if err != nil {
		return fmt.Errorf("inspect hard-link count for %s: %w", file.name, err)
	}
	count, err := openedFileLinkCount(opened, info)
	if err != nil {
		return fmt.Errorf("inspect hard-link count for %s: %w", file.name, err)
	}
	if count != 1 {
		return fmt.Errorf("%s has unexpected hard-link count %d; expected 1", file.name, count)
	}
	return nil
}

func canonicalFuturePath(path string) (string, error) {
	if !filepath.IsAbs(path) {
		return "", errors.New("guarded path must be absolute")
	}
	if hasParentComponent(path) {
		return "", errors.New("guarded path must not contain parent path components")
	}
	return resolveFuturePath(filepath.Clean(path), 0)
}

func hasParentComponent(path string) bool {
	for _, component := range strings.FieldsFunc(path, func(char rune) bool {
		return char == rune(filepath.Separator) || filepath.Separator == '\\' && char == '/'
	}) {
		if component == ".." {
			return true
		}
	}
	return false
}

func pathWithin(candidate, directory string) (bool, error) {
	canonicalCandidate, err := canonicalFuturePath(candidate)
	if err != nil {
		return false, err
	}
	canonicalDirectory, err := canonicalFuturePath(directory)
	if err != nil {
		return false, err
	}
	return pathWithinCanonical(canonicalCandidate, canonicalDirectory)
}

func pathWithinCanonical(canonicalCandidate, canonicalDirectory string) (bool, error) {
	separator := string(filepath.Separator)
	if strings.EqualFold(canonicalCandidate, canonicalDirectory) ||
		strings.HasPrefix(strings.ToLower(canonicalCandidate), strings.ToLower(canonicalDirectory+separator)) {
		return true, nil
	}
	if !strings.EqualFold(filepath.VolumeName(canonicalCandidate), filepath.VolumeName(canonicalDirectory)) {
		return false, nil
	}
	relative, err := filepath.Rel(canonicalDirectory, canonicalCandidate)
	if err != nil {
		return false, fmt.Errorf("compare guarded paths: %w", err)
	}
	return relative == "." || relative != ".." &&
		!strings.HasPrefix(relative, ".."+separator) &&
		!filepath.IsAbs(relative), nil
}

func resolveFuturePath(path string, followedLinks int) (string, error) {
	root, components := splitAbsolutePath(path)
	resolved := root
	for index, component := range components {
		candidate := filepath.Join(resolved, component)
		info, err := os.Lstat(candidate)
		if errors.Is(err, os.ErrNotExist) {
			canonicalResolved, err := platformCanonicalExistingPath(resolved)
			if err != nil {
				return "", fmt.Errorf("canonicalize existing path prefix %s: %w", resolved, err)
			}
			return filepath.Join(append([]string{canonicalResolved}, components[index:]...)...), nil
		}
		if err != nil {
			return "", fmt.Errorf("resolve path aliases for %s: %w", path, err)
		}
		target, alias, err := pathAliasTarget(candidate, info)
		if err != nil {
			return "", fmt.Errorf("read path alias %s: %w", candidate, err)
		}
		if !alias {
			resolved = candidate
			continue
		}
		if followedLinks >= 255 {
			return "", fmt.Errorf("resolve path aliases for %s: too many symbolic links", path)
		}
		target, err = resolveLinkTarget(resolved, target)
		if err != nil {
			return "", fmt.Errorf("resolve path alias %s: %w", candidate, err)
		}
		remaining := append([]string{target}, components[index+1:]...)
		return resolveFuturePath(filepath.Join(remaining...), followedLinks+1)
	}
	canonicalResolved, err := platformCanonicalExistingPath(resolved)
	if err != nil {
		return "", fmt.Errorf("canonicalize existing path %s: %w", resolved, err)
	}
	return canonicalResolved, nil
}

func pathAliasTarget(path string, info os.FileInfo) (string, bool, error) {
	if info.Mode()&os.ModeSymlink != 0 {
		target, err := os.Readlink(path)
		return target, true, err
	}
	return platformPathAliasTarget(path, info)
}

func splitAbsolutePath(path string) (string, []string) {
	current := filepath.Clean(path)
	var reversed []string
	for {
		parent := filepath.Dir(current)
		if parent == current {
			break
		}
		reversed = append(reversed, filepath.Base(current))
		current = parent
	}
	components := make([]string, len(reversed))
	for index := range reversed {
		components[len(reversed)-1-index] = reversed[index]
	}
	return current, components
}
