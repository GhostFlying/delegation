package codexcommand

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

const maximumMetadataBytes = 1 << 20

var managedEnvironmentKeys = []string{
	"CODEX_MANAGED_PACKAGE_ROOT",
	"CODEX_MANAGED_BY_NPM",
	"CODEX_MANAGED_BY_PNPM",
	"CODEX_MANAGED_BY_BUN",
}

// Launch identifies the lexical command selected by the user and the native
// Codex executable resolved from it for the current connector start.
type Launch struct {
	CommandPath      string
	NativePath       string
	Environment      map[string]string
	UnsetEnvironment []string
}

type platformPackage struct {
	target      string
	packageName string
	binaryName  string
}

type packageMetadata struct {
	LayoutVersion int    `json:"layoutVersion"`
	Version       string `json:"version"`
	Target        string `json:"target"`
	Variant       string `json:"variant"`
	Entrypoint    string `json:"entrypoint"`
}

// Resolve accepts a native Codex executable or an official npm command shim.
// Official shims are resolved to their version-matched native optional package.
func Resolve(command string) (Launch, error) {
	if strings.TrimSpace(command) == "" {
		return Launch{}, errors.New("Codex command is required")
	}
	commandPath, err := exec.LookPath(command)
	if err != nil {
		return Launch{}, err
	}
	commandPath, err = filepath.Abs(commandPath)
	if err != nil {
		return Launch{}, fmt.Errorf("resolve Codex command path: %w", err)
	}
	canonical, err := filepath.EvalSymlinks(commandPath)
	if err != nil {
		return Launch{}, fmt.Errorf("resolve Codex command links: %w", err)
	}
	if native, err := isNativeExecutable(canonical); err != nil {
		return Launch{}, err
	} else if native {
		return Launch{
			CommandPath: commandPath, NativePath: canonical,
			UnsetEnvironment: append([]string(nil), managedEnvironmentKeys...),
		}, nil
	}

	packageRoot, packageVersion, err := officialPackageRoot(commandPath, canonical)
	if err != nil {
		return Launch{}, err
	}
	spec, err := currentPlatformPackage()
	if err != nil {
		return Launch{}, err
	}
	nativePath, err := resolvePackageBinary(packageRoot, packageVersion, spec)
	if err != nil {
		return Launch{}, err
	}
	managerKey := "CODEX_MANAGED_BY_NPM"
	normalized := strings.ToLower(filepath.ToSlash(commandPath + "\n" + packageRoot))
	if strings.Contains(normalized, "/.pnpm/") {
		managerKey = "CODEX_MANAGED_BY_PNPM"
	} else if strings.Contains(normalized, "/.bun/") {
		managerKey = "CODEX_MANAGED_BY_BUN"
	}
	return Launch{
		CommandPath: commandPath,
		NativePath:  nativePath,
		Environment: map[string]string{
			"CODEX_MANAGED_PACKAGE_ROOT": packageRoot,
			managerKey:                   "1",
		},
		UnsetEnvironment: append([]string(nil), managedEnvironmentKeys...),
	}, nil
}

func officialPackageRoot(commandPath, canonical string) (string, string, error) {
	if strings.EqualFold(filepath.Ext(commandPath), ".cmd") ||
		strings.EqualFold(filepath.Ext(commandPath), ".bat") {
		candidates, err := windowsShimPackageRootCandidates(commandPath)
		if err != nil {
			return "", "", err
		}
		referenced, referenceErr := windowsShimReferencedPackageRoots(commandPath)
		if referenceErr == nil {
			candidates = append(candidates, referenced...)
		}
		var missingError error
		for _, candidate := range candidates {
			root, version, err := validateOfficialPackageRoot(candidate)
			if err == nil {
				return root, version, nil
			}
			if !errors.Is(err, os.ErrNotExist) {
				return "", "", err
			}
			missingError = err
		}
		if referenceErr != nil {
			return "", "", referenceErr
		}
		return "", "", missingError
	}
	if filepath.Base(canonical) != "codex.js" || filepath.Base(filepath.Dir(canonical)) != "bin" {
		return "", "", errors.New("Codex command is neither a native executable nor an official npm shim")
	}
	return validateOfficialPackageRoot(filepath.Dir(filepath.Dir(canonical)))
}

func windowsShimReferencedPackageRoots(commandPath string) ([]string, error) {
	data, err := readBoundedFile(commandPath)
	if err != nil {
		return nil, fmt.Errorf("read Windows Codex command shim: %w", err)
	}
	segments := strings.Fields(string(data))
	quoted := strings.Split(string(data), `"`)
	for index := 1; index < len(quoted); index += 2 {
		segments = append(segments, quoted[index])
	}
	marker := "/@openai/codex/bin/codex.js"
	roots := make([]string, 0, 1)
	seen := make(map[string]struct{})
	for _, segment := range segments {
		normalized := strings.ReplaceAll(strings.Trim(segment, `"'`), `\`, "/")
		folded := foldASCII(normalized)
		end := strings.Index(folded, marker)
		if end < 0 {
			continue
		}
		end += len(marker)
		candidate := normalized[:end]
		candidateFolded := folded[:end]
		for _, prefix := range []string{"%dp0%", "%~dp0"} {
			if strings.HasPrefix(candidateFolded, prefix) {
				rest := strings.TrimPrefix(candidate[len(prefix):], "/")
				candidate = filepath.Join(filepath.Dir(commandPath), filepath.FromSlash(rest))
				break
			}
		}
		if !filepath.IsAbs(candidate) {
			continue
		}
		root := filepath.Clean(filepath.Dir(filepath.Dir(candidate)))
		key := strings.ToLower(root)
		if _, duplicate := seen[key]; duplicate {
			continue
		}
		seen[key] = struct{}{}
		roots = append(roots, root)
	}
	if len(roots) == 0 {
		return nil, errors.New("Windows Codex command shim does not reference an official @openai/codex entrypoint")
	}
	return roots, nil
}

func foldASCII(value string) string {
	folded := []byte(value)
	for index, char := range folded {
		if char >= 'A' && char <= 'Z' {
			folded[index] = char + ('a' - 'A')
		}
	}
	return string(folded)
}

func windowsShimPackageRootCandidates(commandPath string) ([]string, error) {
	extension := filepath.Ext(commandPath)
	if !strings.EqualFold(extension, ".cmd") && !strings.EqualFold(extension, ".bat") {
		return nil, errors.New("Windows Codex npm shim must use a .cmd or .bat extension")
	}
	if !strings.EqualFold(strings.TrimSuffix(filepath.Base(commandPath), extension), "codex") {
		return nil, errors.New("Windows Codex npm shim must be named codex.cmd or codex.bat")
	}

	commandDirectory := filepath.Dir(commandPath)
	candidates := []string{
		filepath.Join(commandDirectory, "node_modules", "@openai", "codex"),
	}
	if strings.EqualFold(filepath.Base(commandDirectory), ".bin") &&
		strings.EqualFold(filepath.Base(filepath.Dir(commandDirectory)), "node_modules") {
		candidates = append(candidates,
			filepath.Join(filepath.Dir(commandDirectory), "@openai", "codex"),
		)
	}
	return candidates, nil
}

func validateOfficialPackageRoot(root string) (string, string, error) {
	canonical, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", "", fmt.Errorf("resolve official Codex package root: %w", err)
	}
	var metadata struct {
		Name    string            `json:"name"`
		Version string            `json:"version"`
		Bin     map[string]string `json:"bin"`
	}
	if err := readBoundedJSON(filepath.Join(canonical, "package.json"), &metadata); err != nil {
		return "", "", fmt.Errorf("read official Codex package metadata: %w", err)
	}
	if metadata.Name != "@openai/codex" || metadata.Version == "" ||
		filepath.ToSlash(metadata.Bin["codex"]) != "bin/codex.js" {
		return "", "", errors.New("Codex npm package metadata is not an official @openai/codex command")
	}
	return canonical, metadata.Version, nil
}

func resolvePackageBinary(packageRoot, packageVersion string, spec platformPackage) (string, error) {
	relative := filepath.Join("node_modules", "@openai", spec.packageName, "vendor", spec.target)
	candidates := []string{
		// npm may keep the optional package nested under @openai/codex or
		// bundle the platform payload directly in its package.
		filepath.Join(packageRoot, relative),
		filepath.Join(packageRoot, "vendor", spec.target),
	}
	for parent := filepath.Dir(packageRoot); ; parent = filepath.Dir(parent) {
		// npm, pnpm, and bun may hoist the optional package to any actual
		// node_modules ancestor. Do not synthesize node_modules beneath every
		// filesystem ancestor: such a directory is unrelated to this install.
		if strings.EqualFold(filepath.Base(parent), "node_modules") {
			candidates = append(candidates,
				filepath.Join(parent, "@openai", spec.packageName, "vendor", spec.target),
			)
		}
		next := filepath.Dir(parent)
		if next == parent {
			break
		}
	}
	for _, candidate := range candidates {
		manifest := filepath.Join(candidate, "codex-package.json")
		if _, err := os.Stat(manifest); errors.Is(err, os.ErrNotExist) {
			continue
		}
		var metadata packageMetadata
		if err := readBoundedJSON(manifest, &metadata); err != nil {
			return "", fmt.Errorf("read Codex native package metadata: %w", err)
		}
		if metadata.LayoutVersion != 1 || metadata.Version != packageVersion ||
			metadata.Target != spec.target || metadata.Variant != "codex" {
			return "", errors.New("Codex native package metadata does not match this platform")
		}
		entrypoint := filepath.Clean(filepath.FromSlash(metadata.Entrypoint))
		if entrypoint == "." || filepath.IsAbs(entrypoint) || entrypoint == ".." ||
			strings.HasPrefix(entrypoint, ".."+string(filepath.Separator)) {
			return "", errors.New("Codex native package entrypoint escapes its package")
		}
		nativePath, err := filepath.EvalSymlinks(filepath.Join(candidate, entrypoint))
		if err != nil {
			return "", fmt.Errorf("resolve Codex native entrypoint: %w", err)
		}
		candidateRoot, err := filepath.EvalSymlinks(candidate)
		if err != nil {
			return "", fmt.Errorf("resolve Codex native package: %w", err)
		}
		relativeNative, err := filepath.Rel(candidateRoot, nativePath)
		if err != nil || relativeNative == ".." || strings.HasPrefix(relativeNative, ".."+string(filepath.Separator)) {
			return "", errors.New("Codex native entrypoint escapes its package")
		}
		native, err := isNativeExecutable(nativePath)
		if err != nil {
			return "", err
		}
		if !native || filepath.Base(nativePath) != spec.binaryName {
			return "", errors.New("Codex native package entrypoint is not the expected executable")
		}
		return nativePath, nil
	}
	return "", fmt.Errorf("official Codex npm package is missing %s for %s", spec.packageName, spec.target)
}

func readBoundedJSON(path string, destination any) error {
	data, err := readBoundedFile(path)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, destination); err != nil {
		return err
	}
	return nil
}

func readBoundedFile(path string) ([]byte, error) {
	file, err := openRegularFile(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maximumMetadataBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maximumMetadataBytes {
		return nil, errors.New("file exceeds its size limit")
	}
	return data, nil
}

func isNativeExecutable(path string) (bool, error) {
	info, err := os.Stat(path)
	if err != nil {
		return false, fmt.Errorf("inspect Codex executable: %w", err)
	}
	if !info.Mode().IsRegular() {
		return false, errors.New("Codex executable must be a regular file")
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0o111 == 0 {
		return false, errors.New("Codex executable must have an execute bit set")
	}
	file, err := os.Open(path)
	if err != nil {
		return false, fmt.Errorf("inspect Codex executable: %w", err)
	}
	defer file.Close()
	header := make([]byte, 4)
	if _, err := io.ReadFull(file, header); err != nil {
		return false, nil
	}
	if bytes.Equal(header, []byte{0x7f, 'E', 'L', 'F'}) || bytes.Equal(header[:2], []byte{'M', 'Z'}) {
		return true, nil
	}
	for _, magic := range [][]byte{
		{0xfe, 0xed, 0xfa, 0xce}, {0xce, 0xfa, 0xed, 0xfe},
		{0xfe, 0xed, 0xfa, 0xcf}, {0xcf, 0xfa, 0xed, 0xfe},
		{0xca, 0xfe, 0xba, 0xbe}, {0xbe, 0xba, 0xfe, 0xca},
	} {
		if bytes.Equal(header, magic) {
			return true, nil
		}
	}
	return false, nil
}

func currentPlatformPackage() (platformPackage, error) {
	return packageForPlatform(runtime.GOOS, runtime.GOARCH)
}

func packageForPlatform(goos, goarch string) (platformPackage, error) {
	var spec platformPackage
	switch goos + "/" + goarch {
	case "linux/amd64":
		spec = platformPackage{"x86_64-unknown-linux-musl", "codex-linux-x64", "codex"}
	case "linux/arm64":
		spec = platformPackage{"aarch64-unknown-linux-musl", "codex-linux-arm64", "codex"}
	case "darwin/amd64":
		spec = platformPackage{"x86_64-apple-darwin", "codex-darwin-x64", "codex"}
	case "darwin/arm64":
		spec = platformPackage{"aarch64-apple-darwin", "codex-darwin-arm64", "codex"}
	case "windows/amd64":
		spec = platformPackage{"x86_64-pc-windows-msvc", "codex-win32-x64", "codex.exe"}
	case "windows/arm64":
		spec = platformPackage{"aarch64-pc-windows-msvc", "codex-win32-arm64", "codex.exe"}
	default:
		return platformPackage{}, fmt.Errorf("unsupported Codex platform %s/%s", goos, goarch)
	}
	return spec, nil
}
