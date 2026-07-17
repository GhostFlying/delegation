package main

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"crypto/sha256"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/GhostFlying/delegation/internal/buildinfo"
)

type target struct {
	os      string
	arch    string
	archive string
}

var releaseTargets = []target{
	{os: "linux", arch: "amd64", archive: "tar.gz"},
	{os: "linux", arch: "arm64", archive: "tar.gz"},
	{os: "darwin", arch: "amd64", archive: "tar.gz"},
	{os: "darwin", arch: "arm64", archive: "tar.gz"},
	{os: "windows", arch: "amd64", archive: "zip"},
	{os: "windows", arch: "arm64", archive: "zip"},
}

const requiredGoVersion = "go1.26.5"

var versionPattern = regexp.MustCompile(`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:-(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)(?:\.(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*))*)?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$`)

var pinnedGoEnvironment = []string{
	"CGO_ENABLED=0",
	"GO111MODULE=on",
	"GOENV=off",
	"GOEXPERIMENT=none",
	"GOFIPS140=off",
	"GOFLAGS=",
	"GOTOOLCHAIN=local",
	"GOWORK=off",
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("releasepack", flag.ContinueOnError)
	flags.SetOutput(stderr)
	repoRoot := flags.String("repo", ".", "repository root")
	output := flags.String("out", "dist", "new output directory")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "releasepack: unexpected argument %q\n", flags.Arg(0))
		return 2
	}
	if err := packageRelease(*repoRoot, *output); err != nil {
		fmt.Fprintf(stderr, "releasepack: %v\n", err)
		return 1
	}
	if _, err := fmt.Fprintln(stdout, *output); err != nil {
		fmt.Fprintf(stderr, "releasepack: release was packaged but output reporting failed: %v\n", err)
		return 1
	}
	return 0
}

func packageRelease(repoRoot, output string) error {
	root, err := filepath.Abs(repoRoot)
	if err != nil {
		return fmt.Errorf("resolve repository root: %w", err)
	}
	output, err = filepath.Abs(output)
	if err != nil {
		return fmt.Errorf("resolve output directory: %w", err)
	}
	if _, err := os.Lstat(output); err == nil {
		return fmt.Errorf("output already exists: %s", output)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect output: %w", err)
	}
	version, err := readVersion(root)
	if err != nil {
		return err
	}
	if err := verifyGoToolchain(root); err != nil {
		return err
	}
	parent := filepath.Dir(output)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return fmt.Errorf("create output parent: %w", err)
	}
	staging, err := os.MkdirTemp(parent, ".delegation-release-*")
	if err != nil {
		return fmt.Errorf("create release staging directory: %w", err)
	}
	defer os.RemoveAll(staging)

	checksums := make(map[string]string, len(releaseTargets))
	for _, target := range releaseTargets {
		name := fmt.Sprintf("delegation_%s_%s_%s.%s", version, target.os, target.arch, target.archive)
		binaryName := "delegation"
		if target.os == "windows" {
			binaryName += ".exe"
		}
		binaryPath := filepath.Join(staging, ".binary-"+target.os+"-"+target.arch+filepath.Ext(binaryName))
		if err := buildTarget(root, binaryPath, target); err != nil {
			return err
		}
		archivePath := filepath.Join(staging, name)
		if target.archive == "zip" {
			err = writeZip(archivePath, binaryPath, binaryName)
		} else {
			err = writeTarGzip(archivePath, binaryPath, binaryName)
		}
		if err != nil {
			return fmt.Errorf("package %s: %w", name, err)
		}
		if err := os.Remove(binaryPath); err != nil {
			return fmt.Errorf("remove staged binary: %w", err)
		}
		digest, err := fileSHA256(archivePath)
		if err != nil {
			return err
		}
		checksums[name] = digest
	}
	if err := writeChecksumManifest(filepath.Join(staging, "release-artifacts.sha256"), checksums); err != nil {
		return err
	}
	if err := commitReleaseDirectory(staging, output); err != nil {
		return fmt.Errorf("commit release directory: %w", err)
	}
	return nil
}

func readVersion(root string) (string, error) {
	data, err := os.ReadFile(filepath.Join(root, "plugins", "delegation", "VERSION"))
	if err != nil {
		return "", fmt.Errorf("read plugin version: %w", err)
	}
	version := strings.TrimSpace(string(data))
	if !versionPattern.MatchString(version) {
		return "", fmt.Errorf("invalid plugin version %q", version)
	}
	if version != buildinfo.Version {
		return "", fmt.Errorf("plugin version %s does not match runtime version %s", version, buildinfo.Version)
	}
	return version, nil
}

func buildTarget(root, output string, target target) error {
	command := exec.Command(
		"go",
		"build",
		"-trimpath",
		"-buildvcs=false",
		"-ldflags=-buildid=",
		"-o",
		output,
		"./cmd/delegation",
	)
	command.Dir = root
	command.Env = targetEnvironment(os.Environ(), target)
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	if err := command.Run(); err != nil {
		return fmt.Errorf("build %s-%s: %w", target.os, target.arch, err)
	}
	return nil
}

func targetEnvironment(environment []string, target target) []string {
	filtered := sanitizedGoEnvironment(environment)
	filtered = append(filtered, "GOOS="+target.os, "GOARCH="+target.arch)
	if target.arch == "amd64" {
		filtered = append(filtered, "GOAMD64=v1")
	} else {
		filtered = append(filtered, "GOARM64=v8.0")
	}
	return filtered
}

func sanitizedGoEnvironment(environment []string) []string {
	pinned := map[string]bool{
		"CGO_ENABLED":  true,
		"GO111MODULE":  true,
		"GOAMD64":      true,
		"GOARCH":       true,
		"GOARM64":      true,
		"GOENV":        true,
		"GOEXPERIMENT": true,
		"GOFIPS140":    true,
		"GOFLAGS":      true,
		"GOOS":         true,
		"GOROOT":       true,
		"GOTOOLCHAIN":  true,
		"GOWORK":       true,
	}
	filtered := make([]string, 0, len(environment)+len(pinnedGoEnvironment))
	for _, entry := range environment {
		name, _, _ := strings.Cut(entry, "=")
		if pinned[strings.ToUpper(name)] {
			continue
		}
		filtered = append(filtered, entry)
	}
	return append(filtered, pinnedGoEnvironment...)
}

func verifyGoToolchain(root string) error {
	command := exec.Command("go", "env", "GOVERSION")
	command.Dir = root
	command.Env = sanitizedGoEnvironment(os.Environ())
	output, err := command.CombinedOutput()
	if err != nil {
		return fmt.Errorf("verify Go toolchain: %w: %s", err, strings.TrimSpace(string(output)))
	}
	if version := strings.TrimSpace(string(output)); version != requiredGoVersion {
		return fmt.Errorf("Go toolchain is %s, require %s", version, requiredGoVersion)
	}
	return nil
}

func writeTarGzip(destination, binaryPath, binaryName string) error {
	binary, err := os.ReadFile(binaryPath)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	gzipWriter := gzip.NewWriter(file)
	gzipWriter.Header.ModTime = time.Unix(0, 0).UTC()
	tarWriter := tar.NewWriter(gzipWriter)
	header := &tar.Header{
		Name:     binaryName,
		Mode:     0o755,
		Size:     int64(len(binary)),
		ModTime:  time.Unix(0, 0).UTC(),
		Typeflag: tar.TypeReg,
		Format:   tar.FormatUSTAR,
	}
	if err := tarWriter.WriteHeader(header); err != nil {
		return closeArchive(file, tarWriter, gzipWriter, err)
	}
	if _, err := tarWriter.Write(binary); err != nil {
		return closeArchive(file, tarWriter, gzipWriter, err)
	}
	return closeArchive(file, tarWriter, gzipWriter, nil)
}

func closeArchive(file *os.File, tarWriter *tar.Writer, gzipWriter *gzip.Writer, prior error) error {
	return errors.Join(prior, tarWriter.Close(), gzipWriter.Close(), file.Close())
}

func writeZip(destination, binaryPath, binaryName string) error {
	binary, err := os.ReadFile(binaryPath)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	zipWriter := zip.NewWriter(file)
	header := &zip.FileHeader{Name: binaryName, Method: zip.Deflate}
	header.SetMode(0o755)
	header.SetModTime(time.Date(1980, time.January, 1, 0, 0, 0, 0, time.UTC))
	entry, err := zipWriter.CreateHeader(header)
	if err == nil {
		_, err = entry.Write(binary)
	}
	return errors.Join(err, zipWriter.Close(), file.Close())
}

func fileSHA256(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	digest := sha256.New()
	_, copyErr := io.Copy(digest, file)
	closeErr := file.Close()
	if err := errors.Join(copyErr, closeErr); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", digest.Sum(nil)), nil
}

func writeChecksumManifest(path string, checksums map[string]string) error {
	names := make([]string, 0, len(checksums))
	for name := range checksums {
		names = append(names, name)
	}
	sort.Strings(names)
	var content strings.Builder
	for _, name := range names {
		fmt.Fprintf(&content, "%s  %s\n", checksums[name], name)
	}
	if err := os.WriteFile(path, []byte(content.String()), 0o644); err != nil {
		return fmt.Errorf("write checksum manifest: %w", err)
	}
	return nil
}
