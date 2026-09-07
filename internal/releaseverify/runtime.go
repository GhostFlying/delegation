package releaseverify

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/securefs"
)

const (
	runtimeBinaryName    = "delegation"
	releaseNoticeName    = "THIRD_PARTY_NOTICES.txt"
	maximumRuntimeBytes  = 256 << 20
	maximumReleaseNotice = 16 << 20
	maximumVersionOutput = 256
)

type RuntimeMaterial struct {
	BinaryPath   string
	BinarySHA256 string
	Directory    string
}

type VersionProbe func(context.Context, string) (string, error)

// InstallRuntime extracts a verified current-platform archive into the same
// immutable version/platform layout used by plugin installation. A concurrent
// publisher wins only when both of its files are byte-for-byte identical.
func InstallRuntime(
	ctx context.Context, delegationHome string, verified Result, probe VersionProbe,
) (RuntimeMaterial, error) {
	if !filepath.IsAbs(delegationHome) || filepath.Clean(delegationHome) != delegationHome {
		return RuntimeMaterial{}, errors.New("delegation home must be an absolute clean path")
	}
	expectedArtifact, err := artifactName(verified.Version, CurrentPlatform())
	if err != nil {
		return RuntimeMaterial{}, err
	}
	if verified.Artifact.Name != expectedArtifact || verified.ArtifactSHA256 != sha256Hex(verified.Artifact.Content) {
		return RuntimeMaterial{}, errors.New("verified runtime artifact identity is inconsistent")
	}
	if probe == nil {
		probe = probeRuntimeVersion
	}
	if err := ensurePrivateDirectory(delegationHome); err != nil {
		return RuntimeMaterial{}, fmt.Errorf("prepare delegation home: %w", err)
	}
	binRoot := filepath.Join(delegationHome, "bin")
	versionRoot := filepath.Join(binRoot, verified.Version)
	for _, directory := range []string{binRoot, versionRoot} {
		if err := ensurePrivateDirectory(directory); err != nil {
			return RuntimeMaterial{}, fmt.Errorf("prepare immutable runtime parent: %w", err)
		}
	}
	stage, err := os.MkdirTemp(versionRoot, ".install-"+CurrentPlatform().OS+"-"+CurrentPlatform().Architecture+"-")
	if err != nil {
		return RuntimeMaterial{}, fmt.Errorf("create runtime staging directory: %w", err)
	}
	defer os.RemoveAll(stage)
	if err := delegationconfig.PreparePrivateDirectory(stage); err != nil {
		return RuntimeMaterial{}, fmt.Errorf("protect runtime staging directory: %w", err)
	}
	if err := extractRuntimeArchive(verified.Artifact.Content, CurrentPlatform(), stage); err != nil {
		return RuntimeMaterial{}, err
	}
	staged, err := validateRuntimeDirectory(ctx, stage, verified.Version, probe)
	if err != nil {
		return RuntimeMaterial{}, fmt.Errorf("validate staged runtime: %w", err)
	}
	parent, err := securefs.OpenRoot(versionRoot, nil)
	if err != nil {
		return RuntimeMaterial{}, err
	}
	stageName := filepath.Base(stage)
	targetName := CurrentPlatform().OS + "-" + CurrentPlatform().Architecture
	publishErr := publishRuntimeDirectory(parent, stageName, targetName)
	if publishErr == nil {
		publishErr = parent.Sync()
	}
	closeErr := parent.Close()
	if publishErr == nil && closeErr != nil {
		return RuntimeMaterial{}, closeErr
	}
	target := filepath.Join(versionRoot, targetName)
	if publishErr != nil {
		existing, validateErr := validateRuntimeDirectory(ctx, target, verified.Version, probe)
		if validateErr != nil {
			return RuntimeMaterial{}, errors.Join(
				fmt.Errorf("publish immutable runtime: %w", publishErr),
				fmt.Errorf("validate concurrent runtime: %w", validateErr),
			)
		}
		identical, compareErr := runtimeDirectoriesEqual(stage, target)
		if compareErr != nil || !identical || existing.BinarySHA256 != staged.BinarySHA256 {
			return RuntimeMaterial{}, errors.Join(
				fmt.Errorf("publish immutable runtime: %w", publishErr), compareErr,
				errors.New("existing runtime differs from the verified release"),
			)
		}
		return existing, nil
	}
	staged.Directory = target
	staged.BinaryPath = filepath.Join(target, platformBinaryName(CurrentPlatform()))
	return staged, nil
}

func ensurePrivateDirectory(path string) error {
	_, err := os.Lstat(path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return delegationconfig.PreparePrivateDirectory(path)
	case err != nil:
		return err
	default:
		return delegationconfig.ValidatePrivateDirectory(path)
	}
}

func extractRuntimeArchive(content []byte, platform Platform, destination string) error {
	if len(content) == 0 || int64(len(content)) > maxRuntimeArtifactSize {
		return errors.New("runtime archive is empty or oversized")
	}
	if platform.OS == "windows" {
		return extractRuntimeZIP(content, destination)
	}
	return extractRuntimeTarGzip(content, destination)
}

func extractRuntimeTarGzip(content []byte, destination string) error {
	gzipReader, err := gzip.NewReader(bytes.NewReader(content))
	if err != nil {
		return fmt.Errorf("open runtime gzip archive: %w", err)
	}
	defer gzipReader.Close()
	reader := tar.NewReader(gzipReader)
	seen := map[string]bool{}
	for {
		header, nextErr := reader.Next()
		if errors.Is(nextErr, io.EOF) {
			break
		}
		if nextErr != nil {
			return fmt.Errorf("read runtime tar archive: %w", nextErr)
		}
		if header == nil || header.Typeflag != tar.TypeReg || header.Name != filepath.Base(header.Name) ||
			header.Linkname != "" || seen[header.Name] {
			return errors.New("runtime archive must contain exactly two unique root regular files")
		}
		mode, limit, valid := expectedRuntimeEntry(header.Name)
		if !valid || header.Mode != int64(mode.Perm()) || header.Size < 0 || header.Size > limit {
			return fmt.Errorf("runtime archive entry %q violates its file contract", header.Name)
		}
		if err := writeRuntimeEntry(destination, header.Name, mode, reader, header.Size); err != nil {
			return err
		}
		seen[header.Name] = true
	}
	if len(seen) != 2 || !seen[runtimeBinaryName] || !seen[releaseNoticeName] {
		return errors.New("runtime archive does not contain the exact release files")
	}
	return syncRuntimeDirectory(destination)
}

func extractRuntimeZIP(content []byte, destination string) error {
	reader, err := zip.NewReader(bytes.NewReader(content), int64(len(content)))
	if err != nil {
		return fmt.Errorf("open runtime zip archive: %w", err)
	}
	if len(reader.File) != 2 {
		return errors.New("runtime archive must contain exactly two files")
	}
	seen := map[string]bool{}
	for _, entry := range reader.File {
		if entry.Name != filepath.Base(entry.Name) || entry.FileInfo().Mode()&os.ModeType != 0 ||
			entry.Flags&0x1 != 0 || seen[entry.Name] {
			return errors.New("runtime archive must contain exactly two unique root regular files")
		}
		mode, limit, valid := expectedRuntimeEntry(entry.Name)
		if !valid || entry.Mode().Perm() != mode.Perm() || entry.UncompressedSize64 > uint64(limit) {
			return fmt.Errorf("runtime archive entry %q violates its file contract", entry.Name)
		}
		input, openErr := entry.Open()
		if openErr != nil {
			return openErr
		}
		writeErr := writeRuntimeEntry(destination, entry.Name, mode, input, int64(entry.UncompressedSize64))
		closeErr := input.Close()
		if err := errors.Join(writeErr, closeErr); err != nil {
			return err
		}
		seen[entry.Name] = true
	}
	if !seen["delegation.exe"] || !seen[releaseNoticeName] {
		return errors.New("runtime archive does not contain the exact Windows release files")
	}
	return syncRuntimeDirectory(destination)
}

func expectedRuntimeEntry(name string) (os.FileMode, int64, bool) {
	switch name {
	case runtimeBinaryName, "delegation.exe":
		return 0o755, maximumRuntimeBytes, true
	case releaseNoticeName:
		return 0o644, maximumReleaseNotice, true
	default:
		return 0, 0, false
	}
}

func writeRuntimeEntry(
	destination, name string, mode os.FileMode, input io.Reader, size int64,
) error {
	root, err := securefs.OpenRoot(destination, nil)
	if err != nil {
		return err
	}
	defer root.Close()
	file, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	written, copyErr := io.CopyN(file, input, size)
	if copyErr == nil && written != size {
		copyErr = io.ErrUnexpectedEOF
	}
	if copyErr == nil {
		var extra [1]byte
		if count, readErr := input.Read(extra[:]); count != 0 || (readErr != nil && !errors.Is(readErr, io.EOF)) {
			copyErr = errors.New("runtime archive entry exceeds its declared size")
		}
	}
	if copyErr == nil {
		copyErr = file.Chmod(mode)
	}
	if copyErr == nil {
		copyErr = file.Sync()
	}
	closeErr := file.Close()
	return errors.Join(copyErr, closeErr)
}

func syncRuntimeDirectory(path string) error {
	root, err := securefs.OpenRoot(path, nil)
	if err != nil {
		return err
	}
	defer root.Close()
	return root.Sync()
}

func validateRuntimeDirectory(
	ctx context.Context, directory, version string, probe VersionProbe,
) (RuntimeMaterial, error) {
	if err := delegationconfig.ValidatePrivateDirectory(directory); err != nil {
		return RuntimeMaterial{}, fmt.Errorf("validate runtime directory protection: %w", err)
	}
	root, err := securefs.OpenRoot(directory, nil)
	if err != nil {
		return RuntimeMaterial{}, err
	}
	defer root.Close()
	entries, err := root.Entries()
	if err != nil {
		return RuntimeMaterial{}, err
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		info, infoErr := entry.Info()
		if infoErr != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			return RuntimeMaterial{}, errors.New("runtime directory contains a non-regular file")
		}
		expectedMode, _, expectedEntry := expectedRuntimeEntry(entry.Name())
		if !expectedEntry {
			return RuntimeMaterial{}, errors.New("runtime directory contains an unexpected entry")
		}
		if err := validateInstalledRuntimeEntry(
			filepath.Join(directory, entry.Name()), info, expectedMode,
		); err != nil {
			return RuntimeMaterial{}, errors.New("runtime directory contains an entry with unexpected permissions")
		}
		names = append(names, entry.Name())
	}
	sort.Strings(names)
	binaryName := platformBinaryName(CurrentPlatform())
	expected := []string{releaseNoticeName, binaryName}
	sort.Strings(expected)
	if len(names) != 2 || names[0] != expected[0] || names[1] != expected[1] {
		return RuntimeMaterial{}, errors.New("runtime directory does not contain the exact release files")
	}
	binaryPath := filepath.Join(directory, binaryName)
	reported, err := probe(ctx, binaryPath)
	if err != nil {
		return RuntimeMaterial{}, fmt.Errorf("run target runtime version probe: %w", err)
	}
	if reported != version {
		return RuntimeMaterial{}, fmt.Errorf("target runtime reports version %q, expected %q", reported, version)
	}
	binaryFile, err := root.OpenFile(binaryName, os.O_RDONLY, 0)
	if err != nil {
		return RuntimeMaterial{}, err
	}
	digestWriter := sha256.New()
	_, copyErr := io.Copy(digestWriter, binaryFile)
	closeErr := binaryFile.Close()
	if err := errors.Join(copyErr, closeErr, root.VerifyPath()); err != nil {
		return RuntimeMaterial{}, err
	}
	return RuntimeMaterial{
		BinaryPath: binaryPath, BinarySHA256: fmt.Sprintf("%x", digestWriter.Sum(nil)), Directory: directory,
	}, nil
}

func runtimeDirectoriesEqual(first, second string) (bool, error) {
	for _, name := range []string{platformBinaryName(CurrentPlatform()), releaseNoticeName} {
		left, err := os.ReadFile(filepath.Join(first, name))
		if err != nil {
			return false, err
		}
		right, err := os.ReadFile(filepath.Join(second, name))
		if err != nil {
			return false, err
		}
		if !bytes.Equal(left, right) {
			return false, nil
		}
	}
	return true, nil
}

func platformBinaryName(platform Platform) string {
	if platform.OS == "windows" {
		return "delegation.exe"
	}
	return runtimeBinaryName
}

func probeRuntimeVersion(ctx context.Context, path string) (string, error) {
	command := exec.CommandContext(ctx, path, "version")
	var output limitedBuffer
	output.maximum = maximumVersionOutput
	command.Stdout = &output
	command.Stderr = io.Discard
	if err := command.Run(); err != nil {
		return "", err
	}
	value := strings.TrimSuffix(output.String(), "\n")
	value = strings.TrimSuffix(value, "\r")
	if strings.ContainsAny(value, "\r\n") {
		return "", errors.New("runtime version output is not one line")
	}
	return value, nil
}

type limitedBuffer struct {
	bytes.Buffer
	maximum int
}

func (b *limitedBuffer) Write(data []byte) (int, error) {
	if b.Len()+len(data) > b.maximum {
		return 0, errors.New("runtime version output exceeds its size limit")
	}
	return b.Buffer.Write(data)
}
