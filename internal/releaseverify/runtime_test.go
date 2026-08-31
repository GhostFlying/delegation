package releaseverify

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestInstallRuntimePublishesImmutableExactVersion(t *testing.T) {
	archive := currentPlatformArchive(t, []byte("runtime bytes"), []byte("notice"))
	result := Result{
		Version: "0.1.0-alpha.8", Artifact: Asset{Name: mustArtifactName(t, "0.1.0-alpha.8"), Content: archive},
		ArtifactSHA256: sha256Hex(archive),
	}
	home := filepath.Join(t.TempDir(), "delegation-home")
	probe := func(_ context.Context, path string) (string, error) {
		if filepath.Base(path) != platformBinaryName(CurrentPlatform()) {
			return "", fmt.Errorf("unexpected binary path %s", path)
		}
		return result.Version, nil
	}
	var wait sync.WaitGroup
	results := make([]RuntimeMaterial, 4)
	errors := make([]error, 4)
	for index := range results {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			results[index], errors[index] = InstallRuntime(context.Background(), home, result, probe)
		}(index)
	}
	wait.Wait()
	for index, err := range errors {
		if err != nil {
			t.Fatalf("InstallRuntime[%d]() error = %v", index, err)
		}
		if results[index].BinaryPath != results[0].BinaryPath || results[index].BinarySHA256 != results[0].BinarySHA256 {
			t.Fatalf("runtime result[%d] = %#v, first = %#v", index, results[index], results[0])
		}
	}
	entries, err := os.ReadDir(results[0].Directory)
	if err != nil || len(entries) != 2 {
		t.Fatalf("runtime entries = %v, %v", entries, err)
	}
}

func TestInstallRuntimeRejectsVersionMismatchWithoutPublishing(t *testing.T) {
	archive := currentPlatformArchive(t, []byte("runtime bytes"), []byte("notice"))
	version := "0.1.0-alpha.8"
	result := Result{Version: version, Artifact: Asset{Name: mustArtifactName(t, version), Content: archive}, ArtifactSHA256: sha256Hex(archive)}
	home := filepath.Join(t.TempDir(), "delegation-home")
	_, err := InstallRuntime(context.Background(), home, result, func(context.Context, string) (string, error) {
		return "0.1.0-alpha.7", nil
	})
	if err == nil || !strings.Contains(err.Error(), "reports version") {
		t.Fatalf("InstallRuntime() error = %v", err)
	}
	target := filepath.Join(home, "bin", version, CurrentPlatform().OS+"-"+CurrentPlatform().Architecture)
	if _, statErr := os.Stat(target); !os.IsNotExist(statErr) {
		t.Fatalf("target exists after rejected runtime: %v", statErr)
	}
}

func TestExtractRuntimeArchiveRejectsUnsafeEntries(t *testing.T) {
	var compressed bytes.Buffer
	gzipWriter := gzip.NewWriter(&compressed)
	tarWriter := tar.NewWriter(gzipWriter)
	if err := tarWriter.WriteHeader(&tar.Header{Name: "../delegation", Typeflag: tar.TypeReg, Mode: 0o755}); err != nil {
		t.Fatal(err)
	}
	if err := tarWriter.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gzipWriter.Close(); err != nil {
		t.Fatal(err)
	}
	if err := extractRuntimeArchive(compressed.Bytes(), Platform{OS: "linux", Architecture: "amd64"}, t.TempDir()); err == nil {
		t.Fatal("extractRuntimeArchive accepted a traversal entry")
	}
}

func currentPlatformArchive(t *testing.T, binary, notice []byte) []byte {
	t.Helper()
	if CurrentPlatform().OS == "windows" {
		var output bytes.Buffer
		writer := zip.NewWriter(&output)
		for _, entry := range []struct {
			name string
			mode os.FileMode
			data []byte
		}{{"delegation.exe", 0o755, binary}, {releaseNoticeName, 0o644, notice}} {
			header := &zip.FileHeader{Name: entry.name, Method: zip.Deflate}
			header.SetMode(entry.mode)
			destination, err := writer.CreateHeader(header)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := destination.Write(entry.data); err != nil {
				t.Fatal(err)
			}
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
		return output.Bytes()
	}
	var output bytes.Buffer
	gzipWriter := gzip.NewWriter(&output)
	tarWriter := tar.NewWriter(gzipWriter)
	for _, entry := range []struct {
		name string
		mode int64
		data []byte
	}{{runtimeBinaryName, 0o755, binary}, {releaseNoticeName, 0o644, notice}} {
		if err := tarWriter.WriteHeader(&tar.Header{
			Name: entry.name, Mode: entry.mode, Size: int64(len(entry.data)), Typeflag: tar.TypeReg,
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := tarWriter.Write(entry.data); err != nil {
			t.Fatal(err)
		}
	}
	if err := tarWriter.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gzipWriter.Close(); err != nil {
		t.Fatal(err)
	}
	return output.Bytes()
}

func mustArtifactName(t *testing.T, version string) string {
	t.Helper()
	name, err := artifactName(version, CurrentPlatform())
	if err != nil {
		t.Fatal(err)
	}
	return name
}
