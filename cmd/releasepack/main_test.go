package main

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestArchivesAreDeterministicAndContainOneExecutable(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "input")
	if err := os.WriteFile(binary, []byte("test binary\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	for name, test := range map[string]struct {
		extension  string
		binaryName string
		write      func(string, string, string) error
		read       func(*testing.T, string) (string, []byte, os.FileMode)
	}{
		"tar gzip": {extension: ".tar.gz", binaryName: "delegation", write: writeTarGzip, read: readTarGzip},
		"zip":      {extension: ".zip", binaryName: "delegation.exe", write: writeZip, read: readZip},
	} {
		t.Run(name, func(t *testing.T) {
			first := filepath.Join(t.TempDir(), "first"+test.extension)
			second := filepath.Join(t.TempDir(), "second"+test.extension)
			if err := test.write(first, binary, test.binaryName); err != nil {
				t.Fatal(err)
			}
			if err := test.write(second, binary, test.binaryName); err != nil {
				t.Fatal(err)
			}
			firstBytes, err := os.ReadFile(first)
			if err != nil {
				t.Fatal(err)
			}
			secondBytes, err := os.ReadFile(second)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(firstBytes, secondBytes) {
				t.Fatal("archive bytes differ across identical builds")
			}
			entryName, content, mode := test.read(t, first)
			if entryName != test.binaryName || string(content) != "test binary\n" || mode.Perm() != 0o755 {
				t.Fatalf("archive entry = %q, %q, %o", entryName, content, mode.Perm())
			}
		})
	}
}

func TestTargetEnvironmentReplacesBuildSelectors(t *testing.T) {
	got := targetEnvironment(
		[]string{
			"PATH=/bin",
			"GOOS=old",
			"GOARCH=old",
			"CGO_ENABLED=1",
			"GoAmD64=v4",
			"GOARM64=v9.5",
			"GOENV=/tmp/ambient",
			"GOEXPERIMENT=fieldtrack",
			"GOFLAGS=-tags=ambient",
			"GOROOT=/tmp/other-go",
			"GOWORK=/tmp/go.work",
		},
		target{os: "windows", arch: "arm64"},
	)
	want := []string{
		"PATH=/bin",
		"CGO_ENABLED=0",
		"GO111MODULE=on",
		"GOENV=off",
		"GOEXPERIMENT=none",
		"GOFIPS140=off",
		"GOFLAGS=",
		"GOTOOLCHAIN=local",
		"GOWORK=off",
		"GOOS=windows",
		"GOARCH=arm64",
		"GOARM64=v8.0",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("targetEnvironment() = %q, want %q", got, want)
	}
}

func TestChecksumManifestIsSorted(t *testing.T) {
	path := filepath.Join(t.TempDir(), "checksums")
	if err := writeChecksumManifest(path, map[string]string{"z.zip": "two", "a.tar.gz": "one"}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(data), "one  a.tar.gz\ntwo  z.zip\n"; got != want {
		t.Fatalf("checksum manifest = %q, want %q", got, want)
	}
}

func TestPackageReleaseRefusesExistingOutput(t *testing.T) {
	output := t.TempDir()
	err := packageRelease(t.TempDir(), output)
	if err == nil || !strings.Contains(err.Error(), "output already exists") {
		t.Fatalf("packageRelease() error = %v, want existing-output failure", err)
	}
}

func TestCommitReleaseDirectoryDoesNotReplaceConcurrentDestination(t *testing.T) {
	root := t.TempDir()
	staging := filepath.Join(root, "staging")
	destination := filepath.Join(root, "destination")
	if err := os.Mkdir(staging, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(destination, 0o755); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(destination)
	if err != nil {
		t.Fatal(err)
	}
	if err := commitReleaseDirectory(staging, destination); err == nil {
		t.Fatal("commitReleaseDirectory() replaced a concurrent destination")
	}
	after, err := os.Stat(destination)
	if err != nil || !os.SameFile(before, after) {
		t.Fatalf("concurrent destination identity changed: %v", err)
	}
	if _, err := os.Stat(staging); err != nil {
		t.Fatalf("staging directory was moved after failed commit: %v", err)
	}
}

func TestCommitReleaseDirectoryMovesToUnusedDestination(t *testing.T) {
	root := t.TempDir()
	staging := filepath.Join(root, "staging")
	destination := filepath.Join(root, "destination")
	if err := os.Mkdir(staging, 0o755); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(staging, "marker")
	if err := os.WriteFile(marker, []byte("moved"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := commitReleaseDirectory(staging, destination); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(destination, "marker"))
	if err != nil || string(data) != "moved" {
		t.Fatalf("committed marker = %q, %v", data, err)
	}
	if _, err := os.Stat(staging); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("staging directory still exists after commit: %v", err)
	}
}

func readTarGzip(t *testing.T, path string) (string, []byte, os.FileMode) {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		t.Fatal(err)
	}
	defer gzipReader.Close()
	reader := tar.NewReader(gzipReader)
	header, err := reader.Next()
	if err != nil {
		t.Fatal(err)
	}
	content, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reader.Next(); err != io.EOF {
		t.Fatalf("second tar entry error = %v, want EOF", err)
	}
	return header.Name, content, header.FileInfo().Mode()
}

func readZip(t *testing.T, path string) (string, []byte, os.FileMode) {
	t.Helper()
	reader, err := zip.OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if len(reader.File) != 1 {
		t.Fatalf("zip entries = %d, want 1", len(reader.File))
	}
	entry := reader.File[0]
	opened, err := entry.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	content, err := io.ReadAll(opened)
	if err != nil {
		t.Fatal(err)
	}
	return entry.Name, content, entry.Mode()
}

func TestVersionPattern(t *testing.T) {
	for _, version := range []string{"0.1.0", "0.1.0-alpha.1", "1.2.3-alpha-beta", "1.2.3-rc.1+build.01"} {
		if !versionPattern.MatchString(version) {
			t.Fatalf("versionPattern rejected %q", version)
		}
	}
	for _, version := range []string{"latest", "1.2", "01.2.3", "1.02.3", "1.2.03", "1.2.3-01", "1.2.3/other", strings.Repeat("1", 200)} {
		if versionPattern.MatchString(version) {
			t.Fatalf("versionPattern accepted %q", version)
		}
	}
}
