package main

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

func TestArchivesAreDeterministicAndContainExecutableAndNotice(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "input")
	if err := os.WriteFile(binary, []byte("test binary\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	notice := []byte("test notices\n")
	for name, test := range map[string]struct {
		extension  string
		binaryName string
		write      func(string, string, string, []byte) error
		read       func(*testing.T, string) []archiveTestEntry
	}{
		"tar gzip": {extension: ".tar.gz", binaryName: "delegation", write: writeTarGzip, read: readTarGzip},
		"zip":      {extension: ".zip", binaryName: "delegation.exe", write: writeZip, read: readZip},
	} {
		t.Run(name, func(t *testing.T) {
			first := filepath.Join(t.TempDir(), "first"+test.extension)
			second := filepath.Join(t.TempDir(), "second"+test.extension)
			if err := test.write(first, binary, test.binaryName, notice); err != nil {
				t.Fatal(err)
			}
			if err := test.write(second, binary, test.binaryName, notice); err != nil {
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
			entries := test.read(t, first)
			want := []archiveTestEntry{
				{name: test.binaryName, content: []byte("test binary\n"), mode: 0o755},
				{name: releaseNoticeName, content: notice, mode: 0o644},
			}
			if !reflect.DeepEqual(entries, want) {
				t.Fatalf("archive entries = %#v, want %#v", entries, want)
			}
		})
	}
}

func TestRuntimeArchiveVerifierRejectsHostileEntries(t *testing.T) {
	notice := []byte("audited notice\n")
	for name, test := range map[string]struct {
		target target
		write  func(*testing.T, string, []hostileArchiveEntry)
	}{
		"tar gzip": {
			target: target{os: "linux", arch: "amd64", archive: "tar.gz"},
			write:  writeHostileTarGzip,
		},
		"zip": {
			target: target{os: "windows", arch: "amd64", archive: "zip"},
			write:  writeHostileZip,
		},
	} {
		t.Run(name, func(t *testing.T) {
			binaryName := test.target.binaryName()
			valid := []hostileArchiveEntry{
				{name: binaryName, mode: 0o755, content: []byte("binary")},
				{name: releaseNoticeName, mode: 0o644, content: notice},
			}
			mutations := []struct {
				name    string
				entries []hostileArchiveEntry
			}{
				{name: "missing notice", entries: valid[:1]},
				{name: "duplicate notice", entries: append(append([]hostileArchiveEntry{}, valid...), valid[1])},
				{name: "linked notice", entries: []hostileArchiveEntry{valid[0], {
					name: releaseNoticeName, mode: 0o644, typeflag: tar.TypeSymlink, linkname: "elsewhere",
				}}},
				{name: "renamed notice", entries: []hostileArchiveEntry{valid[0], {
					name: "NOTICES.txt", mode: 0o644, content: notice,
				}}},
				{name: "reordered notice", entries: []hostileArchiveEntry{valid[1], valid[0]}},
				{name: "extra entry", entries: append(append([]hostileArchiveEntry{}, valid...), hostileArchiveEntry{
					name: "extra", mode: 0o644, content: []byte("extra"),
				})},
				{name: "wrong notice bytes", entries: []hostileArchiveEntry{valid[0], {
					name: releaseNoticeName, mode: 0o644, content: []byte("other"),
				}}},
			}
			for name, mode := range map[string]os.FileMode{
				"setuid": os.ModeSetuid,
				"setgid": os.ModeSetgid,
				"sticky": os.ModeSticky,
			} {
				mutations = append(
					mutations,
					struct {
						name    string
						entries []hostileArchiveEntry
					}{
						name: "executable " + name,
						entries: []hostileArchiveEntry{
							{name: binaryName, mode: 0o755 | mode, content: []byte("binary")},
							valid[1],
						},
					},
					struct {
						name    string
						entries []hostileArchiveEntry
					}{
						name: "notice " + name,
						entries: []hostileArchiveEntry{
							valid[0],
							{name: releaseNoticeName, mode: 0o644 | mode, content: notice},
						},
					},
				)
			}
			for _, mutation := range mutations {
				t.Run(mutation.name, func(t *testing.T) {
					path := filepath.Join(t.TempDir(), "hostile."+test.target.archive)
					test.write(t, path, mutation.entries)
					if err := verifyRuntimeArchive(path, test.target, notice); err == nil {
						t.Fatal("verifyRuntimeArchive() accepted hostile archive")
					}
				})
			}
		})
	}
}

func TestRuntimeArchiveVerifierRejectsNoncanonicalContainerMetadata(t *testing.T) {
	notice := []byte("audited notice\n")
	t.Run("zip archive comment", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "comment.zip")
		writeHostileZipArchive(t, path, []hostileArchiveEntry{
			{name: "delegation.exe", mode: 0o755, content: []byte("binary")},
			{name: releaseNoticeName, mode: 0o644, content: notice},
		}, "unexpected")
		target := target{os: "windows", arch: "amd64", archive: "zip"}
		if err := verifyRuntimeArchive(path, target, notice); err == nil {
			t.Fatal("verifyRuntimeArchive() accepted a ZIP archive comment")
		}
	})

	for name, mutation := range map[string]struct {
		offset int
		value  byte
	}{
		"flags": {offset: 3, value: 0x04},
		"mtime": {offset: 4, value: 0x01},
		"xfl":   {offset: 8, value: 0x02},
		"os":    {offset: 9, value: 0x03},
	} {
		t.Run("gzip "+name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "metadata.tar.gz")
			writeHostileTarGzip(t, path, []hostileArchiveEntry{
				{name: "delegation", mode: 0o755, content: []byte("binary")},
				{name: releaseNoticeName, mode: 0o644, content: notice},
			})
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			data[mutation.offset] = mutation.value
			if err := os.WriteFile(path, data, 0o644); err != nil {
				t.Fatal(err)
			}
			target := target{os: "linux", arch: "amd64", archive: "tar.gz"}
			if err := verifyRuntimeArchive(path, target, notice); err == nil {
				t.Fatalf("verifyRuntimeArchive() accepted mutated gzip %s", name)
			}
		})
	}
}

type hostileArchiveEntry struct {
	name     string
	mode     os.FileMode
	content  []byte
	typeflag byte
	linkname string
}

func writeHostileTarGzip(t *testing.T, path string, entries []hostileArchiveEntry) {
	t.Helper()
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	gzipWriter := gzip.NewWriter(file)
	gzipWriter.Header.ModTime = time.Unix(0, 0).UTC()
	tarWriter := tar.NewWriter(gzipWriter)
	for _, entry := range entries {
		typeflag := entry.typeflag
		if typeflag == 0 {
			typeflag = tar.TypeReg
		}
		header := &tar.Header{
			Name: entry.name, Mode: unixMode(entry.mode), Size: int64(len(entry.content)),
			Typeflag: typeflag, Linkname: entry.linkname, Format: tar.FormatUSTAR,
			ModTime: time.Unix(0, 0).UTC(),
		}
		if typeflag != tar.TypeReg {
			header.Size = 0
		}
		if err := tarWriter.WriteHeader(header); err != nil {
			t.Fatal(err)
		}
		if _, err := tarWriter.Write(entry.content); err != nil {
			t.Fatal(err)
		}
	}
	if err := errors.Join(tarWriter.Close(), gzipWriter.Close(), file.Close()); err != nil {
		t.Fatal(err)
	}
}

func unixMode(mode os.FileMode) int64 {
	unixMode := int64(mode.Perm())
	if mode&os.ModeSetuid != 0 {
		unixMode |= 0o4000
	}
	if mode&os.ModeSetgid != 0 {
		unixMode |= 0o2000
	}
	if mode&os.ModeSticky != 0 {
		unixMode |= 0o1000
	}
	return unixMode
}

func writeHostileZip(t *testing.T, path string, entries []hostileArchiveEntry) {
	t.Helper()
	writeHostileZipArchive(t, path, entries, "")
}

func writeHostileZipArchive(t *testing.T, path string, entries []hostileArchiveEntry, comment string) {
	t.Helper()
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	zipWriter := zip.NewWriter(file)
	if err := zipWriter.SetComment(comment); err != nil {
		t.Fatal(err)
	}
	for _, archiveEntry := range entries {
		header := &zip.FileHeader{Name: archiveEntry.name, Method: zip.Deflate}
		header.SetMode(archiveEntry.mode)
		if archiveEntry.typeflag == tar.TypeSymlink {
			header.SetMode(os.ModeSymlink | archiveEntry.mode)
			archiveEntry.content = []byte(archiveEntry.linkname)
		}
		header.SetModTime(time.Date(1980, time.January, 1, 0, 0, 0, 0, time.UTC))
		entry, err := zipWriter.CreateHeader(header)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := entry.Write(archiveEntry.content); err != nil {
			t.Fatal(err)
		}
	}
	if err := errors.Join(zipWriter.Close(), file.Close()); err != nil {
		t.Fatal(err)
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

func TestBuildTargetUsesExplicitPrivacyTag(t *testing.T) {
	got := buildTargetArguments("delegation")
	want := []string{
		"build",
		"-tags=ts_omit_logtail",
		"-trimpath",
		"-buildvcs=false",
		"-ldflags=-buildid=",
		"-o",
		"delegation",
		"./cmd/delegation",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("buildTargetArguments() = %q, want %q", got, want)
	}
}

func TestReleaseNoticeCoversProductionLinkedModuleUnion(t *testing.T) {
	root := filepath.Join("..", "..")
	notice, err := readReleaseNotice(root)
	if err != nil {
		t.Fatal(err)
	}
	const componentPrefix = "Component: "
	var noticeComponents []string
	for _, line := range strings.Split(string(notice), "\n") {
		if component, ok := strings.CutPrefix(line, componentPrefix); ok {
			noticeComponents = append(noticeComponents, component)
		}
	}
	var linkedComponents []string
	seen := make(map[string]struct{})
	for _, target := range releaseTargets {
		command := exec.Command(
			"go",
			"list",
			"-buildvcs=false",
			"-tags="+productionBuildTags,
			"-deps",
			"-f",
			"{{if and .Module .Module.Version}}{{.Module.Path}} {{.Module.Version}}{{end}}",
			"./cmd/delegation",
		)
		command.Dir = root
		command.Env = targetEnvironment(os.Environ(), target)
		output, stderr, err := commandStdout(command)
		if err != nil {
			t.Fatalf("list linked modules for %s: %v: %s", target.id(), err, stderr)
		}
		for _, component := range strings.FieldsFunc(string(output), func(character rune) bool {
			return character == '\n' || character == '\r'
		}) {
			if _, ok := seen[component]; ok {
				continue
			}
			seen[component] = struct{}{}
			linkedComponents = append(linkedComponents, component)
		}
	}
	sort.Strings(linkedComponents)
	want := append([]string{"Go toolchain " + requiredGoVersion}, linkedComponents...)
	if !reflect.DeepEqual(noticeComponents, want) {
		t.Fatalf("release notice components differ from linked module union\n got: %q\nwant: %q", noticeComponents, want)
	}
}

func TestReleaseNoticeModuleListingIgnoresDownloadProgress(t *testing.T) {
	const helperEnvironment = "DELEGATION_RELEASEPACK_MODULE_LIST_HELPER"
	if os.Getenv(helperEnvironment) == "1" {
		_, _ = io.WriteString(os.Stdout, "example.com/linked v1.2.3\n")
		_, _ = io.WriteString(os.Stderr, "go: downloading example.com/download v4.5.6\n")
		os.Exit(0)
	}
	command := exec.Command(
		os.Args[0],
		"-test.run=^TestReleaseNoticeModuleListingIgnoresDownloadProgress$",
	)
	command.Env = append(os.Environ(), helperEnvironment+"=1")
	output, stderr, err := commandStdout(command)
	if err != nil {
		t.Fatalf("run module list helper: %v: %s", err, stderr)
	}
	if got, want := string(output), "example.com/linked v1.2.3\n"; got != want {
		t.Fatalf("module list stdout = %q, want %q", got, want)
	}
	if got, want := stderr, "go: downloading example.com/download v4.5.6\n"; got != want {
		t.Fatalf("module list stderr = %q, want %q", got, want)
	}
}

func commandStdout(command *exec.Cmd) ([]byte, string, error) {
	var stderr strings.Builder
	command.Stderr = &stderr
	output, err := command.Output()
	return output, stderr.String(), err
}

func TestPackageTargetAndReleaseVerifierRequireNotice(t *testing.T) {
	root := filepath.Join("..", "..")
	notice := testReleaseNotice(t)
	binary := filepath.Join(t.TempDir(), "delegation.exe")
	if err := os.WriteFile(binary, []byte("signed binary"), 0o755); err != nil {
		t.Fatal(err)
	}
	part := filepath.Join(t.TempDir(), "part")
	if err := runPackageTarget([]string{
		"--repo", root,
		"--target", "windows-amd64",
		"--binary", binary,
		"--out", part,
	}, io.Discard); err != nil {
		t.Fatal(err)
	}
	version, err := readVersion(root)
	if err != nil {
		t.Fatal(err)
	}
	windowsTarget := target{os: "windows", arch: "amd64", archive: "zip"}
	if err := verifyRuntimeArchive(
		filepath.Join(part, windowsTarget.archiveName(version)),
		windowsTarget,
		notice,
	); err != nil {
		t.Fatal(err)
	}

	releaseRoot := filepath.Join(t.TempDir(), "release")
	if err := os.Mkdir(releaseRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	checksums := make(map[string]string, len(releaseTargets))
	for _, releaseTarget := range releaseTargets {
		name := releaseTarget.archiveName(version)
		path := filepath.Join(releaseRoot, name)
		if releaseTarget.archive == "zip" {
			err = writeZip(path, binary, releaseTarget.binaryName(), notice)
		} else {
			err = writeTarGzip(path, binary, releaseTarget.binaryName(), notice)
		}
		if err != nil {
			t.Fatal(err)
		}
		checksums[name], err = fileSHA256(path)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := writeChecksumManifest(
		filepath.Join(releaseRoot, candidateManifestName),
		checksums,
	); err != nil {
		t.Fatal(err)
	}
	if err := verifyRelease(root, releaseRoot); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(releaseRoot, "extra"), []byte("extra"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := verifyRelease(root, releaseRoot); err == nil {
		t.Fatal("verifyRelease() accepted an extra release file")
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

type archiveTestEntry struct {
	name    string
	content []byte
	mode    os.FileMode
}

func readTarGzip(t *testing.T, path string) []archiveTestEntry {
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
	var entries []archiveTestEntry
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		content, err := io.ReadAll(reader)
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, archiveTestEntry{
			name: header.Name, content: content, mode: header.FileInfo().Mode().Perm(),
		})
	}
	return entries
}

func readZip(t *testing.T, path string) []archiveTestEntry {
	t.Helper()
	reader, err := zip.OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	var entries []archiveTestEntry
	for _, entry := range reader.File {
		opened, err := entry.Open()
		if err != nil {
			t.Fatal(err)
		}
		content, readErr := io.ReadAll(opened)
		closeErr := opened.Close()
		if err := errors.Join(readErr, closeErr); err != nil {
			t.Fatal(err)
		}
		entries = append(entries, archiveTestEntry{
			name: entry.Name, content: content, mode: entry.Mode().Perm(),
		})
	}
	return entries
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
