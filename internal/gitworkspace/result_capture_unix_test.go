//go:build !windows

package gitworkspace

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/GhostFlying/delegation/internal/protocol"
)

func TestContentWarningsHandlesLargeGitPathLists(t *testing.T) {
	realRunner := testRunner(t)
	_, source, _ := createRemoteRepository(t, realRunner.Binary)
	if err := os.WriteFile(
		filepath.Join(source, ".gitattributes"), []byte("*.bin filter=lfs\n"), 0o600,
	); err != nil {
		t.Fatal(err)
	}
	pathList := bytes.Repeat([]byte("asset.bin\x00"), maximumOutput/len("asset.bin\x00")+1)
	if len(pathList) <= maximumOutput || len(pathList) > maximumGitPathOutput {
		t.Fatalf("large path list size = %d", len(pathList))
	}
	wrapperDirectory := t.TempDir()
	pathListFile := filepath.Join(wrapperDirectory, "paths")
	if err := os.WriteFile(pathListFile, pathList, 0o600); err != nil {
		t.Fatal(err)
	}
	wrapper := filepath.Join(wrapperDirectory, "git-wrapper")
	if err := os.WriteFile(wrapper, []byte(`#!/bin/sh
case " $* " in
  *" ls-files --format=%(objectmode) "*)
    printf '100644\n'
    exit 0
    ;;
  *" ls-files -z "*)
    exec cat "$DELEGATION_TEST_GIT_PATH_LIST"
    ;;
esac
exec "$DELEGATION_TEST_REAL_GIT" "$@"
`), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DELEGATION_TEST_REAL_GIT", realRunner.Binary)
	t.Setenv("DELEGATION_TEST_GIT_PATH_LIST", pathListFile)
	runner, err := NewRunner(wrapper)
	if err != nil {
		t.Fatal(err)
	}

	warnings, err := runner.contentWarnings(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{protocol.WorkspaceWarningLFSPayloadNotTransferred}
	if !slices.Equal(warnings, want) {
		t.Fatalf("large path-list warnings = %v, want %v", warnings, want)
	}
}

func TestCaptureResultDiscardsLargeDescendantObjectList(t *testing.T) {
	realRunner := testRunner(t)
	remote, source, _ := createManagedResultRepository(t, realRunner)
	base, err := realRunner.Inspect(context.Background(), source, remote)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "commit.txt"), []byte("commit\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitRun(t, realRunner.Binary, source, "add", "commit.txt")
	commitForTest(t, realRunner.Binary, source, "worker commit")

	const blockSize = 4096
	blockCount := maximumOutput/blockSize + 1
	objectListBytes := blockCount * blockSize
	if objectListBytes <= maximumOutput {
		t.Fatalf("test object list size = %d, want more than %d", objectListBytes, maximumOutput)
	}
	wrapperDirectory := t.TempDir()
	wrapper := filepath.Join(wrapperDirectory, "git-wrapper")
	marker := filepath.Join(wrapperDirectory, "large-object-list-invoked")
	if err := os.WriteFile(wrapper, []byte(`#!/bin/sh
case " $* " in
  *" rev-list --objects --missing=error --no-object-names "*)
    "$DELEGATION_TEST_REAL_GIT" "$@" || exit $?
    dd if=/dev/zero bs=4096 count="$DELEGATION_TEST_OBJECT_LIST_BLOCKS" 2>/dev/null || exit $?
    printf '%s\n' "$DELEGATION_TEST_OBJECT_LIST_BLOCKS" > "$DELEGATION_TEST_OBJECT_LIST_MARKER"
    exit 0
    ;;
esac
exec "$DELEGATION_TEST_REAL_GIT" "$@"
`), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DELEGATION_TEST_REAL_GIT", realRunner.Binary)
	t.Setenv("DELEGATION_TEST_OBJECT_LIST_BLOCKS", strconv.Itoa(blockCount))
	t.Setenv("DELEGATION_TEST_OBJECT_LIST_MARKER", marker)
	runner, err := NewRunner(wrapper)
	if err != nil {
		t.Fatal(err)
	}

	artifacts := filepath.Join(t.TempDir(), "result")
	capture, err := runner.CaptureResult(context.Background(), source, artifacts, base.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	if capture.Unchanged || capture.Bundle == nil {
		t.Fatalf("capture = %#v, want changed result with bundle", capture)
	}
	markerData, err := os.ReadFile(marker)
	if err != nil {
		t.Fatalf("large object-list command was not observed: %v", err)
	}
	if strings.TrimSpace(string(markerData)) != strconv.Itoa(blockCount) {
		t.Fatalf("large object-list marker = %q, want %d blocks", markerData, blockCount)
	}
}

func TestCaptureResultRejectsSnapshotDriftAndRemovesArtifacts(t *testing.T) {
	realRunner := testRunner(t)
	remote, source, _ := createManagedResultRepository(t, realRunner)
	base, err := realRunner.Inspect(context.Background(), source, remote)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "commit.txt"), []byte("commit\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitRun(t, realRunner.Binary, source, "add", "commit.txt")
	commitForTest(t, realRunner.Binary, source, "worker commit")

	wrapper := filepath.Join(t.TempDir(), "git-wrapper")
	if err := os.WriteFile(wrapper, []byte(`#!/bin/sh
"$DELEGATION_TEST_REAL_GIT" "$@"
status=$?
case " $* " in
  *" bundle create "*) printf 'drift\n' > "$DELEGATION_TEST_DRIFT_FILE" ;;
esac
exit "$status"
`), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DELEGATION_TEST_REAL_GIT", realRunner.Binary)
	t.Setenv("DELEGATION_TEST_DRIFT_FILE", filepath.Join(source, "nested", "hello.txt"))
	runner, err := NewRunner(wrapper)
	if err != nil {
		t.Fatal(err)
	}
	artifacts := filepath.Join(t.TempDir(), "result")
	_, err = runner.CaptureResult(context.Background(), source, artifacts, base.Manifest)
	if err == nil || !strings.Contains(err.Error(), "changed while") {
		t.Fatalf("CaptureResult() = %v, want snapshot drift rejection", err)
	}
	if _, statErr := os.Lstat(artifacts); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("drifted capture staging still exists: %v", statErr)
	}
}

func TestCaptureResultUsesResolvedArtifactParent(t *testing.T) {
	runner := testRunner(t)
	remote, source, _ := createManagedResultRepository(t, runner)
	base, err := runner.Inspect(context.Background(), source, remote)
	if err != nil {
		t.Fatal(err)
	}
	realParent := t.TempDir()
	alias := filepath.Join(t.TempDir(), "artifact-parent")
	if err := os.Symlink(realParent, alias); err != nil {
		t.Fatal(err)
	}
	capture, err := runner.CaptureResult(
		context.Background(), source, filepath.Join(alias, "result"), base.Manifest,
	)
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(realParent, "result")
	if capture.ArtifactDirectory != want {
		t.Fatalf("artifact directory = %q, want resolved path %q", capture.ArtifactDirectory, want)
	}
	if info, err := os.Stat(want); err != nil || !info.IsDir() {
		t.Fatalf("resolved artifact directory = %v, %v", info, err)
	}
}
