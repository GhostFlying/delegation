//go:build !windows

package gitworkspace

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestCaptureResultRejectsSymlinkedObjectDirectory(t *testing.T) {
	runner := testRunner(t)
	remote, source, _ := createManagedResultRepository(t, runner)
	base, err := runner.Inspect(context.Background(), source, remote)
	if err != nil {
		t.Fatal(err)
	}
	objects := filepath.Join(source, ".git", "objects")
	realObjects := filepath.Join(source, ".git", "objects-real")
	if err := os.Rename(objects, realObjects); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("objects-real", objects); err != nil {
		t.Skipf("creating an object-directory symlink is unavailable: %v", err)
	}
	assertCaptureResultRejectedAndCleaned(t, runner, source, base.Manifest, "objects must be a real directory")
}

func TestCaptureResultRejectsSymlinkedLooseObject(t *testing.T) {
	runner := testRunner(t)
	remote, source, head := createManagedResultRepository(t, runner)
	base, err := runner.Inspect(context.Background(), source, remote)
	if err != nil {
		t.Fatal(err)
	}
	object := filepath.Join(source, ".git", "objects", head[:2], head[2:])
	realObject := object + ".real"
	if err := os.Rename(object, realObject); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Base(realObject), object); err != nil {
		t.Skipf("creating a loose-object symlink is unavailable: %v", err)
	}
	assertCaptureResultRejectedAndCleaned(
		t, runner, source, base.Manifest, "must contain only real regular files",
	)
}

func TestCaptureResultRejectsFileModeConfigDriftBeforeLosingModeChange(t *testing.T) {
	runner := testRunner(t)
	remote, source, _ := createManagedResultRepository(t, runner)
	script := filepath.Join(source, "script.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	gitRun(t, runner.Binary, source, "add", "script.sh")
	commitForTest(t, runner.Binary, source, "add non-executable script")
	base, err := runner.Inspect(context.Background(), source, remote)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(script, 0o755); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(script)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm()&0o100 == 0 {
		t.Skip("the test filesystem does not preserve executable mode")
	}
	gitRun(t, runner.Binary, source, "config", "--local", "core.fileMode", "false")
	assertCaptureResultRejectedAndCleaned(t, runner, source, base.Manifest, "config drifted at core.fileMode")
}
