//go:build !windows

package gitworkspace

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestInspectApplySourceRejectsExecutableConfigBeforeItCanRun(t *testing.T) {
	for _, test := range []struct {
		name      string
		configure func(*testing.T, Runner, string, string)
	}{
		{
			name: "fsmonitor",
			configure: func(t *testing.T, runner Runner, root, script string) {
				gitRun(t, runner.Binary, root, "config", "core.fsmonitor", script)
			},
		},
		{
			name: "clean filter",
			configure: func(t *testing.T, runner Runner, root, script string) {
				if err := os.WriteFile(
					filepath.Join(root, ".gitattributes"), []byte("nested/hello.txt filter=hostile\n"), 0o600,
				); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(
					filepath.Join(root, "nested", "hello.txt"), []byte("force filter evaluation\n"), 0o600,
				); err != nil {
					t.Fatal(err)
				}
				gitRun(t, runner.Binary, root, "config", "filter.hostile.clean", script)
			},
		},
		{
			name: "process filter",
			configure: func(t *testing.T, runner Runner, root, script string) {
				if err := os.WriteFile(
					filepath.Join(root, ".gitattributes"), []byte("nested/hello.txt filter=hostile\n"), 0o600,
				); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(
					filepath.Join(root, "nested", "hello.txt"), []byte("force filter evaluation\n"), 0o600,
				); err != nil {
					t.Fatal(err)
				}
				gitRun(t, runner.Binary, root, "config", "filter.hostile.process", script)
			},
		},
		{
			name: "smudge filter",
			configure: func(t *testing.T, runner Runner, root, script string) {
				if err := os.WriteFile(
					filepath.Join(root, ".gitattributes"), []byte("nested/hello.txt filter=hostile\n"), 0o600,
				); err != nil {
					t.Fatal(err)
				}
				gitRun(t, runner.Binary, root, "config", "filter.hostile.smudge", script)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			runner := testRunner(t)
			remote, root, _ := createRemoteRepository(t, runner.Binary)
			sentinel := filepath.Join(t.TempDir(), "executed")
			script := filepath.Join(t.TempDir(), "hostile.sh")
			content := "#!/bin/sh\nprintf executed > '" + strings.ReplaceAll(sentinel, "'", "'\\''") + "'\nexit 1\n"
			if err := os.WriteFile(script, []byte(content), 0o700); err != nil {
				t.Fatal(err)
			}
			test.configure(t, runner, root, script)
			_, err := runner.InspectApplySource(context.Background(), root)
			if err == nil || !strings.Contains(err.Error(), "apply-unsafe") {
				t.Fatalf("InspectApplySource() error = %v", err)
			}
			if _, statErr := os.Lstat(sentinel); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("hostile %s executed before rejection: %v", test.name, statErr)
			}
			if remote == "" {
				t.Fatal("empty remote fixture")
			}
		})
	}
}

func TestInspectApplySourceRejectsExecutableWorktreeConfigBeforeItCanRun(t *testing.T) {
	runner := testRunner(t)
	_, root, _ := createRemoteRepository(t, runner.Binary)
	linked := filepath.Join(t.TempDir(), "linked")
	gitRun(t, runner.Binary, root, "worktree", "add", "-b", "apply-linked", linked)
	gitRun(t, runner.Binary, root, "config", "extensions.worktreeConfig", "true")
	sentinel := filepath.Join(t.TempDir(), "executed")
	script := filepath.Join(t.TempDir(), "hostile.sh")
	content := "#!/bin/sh\nprintf executed > '" + strings.ReplaceAll(sentinel, "'", "'\\''") + "'\nexit 1\n"
	if err := os.WriteFile(script, []byte(content), 0o700); err != nil {
		t.Fatal(err)
	}
	gitRun(t, runner.Binary, linked, "config", "--worktree", "filter.hostile.smudge", script)
	if _, err := runner.InspectApplySource(context.Background(), linked); err == nil ||
		!strings.Contains(err.Error(), "apply-unsafe") {
		t.Fatalf("InspectApplySource() error = %v", err)
	}
	if _, err := os.Lstat(sentinel); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("hostile worktree smudge filter executed before rejection: %v", err)
	}
}
