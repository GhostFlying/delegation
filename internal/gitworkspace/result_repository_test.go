package gitworkspace

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestCaptureResultRejectsUnconfinedGitMetadata(t *testing.T) {
	tests := []struct {
		name      string
		wantError string
		mutate    func(*testing.T, string, string)
	}{
		{
			name:      "separate_git_directory",
			wantError: "standalone real .git directory",
			mutate: func(t *testing.T, repositoryPath, _ string) {
				t.Helper()
				gitDirectory := filepath.Join(repositoryPath, ".git")
				separate := filepath.Join(t.TempDir(), "separate.git")
				if err := os.Rename(gitDirectory, separate); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(gitDirectory, []byte("gitdir: "+separate+"\n"), 0o600); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:      "linked_worktree_marker",
			wantError: "must not contain gitdir",
			mutate: func(t *testing.T, repositoryPath, _ string) {
				writeResultGitMetadata(t, repositoryPath, "gitdir", "outside\n")
			},
		},
		{
			name:      "common_directory",
			wantError: "must not contain commondir",
			mutate: func(t *testing.T, repositoryPath, _ string) {
				writeResultGitMetadata(t, repositoryPath, "commondir", "outside\n")
			},
		},
		{
			name:      "shallow_history",
			wantError: "must not contain shallow",
			mutate: func(t *testing.T, repositoryPath, head string) {
				writeResultGitMetadata(t, repositoryPath, "shallow", head+"\n")
			},
		},
		{
			name:      "legacy_graft",
			wantError: "must not contain info/grafts",
			mutate: func(t *testing.T, repositoryPath, head string) {
				writeResultGitMetadata(t, repositoryPath, "info/grafts", head+"\n")
			},
		},
		{
			name:      "object_alternate",
			wantError: "must not contain objects/info/alternates",
			mutate: func(t *testing.T, repositoryPath, _ string) {
				writeResultGitMetadata(
					t, repositoryPath, "objects/info/alternates", filepath.Join(t.TempDir(), "objects")+"\n",
				)
			},
		},
		{
			name:      "http_object_alternate",
			wantError: "must not contain objects/info/http-alternates",
			mutate: func(t *testing.T, repositoryPath, _ string) {
				writeResultGitMetadata(
					t, repositoryPath, "objects/info/http-alternates", "https://example.invalid/objects\n",
				)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runner := testRunner(t)
			remote, source, head := createManagedResultRepository(t, runner)
			base, err := runner.Inspect(context.Background(), source, remote)
			if err != nil {
				t.Fatal(err)
			}
			test.mutate(t, source, head)
			assertCaptureResultRejectedAndCleaned(t, runner, source, base.Manifest, test.wantError)
		})
	}
}

func TestCaptureResultRejectsManagedCheckoutConfigDrift(t *testing.T) {
	for _, test := range []struct {
		key   string
		value string
	}{
		{key: "core.hooksPath", value: "worker-hooks"},
		{key: "core.autocrlf", value: "true"},
		{key: "core.eol", value: "crlf"},
		{key: "core.excludesFile", value: "worker-excludes"},
		{key: "core.attributesFile", value: "worker-attributes"},
	} {
		t.Run(test.key, func(t *testing.T) {
			runner := testRunner(t)
			remote, source, _ := createManagedResultRepository(t, runner)
			base, err := runner.Inspect(context.Background(), source, remote)
			if err != nil {
				t.Fatal(err)
			}
			gitRun(t, runner.Binary, source, "config", "--local", test.key, test.value)
			assertCaptureResultRejectedAndCleaned(t, runner, source, base.Manifest, "config drifted at "+test.key)
		})
	}
}

func TestCaptureResultRejectsManagedCheckoutConfigIndirection(t *testing.T) {
	tests := []struct {
		name      string
		wantError string
		mutate    func(*testing.T, Runner, string)
	}{
		{
			name:      "include",
			wantError: "must not include external config files",
			mutate: func(t *testing.T, runner Runner, repositoryPath string) {
				t.Helper()
				included := filepath.Join(t.TempDir(), "included.config")
				if err := os.WriteFile(included, nil, 0o600); err != nil {
					t.Fatal(err)
				}
				gitRun(t, runner.Binary, repositoryPath, "config", "--local", "include.path", included)
			},
		},
		{
			name:      "worktree_override",
			wantError: "worktree config must not override core.eol",
			mutate: func(t *testing.T, runner Runner, repositoryPath string) {
				t.Helper()
				gitRun(t, runner.Binary, repositoryPath, "config", "--local", "extensions.worktreeConfig", "true")
				gitRun(t, runner.Binary, repositoryPath, "config", "--worktree", "core.eol", "lf")
			},
		},
		{
			name:      "partial_clone_fetch",
			wantError: "must not use partial clone object fetching",
			mutate: func(t *testing.T, runner Runner, repositoryPath string) {
				t.Helper()
				gitRun(t, runner.Binary, repositoryPath, "config", "--local", "remote.origin.promisor", "true")
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runner := testRunner(t)
			remote, source, _ := createManagedResultRepository(t, runner)
			base, err := runner.Inspect(context.Background(), source, remote)
			if err != nil {
				t.Fatal(err)
			}
			test.mutate(t, runner, source)
			assertCaptureResultRejectedAndCleaned(t, runner, source, base.Manifest, test.wantError)
		})
	}
}

func writeResultGitMetadata(t *testing.T, repositoryPath, name, contents string) {
	t.Helper()
	path := filepath.Join(repositoryPath, ".git", filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
}
