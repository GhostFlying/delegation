package clilaunch

import (
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
)

func TestResolveCanonicalizesStructuredLaunch(t *testing.T) {
	root := t.TempDir()
	launcher := filepath.Join(root, "launcher")
	runtimeExecutable := filepath.Join(root, "runtime")
	for _, path := range []string{launcher, runtimeExecutable} {
		if err := os.WriteFile(path, []byte("test"), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	launcherLink := filepath.Join(root, "launcher-link")
	runtimeLink := filepath.Join(root, "runtime-link")
	if err := os.Symlink(launcher, launcherLink); err != nil {
		t.Skipf("symbolic links are unavailable: %v", err)
	}
	if err := os.Symlink(runtimeExecutable, runtimeLink); err != nil {
		t.Fatal(err)
	}
	prefix := []string{"run", "--", "traex", "-p", "ultra"}

	resolved, err := Resolve(Spec{
		Executable:      launcherLink,
		PrefixArguments: prefix,
	})
	if err != nil {
		t.Fatal(err)
	}
	resolvedRuntime, err := ResolveRuntimeExecutable(runtimeLink)
	if err != nil {
		t.Fatal(err)
	}
	wantLauncher, err := filepath.EvalSymlinks(launcher)
	if err != nil {
		t.Fatal(err)
	}
	wantRuntime, err := filepath.EvalSymlinks(runtimeExecutable)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Executable != wantLauncher || resolvedRuntime != wantRuntime ||
		!slices.Equal(resolved.PrefixArguments, prefix) {
		t.Fatalf("resolved launch = %#v", resolved)
	}
	prefix[0] = "changed"
	if resolved.PrefixArguments[0] != "run" {
		t.Fatalf("resolved launch retained caller-owned arguments: %#v", resolved)
	}
}

func TestResolveRejectsUnsafeStructuredLaunch(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		spec Spec
		want string
	}{
		{
			name: "relative launcher",
			spec: Spec{Executable: "warmpool"},
			want: "launcher executable must be an absolute path",
		},
		{
			name: "too many prefix arguments",
			spec: Spec{
				Executable:      executable,
				PrefixArguments: make([]string, MaximumPrefixArguments+1),
			},
			want: "at most",
		},
		{
			name: "oversized prefix arguments",
			spec: Spec{
				Executable:      executable,
				PrefixArguments: []string{strings.Repeat("x", MaximumPrefixBytes+1)},
			},
			want: "bytes",
		},
		{
			name: "NUL prefix argument",
			spec: Spec{
				Executable:      executable,
				PrefixArguments: []string{"run\x00shell"},
			},
			want: "must not contain NUL",
		},
		{
			name: "directory launcher",
			spec: Spec{Executable: t.TempDir()},
			want: "must be a regular file",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := Resolve(test.spec); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Resolve() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestResolveRuntimeExecutableRejectsRelativePath(t *testing.T) {
	if _, err := ResolveRuntimeExecutable("traex"); err == nil ||
		!strings.Contains(err.Error(), "must be an absolute path") {
		t.Fatalf("ResolveRuntimeExecutable() error = %v", err)
	}
}

func TestResolveRejectsNonExecutableFileOnUnix(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows executable permission is not represented by Unix mode bits")
	}
	path := filepath.Join(t.TempDir(), "launcher")
	if err := os.WriteFile(path, []byte("test"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Resolve(Spec{Executable: path}); err == nil ||
		!strings.Contains(err.Error(), "must be executable") {
		t.Fatalf("Resolve() error = %v", err)
	}
}
