//go:build !linux

package workerhost

import "testing"

func assertCodexRuntimeFilesystemPermission(
	t *testing.T,
	filesystem map[string]any,
	runtimeExecutable string,
) {
	t.Helper()
	if _, found := filesystem[runtimeExecutable]; found {
		t.Fatalf("managed non-Linux profile grants the CLI runtime executable: %#v", filesystem)
	}
}
