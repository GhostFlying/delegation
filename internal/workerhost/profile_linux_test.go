//go:build linux

package workerhost

import "testing"

func assertCodexRuntimeFilesystemPermission(
	t *testing.T,
	filesystem map[string]any,
	runtimeExecutable string,
) {
	t.Helper()
	if filesystem[runtimeExecutable] != "read" {
		t.Fatalf("managed Linux profile does not grant the exact CLI runtime executable: %#v", filesystem)
	}
}
