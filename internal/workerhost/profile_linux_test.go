//go:build linux

package workerhost

import "testing"

func assertCodexRuntimeFilesystemPermission(
	t *testing.T,
	filesystem map[string]any,
	codexBinary string,
) {
	t.Helper()
	if filesystem[codexBinary] != "read" {
		t.Fatalf("managed Linux profile does not grant the exact Codex executable: %#v", filesystem)
	}
}
