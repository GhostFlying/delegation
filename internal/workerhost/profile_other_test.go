//go:build !linux

package workerhost

import "testing"

func assertCodexRuntimeFilesystemPermission(
	t *testing.T,
	filesystem map[string]any,
	codexBinary string,
) {
	t.Helper()
	if _, found := filesystem[codexBinary]; found {
		t.Fatalf("managed non-Linux profile grants the Codex executable: %#v", filesystem)
	}
}
