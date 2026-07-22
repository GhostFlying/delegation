//go:build linux

package workerhost

func addCodexRuntimeFilesystemPermission(filesystem map[string]any, codexBinary string) {
	filesystem[codexBinary] = "read"
}
