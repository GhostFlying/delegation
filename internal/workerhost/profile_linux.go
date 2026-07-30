//go:build linux

package workerhost

func addCLIRuntimeFilesystemPermission(filesystem map[string]any, runtimeExecutable string) {
	filesystem[runtimeExecutable] = "read"
}
