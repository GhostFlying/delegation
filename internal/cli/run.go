package cli

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/GhostFlying/delegation/internal/buildinfo"
)

const (
	exitUsage       = 2
	exitUnavailable = 69
)

func Run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		writeUsage(stderr)
		return exitUsage
	}

	switch args[0] {
	case "help", "-h", "--help":
		writeUsage(stdout)
		return 0
	case "version":
		return runVersion(args[1:], stdout, stderr)
	case "mcp":
		return runMCP(args[1:], stderr)
	default:
		fmt.Fprintf(stderr, "delegation: unknown command %q\n", args[0])
		writeUsage(stderr)
		return exitUsage
	}
}

func runVersion(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stdout, buildinfo.Version)
		return 0
	}
	if len(args) != 1 || args[0] != "--json" {
		fmt.Fprintln(stderr, "usage: delegation version [--json]")
		return exitUsage
	}

	payload := struct {
		Version string `json:"version"`
	}{Version: buildinfo.Version}
	if err := json.NewEncoder(stdout).Encode(payload); err != nil {
		fmt.Fprintf(stderr, "delegation: encode version: %v\n", err)
		return 1
	}
	return 0
}

func runMCP(args []string, stderr io.Writer) int {
	if len(args) != 1 || args[0] != "root" {
		fmt.Fprintln(stderr, "usage: delegation mcp root")
		return exitUsage
	}

	fmt.Fprintln(stderr, "delegation: root MCP is not available in the M0 runtime scaffold")
	return exitUnavailable
}

func writeUsage(w io.Writer) {
	fmt.Fprintln(w, "usage: delegation <command>")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "commands:")
	fmt.Fprintln(w, "  version [--json]  print runtime version")
	fmt.Fprintln(w, "  mcp root          start the root MCP server")
}
