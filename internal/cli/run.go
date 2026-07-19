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
	case "setup":
		return runSetup(args[1:], stdout, stderr)
	case "doctor":
		return runDoctor(args[1:], stdout, stderr)
	case "credential":
		return runCredential(args[1:], stdout, stderr)
	case "service":
		return runService(args[1:], stdout, stderr)
	case "migrate":
		return runMigrate(args[1:], stdout, stderr)
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

func writeUsage(w io.Writer) {
	fmt.Fprintln(w, "usage: delegation <command>")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "commands:")
	fmt.Fprintln(w, "  version [--json]  print runtime version")
	fmt.Fprintln(w, "  setup <role>      configure a broker or peer")
	fmt.Fprintln(w, "  doctor [--json]   validate the local configuration")
	fmt.Fprintln(w, "  credential <action>  issue or revoke a peer credential")
	fmt.Fprintln(w, "  service <action>  prepare or run the user service")
	fmt.Fprintln(w, "  migrate config    migrate an explicit legacy configuration")
	fmt.Fprintln(w, "  mcp root          start the root MCP server")
}
