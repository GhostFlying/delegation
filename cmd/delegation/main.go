package main

import (
	"os"

	"github.com/GhostFlying/delegation/internal/appserver"
	"github.com/GhostFlying/delegation/internal/cli"
)

func main() {
	if handled, exitCode := appserver.RunDarwinSupervisorIfRequested(); handled {
		os.Exit(exitCode)
	}
	os.Exit(cli.Run(os.Args[1:], os.Stdout, os.Stderr))
}
