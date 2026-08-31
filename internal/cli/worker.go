package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"time"

	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/localbridge"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/runtimeconfig"
)

func runWorker(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 || args[0] != "recheck" {
		fmt.Fprintln(stderr, "usage: delegation worker recheck --config PATH [--json]")
		return exitUsage
	}
	return runWorkerRecheck(args[1:], stdout, stderr, localbridge.RecheckWorker)
}

type workerRecheck func(context.Context, string) (protocol.WorkerReadiness, error)

func runWorkerRecheck(
	args []string, stdout, stderr io.Writer, recheck workerRecheck,
) int {
	flags := flag.NewFlagSet("delegation worker recheck", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", "", "peer configuration file path (required)")
	jsonOutput := flags.Bool("json", false, "print readiness as JSON")
	if code := parseFlags(flags, args); code >= 0 {
		return code
	}
	if *configPath == "" {
		return writeError(stderr, errors.New("--config is required"))
	}
	resolved, err := absolutePath(*configPath)
	if err != nil {
		return writeError(stderr, err)
	}
	cfg, err := runtimeconfig.Read(resolved)
	if err != nil {
		return writeError(stderr, err)
	}
	if cfg.Role != delegationconfig.RolePeer {
		return writeError(stderr, errors.New("worker recheck requires a peer configuration"))
	}
	endpoint, err := localbridge.EndpointForInstance(
		cfg.EffectiveInstanceID(), cfg.ControllerID, cfg.DeviceID,
	)
	if err != nil {
		return writeError(stderr, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), peerStatusReadTimeout)
	readiness, err := recheck(ctx, endpoint)
	cancel()
	if err != nil {
		return writeError(stderr, err)
	}
	if *jsonOutput {
		if err := json.NewEncoder(stdout).Encode(readiness); err != nil {
			return writeError(stderr, err)
		}
		return 0
	}
	fmt.Fprintf(stdout, "worker readiness recheck scheduled\n")
	fmt.Fprintf(stdout, "epoch: %d\n", readiness.Epoch)
	fmt.Fprintf(stdout, "state: %s\n", readiness.State)
	fmt.Fprintf(stdout, "next attempt: %s\n", time.UnixMilli(readiness.NextAttemptAt).Format(time.RFC3339Nano))
	return 0
}
