package cli

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/GhostFlying/delegation/internal/buildinfo"
	delegationconfig "github.com/GhostFlying/delegation/internal/config"
)

type doctorResult struct {
	OK         bool                  `json:"ok"`
	Version    string                `json:"version"`
	ConfigPath string                `json:"configPath"`
	Role       delegationconfig.Role `json:"role"`
	Checks     []string              `json:"checks"`
}

func runDoctor(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("delegation doctor", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", "", "broker or peer configuration file path (required)")
	jsonOutput := flags.Bool("json", false, "print diagnostics as JSON")
	if code := parseFlags(flags, args); code >= 0 {
		return code
	}
	if *configPath == "" {
		return writeError(stderr, errors.New("--config is required because broker and peer may coexist"))
	}
	resolvedConfig, err := absolutePath(*configPath)
	if err != nil {
		return writeError(stderr, err)
	}
	cfg, err := delegationconfig.Read(resolvedConfig)
	if err != nil {
		return writeError(stderr, err)
	}
	checks := []string{"configuration schema and role are valid"}
	if cfg.Role == delegationconfig.RoleBroker {
		if _, err := loadBrokerAuthority(resolvedConfig, cfg); err != nil {
			return writeError(stderr, err)
		}
		checks = append(checks, "broker state and authority paths are safe")
	} else {
		if _, err := loadConnectorAuthority(resolvedConfig, cfg); err != nil {
			return writeError(stderr, err)
		}
		checks = append(checks, "peer authority paths are safe")
	}
	if cfg.Broker.Auth.Mode == delegationconfig.AuthModeToken {
		checks = append(checks, "token file exists and is protected")
	}
	result := doctorResult{
		OK:         true,
		Version:    buildinfo.Version,
		ConfigPath: resolvedConfig,
		Role:       cfg.Role,
		Checks:     checks,
	}
	if *jsonOutput {
		if err := json.NewEncoder(stdout).Encode(result); err != nil {
			return writeError(stderr, fmt.Errorf("encode diagnostics: %w", err))
		}
		return 0
	}
	fmt.Fprintf(stdout, "delegation %s: ok\n", result.Version)
	fmt.Fprintf(stdout, "config: %s\n", result.ConfigPath)
	fmt.Fprintf(stdout, "role: %s\n", result.Role)
	for _, check := range result.Checks {
		fmt.Fprintf(stdout, "- %s\n", check)
	}
	return 0
}
