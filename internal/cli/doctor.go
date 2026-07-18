package cli

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"

	"github.com/GhostFlying/delegation/internal/buildinfo"
	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/tokenfile"
)

type doctorResult struct {
	OK         bool                  `json:"ok"`
	Version    string                `json:"version"`
	ConfigPath string                `json:"configPath"`
	Role       delegationconfig.Role `json:"role"`
	Checks     []string              `json:"checks"`
}

func runDoctor(args []string, stdout, stderr io.Writer) int {
	defaultPath, err := delegationconfig.DefaultPath()
	if err != nil {
		return writeError(stderr, err)
	}
	flags := flag.NewFlagSet("delegation doctor", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", defaultPath, "configuration file path")
	jsonOutput := flags.Bool("json", false, "print diagnostics as JSON")
	if code := parseFlags(flags, args); code >= 0 {
		return code
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
	} else if cfg.Broker.Auth.Mode == delegationconfig.AuthModeToken {
		if err := tokenfile.Validate(cfg.Broker.Auth.TokenFile); err != nil {
			return writeError(stderr, err)
		}
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
