package runtimeconfig

import delegationconfig "github.com/GhostFlying/delegation/internal/config"

// Capabilities reports the transports linked into this executable.
func Capabilities() delegationconfig.RuntimeCapabilities {
	return delegationconfig.RuntimeCapabilities{EmbeddedTailscale: true}
}

// Validate validates configuration against the transports linked into this
// executable.
func Validate(cfg delegationconfig.Config) error {
	return cfg.ValidateForRuntime(Capabilities())
}

// Read loads configuration for this executable, which links the complete
// embedded Tailscale runtime. Reading does not start or otherwise mutate a
// Tailscale node.
func Read(path string) (delegationconfig.Config, error) {
	return delegationconfig.ReadForRuntime(path, Capabilities())
}

// WriteNew atomically writes new configuration after validating it against the
// transports linked into this executable.
func WriteNew(path string, cfg delegationconfig.Config) error {
	return delegationconfig.WriteNewForRuntime(path, cfg, Capabilities())
}
