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

// ReadForUpgrade accepts the current schema or the exact supported bootstrap
// predecessor and returns protected source plus validated target bytes.
func ReadForUpgrade(path string) (
	delegationconfig.Config, []byte, []byte, bool, error,
) {
	return delegationconfig.ReadForUpgrade(path, Capabilities())
}

// ReadForStartupClassification preserves a strictly decoded configuration
// alongside its validation error for narrowly scoped durable failure reporting.
func ReadForStartupClassification(
	path string,
) (delegationconfig.Config, []byte, error) {
	return delegationconfig.ReadForStartupClassification(path, Capabilities())
}

// ReadForRepair permits only the profile-selector migration performed by the
// TraeX service repair transaction, then applies normal runtime validation.
func ReadForRepair(path string) (delegationconfig.Config, []byte, int, error) {
	return delegationconfig.ReadForRepair(path, Capabilities())
}

// WriteNew atomically writes new configuration after validating it against the
// transports linked into this executable.
func WriteNew(path string, cfg delegationconfig.Config) error {
	return delegationconfig.WriteNewForRuntime(path, cfg, Capabilities())
}

func Encode(cfg delegationconfig.Config) ([]byte, error) {
	return delegationconfig.EncodeForRuntime(cfg, Capabilities())
}
