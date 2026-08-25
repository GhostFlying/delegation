package runtimeconfig

import delegationconfig "github.com/GhostFlying/delegation/internal/config"

// Read loads configuration for this executable, which links the complete
// embedded Tailscale runtime. Reading does not start or otherwise mutate a
// Tailscale node.
func Read(path string) (delegationconfig.Config, error) {
	return delegationconfig.ReadForRuntime(
		path,
		delegationconfig.RuntimeCapabilities{EmbeddedTailscale: true},
	)
}
