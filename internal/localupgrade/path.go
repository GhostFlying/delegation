package localupgrade

import (
	"errors"
	"path/filepath"

	delegationconfig "github.com/GhostFlying/delegation/internal/config"
)

func RootForConfig(home string, config delegationconfig.Config) (string, error) {
	if home == "" || !filepath.IsAbs(home) || filepath.Clean(home) != home {
		return "", errors.New("delegation home must be an absolute clean path")
	}
	switch config.Role {
	case delegationconfig.RoleBroker, delegationconfig.RolePeer:
	default:
		return "", errors.New("upgrade configuration role is invalid")
	}
	return filepath.Join(
		home, "upgrades", config.EffectiveInstanceID(), string(config.Role),
	), nil
}
