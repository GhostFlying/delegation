package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path/filepath"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/identity"
)

const LegacySchemaVersion = 3

const (
	LegacyRoleController Role = "controller"
	LegacyRoleDevice     Role = "device"
)

// ReadLegacy reads the final role-based configuration schema for explicit migration.
func ReadLegacy(path string) (Config, error) {
	file, err := openProtectedConfig(path)
	if err != nil {
		return Config{}, fmt.Errorf("read legacy config: %w", err)
	}
	data, readErr := io.ReadAll(io.LimitReader(file, maximumConfigSize+1))
	closeErr := file.Close()
	if readErr != nil || closeErr != nil {
		return Config{}, errors.Join(readErr, closeErr)
	}
	if len(data) > maximumConfigSize {
		return Config{}, fmt.Errorf("legacy config exceeds %d-byte limit", maximumConfigSize)
	}
	var cfg Config
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("decode legacy config: %w", err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return Config{}, err
	}
	if err := cfg.validateLegacy(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) validateLegacy() error {
	if c.SchemaVersion != LegacySchemaVersion {
		return fmt.Errorf("legacy config schema version must be %d, got %d", LegacySchemaVersion, c.SchemaVersion)
	}
	if err := identityID(c.ControllerID, "controllerId"); err != nil {
		return err
	}
	switch c.Role {
	case RoleBroker:
		if c.DeviceID != "" || c.DeviceName != "" || c.Broker.URL != "" {
			return errors.New("legacy broker config contains peer fields")
		}
		if err := validateListen(c.Broker.Listen, c.Broker.AllowInsecureNonLoopback); err != nil {
			return err
		}
		if !filepath.IsAbs(c.Broker.StateFile) {
			return errors.New("legacy broker stateFile must be an absolute path")
		}
	case LegacyRoleController, LegacyRoleDevice:
		if err := identityID(c.DeviceID, "deviceId"); err != nil {
			return err
		}
		if err := controlDeviceName(c.DeviceName); err != nil {
			return err
		}
		if c.Broker.Listen != "" || c.Broker.StateFile != "" {
			return errors.New("legacy connector config contains broker listener or state fields")
		}
		if _, err := UpgradeLegacyBrokerURL(c.Broker.URL, c.Broker.AllowInsecureNonLoopback); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported legacy role %q", c.Role)
	}
	return c.Broker.Auth.validate()
}

func identityID(value, name string) error {
	if err := identity.ValidateID(value); err != nil {
		return fmt.Errorf("%s %w", name, err)
	}
	return nil
}

func controlDeviceName(value string) error {
	if err := control.ValidateDeviceName(value); err != nil {
		return fmt.Errorf("deviceName: %w", err)
	}
	return nil
}

// UpgradeLegacyBrokerURL validates a v3 endpoint and returns its v2 route.
func UpgradeLegacyBrokerURL(raw string, allowInsecureNonLoopback bool) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", errors.New("legacy broker URL must be an absolute ws:// or wss:// URL")
	}
	if parsed.Path == "/v1/connect" {
		parsed.Path = ""
	}
	return NormalizeBrokerURL(parsed.String(), allowInsecureNonLoopback)
}
