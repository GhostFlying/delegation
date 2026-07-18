package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/GhostFlying/delegation/internal/identity"
)

const CurrentSchemaVersion = 2

type Role string

const (
	RoleBroker     Role = "broker"
	RoleController Role = "controller"
	RoleDevice     Role = "device"
)

type AuthMode string

const (
	AuthModeNone  AuthMode = "none"
	AuthModeToken AuthMode = "token"
)

type Config struct {
	SchemaVersion int          `json:"schemaVersion"`
	Role          Role         `json:"role"`
	ControllerID  string       `json:"controllerId"`
	DeviceID      string       `json:"deviceId,omitempty"`
	DeviceName    string       `json:"deviceName,omitempty"`
	Broker        BrokerConfig `json:"broker"`
}

type BrokerConfig struct {
	URL                      string     `json:"url,omitempty"`
	Listen                   string     `json:"listen,omitempty"`
	StateFile                string     `json:"stateFile,omitempty"`
	Auth                     AuthConfig `json:"auth"`
	AllowInsecureNonLoopback bool       `json:"allowInsecureNonLoopback,omitempty"`
}

type AuthConfig struct {
	Mode      AuthMode `json:"mode"`
	TokenFile string   `json:"tokenFile,omitempty"`
}

func DefaultHome() (string, error) {
	if home := os.Getenv("DELEGATION_HOME"); home != "" {
		return filepath.Abs(home)
	}
	userHome, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve user home: %w", err)
	}
	return filepath.Join(userHome, ".delegation"), nil
}

func DefaultPath() (string, error) {
	if path := os.Getenv("DELEGATION_CONFIG"); path != "" {
		return filepath.Abs(path)
	}
	home, err := DefaultHome()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, "config.json"), nil
}

func Read(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}
	var compatibility Config
	if err := json.Unmarshal(data, &compatibility); err != nil {
		return Config{}, fmt.Errorf("decode config: %w", err)
	}
	if compatibility.SchemaVersion != CurrentSchemaVersion {
		return Config{}, compatibility.Validate()
	}
	var cfg Config
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("decode config: %w", err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return Config{}, err
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) Validate() error {
	if c.SchemaVersion != CurrentSchemaVersion {
		if c.SchemaVersion < CurrentSchemaVersion {
			if c.Role == RoleBroker {
				if c.Broker.Auth.Mode != AuthModeToken {
					return fmt.Errorf(
						"config schema version %d is obsolete; back up and move the config aside, then rerun setup broker with all existing settings, including --controller-id, --listen, --auth-mode, --state, and --allow-insecure-nonloopback when previously required",
						c.SchemaVersion,
					)
				}
				return fmt.Errorf(
					"config schema version %d is obsolete; back up and move the config aside, then rerun setup broker with all existing settings, including --controller-id, --listen, --auth-mode, --token-file, and --state",
					c.SchemaVersion,
				)
			}
			return fmt.Errorf(
				"config schema version %d is obsolete; back up and move the config aside, then rerun setup with the existing identity, broker URL, authentication mode, and token path when used",
				c.SchemaVersion,
			)
		}
		return fmt.Errorf("config schema version %d requires a newer delegation runtime", c.SchemaVersion)
	}
	if identity.ValidateID(c.ControllerID) != nil {
		return errors.New("controllerId must be a UUID")
	}

	switch c.Role {
	case RoleBroker:
		if c.DeviceID != "" || c.DeviceName != "" || c.Broker.URL != "" {
			return errors.New("broker config must not contain device fields or broker URL")
		}
		if !filepath.IsAbs(c.Broker.StateFile) {
			return errors.New("broker stateFile must be an absolute path")
		}
		if err := validateListen(c.Broker.Listen, c.Broker.Auth.Mode, c.Broker.AllowInsecureNonLoopback); err != nil {
			return err
		}
	case RoleController, RoleDevice:
		if identity.ValidateID(c.DeviceID) != nil {
			return errors.New("deviceId must be a UUID")
		}
		if strings.TrimSpace(c.DeviceName) == "" {
			return errors.New("deviceName must not be empty")
		}
		if c.Broker.Listen != "" || c.Broker.StateFile != "" || c.Broker.AllowInsecureNonLoopback {
			return errors.New("controller and device config must not contain broker listener or state fields")
		}
		if err := validateBrokerURL(c.Broker.URL, c.Broker.Auth.Mode); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported role %q", c.Role)
	}

	return c.Broker.Auth.validate()
}

func (a AuthConfig) validate() error {
	switch a.Mode {
	case AuthModeNone:
		if a.TokenFile != "" {
			return errors.New("tokenFile must be empty when auth mode is none")
		}
	case AuthModeToken:
		if !filepath.IsAbs(a.TokenFile) {
			return errors.New("tokenFile must be an absolute path when auth mode is token")
		}
	default:
		return fmt.Errorf("unsupported auth mode %q", a.Mode)
	}
	return nil
}

func validateBrokerURL(raw string, mode AuthMode) error {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" {
		return errors.New("broker URL must be an absolute ws:// or wss:// URL")
	}
	if parsed.Scheme != "ws" && parsed.Scheme != "wss" {
		return errors.New("broker URL must use ws:// or wss://")
	}
	if mode == AuthModeToken && parsed.Scheme != "wss" {
		return errors.New("token authentication requires a wss:// broker URL")
	}
	if parsed.Scheme == "ws" && !loopbackHost(parsed.Hostname()) {
		return errors.New("plaintext ws:// broker URLs are allowed only for loopback hosts")
	}
	if port := parsed.Port(); port != "" {
		portNumber, err := strconv.Atoi(port)
		if err != nil || portNumber < 1 || portNumber > 65535 {
			return errors.New("broker URL port must be an integer from 1 through 65535")
		}
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return errors.New("broker URL must not contain credentials, query, or fragment")
	}
	return nil
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var trailing any
	err := decoder.Decode(&trailing)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("decode trailing config data: %w", err)
	}
	return errors.New("config must contain exactly one JSON value")
}

func validateListen(address string, mode AuthMode, allowInsecure bool) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("broker listen address must be host:port: %w", err)
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return errors.New("broker listen port must be an integer from 1 through 65535")
	}
	if allowInsecure && mode != AuthModeNone {
		return errors.New("non-loopback acknowledgement is valid only when auth mode is none")
	}
	if mode != AuthModeNone || loopbackHost(host) {
		return nil
	}
	if !allowInsecure {
		return errors.New("unauthenticated non-loopback listener requires explicit acknowledgement")
	}
	return nil
}

func loopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
