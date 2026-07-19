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

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/identity"
)

const (
	CurrentSchemaVersion = 3
	brokerConnectPath    = "/v1/connect"
	maximumConfigSize    = 1024 * 1024
)

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
	file, err := openProtectedConfig(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}
	data, err := io.ReadAll(io.LimitReader(file, maximumConfigSize+1))
	closeErr := file.Close()
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}
	if closeErr != nil {
		return Config{}, fmt.Errorf("close config: %w", closeErr)
	}
	if len(data) > maximumConfigSize {
		return Config{}, fmt.Errorf("config exceeds %d-byte limit", maximumConfigSize)
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
				if c.SchemaVersion == 1 {
					return fmt.Errorf(
						"config schema version %d is obsolete; back up and move the config aside, move the shared broker token aside, verify the selected new master-token path does not exist, then rerun setup broker with the existing --controller-id, --listen, --auth-mode, --state, and --allow-insecure-nonloopback for any non-loopback listener; omit --token-file only after verifying the default token path is absent, or pass a different nonexistent path so setup creates a fresh private M1 master; do not reuse the schema version 1 broker token, then issue fresh per-device credentials",
						c.SchemaVersion,
					)
				}
				return fmt.Errorf(
					"config schema version %d is obsolete; back up and move the config aside, then rerun setup broker with all existing settings, including --controller-id, --listen, --auth-mode, --token-file when token authentication is used, --state, and --allow-insecure-nonloopback for any non-loopback listener",
					c.SchemaVersion,
				)
			}
			if c.SchemaVersion == 1 {
				return fmt.Errorf(
					"config schema version %d is obsolete; back up and move the config aside, enroll a fresh device-bound credential at the broker, then rerun setup for the same role with the existing --controller-id, --device-id, --device-name, --broker-url, and --auth-mode, the newly transferred credential path in --token-file when token authentication is used, and --allow-insecure-nonloopback when the broker URL is non-loopback ws://; do not reuse the schema version 1 target token",
					c.SchemaVersion,
				)
			}
			return fmt.Errorf(
				"config schema version %d is obsolete; back up and move the config aside, then rerun setup for the same role with the existing --controller-id, --device-id, --device-name, --broker-url, --auth-mode, --token-file when token authentication is used, and --allow-insecure-nonloopback when the broker URL is non-loopback ws://",
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
		if err := validateListen(c.Broker.Listen, c.Broker.AllowInsecureNonLoopback); err != nil {
			return err
		}
	case RoleController, RoleDevice:
		if identity.ValidateID(c.DeviceID) != nil {
			return errors.New("deviceId must be a UUID")
		}
		if err := control.ValidateDeviceName(c.DeviceName); err != nil {
			return fmt.Errorf("deviceName: %w", err)
		}
		if c.Broker.Listen != "" || c.Broker.StateFile != "" {
			return errors.New("controller and device config must not contain broker listener or state fields")
		}
		if _, err := NormalizeBrokerURL(c.Broker.URL, c.Broker.AllowInsecureNonLoopback); err != nil {
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

// NormalizeBrokerURL validates a configured broker endpoint and returns its
// canonical connector URL.
func NormalizeBrokerURL(raw string, allowInsecureNonLoopback bool) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" || parsed.Hostname() == "" || parsed.Opaque != "" {
		return "", errors.New("broker URL must be an absolute ws:// or wss:// URL")
	}
	if parsed.Scheme != "ws" && parsed.Scheme != "wss" {
		return "", errors.New("broker URL must use ws:// or wss://")
	}
	if port := parsed.Port(); port != "" {
		portNumber, err := strconv.Atoi(port)
		if err != nil || portNumber < 1 || portNumber > 65535 {
			return "", errors.New("broker URL port must be an integer from 1 through 65535")
		}
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.ForceQuery ||
		parsed.Fragment != "" || parsed.RawFragment != "" || parsed.RawPath != "" {
		return "", errors.New("broker URL must not contain credentials, query, fragment, or an escaped path")
	}
	if parsed.Path != "" && parsed.Path != "/" && parsed.Path != brokerConnectPath {
		return "", errors.New("broker URL path must be empty or /v1/connect")
	}
	if parsed.Scheme == "ws" && !loopbackHost(parsed.Hostname()) && !allowInsecureNonLoopback {
		return "", errors.New("plaintext non-loopback broker URL requires explicit acknowledgement")
	}
	parsed.Path = brokerConnectPath
	return parsed.String(), nil
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

func validateListen(address string, allowInsecureNonLoopback bool) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("broker listen address must be host:port: %w", err)
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return errors.New("broker listen port must be an integer from 1 through 65535")
	}
	if loopbackHost(host) {
		return nil
	}
	if !allowInsecureNonLoopback {
		return errors.New("plaintext non-loopback listener requires explicit acknowledgement")
	}
	return nil
}

// UsesInsecureNonLoopbackTransport reports whether the configured network hop
// relies on an external encrypted network or tunnel for transport security.
func (c Config) UsesInsecureNonLoopbackTransport() bool {
	switch c.Role {
	case RoleBroker:
		host, _, err := net.SplitHostPort(c.Broker.Listen)
		return err == nil && !loopbackHost(host)
	case RoleController, RoleDevice:
		parsed, err := url.Parse(c.Broker.URL)
		return err == nil && parsed.Scheme == "ws" && !loopbackHost(parsed.Hostname())
	default:
		return false
	}
}

func loopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
