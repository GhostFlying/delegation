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
	CurrentSchemaVersion = 2
	brokerConnectPath    = "/v1/connect"
	maximumConfigSize    = 1024 * 1024
	MaximumWorkerSlots   = 64
)

type Role string

const (
	RoleBroker Role = "broker"
	RolePeer   Role = "peer"
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
	Peer          PeerConfig   `json:"peer"`
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

type PeerConfig struct {
	CodexBinary    string `json:"codexBinary,omitempty"`
	CodexHome      string `json:"codexHome,omitempty"`
	WorkspaceRoot  string `json:"workspaceRoot,omitempty"`
	StateFile      string `json:"stateFile,omitempty"`
	MaxWorkerSlots int    `json:"maxWorkerSlots,omitempty"`
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

func DefaultBrokerPath() (string, error) {
	return defaultPath("broker.json")
}

func DefaultPeerPath() (string, error) {
	return defaultPath("peer.json")
}

func defaultPath(name string) (string, error) {
	if path := os.Getenv("DELEGATION_CONFIG"); path != "" {
		return filepath.Abs(path)
	}
	home, err := DefaultHome()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, name), nil
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
	var header struct {
		SchemaVersion int `json:"schemaVersion"`
	}
	if err := json.Unmarshal(data, &header); err != nil {
		return Config{}, fmt.Errorf("decode config: %w", err)
	}
	if header.SchemaVersion != CurrentSchemaVersion {
		return Config{}, unsupportedSchemaVersion(header.SchemaVersion)
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
		return unsupportedSchemaVersion(c.SchemaVersion)
	}
	if identity.ValidateID(c.ControllerID) != nil {
		return errors.New("controllerId must be a UUID")
	}

	switch c.Role {
	case RoleBroker:
		if c.DeviceID != "" || c.DeviceName != "" || c.Broker.URL != "" || c.Peer != (PeerConfig{}) {
			return errors.New("broker config must not contain peer fields or broker URL")
		}
		if !filepath.IsAbs(c.Broker.StateFile) {
			return errors.New("broker stateFile must be an absolute path")
		}
		if err := validateListen(c.Broker.Listen, c.Broker.AllowInsecureNonLoopback); err != nil {
			return err
		}
	case RolePeer:
		if identity.ValidateID(c.DeviceID) != nil {
			return errors.New("deviceId must be a UUID")
		}
		if err := control.ValidateDeviceName(c.DeviceName); err != nil {
			return fmt.Errorf("deviceName: %w", err)
		}
		if c.Broker.Listen != "" || c.Broker.StateFile != "" {
			return errors.New("peer config must not contain broker listener or state fields")
		}
		if _, err := NormalizeBrokerURL(c.Broker.URL, c.Broker.AllowInsecureNonLoopback); err != nil {
			return err
		}
		if !filepath.IsAbs(c.Peer.CodexBinary) {
			return errors.New("peer codexBinary must be an absolute path")
		}
		if !filepath.IsAbs(c.Peer.CodexHome) {
			return errors.New("peer codexHome must be an absolute path")
		}
		if !filepath.IsAbs(c.Peer.WorkspaceRoot) {
			return errors.New("peer workspaceRoot must be an absolute path")
		}
		if !filepath.IsAbs(c.Peer.StateFile) {
			return errors.New("peer stateFile must be an absolute path")
		}
		if c.Peer.MaxWorkerSlots < 1 || c.Peer.MaxWorkerSlots > MaximumWorkerSlots {
			return fmt.Errorf("peer maxWorkerSlots must be from 1 through %d", MaximumWorkerSlots)
		}
	default:
		return fmt.Errorf("unsupported role %q", c.Role)
	}

	return c.Broker.Auth.validate()
}

func unsupportedSchemaVersion(version int) error {
	return fmt.Errorf(
		"unsupported config schema version %d; this runtime supports only version %d; create a new configuration with setup broker or setup peer",
		version,
		CurrentSchemaVersion,
	)
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
	case RolePeer:
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
