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

	"github.com/GhostFlying/delegation/internal/clilaunch"
	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/hostkind"
	"github.com/GhostFlying/delegation/internal/identity"
	"github.com/GhostFlying/delegation/internal/instanceid"
)

const (
	CurrentSchemaVersion = 4
	DefaultInstanceID    = "default"
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

type TransportMode uint8

const (
	TransportModeTCP TransportMode = iota
	TransportModeTailscale
)

type RuntimeCapabilities struct {
	EmbeddedTailscale bool
}

type Config struct {
	SchemaVersion int             `json:"schemaVersion"`
	InstanceID    string          `json:"instanceId,omitempty"`
	HostKind      hostkind.Kind   `json:"hostKind,omitempty"`
	Role          Role            `json:"role"`
	ControllerID  string          `json:"controllerId"`
	DeviceID      string          `json:"deviceId,omitempty"`
	DeviceName    string          `json:"deviceName,omitempty"`
	Transport     TransportConfig `json:"transport"`
	Broker        BrokerConfig    `json:"broker"`
	Peer          PeerConfig      `json:"peer"`
}

type TransportConfig struct {
	Mode      TransportMode    `json:"mode"`
	Tailscale *TailscaleConfig `json:"tailscale,omitempty"`
}

type TailscaleConfig struct {
	StateDir    string `json:"stateDir"`
	Hostname    string `json:"hostname"`
	AuthKeyFile string `json:"authKeyFile"`
}

type BrokerConfig struct {
	URL                      string     `json:"url,omitempty"`
	Listen                   string     `json:"listen,omitempty"`
	StatusListen             string     `json:"statusListen,omitempty"`
	StateFile                string     `json:"stateFile,omitempty"`
	Auth                     AuthConfig `json:"auth"`
	AllowInsecureNonLoopback bool       `json:"allowInsecureNonLoopback,omitempty"`
}

type AuthConfig struct {
	Mode      AuthMode `json:"mode"`
	TokenFile string   `json:"tokenFile,omitempty"`
}

type PeerConfig struct {
	CLI            *CLIConfig `json:"cli,omitempty"`
	CodexBinary    string     `json:"codexBinary,omitempty"`
	GitBinary      string     `json:"gitBinary,omitempty"`
	CodexHome      string     `json:"codexHome,omitempty"`
	WorkspaceRoot  string     `json:"workspaceRoot,omitempty"`
	StateFile      string     `json:"stateFile,omitempty"`
	MaxWorkerSlots int        `json:"maxWorkerSlots,omitempty"`
}

type CLIConfig struct {
	Command   string          `json:"command,omitempty"`
	Arguments []string        `json:"arguments,omitempty"`
	Launcher  *clilaunch.Spec `json:"launcher,omitempty"`
}

func (p PeerConfig) EffectiveCLI() CLIConfig {
	if p.CLI != nil {
		return *p.CLI
	}
	return CLIConfig{Command: p.CodexBinary}
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
	return DefaultBrokerPathForInstance(DefaultInstanceID)
}

func DefaultPeerPath() (string, error) {
	return DefaultPeerPathForInstance(DefaultInstanceID)
}

func DefaultBrokerPathForInstance(instanceID string) (string, error) {
	return defaultPath(instanceID, "broker.json")
}

func DefaultPeerPathForInstance(instanceID string) (string, error) {
	return defaultPath(instanceID, "peer.json")
}

func defaultPath(instanceID, name string) (string, error) {
	if err := ValidateInstanceID(instanceID); err != nil {
		return "", err
	}
	if path := os.Getenv("DELEGATION_CONFIG"); path != "" {
		return filepath.Abs(path)
	}
	home, err := DefaultHome()
	if err != nil {
		return "", err
	}
	if instanceID != DefaultInstanceID {
		home = filepath.Join(home, "instances", instanceID)
	}
	return filepath.Join(home, name), nil
}

func Read(path string) (Config, error) {
	return ReadForRuntime(path, RuntimeCapabilities{})
}

// ReadForRuntime reads and validates a configuration against explicitly
// available runtime transport capabilities.
func ReadForRuntime(path string, capabilities RuntimeCapabilities) (Config, error) {
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
		SchemaVersion int             `json:"schemaVersion"`
		Transport     json.RawMessage `json:"transport"`
	}
	if err := json.Unmarshal(data, &header); err != nil {
		return Config{}, fmt.Errorf("decode config: %w", err)
	}
	if header.SchemaVersion != CurrentSchemaVersion {
		return Config{}, unsupportedSchemaVersion(header.SchemaVersion)
	}
	if err := validateTransportJSON(header.Transport); err != nil {
		return Config{}, err
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
	if err := cfg.ValidateForRuntime(capabilities); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) Validate() error {
	return c.ValidateForRuntime(RuntimeCapabilities{})
}

// ValidateForRuntime validates a configuration against explicitly available
// runtime transport capabilities.
func (c Config) ValidateForRuntime(capabilities RuntimeCapabilities) error {
	if c.SchemaVersion != CurrentSchemaVersion {
		return unsupportedSchemaVersion(c.SchemaVersion)
	}
	if c.InstanceID != "" {
		if err := ValidateInstanceID(c.InstanceID); err != nil {
			return err
		}
	}
	if c.HostKind != "" {
		if err := c.HostKind.Validate(); err != nil {
			return err
		}
	}
	if identity.ValidateID(c.ControllerID) != nil {
		return errors.New("controllerId must be a UUID")
	}
	if err := c.Transport.validate(); err != nil {
		return err
	}
	if c.Transport.Mode == TransportModeTailscale && !capabilities.EmbeddedTailscale {
		return errors.New("embedded tailscale transport is not supported by this runtime")
	}
	if c.Transport.Mode == TransportModeTailscale && c.Broker.AllowInsecureNonLoopback {
		return errors.New("allowInsecureNonLoopback must be false when transport mode is tailscale")
	}

	switch c.Role {
	case RoleBroker:
		if c.DeviceID != "" || c.DeviceName != "" || c.Broker.URL != "" || c.Peer != (PeerConfig{}) {
			return errors.New("broker config must not contain peer fields or broker URL")
		}
		if !filepath.IsAbs(c.Broker.StateFile) {
			return errors.New("broker stateFile must be an absolute path")
		}
		if c.EffectiveInstanceID() != DefaultInstanceID && c.Broker.StatusListen == "" {
			return errors.New("named broker instances require a status listener")
		}
		switch c.Transport.Mode {
		case TransportModeTCP:
			if err := validateListen(c.Broker.Listen, c.Broker.AllowInsecureNonLoopback); err != nil {
				return err
			}
		case TransportModeTailscale:
			if err := validateTailscaleListen(c.Broker.Listen); err != nil {
				return err
			}
		}
		if c.Broker.StatusListen != "" {
			if err := validateStatusListen(c.Broker.StatusListen); err != nil {
				return err
			}
			if listenPortsConflict(c.Broker.Listen, c.Broker.StatusListen) {
				return errors.New("broker status listener must not overlap the broker listener")
			}
		}
	case RolePeer:
		if identity.ValidateID(c.DeviceID) != nil {
			return errors.New("deviceId must be a UUID")
		}
		if err := control.ValidateDeviceName(c.DeviceName); err != nil {
			return fmt.Errorf("deviceName: %w", err)
		}
		if c.Broker.Listen != "" || c.Broker.StatusListen != "" || c.Broker.StateFile != "" {
			return errors.New("peer config must not contain broker listener or state fields")
		}
		if _, err := NormalizeBrokerURLForTransport(
			c.Broker.URL,
			c.Transport.Mode,
			c.Broker.AllowInsecureNonLoopback,
		); err != nil {
			return err
		}
		if err := c.Peer.validateCLI(); err != nil {
			return err
		}
		if c.EffectiveHostKind() == hostkind.TraeX {
			if c.Peer.CLI == nil {
				return errors.New("TraeX peer requires structured cli configuration")
			}
			if c.Peer.CLI.Launcher == nil {
				return errors.New("TraeX peer requires a CLI launcher")
			}
		}
		if !filepath.IsAbs(c.Peer.GitBinary) {
			return errors.New("peer gitBinary must be an absolute path")
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

func (p PeerConfig) validateCLI() error {
	legacy := p.CodexBinary != ""
	structured := p.CLI != nil
	if legacy == structured {
		return errors.New("peer must configure exactly one of cli or codexBinary")
	}
	if legacy {
		if !filepath.IsAbs(p.CodexBinary) {
			return errors.New("peer codexBinary must be an absolute path")
		}
		return nil
	}
	if !filepath.IsAbs(p.CLI.Command) {
		return errors.New("peer cli command must be an absolute path")
	}
	launch := clilaunch.Spec{
		Executable:      p.CLI.Command,
		PrefixArguments: p.CLI.Arguments,
	}
	if p.CLI.Launcher != nil {
		launch.Executable = p.CLI.Launcher.Executable
		launch.PrefixArguments = make(
			[]string,
			0,
			len(p.CLI.Launcher.PrefixArguments)+1+len(p.CLI.Arguments),
		)
		launch.PrefixArguments = append(
			launch.PrefixArguments,
			p.CLI.Launcher.PrefixArguments...,
		)
		launch.PrefixArguments = append(launch.PrefixArguments, p.CLI.Command)
		launch.PrefixArguments = append(launch.PrefixArguments, p.CLI.Arguments...)
	}
	return clilaunch.Validate(launch)
}

func unsupportedSchemaVersion(version int) error {
	return fmt.Errorf(
		"unsupported config schema version %d; this runtime supports only version %d; create a new configuration with setup broker or setup peer",
		version,
		CurrentSchemaVersion,
	)
}

func (c Config) EffectiveInstanceID() string {
	if c.InstanceID == "" {
		return DefaultInstanceID
	}
	return c.InstanceID
}

func (c Config) EffectiveHostKind() hostkind.Kind {
	if c.HostKind == "" {
		return hostkind.Codex
	}
	return c.HostKind
}

func ValidateInstanceID(value string) error {
	return instanceid.Validate(value)
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

func (m TransportMode) MarshalJSON() ([]byte, error) {
	switch m {
	case TransportModeTCP:
		return json.Marshal("tcp")
	case TransportModeTailscale:
		return json.Marshal("tailscale")
	default:
		return nil, fmt.Errorf("unsupported transport mode %d", m)
	}
}

func (m *TransportMode) UnmarshalJSON(data []byte) error {
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return fmt.Errorf("transport mode must be tcp or tailscale: %w", err)
	}
	switch value {
	case "tcp":
		*m = TransportModeTCP
	case "tailscale":
		*m = TransportModeTailscale
	default:
		return fmt.Errorf("unsupported transport mode %q", value)
	}
	return nil
}

func (t TransportConfig) validate() error {
	switch t.Mode {
	case TransportModeTCP:
		if t.Tailscale != nil {
			return errors.New("tailscale configuration must be absent when transport mode is tcp")
		}
	case TransportModeTailscale:
		if t.Tailscale == nil {
			return errors.New("tailscale configuration is required when transport mode is tailscale")
		}
		if err := t.Tailscale.validate(); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported transport mode %d", t.Mode)
	}
	return nil
}

func (t TailscaleConfig) validate() error {
	if !filepath.IsAbs(t.StateDir) {
		return errors.New("tailscale stateDir must be a non-empty absolute path")
	}
	if !validTailscaleHostname(t.Hostname) {
		return errors.New("tailscale hostname must be a lowercase DNS label from 1 through 63 characters")
	}
	if !filepath.IsAbs(t.AuthKeyFile) {
		return errors.New("tailscale authKeyFile must be a non-empty absolute path")
	}
	return nil
}

func validTailscaleHostname(hostname string) bool {
	if len(hostname) < 1 || len(hostname) > 63 ||
		!lowercaseLetterOrDigit(hostname[0]) ||
		!lowercaseLetterOrDigit(hostname[len(hostname)-1]) {
		return false
	}
	for i := 1; i < len(hostname)-1; i++ {
		if !lowercaseLetterOrDigit(hostname[i]) && hostname[i] != '-' {
			return false
		}
	}
	return true
}

func lowercaseLetterOrDigit(value byte) bool {
	return value >= 'a' && value <= 'z' || value >= '0' && value <= '9'
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

// NormalizeBrokerURLForTransport validates a broker endpoint using the
// selected transport's connector rules.
func NormalizeBrokerURLForTransport(
	raw string,
	mode TransportMode,
	allowInsecureNonLoopback bool,
) (string, error) {
	switch mode {
	case TransportModeTCP:
		return NormalizeBrokerURL(raw, allowInsecureNonLoopback)
	case TransportModeTailscale:
		if allowInsecureNonLoopback {
			return "", errors.New("allowInsecureNonLoopback must be false when transport mode is tailscale")
		}
		return normalizeTailscaleBrokerURL(raw)
	default:
		return "", fmt.Errorf("unsupported transport mode %d", mode)
	}
}

func normalizeTailscaleBrokerURL(raw string) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" || parsed.Hostname() == "" || parsed.Opaque != "" {
		return "", errors.New("tailscale broker URL must be an absolute ws:// URL")
	}
	if parsed.Scheme != "ws" {
		return "", errors.New("tailscale broker URL must use ws://")
	}
	if strings.HasSuffix(parsed.Host, ":") {
		return "", errors.New("tailscale broker URL port must be an integer from 1 through 65535")
	}
	if port := parsed.Port(); port != "" {
		portNumber, err := strconv.Atoi(port)
		if err != nil || portNumber < 1 || portNumber > 65535 {
			return "", errors.New("tailscale broker URL port must be an integer from 1 through 65535")
		}
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.ForceQuery ||
		parsed.Fragment != "" || parsed.RawFragment != "" || parsed.RawPath != "" {
		return "", errors.New("tailscale broker URL must not contain credentials, query, fragment, or an escaped path")
	}
	if parsed.Path != brokerConnectPath {
		return "", errors.New("tailscale broker URL path must be /v1/connect")
	}
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

func validateTransportJSON(raw json.RawMessage) error {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return errors.New("transport configuration is required")
	}
	var header struct {
		Mode *string `json:"mode"`
	}
	if err := json.Unmarshal(raw, &header); err != nil {
		return fmt.Errorf("decode transport configuration: %w", err)
	}
	if header.Mode == nil {
		return errors.New("transport mode is required")
	}
	switch *header.Mode {
	case "tcp", "tailscale":
		return nil
	default:
		return fmt.Errorf("unsupported transport mode %q", *header.Mode)
	}
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

func validateTailscaleListen(address string) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("tailscale broker listen address must be :port: %w", err)
	}
	if host != "" || address != ":"+port {
		return errors.New("tailscale broker listen address must use an empty host")
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return errors.New("tailscale broker listen port must be an integer from 1 through 65535")
	}
	return nil
}

func validateStatusListen(address string) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("broker status listen address must be host:port: %w", err)
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return errors.New("broker status listen port must be an integer from 1 through 65535")
	}
	if !loopbackHost(host) {
		return errors.New("broker status listener must use a loopback address")
	}
	return nil
}

func listenPortsConflict(primaryAddress, statusAddress string) bool {
	_, primaryPort, primaryErr := net.SplitHostPort(primaryAddress)
	_, statusPort, statusErr := net.SplitHostPort(statusAddress)
	if primaryErr != nil || statusErr != nil {
		return false
	}
	primaryPortNumber, primaryErr := strconv.Atoi(primaryPort)
	statusPortNumber, statusErr := strconv.Atoi(statusPort)
	return primaryErr == nil && statusErr == nil && primaryPortNumber == statusPortNumber
}

// UsesInsecureNonLoopbackTransport reports whether the configured network hop
// relies on an external encrypted network or tunnel for transport security.
func (c Config) UsesInsecureNonLoopbackTransport() bool {
	if c.Transport.Mode != TransportModeTCP {
		return false
	}
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
