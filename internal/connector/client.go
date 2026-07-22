package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/GhostFlying/delegation/internal/buildinfo"
	"github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/identity"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/tokenfile"
	"github.com/coder/websocket"
)

const (
	defaultReconnectMinimum = 250 * time.Millisecond
	defaultReconnectMaximum = 10 * time.Second
	connectTimeout          = 30 * time.Second
	writeTimeout            = 10 * time.Second
	maximumPendingCalls     = 128
	maximumHeartbeat        = time.Hour
	maximumReconnect        = 5 * time.Minute
)

var (
	ErrUnavailable = errors.New("connector is not connected to the broker")
	ErrBusy        = errors.New("connector has too many pending broker calls")
)

type DialFunc func(context.Context, string, *websocket.DialOptions) (*websocket.Conn, *http.Response, error)

type Options struct {
	BrokerURL                string
	AllowInsecureNonLoopback bool
	ControllerID             string
	DeviceID                 string
	DeviceName               string
	AuthMode                 config.AuthMode
	Token                    *tokenfile.Token
	RuntimeVersion           string
	OperatingSystem          string
	Architecture             string
	ReconnectMin             time.Duration
	ReconnectMax             time.Duration
	Dial                     DialFunc
	ReportError              func(error)
}

type Status struct {
	Connected         bool
	ConnectionID      string
	RegistryRevision  uint64
	HeartbeatInterval time.Duration
	Features          []string
}

type RPCError struct {
	Code    int
	Message string
}

func (e *RPCError) Error() string {
	return fmt.Sprintf("broker RPC %d: %s", e.Code, e.Message)
}

type Client struct {
	endpoint     string
	hello        protocol.Hello
	token        *tokenfile.Token
	reconnectMin time.Duration
	reconnectMax time.Duration
	dial         DialFunc
	httpClient   *http.Client
	reportError  func(error)
	running      atomic.Bool

	mu      sync.RWMutex
	session *session
	status  Status
	updates chan struct{}
}

func New(options Options) (*Client, error) {
	endpoint, err := config.NormalizeBrokerURL(options.BrokerURL, options.AllowInsecureNonLoopback)
	if err != nil {
		return nil, err
	}
	if options.RuntimeVersion == "" {
		options.RuntimeVersion = buildinfo.Version
	}
	if options.OperatingSystem == "" {
		options.OperatingSystem = runtime.GOOS
	}
	if options.Architecture == "" {
		options.Architecture = runtime.GOARCH
	}
	features := []string{
		protocol.FeatureDeviceRegistry,
		protocol.FeatureFullDuplexRPC,
		protocol.FeatureMailbox,
		protocol.FeaturePeerRoot,
	}
	hello := protocol.Hello{
		ControllerID:   options.ControllerID,
		DeviceID:       options.DeviceID,
		DeviceName:     options.DeviceName,
		OS:             options.OperatingSystem,
		Arch:           options.Architecture,
		RuntimeVersion: options.RuntimeVersion,
		Features:       features,
	}
	if err := hello.Validate(); err != nil {
		return nil, fmt.Errorf("connector identity: %w", err)
	}
	var token *tokenfile.Token
	switch options.AuthMode {
	case config.AuthModeNone:
		if options.Token != nil {
			return nil, errors.New("none authentication must not include a device token")
		}
	case config.AuthModeToken:
		if options.Token == nil {
			return nil, errors.New("token authentication requires a device token")
		}
		copy := *options.Token
		token = &copy
	default:
		return nil, fmt.Errorf("unsupported connector auth mode %q", options.AuthMode)
	}
	reconnectMin := options.ReconnectMin
	if reconnectMin == 0 {
		reconnectMin = defaultReconnectMinimum
	}
	reconnectMax := options.ReconnectMax
	if reconnectMax == 0 {
		reconnectMax = defaultReconnectMaximum
	}
	if reconnectMin < time.Millisecond || reconnectMax < reconnectMin || reconnectMax > maximumReconnect {
		return nil, errors.New("connector reconnect bounds are invalid")
	}
	dial := options.Dial
	if dial == nil {
		dial = websocket.Dial
	}
	reportError := options.ReportError
	if reportError == nil {
		reportError = func(error) {}
	}
	httpTransport := http.DefaultTransport.(*http.Transport).Clone()
	if strings.HasPrefix(endpoint, "ws://") {
		httpTransport.Proxy = nil
	}
	return &Client{
		endpoint:     endpoint,
		hello:        hello,
		token:        token,
		reconnectMin: reconnectMin,
		reconnectMax: reconnectMax,
		dial:         dial,
		httpClient: &http.Client{Transport: httpTransport, CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		}},
		reportError: reportError,
		updates:     make(chan struct{}),
	}, nil
}

func (c *Client) Run(ctx context.Context) error {
	if !c.running.CompareAndSwap(false, true) {
		return errors.New("connector client is already running")
	}
	defer c.running.Store(false)
	backoff := c.reconnectMin
	for {
		healthy, err := c.runSession(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			c.reportError(err)
		}
		if healthy {
			backoff = c.reconnectMin
		}
		if err := waitContext(ctx, fullJitter(backoff)); err != nil {
			return nil
		}
		if !healthy {
			backoff = min(backoff*2, c.reconnectMax)
		}
	}
}

func (c *Client) Call(
	ctx context.Context,
	method, treeID string,
	source *control.PrincipalIdentity,
	params, result any,
) error {
	c.mu.RLock()
	current := c.session
	c.mu.RUnlock()
	if current == nil {
		return ErrUnavailable
	}
	payload, err := current.call(ctx, method, treeID, source, params)
	if err != nil {
		return err
	}
	if result == nil {
		return nil
	}
	if err := decodeResult(payload, result); err != nil {
		return fmt.Errorf("decode broker %s result: %w", method, err)
	}
	return nil
}

func (c *Client) Status() Status {
	c.mu.RLock()
	defer c.mu.RUnlock()
	status := c.status
	status.Features = slices.Clone(status.Features)
	return status
}

func (c *Client) WaitReady(ctx context.Context) error {
	for {
		c.mu.RLock()
		connected := c.status.Connected
		updates := c.updates
		c.mu.RUnlock()
		if connected {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-updates:
		}
	}
}

func (c *Client) runSession(ctx context.Context) (bool, error) {
	connectContext, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()
	header := http.Header{}
	if c.token != nil {
		header.Set("Authorization", "Bearer "+tokenfile.Encode(*c.token))
	}
	connection, response, err := c.dial(
		connectContext,
		c.endpoint,
		&websocket.DialOptions{HTTPClient: c.httpClient, HTTPHeader: header},
	)
	if err != nil {
		if response != nil {
			return false, fmt.Errorf("connect to broker: HTTP %d: %w", response.StatusCode, err)
		}
		return false, fmt.Errorf("connect to broker: %w", err)
	}
	connection.SetReadLimit(protocol.MaxMessageSize)
	current := newSession(c, connection)
	helloResult, err := current.bootstrap(connectContext, c.hello)
	if err != nil {
		current.close(err)
		return false, err
	}
	c.publish(current, helloResult)
	defer c.unpublish(current)
	go current.readLoop()
	go current.heartbeatLoop(helloResult.HeartbeatIntervalMS)
	select {
	case <-ctx.Done():
		current.close(ctx.Err())
	case <-current.done:
	}
	return current.heartbeatSucceeded.Load(), current.err()
}

func (c *Client) publish(current *session, result protocol.HelloResult) {
	c.mu.Lock()
	c.session = current
	c.status = Status{
		Connected:         true,
		ConnectionID:      result.ConnectionID,
		RegistryRevision:  result.Revision,
		HeartbeatInterval: time.Duration(result.HeartbeatIntervalMS) * time.Millisecond,
		Features:          slices.Clone(result.Features),
	}
	c.notifyLocked()
	c.mu.Unlock()
}

func (c *Client) unpublish(current *session) {
	c.mu.Lock()
	if c.session == current {
		c.session = nil
		c.status.Connected = false
		c.status.ConnectionID = ""
		c.notifyLocked()
	}
	c.mu.Unlock()
}

func (c *Client) updateRevision(current *session, revision uint64) {
	c.mu.Lock()
	if c.session == current {
		c.status.RegistryRevision = revision
	}
	c.mu.Unlock()
}

func (c *Client) notifyLocked() {
	close(c.updates)
	c.updates = make(chan struct{})
}

func fullJitter(maximum time.Duration) time.Duration {
	return time.Duration(rand.Int64N(int64(maximum) + 1))
}

func waitContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func decodeResult(payload json.RawMessage, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("result must contain exactly one JSON value")
		}
		return err
	}
	return nil
}

func hasDirection(requestID string, direction protocol.RequestDirection) bool {
	return strings.HasPrefix(requestID, string(direction)+"_")
}

func validateHelloResult(result protocol.HelloResult, hello protocol.Hello) error {
	if err := identity.ValidateID(result.ConnectionID); err != nil {
		return fmt.Errorf("connectionId %w", err)
	}
	if result.HeartbeatIntervalMS < 1 || result.HeartbeatIntervalMS > maximumHeartbeat.Milliseconds() {
		return errors.New("broker heartbeat interval is outside supported bounds")
	}
	if result.Revision == 0 {
		return errors.New("broker registry revision must be positive")
	}
	descriptor := hello.Descriptor()
	descriptor.Features = result.Features
	if err := descriptor.Validate(); err != nil {
		return fmt.Errorf("broker features: %w", err)
	}
	required := []string{
		protocol.FeatureDeviceRegistry,
		protocol.FeatureFullDuplexRPC,
		protocol.FeatureMailbox,
		protocol.FeaturePeerRoot,
	}
	for _, feature := range required {
		if !slices.Contains(result.Features, feature) {
			return fmt.Errorf("broker does not support required feature %q", feature)
		}
	}
	return nil
}
