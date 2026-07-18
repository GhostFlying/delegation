package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/credential"
	"github.com/GhostFlying/delegation/internal/identity"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
	"github.com/GhostFlying/delegation/internal/tokenfile"
	"github.com/coder/websocket"
)

const (
	ConnectPath              = "/v1/connect"
	defaultHeartbeatInterval = 15 * time.Second
	writeTimeout             = 10 * time.Second
	cleanupTimeout           = 5 * time.Second
)

type Registry interface {
	AuthenticateCredential(context.Context, store.CredentialMAC) (store.Credential, error)
	RegisterAuthenticatedDevice(context.Context, store.CredentialMAC, control.DeviceDescriptor, time.Time) (control.Device, error)
	RegisterTrustedDevice(context.Context, control.DeviceDescriptor, time.Time) (control.Device, error)
	HeartbeatDevice(context.Context, string, string, uint64, time.Time) (control.Device, error)
	MarkDeviceOffline(context.Context, string, string, uint64, time.Time) (control.Device, error)
	BeginBrokerEpoch(context.Context, string) (store.PresenceTransition, error)
	EnsureRootTree(context.Context, string, string, string, time.Time) (control.Tree, control.Principal, error)
}

type Options struct {
	ControllerID      string
	AuthMode          config.AuthMode
	MasterToken       *tokenfile.Token
	Registry          Registry
	HeartbeatInterval time.Duration
	Now               func() time.Time
	ReportError       func(error)
}

type Server struct {
	controllerID      string
	authMode          config.AuthMode
	masterToken       tokenfile.Token
	registry          Registry
	heartbeatInterval time.Duration
	now               func() time.Time
	reportError       func(error)
	context           context.Context
	cancel            context.CancelFunc

	mu          sync.Mutex
	connections map[string]*session
	peers       map[*websocket.Conn]struct{}
	closed      bool
	handlers    sync.WaitGroup

	shutdownOnce sync.Once
	shutdownDone chan struct{}
	shutdownErr  error
}

type peerAuthority struct {
	credentialMAC *store.CredentialMAC
}

type session struct {
	server       *Server
	connection   *websocket.Conn
	connectionID string
	deviceID     string
	role         control.DeviceRole
	revision     atomic.Uint64
}

type internalError struct {
	operation string
	err       error
}

func (e *internalError) Error() string {
	return fmt.Sprintf("%s: %v", e.operation, e.err)
}

func (e *internalError) Unwrap() error {
	return e.err
}

func New(options Options) (*Server, error) {
	if err := identity.ValidateID(options.ControllerID); err != nil {
		return nil, fmt.Errorf("controllerId %w", err)
	}
	if options.Registry == nil {
		return nil, errors.New("broker registry is required")
	}
	switch options.AuthMode {
	case config.AuthModeNone:
		if options.MasterToken != nil {
			return nil, errors.New("none authentication must not have a master token")
		}
	case config.AuthModeToken:
		if options.MasterToken == nil {
			return nil, errors.New("token authentication requires a master token")
		}
	default:
		return nil, fmt.Errorf("unsupported broker auth mode %q", options.AuthMode)
	}
	heartbeatInterval := options.HeartbeatInterval
	if heartbeatInterval == 0 {
		heartbeatInterval = defaultHeartbeatInterval
	}
	if heartbeatInterval < time.Millisecond {
		return nil, errors.New("heartbeat interval must be at least one millisecond")
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	reportError := options.ReportError
	if reportError == nil {
		reportError = func(err error) {
			slog.Error("delegation broker failure", "error", err)
		}
	}
	server := &Server{
		controllerID:      options.ControllerID,
		authMode:          options.AuthMode,
		registry:          options.Registry,
		heartbeatInterval: heartbeatInterval,
		now:               now,
		reportError:       reportError,
		connections:       map[string]*session{},
		peers:             map[*websocket.Conn]struct{}{},
		shutdownDone:      make(chan struct{}),
	}
	server.context, server.cancel = context.WithCancel(context.Background())
	if options.MasterToken != nil {
		server.masterToken = *options.MasterToken
	}
	return server, nil
}

func (s *Server) Prepare(ctx context.Context) (store.PresenceTransition, error) {
	return s.registry.BeginBrokerEpoch(ctx, s.controllerID)
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(ConnectPath, s.handleConnect)
	mux.HandleFunc("/healthz", func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("ok\n"))
	})
	return mux
}

func (s *Server) Close(ctx context.Context) error {
	s.shutdownOnce.Do(s.startShutdown)
	select {
	case <-s.shutdownDone:
		return s.shutdownErr
	case <-ctx.Done():
		return fmt.Errorf("close broker: %w", ctx.Err())
	}
}

func (s *Server) startShutdown() {
	s.mu.Lock()
	s.closed = true
	s.cancel()
	peers := make([]*websocket.Conn, 0, len(s.peers))
	for peer := range s.peers {
		peers = append(peers, peer)
	}
	s.mu.Unlock()

	go func() {
		closeErrors := make(chan error, len(peers))
		var forceClose sync.WaitGroup
		for _, peer := range peers {
			forceClose.Add(1)
			go func() {
				defer forceClose.Done()
				if err := normalizePeerCloseError(peer.CloseNow()); err != nil {
					closeErrors <- err
				}
			}()
		}
		forceClose.Wait()
		close(closeErrors)
		s.handlers.Wait()

		var failures []error
		for err := range closeErrors {
			failures = append(failures, fmt.Errorf("force close peer: %w", err))
		}
		cleanupContext, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		_, err := s.registry.BeginBrokerEpoch(cleanupContext, s.controllerID)
		cancel()
		if err != nil {
			failures = append(failures, fmt.Errorf("mark broker devices offline: %w", err))
		}
		s.shutdownErr = errors.Join(failures...)
		if s.shutdownErr != nil {
			s.reportError(s.shutdownErr)
		}
		close(s.shutdownDone)
	}()
}

func normalizePeerCloseError(err error) error {
	if errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}

func (s *Server) beginHandler() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	s.handlers.Add(1)
	return true
}

func (s *Server) trackPeer(peer *websocket.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	s.peers[peer] = struct{}{}
	return true
}

func (s *Server) untrackPeer(peer *websocket.Conn) {
	s.mu.Lock()
	delete(s.peers, peer)
	s.mu.Unlock()
}

func (s *Server) handleConnect(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writer.Header().Set("Allow", http.MethodGet)
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.beginHandler() {
		http.Error(writer, "broker unavailable", http.StatusServiceUnavailable)
		return
	}
	defer s.handlers.Done()

	authContext, cancelAuth := context.WithCancel(request.Context())
	stopBrokerCancellation := context.AfterFunc(s.context, cancelAuth)
	authority, err := s.authenticateRequest(authContext, request)
	stopBrokerCancellation()
	cancelAuth()
	if err != nil {
		if errors.Is(err, store.ErrNotFound) || errors.Is(err, store.ErrCredentialDisabled) {
			writer.Header().Set("WWW-Authenticate", "Bearer")
			http.Error(writer, "unauthorized", http.StatusUnauthorized)
			return
		}
		if !isContextError(err) {
			s.reportError(&internalError{operation: "authenticate connector", err: err})
		}
		http.Error(writer, "broker unavailable", http.StatusServiceUnavailable)
		return
	}
	connection, err := websocket.Accept(writer, request, nil)
	if err != nil {
		return
	}
	defer connection.CloseNow()
	connection.SetReadLimit(protocol.MaxMessageSize)
	if !s.trackPeer(connection) {
		return
	}
	defer s.untrackPeer(connection)

	current, err := s.acceptHello(s.context, connection, authority)
	if err != nil {
		return
	}
	previous, active := s.activate(current)
	if !active {
		s.releaseLease(current)
		return
	}
	if previous != nil {
		_ = previous.connection.CloseNow()
	}
	defer s.deactivate(current)
	if err := current.run(s.context); err != nil {
		var internal *internalError
		if errors.As(err, &internal) && !isContextError(err) {
			s.reportError(internal)
		}
	}
}

func (s *Server) authenticateRequest(ctx context.Context, request *http.Request) (peerAuthority, error) {
	if s.authMode == config.AuthModeNone {
		return peerAuthority{}, nil
	}
	values := request.Header.Values("Authorization")
	if len(values) != 1 || !strings.HasPrefix(values[0], "Bearer ") {
		return peerAuthority{}, store.ErrNotFound
	}
	deviceToken, err := tokenfile.Parse(strings.TrimPrefix(values[0], "Bearer "))
	if err != nil {
		return peerAuthority{}, store.ErrNotFound
	}
	mac := credential.MAC(s.masterToken, deviceToken)
	if _, err := s.registry.AuthenticateCredential(ctx, mac); err != nil {
		return peerAuthority{}, err
	}
	return peerAuthority{credentialMAC: &mac}, nil
}

func (s *Server) acceptHello(
	ctx context.Context,
	connection *websocket.Conn,
	authority peerAuthority,
) (*session, error) {
	readContext, cancel := context.WithTimeout(ctx, s.heartbeatInterval*3)
	defer cancel()
	envelope, err := readEnvelope(readContext, connection)
	if err != nil {
		return nil, err
	}
	if !validPeerDirection(envelope) {
		return nil, errors.New("hello used an invalid request direction")
	}
	if envelope.Kind != protocol.KindRequest || envelope.Method != protocol.MethodHello ||
		envelope.ControllerID != s.controllerID || envelope.TreeID != "" || envelope.Source != nil {
		_ = s.writeError(ctx, connection, envelope, protocol.ErrorInvalidRequest, "invalid hello request")
		return nil, errors.New("invalid hello request")
	}
	hello, err := protocol.DecodePayload[protocol.Hello](envelope.Payload)
	if err != nil || hello.Validate() != nil || hello.ControllerID != s.controllerID {
		_ = s.writeError(ctx, connection, envelope, protocol.ErrorInvalidParams, "invalid hello payload")
		return nil, errors.New("invalid hello payload")
	}
	device, err := s.registerDevice(ctx, authority, hello)
	if err != nil {
		if registrationDenied(err) {
			_ = s.writeError(ctx, connection, envelope, protocol.ErrorForbidden, "device registration denied")
			return nil, err
		}
		if !isContextError(err) {
			s.reportError(&internalError{operation: "register device", err: err})
		}
		_ = s.writeError(ctx, connection, envelope, protocol.ErrorUnavailable, "broker unavailable")
		return nil, err
	}
	connectionID, err := identity.NewID()
	if err != nil {
		_ = s.writeError(ctx, connection, envelope, protocol.ErrorInternal, "broker failed to create connection")
		return nil, err
	}
	current := &session{
		server:       s,
		connection:   connection,
		connectionID: connectionID,
		deviceID:     device.DeviceID,
		role:         device.Role,
	}
	current.revision.Store(device.Revision)
	result := protocol.HelloResult{
		ConnectionID:        connectionID,
		Features:            []string{protocol.FeatureDeviceRegistry, protocol.FeatureRootTree},
		HeartbeatIntervalMS: s.heartbeatInterval.Milliseconds(),
		Revision:            device.Revision,
	}
	if err := s.writeResult(ctx, connection, envelope, result); err != nil {
		s.releaseLease(current)
		return nil, err
	}
	return current, nil
}

func (s *Server) registerDevice(
	ctx context.Context,
	authority peerAuthority,
	hello protocol.Hello,
) (control.Device, error) {
	if authority.credentialMAC == nil {
		return s.registry.RegisterTrustedDevice(ctx, hello.Descriptor(), s.now())
	}
	return s.registry.RegisterAuthenticatedDevice(ctx, *authority.credentialMAC, hello.Descriptor(), s.now())
}

func (s *Server) activate(current *session) (*session, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, false
	}
	previous := s.connections[current.deviceID]
	if previous != nil && previous.revision.Load() > current.revision.Load() {
		return previous, false
	}
	s.connections[current.deviceID] = current
	return previous, true
}

func (s *Server) deactivate(current *session) {
	s.mu.Lock()
	if s.connections[current.deviceID] != current {
		s.mu.Unlock()
		return
	}
	delete(s.connections, current.deviceID)
	s.mu.Unlock()
	s.releaseLease(current)
}

func (s *Server) releaseLease(current *session) {
	ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()
	_, err := s.registry.MarkDeviceOffline(
		ctx, s.controllerID, current.deviceID, current.revision.Load(), s.now(),
	)
	if err != nil && !errors.Is(err, store.ErrStaleRevision) {
		s.reportError(&internalError{operation: "mark device offline", err: err})
	}
}

func (s *session) run(ctx context.Context) error {
	deadline := time.Now().Add(s.server.heartbeatInterval * 3)
	for {
		messageContext, cancel := context.WithDeadline(ctx, deadline)
		envelope, err := readEnvelope(messageContext, s.connection)
		if err == nil {
			var renewed bool
			renewed, err = s.handleEnvelope(messageContext, envelope)
			if renewed {
				deadline = time.Now().Add(s.server.heartbeatInterval * 3)
			}
		}
		cancel()
		if err != nil {
			return err
		}
	}
}

func (s *session) handleEnvelope(ctx context.Context, envelope protocol.Envelope) (bool, error) {
	if !validPeerDirection(envelope) {
		return false, errors.New("connection used an invalid request direction")
	}
	if envelope.ControllerID != s.server.controllerID {
		if envelope.Kind == protocol.KindRequest {
			_ = s.server.writeError(ctx, s.connection, envelope, protocol.ErrorForbidden, "controller mismatch")
		}
		return false, errors.New("connection sent a mismatched controllerId")
	}
	switch envelope.Kind {
	case protocol.KindNotification, protocol.KindResponse:
		return false, nil
	case protocol.KindRequest:
	default:
		return false, errors.New("connection sent an unsupported envelope kind")
	}
	if envelope.Method == protocol.MethodEnsureRootTree {
		return false, s.handleEnsureRootTree(ctx, envelope)
	}
	if envelope.Method != protocol.MethodHeartbeat {
		return false, s.server.writeError(ctx, s.connection, envelope, protocol.ErrorMethodNotFound, "method not found")
	}
	if envelope.TreeID != "" || envelope.Source != nil {
		return false, s.server.writeError(ctx, s.connection, envelope, protocol.ErrorInvalidRequest, "invalid heartbeat request")
	}
	if _, err := protocol.DecodePayload[protocol.Heartbeat](envelope.Payload); err != nil {
		return false, s.server.writeError(ctx, s.connection, envelope, protocol.ErrorInvalidParams, "invalid heartbeat payload")
	}
	now := s.server.now()
	device, err := s.server.registry.HeartbeatDevice(
		ctx, s.server.controllerID, s.deviceID, s.revision.Load(), now,
	)
	if err != nil {
		if leaseConflict(err) {
			_ = s.server.writeError(ctx, s.connection, envelope, protocol.ErrorConflict, "device lease is stale")
			return false, err
		}
		if isContextError(err) {
			return false, err
		}
		_ = s.server.writeError(ctx, s.connection, envelope, protocol.ErrorUnavailable, "broker unavailable")
		return false, &internalError{operation: "heartbeat device", err: err}
	}
	s.revision.Store(device.Revision)
	if err := s.server.writeResult(ctx, s.connection, envelope, protocol.HeartbeatResult{
		Revision: device.Revision, ServerTime: now.UTC().Unix(),
	}); err != nil {
		return false, err
	}
	return true, nil
}

func validPeerDirection(envelope protocol.Envelope) bool {
	if !strings.HasPrefix(envelope.RequestID, string(protocol.DirectionConnector)+"_") {
		return false
	}
	return envelope.Kind != protocol.KindResponse ||
		strings.HasPrefix(envelope.ReplyTo, string(protocol.DirectionBroker)+"_")
}

func registrationDenied(err error) bool {
	return errors.Is(err, store.ErrNotFound) ||
		errors.Is(err, store.ErrCredentialDisabled) ||
		errors.Is(err, store.ErrAuthorizationDenied)
}

func leaseConflict(err error) bool {
	return errors.Is(err, store.ErrNotFound) ||
		errors.Is(err, store.ErrStaleRevision) ||
		errors.Is(err, store.ErrConflict)
}

func isContextError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func readEnvelope(ctx context.Context, connection *websocket.Conn) (protocol.Envelope, error) {
	messageType, data, err := connection.Read(ctx)
	if err != nil {
		return protocol.Envelope{}, err
	}
	if messageType != websocket.MessageText {
		return protocol.Envelope{}, errors.New("broker accepts only text WebSocket messages")
	}
	return protocol.Read(bytes.NewReader(data))
}

func (s *Server) writeResult(
	ctx context.Context,
	connection *websocket.Conn,
	request protocol.Envelope,
	result any,
) error {
	payload, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("encode broker result: %w", err)
	}
	requestID, err := protocol.NewRequestID(protocol.DirectionBroker)
	if err != nil {
		return err
	}
	return s.writeEnvelope(ctx, connection, protocol.Envelope{
		ProtocolVersion: protocol.Version,
		Kind:            protocol.KindResponse,
		RequestID:       requestID,
		ReplyTo:         request.RequestID,
		ControllerID:    s.controllerID,
		TreeID:          request.TreeID,
		Payload:         payload,
	})
}

func (s *Server) writeError(
	ctx context.Context,
	connection *websocket.Conn,
	request protocol.Envelope,
	code int,
	message string,
) error {
	requestID, err := protocol.NewRequestID(protocol.DirectionBroker)
	if err != nil {
		return err
	}
	return s.writeEnvelope(ctx, connection, protocol.Envelope{
		ProtocolVersion: protocol.Version,
		Kind:            protocol.KindResponse,
		RequestID:       requestID,
		ReplyTo:         request.RequestID,
		ControllerID:    s.controllerID,
		TreeID:          request.TreeID,
		Error:           &protocol.Error{Code: code, Message: message},
	})
}

func (s *Server) writeEnvelope(
	ctx context.Context,
	connection *websocket.Conn,
	envelope protocol.Envelope,
) error {
	data, err := protocol.Marshal(envelope)
	if err != nil {
		return err
	}
	writeContext, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()
	return connection.Write(writeContext, websocket.MessageText, data)
}
