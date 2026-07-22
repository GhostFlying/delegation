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
	"slices"
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
	HealthServiceHeader      = "X-Delegation-Service"
	HealthControllerHeader   = "X-Delegation-Controller-Id"
	defaultHeartbeatInterval = 15 * time.Second
	writeTimeout             = 10 * time.Second
	cleanupTimeout           = 5 * time.Second
	maximumPendingHellos     = 64
	maximumDeviceHellos      = 2
	defaultOfflineRetry      = time.Second
	rootMailboxWaitHeadroom  = 16
	maximumAsyncMailboxWaits = config.MaximumWorkerSlots + rootMailboxWaitHeadroom
	mailboxRequestTimeout    = 30 * time.Second
)

type Registry interface {
	AuthenticateCredential(context.Context, store.CredentialMAC) (store.Credential, error)
	RegisterAuthenticatedDevice(context.Context, store.CredentialMAC, control.DeviceDescriptor, time.Time) (control.Device, error)
	RegisterTrustedDevice(context.Context, control.DeviceDescriptor, time.Time) (control.Device, error)
	HeartbeatDevice(context.Context, string, string, uint64, time.Time) (control.Device, error)
	MarkDeviceOffline(context.Context, string, string, uint64, time.Time) (control.Device, error)
	BeginBrokerEpoch(context.Context, string) (store.PresenceTransition, error)
	EnsureRootTree(context.Context, string, string, string, time.Time) (control.Tree, control.Principal, error)
	AuthorizePrincipal(context.Context, control.PrincipalIdentity, control.Capability) (control.Principal, error)
	ListDevices(context.Context, string, store.DevicePageRequest) (store.DevicePage, error)
	DescribeDevice(context.Context, string, string) (store.DeviceRecord, error)
	SendMailboxMessage(context.Context, control.Principal, protocol.MessageTarget, string, string, time.Time) (store.MailboxDelivery, error)
	ReadMailbox(context.Context, control.Principal, uint64, int) (protocol.WaitMailboxResult, error)
}

type Options struct {
	ControllerID      string
	AuthMode          config.AuthMode
	MasterToken       *tokenfile.Token
	Registry          Registry
	HeartbeatInterval time.Duration
	Now               func() time.Time
	NewID             func() (string, error)
	ReportError       func(error)
}

type Server struct {
	controllerID      string
	authMode          config.AuthMode
	masterToken       tokenfile.Token
	registry          Registry
	heartbeatInterval time.Duration
	newID             func() (string, error)
	now               func() time.Time
	reportError       func(error)
	context           context.Context
	cancel            context.CancelFunc

	mu               sync.Mutex
	connections      map[string]*session
	peers            map[*websocket.Conn]struct{}
	pendingHellos    int
	deviceHellos     map[string]int
	helloLimit       int
	deviceHelloLimit int
	closed           bool
	handlers         sync.WaitGroup
	background       sync.WaitGroup
	mailboxNotifier  *mailboxNotifier

	retryMu              sync.Mutex
	offlineRetries       map[string]uint64
	offlineRetryWake     chan struct{}
	offlineRetryInterval time.Duration

	shutdownOnce sync.Once
	shutdownDone chan struct{}
	shutdownErr  error
}

type peerAuthority struct {
	credentialMAC *store.CredentialMAC
	deviceID      string
}

type session struct {
	server        *Server
	connection    *websocket.Conn
	connectionID  string
	deviceID      string
	credentialMAC *store.CredentialMAC
	revision      atomic.Uint64
	asyncSem      chan struct{}
	async         sync.WaitGroup
	asyncMu       sync.Mutex
	asyncCancels  map[string]context.CancelFunc
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
	newID := options.NewID
	if newID == nil {
		newID = identity.NewID
	}
	reportError := options.ReportError
	if reportError == nil {
		reportError = func(err error) {
			slog.Error("delegation broker failure", "error", err)
		}
	}
	server := &Server{
		controllerID:         options.ControllerID,
		authMode:             options.AuthMode,
		registry:             options.Registry,
		heartbeatInterval:    heartbeatInterval,
		newID:                newID,
		now:                  now,
		reportError:          reportError,
		connections:          map[string]*session{},
		peers:                map[*websocket.Conn]struct{}{},
		deviceHellos:         map[string]int{},
		helloLimit:           maximumPendingHellos,
		deviceHelloLimit:     maximumDeviceHellos,
		mailboxNotifier:      newMailboxNotifier(),
		offlineRetries:       map[string]uint64{},
		offlineRetryWake:     make(chan struct{}, 1),
		offlineRetryInterval: defaultOfflineRetry,
		shutdownDone:         make(chan struct{}),
	}
	server.context, server.cancel = context.WithCancel(context.Background())
	server.background.Add(1)
	go server.retryOfflineLoop()
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
		writer.Header().Set(HealthServiceHeader, "broker")
		writer.Header().Set(HealthControllerHeader, s.controllerID)
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
		s.background.Wait()

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

func (s *Server) reserveHello() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.pendingHellos >= s.helloLimit {
		return false
	}
	s.pendingHellos++
	return true
}

func (s *Server) reserveDeviceHello(deviceID string) bool {
	if deviceID == "" {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.deviceHellos[deviceID] >= s.deviceHelloLimit {
		return false
	}
	s.deviceHellos[deviceID]++
	return true
}

func (s *Server) releaseHello(deviceID string) {
	s.mu.Lock()
	s.pendingHellos--
	if deviceID != "" {
		remaining := s.deviceHellos[deviceID] - 1
		if remaining == 0 {
			delete(s.deviceHellos, deviceID)
		} else {
			s.deviceHellos[deviceID] = remaining
		}
	}
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
	if !s.reserveHello() {
		http.Error(writer, "too many pending connector handshakes", http.StatusServiceUnavailable)
		return
	}
	helloDeviceID := ""
	helloReserved := true
	defer func() {
		if helloReserved {
			s.releaseHello(helloDeviceID)
		}
	}()

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
	if !s.reserveDeviceHello(authority.deviceID) {
		http.Error(writer, "too many pending connector handshakes", http.StatusServiceUnavailable)
		return
	}
	helloDeviceID = authority.deviceID
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
	s.releaseHello(helloDeviceID)
	helloReserved = false
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
	registered, err := s.registry.AuthenticateCredential(ctx, mac)
	if err != nil {
		return peerAuthority{}, err
	}
	return peerAuthority{credentialMAC: &mac, deviceID: registered.DeviceID}, nil
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
	for _, feature := range []string{
		protocol.FeatureDeviceRegistry,
		protocol.FeatureFullDuplexRPC,
		protocol.FeatureMailbox,
		protocol.FeaturePeerRoot,
	} {
		if !slices.Contains(hello.Features, feature) {
			_ = s.writeError(ctx, connection, envelope, protocol.ErrorInvalidParams, "peer does not support required protocol features")
			return nil, errors.New("peer does not support required protocol features")
		}
	}
	connectionID, err := s.newID()
	if err != nil {
		internal := &internalError{operation: "create connection ID", err: err}
		s.reportError(internal)
		_ = s.writeError(ctx, connection, envelope, protocol.ErrorInternal, "broker failed to create connection")
		return nil, internal
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
	current := &session{
		server:        s,
		connection:    connection,
		connectionID:  connectionID,
		deviceID:      device.DeviceID,
		credentialMAC: authority.credentialMAC,
		asyncSem:      make(chan struct{}, maximumAsyncMailboxWaits),
		asyncCancels:  make(map[string]context.CancelFunc),
	}
	current.revision.Store(device.Revision)
	result := protocol.HelloResult{
		ConnectionID: connectionID,
		Features: []string{
			protocol.FeatureDeviceRegistry,
			protocol.FeatureFullDuplexRPC,
			protocol.FeatureMailbox,
			protocol.FeaturePeerRoot,
		},
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
		s.enqueueOfflineRetry(current.deviceID, current.revision.Load())
	}
}

func (s *Server) enqueueOfflineRetry(deviceID string, revision uint64) {
	if s.context.Err() != nil {
		return
	}
	s.retryMu.Lock()
	if current := s.offlineRetries[deviceID]; revision > current {
		s.offlineRetries[deviceID] = revision
	}
	s.retryMu.Unlock()
	select {
	case s.offlineRetryWake <- struct{}{}:
	default:
	}
}

func (s *Server) retryOfflineLoop() {
	defer s.background.Done()
	for {
		select {
		case <-s.context.Done():
			return
		case <-s.offlineRetryWake:
		}
		for {
			timer := time.NewTimer(s.offlineRetryInterval)
			select {
			case <-s.context.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			if !s.retryOfflinePass() {
				break
			}
		}
	}
}

func (s *Server) retryOfflinePass() bool {
	s.retryMu.Lock()
	retries := make(map[string]uint64, len(s.offlineRetries))
	for deviceID, revision := range s.offlineRetries {
		retries[deviceID] = revision
	}
	s.retryMu.Unlock()

	for deviceID, revision := range retries {
		ctx, cancel := context.WithTimeout(s.context, cleanupTimeout)
		_, err := s.registry.MarkDeviceOffline(ctx, s.controllerID, deviceID, revision, s.now())
		cancel()
		if err != nil && !errors.Is(err, store.ErrStaleRevision) {
			continue
		}
		s.retryMu.Lock()
		if s.offlineRetries[deviceID] == revision {
			delete(s.offlineRetries, deviceID)
		}
		s.retryMu.Unlock()
	}

	s.retryMu.Lock()
	pending := len(s.offlineRetries) != 0
	s.retryMu.Unlock()
	return pending
}

func (s *session) run(ctx context.Context) error {
	sessionContext, cancelSession := context.WithCancel(ctx)
	defer func() {
		cancelSession()
		s.async.Wait()
	}()
	deadline := time.Now().Add(s.server.heartbeatInterval * 3)
	for {
		messageContext, cancel := context.WithDeadline(ctx, deadline)
		envelope, err := readEnvelope(messageContext, s.connection)
		if err == nil {
			var renewed bool
			renewed, err = s.handleEnvelope(messageContext, sessionContext, envelope)
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

func (s *session) handleEnvelope(
	ctx context.Context,
	sessionContext context.Context,
	envelope protocol.Envelope,
) (bool, error) {
	if !validPeerDirection(envelope) {
		return false, errors.New("connection used an invalid request direction")
	}
	if envelope.ControllerID != s.server.controllerID {
		if envelope.Kind == protocol.KindRequest {
			_ = s.server.writeError(ctx, s.connection, envelope, protocol.ErrorForbidden, "controller mismatch")
		}
		return false, errors.New("connection sent a mismatched controllerId")
	}
	if err := s.validateAuthority(ctx); err != nil {
		if registrationDenied(err) {
			if envelope.Kind == protocol.KindRequest {
				_ = s.server.writeError(ctx, s.connection, envelope, protocol.ErrorForbidden, "session credential is no longer valid")
			}
			return false, err
		}
		if isContextError(err) {
			return false, err
		}
		if envelope.Kind == protocol.KindRequest {
			_ = s.server.writeError(ctx, s.connection, envelope, protocol.ErrorUnavailable, "broker unavailable")
		}
		return false, &internalError{operation: "reauthenticate session", err: err}
	}
	switch envelope.Kind {
	case protocol.KindNotification:
		if envelope.Method == protocol.MethodCancelRequest {
			return false, s.handleCancelRequest(envelope)
		}
		return false, nil
	case protocol.KindResponse:
		return false, nil
	case protocol.KindRequest:
	default:
		return false, errors.New("connection sent an unsupported envelope kind")
	}
	switch envelope.Method {
	case protocol.MethodEnsureRootTree:
		return false, s.handleEnsureRootTree(ctx, envelope)
	case protocol.MethodListDevices:
		return false, s.handleListDevices(ctx, envelope)
	case protocol.MethodDescribeDevice:
		return false, s.handleDescribeDevice(ctx, envelope)
	case protocol.MethodSendMessage:
		return false, s.handleSendMessage(ctx, envelope)
	case protocol.MethodWaitMailbox:
		return false, s.startMailboxWait(ctx, sessionContext, envelope)
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

func (s *session) startMailboxWait(
	responseContext context.Context,
	sessionContext context.Context,
	request protocol.Envelope,
) error {
	select {
	case s.asyncSem <- struct{}{}:
	default:
		return s.server.writeError(
			responseContext,
			s.connection,
			request,
			protocol.ErrorUnavailable,
			"too many pending mailbox waits",
		)
	}
	waitContext, cancelWait := context.WithTimeout(sessionContext, mailboxRequestTimeout)
	cancellationContext, cancelRequest := context.WithCancel(context.Background())
	s.asyncMu.Lock()
	if _, exists := s.asyncCancels[request.RequestID]; exists {
		s.asyncMu.Unlock()
		cancelWait()
		cancelRequest()
		<-s.asyncSem
		return errors.New("duplicate asynchronous requestId")
	}
	s.asyncCancels[request.RequestID] = cancelRequest
	s.asyncMu.Unlock()
	s.async.Add(1)
	go func() {
		defer s.async.Done()
		defer func() {
			s.asyncMu.Lock()
			delete(s.asyncCancels, request.RequestID)
			s.asyncMu.Unlock()
			cancelRequest()
			cancelWait()
			<-s.asyncSem
		}()
		if err := s.handleWaitMailbox(
			waitContext,
			cancellationContext.Done(),
			request,
		); err != nil && !isContextError(err) {
			var internal *internalError
			if errors.As(err, &internal) {
				s.server.reportError(internal)
			}
			_ = s.connection.CloseNow()
		}
	}()
	return nil
}

func (s *session) handleCancelRequest(request protocol.Envelope) error {
	if request.TreeID != "" || request.Source != nil {
		return errors.New("request cancellation must not contain a principal")
	}
	params, err := protocol.DecodePayload[protocol.CancelRequestParams](request.Payload)
	if err != nil || params.Validate() != nil {
		return errors.New("invalid request cancellation payload")
	}
	s.asyncMu.Lock()
	cancel := s.asyncCancels[params.RequestID]
	s.asyncMu.Unlock()
	if cancel != nil {
		cancel()
	}
	return nil
}

func (s *session) validateAuthority(ctx context.Context) error {
	if s.credentialMAC == nil {
		return nil
	}
	credential, err := s.server.registry.AuthenticateCredential(ctx, *s.credentialMAC)
	if err != nil {
		return err
	}
	if credential.ControllerID != s.server.controllerID || credential.DeviceID != s.deviceID {
		return store.ErrAuthorizationDenied
	}
	return nil
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
