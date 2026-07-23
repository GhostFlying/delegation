package localbridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/connector"
	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/protocol"
)

const (
	rootMailboxWaitHeadroom       = 16
	maximumConcurrentWaitCalls    = config.MaximumWorkerSlots + rootMailboxWaitHeadroom
	maximumConcurrentControlCalls = 32
	maximumConcurrentCalls        = maximumConcurrentWaitCalls + maximumConcurrentControlCalls
	localCallTimeout              = 130 * time.Second
	localWorkspaceCallTimeout     = 305 * time.Second
)

type Backend interface {
	Call(context.Context, string, string, *control.PrincipalIdentity, any, any) error
}

type Authorizer interface {
	ManagedWorkerThread(context.Context, string, string) (bool, error)
	AuthorizeWorker(context.Context, control.PrincipalIdentity) error
}

type Server struct {
	listener      net.Listener
	identity      ServiceIdentity
	backend       Backend
	authorizer    Authorizer
	connectionSem chan struct{}
	waitSem       chan struct{}
	controlSem    chan struct{}
	started       atomic.Bool
	closeOnce     sync.Once
	wait          sync.WaitGroup
	serveDone     chan struct{}
	mu            sync.Mutex
	connections   map[net.Conn]struct{}
	closed        bool
	closeErr      error
}

func Listen(endpoint string, identity ServiceIdentity, backend Backend) (*Server, error) {
	return ListenWithAuthorization(endpoint, identity, backend, nil)
}

func ListenWithAuthorization(
	endpoint string,
	identity ServiceIdentity,
	backend Backend,
	authorizer Authorizer,
) (*Server, error) {
	if err := identity.Validate(); err != nil {
		return nil, err
	}
	if backend == nil {
		return nil, errors.New("local bridge backend is required")
	}
	listener, err := listen(endpoint)
	if err != nil {
		return nil, fmt.Errorf("listen on local delegation endpoint: %w", err)
	}
	return &Server{
		listener: listener, identity: identity, backend: backend, authorizer: authorizer,
		connectionSem: make(chan struct{}, maximumConcurrentCalls),
		waitSem:       make(chan struct{}, maximumConcurrentWaitCalls),
		controlSem:    make(chan struct{}, maximumConcurrentControlCalls),
		serveDone:     make(chan struct{}), connections: make(map[net.Conn]struct{}),
	}, nil
}

func (s *Server) Serve(ctx context.Context) error {
	if !s.started.CompareAndSwap(false, true) {
		return errors.New("local bridge server is already running")
	}
	defer close(s.serveDone)
	stop := context.AfterFunc(ctx, s.stop)
	defer stop()
	defer s.wait.Wait()
	for {
		connection, err := s.listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return fmt.Errorf("accept local delegation connection: %w", err)
		}
		select {
		case s.connectionSem <- struct{}{}:
			if !s.track(connection) {
				<-s.connectionSem
				_ = connection.Close()
				continue
			}
			s.wait.Add(1)
			go s.handle(ctx, connection)
		default:
			_ = connection.Close()
		}
	}
}

func (s *Server) Close() error {
	s.stop()
	if s.started.Load() {
		<-s.serveDone
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeErr
}

func (s *Server) stop() {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		connections := make([]net.Conn, 0, len(s.connections))
		for connection := range s.connections {
			connections = append(connections, connection)
		}
		s.mu.Unlock()

		var failures []error
		if err := s.listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			failures = append(failures, fmt.Errorf("close local delegation listener: %w", err))
		}
		for _, connection := range connections {
			if err := connection.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				failures = append(failures, fmt.Errorf("close local delegation connection: %w", err))
			}
		}
		s.mu.Lock()
		s.closeErr = errors.Join(failures...)
		s.mu.Unlock()
	})
}

func (s *Server) track(connection net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	s.connections[connection] = struct{}{}
	return true
}

func (s *Server) untrack(connection net.Conn) {
	s.mu.Lock()
	delete(s.connections, connection)
	s.mu.Unlock()
}

func (s *Server) handle(serverContext context.Context, connection net.Conn) {
	defer s.wait.Done()
	defer func() { <-s.connectionSem }()
	defer s.untrack(connection)
	defer connection.Close()
	readContext, cancelRead := context.WithTimeout(serverContext, localCallTimeout)
	defer cancelRead()
	if deadline, ok := readContext.Deadline(); ok {
		if err := connection.SetDeadline(deadline); err != nil {
			return
		}
	}
	request, err := readJSONFrame[request](connection)
	cancelRead()
	if err != nil {
		return
	}
	if err := request.validate(); err != nil {
		s.writeError(connection, request.RequestID, protocol.ErrorInvalidRequest, "invalid local request")
		connection.Close()
		return
	}
	callTimeout := localCallTimeout
	if request.Method == protocol.MethodSyncWorkspace {
		callTimeout = localWorkspaceCallTimeout
	}
	ctx, cancel := context.WithTimeout(serverContext, callTimeout)
	defer cancel()
	if deadline, ok := ctx.Deadline(); ok {
		if err := connection.SetDeadline(deadline); err != nil {
			return
		}
	}
	releaseCall, admitted := s.admitCall(request.Method)
	if !admitted {
		s.writeError(connection, request.RequestID, protocol.ErrorUnavailable, "local delegation service is busy")
		return
	}
	defer releaseCall()
	peerContext, cancelPeer := context.WithCancel(ctx)
	peerClosed := make(chan struct{})
	go func() {
		defer close(peerClosed)
		var extra [1]byte
		_, _ = connection.Read(extra[:])
		cancelPeer()
	}()
	defer func() {
		cancelPeer()
		_ = connection.Close()
		<-peerClosed
	}()
	payload, rpcErr := s.call(peerContext, request)
	requestID, err := newLocalID()
	if err != nil {
		return
	}
	reply := response{Version: Version, RequestID: requestID, ReplyTo: request.RequestID, Payload: payload}
	if rpcErr != nil {
		reply.Payload = nil
		reply.Error = rpcErr
	}
	_ = writeJSONFrame(connection, reply)
}

func (s *Server) call(ctx context.Context, request request) (json.RawMessage, *protocol.Error) {
	if request.Method == methodIdentity {
		if request.TreeID != "" || request.Source != nil {
			return nil, &protocol.Error{Code: protocol.ErrorInvalidRequest, Message: "invalid bridge identity request"}
		}
		var params struct{}
		if err := decodeResult(request.Payload, &params); err != nil {
			return nil, &protocol.Error{Code: protocol.ErrorInvalidRequest, Message: "invalid bridge identity request"}
		}
		result, err := json.Marshal(s.identity)
		if err != nil {
			return nil, &protocol.Error{Code: protocol.ErrorInternal, Message: "encode bridge identity"}
		}
		return result, nil
	}
	switch request.Method {
	case protocol.MethodEnsureRootTree:
		if request.TreeID != "" || request.Source != nil {
			return nil, &protocol.Error{Code: protocol.ErrorInvalidRequest, Message: "invalid root tree request"}
		}
		params, err := protocol.DecodePayload[protocol.EnsureRootTreeParams](request.Payload)
		if err != nil || params.Validate() != nil {
			return nil, &protocol.Error{Code: protocol.ErrorInvalidParams, Message: "invalid root tree payload"}
		}
		if s.authorizer == nil {
			return nil, &protocol.Error{Code: protocol.ErrorUnavailable, Message: "local authorization unavailable"}
		}
		managed, err := s.authorizer.ManagedWorkerThread(
			ctx, s.identity.ControllerID, params.ExternalThreadID,
		)
		if err != nil {
			return nil, &protocol.Error{Code: protocol.ErrorUnavailable, Message: "local authorization unavailable"}
		}
		if managed {
			return nil, &protocol.Error{Code: protocol.ErrorForbidden, Message: "managed worker thread cannot create a root tree"}
		}
	case protocol.MethodListDevices, protocol.MethodDescribeDevice,
		protocol.MethodSpawnAgent, protocol.MethodListAgents,
		protocol.MethodSendAgent, protocol.MethodFollowupAgent, protocol.MethodInterruptAgent,
		protocol.MethodWaitAgent, protocol.MethodSendMessage, protocol.MethodWaitMailbox,
		protocol.MethodSyncWorkspace:
		if request.TreeID == "" || request.Source == nil {
			return nil, &protocol.Error{Code: protocol.ErrorInvalidRequest, Message: "request requires a principal"}
		}
	default:
		return nil, &protocol.Error{Code: protocol.ErrorMethodNotFound, Message: "method not found"}
	}
	if request.Source != nil &&
		(request.Source.ControllerID != s.identity.ControllerID || request.Source.DeviceID != s.identity.DeviceID) {
		return nil, &protocol.Error{Code: protocol.ErrorForbidden, Message: "principal is not local to this peer"}
	}
	if request.Source != nil && request.Source.ParentAgentID != "" {
		if request.Method != protocol.MethodSendMessage && request.Method != protocol.MethodWaitMailbox {
			return nil, &protocol.Error{Code: protocol.ErrorForbidden, Message: "managed worker method is not allowed"}
		}
		if s.authorizer == nil || s.authorizer.AuthorizeWorker(ctx, *request.Source) != nil {
			return nil, &protocol.Error{Code: protocol.ErrorForbidden, Message: "managed worker is not authorized"}
		}
	}
	var result json.RawMessage
	err := s.backend.Call(
		ctx, request.Method, request.TreeID, request.Source, request.Payload, &result,
	)
	if err == nil {
		return result, nil
	}
	var brokerError *connector.RPCError
	if errors.As(err, &brokerError) {
		return nil, &protocol.Error{Code: brokerError.Code, Message: brokerError.Message}
	}
	if errors.Is(err, connector.ErrUnavailable) || errors.Is(err, connector.ErrBusy) || isContextError(err) {
		return nil, &protocol.Error{Code: protocol.ErrorUnavailable, Message: "delegation service unavailable"}
	}
	return nil, &protocol.Error{Code: protocol.ErrorInternal, Message: "delegation service failed"}
}

func (s *Server) admitCall(method string) (func(), bool) {
	pool := s.controlSem
	if method == protocol.MethodWaitMailbox || method == protocol.MethodWaitAgent {
		pool = s.waitSem
	}
	select {
	case pool <- struct{}{}:
		return func() { <-pool }, true
	default:
		return nil, false
	}
}

func (s *Server) writeError(connection net.Conn, replyTo string, code int, message string) {
	if validateLocalID(replyTo) != nil {
		return
	}
	requestID, err := newLocalID()
	if err != nil {
		return
	}
	_ = writeJSONFrame(connection, response{
		Version:   Version,
		RequestID: requestID,
		ReplyTo:   replyTo,
		Error:     &protocol.Error{Code: code, Message: message},
	})
}

func isContextError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
