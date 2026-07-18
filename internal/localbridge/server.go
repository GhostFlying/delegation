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

	"github.com/GhostFlying/delegation/internal/connector"
	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/protocol"
)

const (
	maximumConcurrentCalls = 32
	localCallTimeout       = 30 * time.Second
)

type Backend interface {
	Call(context.Context, string, string, *control.PrincipalIdentity, any, any) error
}

type Server struct {
	listener  net.Listener
	role      control.DeviceRole
	backend   Backend
	sem       chan struct{}
	started   atomic.Bool
	closeOnce sync.Once
	wait      sync.WaitGroup
	serveDone chan struct{}
}

func Listen(endpoint string, role control.DeviceRole, backend Backend) (*Server, error) {
	if err := role.Validate(); err != nil {
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
		listener: listener, role: role, backend: backend, sem: make(chan struct{}, maximumConcurrentCalls),
		serveDone: make(chan struct{}),
	}, nil
}

func (s *Server) Serve(ctx context.Context) error {
	if !s.started.CompareAndSwap(false, true) {
		return errors.New("local bridge server is already running")
	}
	defer close(s.serveDone)
	stop := context.AfterFunc(ctx, func() {
		_ = s.listener.Close()
	})
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
		case s.sem <- struct{}{}:
			s.wait.Add(1)
			go s.handle(ctx, connection)
		default:
			_ = connection.Close()
		}
	}
}

func (s *Server) Close() error {
	s.closeOnce.Do(func() {
		_ = s.listener.Close()
	})
	if s.started.Load() {
		<-s.serveDone
	}
	return nil
}

func (s *Server) handle(serverContext context.Context, connection net.Conn) {
	defer s.wait.Done()
	defer func() { <-s.sem }()
	defer connection.Close()
	ctx, cancel := context.WithTimeout(serverContext, localCallTimeout)
	defer cancel()
	if deadline, ok := ctx.Deadline(); ok {
		if err := connection.SetDeadline(deadline); err != nil {
			return
		}
	}
	request, err := readJSONFrame[request](connection)
	if err != nil {
		return
	}
	if err := request.validate(); err != nil {
		s.writeError(connection, request.RequestID, protocol.ErrorInvalidRequest, "invalid local request")
		connection.Close()
		return
	}
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
	if s.role != control.DeviceRoleController {
		return nil, &protocol.Error{Code: protocol.ErrorForbidden, Message: "controller bridge required"}
	}
	switch request.Method {
	case protocol.MethodEnsureRootTree:
		if request.TreeID != "" || request.Source != nil {
			return nil, &protocol.Error{Code: protocol.ErrorInvalidRequest, Message: "invalid root tree request"}
		}
	case protocol.MethodListDevices, protocol.MethodDescribeDevice:
		if request.TreeID == "" || request.Source == nil {
			return nil, &protocol.Error{Code: protocol.ErrorInvalidRequest, Message: "device request requires a principal"}
		}
	default:
		return nil, &protocol.Error{Code: protocol.ErrorMethodNotFound, Message: "method not found"}
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
