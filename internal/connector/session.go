package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/coder/websocket"
)

type callResult struct {
	payload json.RawMessage
	err     error
}

type writeNotStartedError struct {
	err error
}

func (e *writeNotStartedError) Error() string {
	return e.err.Error()
}

func (e *writeNotStartedError) Unwrap() error {
	return e.err
}

type pendingCall struct {
	treeID      string
	cancellable bool
	written     bool
	result      chan callResult
}

type session struct {
	client             *Client
	connection         *websocket.Conn
	writeMu            sync.Mutex
	pendingMu          sync.Mutex
	pending            map[string]pendingCall
	closeOnce          sync.Once
	done               chan struct{}
	errMu              sync.Mutex
	closeErr           error
	heartbeatSucceeded atomic.Bool
	inboundSem         chan struct{}
	inboundMu          sync.Mutex
	inbound            map[string]context.CancelFunc
	workspaceInbound   sync.WaitGroup
	workspaceStopping  bool
	context            context.Context
	cancel             context.CancelFunc
}

func newSession(client *Client, connection *websocket.Conn) *session {
	ctx, cancel := context.WithCancel(context.Background())
	return &session{
		client: client, connection: connection, pending: map[string]pendingCall{}, done: make(chan struct{}),
		inboundSem: make(chan struct{}, maximumPendingCalls), inbound: make(map[string]context.CancelFunc),
		context: ctx, cancel: cancel,
	}
}

func (s *session) bootstrap(ctx context.Context, hello protocol.Hello) (protocol.HelloResult, error) {
	payload, err := json.Marshal(hello)
	if err != nil {
		return protocol.HelloResult{}, err
	}
	requestID, err := protocol.NewRequestID(protocol.DirectionConnector)
	if err != nil {
		return protocol.HelloResult{}, err
	}
	request := protocol.Envelope{
		ProtocolVersion: protocol.Version,
		Kind:            protocol.KindRequest,
		RequestID:       requestID,
		Method:          protocol.MethodHello,
		ControllerID:    hello.ControllerID,
		Payload:         payload,
	}
	if err := s.write(ctx, request); err != nil {
		return protocol.HelloResult{}, fmt.Errorf("write broker hello: %w", err)
	}
	response, err := readEnvelope(ctx, s.connection)
	if err != nil {
		return protocol.HelloResult{}, fmt.Errorf("read broker hello: %w", err)
	}
	if response.Kind != protocol.KindResponse || !hasDirection(response.RequestID, protocol.DirectionBroker) ||
		response.ReplyTo != requestID || response.ControllerID != hello.ControllerID ||
		response.TreeID != "" || response.Source != nil {
		return protocol.HelloResult{}, errors.New("broker returned an invalid hello response")
	}
	if response.Error != nil {
		return protocol.HelloResult{}, &RPCError{Code: response.Error.Code, Message: response.Error.Message}
	}
	var result protocol.HelloResult
	if err := decodeResult(response.Payload, &result); err != nil {
		return protocol.HelloResult{}, fmt.Errorf("decode broker hello result: %w", err)
	}
	if err := validateHelloResult(result, hello); err != nil {
		return protocol.HelloResult{}, err
	}
	return result, nil
}

func (s *session) call(
	ctx context.Context,
	method, treeID string,
	source *control.PrincipalIdentity,
	params any,
) (json.RawMessage, error) {
	payload, err := json.Marshal(params)
	if err != nil {
		return nil, fmt.Errorf("encode broker %s params: %w", method, err)
	}
	requestID, err := protocol.NewRequestID(protocol.DirectionConnector)
	if err != nil {
		return nil, err
	}
	request := protocol.Envelope{
		ProtocolVersion: protocol.Version,
		Kind:            protocol.KindRequest,
		RequestID:       requestID,
		Method:          method,
		ControllerID:    s.client.hello.ControllerID,
		TreeID:          treeID,
		Payload:         payload,
	}
	if source != nil {
		copy := *source
		request.Source = &copy
	}
	data, err := protocol.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("encode broker %s request: %w", method, err)
	}
	pending := pendingCall{
		treeID: treeID,
		cancellable: method == protocol.MethodWaitMailbox || method == protocol.MethodWaitAgent ||
			method == protocol.MethodSyncWorkspace,
		result: make(chan callResult, 1),
	}
	if err := s.addPending(requestID, pending); err != nil {
		return nil, err
	}
	if err := s.writeData(ctx, data); err != nil {
		if !s.removePending(requestID) {
			result := <-pending.result
			return result.payload, result.err
		}
		var notStarted *writeNotStartedError
		if errors.As(err, &notStarted) {
			return nil, notStarted.err
		}
		s.close(fmt.Errorf("write broker request: %w", err))
		return nil, err
	}
	s.markPendingWritten(requestID)
	return s.waitForCall(ctx, requestID, pending)
}

func (s *session) waitForCall(
	ctx context.Context,
	requestID string,
	pending pendingCall,
) (json.RawMessage, error) {
	select {
	case result := <-pending.result:
		return result.payload, result.err
	case <-ctx.Done():
		if s.removePending(requestID) {
			if pending.cancellable {
				if err := s.cancelRequest(requestID); err != nil {
					s.close(fmt.Errorf("cancel broker request: %w", err))
				}
			}
			return nil, ctx.Err()
		}
		// complete or close already claimed the pending call. Its buffered
		// response is authoritative even if cancellation became ready first.
		result := <-pending.result
		return result.payload, result.err
	}
}

func (s *session) cancelRequest(requestID string) error {
	payload, err := json.Marshal(protocol.CancelRequestParams{RequestID: requestID})
	if err != nil {
		return err
	}
	cancellationID, err := protocol.NewRequestID(protocol.DirectionConnector)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
	defer cancel()
	return s.write(ctx, protocol.Envelope{
		ProtocolVersion: protocol.Version,
		Kind:            protocol.KindNotification,
		RequestID:       cancellationID,
		Method:          protocol.MethodCancelRequest,
		ControllerID:    s.client.hello.ControllerID,
		Payload:         payload,
	})
}

func (s *session) readLoop() {
	for {
		envelope, err := readEnvelope(context.Background(), s.connection)
		if err != nil {
			s.close(fmt.Errorf("read broker message: %w", err))
			return
		}
		if envelope.ControllerID != s.client.hello.ControllerID ||
			!hasDirection(envelope.RequestID, protocol.DirectionBroker) {
			s.close(errors.New("broker message authority or request direction is invalid"))
			return
		}
		switch envelope.Kind {
		case protocol.KindResponse:
			if envelope.Source != nil || !hasDirection(envelope.ReplyTo, protocol.DirectionConnector) {
				s.close(errors.New("broker response is invalid"))
				return
			}
			if err := s.complete(envelope); err != nil {
				s.close(err)
				return
			}
		case protocol.KindRequest:
			if err := s.handleBrokerRequest(envelope); err != nil {
				s.close(err)
				return
			}
		case protocol.KindNotification:
			if envelope.Method == protocol.MethodCancelRequest {
				if err := s.handleBrokerCancellation(envelope); err != nil {
					s.close(err)
					return
				}
			}
			// Other unknown notifications are forward-compatible and intentionally ignored.
		default:
			s.close(errors.New("broker sent an unsupported message kind"))
			return
		}
	}
}

func (s *session) handleBrokerCancellation(envelope protocol.Envelope) error {
	if envelope.TreeID != "" || envelope.Source != nil {
		return errors.New("broker cancellation authority is invalid")
	}
	params, err := protocol.DecodePayload[protocol.CancelRequestParams](envelope.Payload)
	if err != nil || !hasDirection(params.RequestID, protocol.DirectionBroker) {
		return errors.New("broker cancellation payload is invalid")
	}
	s.inboundMu.Lock()
	cancel := s.inbound[params.RequestID]
	s.inboundMu.Unlock()
	if cancel != nil {
		cancel()
	}
	return nil
}

func (s *session) heartbeatLoop(intervalMS int64) {
	interval := time.Duration(intervalMS) * time.Millisecond
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), interval)
			var result protocol.HeartbeatResult
			err := s.client.Call(ctx, protocol.MethodHeartbeat, "", nil, protocol.Heartbeat{}, &result)
			cancel()
			if err != nil {
				s.close(fmt.Errorf("broker heartbeat: %w", err))
				return
			}
			if result.Revision == 0 || result.ServerTime < 0 {
				s.close(errors.New("broker returned an invalid heartbeat result"))
				return
			}
			s.client.updateRevision(s, result.Revision)
			s.heartbeatSucceeded.Store(true)
		}
	}
}

func (s *session) complete(response protocol.Envelope) error {
	s.pendingMu.Lock()
	pending, found := s.pending[response.ReplyTo]
	if !found {
		s.pendingMu.Unlock()
		return nil
	}
	if response.TreeID != pending.treeID {
		s.pendingMu.Unlock()
		return errors.New("broker response treeId does not match its request")
	}
	delete(s.pending, response.ReplyTo)
	s.pendingMu.Unlock()
	result := callResult{payload: response.Payload}
	if response.Error != nil {
		result.err = &RPCError{Code: response.Error.Code, Message: response.Error.Message}
	}
	pending.result <- result
	return nil
}

func (s *session) addPending(requestID string, pending pendingCall) error {
	select {
	case <-s.done:
		return s.err()
	default:
	}
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	select {
	case <-s.done:
		return s.err()
	default:
	}
	if len(s.pending) >= maximumPendingCalls {
		return ErrBusy
	}
	s.pending[requestID] = pending
	return nil
}

func (s *session) removePending(requestID string) bool {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	if _, found := s.pending[requestID]; !found {
		return false
	}
	delete(s.pending, requestID)
	return true
}

func (s *session) markPendingWritten(requestID string) {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	pending, found := s.pending[requestID]
	if !found {
		return
	}
	pending.written = true
	s.pending[requestID] = pending
}

func (s *session) writeError(request protocol.Envelope, code int, message string) error {
	requestID, err := protocol.NewRequestID(protocol.DirectionConnector)
	if err != nil {
		return err
	}
	return s.write(context.Background(), protocol.Envelope{
		ProtocolVersion: protocol.Version,
		Kind:            protocol.KindResponse,
		RequestID:       requestID,
		ReplyTo:         request.RequestID,
		ControllerID:    s.client.hello.ControllerID,
		TreeID:          request.TreeID,
		Error:           &protocol.Error{Code: code, Message: message},
	})
}

func (s *session) writeResult(request protocol.Envelope, result any) error {
	payload, err := json.Marshal(result)
	if err != nil {
		return err
	}
	requestID, err := protocol.NewRequestID(protocol.DirectionConnector)
	if err != nil {
		return err
	}
	return s.write(context.Background(), protocol.Envelope{
		ProtocolVersion: protocol.Version,
		Kind:            protocol.KindResponse,
		RequestID:       requestID,
		ReplyTo:         request.RequestID,
		ControllerID:    s.client.hello.ControllerID,
		TreeID:          request.TreeID,
		Payload:         payload,
	})
}

func (s *session) write(ctx context.Context, envelope protocol.Envelope) error {
	data, err := protocol.Marshal(envelope)
	if err != nil {
		return err
	}
	return s.writeData(ctx, data)
}

func (s *session) writeData(ctx context.Context, data []byte) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := ctx.Err(); err != nil {
		return &writeNotStartedError{err: err}
	}
	writeContext, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()
	return s.connection.Write(writeContext, websocket.MessageText, data)
}

func (s *session) close(err error) {
	s.closeOnce.Do(func() {
		if err == nil {
			err = ErrUnavailable
		}
		s.errMu.Lock()
		s.closeErr = err
		s.errMu.Unlock()
		s.cancel()
		s.pendingMu.Lock()
		for requestID, pending := range s.pending {
			delete(s.pending, requestID)
			pending.result <- callResult{err: err}
		}
		s.pendingMu.Unlock()
		close(s.done)
		_ = s.connection.CloseNow()
	})
}

func (s *session) err() error {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	if s.closeErr == nil {
		return ErrUnavailable
	}
	return s.closeErr
}

func readEnvelope(ctx context.Context, connection *websocket.Conn) (protocol.Envelope, error) {
	messageType, data, err := connection.Read(ctx)
	if err != nil {
		return protocol.Envelope{}, err
	}
	if messageType != websocket.MessageText {
		return protocol.Envelope{}, errors.New("connector accepts only text WebSocket messages")
	}
	return protocol.Read(bytes.NewReader(data))
}
