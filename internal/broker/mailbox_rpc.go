package broker

import (
	"context"
	"errors"
	"time"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
)

func (s *session) handleSendMessage(ctx context.Context, request protocol.Envelope) error {
	principal, authorized, err := s.authorizeMailbox(
		ctx,
		request,
		control.CapabilityMessageSendParent,
	)
	if !authorized {
		return err
	}
	params, err := protocol.DecodePayload[protocol.SendMessageParams](request.Payload)
	if err != nil || params.Validate() != nil {
		return s.server.writeError(
			ctx, s.connection, request, protocol.ErrorInvalidParams, "invalid message payload",
		)
	}
	delivery, err := s.server.registry.SendMailboxMessage(
		ctx, principal, params.Target, params.MessageID, params.Message, s.server.now(),
	)
	if err != nil {
		return s.handleMailboxStoreError(ctx, request, "send mailbox message", err)
	}
	s.server.mailboxNotifier.notify(mailboxKey{
		controllerID: principal.ControllerID,
		treeID:       principal.TreeID,
		agentID:      delivery.RecipientAgentID,
	})
	return s.server.writeResult(ctx, s.connection, request, protocol.SendMessageResult{
		MessageID: delivery.MessageID,
		Sequence:  delivery.Sequence,
	})
}

func (s *session) handleWaitMailbox(ctx context.Context, request protocol.Envelope) error {
	principal, authorized, err := s.authorizeMailbox(
		ctx,
		request,
		control.CapabilityMessageReceiveSelf,
	)
	if !authorized {
		return err
	}
	params, err := protocol.DecodePayload[protocol.WaitMailboxParams](request.Payload)
	if err != nil || params.Validate() != nil {
		return s.server.writeError(
			ctx, s.connection, request, protocol.ErrorInvalidParams, "invalid mailbox wait payload",
		)
	}
	key := mailboxKey{
		controllerID: principal.ControllerID,
		treeID:       principal.TreeID,
		agentID:      principal.AgentID,
	}
	deadline := time.Now().Add(time.Duration(params.TimeoutMillis) * time.Millisecond)
	for {
		subscription := s.server.mailboxNotifier.subscribe(key)
		result, err := s.server.registry.ReadMailbox(
			ctx, principal, params.Cursor, params.Limit,
		)
		if err != nil {
			subscription.release()
			return s.handleMailboxStoreError(ctx, request, "read mailbox", err)
		}
		if len(result.Messages) != 0 || params.TimeoutMillis == 0 {
			subscription.release()
			return s.server.writeResult(ctx, s.connection, request, result)
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			subscription.release()
			return s.server.writeResult(ctx, s.connection, request, result)
		}
		timer := time.NewTimer(remaining)
		select {
		case <-ctx.Done():
			timer.Stop()
			subscription.release()
			return ctx.Err()
		case <-subscription.channel():
			timer.Stop()
			subscription.release()
		case <-timer.C:
			subscription.release()
			return s.server.writeResult(ctx, s.connection, request, result)
		}
	}
}

func (s *session) authorizeMailbox(
	ctx context.Context,
	request protocol.Envelope,
	workerCapability control.Capability,
) (control.Principal, bool, error) {
	if request.TreeID == "" || request.Source == nil {
		return control.Principal{}, false, s.server.writeError(
			ctx, s.connection, request, protocol.ErrorInvalidRequest, "mailbox request requires a principal",
		)
	}
	if request.Source.DeviceID != s.deviceID {
		return control.Principal{}, false, s.server.writeError(
			ctx, s.connection, request, protocol.ErrorForbidden, "mailbox access denied",
		)
	}
	required := control.CapabilityMessageTree
	if request.Source.ParentAgentID != "" {
		required = workerCapability
	}
	principal, err := s.server.registry.AuthorizePrincipal(ctx, *request.Source, required)
	if err == nil {
		return principal, true, nil
	}
	if isContextError(err) {
		return control.Principal{}, false, err
	}
	if errors.Is(err, store.ErrAuthorizationDenied) {
		return control.Principal{}, false, s.server.writeError(
			ctx, s.connection, request, protocol.ErrorForbidden, "mailbox access denied",
		)
	}
	_ = s.server.writeError(
		ctx, s.connection, request, protocol.ErrorUnavailable, "broker unavailable",
	)
	return control.Principal{}, false, &internalError{operation: "authorize mailbox access", err: err}
}

func (s *session) handleMailboxStoreError(
	ctx context.Context,
	request protocol.Envelope,
	operation string,
	err error,
) error {
	if isContextError(err) {
		return err
	}
	if errors.Is(err, store.ErrAuthorizationDenied) {
		return s.server.writeError(
			ctx, s.connection, request, protocol.ErrorForbidden, "mailbox access denied",
		)
	}
	if errors.Is(err, store.ErrNotFound) {
		return s.server.writeError(
			ctx, s.connection, request, protocol.ErrorNotFound, "message target not found",
		)
	}
	if errors.Is(err, store.ErrMailboxCursorAhead) {
		return s.server.writeError(
			ctx, s.connection, request, protocol.ErrorConflict, "mailbox cursor is ahead of stored messages",
		)
	}
	if errors.Is(err, store.ErrConflict) {
		return s.server.writeError(
			ctx, s.connection, request, protocol.ErrorConflict, "messageId conflicts with an existing message",
		)
	}
	if errors.Is(err, store.ErrMailboxFull) {
		return s.server.writeError(
			ctx, s.connection, request, protocol.ErrorUnavailable, "mailbox pending message quota exceeded",
		)
	}
	_ = s.server.writeError(
		ctx, s.connection, request, protocol.ErrorUnavailable, "broker unavailable",
	)
	return &internalError{operation: operation, err: err}
}
