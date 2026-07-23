package broker

import (
	"context"
	"errors"
	"time"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
)

func (s *session) startAgentWait(
	responseContext context.Context,
	sessionContext context.Context,
	request protocol.Envelope,
) error {
	select {
	case s.asyncSem <- struct{}{}:
	default:
		return s.writeError(
			responseContext, request, protocol.ErrorUnavailable,
			"too many pending agent waits",
		)
	}
	waitContext, cancelWait := context.WithTimeout(sessionContext, agentWaitRequestTimeout)
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
		if err := s.handleWaitAgent(
			waitContext, cancellationContext.Done(), request,
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

func (s *session) handleWaitAgent(
	ctx context.Context,
	canceled <-chan struct{},
	request protocol.Envelope,
) error {
	principal, authorized, err := s.authorizeAgentWait(ctx, request)
	if !authorized {
		return err
	}
	params, err := protocol.DecodePayload[protocol.WaitAgentParams](request.Payload)
	if err != nil || params.Validate() != nil {
		return s.writeError(
			ctx, request, protocol.ErrorInvalidParams, "invalid agent wait payload",
		)
	}
	mailboxKey := mailboxKey{
		controllerID: principal.ControllerID,
		treeID:       principal.TreeID,
		agentID:      principal.AgentID,
	}
	treeKey := treeKey{controllerID: principal.ControllerID, treeID: principal.TreeID}
	deadline := time.Now().Add(time.Duration(params.TimeoutMillis) * time.Millisecond)
	for {
		mailboxSubscription := s.server.mailboxNotifier.subscribe(mailboxKey)
		lifecycleSubscription := s.server.lifecycleNotifier.subscribe(treeKey)
		artifactSubscription := s.server.artifactNotifier.subscribe(treeKey)
		releaseSubscriptions := func() {
			mailboxSubscription.release()
			lifecycleSubscription.release()
			artifactSubscription.release()
		}
		result, err := s.readAgentWait(ctx, principal, params)
		if err != nil {
			releaseSubscriptions()
			return s.handleAgentWaitStoreError(ctx, request, err)
		}
		select {
		case <-canceled:
			releaseSubscriptions()
			return context.Canceled
		default:
		}
		if len(result.Messages) != 0 || len(result.Activities) != 0 ||
			len(result.Artifacts) != 0 || params.TimeoutMillis == 0 {
			releaseSubscriptions()
			return s.writeResult(ctx, request, result)
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			releaseSubscriptions()
			return s.writeResult(ctx, request, result)
		}
		timer := time.NewTimer(remaining)
		select {
		case <-ctx.Done():
			timer.Stop()
			releaseSubscriptions()
			return ctx.Err()
		case <-canceled:
			timer.Stop()
			releaseSubscriptions()
			return context.Canceled
		case <-mailboxSubscription.channel():
			timer.Stop()
			releaseSubscriptions()
		case <-lifecycleSubscription.channel():
			timer.Stop()
			releaseSubscriptions()
		case <-artifactSubscription.channel():
			timer.Stop()
			releaseSubscriptions()
		case <-timer.C:
			releaseSubscriptions()
			return s.writeResult(ctx, request, result)
		}
	}
}

func (s *session) readAgentWait(
	ctx context.Context,
	principal control.Principal,
	params protocol.WaitAgentParams,
) (protocol.WaitAgentResult, error) {
	mailbox, err := s.server.registry.ReadMailbox(
		ctx, principal, params.MailboxCursor, params.MessageLimit+1,
	)
	if err != nil {
		return protocol.WaitAgentResult{}, err
	}
	lifecycle, err := s.server.registry.ListAgentLifecycleActivity(
		ctx,
		principal.Identity(),
		store.AgentLifecyclePageRequest{
			AfterSequence: params.LifecycleCursor,
			Limit:         params.ActivityLimit + 1,
		},
	)
	if err != nil {
		return protocol.WaitAgentResult{}, err
	}
	artifacts, err := s.server.registry.ListChangesArtifacts(
		ctx,
		principal.Identity(),
		store.ChangesArtifactPageRequest{
			AfterSequence: params.ArtifactCursor,
			Limit:         params.ArtifactLimit + 1,
		},
	)
	if err != nil {
		return protocol.WaitAgentResult{}, err
	}
	result := protocol.WaitAgentResult{
		Messages:       mailbox.Messages,
		Activities:     make([]protocol.AgentLifecycleActivity, 0, len(lifecycle.Activities)),
		Artifacts:      artifacts.Artifacts,
		MoreMessages:   len(mailbox.Messages) > params.MessageLimit,
		MoreActivities: len(lifecycle.Activities) > params.ActivityLimit,
		MoreArtifacts:  len(artifacts.Artifacts) > params.ArtifactLimit,
	}
	if result.MoreMessages {
		result.Messages = result.Messages[:params.MessageLimit]
	}
	if result.MoreArtifacts {
		result.Artifacts = result.Artifacts[:params.ArtifactLimit]
	}
	for _, activity := range lifecycle.Activities[:min(len(lifecycle.Activities), params.ActivityLimit)] {
		result.Activities = append(result.Activities, protocol.AgentLifecycleActivity{
			AgentID: activity.AgentID, TargetDeviceID: activity.TargetDeviceID,
			TargetRevision: activity.TargetRevision, Phase: activity.Phase,
			FailureCode: activity.FailureCode, Sequence: activity.Sequence,
			ObservedAt: activity.ObservedAt,
		})
	}
	result.NextMailboxCursor = params.MailboxCursor
	if len(result.Messages) != 0 {
		result.NextMailboxCursor = result.Messages[len(result.Messages)-1].Sequence
	}
	result.NextLifecycleCursor = params.LifecycleCursor
	if len(result.Activities) != 0 {
		result.NextLifecycleCursor = result.Activities[len(result.Activities)-1].Sequence
	}
	result.NextArtifactCursor = params.ArtifactCursor
	if len(result.Artifacts) != 0 {
		result.NextArtifactCursor = result.Artifacts[len(result.Artifacts)-1].Sequence
	}
	return result, nil
}

func (s *session) authorizeAgentWait(
	ctx context.Context,
	request protocol.Envelope,
) (control.Principal, bool, error) {
	if request.TreeID == "" || request.Source == nil {
		return control.Principal{}, false, s.writeError(
			ctx, request, protocol.ErrorInvalidRequest, "agent wait requires a root principal",
		)
	}
	if request.Source.ParentAgentID != "" {
		return control.Principal{}, false, s.writeError(
			ctx, request, protocol.ErrorForbidden, "agent wait access denied",
		)
	}
	if request.Source.DeviceID != s.deviceID {
		return control.Principal{}, false, s.writeError(
			ctx, request, protocol.ErrorForbidden, "agent wait access denied",
		)
	}
	principal, err := s.server.registry.AuthorizePrincipal(
		ctx, *request.Source, control.CapabilityAgentManageDescendants,
	)
	if err == nil && principal.ParentAgentID == "" {
		return principal, true, nil
	}
	if isContextError(err) {
		return control.Principal{}, false, err
	}
	if err == nil || errors.Is(err, store.ErrAuthorizationDenied) {
		return control.Principal{}, false, s.writeError(
			ctx, request, protocol.ErrorForbidden, "agent wait access denied",
		)
	}
	_ = s.writeError(ctx, request, protocol.ErrorUnavailable, "broker unavailable")
	return control.Principal{}, false, &internalError{operation: "authorize agent wait", err: err}
}

func (s *session) handleAgentWaitStoreError(
	ctx context.Context,
	request protocol.Envelope,
	err error,
) error {
	if errors.Is(err, store.ErrMailboxCursorAhead) ||
		errors.Is(err, store.ErrAgentLifecycleCursorAhead) ||
		errors.Is(err, store.ErrChangesArtifactCursorAhead) {
		return s.writeError(
			ctx, request, protocol.ErrorConflict, "agent wait cursor is ahead of stored state",
		)
	}
	if errors.Is(err, store.ErrAuthorizationDenied) {
		return s.writeError(ctx, request, protocol.ErrorForbidden, "agent wait access denied")
	}
	if errors.Is(err, store.ErrNotFound) {
		return s.writeError(ctx, request, protocol.ErrorNotFound, "agent tree not found")
	}
	if isContextError(err) {
		return err
	}
	_ = s.writeError(ctx, request, protocol.ErrorUnavailable, "broker unavailable")
	return &internalError{operation: "read agent wait state", err: err}
}
