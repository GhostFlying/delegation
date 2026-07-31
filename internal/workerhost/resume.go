package workerhost

import (
	"context"
	"errors"
	"fmt"

	"github.com/GhostFlying/delegation/internal/hostkind"
	"github.com/GhostFlying/delegation/internal/rolloutcapture"
	"github.com/GhostFlying/delegation/internal/store"
)

func (h *Host) threadResumeParams(
	ctx context.Context,
	worker store.WorkerReservation,
	intent *store.WorkerTurnStartIntent,
) (threadResumeParams, error) {
	params := threadResumeParams{
		ThreadID: worker.CodexThreadID, CWD: h.workerCWD(worker),
		RuntimeWorkspaceRoots: []string{worker.WorkspacePath},
		ApprovalPolicy:        "never",
		Config:                h.managedConfig(worker), DeveloperMessage: workerInstructions,
		ExcludeTurns: true,
	}
	if h.hostKind != hostkind.TraeX {
		return params, nil
	}
	path, err := h.traeXResumePath(ctx, worker, intent)
	if err != nil {
		return threadResumeParams{}, err
	}
	params.Path = path
	return params, nil
}

func (h *Host) traeXResumePath(
	ctx context.Context,
	worker store.WorkerReservation,
	intent *store.WorkerTurnStartIntent,
) (string, error) {
	if intent != nil {
		if path, ok, err := h.validatedResumeLocator(worker, intent.Rollout); ok || err != nil {
			return path, err
		}
	}
	if worker.LastBoundTurnID != "" &&
		(intent == nil || intent.TurnID != worker.LastBoundTurnID) {
		bound, err := h.state.GetWorkerTurnStartIntentByTurn(
			ctx, worker.WorkerKey, worker.LastBoundTurnID,
		)
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			return "", fmt.Errorf("read managed TraeX rollout locator: %w", err)
		}
		if err == nil {
			if path, ok, locateErr := h.validatedResumeLocator(worker, bound.Rollout); ok || locateErr != nil {
				return path, locateErr
			}
		}
	}
	locator, err := rolloutcapture.Find(h.rolloutHome, worker.CodexThreadID)
	if err != nil {
		return "", fmt.Errorf("locate managed TraeX rollout for resume: %w", err)
	}
	return locator.Path, nil
}

func (h *Host) validatedResumeLocator(
	worker store.WorkerReservation,
	saved store.WorkerRolloutLocator,
) (string, bool, error) {
	if saved.Status != store.WorkerRolloutAvailable {
		return "", false, nil
	}
	if saved.CodexHome != h.rolloutHome {
		return "", true, errors.New("saved managed TraeX rollout belongs to another runtime home")
	}
	locator, err := rolloutcapture.Locate(h.rolloutHome, worker.CodexThreadID, saved.Path)
	if err != nil {
		return "", true, fmt.Errorf("validate managed TraeX rollout for resume: %w", err)
	}
	return locator.Path, true, nil
}
