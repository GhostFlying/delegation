package workerhost

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/GhostFlying/delegation/internal/gitworkspace"
	"github.com/GhostFlying/delegation/internal/identity"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/resultpackagefiles"
	"github.com/GhostFlying/delegation/internal/rolloutcapture"
	"github.com/GhostFlying/delegation/internal/store"
)

const (
	resultCapturePrefix         = "result-"
	resultChangesDirectoryName  = "changes"
	rolloutFlushAttempts        = 7
	rolloutFlushRetryMin        = 20 * time.Millisecond
	rolloutFlushRetryMax        = 250 * time.Millisecond
	rolloutCaptureFailureCode   = "rollout_capture_failed"
	rolloutTerminalMismatchCode = "rollout_terminal_mismatch"
	workspaceCaptureFailureCode = "workspace_capture_failed"
	workspaceResultTooLargeCode = "workspace_result_too_large"
)

type resultFinalizationIntegrityError struct {
	err error
}

func (e *resultFinalizationIntegrityError) Error() string { return e.err.Error() }
func (e *resultFinalizationIntegrityError) Unwrap() error { return e.err }

func (h *Host) processPendingResultFinalizations(ctx context.Context) error {
	outboxes, err := h.state.ListPendingResultCaptures(
		ctx, h.controllerID, h.deviceID, store.MaximumPeerResultPackages,
	)
	if err != nil {
		return classifyResultFinalizationError(err)
	}
	for _, outbox := range outboxes {
		worker, err := h.state.GetWorker(ctx, outbox.WorkerKey)
		if err != nil {
			return classifyResultFinalizationError(err)
		}
		if worker.Status != store.WorkerFinalizing {
			continue
		}
		if err := h.captureResultPackage(ctx, worker, outbox); err != nil {
			return classifyResultFinalizationError(err)
		}
	}
	return nil
}

func (h *Host) captureResultPackage(
	ctx context.Context,
	worker store.WorkerReservation,
	outbox store.ResultOutbox,
) error {
	intent, err := h.state.GetWorkerTurnStartIntentByTurn(
		ctx, worker.WorkerKey, worker.ActiveTurnID,
	)
	if err != nil {
		return err
	}
	if intent.State != store.WorkerTurnStartBound || intent.PackageID != outbox.PackageID ||
		intent.DeviceID != h.deviceID || intent.ManagedThreadID != worker.CodexThreadID ||
		outbox.ReservationLimitBytes != intent.ReservationLimitBytes {
		return &resultFinalizationIntegrityError{err: store.ErrResultPackageAuthority}
	}
	name := resultCapturePrefix + outbox.PackageID
	if err := h.removeResultCapture(name); err != nil {
		return err
	}
	if err := h.artifactRoot.Mkdir(name, 0o700); err != nil {
		return fmt.Errorf("create result capture staging: %w", err)
	}
	stagingPath := filepath.Join(h.artifactRoot.Name(), name)
	defer func() {
		if err := h.removeResultCapture(name); err != nil {
			h.reportError(err)
		}
	}()

	manifest := protocol.ResultManifest{
		Version: protocol.ResultManifestVersion, PackageID: outbox.PackageID,
		ControllerID: worker.ControllerID, TreeID: worker.TreeID,
		SourceAgentID: worker.AgentID, SourceDeviceID: worker.DeviceID,
		ManagedThreadID: worker.CodexThreadID, TurnID: worker.ActiveTurnID,
		LifecycleRevision: worker.Revision, CapturedAt: worker.UpdatedAt,
		Terminal: resultTerminal(worker), Parts: []protocol.ResultPackagePartDescriptor{},
	}
	parts := make([]resultpackagefiles.ResultPackagePartSource, 0, 3)
	workspace, workspaceDescriptors, workspaceParts, err := h.captureResultWorkspace(
		ctx, worker, stagingPath,
	)
	if err != nil {
		return err
	}
	manifest.Workspace = workspace
	manifest.Parts = append(manifest.Parts, workspaceDescriptors...)
	parts = append(parts, workspaceParts...)
	rollout, rolloutDescriptor, rolloutSource, err := h.captureResultRollout(
		ctx, intent, manifest.Terminal, stagingPath,
	)
	if err != nil {
		return err
	}
	manifest.Rollout = rollout
	if rolloutDescriptor != nil {
		manifest.Parts = append(manifest.Parts, *rolloutDescriptor)
		parts = append(parts, *rolloutSource)
	}
	slices.SortFunc(manifest.Parts, func(left, right protocol.ResultPackagePartDescriptor) int {
		if left.Kind < right.Kind {
			return -1
		}
		if left.Kind > right.Kind {
			return 1
		}
		return 0
	})
	slices.SortFunc(parts, func(left, right resultpackagefiles.ResultPackagePartSource) int {
		if left.Kind < right.Kind {
			return -1
		}
		if left.Kind > right.Kind {
			return 1
		}
		return 0
	})
	parts, workspaceDegraded, err := enforceResultPackageBudget(&manifest, parts)
	if err != nil {
		return &resultFinalizationIntegrityError{
			err: fmt.Errorf("fit captured result package budget: %w", err),
		}
	}
	if workspaceDegraded {
		h.reportError(fmt.Errorf(
			"managed workspace result for turn %s exceeds the aggregate package budget",
			worker.ActiveTurnID,
		))
	}
	metadataBytes, descriptor, err := protocol.EncodeResultManifest(manifest)
	if err != nil {
		return &resultFinalizationIntegrityError{
			err: fmt.Errorf("encode captured result manifest: %w", err),
		}
	}
	_, err = h.resultPackages.PublishResultPackage(
		ctx,
		resultpackagefiles.PublishResultPackageRequest{
			Key: outbox.ResultOutboxKey,
			Metadata: protocol.ResultPackageMetadata{
				Manifest: metadataBytes, ManifestDescriptor: descriptor,
			},
			Parts: parts,
		},
	)
	return err
}

func enforceResultPackageBudget(
	manifest *protocol.ResultManifest,
	parts []resultpackagefiles.ResultPackagePartSource,
) ([]resultpackagefiles.ResultPackagePartSource, bool, error) {
	maximumPayloadBytes := protocol.MaximumResultPackageBytes - protocol.MaximumResultManifestBytes
	var payloadBytes int64
	for _, descriptor := range manifest.Parts {
		if descriptor.Size < 1 {
			return nil, false, errors.New("captured result part size must be positive")
		}
		if descriptor.Size > maximumPayloadBytes-payloadBytes {
			if manifest.Workspace.Status != protocol.ResultWorkspaceChanged {
				return nil, false, errors.New("non-workspace result parts exceed the aggregate package budget")
			}
			manifest.Workspace = resultWorkspaceCaptureFailure(
				manifest.Workspace,
				workspaceResultTooLargeCode,
			)
			manifest.Parts = slices.DeleteFunc(
				manifest.Parts,
				func(part protocol.ResultPackagePartDescriptor) bool {
					return part.Kind == protocol.ResultPackagePartChangesBundle ||
						part.Kind == protocol.ResultPackagePartChangesOverlay
				},
			)
			parts = slices.DeleteFunc(parts, func(part resultpackagefiles.ResultPackagePartSource) bool {
				return part.Kind == protocol.ResultPackagePartChangesBundle ||
					part.Kind == protocol.ResultPackagePartChangesOverlay
			})
			return parts, true, nil
		}
		payloadBytes += descriptor.Size
	}
	return parts, false, nil
}

func resultTerminal(worker store.WorkerReservation) protocol.ResultTerminal {
	switch worker.FinalTarget {
	case store.WorkerIdle:
		return protocol.ResultTerminal{Outcome: protocol.ResultTerminalCompleted}
	case store.WorkerInterrupted:
		return protocol.ResultTerminal{
			Outcome: protocol.ResultTerminalInterrupted, FailureCode: worker.FinalFailureCode,
		}
	case store.WorkerFailed:
		return protocol.ResultTerminal{
			Outcome: protocol.ResultTerminalFailed, FailureCode: worker.FinalFailureCode,
		}
	default:
		return protocol.ResultTerminal{}
	}
}

type recoveredTurnTarget struct {
	status      store.WorkerStatus
	failureCode string
}

type rolloutTerminalInspection uint8

const (
	rolloutTerminalUnavailable rolloutTerminalInspection = iota
	rolloutTerminalIncomplete
	rolloutTerminalAvailable
)

func (h *Host) resolveResultTargetsAfterClientExit(
	ctx context.Context,
	intents []store.WorkerTurnStartIntent,
) ([]recoveredTurnTarget, error) {
	targets := make([]recoveredTurnTarget, len(intents))
	pending := make([]int, 0, len(intents))
	for index := range intents {
		targets[index] = recoveredTurnTarget{
			status: store.WorkerInterrupted, failureCode: "app_server_lost",
		}
		if intents[index].Rollout.Status == store.WorkerRolloutAvailable {
			pending = append(pending, index)
		}
	}
	retryDelay := rolloutFlushRetryMin
	for attempt := range rolloutFlushAttempts {
		incomplete := make([]int, 0, len(pending))
		for _, index := range pending {
			outcome, inspection, err := inspectBoundRolloutTerminal(ctx, intents[index])
			if err != nil {
				return nil, err
			}
			switch inspection {
			case rolloutTerminalUnavailable:
			case rolloutTerminalIncomplete:
				incomplete = append(incomplete, index)
			case rolloutTerminalAvailable:
				targets[index] = recoveredTargetForRolloutOutcome(outcome)
			}
		}
		pending = incomplete
		if len(pending) == 0 || attempt == rolloutFlushAttempts-1 {
			break
		}
		if err := h.waitForRolloutFlush(ctx, retryDelay); err != nil {
			return nil, err
		}
		retryDelay = min(retryDelay*2, rolloutFlushRetryMax)
	}
	return targets, nil
}

func recoveredTargetForRolloutOutcome(outcome rolloutcapture.Outcome) recoveredTurnTarget {
	switch outcome {
	case rolloutcapture.OutcomeCompleted:
		return recoveredTurnTarget{status: store.WorkerIdle}
	case rolloutcapture.OutcomeFailed:
		return recoveredTurnTarget{status: store.WorkerFailed, failureCode: "turn_failed"}
	case rolloutcapture.OutcomeAborted:
		return recoveredTurnTarget{
			status: store.WorkerInterrupted, failureCode: "app_server_lost",
		}
	default:
		return recoveredTurnTarget{
			status: store.WorkerInterrupted, failureCode: "app_server_lost",
		}
	}
}

func inspectBoundRolloutTerminal(
	ctx context.Context,
	intent store.WorkerTurnStartIntent,
) (rolloutcapture.Outcome, rolloutTerminalInspection, error) {
	if intent.Rollout.Status != store.WorkerRolloutAvailable {
		return "", rolloutTerminalUnavailable, nil
	}
	source, err := openBoundRollout(intent)
	if err != nil {
		return "", rolloutTerminalUnavailable, nil
	}
	segment, captureErr := rolloutcapture.CaptureSegment(
		ctx, source, intent.Rollout.Offset, intent.TurnID, io.Discard,
	)
	captureErr = errors.Join(captureErr, source.Close())
	if captureErr == nil {
		return segment.Outcome, rolloutTerminalAvailable, nil
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return "", rolloutTerminalUnavailable, contextErr
	}
	if errors.Is(captureErr, rolloutcapture.ErrIncomplete) {
		return "", rolloutTerminalIncomplete, nil
	}
	return "", rolloutTerminalUnavailable, nil
}

func (h *Host) captureResultRollout(
	ctx context.Context,
	intent store.WorkerTurnStartIntent,
	terminal protocol.ResultTerminal,
	stagingPath string,
) (
	protocol.ResultRolloutComponent,
	*protocol.ResultPackagePartDescriptor,
	*resultpackagefiles.ResultPackagePartSource,
	error,
) {
	failure := func(code string, cause error) (
		protocol.ResultRolloutComponent,
		*protocol.ResultPackagePartDescriptor,
		*resultpackagefiles.ResultPackagePartSource,
		error,
	) {
		if cause != nil {
			h.reportError(fmt.Errorf("capture managed rollout for turn %s: %w", intent.TurnID, cause))
		}
		return protocol.ResultRolloutComponent{
			Status: protocol.ResultRolloutCaptureFailed, FailureCode: code,
		}, nil, nil, nil
	}
	if intent.Rollout.Status != store.WorkerRolloutAvailable {
		return failure(intent.Rollout.FailureCode, nil)
	}
	destinationPath := filepath.Join(stagingPath, protocol.ResultRolloutFileName)
	retryDelay := rolloutFlushRetryMin
	var capture rolloutcapture.CompressedSegment
	var err error
	for attempt := range rolloutFlushAttempts {
		capture, err = captureResultRolloutAttempt(ctx, intent, destinationPath)
		if err == nil {
			break
		}
		if contextErr := ctx.Err(); contextErr != nil {
			return protocol.ResultRolloutComponent{}, nil, nil, contextErr
		}
		if !errors.Is(err, rolloutcapture.ErrIncomplete) || attempt == rolloutFlushAttempts-1 {
			return failure(rolloutCaptureFailureCode, err)
		}
		if err := h.waitForRolloutFlush(ctx, retryDelay); err != nil {
			return protocol.ResultRolloutComponent{}, nil, nil, err
		}
		retryDelay = min(retryDelay*2, rolloutFlushRetryMax)
	}
	wantOutcome := rolloutcapture.OutcomeCompleted
	switch terminal.Outcome {
	case protocol.ResultTerminalFailed:
		wantOutcome = rolloutcapture.OutcomeFailed
	case protocol.ResultTerminalInterrupted:
		wantOutcome = rolloutcapture.OutcomeAborted
	}
	if capture.Outcome != wantOutcome {
		_ = os.Remove(destinationPath)
		return failure(rolloutTerminalMismatchCode, errors.New("rollout terminal does not match worker terminal"))
	}
	descriptor := protocol.ResultPackagePartDescriptor{
		Kind: protocol.ResultPackagePartRollout, Size: capture.CompressedBytes,
		SHA256: capture.CompressedSHA256,
	}
	part := resultpackagefiles.ResultPackagePartSource{
		Kind: descriptor.Kind, Path: destinationPath,
	}
	return protocol.ResultRolloutComponent{
		Status: protocol.ResultRolloutAvailable, RawSize: capture.RawBytes,
		RawSHA256: capture.RawSHA256,
	}, &descriptor, &part, nil
}

func captureResultRolloutAttempt(
	ctx context.Context,
	intent store.WorkerTurnStartIntent,
	destinationPath string,
) (rolloutcapture.CompressedSegment, error) {
	source, err := openBoundRollout(intent)
	if err != nil {
		return rolloutcapture.CompressedSegment{}, err
	}
	destination, err := os.OpenFile(destinationPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		_ = source.Close()
		return rolloutcapture.CompressedSegment{}, err
	}
	capture, captureErr := rolloutcapture.CaptureCompressedSegment(
		ctx, source, intent.Rollout.Offset, intent.TurnID, destination,
	)
	captureErr = errors.Join(captureErr, destination.Sync(), destination.Close(), source.Close())
	if captureErr != nil {
		_ = os.Remove(destinationPath)
	}
	return capture, captureErr
}

func openBoundRollout(intent store.WorkerTurnStartIntent) (*os.File, error) {
	source, located, err := rolloutcapture.OpenValidated(
		intent.Rollout.CodexHome, intent.ManagedThreadID, intent.Rollout.Path,
	)
	if err != nil {
		return nil, err
	}
	if located.Path != intent.Rollout.Path || located.Offset < intent.Rollout.Offset {
		_ = source.Close()
		return nil, errors.New("managed rollout no longer contains its bound offset")
	}
	return source, nil
}

func waitForRolloutFlush(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (h *Host) captureResultWorkspace(
	ctx context.Context,
	worker store.WorkerReservation,
	stagingPath string,
) (
	protocol.ResultWorkspaceComponent,
	[]protocol.ResultPackagePartDescriptor,
	[]resultpackagefiles.ResultPackagePartSource,
	error,
) {
	if worker.WorkspaceID == "" {
		return protocol.ResultWorkspaceComponent{
			Status:       protocol.ResultWorkspaceNotManaged,
			BaseWarnings: []string{}, ResultWarnings: []string{},
		}, nil, nil, nil
	}
	workspace, err := h.state.GetPreparedWorkspace(ctx, store.PreparedWorkspaceKey{
		ControllerID: worker.ControllerID, TreeID: worker.TreeID, WorkspaceID: worker.WorkspaceID,
	})
	if err != nil {
		return protocol.ResultWorkspaceComponent{}, nil, nil, err
	}
	if workspace.Status != store.PreparedWorkspaceClaimed ||
		workspace.ClaimedAgentID != worker.AgentID || workspace.TargetDeviceID != h.deviceID ||
		workspace.WorkspacePath != worker.WorkspacePath {
		return protocol.ResultWorkspaceComponent{}, nil, nil,
			&resultFinalizationIntegrityError{err: store.ErrResultPackageAuthority}
	}
	if err := h.verifyPreparedWorkspaceAuthority(workspace); err != nil {
		return h.failedResultWorkspace(workspace, err), nil, nil, nil
	}
	base := protocol.WorkspaceManifest{
		GitURL: workspace.GitURL, HeadOID: workspace.HeadOID,
		ObjectFormat: workspace.ObjectFormat, WorkingDirectory: workspace.WorkingDirectory,
		Clean: workspace.Clean, SourceSnapshotHash: workspace.SourceSnapshotHash,
		Warnings: slices.Clone(workspace.SourceWarnings),
	}
	changesPath := filepath.Join(stagingPath, resultChangesDirectoryName)
	capture, err := h.git.CaptureResult(ctx, workspace.WorkspacePath, changesPath, base)
	if err != nil {
		h.reportError(fmt.Errorf("capture managed workspace result for turn %s: %w", worker.ActiveTurnID, err))
		return h.failedResultWorkspace(workspace, err), nil, nil, nil
	}
	if capture.ArtifactDirectory != changesPath {
		return h.failedResultWorkspace(
			workspace, errors.New("Git capture returned an unexpected result directory"),
		), nil, nil, nil
	}
	component := resultWorkspaceBase(workspace)
	component.Status = protocol.ResultWorkspaceChanged
	if capture.Unchanged {
		component.Status = protocol.ResultWorkspaceUnchanged
	}
	component.ResultHeadOID = capture.ResultHeadOID
	component.ResultSnapshotHash = capture.ResultSnapshotHash
	component.ResultClean = capture.ResultClean
	component.ResultWarnings = slices.Clone(capture.ResultWarnings)
	parts := make([]resultpackagefiles.ResultPackagePartSource, 0, 2)
	descriptors := make([]protocol.ResultPackagePartDescriptor, 0, 2)
	for _, artifact := range []*gitworkspace.ResultArtifact{capture.Bundle, capture.Overlay} {
		if artifact == nil {
			continue
		}
		kind := protocol.ResultPackagePartChangesBundle
		expectedName := gitworkspace.ChangesBundleName
		if artifact.Kind == protocol.WorkspaceArtifactOverlay {
			kind = protocol.ResultPackagePartChangesOverlay
			expectedName = gitworkspace.ChangesOverlayName
		} else if artifact.Kind != protocol.WorkspaceArtifactBundle {
			h.reportError(fmt.Errorf("capture managed workspace returned unsupported part %q", artifact.Kind))
			return h.failedResultWorkspace(workspace, errors.New("unsupported workspace result part")), nil, nil, nil
		}
		if artifact.Name != expectedName || artifact.Path != filepath.Join(changesPath, expectedName) {
			return h.failedResultWorkspace(
				workspace, errors.New("Git capture returned an unexpected result part path"),
			), nil, nil, nil
		}
		descriptors = append(descriptors, protocol.ResultPackagePartDescriptor{
			Kind: kind, Size: artifact.Size, SHA256: artifact.SHA256,
		})
		parts = append(parts, resultpackagefiles.ResultPackagePartSource{Kind: kind, Path: artifact.Path})
	}
	return component, descriptors, parts, nil
}

func resultWorkspaceBase(workspace store.PreparedWorkspace) protocol.ResultWorkspaceComponent {
	return protocol.ResultWorkspaceComponent{
		WorkspaceID: workspace.WorkspaceID, SourceDeviceID: workspace.SourceDeviceID,
		TargetDeviceID: workspace.TargetDeviceID, ObjectFormat: workspace.ObjectFormat,
		BaseHeadOID: workspace.HeadOID, BaseManifestHash: workspace.ManifestHash,
		BaseSnapshotHash: workspace.SourceSnapshotHash, BaseClean: workspace.Clean,
		BaseWarnings: slices.Clone(workspace.Warnings), ResultWarnings: []string{},
	}
}

func (h *Host) failedResultWorkspace(
	workspace store.PreparedWorkspace,
	cause error,
) protocol.ResultWorkspaceComponent {
	if cause != nil {
		h.reportError(fmt.Errorf("load managed workspace for result capture: %w", cause))
	}
	return resultWorkspaceCaptureFailure(resultWorkspaceBase(workspace), workspaceCaptureFailureCode)
}

func resultWorkspaceCaptureFailure(
	component protocol.ResultWorkspaceComponent,
	failureCode string,
) protocol.ResultWorkspaceComponent {
	component.Status = protocol.ResultWorkspaceCaptureFailed
	component.ResultHeadOID = ""
	component.ResultSnapshotHash = ""
	component.ResultClean = false
	component.ResultWarnings = []string{}
	component.FailureCode = failureCode
	return component
}

func (h *Host) removeResultCapture(name string) error {
	if len(name) <= len(resultCapturePrefix) || name[:len(resultCapturePrefix)] != resultCapturePrefix ||
		identity.ValidateID(name[len(resultCapturePrefix):]) != nil {
		return fmt.Errorf("refuse to remove unexpected result capture path %q", name)
	}
	if err := h.artifactRoot.RemoveAll(name); err != nil {
		return err
	}
	return syncDirectory(h.artifactRoot)
}

func cleanupResultCaptureStaging(root *os.Root) error {
	entries, err := fs.ReadDir(root.FS(), ".")
	if err != nil {
		return err
	}
	changed := false
	for _, entry := range entries {
		name := entry.Name()
		if len(name) <= len(resultCapturePrefix) || name[:len(resultCapturePrefix)] != resultCapturePrefix {
			continue
		}
		if err := identity.ValidateID(name[len(resultCapturePrefix):]); err != nil {
			return fmt.Errorf("invalid result capture staging path %q", name)
		}
		if err := root.RemoveAll(name); err != nil {
			return err
		}
		changed = true
	}
	if changed {
		return syncDirectory(root)
	}
	return nil
}

func classifyResultFinalizationError(err error) error {
	if err == nil {
		return nil
	}
	var integrityError *resultFinalizationIntegrityError
	if errors.As(err, &integrityError) ||
		errors.Is(err, resultpackagefiles.ErrPublicationIntegrity) ||
		errors.Is(err, store.ErrNotFound) ||
		errors.Is(err, store.ErrResultPackageAuthority) ||
		errors.Is(err, store.ErrResultPackageConflict) ||
		errors.Is(err, store.ErrResultPackageQuota) ||
		errors.Is(err, store.ErrResultPackageTransition) {
		return err
	}
	return &artifactRetentionError{
		err: fmt.Errorf("retry result package finalization: %w", err),
	}
}

// RecordResultPackageAcknowledgement publishes the worker revision that was
// committed atomically with the broker's result metadata acknowledgement.
func (h *Host) RecordResultPackageAcknowledgement(
	finalization store.WorkerResultFinalization,
) error {
	if finalization.Worker.DeviceID != h.deviceID {
		return store.ErrResultPackageAuthority
	}
	_, err := h.recordWorkerChange(finalization.Worker, nil)
	return err
}
