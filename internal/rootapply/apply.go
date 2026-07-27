package rootapply

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"

	"github.com/GhostFlying/delegation/internal/gitworkspace"
	"github.com/GhostFlying/delegation/internal/localbridge"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/resultpackagefiles"
)

var errSimulatedCrash = errors.New("simulated root apply crash")
var errWorkspaceChangedBeforeMutation = errors.New("root workspace changed before mutation")

func hashPath(path string) string {
	digest := sha256.Sum256([]byte(path))
	return hex.EncodeToString(digest[:])
}

func (m *Manager) PrepareResultApply(
	ctx context.Context,
	request localbridge.ResultApplyRequest,
) (localbridge.ResultApplyPreparation, error) {
	if err := request.Root.Validate(); err != nil || request.Root.ParentAgentID != "" {
		return localbridge.ResultApplyPreparation{}, localbridge.ErrApplyRequestConflict
	}
	if err := request.Params.Validate(); err != nil {
		return localbridge.ResultApplyPreparation{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	exists, err := journalExists(m.journal, request.Params.ApplyID)
	if err != nil {
		return localbridge.ResultApplyPreparation{}, err
	}
	if exists {
		lease, err := m.openJournal(request.Params.ApplyID)
		if err != nil {
			return localbridge.ResultApplyPreparation{}, err
		}
		defer lease.close()
		record, err := lease.read()
		if err != nil {
			return localbridge.ResultApplyPreparation{}, err
		}
		if !record.Request.matches(request) {
			return localbridge.ResultApplyPreparation{}, localbridge.ErrApplyRequestConflict
		}
		if record.State == journalCompleted {
			if err := lease.compactArtifacts(); err != nil {
				return localbridge.ResultApplyPreparation{}, err
			}
			result := *record.Result
			return localbridge.ResultApplyPreparation{Completed: &result}, nil
		}
		if record.AuthorizationParams == nil {
			return localbridge.ResultApplyPreparation{}, localbridge.ErrApplyRecoveryRequired
		}
		params := *record.AuthorizationParams
		return localbridge.ResultApplyPreparation{Authorization: &params}, nil
	}
	usage, err := m.maintainJournals(true)
	if err != nil {
		return localbridge.ResultApplyPreparation{}, err
	}
	if !usage.canAdmit(m.retention) {
		return localbridge.ResultApplyPreparation{}, localbridge.ErrApplyBacklog
	}

	manifest, err := m.packages.LookupApplyManifest(ctx, resultpackagefiles.ApplyPackageRequest{
		Root: request.Root, PackageID: request.Params.PackageID,
	})
	if err != nil {
		return localbridge.ResultApplyPreparation{}, m.mapPackageError(err)
	}
	switch manifest.Workspace.Status {
	case protocol.ResultWorkspaceNotManaged, protocol.ResultWorkspaceUnchanged:
		result := localbridge.ApplyAgentChangesResult{
			ApplyID: request.Params.ApplyID, PackageID: request.Params.PackageID,
			Outcome: localbridge.ApplyAgentChangesUnchanged,
		}
		if err := m.writeTerminalWithoutAuthorization(request, result); err != nil {
			return localbridge.ResultApplyPreparation{}, err
		}
		return localbridge.ResultApplyPreparation{Completed: &result}, nil
	case protocol.ResultWorkspaceCaptureFailed:
		return localbridge.ResultApplyPreparation{}, localbridge.ErrApplyPackageUnavailable
	case protocol.ResultWorkspaceChanged:
	default:
		return localbridge.ResultApplyPreparation{}, localbridge.ErrApplyPackageUnavailable
	}
	repository, err := m.runner.InspectApplySource(ctx, request.Params.SourcePath)
	if err != nil {
		if isContextError(err) {
			return localbridge.ResultApplyPreparation{}, err
		}
		result := needsResolution(request, "root_workspace_conflict")
		if writeErr := m.writeTerminalWithoutAuthorization(request, result); writeErr != nil {
			return localbridge.ResultApplyPreparation{}, writeErr
		}
		return localbridge.ResultApplyPreparation{Completed: &result}, nil
	}
	params := protocol.AuthorizeResultApplyParams{
		ApplyID: request.Params.ApplyID, PackageID: request.Params.PackageID,
		SourcePathSHA256: hashPath(request.Params.SourcePath), GitURL: repository.Manifest.GitURL,
	}
	if err := params.Validate(); err != nil {
		return localbridge.ResultApplyPreparation{}, err
	}
	lease, err := m.createJournal(request.Params.ApplyID)
	if err != nil {
		return localbridge.ResultApplyPreparation{}, err
	}
	defer lease.close()
	record := journalRecord{
		Version: journalVersion, Request: bindingFor(request, repository.Manifest.GitURL),
		AuthorizationParams: &params, Manifest: &manifest, State: journalAuthorizing,
	}
	if err := lease.write(record); err != nil {
		_ = lease.close()
		_ = m.journal.RemoveAll(request.Params.ApplyID)
		_ = syncDirectory(m.journal)
		return localbridge.ResultApplyPreparation{}, err
	}
	return localbridge.ResultApplyPreparation{Authorization: &params}, nil
}

func (m *Manager) writeTerminalWithoutAuthorization(
	request localbridge.ResultApplyRequest,
	result localbridge.ApplyAgentChangesResult,
) error {
	lease, err := m.createJournal(request.Params.ApplyID)
	if err != nil {
		return err
	}
	defer lease.close()
	record := journalRecord{
		Version: journalVersion, Request: bindingFor(request, ""), State: journalCompleted,
		Result: &result,
	}
	if err := lease.write(record); err != nil {
		_ = lease.close()
		_ = m.journal.RemoveAll(request.Params.ApplyID)
		_ = syncDirectory(m.journal)
		return err
	}
	return nil
}

func (m *Manager) ApplyAuthorizedResult(
	ctx context.Context,
	request localbridge.ResultApplyRequest,
	authorization protocol.AuthorizeResultApplyResult,
) (localbridge.ApplyAgentChangesResult, error) {
	if err := authorization.Validate(); err != nil || authorization.ApplyID != request.Params.ApplyID ||
		authorization.PackageID != request.Params.PackageID {
		return localbridge.ApplyAgentChangesResult{}, localbridge.ErrApplyRequestConflict
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	lease, err := m.openJournal(request.Params.ApplyID)
	if err != nil {
		return localbridge.ApplyAgentChangesResult{}, err
	}
	defer lease.close()
	record, err := lease.read()
	if err != nil {
		return localbridge.ApplyAgentChangesResult{}, err
	}
	if !record.Request.matches(request) {
		return localbridge.ApplyAgentChangesResult{}, localbridge.ErrApplyRequestConflict
	}
	if record.State == journalCompleted {
		if err := lease.compactArtifacts(); err != nil {
			return localbridge.ApplyAgentChangesResult{}, err
		}
		return *record.Result, nil
	}
	if record.AuthorizationParams == nil ||
		record.AuthorizationParams.ApplyID != authorization.ApplyID ||
		record.AuthorizationParams.PackageID != authorization.PackageID {
		return localbridge.ApplyAgentChangesResult{}, localbridge.ErrApplyRequestConflict
	}
	if record.Authorization != nil && *record.Authorization != authorization {
		return localbridge.ApplyAgentChangesResult{}, localbridge.ErrApplyRecoveryRequired
	}
	if record.Authorization == nil {
		record.Authorization = &authorization
		record.State = journalBuilding
		if err := lease.write(record); err != nil {
			return localbridge.ApplyAgentChangesResult{}, err
		}
	}
	return m.continueApply(ctx, lease, record, request)
}

func (m *Manager) continueApply(
	ctx context.Context,
	lease *journalLease,
	record journalRecord,
	request localbridge.ResultApplyRequest,
) (localbridge.ApplyAgentChangesResult, error) {
	switch record.State {
	case journalBuilding:
		return m.buildAndApply(ctx, lease, record, request)
	case journalPrepared:
		return m.mutate(ctx, lease, record, request)
	case journalMutating, journalVerifying:
		matchesDesired, err := m.matchesSnapshot(ctx, request.Params.SourcePath, *record.Desired)
		if err != nil {
			return localbridge.ApplyAgentChangesResult{}, err
		}
		if matchesDesired {
			return m.complete(lease, record, localbridge.ApplyAgentChangesApplied)
		}
		matchesBase, err := m.matchesSnapshot(ctx, request.Params.SourcePath, *record.Base)
		if err != nil {
			return localbridge.ApplyAgentChangesResult{}, err
		}
		if matchesBase {
			return m.mutate(ctx, lease, record, request)
		}
		return m.requireRecovery(lease, record)
	case journalRecoveryRequired:
		matchesDesired, err := m.matchesSnapshot(ctx, request.Params.SourcePath, *record.Desired)
		if err != nil {
			return localbridge.ApplyAgentChangesResult{}, err
		}
		if matchesDesired {
			return m.complete(lease, record, localbridge.ApplyAgentChangesApplied)
		}
		matchesBase, err := m.matchesSnapshot(ctx, request.Params.SourcePath, *record.Base)
		if err != nil {
			return localbridge.ApplyAgentChangesResult{}, err
		}
		if matchesBase {
			return m.mutate(ctx, lease, record, request)
		}
		return needsResolution(request, "root_workspace_recovery_required"), nil
	case journalCompleted:
		return *record.Result, nil
	case journalAuthorizing:
		return localbridge.ApplyAgentChangesResult{}, localbridge.ErrApplyRecoveryRequired
	default:
		return localbridge.ApplyAgentChangesResult{}, localbridge.ErrApplyRecoveryRequired
	}
}

func (m *Manager) buildAndApply(
	ctx context.Context,
	lease *journalLease,
	record journalRecord,
	request localbridge.ResultApplyRequest,
) (localbridge.ApplyAgentChangesResult, error) {
	repository, matches, err := m.inspectBase(ctx, request.Params.SourcePath, record)
	if err != nil {
		return localbridge.ApplyAgentChangesResult{}, err
	}
	if !matches {
		return m.completeNeedsResolution(lease, record, "root_workspace_conflict")
	}
	if err := clearBuildArtifacts(lease.root); err != nil {
		return localbridge.ApplyAgentChangesResult{}, err
	}
	packageRoot, err := createPrivateSubdirectory(lease.root, packageDirectoryName)
	if err != nil {
		return localbridge.ApplyAgentChangesResult{}, err
	}
	materialized, materializeErr := m.packages.MaterializeApplyWorkspace(
		ctx,
		resultpackagefiles.MaterializeApplyPackageRequest{
			ApplyPackageRequest: resultpackagefiles.ApplyPackageRequest{
				Root: request.Root, PackageID: request.Params.PackageID,
			},
			Authorization: *record.Authorization,
		},
		packageRoot,
	)
	closePackageErr := packageRoot.Close()
	if materializeErr != nil || closePackageErr != nil {
		return localbridge.ApplyAgentChangesResult{}, m.mapPackageError(errors.Join(materializeErr, closePackageErr))
	}
	if !sameResultManifest(materialized, *record.Manifest) {
		return m.requireRecovery(lease, record)
	}
	base := repository.Manifest
	baseBundle := filepath.Join(lease.path, baseBundleFileName)
	if _, err := m.runner.CreateApplyBundle(ctx, repository.Root, baseBundle, base); err != nil {
		return localbridge.ApplyAgentChangesResult{}, err
	}
	staging := filepath.Join(lease.path, stagingDirectoryName)
	if err := m.runner.ApplyRootReconstructionBundle(ctx, staging, baseBundle, base); err != nil {
		return localbridge.ApplyAgentChangesResult{}, fmt.Errorf("reconstruct root apply base: %w", err)
	}
	resultManifest := manifestForResult(base, record.Manifest.Workspace)
	packagePath := filepath.Join(lease.path, packageDirectoryName)
	if resultManifest.HeadOID != base.HeadOID {
		if err := m.runner.ApplyRootReconstructionBundle(
			ctx, staging, filepath.Join(packagePath, protocol.ResultChangesBundleFileName), resultManifest,
		); err != nil {
			return localbridge.ApplyAgentChangesResult{}, fmt.Errorf("reconstruct worker result bundle: %w", err)
		}
	}
	if !resultManifest.Clean {
		if err := m.runner.ApplyOverlay(
			ctx, staging, filepath.Join(packagePath, protocol.ResultChangesOverlayFileName), resultManifest,
		); err != nil {
			return localbridge.ApplyAgentChangesResult{}, fmt.Errorf("reconstruct worker result overlay: %w", err)
		}
	}
	stagingSourcePath := filepath.Join(staging, filepath.FromSlash(base.WorkingDirectory))
	reconstructed, err := m.runner.InspectApplyStaging(ctx, stagingSourcePath, base.GitURL)
	if err != nil || !sameWorkspaceManifest(reconstructed.Manifest, resultManifest) {
		return localbridge.ApplyAgentChangesResult{}, errors.New("reconstructed worker result differs from its manifest")
	}
	flattened, err := m.runner.FlattenStagingResult(ctx, staging, base)
	if err != nil {
		return localbridge.ApplyAgentChangesResult{}, err
	}
	var desired *artifactDescriptor
	if !flattened.Manifest.Clean {
		path := filepath.Join(lease.path, desiredOverlayName)
		if err := m.runner.CreateApplyOverlay(ctx, staging, path, flattened.Manifest); err != nil {
			return localbridge.ApplyAgentChangesResult{}, fmt.Errorf("capture flattened root apply snapshot: %w", err)
		}
		descriptor, err := describeArtifact(path)
		if err != nil {
			return localbridge.ApplyAgentChangesResult{}, err
		}
		desired = &descriptor
	}
	if err := clearReconstructionArtifacts(lease.root); err != nil {
		return localbridge.ApplyAgentChangesResult{}, err
	}
	if err := syncDirectory(lease.root); err != nil {
		return localbridge.ApplyAgentChangesResult{}, err
	}
	record.Base = &base
	record.Desired = &flattened.Manifest
	record.DesiredData = desired
	record.State = journalPrepared
	if err := lease.write(record); err != nil {
		return localbridge.ApplyAgentChangesResult{}, err
	}
	return m.mutate(ctx, lease, record, request)
}

func (m *Manager) mutate(
	ctx context.Context,
	lease *journalLease,
	record journalRecord,
	request localbridge.ResultApplyRequest,
) (localbridge.ApplyAgentChangesResult, error) {
	matchesBase, err := m.matchesSnapshot(ctx, request.Params.SourcePath, *record.Base)
	if err != nil {
		return localbridge.ApplyAgentChangesResult{}, err
	}
	if !matchesBase {
		return m.completeNeedsResolution(lease, record, "root_workspace_conflict")
	}
	if err := verifySnapshotArtifact(lease.path, desiredOverlayName, record.DesiredData); err != nil {
		return m.requireRecovery(lease, record)
	}
	record.State = journalMutating
	if err := lease.write(record); err != nil {
		return localbridge.ApplyAgentChangesResult{}, err
	}
	if err := m.triggerFault(faultBeforeMutation); err != nil {
		return localbridge.ApplyAgentChangesResult{}, err
	}
	matchesBase, err = m.matchesSnapshot(ctx, request.Params.SourcePath, *record.Base)
	if err != nil {
		return localbridge.ApplyAgentChangesResult{}, err
	}
	if !matchesBase {
		return m.completeNeedsResolution(lease, record, "root_workspace_conflict")
	}
	if err := m.applySnapshot(
		ctx, request.Params.SourcePath, lease.path, *record.Base, *record.Desired,
		desiredOverlayName,
	); err != nil {
		if errors.Is(err, errSimulatedCrash) {
			return localbridge.ApplyAgentChangesResult{}, err
		}
		if errors.Is(err, errWorkspaceChangedBeforeMutation) ||
			errors.Is(err, gitworkspace.ErrRootApplyConflict) {
			return m.completeNeedsResolution(lease, record, "root_workspace_conflict")
		}
		return m.requireRecovery(lease, record)
	}
	if err := m.triggerFault(faultAfterMutation); err != nil {
		return localbridge.ApplyAgentChangesResult{}, err
	}
	record.State = journalVerifying
	if err := lease.write(record); err != nil {
		return localbridge.ApplyAgentChangesResult{}, err
	}
	matchesDesired, err := m.matchesSnapshot(ctx, request.Params.SourcePath, *record.Desired)
	if err != nil {
		return localbridge.ApplyAgentChangesResult{}, err
	}
	if !matchesDesired {
		return m.requireRecovery(lease, record)
	}
	return m.complete(lease, record, localbridge.ApplyAgentChangesApplied)
}

func (m *Manager) inspectBase(
	ctx context.Context,
	sourcePath string,
	record journalRecord,
) (gitworkspace.Repository, bool, error) {
	repository, err := m.runner.InspectApplySource(ctx, sourcePath)
	if err != nil {
		if isContextError(err) {
			return gitworkspace.Repository{}, false, err
		}
		return gitworkspace.Repository{}, false, nil
	}
	workspace := record.Manifest.Workspace
	manifestHash, err := gitworkspace.ManifestHash(repository.Manifest)
	if err != nil {
		return gitworkspace.Repository{}, false, err
	}
	matches := record.Authorization != nil && record.AuthorizationParams != nil &&
		record.AuthorizationParams.GitURL == repository.Manifest.GitURL &&
		record.AuthorizationParams.SourcePathSHA256 == hashPath(sourcePath) &&
		record.Authorization.BaseManifestHash == manifestHash &&
		workspace.BaseManifestHash == manifestHash && workspace.WorkspaceID == record.Authorization.WorkspaceID &&
		repository.Manifest.HeadOID == workspace.BaseHeadOID &&
		repository.Manifest.ObjectFormat == workspace.ObjectFormat &&
		repository.Manifest.SourceSnapshotHash == workspace.BaseSnapshotHash &&
		repository.Manifest.Clean == workspace.BaseClean &&
		slices.Equal(repository.Manifest.Warnings, workspace.BaseWarnings)
	return repository, matches, nil
}

func (m *Manager) matchesSnapshot(
	ctx context.Context,
	sourcePath string,
	want protocol.WorkspaceManifest,
) (bool, error) {
	repository, err := m.runner.InspectApplySource(ctx, sourcePath)
	if err != nil {
		return false, err
	}
	return sameWorkspaceManifest(repository.Manifest, want), nil
}

func (m *Manager) applySnapshot(
	ctx context.Context,
	sourcePath string,
	journalPath string,
	base protocol.WorkspaceManifest,
	desired protocol.WorkspaceManifest,
	overlayName string,
) error {
	repository, err := m.runner.InspectApplySource(ctx, sourcePath)
	if err != nil {
		return err
	}
	if !sameWorkspaceManifest(repository.Manifest, base) {
		return errWorkspaceChangedBeforeMutation
	}
	beforeMutation := func() error {
		if err := m.triggerFault(faultBeforeDestructiveWrite); err != nil {
			return err
		}
		current, err := m.runner.InspectApplySource(ctx, sourcePath)
		if err != nil {
			if isContextError(err) {
				return err
			}
			return errWorkspaceChangedBeforeMutation
		}
		if !sameWorkspaceManifest(current.Manifest, base) {
			return errWorkspaceChangedBeforeMutation
		}
		return nil
	}
	if desired.Clean {
		return m.runner.ApplyCleanSnapshotPreservingConfig(
			ctx, repository.Root, base, desired, beforeMutation,
		)
	}
	return m.runner.ApplyOverlayPreservingConfig(
		ctx, repository.Root, filepath.Join(journalPath, overlayName), base, desired,
		beforeMutation,
	)
}

func (m *Manager) complete(
	lease *journalLease,
	record journalRecord,
	outcome localbridge.ApplyAgentChangesOutcome,
) (localbridge.ApplyAgentChangesResult, error) {
	result := localbridge.ApplyAgentChangesResult{
		ApplyID: record.Request.ApplyID, PackageID: record.Request.PackageID, Outcome: outcome,
	}
	record.State = journalCompleted
	record.Result = &result
	if err := lease.compactTerminal(record); err != nil {
		return localbridge.ApplyAgentChangesResult{}, err
	}
	return result, nil
}

func (m *Manager) completeNeedsResolution(
	lease *journalLease,
	record journalRecord,
	failureCode string,
) (localbridge.ApplyAgentChangesResult, error) {
	result := localbridge.ApplyAgentChangesResult{
		ApplyID: record.Request.ApplyID, PackageID: record.Request.PackageID,
		Outcome: localbridge.ApplyAgentChangesNeedsResolution, FailureCode: failureCode,
	}
	record.State = journalCompleted
	record.Result = &result
	if err := lease.compactTerminal(record); err != nil {
		return localbridge.ApplyAgentChangesResult{}, err
	}
	return result, nil
}

func (m *Manager) requireRecovery(
	lease *journalLease,
	record journalRecord,
) (localbridge.ApplyAgentChangesResult, error) {
	if record.Manifest == nil || record.AuthorizationParams == nil || record.Authorization == nil ||
		record.Base == nil || record.Desired == nil {
		return m.completeNeedsResolution(lease, record, "root_workspace_recovery_required")
	}
	record.State = journalRecoveryRequired
	record.Result = nil
	if err := lease.write(record); err != nil {
		return localbridge.ApplyAgentChangesResult{}, localbridge.ErrApplyRecoveryRequired
	}
	return localbridge.ApplyAgentChangesResult{
		ApplyID: record.Request.ApplyID, PackageID: record.Request.PackageID,
		Outcome:     localbridge.ApplyAgentChangesNeedsResolution,
		FailureCode: "root_workspace_recovery_required",
	}, nil
}

func needsResolution(
	request localbridge.ResultApplyRequest,
	failureCode string,
) localbridge.ApplyAgentChangesResult {
	return localbridge.ApplyAgentChangesResult{
		ApplyID: request.Params.ApplyID, PackageID: request.Params.PackageID,
		Outcome: localbridge.ApplyAgentChangesNeedsResolution, FailureCode: failureCode,
	}
}

func manifestForResult(
	base protocol.WorkspaceManifest,
	workspace protocol.ResultWorkspaceComponent,
) protocol.WorkspaceManifest {
	return protocol.WorkspaceManifest{
		GitURL: base.GitURL, HeadOID: workspace.ResultHeadOID,
		ObjectFormat: base.ObjectFormat, WorkingDirectory: base.WorkingDirectory,
		Clean: workspace.ResultClean, SourceSnapshotHash: workspace.ResultSnapshotHash,
		Warnings: append([]string(nil), workspace.ResultWarnings...),
	}
}

func sameWorkspaceManifest(left, right protocol.WorkspaceManifest) bool {
	return left.GitURL == right.GitURL && left.HeadOID == right.HeadOID &&
		left.ObjectFormat == right.ObjectFormat && left.WorkingDirectory == right.WorkingDirectory &&
		left.Clean == right.Clean && left.SourceSnapshotHash == right.SourceSnapshotHash &&
		slices.Equal(left.Warnings, right.Warnings)
}

func sameResultManifest(left, right protocol.ResultManifest) bool {
	leftBytes, _, leftErr := protocol.EncodeResultManifest(left)
	rightBytes, _, rightErr := protocol.EncodeResultManifest(right)
	return leftErr == nil && rightErr == nil && slices.Equal(leftBytes, rightBytes)
}

func createPrivateSubdirectory(parent *os.Root, name string) (*os.Root, error) {
	if err := parent.Mkdir(name, 0o700); err != nil {
		return nil, err
	}
	info, err := parent.Lstat(name)
	if err != nil || !privateEntry(info, true) {
		return nil, errors.New("root apply subdirectory is not private")
	}
	return parent.OpenRoot(name)
}

func clearBuildArtifacts(root *os.Root) error {
	for _, name := range []string{
		packageDirectoryName, stagingDirectoryName, baseBundleFileName,
		desiredOverlayName,
	} {
		if err := root.RemoveAll(name); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}
	return syncDirectory(root)
}

func clearReconstructionArtifacts(root *os.Root) error {
	for _, name := range []string{packageDirectoryName, stagingDirectoryName, baseBundleFileName} {
		if err := root.RemoveAll(name); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}
	return syncDirectory(root)
}

func describeArtifact(path string) (artifactDescriptor, error) {
	expected, err := os.Lstat(path)
	if err != nil || expected.Mode()&os.ModeSymlink != 0 || !expected.Mode().IsRegular() ||
		expected.Size() < 1 || expected.Size() > protocol.MaximumWorkspaceArtifactBytes {
		return artifactDescriptor{}, errors.New("root apply artifact has invalid size or type")
	}
	file, err := os.Open(path)
	if err != nil {
		return artifactDescriptor{}, err
	}
	info, err := file.Stat()
	if err != nil || !os.SameFile(expected, info) || !info.Mode().IsRegular() || info.Size() < 1 ||
		info.Size() > protocol.MaximumWorkspaceArtifactBytes {
		_ = file.Close()
		return artifactDescriptor{}, errors.New("root apply artifact has invalid size or type")
	}
	digest := sha256.New()
	written, readErr := io.Copy(digest, io.LimitReader(file, protocol.MaximumWorkspaceArtifactBytes+1))
	closeErr := file.Close()
	if readErr != nil || closeErr != nil {
		return artifactDescriptor{}, errors.Join(readErr, closeErr)
	}
	if written != info.Size() {
		return artifactDescriptor{}, errors.New("root apply artifact changed while it was hashed")
	}
	return artifactDescriptor{Size: written, SHA256: hex.EncodeToString(digest.Sum(nil))}, nil
}

func verifySnapshotArtifact(directory, name string, descriptor *artifactDescriptor) error {
	if descriptor == nil {
		return nil
	}
	actual, err := describeArtifact(filepath.Join(directory, name))
	if err != nil || actual != *descriptor {
		return localbridge.ErrApplyRecoveryRequired
	}
	return nil
}

func isContextError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
