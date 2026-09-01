package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"github.com/GhostFlying/delegation/internal/buildinfo"
	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/localbridge"
	"github.com/GhostFlying/delegation/internal/localupgrade"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/userservice"
	"github.com/GhostFlying/delegation/internal/workerreadiness"
)

type serviceUpgradeManager struct {
	mu              sync.Mutex
	store           *localupgrade.Store
	manager         *localupgrade.Manager
	home            string
	config          delegationconfig.Config
	configPath      string
	environmentFile string
	sourceBinary    string
	currentVersion  string
}

func newServiceUpgradeManager(
	config delegationconfig.Config, configPath, environmentFile, sourceBinary, currentVersion string,
) (*serviceUpgradeManager, error) {
	home, err := delegationconfig.DefaultHome()
	if err != nil {
		return nil, err
	}
	home, err = filepath.Abs(home)
	if err != nil {
		return nil, err
	}
	root, err := localupgrade.RootForConfig(home, config)
	if err != nil {
		return nil, err
	}
	transactionStore, err := localupgrade.OpenStore(root)
	if err != nil {
		return nil, err
	}
	manager, err := localupgrade.NewManager(transactionStore, localupgrade.DefaultOneShotOperations())
	if err != nil {
		return nil, err
	}
	return &serviceUpgradeManager{
		store: transactionStore, manager: manager, home: home, config: config,
		configPath: configPath, environmentFile: environmentFile, sourceBinary: sourceBinary,
		currentVersion: currentVersion,
	}, nil
}

func (m *serviceUpgradeManager) PrepareLocalUpgrade(
	ctx context.Context, targetVersion, configPath, environmentFile string,
) (localbridge.UpgradeSnapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.prepareLocalUpgrade(ctx, targetVersion, configPath, environmentFile, "", "")
}

func (m *serviceUpgradeManager) prepareLocalUpgrade(
	ctx context.Context, targetVersion, configPath, environmentFile, transactionID, controllerTransactionID string,
) (localbridge.UpgradeSnapshot, error) {
	if filepath.Clean(configPath) != m.configPath ||
		(environmentFile != "" && filepath.Clean(environmentFile) != m.environmentFile) ||
		(environmentFile == "" && m.environmentFile != "") {
		return localbridge.UpgradeSnapshot{}, errors.New("upgrade request does not match the running service invocation")
	}
	result, err := localupgrade.Prepare(ctx, localupgrade.PrepareOptions{
		Store: m.store, Home: m.home, Config: m.config, ConfigPath: m.configPath,
		EnvironmentFile: m.environmentFile, SourceBinary: m.sourceBinary,
		CurrentVersion: m.currentVersion, TargetVersion: targetVersion,
		TransactionID: transactionID, ControllerTransactionID: controllerTransactionID,
	})
	if err != nil {
		return localbridge.UpgradeSnapshot{}, err
	}
	return bridgeUpgradeSnapshot(result.Journal), nil
}

func (m *serviceUpgradeManager) PrepareCoordinatedUpgrade(
	ctx context.Context, params protocol.PrepareUpgradeParams,
) (protocol.UpgradeSnapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := params.Validate(); err != nil {
		return protocol.UpgradeSnapshot{}, err
	}
	_, err := m.prepareLocalUpgrade(
		ctx, params.TargetVersion, m.configPath, m.environmentFile,
		params.TransactionID, params.ControllerTransactionID,
	)
	if err != nil {
		return protocol.UpgradeSnapshot{}, err
	}
	journal, err := m.store.Load()
	if err != nil {
		return protocol.UpgradeSnapshot{}, err
	}
	return coordinatedUpgradeSnapshot(journal), nil
}

func (m *serviceUpgradeManager) ArmCoordinatedUpgrade(
	ctx context.Context, params protocol.UpgradeTransactionParams,
) (protocol.UpgradeSnapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.runCoordinatedUpgrade(ctx, params, m.manager.Arm)
}

func (m *serviceUpgradeManager) ActivateCoordinatedUpgrade(
	ctx context.Context, params protocol.UpgradeTransactionParams,
) (protocol.UpgradeSnapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.runCoordinatedUpgrade(ctx, params, m.manager.AuthorizeAndLaunch)
}

func (m *serviceUpgradeManager) CancelCoordinatedUpgrade(
	ctx context.Context, params protocol.UpgradeTransactionParams,
) (protocol.UpgradeSnapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := params.Validate(); err != nil {
		return protocol.UpgradeSnapshot{}, err
	}
	journal, err := m.store.Load()
	if err != nil {
		return protocol.UpgradeSnapshot{}, err
	}
	if journal.TransactionID != params.TransactionID {
		return protocol.UpgradeSnapshot{}, errors.New("coordinated upgrade transaction identity does not match")
	}
	if journal.ControllerTransactionID == "" {
		journal, err = m.store.BindControllerTransaction(params.TransactionID, params.ControllerTransactionID)
		if err != nil {
			return protocol.UpgradeSnapshot{}, err
		}
	}
	return m.runCoordinatedUpgrade(ctx, params, m.manager.Cancel)
}

func (m *serviceUpgradeManager) CoordinatedUpgradeStatus(
	_ context.Context, params protocol.UpgradeTransactionParams,
) (protocol.UpgradeSnapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := params.Validate(); err != nil {
		return protocol.UpgradeSnapshot{}, err
	}
	journal, err := m.loadCoordinatedUpgrade(params)
	if err != nil {
		return protocol.UpgradeSnapshot{}, err
	}
	return coordinatedUpgradeSnapshot(journal), nil
}

func (m *serviceUpgradeManager) runCoordinatedUpgrade(
	ctx context.Context, params protocol.UpgradeTransactionParams,
	operation func(context.Context, string) (localupgrade.Journal, error),
) (protocol.UpgradeSnapshot, error) {
	if err := params.Validate(); err != nil {
		return protocol.UpgradeSnapshot{}, err
	}
	if _, err := m.loadCoordinatedUpgrade(params); err != nil {
		return protocol.UpgradeSnapshot{}, err
	}
	journal, err := operation(ctx, params.TransactionID)
	return coordinatedUpgradeSnapshot(journal), err
}

func (m *serviceUpgradeManager) loadCoordinatedUpgrade(
	params protocol.UpgradeTransactionParams,
) (localupgrade.Journal, error) {
	journal, err := m.store.Load()
	if err != nil {
		return localupgrade.Journal{}, err
	}
	if journal.TransactionID != params.TransactionID ||
		journal.ControllerTransactionID != params.ControllerTransactionID {
		return localupgrade.Journal{}, errors.New("coordinated upgrade transaction identity does not match")
	}
	return journal, nil
}

func coordinatedUpgradeSnapshot(journal localupgrade.Journal) protocol.UpgradeSnapshot {
	return protocol.UpgradeSnapshot{
		ControllerTransactionID: journal.ControllerTransactionID,
		TransactionID:           journal.TransactionID,
		State:                   string(journal.State),
		SourceVersion:           journal.SourceVersion,
		TargetVersion:           journal.TargetVersion,
		TargetRuntimeDigest:     journal.TargetRuntimeDigest,
		ConfigDigest:            journal.ConfigDigest,
		SourceReadinessEpoch:    journal.SourceReadinessEpoch,
		CommitAuthorized:        journal.CommitAuthorized,
		FailureCode:             journal.FailureCode,
		UpdatedAt:               journal.UpdatedAt,
	}
}

type legacyBootstrapDependencies struct {
	discoverSource func(context.Context, userservice.ServiceRole, userservice.Invocation) (userservice.Invocation, error)
	probeVersion   func(context.Context, string) (string, error)
	newManager     func(delegationconfig.Config, string, string, string, string) (localUpgradeControl, error)
}

type localUpgradeControl interface {
	PrepareLocalUpgrade(context.Context, string, string, string) (localbridge.UpgradeSnapshot, error)
	ArmLocalUpgrade(context.Context, string) (localbridge.UpgradeSnapshot, error)
	ActivateLocalUpgrade(context.Context, string) (localbridge.UpgradeSnapshot, error)
	CancelLocalUpgrade(context.Context, string) (localbridge.UpgradeSnapshot, error)
	LocalUpgrade(context.Context) (*localbridge.UpgradeSnapshot, error)
}

func bootstrapServiceUpgrade(
	ctx context.Context, cfg delegationconfig.Config, configPath, environmentFile, targetVersion string,
) (localbridge.UpgradeSnapshot, error) {
	endpoint, identity, err := localUpgradeEndpoint(cfg)
	if err != nil {
		return localbridge.UpgradeSnapshot{}, err
	}
	probeContext, cancelProbe := context.WithTimeout(ctx, 2*time.Second)
	probeErr := localbridge.Probe(probeContext, endpoint, identity)
	cancelProbe()
	if probeErr == nil {
		result, prepareErr := localbridge.PrepareUpgrade(
			ctx, endpoint, targetVersion, configPath, environmentFile,
		)
		if prepareErr != nil {
			return result, prepareErr
		}
		result, err = localbridge.ArmUpgrade(ctx, endpoint, result.TransactionID)
		if err != nil {
			return result, err
		}
		return activateAndWaitForLocalUpgrade(ctx, endpoint, result)
	}
	if err := ctx.Err(); err != nil {
		return localbridge.UpgradeSnapshot{}, err
	}
	return bootstrapLegacyServiceUpgrade(ctx, cfg, configPath, environmentFile, targetVersion)
}

func cancelServiceUpgrade(
	ctx context.Context, cfg delegationconfig.Config, configPath, transactionID string,
) (localbridge.UpgradeSnapshot, error) {
	endpoint, identity, err := localUpgradeEndpoint(cfg)
	if err != nil {
		return localbridge.UpgradeSnapshot{}, err
	}
	probeContext, cancelProbe := context.WithTimeout(ctx, 2*time.Second)
	probeErr := localbridge.Probe(probeContext, endpoint, identity)
	cancelProbe()
	if probeErr == nil {
		return localbridge.CancelUpgrade(ctx, endpoint, transactionID)
	}
	if err := ctx.Err(); err != nil {
		return localbridge.UpgradeSnapshot{}, err
	}
	return cancelLocalUpgrade(ctx, cfg, configPath, transactionID)
}

func bootstrapLegacyServiceUpgrade(
	ctx context.Context, cfg delegationconfig.Config, configPath, environmentFile, targetVersion string,
) (localbridge.UpgradeSnapshot, error) {
	return bootstrapLegacyServiceUpgradeWithDependencies(
		ctx, cfg, configPath, environmentFile, targetVersion, legacyBootstrapDependencies{},
	)
}

func bootstrapLegacyServiceUpgradeWithDependencies(
	ctx context.Context, cfg delegationconfig.Config, configPath, environmentFile, targetVersion string,
	dependencies legacyBootstrapDependencies,
) (localbridge.UpgradeSnapshot, error) {
	if dependencies.discoverSource == nil {
		dependencies.discoverSource = userservice.DiscoverUpgradeSource
	}
	if dependencies.probeVersion == nil {
		dependencies.probeVersion = probeLegacyRuntimeVersion
	}
	if dependencies.newManager == nil {
		dependencies.newManager = func(
			config delegationconfig.Config, configPath, environmentFile, sourceBinary, currentVersion string,
		) (localUpgradeControl, error) {
			return newServiceUpgradeManager(config, configPath, environmentFile, sourceBinary, currentVersion)
		}
	}
	role, err := configuredServiceRole(cfg.Role)
	if err != nil {
		return localbridge.UpgradeSnapshot{}, err
	}
	expected := userservice.Invocation{
		ConfigPath: configPath, EnvironmentFile: environmentFile,
		InstanceID: cfg.EffectiveInstanceID(),
	}
	resumeManager, err := dependencies.newManager(cfg, configPath, environmentFile, "", "")
	if err != nil {
		return localbridge.UpgradeSnapshot{}, err
	}
	existing, err := resumeManager.LocalUpgrade(ctx)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return localbridge.UpgradeSnapshot{}, err
	}
	if existing != nil {
		if existing.TargetVersion == targetVersion || !upgradeSnapshotTerminal(*existing) {
			result, prepareErr := resumeManager.PrepareLocalUpgrade(
				ctx, targetVersion, configPath, environmentFile,
			)
			if prepareErr != nil {
				return result, prepareErr
			}
			return armAndActivateLocalUpgrade(ctx, resumeManager, result)
		}
	}
	source, err := dependencies.discoverSource(ctx, role, expected)
	if err != nil {
		return localbridge.UpgradeSnapshot{}, fmt.Errorf("discover legacy managed service: %w", err)
	}
	if !filepath.IsAbs(source.BinaryPath) || filepath.Clean(source.BinaryPath) != source.BinaryPath {
		return localbridge.UpgradeSnapshot{}, errors.New("legacy service executable path is not absolute and clean")
	}
	currentVersion, err := dependencies.probeVersion(ctx, source.BinaryPath)
	if err != nil {
		return localbridge.UpgradeSnapshot{}, fmt.Errorf("verify legacy service runtime version: %w", err)
	}
	manager, err := dependencies.newManager(
		cfg, configPath, environmentFile, source.BinaryPath, currentVersion,
	)
	if err != nil {
		return localbridge.UpgradeSnapshot{}, err
	}
	result, err := manager.PrepareLocalUpgrade(ctx, targetVersion, configPath, environmentFile)
	if err != nil {
		return result, err
	}
	return armAndActivateLocalUpgrade(ctx, manager, result)
}

func upgradeSnapshotTerminal(snapshot localbridge.UpgradeSnapshot) bool {
	switch localupgrade.State(snapshot.State) {
	case localupgrade.StateCommitted, localupgrade.StateRolledBack, localupgrade.StateRollbackFailed:
		return true
	default:
		return false
	}
}

func armAndActivateLocalUpgrade(
	ctx context.Context, manager localUpgradeControl, result localbridge.UpgradeSnapshot,
) (localbridge.UpgradeSnapshot, error) {
	armed, err := manager.ArmLocalUpgrade(ctx, result.TransactionID)
	if err != nil {
		return armed, err
	}
	return activateAndWaitForUpgrade(
		ctx, armed, manager.ActivateLocalUpgrade, manager.LocalUpgrade,
	)
}

func cancelLocalUpgrade(
	ctx context.Context, cfg delegationconfig.Config, configPath, transactionID string,
) (localbridge.UpgradeSnapshot, error) {
	manager, err := newServiceUpgradeManager(cfg, configPath, "", "", "")
	if err != nil {
		return localbridge.UpgradeSnapshot{}, err
	}
	journal, err := manager.store.Load()
	if err != nil {
		return localbridge.UpgradeSnapshot{}, err
	}
	if journal.TransactionID != transactionID || journal.Role != cfg.Role ||
		journal.InstanceID != cfg.EffectiveInstanceID() || journal.ControllerID != cfg.ControllerID ||
		journal.DeviceID != cfg.DeviceID || journal.Invocation.ConfigPath != configPath ||
		(cfg.Role == delegationconfig.RoleBroker && journal.Invocation.EnvironmentFile != "") {
		return localbridge.UpgradeSnapshot{}, errors.New("upgrade journal does not match the requested managed service")
	}
	return manager.CancelLocalUpgrade(ctx, transactionID)
}

func probeLegacyRuntimeVersion(ctx context.Context, binaryPath string) (string, error) {
	command := exec.CommandContext(ctx, binaryPath, "version", "--json")
	var stdout boundedUpgradeOutput
	stdout.maximum = 256
	command.Stdout = &stdout
	command.Stderr = io.Discard
	if err := command.Run(); err != nil {
		return "", err
	}
	var payload struct {
		Version string `json:"version"`
	}
	decoder := json.NewDecoder(bytes.NewReader(stdout.Bytes()))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil {
		return "", err
	}
	if payload.Version == "" || decoder.Decode(&struct{}{}) != io.EOF {
		return "", errors.New("legacy runtime returned an invalid version document")
	}
	return payload.Version, nil
}

type boundedUpgradeOutput struct {
	bytes.Buffer
	maximum int
}

func (b *boundedUpgradeOutput) Write(data []byte) (int, error) {
	if b.Len()+len(data) > b.maximum {
		return 0, errors.New("legacy runtime version output exceeds the size limit")
	}
	return b.Buffer.Write(data)
}

func (m *serviceUpgradeManager) ArmLocalUpgrade(
	ctx context.Context, transactionID string,
) (localbridge.UpgradeSnapshot, error) {
	if err := m.rejectControllerBoundLocalOperation(transactionID); err != nil {
		return localbridge.UpgradeSnapshot{}, err
	}
	j, err := m.manager.Arm(ctx, transactionID)
	return bridgeUpgradeSnapshot(j), err
}

func (m *serviceUpgradeManager) ActivateLocalUpgrade(
	ctx context.Context, transactionID string,
) (localbridge.UpgradeSnapshot, error) {
	if err := m.rejectControllerBoundLocalOperation(transactionID); err != nil {
		return localbridge.UpgradeSnapshot{}, err
	}
	j, err := m.manager.AuthorizeAndLaunch(ctx, transactionID)
	return bridgeUpgradeSnapshot(j), err
}

func (m *serviceUpgradeManager) CancelLocalUpgrade(
	ctx context.Context, transactionID string,
) (localbridge.UpgradeSnapshot, error) {
	if err := m.rejectControllerBoundLocalOperation(transactionID); err != nil {
		return localbridge.UpgradeSnapshot{}, err
	}
	j, err := m.manager.Cancel(ctx, transactionID)
	return bridgeUpgradeSnapshot(j), err
}

func (m *serviceUpgradeManager) rejectControllerBoundLocalOperation(transactionID string) error {
	journal, err := m.store.Load()
	if err != nil {
		return err
	}
	if journal.TransactionID != transactionID {
		return errors.New("upgrade transaction ID does not match the active journal")
	}
	if journal.ControllerTransactionID != "" {
		return errors.New("controller-coordinated upgrade must use the controller transaction")
	}
	return nil
}

func (m *serviceUpgradeManager) LocalUpgrade(context.Context) (*localbridge.UpgradeSnapshot, error) {
	j, err := m.store.Load()
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	snapshot := bridgeUpgradeSnapshot(j)
	return &snapshot, nil
}

func (m *serviceUpgradeManager) LocalUpgradeRuntime(context.Context) (localbridge.UpgradeRuntimeIdentity, error) {
	digest, err := workerreadiness.RuntimeDigest(m.sourceBinary)
	if err != nil {
		return localbridge.UpgradeRuntimeIdentity{}, err
	}
	return localbridge.UpgradeRuntimeIdentity{Version: buildinfo.Version, Digest: digest}, nil
}

func bridgeUpgradeSnapshot(journal localupgrade.Journal) localbridge.UpgradeSnapshot {
	snapshot := journal.Snapshot()
	return localbridge.UpgradeSnapshot{
		TransactionID: snapshot.TransactionID, State: string(snapshot.State),
		SourceVersion: snapshot.SourceVersion, TargetVersion: snapshot.TargetVersion,
		CommitAuthorized: snapshot.CommitAuthorized, FailureCode: snapshot.FailureCode,
		UpdatedAt: snapshot.UpdatedAt,
	}
}

func localUpgradeEndpoint(config delegationconfig.Config) (string, localbridge.ServiceIdentity, error) {
	identity := localbridge.ServiceIdentity{
		Role: config.Role, ControllerID: config.ControllerID, InstanceID: config.EffectiveInstanceID(),
	}
	var (
		endpoint string
		err      error
	)
	if config.Role == delegationconfig.RoleBroker {
		endpoint, err = localbridge.BrokerEndpointForInstance(
			config.EffectiveInstanceID(), config.ControllerID,
		)
	} else {
		identity.DeviceID = config.DeviceID
		endpoint, err = localbridge.EndpointForInstance(
			config.EffectiveInstanceID(), config.ControllerID, config.DeviceID,
		)
	}
	return endpoint, identity, err
}

func runCommittedUpgradeCleanup(ctx context.Context, manager *serviceUpgradeManager) {
	if manager == nil {
		return
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		j, err := manager.store.Load()
		if err == nil && j.State == localupgrade.StateCommitted {
			if _, cleanupErr := manager.manager.Cleanup(ctx, j.TransactionID); cleanupErr == nil {
				return
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func qualifyLocalUpgrade(ctx context.Context, journal localupgrade.Journal) error {
	if err := validateUpgradeActivatorRuntime(journal); err != nil {
		return err
	}
	config := delegationconfig.Config{
		Role: journal.Role, InstanceID: journal.InstanceID, ControllerID: journal.ControllerID,
		DeviceID: journal.DeviceID,
	}
	endpoint, expected, err := localUpgradeEndpoint(config)
	if err != nil {
		return err
	}
	runtimeIdentity, err := localbridge.ReadUpgradeRuntime(ctx, endpoint)
	if err != nil {
		return fmt.Errorf("qualify target runtime identity: %w", err)
	}
	if runtimeIdentity.Version != journal.TargetVersion ||
		runtimeIdentity.Digest != journal.TargetRuntimeDigest {
		return errors.New("running service does not match the target runtime identity")
	}
	configDigest, err := workerreadiness.ConfigDigest(
		journal.Invocation.ConfigPath, journal.Invocation.EnvironmentFile,
	)
	if err != nil || configDigest != journal.ConfigDigest {
		return errors.Join(err, errors.New("running service configuration does not match the upgrade target"))
	}
	if journal.Role == delegationconfig.RoleBroker {
		if err := localbridge.Probe(ctx, endpoint, expected); err != nil {
			return fmt.Errorf("qualify target service identity: %w", err)
		}
		return nil
	}
	status, err := localbridge.ReadStatusForIdentity(ctx, endpoint, expected)
	if err != nil {
		return fmt.Errorf("qualify target peer status: %w", err)
	}
	readiness := status.WorkerReadiness
	if status.Version != journal.TargetVersion || !status.ServiceRunning ||
		status.ConnectionState != localbridge.ConnectionReady || !status.Connected ||
		!status.WorkerSyncReady {
		return errors.New("target peer has not completed connection and lifecycle synchronization")
	}
	if readiness.Epoch <= journal.SourceReadinessEpoch ||
		readiness.State != protocol.WorkerReadinessReady ||
		readiness.RuntimeDigest != journal.TargetRuntimeDigest ||
		readiness.ConfigDigest != journal.ConfigDigest ||
		!status.WorkerReady || !status.Dispatchable {
		return errors.New("target peer has not completed the required new execution-readiness epoch")
	}
	return nil
}

func validateUpgradeActivatorRuntime(journal localupgrade.Journal) error {
	if buildinfo.Version != journal.TargetVersion {
		return errors.New("activator runtime version does not match the upgrade target")
	}
	runtimePath, err := os.Executable()
	if err != nil {
		return err
	}
	runtimePath, err = filepath.EvalSymlinks(runtimePath)
	if err != nil {
		return err
	}
	digest, err := workerreadiness.RuntimeDigest(runtimePath)
	if err != nil {
		return err
	}
	if filepath.Clean(runtimePath) != filepath.Clean(journal.Invocation.TargetBinaryPath) ||
		digest != journal.TargetRuntimeDigest {
		return errors.New("activator runtime identity does not match protected target material")
	}
	return nil
}
