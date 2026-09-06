package localupgrade

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
	"reflect"
	"runtime"
	"time"

	"github.com/GhostFlying/delegation/internal/codexconfig"
	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/hostkind"
	"github.com/GhostFlying/delegation/internal/identity"
	"github.com/GhostFlying/delegation/internal/releaseverify"
	"github.com/GhostFlying/delegation/internal/securefs"
	"github.com/GhostFlying/delegation/internal/store"
	"github.com/GhostFlying/delegation/internal/traexauth"
	"github.com/GhostFlying/delegation/internal/userservice"
	"github.com/GhostFlying/delegation/internal/workerprofile"
	"github.com/GhostFlying/delegation/internal/workerreadiness"
)

const (
	CompatibilitySchemaVersion = 2
	TailscaleGeneration        = 1
	maximumCompatibilityOutput = 16 << 10
	maximumConfigDigestFile    = 1 << 20
)

type Compatibility struct {
	SchemaVersion        int                    `json:"schemaVersion"`
	RuntimeVersion       string                 `json:"runtimeVersion"`
	Platform             string                 `json:"platform"`
	Architecture         string                 `json:"architecture"`
	ConfigSchemaVersion  int                    `json:"configSchemaVersion"`
	DatabaseKind         store.DatabaseKind     `json:"databaseKind"`
	DatabaseIdentity     store.DatabaseIdentity `json:"databaseIdentity"`
	WorkerProfileVersion int                    `json:"workerProfileVersion"`
	TailscaleGeneration  int                    `json:"tailscaleGeneration"`
}

func (c Compatibility) Validate() error {
	if c.SchemaVersion != CompatibilitySchemaVersion || !validVersion(c.RuntimeVersion) {
		return errors.New("target runtime compatibility identity is invalid")
	}
	if c.Platform != runtime.GOOS || c.Architecture != runtime.GOARCH {
		return errors.New("target runtime compatibility platform does not match this host")
	}
	if c.ConfigSchemaVersion != delegationconfig.CurrentSchemaVersion {
		return errors.New("target runtime does not support the current config schema")
	}
	current, err := store.CurrentDatabaseIdentity(c.DatabaseKind)
	if err != nil {
		return err
	}
	if c.DatabaseIdentity.ApplicationID != current.ApplicationID || c.DatabaseIdentity.SchemaVersion < 1 {
		return errors.New("target runtime database identity is invalid")
	}
	if c.DatabaseKind == store.DatabasePeer {
		if c.WorkerProfileVersion < workerprofile.CurrentVersion {
			return errors.New("target runtime worker profile version is invalid")
		}
	} else if c.WorkerProfileVersion != 0 {
		return errors.New("broker compatibility contains a worker profile version")
	}
	if c.TailscaleGeneration != TailscaleGeneration {
		return errors.New("target runtime Tailscale compatibility generation differs")
	}
	return nil
}

func CurrentCompatibility(role delegationconfig.Role, runtimeVersion string) (Compatibility, error) {
	kind, err := databaseKind(role)
	if err != nil {
		return Compatibility{}, err
	}
	databaseIdentity, err := store.CurrentDatabaseIdentity(kind)
	if err != nil {
		return Compatibility{}, err
	}
	result := Compatibility{
		SchemaVersion: CompatibilitySchemaVersion, RuntimeVersion: runtimeVersion,
		Platform: runtime.GOOS, Architecture: runtime.GOARCH,
		ConfigSchemaVersion: delegationconfig.CurrentSchemaVersion,
		DatabaseKind:        kind, DatabaseIdentity: databaseIdentity,
		TailscaleGeneration: TailscaleGeneration,
	}
	if role == delegationconfig.RolePeer {
		result.WorkerProfileVersion = workerprofile.CurrentVersion
	}
	return result, result.Validate()
}

type TargetProbe func(context.Context, string, string, string) (Compatibility, error)

func ProbeTarget(ctx context.Context, binary, configPath, environmentFile string) (Compatibility, error) {
	arguments := []string{"service", "upgrade-compatibility", "--config", configPath, "--json"}
	if environmentFile != "" {
		arguments = append(arguments, "--environment-file", environmentFile)
	}
	command := exec.CommandContext(ctx, binary, arguments...)
	var output boundedOutput
	output.maximum = maximumCompatibilityOutput
	command.Stdout = &output
	command.Stderr = io.Discard
	if err := command.Run(); err != nil {
		return Compatibility{}, fmt.Errorf("run target compatibility probe: %w", err)
	}
	var result Compatibility
	decoder := json.NewDecoder(bytes.NewReader(output.Bytes()))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return Compatibility{}, fmt.Errorf("decode target compatibility: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return Compatibility{}, errors.New("target compatibility must contain one JSON value")
	}
	if err := result.Validate(); err != nil {
		return Compatibility{}, err
	}
	return result, nil
}

type PrepareDependencies struct {
	AcquireRelease     func(context.Context, string, string) (releaseverify.Result, error)
	InstallRuntime     func(context.Context, string, releaseverify.Result) (releaseverify.RuntimeMaterial, error)
	ProbeTarget        TargetProbe
	PrepareService     func(context.Context, userservice.ServiceRole, userservice.Invocation, userservice.Invocation) (userservice.UpgradePlan, error)
	ReadBlockers       func(context.Context) (store.UpgradeBlockers, error)
	ReadReadinessEpoch func(context.Context) (uint64, error)
	NewID              func() (string, error)
	Now                func() time.Time
}

type PrepareOptions struct {
	Store                   *Store
	Home                    string
	Config                  delegationconfig.Config
	ConfigPath              string
	EnvironmentFile         string
	SourceBinary            string
	CurrentVersion          string
	TargetVersion           string
	TransactionID           string
	ControllerTransactionID string
	Dependencies            PrepareDependencies
}

type PrepareResult struct {
	Journal Journal
	Resumed bool
}

// Prepare verifies all local identities and writes protected transaction
// material without mutating the live service definition or database.
func Prepare(ctx context.Context, options PrepareOptions) (PrepareResult, error) {
	if options.Store == nil {
		return PrepareResult{}, errors.New("upgrade transaction store is required")
	}
	if options.TransactionID != "" {
		if err := identity.ValidateID(options.TransactionID); err != nil {
			return PrepareResult{}, fmt.Errorf("upgrade transaction ID: %w", err)
		}
	}
	if options.ControllerTransactionID != "" {
		if options.TransactionID == "" {
			return PrepareResult{}, errors.New("controller-bound upgrade requires a reserved transaction ID")
		}
		if err := identity.ValidateID(options.ControllerTransactionID); err != nil {
			return PrepareResult{}, fmt.Errorf("controller transaction ID: %w", err)
		}
	}
	lock, err := acquireJournalLock(filepath.Join(options.Store.path, "prepare.lock"))
	if err != nil {
		return PrepareResult{}, fmt.Errorf("acquire upgrade preparation lock: %w", err)
	}
	defer lock.Close()
	if existing, loadErr := options.Store.Load(); loadErr == nil {
		if existing.TargetVersion == options.TargetVersion {
			if err := validateResumeRequest(existing, options); err != nil {
				return PrepareResult{}, err
			}
			return PrepareResult{Journal: existing, Resumed: true}, nil
		}
		if !existing.Terminal() {
			return PrepareResult{}, ErrTargetConflict
		}
	} else if !errors.Is(loadErr, os.ErrNotExist) {
		return PrepareResult{}, loadErr
	}
	if err := validatePrepareOptions(options); err != nil {
		return PrepareResult{}, err
	}
	dependencies := options.Dependencies
	if dependencies.AcquireRelease == nil {
		dependencies.AcquireRelease = func(ctx context.Context, current, target string) (releaseverify.Result, error) {
			return releaseverify.AcquireCanonicalRelease(ctx, current, target, releaseverify.AcquisitionDependencies{})
		}
	}
	if dependencies.InstallRuntime == nil {
		dependencies.InstallRuntime = func(ctx context.Context, home string, result releaseverify.Result) (releaseverify.RuntimeMaterial, error) {
			return releaseverify.InstallRuntime(ctx, home, result, nil)
		}
	}
	if dependencies.ProbeTarget == nil {
		dependencies.ProbeTarget = ProbeTarget
	}
	if dependencies.PrepareService == nil {
		dependencies.PrepareService = userservice.PrepareUpgrade
	}
	if dependencies.ReadBlockers == nil {
		dependencies.ReadBlockers = func(ctx context.Context) (store.UpgradeBlockers, error) {
			return defaultBlockers(ctx, options.Config)
		}
	}
	if dependencies.ReadReadinessEpoch == nil && options.Config.Role == delegationconfig.RolePeer {
		dependencies.ReadReadinessEpoch = func(ctx context.Context) (uint64, error) {
			identity, err := store.InspectUpgradeDatabase(ctx, options.Config.Peer.StateFile, store.DatabasePeer)
			if err != nil {
				return 0, err
			}
			current, err := store.CurrentDatabaseIdentity(store.DatabasePeer)
			if err != nil {
				return 0, err
			}
			if identity != current {
				return 0, nil
			}
			state, err := store.OpenPeer(ctx, options.Config.Peer.StateFile)
			if err != nil {
				return 0, err
			}
			defer state.Close()
			readiness, err := state.WorkerReadiness(ctx)
			if errors.Is(err, store.ErrNotFound) {
				return 0, nil
			}
			return readiness.Epoch, err
		}
	}
	if dependencies.NewID == nil {
		dependencies.NewID = identity.NewID
	}
	if dependencies.Now == nil {
		dependencies.Now = time.Now
	}

	configured, sourceConfig, targetConfig, _, err := delegationconfig.ReadForUpgrade(
		options.ConfigPath, delegationconfig.RuntimeCapabilities{EmbeddedTailscale: true},
	)
	if err != nil {
		return PrepareResult{}, err
	}
	if !reflect.DeepEqual(configured, options.Config) {
		return PrepareResult{}, errors.New("upgrade configuration identity changed before preparation")
	}
	credential, err := credentialSourceMaterial(configured)
	if err != nil {
		return PrepareResult{}, err
	}
	sourceConfigDigest, err := configurationDigest(
		options.ConfigPath, sourceConfig, options.EnvironmentFile, credential,
	)
	if err != nil {
		return PrepareResult{}, err
	}
	targetConfigDigest, err := configurationDigest(
		options.ConfigPath, targetConfig, options.EnvironmentFile, credential,
	)
	if err != nil {
		return PrepareResult{}, err
	}
	sourceDigest, err := regularFileDigest(options.SourceBinary)
	if err != nil {
		return PrepareResult{}, fmt.Errorf("hash source runtime: %w", err)
	}
	verified, err := dependencies.AcquireRelease(ctx, options.CurrentVersion, options.TargetVersion)
	if err != nil {
		return PrepareResult{}, err
	}
	runtimeMaterial, err := dependencies.InstallRuntime(ctx, options.Home, verified)
	if err != nil {
		return PrepareResult{}, err
	}
	transactionID := options.TransactionID
	if transactionID == "" {
		transactionID, err = dependencies.NewID()
		if err != nil {
			return PrepareResult{}, fmt.Errorf("create upgrade transaction ID: %w", err)
		}
	}
	materialRoot := filepath.Join(options.Store.path, "transactions", transactionID)
	if err := delegationconfig.PreparePrivateDirectory(materialRoot); err != nil {
		return PrepareResult{}, fmt.Errorf("prepare upgrade transaction material: %w", err)
	}
	sourceConfigPath := filepath.Join(materialRoot, "source.config.json")
	targetConfigPath := filepath.Join(materialRoot, "target.config.json")
	if err := writeProtectedMaterial(materialRoot, filepath.Base(sourceConfigPath), sourceConfig); err != nil {
		return PrepareResult{}, err
	}
	if err := writeProtectedMaterial(materialRoot, filepath.Base(targetConfigPath), targetConfig); err != nil {
		return PrepareResult{}, err
	}
	compatibility, err := dependencies.ProbeTarget(ctx, runtimeMaterial.BinaryPath, targetConfigPath, options.EnvironmentFile)
	if err != nil {
		return PrepareResult{}, err
	}
	if compatibility.RuntimeVersion != options.TargetVersion {
		return PrepareResult{}, errors.New("target compatibility version does not match the requested release")
	}
	databaseKind, err := databaseKind(options.Config.Role)
	if err != nil {
		return PrepareResult{}, err
	}
	if compatibility.DatabaseKind != databaseKind {
		return PrepareResult{}, errors.New("target runtime database kind does not match the configured role")
	}
	databasePath := configuredDatabasePath(options.Config)
	sourceIdentity, err := store.InspectUpgradeDatabase(ctx, databasePath, databaseKind)
	if err != nil {
		return PrepareResult{}, fmt.Errorf("inspect source database compatibility: %w", err)
	}
	if !store.SupportedUpgradeSource(databaseKind, sourceIdentity, compatibility.DatabaseIdentity) {
		return PrepareResult{}, errors.New("source database schema is not directly compatible with the target runtime")
	}
	if databaseKind == store.DatabasePeer {
		if err := store.ValidatePeerWorkerProfileUpgrade(
			ctx, databasePath, options.Config.ControllerID, options.Config.DeviceID,
			compatibility.WorkerProfileVersion,
		); err != nil {
			return PrepareResult{}, fmt.Errorf("validate worker profile upgrade: %w", err)
		}
	}
	var sourceReadinessEpoch uint64
	if options.Config.Role == delegationconfig.RolePeer && dependencies.ReadReadinessEpoch != nil {
		sourceReadinessEpoch, err = dependencies.ReadReadinessEpoch(ctx)
		if err != nil {
			return PrepareResult{}, fmt.Errorf("read source worker readiness epoch: %w", err)
		}
	}
	blockers, err := dependencies.ReadBlockers(ctx)
	if err != nil {
		return PrepareResult{}, fmt.Errorf("read upgrade blockers: %w", err)
	}
	if !blockers.Empty() {
		return PrepareResult{}, fmt.Errorf("upgrade preflight found active durable work: %+v", blockers)
	}
	serviceRole, err := serviceRole(options.Config.Role)
	if err != nil {
		return PrepareResult{}, err
	}
	sourceInvocation := userservice.Invocation{
		BinaryPath: options.SourceBinary, ConfigPath: options.ConfigPath,
		EnvironmentFile: options.EnvironmentFile, InstanceID: options.Config.EffectiveInstanceID(),
	}
	targetInvocation := sourceInvocation
	targetInvocation.BinaryPath = runtimeMaterial.BinaryPath
	plan, err := dependencies.PrepareService(ctx, serviceRole, sourceInvocation, targetInvocation)
	if err != nil {
		return PrepareResult{}, fmt.Errorf("inspect managed service for upgrade: %w", err)
	}
	if currentBlockers, err := dependencies.ReadBlockers(ctx); err != nil {
		return PrepareResult{}, fmt.Errorf("recheck durable upgrade blockers: %w", err)
	} else if !currentBlockers.Empty() {
		return PrepareResult{}, fmt.Errorf("upgrade preflight changed while preparing: %+v", currentBlockers)
	}
	configDigestAfter, err := CurrentConfigurationDigest(
		options.ConfigPath, options.EnvironmentFile, options.Config.Peer.TraeAuthFile,
	)
	if err != nil || configDigestAfter != sourceConfigDigest {
		return PrepareResult{}, errors.Join(err, errors.New("upgrade configuration changed during preflight"))
	}
	sourceDigestAfter, err := regularFileDigest(options.SourceBinary)
	if err != nil || sourceDigestAfter != sourceDigest {
		return PrepareResult{}, errors.Join(err, errors.New("source runtime changed during preflight"))
	}
	oldDefinitionPath := filepath.Join(materialRoot, "source.definition")
	newDefinitionPath := filepath.Join(materialRoot, "target.definition")
	if err := writeProtectedMaterial(materialRoot, filepath.Base(oldDefinitionPath), plan.OldDefinition); err != nil {
		return PrepareResult{}, err
	}
	if err := writeProtectedMaterial(materialRoot, filepath.Base(newDefinitionPath), plan.NewDefinition); err != nil {
		return PrepareResult{}, err
	}
	databaseMaterial := Database{
		Kind: databaseKind, CanonicalPath: databasePath,
		ShadowPath:     filepath.Join(filepath.Dir(databasePath), "."+filepath.Base(databasePath)+"-upgrade-"+transactionID+".shadow"),
		RollbackPath:   filepath.Join(filepath.Dir(databasePath), "."+filepath.Base(databasePath)+"-upgrade-"+transactionID+".rollback"),
		SourceIdentity: sourceIdentity, TargetIdentity: compatibility.DatabaseIdentity,
	}
	if databaseKind == store.DatabasePeer {
		databaseMaterial.ControllerID = options.Config.ControllerID
		databaseMaterial.DeviceID = options.Config.DeviceID
		databaseMaterial.TargetWorkerProfileVersion = compatibility.WorkerProfileVersion
	}
	now := dependencies.Now().UnixMilli()
	journal := Journal{
		SchemaVersion: JournalSchemaVersion, TransactionID: transactionID, State: StatePrepared,
		ControllerTransactionID: options.ControllerTransactionID,
		Role:                    options.Config.Role, InstanceID: options.Config.EffectiveInstanceID(),
		ControllerID: options.Config.ControllerID, DeviceID: options.Config.DeviceID,
		SourceVersion: options.CurrentVersion, TargetVersion: options.TargetVersion,
		SourceRuntimeDigest: sourceDigest, TargetRuntimeDigest: runtimeMaterial.BinarySHA256,
		ConfigDigest: targetConfigDigest, SourceConfigDigest: sourceConfigDigest,
		SourceReadinessEpoch: sourceReadinessEpoch,
		Platform:             runtime.GOOS, Architecture: runtime.GOARCH,
		Invocation: Invocation{
			BinaryPath: options.SourceBinary, TargetBinaryPath: runtimeMaterial.BinaryPath,
			ConfigPath: options.ConfigPath, EnvironmentFile: options.EnvironmentFile,
			TraeAuthFile: options.Config.Peer.TraeAuthFile,
			NativeName:   plan.NativeName, DefinitionPath: plan.Artifact,
			UserIdentity: plan.UserIdentity, ProcessIDs: append([]int(nil), plan.ProcessIDs...),
			ProcessGroup: plan.ProcessGroup,
		},
		Definition: Definition{
			Kind: plan.Kind, OldDigest: digestBytes(plan.OldDefinition), NewDigest: digestBytes(plan.NewDefinition),
			OldPath: oldDefinitionPath, NewPath: newDefinitionPath,
		},
		Configuration: Configuration{
			CanonicalPath: options.ConfigPath, SourcePath: sourceConfigPath, TargetPath: targetConfigPath,
			ShadowPath:   filepath.Join(filepath.Dir(options.ConfigPath), "."+filepath.Base(options.ConfigPath)+"-upgrade-"+transactionID+".shadow"),
			RollbackPath: filepath.Join(filepath.Dir(options.ConfigPath), "."+filepath.Base(options.ConfigPath)+"-upgrade-"+transactionID+".rollback"),
			SourceDigest: digestBytes(sourceConfig), TargetDigest: digestBytes(targetConfig),
		},
		Database:      databaseMaterial,
		ActivatorPath: filepath.Join(materialRoot, activatorDefinitionName(transactionID)), CreatedAt: now, UpdatedAt: now,
	}
	created, resumed, err := options.Store.CreateOrResume(journal)
	if err != nil {
		return PrepareResult{}, err
	}
	return PrepareResult{Journal: created, Resumed: resumed}, nil
}

func validatePrepareOptions(options PrepareOptions) error {
	if options.Home == "" || !filepath.IsAbs(options.Home) || filepath.Clean(options.Home) != options.Home {
		return errors.New("delegation home must be an absolute clean path")
	}
	for name, path := range map[string]string{"config": options.ConfigPath, "source runtime": options.SourceBinary} {
		if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
			return fmt.Errorf("%s path must be absolute and clean", name)
		}
	}
	if err := options.Config.ValidateForRuntime(delegationconfig.RuntimeCapabilities{EmbeddedTailscale: true}); err != nil {
		return err
	}
	switch options.Config.Role {
	case delegationconfig.RoleBroker:
		if options.EnvironmentFile != "" {
			return errors.New("broker upgrade must not use an environment file")
		}
	case delegationconfig.RolePeer:
		if options.EnvironmentFile == "" || !filepath.IsAbs(options.EnvironmentFile) || filepath.Clean(options.EnvironmentFile) != options.EnvironmentFile {
			return errors.New("peer upgrade requires an absolute clean environment file")
		}
		if runtime.GOOS == "windows" && options.Config.EffectiveHostKind() == hostkind.TraeX {
			return errors.New("TraeX service upgrade is unsupported on Windows")
		}
		if err := codexconfig.ValidateManagedRuntimeHome(options.Config.EffectiveHostKind(), options.Config.Peer.CodexHome); err != nil {
			return fmt.Errorf("validate managed home for upgrade: %w", err)
		}
	default:
		return fmt.Errorf("unsupported upgrade role %q", options.Config.Role)
	}
	return nil
}

func validateResumeRequest(journal Journal, options PrepareOptions) error {
	if journal.Role != options.Config.Role || journal.InstanceID != options.Config.EffectiveInstanceID() ||
		journal.ControllerID != options.Config.ControllerID || journal.DeviceID != options.Config.DeviceID ||
		journal.Invocation.ConfigPath != options.ConfigPath ||
		journal.Invocation.EnvironmentFile != options.EnvironmentFile ||
		journal.Invocation.TraeAuthFile != options.Config.Peer.TraeAuthFile {
		return errors.New("same-target upgrade does not match the configured service identity")
	}
	if options.TransactionID != "" && journal.TransactionID != options.TransactionID {
		return errors.New("same-target upgrade does not match the reserved transaction identity")
	}
	if journal.ControllerTransactionID != options.ControllerTransactionID {
		return errors.New("same-target upgrade does not match the controller transaction identity")
	}
	return nil
}

func protectedConfigurationDigest(paths ...string) (string, error) {
	return workerreadiness.ConfigDigest(paths...)
}

func configurationDigest(
	configPath string, configData []byte, environmentPath string,
	credential workerreadiness.ConfigMaterial,
) (string, error) {
	materials := []workerreadiness.ConfigMaterial{{Path: configPath, Data: configData}}
	if environmentPath != "" {
		data, err := delegationconfig.ReadProtectedFile(environmentPath, maximumConfigDigestFile)
		if err != nil {
			return "", err
		}
		materials = append(materials, workerreadiness.ConfigMaterial{
			Path: environmentPath, Data: data,
		})
	}
	if credential.Path != "" {
		materials = append(materials, credential)
	}
	return workerreadiness.ConfigDigestMaterials(materials...)
}

// CurrentConfigurationDigest returns the readiness identity of the protected
// service inputs currently on disk, including the configured TraeX account
// source. The journal freezes the credential path so activation can verify the
// exact PREPARE input without depending on the config migration phase.
func CurrentConfigurationDigest(
	configPath, environmentPath, traeAuthPath string,
) (string, error) {
	return protectedConfigurationDigest(configPath, environmentPath, traeAuthPath)
}

// JournalConfigurationDigest validates the exact protected inputs frozen in
// an upgrade journal. It intentionally does not decode the configuration: the
// activator may run while the canonical config is at either side of an atomic
// schema migration, and the journal already binds both accepted digests.
func JournalConfigurationDigest(journal Journal) (string, error) {
	return CurrentConfigurationDigest(
		journal.Invocation.ConfigPath, journal.Invocation.EnvironmentFile,
		journal.Invocation.TraeAuthFile,
	)
}

func credentialSourceMaterial(
	cfg delegationconfig.Config,
) (workerreadiness.ConfigMaterial, error) {
	if cfg.Role != delegationconfig.RolePeer || cfg.EffectiveHostKind() != hostkind.TraeX {
		return workerreadiness.ConfigMaterial{}, nil
	}
	data, err := traexauth.ReadSource(cfg.Peer.TraeAuthFile)
	if err != nil {
		return workerreadiness.ConfigMaterial{}, err
	}
	return workerreadiness.ConfigMaterial{Path: cfg.Peer.TraeAuthFile, Data: data}, nil
}

func regularFileDigest(path string) (string, error) { return workerreadiness.RuntimeDigest(path) }

func validateActivationMaterial(journal Journal) error {
	configDigest, err := JournalConfigurationDigest(journal)
	if err != nil || configDigest != journal.SourceConfigDigest && configDigest != journal.ConfigDigest {
		return errors.Join(err, errors.New("upgrade configuration changed after preparation"))
	}
	for _, material := range []struct{ path, digest string }{
		{journal.Configuration.SourcePath, journal.Configuration.SourceDigest},
		{journal.Configuration.TargetPath, journal.Configuration.TargetDigest},
	} {
		data, readErr := delegationconfig.ReadProtectedFile(material.path, maximumConfigDigestFile)
		if readErr != nil || digestBytes(data) != material.digest {
			return errors.Join(readErr, errors.New("protected upgrade configuration material changed"))
		}
	}
	for label, material := range map[string]struct {
		path   string
		digest string
	}{
		"source": {journal.Invocation.BinaryPath, journal.SourceRuntimeDigest},
		"target": {journal.Invocation.TargetBinaryPath, journal.TargetRuntimeDigest},
	} {
		digest, digestErr := regularFileDigest(material.path)
		if digestErr != nil || digest != material.digest {
			return errors.Join(digestErr, fmt.Errorf("%s runtime changed after preparation", label))
		}
	}
	return nil
}

func writeProtectedMaterial(rootPath, name string, data []byte) error {
	root, err := securefs.OpenRoot(rootPath, nil)
	if err != nil {
		return err
	}
	defer root.Close()
	file, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(data)
	if writeErr == nil {
		writeErr = file.Sync()
	}
	closeErr := file.Close()
	if err := errors.Join(writeErr, closeErr); err != nil {
		_ = root.Remove(name)
		return err
	}
	return root.Sync()
}

func configuredDatabasePath(config delegationconfig.Config) string {
	if config.Role == delegationconfig.RoleBroker {
		return config.Broker.StateFile
	}
	return config.Peer.StateFile
}

// ReadConfiguredBlockers opens the configured current-schema database and
// returns the local preflight view used by running-service management.
func ReadConfiguredBlockers(ctx context.Context, config delegationconfig.Config) (store.UpgradeBlockers, error) {
	return defaultBlockers(ctx, config)
}

func defaultBlockers(ctx context.Context, config delegationconfig.Config) (store.UpgradeBlockers, error) {
	kind, err := databaseKind(config.Role)
	if err != nil {
		return store.UpgradeBlockers{}, err
	}
	return store.ReadUpgradeBlockers(
		ctx, configuredDatabasePath(config), kind, config.ControllerID, config.DeviceID,
	)
}

func databaseKind(role delegationconfig.Role) (store.DatabaseKind, error) {
	switch role {
	case delegationconfig.RoleBroker:
		return store.DatabaseBroker, nil
	case delegationconfig.RolePeer:
		return store.DatabasePeer, nil
	default:
		return "", fmt.Errorf("unsupported upgrade role %q", role)
	}
}

func serviceRole(role delegationconfig.Role) (userservice.ServiceRole, error) {
	switch role {
	case delegationconfig.RoleBroker:
		return userservice.ServiceRoleBroker, nil
	case delegationconfig.RolePeer:
		return userservice.ServiceRolePeer, nil
	default:
		return "", fmt.Errorf("unsupported upgrade role %q", role)
	}
}

func activatorDefinitionName(transactionID string) string {
	switch runtime.GOOS {
	case "linux":
		return "delegation-upgrade-" + transactionID + ".service"
	case "darwin":
		return "com.github.ghostflying.delegation.upgrade." + transactionID + ".plist"
	case "windows":
		return "delegation-upgrade-" + transactionID + ".xml"
	default:
		return "delegation-upgrade-" + transactionID + ".definition"
	}
}

type boundedOutput struct {
	bytes.Buffer
	maximum int
}

func (b *boundedOutput) Write(data []byte) (int, error) {
	if b.Len()+len(data) > b.maximum {
		return 0, errors.New("target compatibility output exceeds its size limit")
	}
	return b.Buffer.Write(data)
}
