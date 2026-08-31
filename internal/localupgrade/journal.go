// Package localupgrade implements the durable, role-local half of a managed
// Delegation service upgrade. Controller-wide coordination is deliberately
// outside this package.
package localupgrade

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/identity"
	"github.com/GhostFlying/delegation/internal/instanceid"
	"github.com/GhostFlying/delegation/internal/store"
	"github.com/GhostFlying/delegation/internal/userservice"
	"golang.org/x/mod/semver"
)

const (
	JournalSchemaVersion = 2
	maximumJournalBytes  = 64 << 10
	maximumFailureBytes  = 64
)

type State string

const (
	StatePrepared                State = "prepared"
	StateArmed                   State = "armed"
	StateActivating              State = "activating"
	StateStarted                 State = "started"
	StateQualified               State = "qualified"
	StateCommitted               State = "committed"
	StateRollbackRequired        State = "rollback_required"
	StateRolledBack              State = "rolled_back"
	StateRollbackFailed          State = "rollback_failed"
	StateForwardRecoveryRequired State = "forward_recovery_required"
)

type Invocation struct {
	BinaryPath       string `json:"binaryPath"`
	TargetBinaryPath string `json:"targetBinaryPath"`
	ConfigPath       string `json:"configPath"`
	EnvironmentFile  string `json:"environmentFile,omitempty"`
	NativeName       string `json:"nativeName"`
	DefinitionPath   string `json:"definitionPath"`
	UserIdentity     string `json:"userIdentity"`
	ProcessIDs       []int  `json:"processIds"`
	ProcessGroup     string `json:"processGroup,omitempty"`
}

type Definition struct {
	Kind      userservice.Kind `json:"kind"`
	OldDigest string           `json:"oldDigest"`
	NewDigest string           `json:"newDigest"`
	OldPath   string           `json:"oldPath"`
	NewPath   string           `json:"newPath"`
}

type Database struct {
	Kind           store.DatabaseKind     `json:"kind"`
	CanonicalPath  string                 `json:"canonicalPath"`
	ShadowPath     string                 `json:"shadowPath"`
	RollbackPath   string                 `json:"rollbackPath"`
	SourceIdentity store.DatabaseIdentity `json:"sourceIdentity"`
	TargetIdentity store.DatabaseIdentity `json:"targetIdentity"`
	SourceDigest   string                 `json:"sourceDigest,omitempty"`
	TargetDigest   string                 `json:"targetDigest,omitempty"`
}

type Configuration struct {
	CanonicalPath string `json:"canonicalPath"`
	SourcePath    string `json:"sourcePath"`
	TargetPath    string `json:"targetPath"`
	ShadowPath    string `json:"shadowPath"`
	RollbackPath  string `json:"rollbackPath"`
	SourceDigest  string `json:"sourceDigest"`
	TargetDigest  string `json:"targetDigest"`
}

type Progress struct {
	ServiceStopped        bool `json:"serviceStopped"`
	ConfigurationPrepared bool `json:"configurationPrepared"`
	DatabasePrepared      bool `json:"databasePrepared"`
	DefinitionSwitched    bool `json:"definitionSwitched"`
	ConfigurationSwitched bool `json:"configurationSwitched"`
	DatabaseSwitched      bool `json:"databaseSwitched"`
	ServiceStarted        bool `json:"serviceStarted"`
	Qualified             bool `json:"qualified"`
}

type Journal struct {
	SchemaVersion           int                   `json:"schemaVersion"`
	TransactionID           string                `json:"transactionId"`
	State                   State                 `json:"state"`
	CommitAuthorized        bool                  `json:"commitAuthorized"`
	Role                    delegationconfig.Role `json:"role"`
	InstanceID              string                `json:"instanceId"`
	ControllerID            string                `json:"controllerId"`
	ControllerTransactionID string                `json:"controllerTransactionId,omitempty"`
	DeviceID                string                `json:"deviceId,omitempty"`
	SourceVersion           string                `json:"sourceVersion"`
	TargetVersion           string                `json:"targetVersion"`
	SourceRuntimeDigest     string                `json:"sourceRuntimeDigest"`
	TargetRuntimeDigest     string                `json:"targetRuntimeDigest"`
	ConfigDigest            string                `json:"configDigest"`
	SourceConfigDigest      string                `json:"sourceConfigDigest"`
	SourceReadinessEpoch    uint64                `json:"sourceReadinessEpoch,omitempty"`
	Platform                string                `json:"platform"`
	Architecture            string                `json:"architecture"`
	Invocation              Invocation            `json:"invocation"`
	Definition              Definition            `json:"definition"`
	Configuration           Configuration         `json:"configuration"`
	Database                Database              `json:"database"`
	ActivatorPath           string                `json:"activatorPath"`
	Progress                Progress              `json:"progress"`
	FailureCode             string                `json:"failureCode,omitempty"`
	CreatedAt               int64                 `json:"createdAt"`
	UpdatedAt               int64                 `json:"updatedAt"`
}

type Snapshot struct {
	TransactionID    string `json:"transactionId"`
	State            State  `json:"state"`
	SourceVersion    string `json:"sourceVersion"`
	TargetVersion    string `json:"targetVersion"`
	CommitAuthorized bool   `json:"commitAuthorized"`
	FailureCode      string `json:"failureCode,omitempty"`
	UpdatedAt        int64  `json:"updatedAt"`
}

func (j Journal) Snapshot() Snapshot {
	return Snapshot{
		TransactionID: j.TransactionID, State: j.State, SourceVersion: j.SourceVersion,
		TargetVersion: j.TargetVersion, CommitAuthorized: j.CommitAuthorized,
		FailureCode: j.FailureCode, UpdatedAt: j.UpdatedAt,
	}
}

func (j Journal) Terminal() bool {
	switch j.State {
	case StateCommitted, StateRolledBack, StateRollbackFailed:
		return true
	default:
		return false
	}
}

func (j Journal) Validate() error {
	if j.SchemaVersion != JournalSchemaVersion {
		return fmt.Errorf("unsupported upgrade journal schema %d", j.SchemaVersion)
	}
	if err := identity.ValidateID(j.TransactionID); err != nil {
		return fmt.Errorf("transactionId %w", err)
	}
	if err := validateState(j.State); err != nil {
		return err
	}
	if err := instanceid.Validate(j.InstanceID); err != nil {
		return fmt.Errorf("instanceId %w", err)
	}
	if err := identity.ValidateID(j.ControllerID); err != nil {
		return fmt.Errorf("controllerId %w", err)
	}
	if j.ControllerTransactionID != "" {
		if err := identity.ValidateID(j.ControllerTransactionID); err != nil {
			return fmt.Errorf("controllerTransactionId %w", err)
		}
	}
	switch j.Role {
	case delegationconfig.RoleBroker:
		if j.DeviceID != "" || j.Invocation.EnvironmentFile != "" || j.SourceReadinessEpoch != 0 {
			return errors.New("broker journal contains peer-only identity")
		}
	case delegationconfig.RolePeer:
		if identity.ValidateID(j.DeviceID) != nil || j.Invocation.EnvironmentFile == "" {
			return errors.New("peer journal identity is incomplete")
		}
	default:
		return fmt.Errorf("unsupported upgrade role %q", j.Role)
	}
	if !validVersion(j.SourceVersion) || !validVersion(j.TargetVersion) ||
		semver.Compare("v"+j.TargetVersion, "v"+j.SourceVersion) <= 0 {
		return errors.New("upgrade versions are not a strict forward transition")
	}
	for name, digest := range map[string]string{
		"source runtime": j.SourceRuntimeDigest, "target runtime": j.TargetRuntimeDigest,
		"config": j.ConfigDigest, "source config": j.SourceConfigDigest,
		"old definition":       j.Definition.OldDigest,
		"new definition":       j.Definition.NewDigest,
		"configuration source": j.Configuration.SourceDigest,
		"configuration target": j.Configuration.TargetDigest,
	} {
		if !validDigest(digest) {
			return fmt.Errorf("%s digest is invalid", name)
		}
	}
	for name, digest := range map[string]string{
		"source database": j.Database.SourceDigest, "target database": j.Database.TargetDigest,
	} {
		if digest != "" && !validDigest(digest) {
			return fmt.Errorf("%s digest is invalid", name)
		}
	}
	paths := []string{
		j.Invocation.BinaryPath, j.Invocation.TargetBinaryPath, j.Invocation.ConfigPath,
		j.Invocation.DefinitionPath,
		j.Definition.OldPath, j.Definition.NewPath, j.Database.CanonicalPath,
		j.Database.ShadowPath, j.Database.RollbackPath, j.ActivatorPath,
		j.Configuration.CanonicalPath, j.Configuration.SourcePath,
		j.Configuration.TargetPath, j.Configuration.ShadowPath,
		j.Configuration.RollbackPath,
	}
	if j.Invocation.EnvironmentFile != "" {
		paths = append(paths, j.Invocation.EnvironmentFile)
	}
	for _, path := range paths {
		if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
			return errors.New("upgrade journal paths must be non-empty, absolute, and clean")
		}
	}
	if j.Configuration.CanonicalPath != j.Invocation.ConfigPath ||
		filepath.Dir(j.Configuration.CanonicalPath) != filepath.Dir(j.Configuration.ShadowPath) ||
		filepath.Dir(j.Configuration.CanonicalPath) != filepath.Dir(j.Configuration.RollbackPath) ||
		j.Configuration.CanonicalPath == j.Configuration.ShadowPath ||
		j.Configuration.CanonicalPath == j.Configuration.RollbackPath ||
		j.Configuration.ShadowPath == j.Configuration.RollbackPath {
		return errors.New("upgrade configuration material paths are inconsistent")
	}
	if j.Database.CanonicalPath == j.Database.ShadowPath ||
		j.Database.CanonicalPath == j.Database.RollbackPath ||
		j.Database.ShadowPath == j.Database.RollbackPath ||
		filepath.Dir(j.Database.CanonicalPath) != filepath.Dir(j.Database.ShadowPath) ||
		filepath.Dir(j.Database.CanonicalPath) != filepath.Dir(j.Database.RollbackPath) {
		return errors.New("upgrade database material must use distinct names in one directory")
	}
	if _, err := store.CurrentDatabaseIdentity(j.Database.Kind); err != nil {
		return err
	}
	if j.Database.SourceIdentity.ApplicationID == 0 || j.Database.SourceIdentity.SchemaVersion <= 0 ||
		j.Database.TargetIdentity.ApplicationID != j.Database.SourceIdentity.ApplicationID ||
		j.Database.TargetIdentity.SchemaVersion < j.Database.SourceIdentity.SchemaVersion {
		return errors.New("upgrade database identities are incompatible")
	}
	if j.Platform != runtime.GOOS || j.Architecture != runtime.GOARCH {
		return errors.New("upgrade journal platform does not match this runtime")
	}
	if j.Invocation.NativeName == "" || !validBoundedText(j.Invocation.NativeName, 256) ||
		j.Invocation.UserIdentity == "" || !validBoundedText(j.Invocation.UserIdentity, 256) {
		return errors.New("upgrade native service identity is invalid")
	}
	if len(j.Invocation.ProcessIDs) == 0 || len(j.Invocation.ProcessIDs) > 2 {
		return errors.New("upgrade source process identity is invalid")
	}
	seenPIDs := make(map[int]struct{}, len(j.Invocation.ProcessIDs))
	for _, pid := range j.Invocation.ProcessIDs {
		if pid <= 0 {
			return errors.New("upgrade source process identity is invalid")
		}
		if _, exists := seenPIDs[pid]; exists {
			return errors.New("upgrade source process identity contains duplicates")
		}
		seenPIDs[pid] = struct{}{}
	}
	switch j.Definition.Kind {
	case userservice.KindSystemd:
		if !validBoundedText(j.Invocation.ProcessGroup, 4096) {
			return errors.New("systemd upgrade process group is invalid")
		}
	case userservice.KindLaunchAgent, userservice.KindScheduledTask:
		if j.Invocation.ProcessGroup != "" {
			return errors.New("non-systemd upgrade contains a process group")
		}
	default:
		return fmt.Errorf("unsupported service definition kind %q", j.Definition.Kind)
	}
	if (runtime.GOOS == "linux" && j.Definition.Kind != userservice.KindSystemd) ||
		(runtime.GOOS == "darwin" && j.Definition.Kind != userservice.KindLaunchAgent) ||
		(runtime.GOOS == "windows" && j.Definition.Kind != userservice.KindScheduledTask) {
		return errors.New("upgrade service definition does not match this platform")
	}
	if j.FailureCode != "" && !validFailureCode(j.FailureCode) {
		return errors.New("upgrade failure code is invalid")
	}
	if j.CreatedAt <= 0 || j.UpdatedAt < j.CreatedAt {
		return errors.New("upgrade journal timestamps are invalid")
	}
	if j.State == StateCommitted && (!j.CommitAuthorized || !j.Progress.Qualified) {
		return errors.New("committed upgrade lacks authorization or qualification")
	}
	if j.State == StateQualified && (!j.CommitAuthorized || !j.Progress.Qualified) {
		return errors.New("qualified upgrade lacks qualification progress")
	}
	if j.State == StateStarted && (!j.CommitAuthorized || !j.Progress.ServiceStarted) {
		return errors.New("started upgrade lacks authorization or start progress")
	}
	if j.State == StateActivating && !j.CommitAuthorized {
		return errors.New("activation requires durable commit authorization")
	}
	if (j.State == StateRollbackRequired || j.State == StateRolledBack || j.State == StateRollbackFailed) &&
		j.CommitAuthorized {
		return errors.New("authorized upgrade cannot enter rollback state")
	}
	if j.State == StateForwardRecoveryRequired && !j.CommitAuthorized {
		return errors.New("forward recovery requires durable commit authorization")
	}
	if (j.Progress.ConfigurationPrepared && !j.Progress.ServiceStopped) ||
		(j.Progress.DatabasePrepared && !j.Progress.ConfigurationPrepared) ||
		(j.Progress.DefinitionSwitched && !j.Progress.DatabasePrepared) ||
		(j.Progress.ConfigurationSwitched && !j.Progress.DefinitionSwitched) ||
		(j.Progress.DatabaseSwitched && !j.Progress.ConfigurationSwitched) ||
		(j.Progress.ServiceStarted && !j.Progress.DatabaseSwitched) ||
		(j.Progress.Qualified && !j.Progress.ServiceStarted) {
		return errors.New("upgrade activation progress is inconsistent")
	}
	return nil
}

func validateState(state State) error {
	switch state {
	case StatePrepared, StateArmed, StateActivating, StateStarted, StateQualified, StateCommitted,
		StateRollbackRequired, StateRolledBack, StateRollbackFailed, StateForwardRecoveryRequired:
		return nil
	default:
		return fmt.Errorf("unsupported upgrade state %q", state)
	}
}

func validDigest(value string) bool {
	if len(value) != sha256.Size*2 || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

var versionPattern = regexp.MustCompile(`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$`)

func validVersion(value string) bool {
	return versionPattern.MatchString(value) && semver.IsValid("v"+value)
}

func validFailureCode(value string) bool {
	if len(value) > maximumFailureBytes || value == "" {
		return false
	}
	for index, character := range value {
		if (character >= 'a' && character <= 'z') || (index > 0 && character >= '0' && character <= '9') ||
			(index > 0 && character == '_') {
			continue
		}
		return false
	}
	return true
}

func validBoundedText(value string, maximum int) bool {
	if len(value) == 0 || len(value) > maximum || !utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if !unicode.IsPrint(character) {
			return false
		}
	}
	return true
}

func nextTimestamp(previous int64, now time.Time) int64 {
	value := now.UnixMilli()
	if value <= previous {
		return previous + 1
	}
	return value
}
