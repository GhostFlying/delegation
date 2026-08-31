package localupgrade

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/store"
	"github.com/GhostFlying/delegation/internal/userservice"
)

func TestJournalCreateResumeConflictAndMonotonicTransitions(t *testing.T) {
	root := filepath.Join(t.TempDir(), "upgrade")
	transactionStore, err := OpenStore(root)
	if err != nil {
		t.Fatal(err)
	}
	journal := testJournal(t)
	created, resumed, err := transactionStore.CreateOrResume(journal)
	if err != nil || resumed {
		t.Fatalf("CreateOrResume() = %#v, %v, %v", created, resumed, err)
	}
	loaded, resumed, err := transactionStore.CreateOrResume(journal)
	if err != nil || !resumed || loaded.TransactionID != journal.TransactionID {
		t.Fatalf("resume = %#v, %v, %v", loaded, resumed, err)
	}
	conflict := journal
	conflict.TargetVersion = "0.1.0-alpha.9"
	if _, _, err := transactionStore.CreateOrResume(conflict); !errors.Is(err, ErrTargetConflict) {
		t.Fatalf("conflicting target error = %v", err)
	}
	updated, err := transactionStore.Update(journal.TransactionID, func(current *Journal) error {
		current.State = StateArmed
		return nil
	})
	if err != nil || updated.State != StateArmed {
		t.Fatalf("arm = %#v, %v", updated, err)
	}
	updated, err = transactionStore.Update(journal.TransactionID, func(current *Journal) error {
		current.CommitAuthorized = true
		current.State = StateActivating
		return nil
	})
	if err != nil || !updated.CommitAuthorized {
		t.Fatalf("authorize = %#v, %v", updated, err)
	}
	if _, err := transactionStore.Update(journal.TransactionID, func(current *Journal) error {
		current.CommitAuthorized = false
		return nil
	}); err == nil {
		t.Fatal("Update accepted cleared commit authorization")
	}
	if _, err := transactionStore.Update(journal.TransactionID, func(current *Journal) error {
		current.State = StateRollbackRequired
		return nil
	}); err == nil {
		t.Fatal("Update accepted rollback after commit authorization")
	}
}

func TestJournalRejectsAuthorizationOutsideArmedActivationAndBackwardPhases(t *testing.T) {
	for _, initial := range []State{StatePrepared, StateStarted} {
		t.Run(string(initial), func(t *testing.T) {
			before := testJournal(t)
			before.State = initial
			after := before
			after.CommitAuthorized = true
			if err := validateMutation(before, after); err == nil {
				t.Fatalf("same-state %s authorization was accepted", initial)
			}
		})
	}
	transactionStore, err := OpenStore(filepath.Join(t.TempDir(), "upgrade"))
	if err != nil {
		t.Fatal(err)
	}
	journal := testJournal(t)
	if _, _, err := transactionStore.CreateOrResume(journal); err != nil {
		t.Fatal(err)
	}
	if _, err := transactionStore.Update(journal.TransactionID, func(current *Journal) error {
		current.State = StateArmed
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := transactionStore.Update(journal.TransactionID, func(current *Journal) error {
		current.State = StateActivating
		return nil
	}); err == nil {
		t.Fatal("unauthorized armed to activating transition was accepted")
	}
}

func TestSameTargetTerminalJournalResumesAndHigherTargetMayReplace(t *testing.T) {
	transactionStore, err := OpenStore(filepath.Join(t.TempDir(), "upgrade"))
	if err != nil {
		t.Fatal(err)
	}
	journal := testJournal(t)
	if _, _, err := transactionStore.CreateOrResume(journal); err != nil {
		t.Fatal(err)
	}
	for _, mutation := range []func(*Journal){
		func(current *Journal) { current.State = StateArmed },
		func(current *Journal) { current.State = StateActivating; current.CommitAuthorized = true },
		func(current *Journal) {
			current.State = StateStarted
			current.Progress = fullProgress()
			current.Progress.Qualified = false
		},
		func(current *Journal) { current.State = StateQualified; current.Progress = fullProgress() },
		func(current *Journal) { current.State = StateCommitted },
	} {
		if _, err := transactionStore.Update(journal.TransactionID, func(current *Journal) error {
			mutation(current)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	resumed, didResume, err := transactionStore.CreateOrResume(journal)
	if err != nil || !didResume || resumed.State != StateCommitted {
		t.Fatalf("terminal resume = %#v, %v, %v", resumed, didResume, err)
	}
	higher := testJournal(t)
	higher.TransactionID = "123e4567-e89b-42d3-a456-426614174899"
	higher.SourceVersion = journal.TargetVersion
	higher.TargetVersion = "0.1.0-alpha.9"
	created, didResume, err := transactionStore.CreateOrResume(higher)
	if err != nil || didResume || created.TransactionID != higher.TransactionID {
		t.Fatalf("higher target = %#v, %v, %v", created, didResume, err)
	}
}

func TestJournalRejectsImmutableMutationAndUnknownFields(t *testing.T) {
	transactionStore, err := OpenStore(filepath.Join(t.TempDir(), "upgrade"))
	if err != nil {
		t.Fatal(err)
	}
	journal := testJournal(t)
	if _, _, err := transactionStore.CreateOrResume(journal); err != nil {
		t.Fatal(err)
	}
	if _, err := transactionStore.Update(journal.TransactionID, func(current *Journal) error {
		current.Invocation.ConfigPath += "-changed"
		return nil
	}); err == nil {
		t.Fatal("Update accepted immutable config path mutation")
	}
	path := filepath.Join(transactionStore.Path(), journalName)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data = []byte(strings.Replace(string(data), `"schemaVersion":1`, `"unknown":true,"schemaVersion":1`, 1))
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := transactionStore.Load(); err == nil {
		t.Fatal("Load accepted unknown journal field")
	}
}

func TestEveryJournalStateValidatesAndSnapshotOmitsSecrets(t *testing.T) {
	states := []State{
		StatePrepared, StateArmed, StateActivating, StateStarted, StateQualified, StateCommitted,
		StateRollbackRequired, StateRolledBack, StateRollbackFailed, StateForwardRecoveryRequired,
	}
	for _, state := range states {
		t.Run(string(state), func(t *testing.T) {
			journal := testJournal(t)
			journal.State = state
			switch state {
			case StateCommitted:
				journal.CommitAuthorized = true
				journal.Progress = fullProgress()
			case StateQualified:
				journal.CommitAuthorized = true
				journal.Progress = fullProgress()
			case StateStarted:
				journal.CommitAuthorized = true
				journal.Progress = fullProgress()
				journal.Progress.Qualified = false
			case StateActivating:
				journal.CommitAuthorized = true
			case StateForwardRecoveryRequired:
				journal.CommitAuthorized = true
			}
			if err := journal.Validate(); err != nil {
				t.Fatal(err)
			}
		})
	}
	journal := testJournal(t)
	snapshot := journal.Snapshot()
	if snapshot.TransactionID != journal.TransactionID || snapshot.TargetVersion != journal.TargetVersion {
		t.Fatalf("snapshot = %#v", snapshot)
	}
	if strings.Contains(strings.TrimSpace(snapshot.FailureCode), "secret") {
		t.Fatal("snapshot exposed secret")
	}
}

func fullProgress() Progress {
	return Progress{
		ServiceStopped: true, ConfigurationPrepared: true, DatabasePrepared: true,
		DefinitionSwitched: true, ConfigurationSwitched: true, DatabaseSwitched: true,
		ServiceStarted: true, Qualified: true,
	}
}

func testJournal(t *testing.T) Journal {
	t.Helper()
	root := t.TempDir()
	identity, err := store.CurrentDatabaseIdentity(store.DatabasePeer)
	if err != nil {
		t.Fatal(err)
	}
	kind := userservice.KindSystemd
	processGroup := "/user.slice/delegation"
	switch runtime.GOOS {
	case "darwin":
		kind = userservice.KindLaunchAgent
		processGroup = ""
	case "windows":
		kind = userservice.KindScheduledTask
		processGroup = ""
	}
	return Journal{
		SchemaVersion: JournalSchemaVersion, TransactionID: "123e4567-e89b-42d3-a456-426614174800",
		State: StatePrepared, Role: delegationconfig.RolePeer, InstanceID: "default",
		ControllerID:  "123e4567-e89b-42d3-a456-426614174801",
		DeviceID:      "123e4567-e89b-42d3-a456-426614174802",
		SourceVersion: "0.1.0-alpha.7", TargetVersion: "0.1.0-alpha.8",
		SourceRuntimeDigest: strings.Repeat("1", 64), TargetRuntimeDigest: strings.Repeat("2", 64),
		ConfigDigest: strings.Repeat("3", 64), SourceConfigDigest: strings.Repeat("3", 64),
		SourceReadinessEpoch: 1, Platform: runtime.GOOS, Architecture: runtime.GOARCH,
		Invocation: Invocation{
			BinaryPath: filepath.Join(root, "old"), TargetBinaryPath: filepath.Join(root, "new"),
			ConfigPath:      filepath.Join(root, "peer.json"),
			EnvironmentFile: filepath.Join(root, "peer.env"), NativeName: "delegation-peer.service",
			DefinitionPath: filepath.Join(root, "delegation-peer.service"), UserIdentity: "current-user",
			ProcessIDs: []int{123}, ProcessGroup: processGroup,
		},
		Definition: Definition{
			Kind: kind, OldDigest: strings.Repeat("4", 64),
			NewDigest: strings.Repeat("5", 64), OldPath: filepath.Join(root, "old.service"),
			NewPath: filepath.Join(root, "new.service"),
		},
		Configuration: Configuration{
			CanonicalPath: filepath.Join(root, "peer.json"),
			SourcePath:    filepath.Join(root, "source.config.json"),
			TargetPath:    filepath.Join(root, "target.config.json"),
			ShadowPath:    filepath.Join(root, ".peer.json.shadow"),
			RollbackPath:  filepath.Join(root, ".peer.json.rollback"),
			SourceDigest:  strings.Repeat("6", 64), TargetDigest: strings.Repeat("6", 64),
		},
		Database: Database{
			Kind: store.DatabasePeer, CanonicalPath: filepath.Join(root, "peer.sqlite3"),
			ShadowPath:     filepath.Join(root, "peer.shadow.sqlite3"),
			RollbackPath:   filepath.Join(root, "peer.rollback.sqlite3"),
			SourceIdentity: identity, TargetIdentity: identity,
		},
		ActivatorPath: filepath.Join(root, "activate"), CreatedAt: time.Now().UnixMilli(),
		UpdatedAt: time.Now().UnixMilli(),
	}
}
