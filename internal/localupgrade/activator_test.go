package localupgrade

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/GhostFlying/delegation/internal/userservice"
)

func TestActivatorResumesAfterEveryUnjournaledAction(t *testing.T) {
	steps := []activationStep{
		stepServiceStop, stepConfigurationPrepare, stepDatabasePrepare, stepDefinitionSwitch,
		stepConfigurationSwitch, stepDatabaseSwitch, stepServiceStart, stepQualification,
	}
	for _, step := range steps {
		t.Run(string(step), func(t *testing.T) {
			transactionStore, journal := createAuthorizedActivationJournal(t)
			fake := newFakeActivation()
			activator, err := NewActivator(transactionStore, fake.operations())
			if err != nil {
				t.Fatal(err)
			}
			crashed := false
			activator.afterAction = func(completed activationStep) error {
				if completed == step && !crashed {
					crashed = true
					return errors.New("injected activator process loss")
				}
				return nil
			}
			if _, err := activator.Run(context.Background(), journal.TransactionID); err == nil {
				t.Fatal("first activation did not reach injected crash point")
			}
			result, err := activator.Run(context.Background(), journal.TransactionID)
			if err != nil {
				t.Fatal(err)
			}
			if result.State != StateCommitted || !result.Progress.Qualified ||
				result.FailureCode != "" {
				t.Fatalf("resumed activation = %#v", result)
			}
			if fake.startCalls != 1 {
				t.Fatalf("target start calls = %d, want exactly one", fake.startCalls)
			}
		})
	}
}

func TestActivatorRecordsForwardRecoveryAndResumesTarget(t *testing.T) {
	transactionStore, journal := createAuthorizedActivationJournal(t)
	fake := newFakeActivation()
	fake.switchDatabaseFailures = 1
	activator, err := NewActivator(transactionStore, fake.operations())
	if err != nil {
		t.Fatal(err)
	}
	failed, err := activator.Run(context.Background(), journal.TransactionID)
	if err == nil || failed.State != StateForwardRecoveryRequired ||
		failed.FailureCode != "database_switch_failed" {
		t.Fatalf("failed activation = %#v, %v", failed, err)
	}
	resumed, err := activator.Run(context.Background(), journal.TransactionID)
	if err != nil || resumed.State != StateCommitted || !resumed.Progress.Qualified ||
		resumed.FailureCode != "" {
		t.Fatalf("forward recovery = %#v, %v", resumed, err)
	}
}

func TestActivatorRefusesMutationBeforeCommitAuthorization(t *testing.T) {
	transactionStore, journal := createActivationJournal(t)
	fake := newFakeActivation()
	activator, err := NewActivator(transactionStore, fake.operations())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := activator.Run(context.Background(), journal.TransactionID); err == nil {
		t.Fatal("activator accepted an unauthorized transaction")
	}
	if fake.stopCalls != 0 || fake.prepareDatabaseCalls != 0 || fake.startCalls != 0 {
		t.Fatalf("unauthorized activation performed mutations: %#v", fake)
	}
}

func TestCancelStopsAtDurableAuthorizationBoundary(t *testing.T) {
	t.Run("prepared", func(t *testing.T) {
		transactionStore, journal := createActivationJournal(t)
		canceled, err := Cancel(transactionStore, journal.TransactionID)
		if err != nil || canceled.State != StateRolledBack {
			t.Fatalf("Cancel() = %#v, %v", canceled, err)
		}
		resumed, err := Cancel(transactionStore, journal.TransactionID)
		if err != nil || resumed.State != StateRolledBack {
			t.Fatalf("repeated Cancel() = %#v, %v", resumed, err)
		}
	})
	t.Run("armed", func(t *testing.T) {
		transactionStore, journal := createActivationJournal(t)
		if _, err := transactionStore.Update(journal.TransactionID, func(current *Journal) error {
			current.State = StateArmed
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		canceled, err := Cancel(transactionStore, journal.TransactionID)
		if err != nil || canceled.State != StateRolledBack {
			t.Fatalf("Cancel() = %#v, %v", canceled, err)
		}
	})
	t.Run("authorized", func(t *testing.T) {
		transactionStore, journal := createAuthorizedActivationJournal(t)
		if _, err := Cancel(transactionStore, journal.TransactionID); err == nil {
			t.Fatal("Cancel() accepted an authorized transaction")
		}
	})
}

func TestServicePlanRejectsChangedProtectedDefinitions(t *testing.T) {
	_, journal := createActivationJournal(t)
	if err := os.WriteFile(journal.Definition.NewPath, []byte("changed"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := servicePlanFromJournal(journal); err == nil {
		t.Fatal("servicePlanFromJournal accepted changed definition material")
	}
}

type fakeActivation struct {
	stopped                bool
	running                bool
	stopCalls              int
	prepareDatabaseCalls   int
	switchDefinitionCalls  int
	switchDatabaseCalls    int
	startCalls             int
	qualifyCalls           int
	switchDatabaseFailures int
}

func newFakeActivation() *fakeActivation {
	return &fakeActivation{}
}

func (f *fakeActivation) operations() ActivationOperations {
	return ActivationOperations{
		StopService: func(context.Context, userservice.UpgradePlan) error {
			f.stopCalls++
			f.stopped = true
			f.running = false
			return nil
		},
		AcquireLease: func(Journal) (ActivationLease, error) { return testActivationLease{}, nil },
		PrepareConfiguration: func(configuration Configuration) (Configuration, error) {
			return configuration, nil
		},
		PrepareDatabase: func(_ context.Context, database Database) (Database, error) {
			f.prepareDatabaseCalls++
			if !f.stopped {
				return Database{}, errors.New("database prepared before service stopped")
			}
			database.SourceDigest = digestBytes([]byte("source database"))
			database.TargetDigest = digestBytes([]byte("target database"))
			return database, nil
		},
		SwitchDefinition: func(_ context.Context, _ userservice.UpgradePlan, target bool) error {
			if !target || !f.stopped {
				return errors.New("unsafe definition switch")
			}
			f.switchDefinitionCalls++
			return nil
		},
		SwitchDatabase: func(Database) (bool, error) {
			f.switchDatabaseCalls++
			if f.switchDatabaseFailures > 0 {
				f.switchDatabaseFailures--
				return false, errors.New("injected database switch failure")
			}
			return true, nil
		},
		SwitchConfiguration: func(Configuration) (bool, error) { return true, nil },
		ServiceMatches: func(context.Context, userservice.UpgradePlan, bool) (bool, error) {
			return f.running, nil
		},
		StartService: func(_ context.Context, _ userservice.UpgradePlan, target bool) error {
			if !target {
				return errors.New("activator attempted source rollback")
			}
			f.startCalls++
			f.running = true
			return nil
		},
		Qualify: func(context.Context, Journal) error {
			f.qualifyCalls++
			if !f.running {
				return errors.New("qualification ran before target start")
			}
			return nil
		},
	}
}

type testActivationLease struct{}

func (testActivationLease) Close() error { return nil }

func createActivationJournal(t *testing.T) (*Store, Journal) {
	t.Helper()
	journal := testJournal(t)
	oldDefinition := []byte("old service definition\n")
	newDefinition := []byte("new service definition\n")
	config := []byte("config\n")
	environment := []byte("environment\n")
	sourceRuntime := []byte("source runtime\n")
	targetRuntime := []byte("target runtime\n")
	for path, content := range map[string][]byte{
		journal.Definition.OldPath:          oldDefinition,
		journal.Definition.NewPath:          newDefinition,
		journal.Invocation.ConfigPath:       config,
		journal.Configuration.SourcePath:    config,
		journal.Configuration.TargetPath:    config,
		journal.Invocation.EnvironmentFile:  environment,
		journal.Invocation.BinaryPath:       sourceRuntime,
		journal.Invocation.TargetBinaryPath: targetRuntime,
	} {
		if err := os.WriteFile(path, content, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	journal.Definition.OldDigest = digestBytes(oldDefinition)
	journal.Definition.NewDigest = digestBytes(newDefinition)
	journal.SourceRuntimeDigest = digestBytes(sourceRuntime)
	journal.TargetRuntimeDigest = digestBytes(targetRuntime)
	configDigest, err := protectedConfigurationDigest(
		journal.Invocation.ConfigPath, journal.Invocation.EnvironmentFile,
	)
	if err != nil {
		t.Fatal(err)
	}
	journal.ConfigDigest = configDigest
	journal.SourceConfigDigest = configDigest
	journal.Configuration.SourceDigest = digestBytes(config)
	journal.Configuration.TargetDigest = digestBytes(config)
	transactionStore, err := OpenStore(filepath.Join(filepath.Dir(journal.ActivatorPath), "transaction"))
	if err != nil {
		t.Fatal(err)
	}
	journal, resumed, err := transactionStore.CreateOrResume(journal)
	if err != nil || resumed {
		t.Fatalf("CreateOrResume() = %#v, %v, %v", journal, resumed, err)
	}
	return transactionStore, journal
}

func createAuthorizedActivationJournal(t *testing.T) (*Store, Journal) {
	t.Helper()
	transactionStore, journal := createActivationJournal(t)
	var err error
	journal, err = transactionStore.Update(journal.TransactionID, func(current *Journal) error {
		current.State = StateArmed
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	journal, err = transactionStore.Update(journal.TransactionID, func(current *Journal) error {
		current.State = StateActivating
		current.CommitAuthorized = true
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return transactionStore, journal
}
