package localupgrade

import (
	"context"
	"errors"
	"os"
	"testing"
)

func TestManagerArmPersistsDefinitionAndIsIdempotent(t *testing.T) {
	transactionStore, journal := createActivationJournal(t)
	fake := &fakeOneShot{}
	manager := newTestManager(t, transactionStore, fake)

	armed, err := manager.Arm(context.Background(), journal.TransactionID)
	if err != nil || armed.State != StateArmed || fake.installCalls != 1 || !fake.installed {
		t.Fatalf("Arm() = %#v, %v, fake=%#v", armed, err, fake)
	}
	armed, err = manager.Arm(context.Background(), journal.TransactionID)
	if err != nil || armed.State != StateArmed || fake.installCalls != 2 || !fake.installed {
		t.Fatalf("repeated Arm() = %#v, %v, fake=%#v", armed, err, fake)
	}
}

func TestManagerArmResumesDefinitionInstalledBeforeJournalUpdate(t *testing.T) {
	transactionStore, journal := createActivationJournal(t)
	fake := &fakeOneShot{installed: true}
	manager := newTestManager(t, transactionStore, fake)

	armed, err := manager.Arm(context.Background(), journal.TransactionID)
	if err != nil || armed.State != StateArmed || fake.installCalls != 1 {
		t.Fatalf("Arm() = %#v, %v, fake=%#v", armed, err, fake)
	}
}

func TestManagerDurablyAuthorizesBeforeLaunchAndAcknowledges(t *testing.T) {
	transactionStore, journal := createActivationJournal(t)
	fake := &fakeOneShot{}
	manager := newTestManager(t, transactionStore, fake)
	if _, err := manager.Arm(context.Background(), journal.TransactionID); err != nil {
		t.Fatal(err)
	}
	fake.beforeLaunch = func() error {
		loaded, err := transactionStore.Load()
		if err != nil {
			return err
		}
		if loaded.State != StateActivating || !loaded.CommitAuthorized {
			return errors.New("activator launched before durable authorization")
		}
		return nil
	}

	launched, err := manager.AuthorizeAndLaunch(context.Background(), journal.TransactionID)
	if err != nil || launched.State != StateActivating || !launched.CommitAuthorized ||
		fake.launchCalls != 1 || !fake.launched {
		t.Fatalf("AuthorizeAndLaunch() = %#v, %v, fake=%#v", launched, err, fake)
	}
}

func TestManagerRetriesLaunchOnlyInForwardDirection(t *testing.T) {
	transactionStore, journal := createActivationJournal(t)
	fake := &fakeOneShot{launchFailures: 1}
	manager := newTestManager(t, transactionStore, fake)
	if _, err := manager.Arm(context.Background(), journal.TransactionID); err != nil {
		t.Fatal(err)
	}
	failed, err := manager.AuthorizeAndLaunch(context.Background(), journal.TransactionID)
	if err == nil || !failed.CommitAuthorized || failed.State != StateActivating {
		t.Fatalf("failed launch = %#v, %v", failed, err)
	}
	if _, err := manager.Cancel(context.Background(), journal.TransactionID); err == nil {
		t.Fatal("Cancel accepted a transaction after launch authorization")
	}
	launched, err := manager.AuthorizeAndLaunch(context.Background(), journal.TransactionID)
	if err != nil || !launched.CommitAuthorized || fake.launchCalls != 2 || !fake.launched {
		t.Fatalf("retry = %#v, %v, fake=%#v", launched, err, fake)
	}
}

func TestManagerRejectsChangedMaterialBeforeAuthorization(t *testing.T) {
	transactionStore, journal := createActivationJournal(t)
	fake := &fakeOneShot{}
	manager := newTestManager(t, transactionStore, fake)
	if _, err := manager.Arm(context.Background(), journal.TransactionID); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(journal.Invocation.ConfigPath, []byte("changed"), 0o600); err != nil {
		t.Fatal(err)
	}
	rejected, err := manager.AuthorizeAndLaunch(context.Background(), journal.TransactionID)
	if err == nil || rejected.CommitAuthorized || rejected.State != StateArmed || fake.launchCalls != 0 {
		t.Fatalf("AuthorizeAndLaunch() = %#v, %v, fake=%#v", rejected, err, fake)
	}
}

func TestManagerCancelCleansActivatorBeforeRollback(t *testing.T) {
	transactionStore, journal := createActivationJournal(t)
	fake := &fakeOneShot{}
	manager := newTestManager(t, transactionStore, fake)
	if _, err := manager.Arm(context.Background(), journal.TransactionID); err != nil {
		t.Fatal(err)
	}
	fake.beforeRemove = func() error {
		loaded, err := transactionStore.Load()
		if err != nil {
			return err
		}
		if loaded.State != StateArmed {
			return errors.New("journal rolled back before activator cleanup")
		}
		return nil
	}
	canceled, err := manager.Cancel(context.Background(), journal.TransactionID)
	if err != nil || canceled.State != StateRolledBack || fake.installed || fake.removeCalls != 1 {
		t.Fatalf("Cancel() = %#v, %v, fake=%#v", canceled, err, fake)
	}
}

func TestManagerCancelCleanupFailureLeavesTransactionCancelable(t *testing.T) {
	transactionStore, journal := createActivationJournal(t)
	fake := &fakeOneShot{removeFailures: 1}
	manager := newTestManager(t, transactionStore, fake)
	if _, err := manager.Arm(context.Background(), journal.TransactionID); err != nil {
		t.Fatal(err)
	}
	failed, err := manager.Cancel(context.Background(), journal.TransactionID)
	if err == nil || failed.State != StateArmed {
		t.Fatalf("failed cancel = %#v, %v", failed, err)
	}
	canceled, err := manager.Cancel(context.Background(), journal.TransactionID)
	if err != nil || canceled.State != StateRolledBack || fake.removeCalls != 2 {
		t.Fatalf("retry = %#v, %v, fake=%#v", canceled, err, fake)
	}
}

type fakeOneShot struct {
	installed      bool
	launched       bool
	installCalls   int
	launchCalls    int
	removeCalls    int
	launchFailures int
	removeFailures int
	beforeLaunch   func() error
	beforeRemove   func() error
}

func (f *fakeOneShot) operations() OneShotOperations {
	return OneShotOperations{
		Install: func(context.Context, Journal, string) error {
			f.installCalls++
			f.installed = true
			return nil
		},
		Launch: func(context.Context, Journal, string) error {
			f.launchCalls++
			if f.beforeLaunch != nil {
				if err := f.beforeLaunch(); err != nil {
					return err
				}
			}
			if f.launchFailures > 0 {
				f.launchFailures--
				return errors.New("injected launch acknowledgement loss")
			}
			f.launched = true
			return nil
		},
		Remove: func(context.Context, Journal, string) error {
			f.removeCalls++
			if f.beforeRemove != nil {
				if err := f.beforeRemove(); err != nil {
					return err
				}
			}
			if f.removeFailures > 0 {
				f.removeFailures--
				return errors.New("injected cleanup failure")
			}
			f.installed = false
			return nil
		},
	}
}

func newTestManager(t *testing.T, transactionStore *Store, fake *fakeOneShot) *Manager {
	t.Helper()
	manager, err := NewManager(transactionStore, fake.operations())
	if err != nil {
		t.Fatal(err)
	}
	return manager
}
