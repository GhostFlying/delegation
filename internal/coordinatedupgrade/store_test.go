package coordinatedupgrade

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestStoreFreezesParticipantsAndCommitIsIrreversible(t *testing.T) {
	transactionStore := testStore(t)
	journal := testControllerJournal()
	created, resumed, err := transactionStore.CreateOrResume(journal)
	if err != nil || resumed {
		t.Fatalf("CreateOrResume() = %#v, %v, %v", created, resumed, err)
	}
	if _, err := transactionStore.Update(journal.TransactionID, func(current *Journal) error {
		current.Participants[0].ConnectionID = testReplacementConnectionID
		return nil
	}); err == nil {
		t.Fatal("Update accepted a changed participant connection generation")
	}
	for _, mutation := range []func(*Journal){
		func(current *Journal) {
			current.Participants[0].LocalTransactionID = testPeerTransactionID
			current.Participants[0].TargetRuntimeDigest = testPeerRuntimeDigest
			current.Participants[0].ConfigDigest = testPeerConfigDigest
			current.Participants[0].SourceReadinessEpoch = 4
			current.Participants[0].State = ParticipantPrepared
		},
		func(current *Journal) {
			current.Broker = LocalParticipant{
				TransactionID: testBrokerTransactionID, State: "prepared",
				TargetRuntimeDigest: testBrokerRuntimeDigest, ConfigDigest: testBrokerConfigDigest,
				UpdatedAt: current.UpdatedAt + 1,
			}
			current.State = StateArming
		},
		func(current *Journal) {
			current.Participants[0].State = ParticipantArmed
			current.Broker.State = "armed"
			current.State = StateArmed
		},
		func(current *Journal) {
			current.GlobalCommit = true
			current.State = StateCommitted
		},
	} {
		if _, err := transactionStore.Update(journal.TransactionID, func(current *Journal) error {
			mutation(current)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := transactionStore.Update(journal.TransactionID, func(current *Journal) error {
		current.GlobalCommit = false
		current.State = StateArmed
		return nil
	}); err == nil {
		t.Fatal("Update accepted clearing the global COMMIT decision")
	}
	if _, err := transactionStore.Update(journal.TransactionID, func(current *Journal) error {
		current.State = StateCanceling
		return nil
	}); err == nil {
		t.Fatal("Update accepted cancellation after global COMMIT")
	}
}

func TestStoreSameTargetResumesWithoutReplacingTransaction(t *testing.T) {
	transactionStore := testStore(t)
	journal := testControllerJournal()
	if _, _, err := transactionStore.CreateOrResume(journal); err != nil {
		t.Fatal(err)
	}
	retry := journal
	retry.TransactionID = testReplacementConnectionID
	resumed, didResume, err := transactionStore.CreateOrResume(retry)
	if err != nil || !didResume || resumed.TransactionID != journal.TransactionID {
		t.Fatalf("same target resume = %#v, %v, %v", resumed, didResume, err)
	}
	conflict := retry
	conflict.TargetVersion = "0.3.0"
	if _, _, err := transactionStore.CreateOrResume(conflict); !errors.Is(err, ErrTargetConflict) {
		t.Fatalf("conflicting active target error = %v", err)
	}
}

func TestStoreRejectsUnknownOrInsecureJournal(t *testing.T) {
	transactionStore := testStore(t)
	journal := testControllerJournal()
	if _, _, err := transactionStore.CreateOrResume(journal); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(transactionStore.path, journalName)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data = append([]byte(`{"unknown":true,`), data[1:]...)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := transactionStore.Load(); err == nil {
		t.Fatal("Load accepted an unknown journal field")
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := transactionStore.Load(); err == nil {
		t.Fatal("Load accepted an insecure journal mode")
	}
}

func testStore(t *testing.T) *Store {
	t.Helper()
	transactionStore, err := OpenStore(filepath.Join(t.TempDir(), "controller"))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_800_000_000, 0)
	transactionStore.now = func() time.Time { return now }
	return transactionStore
}

func testControllerJournal() Journal {
	now := time.Unix(1_800_000_000, 0).UnixMilli()
	return Journal{
		SchemaVersion: JournalSchemaVersion, TransactionID: testControllerTransactionID,
		State: StatePreparing, SourceVersion: testSourceVersion, TargetVersion: testTargetVersion,
		Deadline: now + int64(time.Hour/time.Millisecond), CreatedAt: now, UpdatedAt: now,
		Participants: []Participant{{
			DeviceID: testPeerDeviceID, ConnectionID: testPeerConnectionID, SourceVersion: testSourceVersion,
			State: ParticipantPending, UpdatedAt: now,
		}},
	}
}
