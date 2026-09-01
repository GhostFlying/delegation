package cli

import (
	"errors"
	"os"
	"testing"

	"github.com/GhostFlying/delegation/internal/localupgrade"
	"github.com/GhostFlying/delegation/internal/protocol"
)

func TestValidateCoordinatedCancellationDistinguishesAbsentReservedTransaction(t *testing.T) {
	params := protocol.UpgradeTransactionParams{
		ControllerTransactionID: "123e4567-e89b-42d3-a456-426614174891",
		TransactionID:           "123e4567-e89b-42d3-a456-426614174892",
	}
	previous := localupgrade.Journal{
		TransactionID: "123e4567-e89b-42d3-a456-426614174893",
		State:         localupgrade.StateCommitted,
	}
	if err := validateCoordinatedCancellation(previous, params); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("terminal mismatch error = %v, want not found", err)
	}
	previous.State = localupgrade.StatePrepared
	if err := validateCoordinatedCancellation(previous, params); err == nil || errors.Is(err, os.ErrNotExist) {
		t.Fatalf("active mismatch error = %v, want fail-closed conflict", err)
	}
}

func TestValidateCoordinatedCancellationBindsOnlyMatchingReservedTransaction(t *testing.T) {
	params := protocol.UpgradeTransactionParams{
		ControllerTransactionID: "123e4567-e89b-42d3-a456-426614174891",
		TransactionID:           "123e4567-e89b-42d3-a456-426614174892",
	}
	journal := localupgrade.Journal{TransactionID: params.TransactionID, State: localupgrade.StatePrepared}
	if err := validateCoordinatedCancellation(journal, params); err != nil {
		t.Fatalf("unbound matching transaction error = %v", err)
	}
	journal.ControllerTransactionID = "123e4567-e89b-42d3-a456-426614174894"
	if err := validateCoordinatedCancellation(journal, params); err == nil {
		t.Fatal("cancellation accepted another controller transaction")
	}
}
