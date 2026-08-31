package protocol

import "testing"

const (
	testUpgradeControllerTransactionID = "123e4567-e89b-42d3-a456-426614174901"
	testUpgradeTransactionID           = "123e4567-e89b-42d3-a456-426614174902"
)

func TestUpgradePayloadValidation(t *testing.T) {
	prepare := PrepareUpgradeParams{
		ControllerTransactionID: testUpgradeControllerTransactionID,
		TargetVersion:           "0.2.0",
	}
	if err := prepare.Validate(); err != nil {
		t.Fatal(err)
	}
	transaction := UpgradeTransactionParams{
		ControllerTransactionID: testUpgradeControllerTransactionID,
		TransactionID:           testUpgradeTransactionID,
	}
	if err := transaction.Validate(); err != nil {
		t.Fatal(err)
	}
	invalidPrepare := prepare
	invalidPrepare.TargetVersion = "latest"
	if err := invalidPrepare.Validate(); err == nil {
		t.Fatal("upgrade prepare accepted a non-canonical target version")
	}
	invalidTransaction := transaction
	invalidTransaction.ControllerTransactionID = "invalid"
	if err := invalidTransaction.Validate(); err == nil {
		t.Fatal("upgrade transaction accepted an invalid controller ID")
	}
}

func TestUpgradeSnapshotValidation(t *testing.T) {
	snapshot := UpgradeSnapshot{
		ControllerTransactionID: testUpgradeControllerTransactionID,
		TransactionID:           testUpgradeTransactionID,
		State:                   "prepared",
		SourceVersion:           "0.1.0",
		TargetVersion:           "0.2.0",
		TargetRuntimeDigest:     string(make([]byte, 64)),
		ConfigDigest:            string(make([]byte, 64)),
		UpdatedAt:               1,
	}
	for index := range 64 {
		snapshot.TargetRuntimeDigest = snapshot.TargetRuntimeDigest[:index] + "a" + snapshot.TargetRuntimeDigest[index+1:]
		snapshot.ConfigDigest = snapshot.ConfigDigest[:index] + "b" + snapshot.ConfigDigest[index+1:]
	}
	if err := snapshot.Validate(); err != nil {
		t.Fatal(err)
	}
	invalid := snapshot
	invalid.TargetVersion = invalid.SourceVersion
	if err := invalid.Validate(); err == nil {
		t.Fatal("upgrade snapshot accepted a non-forward version")
	}
	invalid = snapshot
	invalid.TargetRuntimeDigest = "AA" + invalid.TargetRuntimeDigest[2:]
	if err := invalid.Validate(); err == nil {
		t.Fatal("upgrade snapshot accepted a non-canonical digest")
	}
}
