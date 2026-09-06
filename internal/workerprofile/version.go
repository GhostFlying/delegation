package workerprofile

// CurrentVersion identifies the managed-worker permission semantics emitted by
// this runtime. Stored workers are resumed only after their profile history has
// been explicitly qualified for this version.
const CurrentVersion = 7

// CanMigrateSource reports the exact persisted profile generations whose
// inactive worker history has semantics understood by this runtime. A future
// target may advertise a higher profile during PREPARE, but its activator must
// still make the final CanUpgrade check with that target runtime's own table.
func CanMigrateSource(source int) bool {
	switch source {
	case 5, 6, CurrentVersion:
		return true
	default:
		return false
	}
}

// CanUpgrade reports the exact historical profile transitions supported by
// this runtime's stopped-shadow database migration. Versions 5 and 6 are the
// profiles published by alpha.4 and alpha.7 respectively.
func CanUpgrade(source, target int) bool {
	if target != CurrentVersion {
		return false
	}
	return CanMigrateSource(source)
}
