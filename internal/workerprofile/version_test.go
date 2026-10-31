package workerprofile

import "testing"

func TestStoppedShadowProfileMigrationTable(t *testing.T) {
	for _, source := range []int{5, 6, 7, CurrentVersion} {
		if !CanMigrateSource(source) || !CanUpgrade(source, CurrentVersion) {
			t.Fatalf("supported profile %d cannot migrate to %d", source, CurrentVersion)
		}
	}
	for _, source := range []int{0, 4, CurrentVersion + 1} {
		if CanMigrateSource(source) || CanUpgrade(source, CurrentVersion) {
			t.Fatalf("unsupported source profile %d accepted", source)
		}
	}
	for _, target := range []int{7, CurrentVersion + 1} {
		if CanUpgrade(7, target) {
			t.Fatalf("migration to a different runtime profile %d accepted", target)
		}
	}
}
