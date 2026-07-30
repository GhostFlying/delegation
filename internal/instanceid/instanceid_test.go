package instanceid

import "testing"

func TestValidate(t *testing.T) {
	for _, value := range []string{"default", "codex-2", "a", "a123", "traex-main"} {
		if err := Validate(value); err != nil {
			t.Fatalf("Validate(%q) error = %v", value, err)
		}
	}
	for _, value := range []string{"", "A", "2codex", "codex_", "codex-", "codex.main", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"} {
		if err := Validate(value); err == nil {
			t.Fatalf("Validate(%q) succeeded", value)
		}
	}
}
