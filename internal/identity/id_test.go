package identity

import "testing"

func TestNewIDReturnsValidDistinctUUIDs(t *testing.T) {
	first, err := NewID()
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewID()
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatalf("NewID returned duplicate values %q", first)
	}
	for _, value := range []string{first, second} {
		if err := ValidateID(value); err != nil {
			t.Fatalf("ValidateID(%q): %v", value, err)
		}
		if value[14] != '4' {
			t.Fatalf("NewID() = %q, want UUID version 4", value)
		}
	}
}

func TestValidateIDRejectsMalformedValues(t *testing.T) {
	for _, value := range []string{
		"",
		"123e4567-e89b-42d3-a456-42661417400",
		"123e4567_e89b-42d3-a456-426614174000",
		"123e4567-e89b-42d3-a456-42661417400z",
		"123E4567-E89B-42D3-A456-426614174000",
	} {
		if err := ValidateID(value); err == nil {
			t.Fatalf("ValidateID(%q) succeeded", value)
		}
	}
}
