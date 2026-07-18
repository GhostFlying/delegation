package credential

import (
	"encoding/hex"
	"testing"

	"github.com/GhostFlying/delegation/internal/tokenfile"
)

func TestMACIsDeterministicAndDomainBound(t *testing.T) {
	master := tokenfile.Token{1}
	deviceToken := tokenfile.Token{2}
	first := MAC(master, deviceToken)
	second := MAC(master, deviceToken)
	if first != second {
		t.Fatalf("MAC is not deterministic: %x != %x", first, second)
	}
	if first == MAC(tokenfile.Token{3}, deviceToken) {
		t.Fatal("MAC did not depend on the master token")
	}
	if first == MAC(master, tokenfile.Token{4}) {
		t.Fatal("MAC did not depend on the device token")
	}
}

func TestMACV1KnownAnswer(t *testing.T) {
	master := tokenfile.Token{1}
	deviceToken := tokenfile.Token{2}
	got := MAC(master, deviceToken)
	const want = "92bb0feedfc8d12cad87a66e7857f9510f223de688c1a842a091ddca4bf7c83b"
	if encoded := hex.EncodeToString(got[:]); encoded != want {
		t.Fatalf("MAC v1 = %s, want %s", encoded, want)
	}
}

func TestMasterTokenDoesNotAuthenticateAsUnissuedDeviceToken(t *testing.T) {
	master := tokenfile.Token{1}
	issued := tokenfile.Token{2}
	if MAC(master, master) == MAC(master, issued) {
		t.Fatal("master and issued device token produced the same MAC")
	}
}
