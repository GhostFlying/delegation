package rootmcp

import (
	"encoding/base64"
	"testing"
)

func TestDeviceCursorRoundTripAndValidation(t *testing.T) {
	encoded, err := encodeCursor(42, rootMCPDeviceID)
	if err != nil {
		t.Fatal(err)
	}
	deviceID, revision, err := decodeCursor(encoded)
	if err != nil || deviceID != rootMCPDeviceID || revision == nil || *revision != 42 {
		t.Fatalf("decoded cursor = %q, %#v, error %v", deviceID, revision, err)
	}
	for _, invalid := range []string{
		"invalid!",
		base64.RawURLEncoding.EncodeToString([]byte(`{"revision":0,"deviceId":"` + rootMCPDeviceID + `"}`)),
		base64.RawURLEncoding.EncodeToString([]byte(`{"revision":1,"deviceId":"` + rootMCPDeviceID + `","extra":true}`)),
	} {
		if _, _, err := decodeCursor(invalid); err == nil {
			t.Fatalf("decodeCursor accepted %q", invalid)
		}
	}
}

func TestAgentCursorIsBoundToOneTree(t *testing.T) {
	encoded, err := encodeAgentCursor(rootMCPTreeID, 32)
	if err != nil {
		t.Fatal(err)
	}
	sequence, err := decodeAgentCursor(encoded, rootMCPTreeID)
	if err != nil || sequence != 32 {
		t.Fatalf("decoded agent cursor = %d, error %v", sequence, err)
	}
	if _, err := decodeAgentCursor(encoded, rootMCPThreadID); err == nil {
		t.Fatal("agent cursor was accepted for another tree")
	}
	invalid := base64.RawURLEncoding.EncodeToString([]byte(
		`{"treeId":"` + rootMCPTreeID + `","afterSequence":0}`,
	))
	if _, err := decodeAgentCursor(invalid, rootMCPTreeID); err == nil {
		t.Fatal("agent cursor accepted a zero sequence")
	}
}
