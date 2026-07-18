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
