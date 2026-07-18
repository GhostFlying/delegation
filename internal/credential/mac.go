package credential

import (
	"crypto/hmac"
	"crypto/sha256"

	"github.com/GhostFlying/delegation/internal/store"
	"github.com/GhostFlying/delegation/internal/tokenfile"
)

const macDomain = "delegation/device-credential-mac/v1\x00"

func MAC(master, deviceToken tokenfile.Token) store.CredentialMAC {
	digest := hmac.New(sha256.New, master[:])
	_, _ = digest.Write([]byte(macDomain))
	_, _ = digest.Write(deviceToken[:])
	var result store.CredentialMAC
	copy(result[:], digest.Sum(nil))
	return result
}
