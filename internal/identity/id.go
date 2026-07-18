package identity

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strings"
)

// NewID returns a random RFC 4122 version 4 UUID.
func NewID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	value[6] = value[6]&0x0f | 0x40
	value[8] = value[8]&0x3f | 0x80
	encoded := make([]byte, 32)
	hex.Encode(encoded, value)
	return string(encoded[0:8]) + "-" + string(encoded[8:12]) + "-" + string(encoded[12:16]) + "-" + string(encoded[16:20]) + "-" + string(encoded[20:32]), nil
}

func ValidateID(value string) error {
	if len(value) != 36 {
		return errors.New("must be a UUID")
	}
	for index, char := range value {
		if index == 8 || index == 13 || index == 18 || index == 23 {
			if char != '-' {
				return errors.New("must be a UUID")
			}
			continue
		}
		if !strings.ContainsRune("0123456789abcdefABCDEF", char) {
			return errors.New("must be a UUID")
		}
	}
	return nil
}
