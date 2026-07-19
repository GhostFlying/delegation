package config

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

func randomConfigSuffix() (string, error) {
	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		return "", fmt.Errorf("generate temporary config name: %w", err)
	}
	return hex.EncodeToString(random), nil
}
