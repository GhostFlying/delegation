package workerreadiness

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
)

func RuntimeDigest(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open runtime executable for readiness digest: %w", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return "", fmt.Errorf("inspect runtime executable for readiness digest: %w", err)
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("runtime executable for readiness digest must be a regular file")
	}
	digest := sha256.New()
	if _, err := io.Copy(digest, file); err != nil {
		return "", fmt.Errorf("hash runtime executable for readiness: %w", err)
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

func ConfigDigest(paths ...string) (string, error) {
	digest := sha256.New()
	for _, path := range paths {
		if path == "" {
			continue
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return "", fmt.Errorf("read readiness configuration %s: %w", path, err)
		}
		if _, err := fmt.Fprintf(digest, "%d:%s\x00", len(path), path); err != nil {
			return "", err
		}
		if _, err := fmt.Fprintf(digest, "%d:", len(data)); err != nil {
			return "", err
		}
		if _, err := digest.Write(data); err != nil {
			return "", err
		}
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}
