package workerreadiness

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	delegationconfig "github.com/GhostFlying/delegation/internal/config"
)

const maximumConfigMaterialBytes = 1 << 20

// ConfigMaterial is one path-bound input to an execution-readiness digest.
// Data may be supplied by a caller that already held and validated a protected
// source, so the digest describes the exact bytes used to start the worker.
type ConfigMaterial struct {
	Path string
	Data []byte
}

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
	materials, err := ReadConfigMaterials(paths...)
	if err != nil {
		return "", err
	}
	return ConfigDigestMaterials(materials...)
}

// ReadConfigMaterials reads protected, bounded configuration inputs. Empty
// paths are ignored so broker and peer callers can share the same call shape.
func ReadConfigMaterials(paths ...string) ([]ConfigMaterial, error) {
	materials := make([]ConfigMaterial, 0, len(paths))
	for _, path := range paths {
		if path == "" {
			continue
		}
		data, err := delegationconfig.ReadProtectedFile(path, maximumConfigMaterialBytes)
		if err != nil {
			return nil, fmt.Errorf("read readiness configuration %s: %w", path, err)
		}
		materials = append(materials, ConfigMaterial{Path: path, Data: data})
	}
	return materials, nil
}

// ConfigDigestMaterials hashes both each canonical source path and its exact
// bytes. This makes moving a protected credential source or rotating its
// contents begin a new readiness epoch.
func ConfigDigestMaterials(materials ...ConfigMaterial) (string, error) {
	digest := sha256.New()
	for _, material := range materials {
		if material.Path == "" || !filepath.IsAbs(material.Path) ||
			filepath.Clean(material.Path) != material.Path {
			return "", errors.New("readiness configuration path must be non-empty, absolute, and clean")
		}
		if len(material.Data) > maximumConfigMaterialBytes {
			return "", fmt.Errorf("readiness configuration %s exceeds %d-byte limit",
				material.Path, maximumConfigMaterialBytes)
		}
		if _, err := fmt.Fprintf(digest, "%d:%s\x00", len(material.Path), material.Path); err != nil {
			return "", err
		}
		if _, err := fmt.Fprintf(digest, "%d:", len(material.Data)); err != nil {
			return "", err
		}
		if _, err := digest.Write(material.Data); err != nil {
			return "", err
		}
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}
