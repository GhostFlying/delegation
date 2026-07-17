package buildinfo

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestVersionMatchesPluginMetadata(t *testing.T) {
	pluginRoot := filepath.Join("..", "..", "plugins", "delegation")

	versionFile, err := os.ReadFile(filepath.Join(pluginRoot, "VERSION"))
	if err != nil {
		t.Fatal(err)
	}

	manifestFile, err := os.ReadFile(filepath.Join(pluginRoot, ".codex-plugin", "plugin.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		Version string `json:"version"`
	}
	if err := json.Unmarshal(manifestFile, &manifest); err != nil {
		t.Fatal(err)
	}

	versions := []string{Version, strings.TrimSpace(string(versionFile)), manifest.Version}
	for _, version := range versions[1:] {
		if version != versions[0] {
			t.Fatalf("runtime, VERSION, and plugin manifest versions differ: %q", versions)
		}
	}
}
