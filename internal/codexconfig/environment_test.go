package codexconfig

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestLoadParsesProviderOverridesWithoutSecrets(t *testing.T) {
	raw := `{
        "model": "gpt-5.2",
        "model_provider": "gateway",
        "model_providers.gateway": {
            "name": "Local gateway",
            "base_url": "https://gateway.example.test/v1",
            "wire_api": "responses",
            "env_key": "GATEWAY_API_KEY",
            "env_http_headers": {"X-Tenant": "GATEWAY_TENANT"},
            "requires_openai_auth": false
        },
        "model_reasoning_effort": "high"
    }`
	got, err := Load(func(name string) (string, bool) {
		if name != EnvironmentVariable {
			t.Fatalf("lookup name = %q", name)
		}
		return raw, true
	})
	if err != nil {
		t.Fatal(err)
	}
	provider := got["model_providers.gateway"].(map[string]any)
	if got["model"] != "gpt-5.2" || got["model_provider"] != "gateway" ||
		provider["env_key"] != "GATEWAY_API_KEY" || provider["requires_openai_auth"] != false {
		t.Fatalf("loaded config = %#v", got)
	}

	clone := Clone(got)
	cloneProvider := clone["model_providers.gateway"].(map[string]any)
	cloneProvider["name"] = "changed"
	if reflect.DeepEqual(clone, got) || provider["name"] != "Local gateway" {
		t.Fatalf("Clone aliased source: clone %#v, source %#v", clone, got)
	}
}

func TestLoadAllowsMissingConfiguration(t *testing.T) {
	for _, lookup := range []func(string) (string, bool){
		func(string) (string, bool) { return "", false },
		func(string) (string, bool) { return "  ", true },
	} {
		got, err := Load(lookup)
		if err != nil || len(got) != 0 {
			t.Fatalf("Load missing config = %#v, %v", got, err)
		}
	}
}

func TestLoadRejectsManagedAndSensitiveConfiguration(t *testing.T) {
	tests := map[string]string{
		"MCP root":              `{"mcp_servers.delegation":{"command":"other"}}`,
		"nested MCP root":       `{"mcp_servers":{"delegation":{"command":"other"}}}`,
		"plugins":               `{"features.plugins":true}`,
		"sandbox":               `{"sandbox_mode":"danger-full-access"}`,
		"shell environment":     `{"shell_environment_policy":{"inherit":"all"}}`,
		"embedded bearer token": `{"model_providers.gateway":{"experimental_bearer_token":"secret"}}`,
		"authorization header":  `{"model_providers.gateway":{"http_headers":{"Authorization":"secret"}}}`,
	}
	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := Load(func(string) (string, bool) { return raw, true })
			if err == nil {
				t.Fatal("Load accepted unsafe config")
			}
		})
	}
}

func TestLoadRejectsMalformedAmbiguousAndOversizedJSON(t *testing.T) {
	tests := map[string]string{
		"non-object":      `[]`,
		"duplicate":       `{"model":"one","model":"two"}`,
		"invalid key":     `{"model..name":"x"}`,
		"trailing":        `{} {}`,
		"invalid syntax":  `{"model":`,
		"invalid UTF-8":   string([]byte{'{', '"', 'x', '"', ':', '"', 0xff, '"', '}'}),
		"oversized":       `{"model":"` + strings.Repeat("x", maximumConfigBytes) + `"}`,
		"excessive depth": strings.Repeat(`{"x":`, maximumJSONDepth+2) + `0` + strings.Repeat(`}`, maximumJSONDepth+2),
	}
	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := Load(func(string) (string, bool) { return raw, true })
			if err == nil {
				encoded, _ := json.Marshal(raw)
				t.Fatalf("Load accepted invalid JSON %s", encoded)
			}
		})
	}
}
