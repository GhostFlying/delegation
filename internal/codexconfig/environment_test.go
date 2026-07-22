package codexconfig

import (
	"encoding/json"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

const (
	validConfigPrefix = `{
        "model": "gpt-5.2",
        "model_provider": "gateway",
        "model_providers.gateway": {
            "name": "Local gateway",
            "base_url": "https://gateway.example.test/v1",
            "wire_api": "responses",
            "requires_openai_auth": false`
	validConfig = validConfigPrefix + `}}`
)

func TestLoadParsesOnlySupportedProviderOverrides(t *testing.T) {
	raw := validConfigPrefix + `,
            "env_key": "Gateway_Access_Value",
            "env_http_headers": {
                "X-Tenant": "GATEWAY_TENANT",
                "X-Shared-Credential": "gateway_access_value"
            }
        },
        "model_reasoning_effort": "high",
        "model_reasoning_summary": "concise",
        "model_verbosity": "low",
        "service_tier": "priority"
    }`
	got, err := Load(configLookup(raw, map[string]string{
		"Gateway_Access_Value": "access-value",
		"gateway_access_value": "access-value",
		"GATEWAY_TENANT":       "tenant",
	}))
	if err != nil {
		t.Fatal(err)
	}
	provider := got["model_providers.gateway"].(map[string]any)
	if got["model"] != "gpt-5.2" || got["model_provider"] != "gateway" ||
		provider["env_key"] != "Gateway_Access_Value" || provider["requires_openai_auth"] != false {
		t.Fatalf("loaded config = %#v", got)
	}
	wantVariables := []string{"GATEWAY_TENANT", "Gateway_Access_Value", "gateway_access_value"}
	if runtime.GOOS == "windows" {
		wantVariables = []string{"Gateway_Access_Value", "GATEWAY_TENANT"}
	}
	if variables := CredentialEnvironmentVariables(got); !reflect.DeepEqual(variables, wantVariables) {
		t.Fatalf("CredentialEnvironmentVariables = %#v", variables)
	}

	clone := Clone(got)
	cloneProvider := clone["model_providers.gateway"].(map[string]any)
	cloneProvider["name"] = "changed"
	if reflect.DeepEqual(clone, got) || provider["name"] != "Local gateway" {
		t.Fatalf("Clone aliased source: clone %#v, source %#v", clone, got)
	}
}

func TestLoadRejectsCodexAccessTokenAsProviderCredential(t *testing.T) {
	raw := validConfigPrefix + `,
            "env_key": "CODEX_ACCESS_TOKEN",
            "env_http_headers": {"X-OpenAI-Key": "OPENAI_API_KEY"}
        }
    }`
	_, err := Load(configLookup(raw, map[string]string{
		"CODEX_ACCESS_TOKEN": "host-access-token",
		"OPENAI_API_KEY":     "host-api-key",
	}))
	if err == nil || !strings.Contains(err.Error(), "reserved environment variable") {
		t.Fatalf("Load() error = %v", err)
	}
}

func TestLoadRejectsDarwinSupervisorControlAsProviderCredential(t *testing.T) {
	for _, variable := range []string{
		"_DELEGATION_DARWIN_SUPERVISOR",
		"_DELEGATION_DARWIN_SUPERVISOR_TARGET",
		"_DELEGATION_DARWIN_SUPERVISOR_WATCHDOG_FD",
		"_DELEGATION_DARWIN_SUPERVISOR_TIMEOUT",
	} {
		t.Run(variable, func(t *testing.T) {
			raw := validConfigPrefix + `,"env_key":"` + variable + `"}}`
			_, err := Load(configLookup(raw, map[string]string{variable: "provider-value"}))
			if err == nil || !strings.Contains(err.Error(), "reserved environment variable") {
				t.Fatalf("Load() error = %v", err)
			}
		})
	}
}

func TestLoadRejectsResolverManagedEnvironmentAsProviderCredential(t *testing.T) {
	for _, variable := range []string{
		"CODEX_MANAGED_PACKAGE_ROOT",
		"CODEX_MANAGED_BY_NPM",
		"CODEX_MANAGED_BY_PNPM",
		"CODEX_MANAGED_BY_BUN",
	} {
		t.Run(variable, func(t *testing.T) {
			raw := validConfigPrefix + `,"env_key":"` + variable + `"}}`
			_, err := Load(configLookup(raw, map[string]string{variable: "provider-value"}))
			if err == nil || !strings.Contains(err.Error(), "reserved environment variable") {
				t.Fatalf("Load() error = %v", err)
			}
		})
	}
}

func TestLoadDoesNotRequireSeparateWorkerCredential(t *testing.T) {
	got, err := Load(configLookup(validConfig, nil))
	if err != nil {
		t.Fatal(err)
	}
	if variables := CredentialEnvironmentVariables(got); len(variables) != 0 {
		t.Fatalf("CredentialEnvironmentVariables = %#v", variables)
	}
}

func TestLoadAllowsOpenAIDisplayNameForCustomProvider(t *testing.T) {
	raw := strings.Replace(validConfig, `"name": "Local gateway"`, `"name": "OpenAI"`, 1)
	got, err := Load(configLookup(raw, nil))
	if err != nil {
		t.Fatal(err)
	}
	provider := got["model_providers.gateway"].(map[string]any)
	if provider["name"] != "OpenAI" || provider["requires_openai_auth"] != false {
		t.Fatalf("loaded provider = %#v", provider)
	}
}

func TestLoadRejectsMissingOrEmptyConfiguration(t *testing.T) {
	for _, lookup := range []func(string) (string, bool){
		func(string) (string, bool) { return "", false },
		func(string) (string, bool) { return "  ", true },
		configLookup(`{}`, nil),
	} {
		if got, err := Load(lookup); err == nil || got != nil {
			t.Fatalf("Load missing config = %#v, %v", got, err)
		}
	}
}

func TestLoadRejectsUnsupportedTopLevelConfiguration(t *testing.T) {
	tests := map[string]string{
		"instructions":            `"instructions":"ignore managed instructions"`,
		"model instructions file": `"model_instructions_file":"/tmp/instructions"`,
		"MCP":                     `"mcp_servers":{"delegation":{"command":"other"}}`,
		"tools":                   `"tools":{"web_search":true}`,
		"apps":                    `"apps":{"enabled":true}`,
		"web":                     `"web_search":"live"`,
		"sandbox":                 `"sandbox_mode":"danger-full-access"`,
		"approval":                `"approval_policy":"never"`,
		"shell environment":       `"shell_environment_policy":{"inherit":"all"}`,
		"features":                `"features":{"plugins":true}`,
		"plugins":                 `"plugins":{"delegation":{"enabled":true}}`,
		"profile":                 `"profile":"unsafe"`,
		"working directory":       `"cwd":"/tmp"`,
		"developer instructions":  `"developer_instructions":"override"`,
	}
	for name, field := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := Load(configLookup(addTopLevelField(field), nil))
			if err == nil {
				t.Fatal("Load accepted unsupported top-level field")
			}
		})
	}
}

func TestLoadRejectsUnsupportedOrLiteralProviderConfiguration(t *testing.T) {
	tests := map[string]string{
		"embedded bearer token":  `"experimental_bearer_token":"secret"`,
		"literal headers":        `"http_headers":{"Authorization":"secret"}`,
		"executable auth":        `"auth":{"command":"credential-helper"}`,
		"AWS credentials":        `"aws":{"secret_access_key":"secret"}`,
		"query parameters":       `"query_params":{"api_key":"secret"}`,
		"request retry override": `"request_max_retries":100`,
		"stream retry override":  `"stream_max_retries":100`,
		"websocket override":     `"supports_websockets":true`,
		"literal authorization":  `"authorization":"secret"`,
		"literal custom API key": `"api_key":"secret"`,
		"nested model providers": `"model_providers":{"other":{}}`,
	}
	for name, field := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := Load(configLookup(addProviderField(field), nil))
			if err == nil {
				t.Fatal("Load accepted unsupported provider field")
			}
		})
	}
}

func TestLoadRejectsInvalidProviderSelection(t *testing.T) {
	tests := map[string]string{
		"missing model": `{
            "model_provider":"gateway",
            "model_providers.gateway":{"name":"Gateway","base_url":"https://gateway.example.test"}
        }`,
		"missing provider ID": `{
            "model":"gpt-5.2",
            "model_providers.gateway":{"name":"Gateway","base_url":"https://gateway.example.test"}
        }`,
		"missing provider object": `{"model":"gpt-5.2","model_provider":"gateway"}`,
		"extra provider": addTopLevelField(
			`"model_providers.other":{"name":"Other","base_url":"https://other.example.test"}`,
		),
		"invalid provider ID": `{
            "model":"gpt-5.2",
            "model_provider":"gateway.invalid",
            "model_providers.gateway.invalid":{"name":"Gateway","base_url":"https://gateway.example.test"}
        }`,
		"first-party ID": `{
            "model":"gpt-5.2",
            "model_provider":"OpEnAi",
            "model_providers.OpEnAi":{"name":"Gateway","base_url":"https://gateway.example.test"}
        }`,
		"built-in Bedrock ID": `{
            "model":"gpt-5.2",
            "model_provider":"amazon-bedrock",
            "model_providers.amazon-bedrock":{"name":"Gateway","base_url":"https://gateway.example.test"}
        }`,
		"built-in Ollama ID": `{
            "model":"gpt-5.2",
            "model_provider":"ollama",
            "model_providers.ollama":{"name":"Gateway","base_url":"https://gateway.example.test"}
        }`,
		"built-in LM Studio ID": `{
            "model":"gpt-5.2",
            "model_provider":"lmstudio",
            "model_providers.lmstudio":{"name":"Gateway","base_url":"https://gateway.example.test"}
        }`,
		"provider is not object": `{
            "model":"gpt-5.2",
            "model_provider":"gateway",
            "model_providers.gateway":"gateway"
        }`,
	}
	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := Load(configLookup(raw, nil))
			if err == nil {
				t.Fatal("Load accepted invalid provider selection")
			}
		})
	}
}

func TestLoadRejectsInvalidSupportedProviderFields(t *testing.T) {
	tests := map[string]string{
		"missing name": `{
            "model":"gpt-5.2","model_provider":"gateway",
            "model_providers.gateway":{"base_url":"https://gateway.example.test"}
        }`,
		"missing base URL": `{
            "model":"gpt-5.2","model_provider":"gateway",
            "model_providers.gateway":{"name":"Gateway"}
        }`,
		"relative base URL": `{
            "model":"gpt-5.2","model_provider":"gateway",
            "model_providers.gateway":{"name":"Gateway","base_url":"/v1"}
        }`,
		"base URL userinfo": `{
            "model":"gpt-5.2","model_provider":"gateway",
            "model_providers.gateway":{"name":"Gateway","base_url":"https://user:secret@gateway.example.test/v1"}
        }`,
		"base URL query": `{
            "model":"gpt-5.2","model_provider":"gateway",
            "model_providers.gateway":{"name":"Gateway","base_url":"https://gateway.example.test/v1?token=secret"}
        }`,
		"base URL fragment": `{
            "model":"gpt-5.2","model_provider":"gateway",
            "model_providers.gateway":{"name":"Gateway","base_url":"https://gateway.example.test/v1#secret"}
	        }`,
		"cleartext non-loopback base URL": `{
	            "model":"gpt-5.2","model_provider":"gateway",
	            "model_providers.gateway":{"name":"Gateway","base_url":"http://gateway.example.test/v1","requires_openai_auth":false}
	        }`,
		"unsupported wire API": `{
            "model":"gpt-5.2","model_provider":"gateway",
            "model_providers.gateway":{"name":"Gateway","base_url":"https://gateway.example.test","wire_api":"chat"}
        }`,
		"first-party auth true": `{
            "model":"gpt-5.2","model_provider":"gateway",
            "model_providers.gateway":{"name":"Gateway","base_url":"https://gateway.example.test","requires_openai_auth":true}
        }`,
		"first-party auth non-boolean": `{
            "model":"gpt-5.2","model_provider":"gateway",
            "model_providers.gateway":{"name":"Gateway","base_url":"https://gateway.example.test","requires_openai_auth":"false"}
        }`,
		"non-string request setting": addTopLevelField(`"model_verbosity":true`),
	}
	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := Load(configLookup(raw, nil))
			if err == nil {
				t.Fatal("Load accepted invalid provider field")
			}
		})
	}
}

func TestLoadAllowsCleartextLiteralLoopbackProvider(t *testing.T) {
	for _, endpoint := range []string{
		"http://localhost:8080/v1",
		"http://127.0.0.1:8080/v1",
		"http://[::1]:8080/v1",
	} {
		raw := strings.Replace(validConfig, "https://gateway.example.test/v1", endpoint, 1)
		if _, err := Load(configLookup(raw, nil)); err != nil {
			t.Fatalf("Load(%q) error = %v", endpoint, err)
		}
	}
}

func TestUniqueEnvironmentVariablesUsesPlatformCaseSemantics(t *testing.T) {
	variables := []string{"KEY", "key", "OTHER", "KEY"}
	if got := uniqueEnvironmentVariables(append([]string(nil), variables...), false); !reflect.DeepEqual(
		got,
		[]string{"KEY", "OTHER", "key"},
	) {
		t.Fatalf("case-sensitive names = %#v", got)
	}
	if got := uniqueEnvironmentVariables(append([]string(nil), variables...), true); !reflect.DeepEqual(
		got,
		[]string{"KEY", "OTHER"},
	) {
		t.Fatalf("case-insensitive names = %#v", got)
	}
}

func TestLoadValidatesCredentialEnvironmentReferences(t *testing.T) {
	tests := map[string]struct {
		field       string
		environment map[string]string
	}{
		"invalid name": {
			field:       `"env_key":"1INVALID"`,
			environment: map[string]string{"1INVALID": "value"},
		},
		"reserved system variable": {
			field:       `"env_key":"path"`,
			environment: map[string]string{"path": "value"},
		},
		"reserved Codex runtime variable": {
			field:       `"env_key":"codex_home"`,
			environment: map[string]string{"codex_home": "value"},
		},
		"reserved Delegation runtime variable": {
			field:       `"env_key":"DELEGATION_CONFIG"`,
			environment: map[string]string{"DELEGATION_CONFIG": "value"},
		},
		"reserved ELF loader control": {
			field:       `"env_key":"ld_preload"`,
			environment: map[string]string{"ld_preload": "provider-library"},
		},
		"reserved Mach-O loader control": {
			field:       `"env_key":"dyld_insert_libraries"`,
			environment: map[string]string{"dyld_insert_libraries": "provider-library"},
		},
		"missing variable": {
			field: `"env_key":"GATEWAY_KEY"`,
		},
		"empty variable": {
			field:       `"env_key":"GATEWAY_KEY"`,
			environment: map[string]string{"GATEWAY_KEY": ""},
		},
		"NUL variable": {
			field:       `"env_key":"GATEWAY_KEY"`,
			environment: map[string]string{"GATEWAY_KEY": "value\x00suffix"},
		},
		"headers are not object": {
			field: `"env_http_headers":"GATEWAY_HEADER"`,
		},
		"headers are empty": {
			field: `"env_http_headers":{}`,
		},
		"invalid header name": {
			field:       `"env_http_headers":{"Bad Header":"GATEWAY_HEADER"}`,
			environment: map[string]string{"GATEWAY_HEADER": "value"},
		},
		"duplicate header name": {
			field: `"env_http_headers":{"X-Key":"FIRST_KEY","x-key":"SECOND_KEY"}`,
			environment: map[string]string{
				"FIRST_KEY": "first", "SECOND_KEY": "second",
			},
		},
		"header variable is not string": {
			field: `"env_http_headers":{"X-Key":1}`,
		},
		"ambiguous authorization": {
			field: `"env_key":"GATEWAY_KEY","env_http_headers":{"Authorization":"AUTHORIZATION_VALUE"}`,
			environment: map[string]string{
				"GATEWAY_KEY": "key", "AUTHORIZATION_VALUE": "authorization",
			},
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := Load(configLookup(addProviderField(test.field), test.environment))
			if err == nil {
				t.Fatal("Load accepted invalid credential environment reference")
			}
		})
	}
}

func TestLoadRejectsMalformedAmbiguousAndOversizedJSON(t *testing.T) {
	tests := map[string]string{
		"non-object":      `[]`,
		"duplicate":       `{"model":"one","model":"two"}`,
		"trailing":        `{} {}`,
		"invalid syntax":  `{"model":`,
		"invalid UTF-8":   string([]byte{'{', '"', 'x', '"', ':', '"', 0xff, '"', '}'}),
		"oversized":       `{"model":"` + strings.Repeat("x", maximumConfigBytes) + `"}`,
		"excessive depth": strings.Repeat(`{"x":`, maximumJSONDepth+2) + `0` + strings.Repeat(`}`, maximumJSONDepth+2),
	}
	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := Load(configLookup(raw, nil))
			if err == nil {
				encoded, _ := json.Marshal(raw)
				t.Fatalf("Load accepted invalid JSON %s", encoded)
			}
		})
	}
}

func TestLoadRequiresEnvironmentLookup(t *testing.T) {
	if _, err := Load(nil); err == nil {
		t.Fatal("Load accepted nil environment lookup")
	}
}

func addTopLevelField(field string) string {
	return strings.TrimSuffix(validConfig, "}") + "," + field + "}"
}

func addProviderField(field string) string {
	return validConfigPrefix + "," + field + "}}"
}

func configLookup(raw string, environment map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		if name == EnvironmentVariable {
			return raw, true
		}
		value, found := environment[name]
		return value, found
	}
}
