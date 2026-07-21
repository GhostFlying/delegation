package codexconfig

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"unicode/utf8"
)

const (
	EnvironmentVariable = "DELEGATION_CODEX_CONFIG_JSON"
	maximumConfigBytes  = 64 * 1024
	maximumJSONDepth    = 32
)

var (
	configKeyPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_.-]{0,127}$`)
	managedRoots     = map[string]struct{}{
		"agents":                         {},
		"allow_login_shell":              {},
		"analytics":                      {},
		"approval_policy":                {},
		"approvals_reviewer":             {},
		"base_instructions":              {},
		"cwd":                            {},
		"developer_instructions":         {},
		"features":                       {},
		"hooks":                          {},
		"marketplaces":                   {},
		"mcp_servers":                    {},
		"memories":                       {},
		"notify":                         {},
		"permissions":                    {},
		"plugins":                        {},
		"project_doc_fallback_filenames": {},
		"project_doc_max_bytes":          {},
		"sandbox_mode":                   {},
		"shell_environment_policy":       {},
		"skills":                         {},
		"telemetry":                      {},
		"tui":                            {},
		"windows":                        {},
	}
	sensitiveKeys = map[string]struct{}{
		"api_key":                   {},
		"access_token":              {},
		"authorization":             {},
		"experimental_bearer_token": {},
		"password":                  {},
		"proxy_password":            {},
		"secret":                    {},
		"token":                     {},
	}
)

// Load parses non-secret Codex overrides from the connector service
// environment. Missing configuration is valid and leaves Codex defaults in
// effect.
func Load(lookup func(string) (string, bool)) (map[string]any, error) {
	if lookup == nil {
		return nil, errors.New("environment lookup is required")
	}
	raw, found := lookup(EnvironmentVariable)
	if !found || strings.TrimSpace(raw) == "" {
		return map[string]any{}, nil
	}
	if len(raw) > maximumConfigBytes {
		return nil, fmt.Errorf("%s exceeds %d bytes", EnvironmentVariable, maximumConfigBytes)
	}
	if !utf8.ValidString(raw) {
		return nil, fmt.Errorf("%s must contain valid UTF-8 JSON", EnvironmentVariable)
	}
	decoder := json.NewDecoder(bytes.NewBufferString(raw))
	decoder.UseNumber()
	value, err := decodeValue(decoder, 0)
	if err != nil {
		return nil, fmt.Errorf("decode %s: %w", EnvironmentVariable, err)
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, fmt.Errorf("%s must contain exactly one JSON value", EnvironmentVariable)
		}
		return nil, fmt.Errorf("decode trailing %s data: %w", EnvironmentVariable, err)
	}
	config, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s must be a JSON object", EnvironmentVariable)
	}
	for key, entry := range config {
		if !configKeyPattern.MatchString(key) || strings.Contains(key, "..") || strings.HasSuffix(key, ".") {
			return nil, fmt.Errorf("%s contains invalid override key %q", EnvironmentVariable, key)
		}
		root, _, _ := strings.Cut(key, ".")
		if _, managed := managedRoots[root]; managed {
			return nil, fmt.Errorf("%s cannot override connector-managed key %q", EnvironmentVariable, root)
		}
		if err := rejectSensitiveValues(entry, key); err != nil {
			return nil, err
		}
	}
	return config, nil
}

func Clone(config map[string]any) map[string]any {
	if config == nil {
		return map[string]any{}
	}
	return cloneObject(config)
}

func decodeValue(decoder *json.Decoder, depth int) (any, error) {
	if depth > maximumJSONDepth {
		return nil, fmt.Errorf("JSON nesting exceeds %d levels", maximumJSONDepth)
	}
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	delimiter, isDelimiter := token.(json.Delim)
	if !isDelimiter {
		return token, nil
	}
	switch delimiter {
	case '{':
		object := make(map[string]any)
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return nil, err
			}
			key, ok := keyToken.(string)
			if !ok {
				return nil, errors.New("JSON object key must be a string")
			}
			if _, duplicate := object[key]; duplicate {
				return nil, fmt.Errorf("duplicate JSON key %q", key)
			}
			value, err := decodeValue(decoder, depth+1)
			if err != nil {
				return nil, err
			}
			object[key] = value
		}
		if token, err := decoder.Token(); err != nil || token != json.Delim('}') {
			return nil, errors.New("JSON object is not terminated")
		}
		return object, nil
	case '[':
		array := make([]any, 0)
		for decoder.More() {
			value, err := decodeValue(decoder, depth+1)
			if err != nil {
				return nil, err
			}
			array = append(array, value)
		}
		if token, err := decoder.Token(); err != nil || token != json.Delim(']') {
			return nil, errors.New("JSON array is not terminated")
		}
		return array, nil
	default:
		return nil, fmt.Errorf("unexpected JSON delimiter %q", delimiter)
	}
}

func rejectSensitiveValues(value any, path string) error {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			if _, sensitive := sensitiveKeys[strings.ToLower(key)]; sensitive {
				return fmt.Errorf(
					"%s embeds sensitive field %q; reference a separate credential environment variable instead",
					EnvironmentVariable,
					path+"."+key,
				)
			}
			if err := rejectSensitiveValues(child, path+"."+key); err != nil {
				return err
			}
		}
	case []any:
		for index, child := range typed {
			if err := rejectSensitiveValues(child, fmt.Sprintf("%s[%d]", path, index)); err != nil {
				return err
			}
		}
	case string:
		if strings.ContainsRune(typed, '\x00') {
			return fmt.Errorf("%s contains a NUL string at %q", EnvironmentVariable, path)
		}
	}
	return nil
}

func cloneObject(source map[string]any) map[string]any {
	result := make(map[string]any, len(source))
	for key, value := range source {
		result[key] = cloneValue(value)
	}
	return result
}

func cloneValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return cloneObject(typed)
	case []any:
		result := make([]any, len(typed))
		for index, entry := range typed {
			result[index] = cloneValue(entry)
		}
		return result
	default:
		return typed
	}
}
