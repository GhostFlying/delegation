package codexconfig

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"unicode/utf8"
)

const (
	EnvironmentVariable = "DELEGATION_CODEX_CONFIG_JSON"
	maximumConfigBytes  = 16 * 1024
	maximumJSONDepth    = 32
)

var (
	providerIDPattern  = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_-]{0,63}$`)
	environmentPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,127}$`)
	requestKeys        = map[string]struct{}{
		"model_reasoning_effort":  {},
		"model_reasoning_summary": {},
		"model_verbosity":         {},
		"service_tier":            {},
	}
	providerKeys = map[string]struct{}{
		"name": {}, "base_url": {}, "wire_api": {}, "env_key": {},
		"env_http_headers": {}, "requires_openai_auth": {},
	}
	reservedProviderIDs = map[string]struct{}{
		"openai": {}, "amazon-bedrock": {}, "ollama": {}, "lmstudio": {},
	}
	reservedCredentialEnvironment = makeReservedCredentialEnvironment()
)

// Load parses the required non-secret custom provider configuration from the
// connector service environment.
func Load(lookup func(string) (string, bool)) (map[string]any, error) {
	if lookup == nil {
		return nil, errors.New("environment lookup is required")
	}
	raw, found := lookup(EnvironmentVariable)
	if !found || strings.TrimSpace(raw) == "" {
		return nil, fmt.Errorf("%s is required for managed workers", EnvironmentVariable)
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
	if err := validateConfig(config, lookup); err != nil {
		return nil, err
	}
	return config, nil
}

// CredentialEnvironmentVariables returns the provider credential variables
// referenced by an already validated override map.
func CredentialEnvironmentVariables(config map[string]any) []string {
	variables := make([]string, 0)
	for key, value := range config {
		if !strings.HasPrefix(key, "model_providers.") {
			continue
		}
		provider, ok := value.(map[string]any)
		if !ok {
			continue
		}
		if variable, ok := provider["env_key"].(string); ok && variable != "" {
			variables = append(variables, variable)
		}
		if headers, ok := provider["env_http_headers"].(map[string]any); ok {
			for _, value := range headers {
				if variable, ok := value.(string); ok && variable != "" {
					variables = append(variables, variable)
				}
			}
		}
	}
	return uniqueEnvironmentVariables(variables, runtime.GOOS == "windows")
}

func uniqueEnvironmentVariables(variables []string, caseInsensitive bool) []string {
	if caseInsensitive {
		slices.SortFunc(variables, func(left, right string) int {
			if comparison := strings.Compare(strings.ToUpper(left), strings.ToUpper(right)); comparison != 0 {
				return comparison
			}
			return strings.Compare(left, right)
		})
		return slices.CompactFunc(variables, strings.EqualFold)
	}
	slices.Sort(variables)
	return slices.Compact(variables)
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

func validateConfig(config map[string]any, lookup func(string) (string, bool)) error {
	if len(config) == 0 {
		return fmt.Errorf("%s must define a custom model provider", EnvironmentVariable)
	}
	if _, err := requiredBoundedString(config, "model"); err != nil {
		return err
	}
	providerID, err := requiredBoundedString(config, "model_provider")
	if err != nil {
		return err
	}
	if !providerIDPattern.MatchString(providerID) {
		return fmt.Errorf("%s model_provider is invalid", EnvironmentVariable)
	}
	if _, reserved := reservedProviderIDs[strings.ToLower(providerID)]; reserved {
		return fmt.Errorf("%s cannot select reserved provider %q", EnvironmentVariable, providerID)
	}

	providerKey := "model_providers." + providerID
	providerCount := 0
	for key, value := range config {
		switch {
		case key == "model", key == "model_provider":
		case key == providerKey:
			providerCount++
			provider, ok := value.(map[string]any)
			if !ok {
				return fmt.Errorf("%s %s must be an object", EnvironmentVariable, providerKey)
			}
			if err := validateProvider(provider, lookup); err != nil {
				return fmt.Errorf("%s %s: %w", EnvironmentVariable, providerKey, err)
			}
		case strings.HasPrefix(key, "model_providers."):
			return fmt.Errorf("%s may configure only selected provider %q", EnvironmentVariable, providerID)
		default:
			if _, allowed := requestKeys[key]; !allowed {
				return fmt.Errorf("%s cannot override key %q", EnvironmentVariable, key)
			}
			if _, err := boundedString(value, key); err != nil {
				return err
			}
		}
	}
	if providerCount != 1 {
		return fmt.Errorf("%s must define exactly one %s object", EnvironmentVariable, providerKey)
	}
	return nil
}

func validateProvider(provider map[string]any, lookup func(string) (string, bool)) error {
	for key := range provider {
		if _, allowed := providerKeys[key]; !allowed {
			return fmt.Errorf("field %q is not allowed", key)
		}
	}
	if _, err := requiredBoundedString(provider, "name"); err != nil {
		return err
	}
	baseURL, err := requiredBoundedString(provider, "base_url")
	if err != nil {
		return err
	}
	parsed, err := url.Parse(baseURL)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return errors.New("base_url must be an absolute http:// or https:// URL")
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return errors.New("base_url must not contain userinfo, query parameters, or a fragment")
	}
	if parsed.Scheme == "http" && !isLoopbackHost(parsed.Hostname()) {
		return errors.New("base_url must use https except for a literal loopback or localhost endpoint")
	}
	if wireAPI, found := provider["wire_api"]; found {
		value, err := boundedString(wireAPI, "wire_api")
		if err != nil || value != "responses" {
			return errors.New("wire_api must be responses")
		}
	}
	requiresAuth, found := provider["requires_openai_auth"]
	value, ok := requiresAuth.(bool)
	if !found || !ok || value {
		return errors.New("requires_openai_auth must be explicitly false")
	}

	envKey := ""
	if value, found := provider["env_key"]; found {
		envKey, err = credentialEnvironment(value, "env_key", lookup)
		if err != nil {
			return err
		}
	}
	if value, found := provider["env_http_headers"]; found {
		headers, ok := value.(map[string]any)
		if !ok || len(headers) == 0 {
			return errors.New("env_http_headers must be a non-empty object")
		}
		seenHeaders := make(map[string]struct{}, len(headers))
		for header, rawVariable := range headers {
			if !validHTTPHeaderName(header) {
				return fmt.Errorf("env_http_headers contains invalid header %q", header)
			}
			normalizedHeader := strings.ToLower(header)
			if _, duplicate := seenHeaders[normalizedHeader]; duplicate {
				return fmt.Errorf("env_http_headers contains duplicate header %q", header)
			}
			seenHeaders[normalizedHeader] = struct{}{}
			if envKey != "" && strings.EqualFold(header, "Authorization") {
				return errors.New("env_key and env_http_headers.Authorization are ambiguous")
			}
			if _, err := credentialEnvironment(rawVariable, "env_http_headers."+header, lookup); err != nil {
				return err
			}
		}
	}
	return nil
}

func requiredBoundedString(object map[string]any, key string) (string, error) {
	value, found := object[key]
	if !found {
		return "", fmt.Errorf("%s is required", key)
	}
	return boundedString(value, key)
}

func boundedString(value any, path string) (string, error) {
	text, ok := value.(string)
	if !ok || strings.TrimSpace(text) == "" || len(text) > 1024 || strings.ContainsRune(text, '\x00') {
		return "", fmt.Errorf("%s %s must be a bounded non-empty string", EnvironmentVariable, path)
	}
	return text, nil
}

func credentialEnvironment(value any, path string, lookup func(string) (string, bool)) (string, error) {
	variable, err := boundedString(value, path)
	if err != nil {
		return "", err
	}
	if !environmentPattern.MatchString(variable) {
		return "", fmt.Errorf("%s %s is not a valid environment variable name", EnvironmentVariable, path)
	}
	if _, reserved := reservedCredentialEnvironment[strings.ToUpper(variable)]; reserved {
		return "", fmt.Errorf("%s %s references reserved environment variable %q", EnvironmentVariable, path, variable)
	}
	if secret, found := lookup(variable); !found || secret == "" || strings.ContainsRune(secret, '\x00') {
		return "", fmt.Errorf("%s %s references missing or empty environment variable %q", EnvironmentVariable, path, variable)
	}
	return variable, nil
}

func validHTTPHeaderName(name string) bool {
	if name == "" || len(name) > 256 {
		return false
	}
	for index := range len(name) {
		character := name[index]
		if !strings.ContainsRune("!#$%&'*+-.^_`|~", rune(character)) &&
			(character < '0' || character > '9') &&
			(character < 'A' || character > 'Z') &&
			(character < 'a' || character > 'z') {
			return false
		}
	}
	return true
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	address := net.ParseIP(host)
	return address != nil && address.IsLoopback()
}

func makeReservedCredentialEnvironment() map[string]struct{} {
	reserved := []string{
		"PATH", "PATHEXT", "SHELL", "COMSPEC", "SYSTEMROOT", "SYSTEMDRIVE",
		"TMPDIR", "TEMP", "TMP", "HOME", "LANG", "LC_ALL", "LC_CTYPE", "LOGNAME", "USER",
		"USERNAME", "USERDOMAIN", "USERPROFILE", "HOMEDRIVE", "HOMEPATH",
		"PROGRAMFILES", "PROGRAMFILES(X86)", "PROGRAMW6432", "PROGRAMDATA",
		"LOCALAPPDATA", "APPDATA", "POWERSHELL", "PWSH", "CODEX_HOME", "CODEX_SQLITE_HOME",
		"CODEX_THREAD_ID", "CODEX_PERMISSION_PROFILE", "CODEX_ACCESS_TOKEN",
		"CODEX_MANAGED_PACKAGE_ROOT", "CODEX_MANAGED_BY_NPM", "CODEX_MANAGED_BY_PNPM",
		"CODEX_MANAGED_BY_BUN",
		"LD_PRELOAD", "LD_LIBRARY_PATH", "LD_AUDIT", "LD_DEBUG", "LD_PROFILE", "GLIBC_TUNABLES",
		"DYLD_INSERT_LIBRARIES", "DYLD_LIBRARY_PATH", "DYLD_FRAMEWORK_PATH",
		"DYLD_FALLBACK_LIBRARY_PATH", "DYLD_FALLBACK_FRAMEWORK_PATH", "DYLD_PRINT_TO_FILE",
		"DELEGATION_HOME", "DELEGATION_CONFIG",
		"DELEGATION_BINARY", EnvironmentVariable,
		"_DELEGATION_DARWIN_SUPERVISOR",
		"_DELEGATION_DARWIN_SUPERVISOR_TARGET",
		"_DELEGATION_DARWIN_SUPERVISOR_WATCHDOG_FD",
		"_DELEGATION_DARWIN_SUPERVISOR_TIMEOUT",
	}
	result := make(map[string]struct{}, len(reserved))
	for _, variable := range reserved {
		result[strings.ToUpper(variable)] = struct{}{}
	}
	return result
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
