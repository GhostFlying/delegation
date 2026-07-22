package serviceenv

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"regexp"
	"runtime"
	"strings"
	"unicode/utf8"

	"github.com/GhostFlying/delegation/internal/codexconfig"
	delegationconfig "github.com/GhostFlying/delegation/internal/config"
)

const (
	maximumFileBytes = 64 * 1024
	maximumEntries   = 64
	maximumLineBytes = 32 * 1024
)

var environmentNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,127}$`)

type Resolved struct {
	Config      map[string]any
	Environment map[string]string
}

func LoadInherited() (Resolved, error) {
	config, err := codexconfig.Load(os.LookupEnv)
	if err != nil {
		return Resolved{}, err
	}
	return resolve(config, os.LookupEnv, nil)
}

func LoadProtectedFile(path string) (Resolved, error) {
	data, err := delegationconfig.ReadProtectedFile(path, maximumFileBytes)
	if err != nil {
		return Resolved{}, fmt.Errorf("read peer service environment: %w", err)
	}
	environment, err := parse(data)
	if err != nil {
		return Resolved{}, err
	}
	lookup := mapLookup(environment)
	config, err := codexconfig.Load(lookup)
	if err != nil {
		return Resolved{}, err
	}
	return resolve(config, lookup, environment)
}

func resolve(
	config map[string]any,
	lookup func(string) (string, bool),
	provided map[string]string,
) (Resolved, error) {
	credentials := codexconfig.CredentialEnvironmentVariables(config)
	allowed := append([]string{codexconfig.EnvironmentVariable}, credentials...)
	if provided != nil {
		for name := range provided {
			if !containsName(allowed, name) {
				return Resolved{}, fmt.Errorf("peer service environment contains unreferenced variable %q", name)
			}
		}
	}
	environment := make(map[string]string, len(credentials))
	for _, name := range credentials {
		value, found := lookup(name)
		if !found || value == "" {
			return Resolved{}, fmt.Errorf("peer service environment variable %s is missing or empty", name)
		}
		environment[name] = value
	}
	return Resolved{Config: codexconfig.Clone(config), Environment: environment}, nil
}

func parse(data []byte) (map[string]string, error) {
	if !utf8.Valid(data) {
		return nil, errors.New("peer service environment must contain valid UTF-8")
	}
	if bytes.IndexByte(data, 0) >= 0 {
		return nil, errors.New("peer service environment must not contain NUL")
	}
	environment := make(map[string]string)
	normalizedNames := make(map[string]string)
	for index, rawLine := range bytes.Split(data, []byte{'\n'}) {
		lineNumber := index + 1
		if len(rawLine) > maximumLineBytes {
			return nil, fmt.Errorf("peer service environment line %d exceeds %d bytes", lineNumber, maximumLineBytes)
		}
		if len(rawLine) > 0 && rawLine[len(rawLine)-1] == '\r' {
			rawLine = rawLine[:len(rawLine)-1]
		}
		if bytes.IndexByte(rawLine, '\r') >= 0 {
			return nil, fmt.Errorf("peer service environment line %d contains an invalid carriage return", lineNumber)
		}
		line := string(rawLine)
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		name, value, found := strings.Cut(line, "=")
		if !found || !environmentNamePattern.MatchString(name) {
			return nil, fmt.Errorf("peer service environment line %d must be NAME=literal value", lineNumber)
		}
		normalized := canonicalEnvironmentName(name)
		if previous, duplicate := normalizedNames[normalized]; duplicate {
			return nil, fmt.Errorf(
				"peer service environment line %d duplicates variable %q",
				lineNumber,
				previous,
			)
		}
		if len(environment) >= maximumEntries {
			return nil, fmt.Errorf("peer service environment exceeds %d entries", maximumEntries)
		}
		normalizedNames[normalized] = name
		environment[name] = value
	}
	return environment, nil
}

func canonicalEnvironmentName(name string) string {
	if runtime.GOOS == "windows" {
		return strings.ToUpper(name)
	}
	return name
}

func mapLookup(environment map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		if value, found := environment[name]; found {
			return value, true
		}
		if runtime.GOOS == "windows" {
			for candidate, value := range environment {
				if strings.EqualFold(candidate, name) {
					return value, true
				}
			}
		}
		return "", false
	}
}

func containsName(names []string, candidate string) bool {
	for _, name := range names {
		if name == candidate || runtime.GOOS == "windows" && strings.EqualFold(name, candidate) {
			return true
		}
	}
	return false
}
