//go:build integration && live && (linux || darwin)

package codex_peer_e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/identity"
)

func isTraeXLiveProcessCommand(command string) bool {
	return command == "traex" || command == "traecli"
}

func runTraeXLive(
	t *testing.T, environment []string, binary string, args ...string,
) (string, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary, args...)
	command.Env = environment
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	err := command.Run()
	if ctx.Err() != nil {
		t.Fatalf(
			"command timed out: %s %s\nstdout: %s\nstderr: %s",
			binary, strings.Join(args, " "), stdout.String(), stderr.String(),
		)
	}
	if err != nil {
		t.Fatalf(
			"command failed: %s %s: %v\nstdout: %s\nstderr: %s",
			binary, strings.Join(args, " "), err, stdout.String(), stderr.String(),
		)
	}
	return stdout.String(), stderr.String()
}

func newTraeXLiveIdentity(t *testing.T) string {
	t.Helper()
	value, err := identity.NewID()
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func optionalLiveProtectedFile(t *testing.T, variable string) string {
	t.Helper()
	path := os.Getenv(variable)
	if path == "" {
		t.Skipf("%s is not set", variable)
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		t.Fatalf("resolve %s: %v", variable, err)
	}
	return absolute
}

func liveTraeXAccessToken(t *testing.T, auth []byte) string {
	t.Helper()
	var document struct {
		Trae struct {
			AccessToken string `json:"access_token"`
		} `json:"trae"`
	}
	if err := json.Unmarshal(auth, &document); err != nil {
		t.Fatalf("decode validated TraeX authentication source: %v", err)
	}
	if document.Trae.AccessToken == "" {
		t.Fatal("validated TraeX authentication source omitted an access token")
	}
	return document.Trae.AccessToken
}

func shellSingleQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}

func assertTraeXAccountBoundary(
	t *testing.T, rolloutPath, sourceAuthPath, managedAuthPath, accessToken string,
) {
	t.Helper()
	rollout, err := os.ReadFile(rolloutPath)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(rollout, []byte(accessToken)) {
		t.Fatal("TraeX rollout contains the protected account access token")
	}

	authCalls := make(map[string]struct{})
	toolNames := make(map[string]struct{})
	var authReferences []string
	sourceDenied := false
	managedDenied := false
	sourceAliasSetup := false
	managedAliasSetup := false
	sourceAliasDenied := false
	managedAliasDenied := false
	for _, line := range bytes.Split(rollout, []byte{'\n'}) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var record any
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatalf("decode TraeX rollout record: %v", err)
		}
		walkTraeXRollout(record, func(item map[string]any) {
			typeName, _ := item["type"].(string)
			name, _ := item["name"].(string)
			if name != "" {
				toolNames[name] = struct{}{}
			}
			encoded, _ := json.Marshal(item)
			if bytes.Contains(encoded, []byte(sourceAuthPath)) ||
				bytes.Contains(encoded, []byte(managedAuthPath)) {
				keys := make([]string, 0, len(item))
				for key := range item {
					keys = append(keys, key)
				}
				authReferences = append(authReferences, fmt.Sprintf(
					"type=%q name=%q keys=%v", typeName, name, keys,
				))
			}
			switch typeName {
			case "function_call":
				arguments, _ := item["arguments"].(string)
				callID, _ := item["call_id"].(string)
				if isTraeXShellTool(name) && callID != "" &&
					strings.Contains(arguments, sourceAuthPath) &&
					strings.Contains(arguments, managedAuthPath) {
					authCalls[callID] = struct{}{}
				}
			case "function_call_output", "exec_command_end":
				callID, _ := item["call_id"].(string)
				if _, found := authCalls[callID]; !found {
					return
				}
				output := traeXToolOutput(item)
				sourceDenied = sourceDenied || strings.Contains(output, "SOURCE_AUTH_RESULT=blocked")
				managedDenied = managedDenied || strings.Contains(output, "MANAGED_AUTH_RESULT=blocked")
				sourceAliasSetup = sourceAliasSetup || strings.Contains(output, "SOURCE_ALIAS_SETUP=ok")
				managedAliasSetup = managedAliasSetup || strings.Contains(output, "MANAGED_ALIAS_SETUP=ok")
				sourceAliasDenied = sourceAliasDenied || strings.Contains(output, "SOURCE_ALIAS_RESULT=blocked")
				managedAliasDenied = managedAliasDenied || strings.Contains(output, "MANAGED_ALIAS_RESULT=blocked")
			}
		})
	}
	if len(authCalls) == 0 {
		t.Fatalf(
			"real TraeX account turn did not expose a recognizable credential boundary probe; tool names=%v auth references=%v",
			toolNames, authReferences,
		)
	}
	if !sourceDenied || !managedDenied || !sourceAliasSetup || !managedAliasSetup ||
		!sourceAliasDenied || !managedAliasDenied {
		t.Fatalf(
			"TraeX worker credential deny result: source=%t managed=%t sourceAliasSetup=%t managedAliasSetup=%t sourceAlias=%t managedAlias=%t",
			sourceDenied, managedDenied, sourceAliasSetup, managedAliasSetup,
			sourceAliasDenied, managedAliasDenied,
		)
	}
}

func isTraeXShellTool(name string) bool {
	return name == "exec_command" || name == "exec" || name == "Bash" ||
		name == "shell_command"
}

func traeXToolOutput(item map[string]any) string {
	var output strings.Builder
	for _, key := range []string{"output", "aggregated_output", "stdout", "stderr", "formatted_output"} {
		value, _ := item[key].(string)
		output.WriteString(value)
		output.WriteByte('\n')
	}
	return output.String()
}

func walkTraeXRollout(value any, visit func(map[string]any)) {
	switch value := value.(type) {
	case map[string]any:
		visit(value)
		for _, child := range value {
			walkTraeXRollout(child, visit)
		}
	case []any:
		for _, child := range value {
			walkTraeXRollout(child, visit)
		}
	}
}
