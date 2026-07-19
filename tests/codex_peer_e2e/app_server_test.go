//go:build integration && linux

package codex_peer_e2e

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func assertMCPReady(t *testing.T, current peer, codexBinary, delegationBinary string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, codexBinary, "app-server", "--stdio")
	command.Env = append(commandEnv(current),
		"CODEX_HOME="+current.codexHome,
		"DELEGATION_BINARY="+delegationBinary,
		"DELEGATION_CONFIG="+current.configPath,
	)
	stdin, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprintln(stdin, `{"id":1,"method":"initialize","params":{"clientInfo":{"name":"delegation-e2e","version":"1.0.0"},"capabilities":null}}`); err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 64*1024), 4<<20)
	var transcript []string
	if !scanForResponse(scanner, &transcript, 1, nil) {
		stdin.Close()
		_ = command.Wait()
		t.Fatalf("peer %s MCP preflight returned no initialize response: %s\nstderr: %s", current.label, strings.Join(transcript, "\n"), stderr.String())
	}
	time.Sleep(time.Second)
	if _, err := fmt.Fprintln(stdin, `{"method":"initialized"}`); err != nil {
		t.Fatal(err)
	}
	var statusResponse map[string]any
	ready := false
	for attempt := range 30 {
		requestID := float64(attempt + 2)
		if _, err := fmt.Fprintf(stdin, `{"id":%d,"method":"mcpServerStatus/list","params":{"detail":"full"}}`+"\n", attempt+2); err != nil {
			t.Fatal(err)
		}
		if !scanForResponse(scanner, &transcript, requestID, &statusResponse) {
			break
		}
		if delegationToolsReady(statusResponse) {
			ready = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err := stdin.Close(); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err != nil || ctx.Err() != nil {
		t.Fatalf("peer %s app-server MCP preflight failed: %v\nstdout: %s\nstderr: %s", current.label, errors.Join(err, ctx.Err()), strings.Join(transcript, "\n"), stderr.String())
	}
	line, _ := json.Marshal(statusResponse)
	if !ready {
		config, _ := os.ReadFile(filepath.Join(current.codexHome, "config.toml"))
		plugins, _ := filepath.Glob(filepath.Join(current.codexHome, "plugins", "cache", "delegation", "delegation", "*"))
		t.Fatalf("peer %s MCP preflight did not become ready: %s\nconfig: %s\nplugin cache: %v\nstderr: %s", current.label, line, config, plugins, stderr.String())
	}
	if statusResponse["error"] != nil {
		t.Fatalf("peer %s MCP preflight error: %s\nstderr: %s", current.label, line, stderr.String())
	}
	return
}

func delegationToolsReady(response map[string]any) bool {
	if response["error"] != nil {
		return false
	}
	result, _ := response["result"].(map[string]any)
	servers, _ := result["data"].([]any)
	for _, value := range servers {
		server, _ := value.(map[string]any)
		tools, _ := server["tools"].(map[string]any)
		if server["name"] == "delegation" && tools["list_devices"] != nil && tools["describe_device"] != nil {
			return true
		}
	}
	return false
}

func scanForResponse(scanner *bufio.Scanner, transcript *[]string, id float64, found *map[string]any) bool {
	for scanner.Scan() {
		line := scanner.Text()
		*transcript = append(*transcript, line)
		var response map[string]any
		if json.Unmarshal([]byte(line), &response) != nil || response["id"] != id {
			continue
		}
		if found != nil {
			*found = response
		}
		return true
	}
	return false
}
