//go:build integration && linux

package codex_peer_e2e

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const (
	networkID = "123e4567-e89b-42d3-a456-426614174900"
	finalText = "delegation-peer-topology-e2e-ok"
)

var deviceIDs = map[string]string{
	"A": "123e4567-e89b-42d3-a456-426614174901",
	"B": "123e4567-e89b-42d3-a456-426614174902",
	"C": "123e4567-e89b-42d3-a456-426614174903",
}

type peer struct {
	label          string
	home           string
	codexHome      string
	delegationHome string
	configPath     string
	managedConfig  string
}

type execResult struct {
	threadID string
	stdout   string
	stderr   string
}

func TestCodexPeerTopology(t *testing.T) {
	delegationBinary := requiredExecutable(t, "DELEGATION_E2E_BINARY")
	codexBinary := requiredExecutable(t, "CODEX_BINARY")
	repoRoot := repositoryRoot(t)
	userHome, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	root, err := os.MkdirTemp(userHome, ".dpe-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}

	mock := &mockResponses{calls: make(map[string]int)}
	modelServer := httptest.NewServer(mock)
	t.Cleanup(modelServer.Close)
	peers := createPeers(t, root, modelServer.URL)
	brokerAddress := freeAddress(t)
	brokerConfig := filepath.Join(peers[0].delegationHome, "broker.json")
	statePath := filepath.Join(peers[0].delegationHome, "state", "broker.sqlite3")
	brokerToken := filepath.Join(peers[0].delegationHome, "secrets", "broker.token")
	run(t, commandEnv(peers[0]), delegationBinary,
		"setup", "broker", "--config", brokerConfig,
		"--controller-id", networkID, "--listen", brokerAddress,
		"--state", statePath, "--auth-mode", "token", "--token-file", brokerToken, "--json",
	)
	startService(t, commandEnv(peers[0]), delegationBinary, brokerConfig)
	waitForHealth(t, "http://"+brokerAddress+"/healthz")

	for _, current := range peers {
		tokenPath := filepath.Join(current.delegationHome, "secrets", "peer.token")
		run(t, commandEnv(peers[0]), delegationBinary,
			"credential", "issue", "--config", brokerConfig,
			"--device-id", deviceIDs[current.label], "--out", tokenPath, "--json",
		)
		run(t, commandEnv(current), delegationBinary,
			"setup", "peer", "--config", current.configPath,
			"--controller-id", networkID, "--device-id", deviceIDs[current.label],
			"--device-name", "peer-"+strings.ToLower(current.label),
			"--broker-url", "ws://"+brokerAddress+"/v1/connect",
			"--auth-mode", "token", "--token-file", tokenPath, "--json",
		)
		startService(t, commandEnv(current), delegationBinary, current.configPath)
	}
	waitForCount(t, statePath, "SELECT count(*) FROM devices WHERE online = 1", 3)
	assertCount(t, statePath, "SELECT count(*) FROM trees", 0)
	assertCount(t, statePath, "SELECT count(*) FROM principals", 0)

	for _, current := range peers {
		installPlugin(t, current, codexBinary, repoRoot)
		assertMCPReady(t, current, codexBinary, delegationBinary)
	}
	lazy := runCodex(t, peers[0], codexBinary, delegationBinary, repoRoot, "lazy", "")
	if lazy.threadID == "" || !strings.Contains(lazy.stdout, finalText) {
		t.Fatalf("lazy task output did not complete: %s\n%s", lazy.stdout, lazy.stderr)
	}
	assertCount(t, statePath, "SELECT count(*) FROM trees", 0)

	results := make(map[string]execResult)
	for index, testCase := range []string{"a1", "b1", "c1"} {
		current := peers[index]
		result := runCodex(t, current, codexBinary, delegationBinary, repoRoot, testCase, "")
		mock.check(t)
		results[current.label] = result
	}
	if results["A"].threadID == results["B"].threadID ||
		results["A"].threadID == results["C"].threadID ||
		results["B"].threadID == results["C"].threadID {
		t.Fatalf("root task thread IDs are not distinct: %#v", results)
	}
	assertRootBindings(t, statePath, map[string]string{
		results["A"].threadID: deviceIDs["A"],
		results["B"].threadID: deviceIDs["B"],
		results["C"].threadID: deviceIDs["C"],
	})

	secondA := runCodex(t, peers[0], codexBinary, delegationBinary, repoRoot, "a2", "")
	if secondA.threadID == results["A"].threadID {
		t.Fatalf("two user tasks on peer A reused thread %s", secondA.threadID)
	}
	assertCount(t, statePath, "SELECT count(*) FROM trees", 4)
	resumedA := runCodex(t, peers[0], codexBinary, delegationBinary, repoRoot, "a1-resume", results["A"].threadID)
	if resumedA.threadID != results["A"].threadID {
		t.Fatalf("cold resume thread = %q, want %q", resumedA.threadID, results["A"].threadID)
	}
	assertCount(t, statePath, "SELECT count(*) FROM trees", 4)

	copyRollout(t, peers[0].codexHome, peers[1].codexHome, results["A"].threadID)
	runCodex(t, peers[1], codexBinary, delegationBinary, repoRoot, "cross-conflict", results["A"].threadID)
	assertCount(t, statePath, "SELECT count(*) FROM trees", 4)
	assertPrincipalDistribution(t, statePath, map[string]int{
		deviceIDs["A"]: 2,
		deviceIDs["B"]: 1,
		deviceIDs["C"]: 1,
	})
	for _, current := range peers {
		matches, err := filepath.Glob(filepath.Join(current.home, ".delegation", "run", "*.sock"))
		if err != nil || len(matches) != 1 {
			t.Fatalf("peer %s local bridge sockets = %v, error %v", current.label, matches, err)
		}
	}
	mock.verify(t, []string{"lazy", "a1", "b1", "c1", "a2", "a1-resume", "cross-conflict"})
}

func createPeers(t *testing.T, root, modelURL string) []peer {
	t.Helper()
	peers := make([]peer, 0, 3)
	for _, label := range []string{"A", "B", "C"} {
		base := filepath.Join(root, strings.ToLower(label))
		current := peer{
			label:          label,
			home:           filepath.Join(base, "h"),
			codexHome:      filepath.Join(base, "c"),
			delegationHome: filepath.Join(base, "d"),
		}
		current.configPath = filepath.Join(current.delegationHome, "peer.json")
		current.managedConfig = fmt.Sprintf(`{
				"model":"gpt-5.2",
				"model_provider":"delegation_mock",
				"model_providers.delegation_mock":{
					"name":"Delegation acceptance mock",
					"base_url":%q,
					"wire_api":"responses",
					"requires_openai_auth":false
				}
			}`, modelURL+"/v1")
		for _, directory := range []string{current.home, current.codexHome, current.delegationHome} {
			if err := os.MkdirAll(directory, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(directory, 0o700); err != nil {
				t.Fatal(err)
			}
		}
		config := fmt.Sprintf(`model = "gpt-5.2"
model_provider = "delegation_mock"
approval_policy = "never"
sandbox_mode = "read-only"

[features]
plugins = true

[model_providers.delegation_mock]
name = "Delegation acceptance mock"
base_url = %q
wire_api = "responses"
requires_openai_auth = false
supports_websockets = false
request_max_retries = 0
stream_max_retries = 0
http_headers = { "x-delegation-test-peer" = %q }
`, modelURL+"/v1", label)
		if err := os.WriteFile(filepath.Join(current.codexHome, "config.toml"), []byte(config), 0o600); err != nil {
			t.Fatal(err)
		}
		peers = append(peers, current)
	}
	return peers
}

func installPlugin(t *testing.T, current peer, codexBinary, repoRoot string) {
	t.Helper()
	environment := append(commandEnv(current), "CODEX_HOME="+current.codexHome)
	run(t, environment, codexBinary, "plugin", "marketplace", "add", repoRoot, "--json")
	run(t, environment, codexBinary, "plugin", "add", "delegation@delegation", "--json")
}

func runCodex(
	t *testing.T,
	current peer,
	codexBinary, delegationBinary, repoRoot, testCase, resumeThread string,
) execResult {
	t.Helper()
	prompt := "delegation-e2e-case=" + testCase
	args := []string{"exec"}
	if resumeThread == "" {
		args = append(args, "--strict-config", "--json", "--skip-git-repo-check", "-C", repoRoot, prompt)
	} else {
		args = append(args, "resume", "--all", "--strict-config", "--json", "--skip-git-repo-check", resumeThread, prompt)
	}
	environment := append(commandEnv(current),
		"CODEX_HOME="+current.codexHome,
		"DELEGATION_BINARY="+delegationBinary,
		"DELEGATION_CONFIG="+current.configPath,
	)
	stdout, stderr := run(t, environment, codexBinary, args...)
	result := execResult{stdout: stdout, stderr: stderr}
	for _, line := range strings.Split(stdout, "\n") {
		var event map[string]any
		if json.Unmarshal([]byte(line), &event) == nil && event["type"] == "thread.started" {
			result.threadID, _ = event["thread_id"].(string)
		}
	}
	if result.threadID == "" {
		t.Fatalf("Codex task %s did not emit thread.started: %s\n%s", testCase, stdout, stderr)
	}
	if !strings.Contains(stdout, finalText) {
		t.Fatalf("Codex task %s did not emit the final marker: %s\n%s", testCase, stdout, stderr)
	}
	return result
}
