//go:build integration && linux

package codex_peer_e2e

import (
	"bytes"
	"context"
	"database/sql"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func commandEnv(current peer) []string {
	return append(os.Environ(), "HOME="+current.home, "DELEGATION_HOME="+current.delegationHome)
}

func run(t *testing.T, environment []string, binary string, args ...string) (string, string) {
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
		t.Fatalf("command timed out: %s %s\nstdout: %s\nstderr: %s", binary, strings.Join(args, " "), stdout.String(), stderr.String())
	}
	if err != nil {
		t.Fatalf("command failed: %s %s: %v\nstdout: %s\nstderr: %s", binary, strings.Join(args, " "), err, stdout.String(), stderr.String())
	}
	return stdout.String(), stderr.String()
}

func startService(t *testing.T, environment []string, binary, configPath string) {
	t.Helper()
	logPath := configPath + ".service.log"
	log, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(binary, "service", "run", "--config", configPath)
	command.Env = environment
	command.Stdout = log
	command.Stderr = log
	if err := command.Start(); err != nil {
		log.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = command.Process.Kill()
		_ = command.Wait()
		_ = log.Close()
	})
}

func freeAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func waitForHealth(t *testing.T, endpoint string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		response, err := http.Get(endpoint)
		if err == nil {
			response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("broker health endpoint did not become ready: %s", endpoint)
}

func waitForCount(t *testing.T, statePath, query string, want int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if count, err := queryCount(statePath, query); err == nil && count == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	assertCount(t, statePath, query, want)
}

func assertCount(t *testing.T, statePath, query string, want int) {
	t.Helper()
	got, err := queryCount(statePath, query)
	if err != nil || got != want {
		t.Fatalf("query %q = %d, error %v; want %d", query, got, err, want)
	}
}

func queryCount(statePath, query string) (int, error) {
	database, err := sql.Open("sqlite", "file:"+filepath.ToSlash(statePath)+"?_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)")
	if err != nil {
		return 0, err
	}
	defer database.Close()
	var count int
	err = database.QueryRow(query).Scan(&count)
	return count, err
}

func assertRootBindings(t *testing.T, statePath string, want map[string]string) {
	t.Helper()
	database := openDatabase(t, statePath)
	defer database.Close()
	rows, err := database.Query("SELECT external_thread_id, root_device_id FROM trees")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := make(map[string]string)
	for rows.Next() {
		var threadID, deviceID string
		if err := rows.Scan(&threadID, &deviceID); err != nil {
			t.Fatal(err)
		}
		got[threadID] = deviceID
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !mapsEqual(got, want) {
		t.Fatalf("root bindings = %#v, want %#v", got, want)
	}
}

func assertPrincipalDistribution(t *testing.T, statePath string, want map[string]int) {
	t.Helper()
	database := openDatabase(t, statePath)
	defer database.Close()
	rows, err := database.Query("SELECT device_id, count(*) FROM principals GROUP BY device_id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := make(map[string]int)
	for rows.Next() {
		var deviceID string
		var count int
		if err := rows.Scan(&deviceID, &count); err != nil {
			t.Fatal(err)
		}
		got[deviceID] = count
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !mapsEqual(got, want) {
		t.Fatalf("principal distribution = %#v, want %#v", got, want)
	}
}

func openDatabase(t *testing.T, statePath string) *sql.DB {
	t.Helper()
	database, err := sql.Open("sqlite", "file:"+filepath.ToSlash(statePath)+"?_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)")
	if err != nil {
		t.Fatal(err)
	}
	return database
}

func mapsEqual[K comparable, V comparable](first, second map[K]V) bool {
	if len(first) != len(second) {
		return false
	}
	for key, value := range first {
		if second[key] != value {
			return false
		}
	}
	return true
}

func copyRollout(t *testing.T, sourceHome, destinationHome, threadID string) {
	t.Helper()
	var matches []string
	err := filepath.WalkDir(sourceHome, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() && strings.Contains(entry.Name(), threadID) && strings.HasSuffix(entry.Name(), ".jsonl") {
			matches = append(matches, path)
		}
		return nil
	})
	if err != nil || len(matches) != 1 {
		t.Fatalf("rollouts for thread %s = %v, error %v", threadID, matches, err)
	}
	relative, err := filepath.Rel(sourceHome, matches[0])
	if err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(destinationHome, relative)
	if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(matches[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(destination, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func requiredExecutable(t *testing.T, variable string) string {
	t.Helper()
	path := os.Getenv(variable)
	if path == "" {
		t.Fatalf("%s is required", variable)
	}
	resolved, err := exec.LookPath(path)
	if err != nil {
		t.Fatalf("resolve %s: %v", variable, err)
	}
	return resolved
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	command := exec.Command("git", "rev-parse", "--show-toplevel")
	output, err := command.Output()
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(output))
}
