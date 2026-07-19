package store

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestBrokerLeaseIsExclusiveAcrossProcesses(t *testing.T) {
	const helperEnvironment = "DELEGATION_TEST_BROKER_LEASE"
	if mode := os.Getenv(helperEnvironment); mode != "" {
		lease, err := AcquireBrokerLease(os.Getenv("DELEGATION_TEST_BROKER_STATE"))
		switch mode {
		case "held":
			if lease != nil {
				_ = lease.Close()
				t.Fatal("child acquired an already-held broker lease")
			}
			if !errors.Is(err, ErrBrokerLeaseHeld) {
				t.Fatalf("child lease error = %v, want ErrBrokerLeaseHeld", err)
			}
		case "available":
			if err != nil {
				t.Fatal(err)
			}
			if err := lease.Close(); err != nil {
				t.Fatal(err)
			}
		case "hold":
			if err != nil {
				t.Fatal(err)
			}
			defer lease.Close()
			if _, err := fmt.Fprintln(os.Stdout, "ready"); err != nil {
				t.Fatal(err)
			}
			if _, err := io.Copy(io.Discard, os.Stdin); err != nil {
				t.Fatal(err)
			}
		default:
			t.Fatalf("unknown broker lease helper mode %q", mode)
		}
		return
	}

	statePath := filepath.Join(t.TempDir(), "state", "broker.sqlite3")
	lease, err := AcquireBrokerLease(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(statePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("acquiring broker lease opened the state database: %v", err)
	}
	registry, err := Open(context.Background(), statePath)
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	runBrokerLeaseHelper(t, helperEnvironment, "held", statePath)
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	runBrokerLeaseHelper(t, helperEnvironment, "available", statePath)

	holder := startBrokerLeaseHolder(t, helperEnvironment, statePath)
	runBrokerLeaseHelper(t, helperEnvironment, "held", statePath)
	if err := holder.command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = holder.stdin.Close()
	if err := holder.command.Wait(); err == nil {
		t.Fatal("killed broker lease helper exited successfully")
	}
	runBrokerLeaseHelper(t, helperEnvironment, "available", statePath)
	if _, err := os.Stat(statePath + ".broker.lock"); err != nil {
		t.Fatalf("persistent broker lease file is missing: %v", err)
	}
}

func runBrokerLeaseHelper(t *testing.T, helperEnvironment, mode, statePath string) {
	t.Helper()
	command := exec.Command(os.Args[0], "-test.run=^TestBrokerLeaseIsExclusiveAcrossProcesses$", "-test.count=1")
	command.Env = append(os.Environ(),
		helperEnvironment+"="+mode,
		"DELEGATION_TEST_BROKER_STATE="+statePath,
	)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("broker lease helper failed: %v\n%s", err, output)
	}
}

type brokerLeaseHolder struct {
	command *exec.Cmd
	stdin   io.WriteCloser
}

func startBrokerLeaseHolder(t *testing.T, helperEnvironment, statePath string) brokerLeaseHolder {
	t.Helper()
	command := exec.Command(os.Args[0], "-test.run=^TestBrokerLeaseIsExclusiveAcrossProcesses$", "-test.count=1")
	command.Env = append(os.Environ(),
		helperEnvironment+"=hold",
		"DELEGATION_TEST_BROKER_STATE="+statePath,
	)
	stdin, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = command.Process.Kill()
		_ = command.Wait()
		_ = stdin.Close()
	})
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil {
		t.Fatalf("read broker lease helper readiness: %v", err)
	}
	if line != "ready\n" {
		t.Fatalf("broker lease helper readiness = %q", line)
	}
	return brokerLeaseHolder{command: command, stdin: stdin}
}
