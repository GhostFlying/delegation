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

func TestTailscaleStateDirLeaseIsExclusiveIndependentAndNonMutating(t *testing.T) {
	root := filepath.Join(t.TempDir(), "state")
	if err := createPrivateDirectory(root); err != nil {
		t.Fatal(err)
	}
	firstDir := filepath.Join(root, "first-tsnet")
	secondDir := filepath.Join(root, "second-tsnet")
	if err := createPrivateDirectory(firstDir); err != nil {
		t.Fatal(err)
	}
	markerPath := filepath.Join(firstDir, "tailscaled.state")
	marker := []byte("existing tailscale state")
	if err := os.WriteFile(markerPath, marker, 0o600); err != nil {
		t.Fatal(err)
	}

	first, err := AcquireTailscaleStateDirLease(firstDir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(secondDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unrelated state directory changed before lease: %v", err)
	}
	duplicate, err := AcquireTailscaleStateDirLease(firstDir)
	if duplicate != nil {
		_ = duplicate.Close()
		t.Fatal("duplicate tailscale state directory lease succeeded")
	}
	if !errors.Is(err, ErrTailscaleStateDirLeaseHeld) {
		t.Fatalf("duplicate lease error = %v, want ErrTailscaleStateDirLeaseHeld", err)
	}
	second, err := AcquireTailscaleStateDirLease(secondDir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(secondDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("acquiring lease created the tailscale state directory: %v", err)
	}
	after, err := os.ReadFile(markerPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(marker) {
		t.Fatalf("existing tailscale state = %q, want %q", after, marker)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	reacquired, err := AcquireTailscaleStateDirLease(firstDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := reacquired.Close(); err != nil {
		t.Fatal(err)
	}
	for _, lockPath := range []string{
		firstDir + ".tailscale.lock",
		secondDir + ".tailscale.lock",
	} {
		if _, err := os.Stat(lockPath); err != nil {
			t.Fatalf("persistent tailscale lease file %q is missing: %v", lockPath, err)
		}
	}
}

func TestTailscaleStateDirLeaseRejectsAliasWithoutChangingTarget(t *testing.T) {
	root := filepath.Join(t.TempDir(), "state")
	if err := createPrivateDirectory(root); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(root, "target")
	if err := createPrivateDirectory(target); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(root, "alias")
	if err := os.Symlink(target, alias); err != nil {
		t.Skipf("creating directory symlink is unavailable: %v", err)
	}
	if _, err := AcquireTailscaleStateDirLease(alias); err == nil {
		t.Fatal("tailscale state directory lease accepted a symbolic link")
	}
	info, err := os.Lstat(alias)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		t.Fatal("lease acquisition changed the symbolic link target")
	}
}

func TestTailscaleStateDirLeaseIsExclusiveAcrossProcesses(t *testing.T) {
	const helperEnvironment = "DELEGATION_TEST_TAILSCALE_STATE_DIR_LEASE"
	if mode := os.Getenv(helperEnvironment); mode != "" {
		lease, err := AcquireTailscaleStateDirLease(os.Getenv("DELEGATION_TEST_TAILSCALE_STATE_DIR"))
		switch mode {
		case "held":
			if lease != nil {
				_ = lease.Close()
				t.Fatal("child acquired an already-held tailscale state directory lease")
			}
			if !errors.Is(err, ErrTailscaleStateDirLeaseHeld) {
				t.Fatalf("child lease error = %v, want ErrTailscaleStateDirLeaseHeld", err)
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
			t.Fatalf("unknown tailscale state directory lease helper mode %q", mode)
		}
		return
	}

	root := filepath.Join(t.TempDir(), "state")
	if err := createPrivateDirectory(root); err != nil {
		t.Fatal(err)
	}
	firstDir := filepath.Join(root, "first")
	secondDir := filepath.Join(root, "second")

	holder := startTailscaleLeaseHolder(t, helperEnvironment, firstDir)
	runTailscaleLeaseHelper(t, helperEnvironment, "held", firstDir)
	runTailscaleLeaseHelper(t, helperEnvironment, "available", secondDir)
	if err := holder.stdin.Close(); err != nil {
		t.Fatal(err)
	}
	if err := holder.command.Wait(); err != nil {
		t.Fatalf("normal tailscale lease holder exit: %v", err)
	}
	holder.waited = true
	runTailscaleLeaseHelper(t, helperEnvironment, "available", firstDir)

	killed := startTailscaleLeaseHolder(t, helperEnvironment, firstDir)
	if err := killed.command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = killed.stdin.Close()
	if err := killed.command.Wait(); err == nil {
		t.Fatal("killed tailscale lease helper exited successfully")
	}
	killed.waited = true
	runTailscaleLeaseHelper(t, helperEnvironment, "available", firstDir)

	for _, lockPath := range []string{
		firstDir + ".tailscale.lock",
		secondDir + ".tailscale.lock",
	} {
		if _, err := os.Stat(lockPath); err != nil {
			t.Fatalf("persistent tailscale lease file %q is missing: %v", lockPath, err)
		}
	}
}

func runTailscaleLeaseHelper(t *testing.T, helperEnvironment, mode, stateDir string) {
	t.Helper()
	command := exec.Command(
		os.Args[0],
		"-test.run=^TestTailscaleStateDirLeaseIsExclusiveAcrossProcesses$",
		"-test.count=1",
	)
	command.Env = append(os.Environ(),
		helperEnvironment+"="+mode,
		"DELEGATION_TEST_TAILSCALE_STATE_DIR="+stateDir,
	)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("tailscale lease helper failed: %v\n%s", err, output)
	}
}

type tailscaleLeaseHolder struct {
	command *exec.Cmd
	stdin   io.WriteCloser
	waited  bool
}

func startTailscaleLeaseHolder(
	t *testing.T,
	helperEnvironment, stateDir string,
) *tailscaleLeaseHolder {
	t.Helper()
	command := exec.Command(
		os.Args[0],
		"-test.run=^TestTailscaleStateDirLeaseIsExclusiveAcrossProcesses$",
		"-test.count=1",
	)
	command.Env = append(os.Environ(),
		helperEnvironment+"=hold",
		"DELEGATION_TEST_TAILSCALE_STATE_DIR="+stateDir,
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
	holder := &tailscaleLeaseHolder{command: command, stdin: stdin}
	t.Cleanup(func() {
		if holder.waited {
			return
		}
		_ = command.Process.Kill()
		_ = command.Wait()
		_ = stdin.Close()
	})
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil {
		t.Fatalf("read tailscale lease helper readiness: %v", err)
	}
	if line != "ready\n" {
		t.Fatalf("tailscale lease helper readiness = %q", line)
	}
	return holder
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
