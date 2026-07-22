package appserver

import (
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"
)

func TestProcessExitIsReportedBeforeInheritedOutputPipesDrain(t *testing.T) {
	stdout := &observedReadCloser{closed: make(chan struct{})}
	stderr := &observedReadCloser{closed: make(chan struct{})}
	client := &Client{
		command:       &exec.Cmd{},
		processOwner:  immediateOwnedProcess{},
		stdout:        stdout,
		stderrPipe:    stderr,
		stdoutDone:    make(chan struct{}),
		stderrDone:    make(chan struct{}),
		processExited: make(chan struct{}),
	}
	client.closing.Store(true)
	waitDone := make(chan struct{})
	go func() {
		client.waitLoop()
		close(waitDone)
	}()

	select {
	case <-client.processExited:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("process exit was blocked by inherited output pipes")
	}
	select {
	case <-waitDone:
		t.Fatal("output drain returned before its bounded timeout")
	default:
	}
	select {
	case <-stdout.closed:
	case <-time.After(2 * processOutputDrainTimeout):
		t.Fatal("stdout pipe was not closed after the drain timeout")
	}
	select {
	case <-stderr.closed:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("stderr pipe was not closed after the drain timeout")
	}
	select {
	case <-waitDone:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("wait loop did not finish after closing inherited output pipes")
	}
}

type immediateOwnedProcess struct{}

func (immediateOwnedProcess) Attach(*os.Process) error {
	return nil
}

func (immediateOwnedProcess) Wait(*exec.Cmd) processWaitResult {
	return processWaitResult{}
}

func (immediateOwnedProcess) Terminate() error {
	return nil
}

type observedReadCloser struct {
	once   sync.Once
	closed chan struct{}
}

func (*observedReadCloser) Read([]byte) (int, error) {
	return 0, nil
}

func (r *observedReadCloser) Close() error {
	r.once.Do(func() { close(r.closed) })
	return nil
}
