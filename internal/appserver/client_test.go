package appserver

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	helperModeEnvironment  = "DELEGATION_APP_SERVER_HELPER_MODE"
	helperValueEnvironment = "DELEGATION_APP_SERVER_HELPER_VALUE"
	helperFileEnvironment  = "DELEGATION_APP_SERVER_HELPER_FILE"
	helperUnsetEnvironment = "DELEGATION_APP_SERVER_HELPER_UNSET"
)

func TestOpenChildStdioClosesEveryPipeAfterPartialFailure(t *testing.T) {
	var opened []*os.File
	calls := 0
	_, err := openChildStdioWith(func() (*os.File, *os.File, error) {
		calls++
		if calls == 3 {
			return nil, nil, errors.New("injected pipe failure")
		}
		reader, writer, err := os.Pipe()
		opened = append(opened, reader, writer)
		return reader, writer, err
	})
	if err == nil {
		t.Fatal("openChildStdioWith accepted a partial pipe failure")
	}
	for _, file := range opened {
		if err := file.Close(); !errors.Is(err, os.ErrClosed) {
			t.Fatalf("partially opened pipe remained open: %v", err)
		}
	}
}

func TestClientRoutesConcurrentResponsesAndNotifications(t *testing.T) {
	client := startHelperClient(t, "normal", Options{
		Environment: map[string]string{helperValueEnvironment: "inherited-provider-value"},
	})

	var inspect struct {
		Arguments     []string `json:"arguments"`
		CodexHome     string   `json:"codexHome"`
		InheritedPath string   `json:"inheritedPath"`
		ProviderValue string   `json:"providerValue"`
	}
	callWithTimeout(t, client, "test/inspect", nil, &inspect)
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	expectedArguments := []string{filepath.Clean(executable), "app-server", "--listen", "stdio://"}
	if len(inspect.Arguments) != len(expectedArguments) {
		t.Fatalf("helper arguments = %q, want %q", inspect.Arguments, expectedArguments)
	}
	for index := range expectedArguments {
		actual := inspect.Arguments[index]
		if index == 0 {
			actual = filepath.Clean(actual)
		}
		if actual != expectedArguments[index] {
			t.Fatalf("helper arguments = %q, want %q", inspect.Arguments, expectedArguments)
		}
	}
	if inspect.CodexHome == "" || inspect.InheritedPath == "" || inspect.ProviderValue != "inherited-provider-value" {
		t.Fatalf("helper environment = %+v", inspect)
	}
	entries, err := os.ReadDir(inspect.CodexHome)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("app-server client wrote files into CODEX_HOME: %v", entries)
	}

	type callOutcome struct {
		result string
		err    error
	}
	slowResult := make(chan callOutcome, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		var result string
		err := client.Call(ctx, "test/slow", map[string]string{"value": "slow"}, &result)
		slowResult <- callOutcome{result: result, err: err}
	}()
	waitNotification(t, client, "turn/started")
	var fast string
	callWithTimeout(t, client, "test/fast", map[string]string{"value": "fast"}, &fast)
	if fast != "fast" {
		t.Fatalf("fast response = %q", fast)
	}
	select {
	case outcome := <-slowResult:
		if outcome.err != nil || outcome.result != "slow" {
			t.Fatalf("slow response = %q, %v", outcome.result, outcome.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for slow response")
	}

	callWithTimeout(t, client, "test/notify", nil, nil)
	notification := waitNotification(t, client, "turn/completed")
	var params map[string]bool
	if err := json.Unmarshal(notification.Params, &params); err != nil {
		t.Fatal(err)
	}
	if !params["ready"] {
		t.Fatalf("notification params = %s", notification.Params)
	}

	lateCtx, lateCancel := context.WithCancel(context.Background())
	lateResult := make(chan error, 1)
	go func() {
		lateResult <- client.Call(lateCtx, "test/late", nil, nil)
	}()
	waitNotification(t, client, "thread/status/changed")
	lateCancel()
	if err := <-lateResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("late call error = %v", err)
	}
	var afterLate string
	callWithTimeout(t, client, "test/after-late", nil, &afterLate)
	if afterLate != "after-late" {
		t.Fatalf("response after late response = %q", afterLate)
	}
}

func TestFinishStoppedCallReturnsClaimedResponse(t *testing.T) {
	terminalErr := errors.New("injected terminal error")
	client := &Client{
		pending:     make(map[uint64]pendingCall),
		done:        make(chan struct{}),
		terminalErr: terminalErr,
	}
	close(client.done)
	pending := pendingCall{result: make(chan response, 1)}
	pending.result <- response{result: json.RawMessage(`"claimed"`)}

	callResponse := client.finishStoppedCall(1, pending)
	if callResponse.err != nil || string(callResponse.result) != `"claimed"` {
		t.Fatalf("claimed response after stop = %q, %v", callResponse.result, callResponse.err)
	}
}

func TestFinishStoppedCallReturnsTerminalErrorWhenUnclaimed(t *testing.T) {
	terminalErr := errors.New("injected terminal error")
	pending := pendingCall{result: make(chan response, 1)}
	client := &Client{
		pending:     map[uint64]pendingCall{1: pending},
		done:        make(chan struct{}),
		terminalErr: terminalErr,
	}
	close(client.done)

	callResponse := client.finishStoppedCall(1, pending)
	if !errors.Is(callResponse.err, terminalErr) {
		t.Fatalf("unclaimed response after stop error = %v, want %v", callResponse.err, terminalErr)
	}
	if len(client.pending) != 0 {
		t.Fatalf("unclaimed response remained pending: %#v", client.pending)
	}
}

func TestUnexpectedServerRequestIsRejectedAndFatal(t *testing.T) {
	observed := filepath.Join(t.TempDir(), "callback-response.json")
	client := startHelperClient(t, "callback", Options{
		Environment: map[string]string{helperFileEnvironment: observed},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := client.Call(ctx, "test/callback", nil, nil)
	var callbackErr *UnexpectedServerRequestError
	if !errors.As(err, &callbackErr) || callbackErr.Method != "item/commandExecution/requestApproval" {
		t.Fatalf("callback call error = %T %v", err, err)
	}
	select {
	case fatalErr := <-client.Fatal():
		if !errors.As(fatalErr, &callbackErr) {
			t.Fatalf("fatal error = %T %v", fatalErr, fatalErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for fatal callback event")
	}
	var rejection struct {
		ID    string `json:"id"`
		Error struct {
			Code int `json:"code"`
		} `json:"error"`
	}
	data := waitForJSONFile(t, observed, &rejection)
	if rejection.ID != "callback-1" || rejection.Error.Code != jsonRPCMethodNotFound {
		t.Fatalf("callback rejection = %s", data)
	}
}

func TestCloseKillsHelperAfterTimeout(t *testing.T) {
	client := startHelperClient(t, "hang-on-eof", Options{CloseTimeout: 50 * time.Millisecond})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started := time.Now()
	err := client.Close(ctx)
	if !errors.Is(err, ErrCloseTimeout) {
		t.Fatalf("Close() error = %v, want ErrCloseTimeout", err)
	}
	if errors.Is(err, ErrProcessExitUnconfirmed) {
		t.Fatalf("Close() did not confirm process-group cleanup: %v", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("Close() took %s", elapsed)
	}
	select {
	case <-client.processExited:
	case <-time.After(2 * time.Second):
		t.Fatal("helper process survived Close timeout kill")
	}
	if client.killErr != nil {
		t.Fatalf("process ownership cleanup error = %v", client.killErr)
	}
}

func TestCloseWaitsForConfirmedExitAfterForcedTermination(t *testing.T) {
	owner := &gatedOwnedProcess{terminated: make(chan struct{})}
	client := closeContractClient(owner, 5*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	closeDone := make(chan error, 1)
	go func() { closeDone <- client.Close(ctx) }()
	select {
	case <-owner.terminated:
	case <-time.After(time.Second):
		t.Fatal("forced termination did not start")
	}
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned before process exit was confirmed: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(client.processExited)
	err := <-closeDone
	if !errors.Is(err, ErrCloseTimeout) || errors.Is(err, ErrProcessExitUnconfirmed) {
		t.Fatalf("Close() error = %v", err)
	}
	select {
	case <-client.processExited:
	default:
		t.Fatal("Close returned before process exit was confirmed")
	}
}

func TestCloseFailsClosedWhenForcedExitIsUnconfirmed(t *testing.T) {
	owner := &gatedOwnedProcess{terminated: make(chan struct{})}
	client := closeContractClient(owner, 5*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	err := client.Close(ctx)
	if !errors.Is(err, ErrCloseTimeout) || !errors.Is(err, ErrProcessExitUnconfirmed) ||
		!errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestCloseFailsClosedWhenForcedCleanupReportsFailure(t *testing.T) {
	owner := &gatedOwnedProcess{terminated: make(chan struct{})}
	client := closeContractClient(owner, 5*time.Millisecond)
	stdout, stderr := attachBlockingOutput(client)
	cleanupFailure := errors.New("injected process containment failure")
	go func() {
		<-owner.terminated
		client.errMu.Lock()
		client.waitResult.cleanupErr = cleanupFailure
		client.errMu.Unlock()
		close(client.processExited)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := client.Close(ctx)
	if !errors.Is(err, ErrCloseTimeout) || !errors.Is(err, ErrProcessExitUnconfirmed) ||
		!errors.Is(err, cleanupFailure) {
		t.Fatalf("Close() error = %v", err)
	}
	assertClientOutputStopped(t, client, stdout, stderr)
}

func TestCloseUsesWaitAsFinalCleanupProof(t *testing.T) {
	requestErr := errors.New("injected termination request failure")
	owner := &gatedOwnedProcess{terminated: make(chan struct{}), terminateErr: requestErr}
	client := closeContractClient(owner, 5*time.Millisecond)
	go func() {
		<-owner.terminated
		close(client.processExited)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := client.Close(ctx)
	if !errors.Is(err, ErrCloseTimeout) || !errors.Is(err, requestErr) ||
		errors.Is(err, ErrProcessExitUnconfirmed) {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestClosePreservesCallerCancellationAfterConfirmedForcedExit(t *testing.T) {
	owner := &gatedOwnedProcess{terminated: make(chan struct{})}
	client := closeContractClient(owner, time.Second)
	owner.onTerminate = func() { close(client.processExited) }
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := client.Close(ctx)
	if !errors.Is(err, context.Canceled) || errors.Is(err, ErrCloseTimeout) ||
		errors.Is(err, ErrProcessExitUnconfirmed) {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestCloseDistinguishesProcessExitFromCleanupFailure(t *testing.T) {
	directExitErr := errors.New("injected direct process exit")
	directExit := closeContractClient(&gatedOwnedProcess{}, time.Second)
	directExit.waitResult.exitErr = directExitErr
	close(directExit.processExited)
	if err := directExit.Close(context.Background()); !errors.Is(err, directExitErr) ||
		errors.Is(err, ErrProcessExitUnconfirmed) {
		t.Fatalf("direct-exit Close() error = %v", err)
	}

	cleanupErr := errors.New("injected descendant cleanup failure")
	cleanupFailure := closeContractClient(&gatedOwnedProcess{}, time.Second)
	cleanupFailure.waitResult.cleanupErr = cleanupErr
	close(cleanupFailure.processExited)
	if err := cleanupFailure.Close(context.Background()); !errors.Is(err, cleanupErr) ||
		!errors.Is(err, ErrProcessExitUnconfirmed) {
		t.Fatalf("cleanup-failure Close() error = %v", err)
	}
}

func TestCloseClosesOutputWhenForcedExitIsUnconfirmed(t *testing.T) {
	owner := &gatedOwnedProcess{terminated: make(chan struct{})}
	client := closeContractClient(owner, 5*time.Millisecond)
	stdout, stderr := attachBlockingOutput(client)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	err := client.Close(ctx)
	if !errors.Is(err, ErrProcessExitUnconfirmed) {
		t.Fatalf("Close() error = %v, want ErrProcessExitUnconfirmed", err)
	}
	assertClientOutputStopped(t, client, stdout, stderr)
}

func attachBlockingOutput(client *Client) (*blockingReadCloser, *blockingReadCloser) {
	stdout := newBlockingReadCloser()
	stderr := newBlockingReadCloser()
	client.stdout = stdout
	client.stderrPipe = stderr
	client.stdoutDone = make(chan struct{})
	client.stderrDone = make(chan struct{})
	client.notifications = make(chan Notification)
	client.stderr = newTailBuffer(1024)
	go client.readLoop()
	go client.drainStderr(stderr)
	return stdout, stderr
}

func assertClientOutputStopped(
	t *testing.T,
	client *Client,
	stdout, stderr *blockingReadCloser,
) {
	t.Helper()
	for name, closed := range map[string]<-chan struct{}{
		"stdout": stdout.closed,
		"stderr": stderr.closed,
	} {
		select {
		case <-closed:
		case <-time.After(time.Second):
			t.Fatalf("%s remained open after unconfirmed exit", name)
		}
	}
	for name, done := range map[string]<-chan struct{}{
		"stdout reader": client.stdoutDone,
		"stderr reader": client.stderrDone,
	} {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatalf("%s remained blocked after unconfirmed exit", name)
		}
	}
	select {
	case _, open := <-client.Notifications():
		if open {
			t.Fatal("notification stream remained open after unconfirmed exit")
		}
	case <-time.After(time.Second):
		t.Fatal("notification stream remained blocked after unconfirmed exit")
	}
}

func TestNotificationsCloseAfterClientClose(t *testing.T) {
	client := startHelperClient(t, "normal", Options{})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Close(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case _, open := <-client.Notifications():
		if open {
			t.Fatal("notification stream remained open after app-server output ended")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for notification stream to close")
	}
}

type gatedOwnedProcess struct {
	terminated   chan struct{}
	once         sync.Once
	terminateErr error
	onTerminate  func()
}

type blockingReadCloser struct {
	once   sync.Once
	closed chan struct{}
}

func newBlockingReadCloser() *blockingReadCloser {
	return &blockingReadCloser{closed: make(chan struct{})}
}

func (r *blockingReadCloser) Read([]byte) (int, error) {
	<-r.closed
	return 0, io.ErrClosedPipe
}

func (r *blockingReadCloser) Close() error {
	r.once.Do(func() { close(r.closed) })
	return nil
}

func (o *gatedOwnedProcess) Attach(*os.Process) error {
	return nil
}

func (o *gatedOwnedProcess) Wait(*exec.Cmd) processWaitResult {
	return processWaitResult{}
}

func (o *gatedOwnedProcess) Terminate() error {
	o.once.Do(func() {
		if o.terminated != nil {
			close(o.terminated)
		}
		if o.onTerminate != nil {
			o.onTerminate()
		}
	})
	return o.terminateErr
}

type discardWriteCloser struct{}

func (discardWriteCloser) Write(data []byte) (int, error) {
	return len(data), nil
}

func (discardWriteCloser) Close() error {
	return nil
}

func closeContractClient(owner ownedProcess, closeTimeout time.Duration) *Client {
	return &Client{
		processOwner: owner,
		stdin:        discardWriteCloser{}, closeTimeout: closeTimeout,
		processExited: make(chan struct{}), done: make(chan struct{}),
		pending: make(map[uint64]pendingCall), fatal: make(chan error, 1),
	}
}

func TestCloseDrainsLifecycleNotificationWrittenAfterEOF(t *testing.T) {
	client := startHelperClient(t, "normal", Options{})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Notify(ctx, "test/complete-on-eof", nil); err != nil {
		t.Fatal(err)
	}
	if err := client.Close(ctx); err != nil {
		t.Fatal(err)
	}
	var notifications []Notification
	for notification := range client.Notifications() {
		notifications = append(notifications, notification)
	}
	if len(notifications) != 1 || notifications[0].Method != "turn/completed" ||
		string(notifications[0].Params) != `{"afterEof":true}` {
		t.Fatalf("notifications drained during Close = %#v", notifications)
	}
}

func TestCloseDrainsLifecycleNotificationAfterLateRPCResponse(t *testing.T) {
	client := startHelperClient(t, "normal", Options{})
	callResult := make(chan error, 1)
	go func() {
		callResult <- client.Call(context.Background(), "test/slow-complete-on-eof", nil, nil)
	}()
	waitNotification(t, client, "turn/started")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-callResult; !errors.Is(err, ErrClosed) {
		t.Fatalf("pending Call() error = %v, want ErrClosed", err)
	}
	var notifications []Notification
	for notification := range client.Notifications() {
		notifications = append(notifications, notification)
	}
	if len(notifications) != 1 || notifications[0].Method != "turn/completed" ||
		string(notifications[0].Params) != `{"afterLateResponse":true}` {
		t.Fatalf("notifications after late RPC response = %#v", notifications)
	}
}

func TestCloseKillsOwnedProcessGroup(t *testing.T) {
	heartbeat := filepath.Join(t.TempDir(), "grandchild-heartbeat")
	client := startHelperClient(t, "spawn-grandchild-hang", Options{
		CloseTimeout: 50 * time.Millisecond,
		Environment:  map[string]string{helperFileEnvironment: heartbeat},
	})
	waitForFileGrowth(t, heartbeat)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Close(ctx); !errors.Is(err, ErrCloseTimeout) {
		t.Fatalf("Close() error = %v, want ErrCloseTimeout", err)
	}
	assertFileStopsGrowing(t, heartbeat)
}

func TestUnexpectedRootExitKillsOwnedProcessGroup(t *testing.T) {
	heartbeat := filepath.Join(t.TempDir(), "grandchild-heartbeat")
	client := startHelperClient(t, "spawn-grandchild-hang", Options{
		Environment: map[string]string{helperFileEnvironment: heartbeat},
	})
	waitForFileGrowth(t, heartbeat)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := client.Call(ctx, "test/exit", nil, nil)
	var processErr *ProcessError
	if !errors.As(err, &processErr) {
		t.Fatalf("exit Call() error = %T %v, want ProcessError", err, err)
	}
	assertFileStopsGrowing(t, heartbeat)
}

func TestOversizedRequestDoesNotTerminateClient(t *testing.T) {
	client := startHelperClient(t, "normal", Options{})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := client.Call(ctx, "test/oversized", strings.Repeat("x", MaxMessageBytes), nil)
	if !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("oversized Call() error = %v", err)
	}
	var result string
	callWithTimeout(t, client, "test/fast-standalone", nil, &result)
	if result != "fast-standalone" {
		t.Fatalf("response after oversized request = %q", result)
	}
}

func TestNotificationOverflowIsFatal(t *testing.T) {
	client := startHelperClient(t, "normal", Options{NotificationBuffer: 1})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := client.Call(ctx, "test/overflow", nil, nil)
	if !errors.Is(err, ErrNotificationOverflow) {
		t.Fatalf("overflow Call() error = %v", err)
	}
	select {
	case fatalErr := <-client.Fatal():
		if !errors.Is(fatalErr, ErrNotificationOverflow) {
			t.Fatalf("fatal error = %v", fatalErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for notification overflow")
	}
}

func TestHighVolumeNotificationsAreFilteredFromLifecycleQueue(t *testing.T) {
	client := startHelperClient(t, "normal", Options{NotificationBuffer: 1})
	callWithTimeout(t, client, "test/high-volume", nil, nil)
	notification := waitNotification(t, client, "turn/completed")
	if string(notification.Params) != `{"turnId":"turn-high-volume"}` {
		t.Fatalf("turn/completed params = %s", notification.Params)
	}
	select {
	case err := <-client.Fatal():
		t.Fatalf("high-volume notifications terminated client: %v", err)
	default:
	}
}

func TestBlockedCallWriteTerminatesOnCancellation(t *testing.T) {
	client := startHelperClient(t, "block-after-prefix", Options{})
	ctx, cancel := context.WithCancel(context.Background())
	callResult := make(chan error, 1)
	go func() {
		callResult <- client.Call(ctx, "test/blocked-write", strings.Repeat("x", 8<<20), nil)
	}()
	waitNotification(t, client, "turn/started")
	started := time.Now()
	cancel()
	select {
	case err := <-callResult:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("blocked Call() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked Call() ignored cancellation")
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("blocked Call() cancellation took %s", elapsed)
	}
	select {
	case <-client.processExited:
	case <-time.After(2 * time.Second):
		t.Fatal("app-server process survived blocked write cancellation")
	}
}

func TestCanceledCallWaitingForWriterDoesNotTerminateClient(t *testing.T) {
	client := startHelperClient(t, "normal", Options{})
	<-client.writeGate
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := client.Call(ctx, "test/not-written", nil, nil)
	if !errors.Is(err, context.Canceled) || !errors.Is(err, ErrRequestNotWritten) {
		t.Fatalf("Call() error = %v, want canceled request-not-written error", err)
	}
	client.writeGate <- struct{}{}

	var result string
	callWithTimeout(t, client, "test/fast-standalone", nil, &result)
	if result != "fast-standalone" {
		t.Fatalf("response after canceled unsent call = %q", result)
	}
	select {
	case err := <-client.Fatal():
		t.Fatalf("canceled unsent call terminated client: %v", err)
	default:
	}
}

func TestCloseInterruptsBlockedCallWrite(t *testing.T) {
	client := startHelperClient(t, "block-after-prefix", Options{CloseTimeout: 50 * time.Millisecond})
	callResult := make(chan error, 1)
	go func() {
		callResult <- client.Call(context.Background(), "test/blocked-write", strings.Repeat("x", 8<<20), nil)
	}()
	waitNotification(t, client, "turn/started")
	started := time.Now()
	err := client.Close(context.Background())
	var exitErr *exec.ExitError
	if err != nil && !errors.Is(err, ErrCloseTimeout) && !errors.As(err, &exitErr) {
		t.Fatalf("Close() error = %v, want a timeout or child exit", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("Close() with blocked writer took %s", elapsed)
	}
	select {
	case err := <-callResult:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("blocked Call() after Close error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked Call() survived Close")
	}
	select {
	case <-client.processExited:
	case <-time.After(2 * time.Second):
		t.Fatal("app-server process survived Close with blocked writer")
	}
}

func TestProcessErrorKeepsBoundedStderrTail(t *testing.T) {
	client := startHelperClient(t, "exit-with-stderr", Options{StderrLimit: 256})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := client.Call(ctx, "test/exit", nil, nil)
	var processErr *ProcessError
	if !errors.As(err, &processErr) {
		t.Fatalf("Call() error = %T %v, want ProcessError", err, err)
	}
	// The Darwin process supervisor appends its own bounded diagnostic after
	// the child exits, so the child marker is not necessarily the final bytes.
	if len(processErr.StderrTail) != 256 || !bytes.Contains(processErr.StderrTail, []byte("stderr-tail-marker")) {
		t.Fatalf("stderr tail length/suffix = %d %q", len(processErr.StderrTail), processErr.StderrTail)
	}
	if strings.Contains(processErr.Error(), "stderr-tail-marker") {
		t.Fatal("ProcessError.Error exposed captured stderr")
	}
}

func TestReadBoundedLine(t *testing.T) {
	reader := bufio.NewReaderSize(strings.NewReader("12345678\nnext\n"), 4)
	line, err := readBoundedLine(reader, 8)
	if err != nil || string(line) != "12345678" {
		t.Fatalf("first line = %q, %v", line, err)
	}
	line, err = readBoundedLine(reader, 4)
	if err != nil || string(line) != "next" {
		t.Fatalf("second line = %q, %v", line, err)
	}
	reader = bufio.NewReaderSize(strings.NewReader("123456789\n"), 4)
	if _, err := readBoundedLine(reader, 8); !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("oversized line error = %v", err)
	}
}

func TestEnvironmentCODEXHOMEConflict(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	_, err = validateOptions(Options{
		Binary: executable, SupervisorBinary: executable,
		CodexHome: t.TempDir(), Environment: map[string]string{"CODEX_HOME": t.TempDir()},
	})
	if err == nil || !strings.Contains(err.Error(), "conflicts") {
		t.Fatalf("validateOptions() error = %v", err)
	}
}

func TestUnsetEnvironmentIsNotInherited(t *testing.T) {
	t.Setenv(helperUnsetEnvironment, "must-not-reach-app-server")
	client := startHelperClient(t, "normal", Options{
		UnsetEnvironment: []string{helperUnsetEnvironment},
	})
	var inspect struct {
		UnsetValue string `json:"unsetValue"`
	}
	callWithTimeout(t, client, "test/inspect", nil, &inspect)
	if inspect.UnsetValue != "" {
		t.Fatalf("unset environment value reached app-server: %q", inspect.UnsetValue)
	}
}

func TestEnvironmentOverrideWinsOverUnsetOfSameKey(t *testing.T) {
	t.Setenv(helperUnsetEnvironment, "inherited")
	environment := buildEnvironment(
		map[string]string{helperUnsetEnvironment: "override"},
		[]string{helperUnsetEnvironment},
		t.TempDir(),
	)
	for _, entry := range environment {
		if entry == helperUnsetEnvironment+"=override" {
			return
		}
	}
	t.Fatalf("environment does not contain the explicit override: %q", environment)
}

func startHelperClient(t *testing.T, mode string, overrides Options) *Client {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	codexHome := t.TempDir()
	environment := map[string]string{helperModeEnvironment: mode}
	for key, value := range overrides.Environment {
		environment[key] = value
	}
	options := Options{
		Binary: executable, SupervisorBinary: executable,
		CodexHome: codexHome, Environment: environment,
		UnsetEnvironment: overrides.UnsetEnvironment,
		ClientVersion:    "test", HandshakeTimeout: 2 * time.Second,
		CloseTimeout: overrides.CloseTimeout, StderrLimit: overrides.StderrLimit,
		NotificationBuffer: overrides.NotificationBuffer, MaxPendingCalls: overrides.MaxPendingCalls,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, err := Start(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = client.Close(ctx)
	})
	return client
}

func callWithTimeout(t *testing.T, client *Client, method string, params, result any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Call(ctx, method, params, result); err != nil {
		t.Fatalf("Call(%q) error = %v", method, err)
	}
}

func waitNotification(t *testing.T, client *Client, method string) Notification {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case notification, open := <-client.Notifications():
			if !open {
				t.Fatalf("notification stream closed waiting for %q: %v", method, client.Err())
			}
			if notification.Method == method {
				return notification
			}
		case err := <-client.Fatal():
			t.Fatalf("client failed waiting for notification: %v", err)
		case <-deadline:
			t.Fatalf("timed out waiting for notification %q", method)
		}
	}
}

func waitForFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", path)
}

func waitForJSONFile(t *testing.T, path string, destination any) []byte {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(path)
		if err == nil {
			err = json.Unmarshal(data, destination)
			if err == nil {
				return data
			}
		}
		lastErr = err
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out reading JSON from %s: %v", path, lastErr)
	return nil
}

func waitForFileGrowth(t *testing.T, path string) {
	t.Helper()
	waitForFile(t, path)
	initial, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		current, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if current.Size() > initial.Size() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("grandchild heartbeat did not grow from %d bytes", initial.Size())
}

func assertFileStopsGrowing(t *testing.T, path string) {
	t.Helper()
	time.Sleep(100 * time.Millisecond)
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() != before.Size() {
		t.Fatalf("owned grandchild survived process-group termination: heartbeat grew from %d to %d", before.Size(), after.Size())
	}
}

func runHelperProcess() int {
	mode := os.Getenv(helperModeEnvironment)
	if mode == "grandchild-heartbeat" {
		for {
			file, err := os.OpenFile(os.Getenv(helperFileEnvironment), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
			if err != nil {
				return helperFailure("open heartbeat: %v", err)
			}
			_, writeErr := file.WriteString("x")
			closeErr := file.Close()
			if writeErr != nil || closeErr != nil {
				return helperFailure("write heartbeat: %v %v", writeErr, closeErr)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	reader := bufio.NewScanner(os.Stdin)
	reader.Buffer(make([]byte, 64<<10), MaxMessageBytes+1)
	writer := bufio.NewWriter(os.Stdout)
	var writeMu sync.Mutex
	writeMessage := func(message any) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		if err := json.NewEncoder(writer).Encode(message); err != nil {
			return err
		}
		return writer.Flush()
	}
	readRequest := func() (map[string]json.RawMessage, error) {
		if !reader.Scan() {
			if err := reader.Err(); err != nil {
				return nil, err
			}
			return nil, fmt.Errorf("unexpected stdin EOF")
		}
		var message map[string]json.RawMessage
		if err := json.Unmarshal(reader.Bytes(), &message); err != nil {
			return nil, err
		}
		return message, nil
	}
	initialize, err := readRequest()
	if err != nil || string(initialize["method"]) != `"initialize"` {
		return helperFailure("missing initialize request: %v %s", err, initialize["method"])
	}
	var initializeParams struct {
		ClientInfo struct {
			Name    string `json:"name"`
			Title   string `json:"title"`
			Version string `json:"version"`
		} `json:"clientInfo"`
		Capabilities struct {
			ExperimentalAPI                bool     `json:"experimentalApi"`
			RequestAttestation             bool     `json:"requestAttestation"`
			MCPServerOpenAIFormElicitation bool     `json:"mcpServerOpenaiFormElicitation"`
			OptOutNotificationMethods      []string `json:"optOutNotificationMethods"`
		} `json:"capabilities"`
	}
	if err := json.Unmarshal(initialize["params"], &initializeParams); err != nil {
		return helperFailure("decode initialize params: %v", err)
	}
	if initializeParams.ClientInfo.Name != "delegation" ||
		initializeParams.ClientInfo.Title != "Delegation Connector" ||
		initializeParams.ClientInfo.Version != "test" ||
		!initializeParams.Capabilities.ExperimentalAPI ||
		initializeParams.Capabilities.RequestAttestation ||
		initializeParams.Capabilities.MCPServerOpenAIFormElicitation ||
		len(initializeParams.Capabilities.OptOutNotificationMethods) != len(highVolumeNotificationMethods) {
		return helperFailure("unexpected initialize params: %+v", initializeParams)
	}
	for index, method := range highVolumeNotificationMethods {
		if initializeParams.Capabilities.OptOutNotificationMethods[index] != method {
			return helperFailure("unexpected notification opt-outs: %v", initializeParams.Capabilities.OptOutNotificationMethods)
		}
	}
	if err := writeMessage(map[string]any{"id": initialize["id"], "result": map[string]any{"server": "mock"}}); err != nil {
		return helperFailure("write initialize response: %v", err)
	}
	initialized, err := readRequest()
	if err != nil || string(initialized["method"]) != `"initialized"` {
		return helperFailure("missing initialized notification: %v %s", err, initialized["method"])
	}
	if _, hasParams := initialized["params"]; hasParams {
		return helperFailure("initialized notification unexpectedly contains params")
	}
	if mode == "spawn-grandchild-hang" {
		executable, err := os.Executable()
		if err != nil {
			return helperFailure("resolve grandchild executable: %v", err)
		}
		child := exec.Command(executable)
		child.Env = setEnvironment(os.Environ(), helperModeEnvironment, "grandchild-heartbeat")
		if err := child.Start(); err != nil {
			return helperFailure("start grandchild: %v", err)
		}
		if err := child.Process.Release(); err != nil {
			return helperFailure("release grandchild: %v", err)
		}
	}
	if mode == "block-after-prefix" {
		prefix := make([]byte, 4<<10)
		if _, err := io.ReadFull(os.Stdin, prefix); err != nil {
			return helperFailure("read blocked request prefix: %v", err)
		}
		if err := writeMessage(map[string]any{
			"method": "turn/started", "params": map[string]bool{"writeBlocked": true},
		}); err != nil {
			return helperFailure("write blocked request notification: %v", err)
		}
		for {
			time.Sleep(time.Hour)
		}
	}

	var slowID json.RawMessage
	var lateID json.RawMessage
	notifyOnEOF := false
	completeSlowOnEOF := false
	for reader.Scan() {
		var request map[string]json.RawMessage
		if err := json.Unmarshal(reader.Bytes(), &request); err != nil {
			return helperFailure("decode request: %v", err)
		}
		var method string
		if err := json.Unmarshal(request["method"], &method); err != nil {
			return helperFailure("decode method: %v", err)
		}
		switch method {
		case "test/inspect":
			result := map[string]any{
				"arguments": os.Args, "codexHome": os.Getenv("CODEX_HOME"),
				"inheritedPath": os.Getenv("PATH"), "providerValue": os.Getenv(helperValueEnvironment),
				"unsetValue": os.Getenv(helperUnsetEnvironment),
			}
			if err := writeMessage(map[string]any{"id": request["id"], "result": result}); err != nil {
				return helperFailure("write inspect response: %v", err)
			}
		case "test/slow":
			slowID = append(slowID[:0], request["id"]...)
			if err := writeMessage(map[string]any{"method": "turn/started", "params": map[string]bool{"ready": true}}); err != nil {
				return helperFailure("write slow notification: %v", err)
			}
		case "test/slow-complete-on-eof":
			slowID = append(slowID[:0], request["id"]...)
			completeSlowOnEOF = true
			if err := writeMessage(map[string]any{
				"method": "turn/started", "params": map[string]bool{"ready": true},
			}); err != nil {
				return helperFailure("write slow EOF notification: %v", err)
			}
		case "test/fast":
			if err := writeMessage(map[string]any{"id": request["id"], "result": "fast"}); err != nil {
				return helperFailure("write fast response: %v", err)
			}
			if err := writeMessage(map[string]any{"id": slowID, "result": "slow"}); err != nil {
				return helperFailure("write slow response: %v", err)
			}
		case "test/fast-standalone":
			if err := writeMessage(map[string]any{"id": request["id"], "result": "fast-standalone"}); err != nil {
				return helperFailure("write standalone fast response: %v", err)
			}
		case "test/notify":
			if err := writeMessage(map[string]any{"method": "turn/completed", "params": map[string]bool{"ready": true}}); err != nil {
				return helperFailure("write test notification: %v", err)
			}
			if err := writeMessage(map[string]any{"id": request["id"], "result": nil}); err != nil {
				return helperFailure("write notify response: %v", err)
			}
		case "test/late":
			lateID = append(lateID[:0], request["id"]...)
			if err := writeMessage(map[string]any{"method": "thread/status/changed"}); err != nil {
				return helperFailure("write late notification: %v", err)
			}
		case "test/after-late":
			if err := writeMessage(map[string]any{"id": lateID, "result": "ignored"}); err != nil {
				return helperFailure("write late response: %v", err)
			}
			if err := writeMessage(map[string]any{"id": request["id"], "result": "after-late"}); err != nil {
				return helperFailure("write response after late response: %v", err)
			}
		case "test/callback":
			callback := map[string]any{
				"id": "callback-1", "method": "item/commandExecution/requestApproval", "params": map[string]any{},
			}
			if err := writeMessage(callback); err != nil {
				return helperFailure("write callback: %v", err)
			}
			response, err := readRequest()
			if err != nil {
				return helperFailure("read callback rejection: %v", err)
			}
			data, _ := json.Marshal(response)
			if err := os.WriteFile(os.Getenv(helperFileEnvironment), data, 0o600); err != nil {
				return helperFailure("record callback rejection: %v", err)
			}
			time.Sleep(time.Second)
		case "test/exit":
			_, _ = fmt.Fprint(os.Stderr, strings.Repeat("discarded-stderr-", 32)+"stderr-tail-marker")
			return 7
		case "test/overflow":
			for index, method := range []string{"turn/started", "turn/completed"} {
				if err := writeMessage(map[string]any{"method": method, "params": map[string]int{"index": index}}); err != nil {
					return helperFailure("write overflow notification: %v", err)
				}
			}
			if err := writeMessage(map[string]any{"id": request["id"], "result": nil}); err != nil {
				return helperFailure("write overflow response: %v", err)
			}
		case "test/high-volume":
			for index := range 2048 {
				if err := writeMessage(map[string]any{
					"method": "item/agentMessage/delta", "params": map[string]int{"index": index},
				}); err != nil {
					return helperFailure("write high-volume notification: %v", err)
				}
			}
			if err := writeMessage(map[string]any{
				"method": "turn/completed", "params": map[string]string{"turnId": "turn-high-volume"},
			}); err != nil {
				return helperFailure("write high-volume turn completion: %v", err)
			}
			if err := writeMessage(map[string]any{"id": request["id"], "result": nil}); err != nil {
				return helperFailure("write high-volume response: %v", err)
			}
		case "test/complete-on-eof":
			notifyOnEOF = true
		default:
			return helperFailure("unexpected method %q", method)
		}
	}
	if err := reader.Err(); err != nil {
		return helperFailure("read client stream: %v", err)
	}
	if notifyOnEOF {
		if err := writeMessage(map[string]any{
			"method": "turn/completed", "params": map[string]bool{"afterEof": true},
		}); err != nil {
			return helperFailure("write completion after EOF: %v", err)
		}
	}
	if completeSlowOnEOF {
		if err := writeMessage(map[string]any{"id": slowID, "result": nil}); err != nil {
			return helperFailure("write late response after EOF: %v", err)
		}
		if err := writeMessage(map[string]any{
			"method": "turn/completed", "params": map[string]bool{"afterLateResponse": true},
		}); err != nil {
			return helperFailure("write completion after late response: %v", err)
		}
	}
	if mode == "hang-on-eof" || mode == "spawn-grandchild-hang" {
		for {
			time.Sleep(time.Hour)
		}
	}
	return 0
}

func helperFailure(format string, args ...any) int {
	_, _ = fmt.Fprintf(os.Stderr, format+"\n", args...)
	return 2
}
