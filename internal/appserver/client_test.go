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
)

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
	expectedArguments := []string{filepath.Clean(executable), "app-server", "--listen", "stdio://", "--session-source", "app-server"}
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
	waitForFile(t, observed)
	data, err := os.ReadFile(observed)
	if err != nil {
		t.Fatal(err)
	}
	var rejection struct {
		ID    string `json:"id"`
		Error struct {
			Code int `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(data, &rejection); err != nil {
		t.Fatal(err)
	}
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
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("Close() took %s", elapsed)
	}
	select {
	case <-client.processExited:
	case <-time.After(2 * time.Second):
		t.Fatal("helper process survived Close timeout kill")
	}
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
	client := startHelperClient(t, "exit-with-stderr", Options{StderrLimit: 64})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := client.Call(ctx, "test/exit", nil, nil)
	var processErr *ProcessError
	if !errors.As(err, &processErr) {
		t.Fatalf("Call() error = %T %v, want ProcessError", err, err)
	}
	if len(processErr.StderrTail) != 64 || !bytes.HasSuffix(processErr.StderrTail, []byte("stderr-tail-marker")) {
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
		Binary: executable, CodexHome: t.TempDir(), Environment: map[string]string{"CODEX_HOME": t.TempDir()},
	})
	if err == nil || !strings.Contains(err.Error(), "conflicts") {
		t.Fatalf("validateOptions() error = %v", err)
	}
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
		Binary: executable, CodexHome: codexHome, Environment: environment,
		ClientVersion: "test", HandshakeTimeout: 2 * time.Second,
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
		case notification := <-client.Notifications():
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

func runHelperProcess() int {
	mode := os.Getenv(helperModeEnvironment)
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
		initializeParams.Capabilities.ExperimentalAPI ||
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
			}
			if err := writeMessage(map[string]any{"id": request["id"], "result": result}); err != nil {
				return helperFailure("write inspect response: %v", err)
			}
		case "test/slow":
			slowID = append(slowID[:0], request["id"]...)
			if err := writeMessage(map[string]any{"method": "turn/started", "params": map[string]bool{"ready": true}}); err != nil {
				return helperFailure("write slow notification: %v", err)
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
		default:
			return helperFailure("unexpected method %q", method)
		}
	}
	if err := reader.Err(); err != nil {
		return helperFailure("read client stream: %v", err)
	}
	if mode == "hang-on-eof" {
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
