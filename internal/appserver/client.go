package appserver

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/GhostFlying/delegation/internal/buildinfo"
)

const (
	// MaxMessageBytes bounds each JSONL message in either direction.
	MaxMessageBytes = 16 << 20

	defaultHandshakeTimeout  = 30 * time.Second
	defaultCloseTimeout      = 10 * time.Second
	defaultWriteTimeout      = 10 * time.Second
	defaultStderrLimit       = 64 << 10
	defaultNotificationQueue = 512
	defaultMaxPendingCalls   = 256
	maximumStderrLimit       = 1 << 20
	maximumNotificationQueue = 8192
	maximumPendingCalls      = 4096
	maximumAbandonedCalls    = 4096
	callbackRejectTimeout    = 2 * time.Second
	callbackDrainTimeout     = 250 * time.Millisecond
)

var highVolumeNotificationMethods = []string{
	"command/exec/outputDelta",
	"item/agentMessage/delta",
	"item/commandExecution/outputDelta",
	"item/fileChange/outputDelta",
	"item/fileChange/patchUpdated",
	"item/mcpToolCall/progress",
	"item/plan/delta",
	"item/reasoning/summaryPartAdded",
	"item/reasoning/summaryTextDelta",
	"item/reasoning/textDelta",
	"process/outputDelta",
	"thread/realtime/outputAudio/delta",
	"thread/realtime/transcript/delta",
}

var (
	ErrClosed               = errors.New("app-server client is closed")
	ErrBusy                 = errors.New("app-server client has too many pending calls")
	ErrMessageTooLarge      = errors.New("app-server JSON message exceeds 16 MiB")
	ErrNotificationOverflow = errors.New("app-server notification queue is full")
	ErrCloseTimeout         = errors.New("timed out closing app-server")
)

// Options defines one isolated app-server child process.
type Options struct {
	Binary             string
	CodexHome          string
	Environment        map[string]string
	ClientVersion      string
	HandshakeTimeout   time.Duration
	CloseTimeout       time.Duration
	StderrLimit        int
	NotificationBuffer int
	MaxPendingCalls    int
}

// ProcessError reports an unexpected child exit without including stderr in its Error string.
type ProcessError struct {
	Err        error
	StderrTail []byte
}

func (e *ProcessError) Error() string {
	if e.Err == nil {
		return "app-server exited unexpectedly"
	}
	return fmt.Sprintf("app-server exited unexpectedly: %v", e.Err)
}

func (e *ProcessError) Unwrap() error {
	return e.Err
}

// UnexpectedServerRequestError reports a callback that the managed worker profile cannot service.
type UnexpectedServerRequestError struct {
	Method string
}

func (e *UnexpectedServerRequestError) Error() string {
	return fmt.Sprintf("app-server sent unsupported client callback %q", e.Method)
}

type pendingCall struct {
	result chan response
}

type Client struct {
	command       *exec.Cmd
	stdin         io.WriteCloser
	stdout        io.ReadCloser
	closeTimeout  time.Duration
	maxPending    int
	stderr        *tailBuffer
	stderrDone    chan struct{}
	processExited chan struct{}

	nextID  atomic.Uint64
	closing atomic.Bool

	writeGate    chan struct{}
	pendingMu    sync.Mutex
	pending      map[uint64]pendingCall
	abandoned    map[uint64]struct{}
	abandonOrder []uint64

	notifications chan Notification
	fatal         chan error
	done          chan struct{}
	terminateOnce sync.Once
	stdinOnce     sync.Once
	killOnce      sync.Once

	errMu       sync.Mutex
	terminalErr error
	waitErr     error
}

// Start launches and initializes a long-lived Codex app-server over stdio.
func Start(ctx context.Context, options Options) (*Client, error) {
	validated, err := validateOptions(options)
	if err != nil {
		return nil, err
	}
	command := exec.Command(validated.Binary, "app-server", "--listen", "stdio://", "--session-source", "app-server")
	command.Env = buildEnvironment(validated.Environment, validated.CodexHome)
	stdin, err := command.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("open app-server stdin: %w", err)
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return nil, fmt.Errorf("open app-server stdout: %w", err)
	}
	stderr, err := command.StderrPipe()
	if err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		return nil, fmt.Errorf("open app-server stderr: %w", err)
	}
	client := &Client{
		command: command, stdin: stdin, stdout: stdout, closeTimeout: validated.CloseTimeout,
		maxPending: validated.MaxPendingCalls, stderr: newTailBuffer(validated.StderrLimit),
		stderrDone: make(chan struct{}), processExited: make(chan struct{}), writeGate: make(chan struct{}, 1),
		pending: map[uint64]pendingCall{}, abandoned: map[uint64]struct{}{},
		notifications: make(chan Notification, validated.NotificationBuffer), fatal: make(chan error, 1),
		done: make(chan struct{}),
	}
	client.writeGate <- struct{}{}
	if err := command.Start(); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		_ = stderr.Close()
		return nil, fmt.Errorf("start app-server: %w", err)
	}
	go client.drainStderr(stderr)
	go client.readLoop()
	go client.waitLoop()

	handshakeCtx, cancel := context.WithTimeout(ctx, validated.HandshakeTimeout)
	defer cancel()
	initialize := map[string]any{
		"clientInfo": map[string]any{
			"name": "delegation", "title": "Delegation Connector", "version": validated.ClientVersion,
		},
		"capabilities": map[string]any{
			"experimentalApi": false, "requestAttestation": false,
			"mcpServerOpenaiFormElicitation": false,
			"optOutNotificationMethods":      highVolumeNotificationMethods,
		},
	}
	if err := client.Call(handshakeCtx, "initialize", initialize, nil); err != nil {
		client.fail(fmt.Errorf("initialize app-server: %w", err))
		_ = client.Close(context.Background())
		return nil, fmt.Errorf("initialize app-server: %w", err)
	}
	if err := client.Notify(handshakeCtx, "initialized", nil); err != nil {
		client.fail(fmt.Errorf("notify app-server initialized: %w", err))
		_ = client.Close(context.Background())
		return nil, fmt.Errorf("notify app-server initialized: %w", err)
	}
	return client, nil
}

// Call sends one request and decodes its result. Concurrent calls are supported.
func (c *Client) Call(ctx context.Context, method string, params, result any) error {
	if method == "" {
		return errors.New("app-server RPC method is empty")
	}
	id := c.nextID.Add(1)
	data, err := marshalRequest(id, method, params)
	if err != nil {
		return fmt.Errorf("encode app-server %s request: %w", method, err)
	}
	if len(data) > MaxMessageBytes {
		return ErrMessageTooLarge
	}
	release, err := c.acquireWriter(ctx)
	if err != nil {
		return err
	}
	pending := pendingCall{result: make(chan response, 1)}
	if err := c.addPending(id, pending); err != nil {
		release()
		return err
	}
	if err := c.writeLocked(ctx, data); err != nil {
		c.removePending(id, false)
		c.fail(fmt.Errorf("write app-server %s request: %w", method, err))
		release()
		return err
	}
	release()

	select {
	case response := <-pending.result:
		if response.err != nil {
			return response.err
		}
		if result == nil {
			return nil
		}
		if err := json.Unmarshal(response.result, result); err != nil {
			return fmt.Errorf("decode app-server %s result: %w", method, err)
		}
		return nil
	case <-ctx.Done():
		c.removePending(id, true)
		return ctx.Err()
	case <-c.done:
		c.removePending(id, false)
		if err := c.Err(); err != nil {
			return err
		}
		return ErrClosed
	}
}

// Notify sends one client notification.
func (c *Client) Notify(ctx context.Context, method string, params any) error {
	if method == "" {
		return errors.New("app-server notification method is empty")
	}
	data, err := marshalNotification(method, params)
	if err != nil {
		return fmt.Errorf("encode app-server %s notification: %w", method, err)
	}
	if len(data) > MaxMessageBytes {
		return ErrMessageTooLarge
	}
	release, err := c.acquireWriter(ctx)
	if err != nil {
		return err
	}
	if err := c.writeLocked(ctx, data); err != nil {
		c.fail(fmt.Errorf("write app-server %s notification: %w", method, err))
		release()
		return err
	}
	release()
	return nil
}

func (c *Client) ThreadStart(ctx context.Context, params, result any) error {
	return c.Call(ctx, MethodThreadStart, params, result)
}

func (c *Client) ThreadResume(ctx context.Context, params, result any) error {
	return c.Call(ctx, MethodThreadResume, params, result)
}

func (c *Client) MCPServerStatusList(ctx context.Context, params, result any) error {
	return c.Call(ctx, MethodMCPServerStatusList, params, result)
}

func (c *Client) TurnStart(ctx context.Context, params, result any) error {
	return c.Call(ctx, MethodTurnStart, params, result)
}

func (c *Client) TurnSteer(ctx context.Context, params, result any) error {
	return c.Call(ctx, MethodTurnSteer, params, result)
}

func (c *Client) TurnInterrupt(ctx context.Context, params, result any) error {
	return c.Call(ctx, MethodTurnInterrupt, params, result)
}

// Notifications returns the bounded stream of lifecycle notifications needed
// to track managed threads, turns, errors, and MCP startup. Other server
// notifications are drained and intentionally discarded.
func (c *Client) Notifications() <-chan Notification {
	return c.notifications
}

// Fatal returns a channel that receives the first terminal protocol or process error.
func (c *Client) Fatal() <-chan error {
	return c.fatal
}

// Done closes when the client becomes unusable or Close starts.
func (c *Client) Done() <-chan struct{} {
	return c.done
}

// Err returns the terminal error, or nil after an intentional Close.
func (c *Client) Err() error {
	c.errMu.Lock()
	defer c.errMu.Unlock()
	return c.terminalErr
}

// StderrTail returns a copy of the bounded diagnostic tail.
func (c *Client) StderrTail() []byte {
	return c.stderr.snapshot()
}

// Close closes stdin and waits for the child, killing it when the close deadline expires.
func (c *Client) Close(ctx context.Context) error {
	c.closing.Store(true)
	c.terminate(nil)
	c.closeInput()
	waitCtx, cancel := context.WithTimeout(ctx, c.closeTimeout)
	defer cancel()
	select {
	case <-c.processExited:
		c.errMu.Lock()
		waitErr := c.waitErr
		c.errMu.Unlock()
		if waitErr != nil {
			return fmt.Errorf("close app-server: %w", waitErr)
		}
		return nil
	case <-waitCtx.Done():
		c.killProcess()
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("close app-server: %w", err)
		}
		return fmt.Errorf("%w: %v", ErrCloseTimeout, waitCtx.Err())
	}
}

func (c *Client) readLoop() {
	reader := bufio.NewReaderSize(c.stdout, 64<<10)
	for {
		line, err := readBoundedLine(reader, MaxMessageBytes)
		if err != nil {
			if errors.Is(err, io.EOF) {
				if c.closing.Load() {
					return
				}
				select {
				case <-c.processExited:
					return
				case <-time.After(100 * time.Millisecond):
					c.fail(errors.New("app-server stdout closed unexpectedly"))
					return
				}
			}
			c.fail(fmt.Errorf("read app-server message: %w", err))
			return
		}
		if len(line) == 0 {
			c.fail(errors.New("app-server emitted an empty JSON line"))
			return
		}
		message, err := decodeMessage(line)
		if err != nil {
			c.fail(err)
			return
		}
		switch {
		case message.isResponse:
			if err := c.complete(message); err != nil {
				c.fail(err)
				return
			}
		case message.isNotification:
			if !isLifecycleNotification(message.method) {
				continue
			}
			select {
			case c.notifications <- Notification{Method: message.method, Params: cloneRaw(message.params)}:
			case <-c.done:
				return
			default:
				c.fail(ErrNotificationOverflow)
				return
			}
		case message.isRequest:
			data, marshalErr := marshalMethodNotFound(message.id)
			if marshalErr == nil {
				writeCtx, cancel := context.WithTimeout(context.Background(), callbackRejectTimeout)
				marshalErr = c.write(writeCtx, data)
				cancel()
			}
			requestErr := &UnexpectedServerRequestError{Method: message.method}
			if marshalErr != nil {
				c.fail(errors.Join(requestErr, fmt.Errorf("reject app-server callback: %w", marshalErr)))
			} else {
				c.failAfterCallback(requestErr)
			}
			return
		}
	}
}

func (c *Client) complete(message decodedMessage) error {
	c.pendingMu.Lock()
	pending, found := c.pending[message.responseID]
	if found {
		delete(c.pending, message.responseID)
		c.pendingMu.Unlock()
		if message.rpcError != nil {
			pending.result <- response{err: message.rpcError}
		} else {
			pending.result <- response{result: cloneRaw(message.result)}
		}
		return nil
	}
	if _, abandoned := c.abandoned[message.responseID]; abandoned {
		delete(c.abandoned, message.responseID)
		c.pendingMu.Unlock()
		return nil
	}
	c.pendingMu.Unlock()
	return fmt.Errorf("app-server response references unknown request ID %d", message.responseID)
}

func (c *Client) addPending(id uint64, pending pendingCall) error {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	select {
	case <-c.done:
		if err := c.Err(); err != nil {
			return err
		}
		return ErrClosed
	default:
	}
	if len(c.pending) >= c.maxPending {
		return ErrBusy
	}
	c.pending[id] = pending
	return nil
}

func (c *Client) removePending(id uint64, abandon bool) {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	if _, found := c.pending[id]; !found {
		return
	}
	delete(c.pending, id)
	if !abandon {
		return
	}
	c.abandoned[id] = struct{}{}
	c.abandonOrder = append(c.abandonOrder, id)
	if len(c.abandonOrder) > maximumAbandonedCalls {
		oldest := c.abandonOrder[0]
		c.abandonOrder = c.abandonOrder[1:]
		delete(c.abandoned, oldest)
	}
}

func (c *Client) acquireWriter(ctx context.Context) (func(), error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.done:
		if err := c.Err(); err != nil {
			return nil, err
		}
		return nil, ErrClosed
	case <-c.writeGate:
		return func() { c.writeGate <- struct{}{} }, nil
	}
}

func (c *Client) write(ctx context.Context, data []byte) error {
	release, err := c.acquireWriter(ctx)
	if err != nil {
		return err
	}
	err = c.writeLocked(ctx, data)
	release()
	return err
}

func (c *Client) writeLocked(ctx context.Context, data []byte) error {
	if len(data) > MaxMessageBytes {
		return ErrMessageTooLarge
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	line := make([]byte, len(data)+1)
	copy(line, data)
	line[len(data)] = '\n'

	writeDone := make(chan error, 1)
	go func() {
		writeDone <- writeAll(c.stdin, line)
	}()
	writeCtx, cancel := context.WithTimeout(ctx, defaultWriteTimeout)
	defer cancel()
	select {
	case err := <-writeDone:
		return c.normalizeWriteResult(err)
	case <-writeCtx.Done():
		select {
		case err := <-writeDone:
			return c.normalizeWriteResult(err)
		default:
		}
		// Anonymous child-process pipes do not reliably support write deadlines on
		// every platform. Closing the pipe and killing the owned child guarantees
		// that the blocked Write is interrupted without depending on that support.
		c.closeInput()
		c.killProcess()
		return writeCtx.Err()
	case <-c.done:
		select {
		case err := <-writeDone:
			return c.normalizeWriteResult(err)
		default:
		}
		if err := c.Err(); err != nil {
			return err
		}
		return ErrClosed
	}
}

func (c *Client) normalizeWriteResult(err error) error {
	if err == nil {
		return nil
	}
	select {
	case <-c.done:
		if terminalErr := c.Err(); terminalErr != nil {
			return terminalErr
		}
		return ErrClosed
	default:
		return err
	}
}

func writeAll(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		written, err := writer.Write(data)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		data = data[written:]
	}
	return nil
}

func (c *Client) drainStderr(stderr io.Reader) {
	defer close(c.stderrDone)
	_, _ = io.Copy(c.stderr, stderr)
}

func (c *Client) waitLoop() {
	err := c.command.Wait()
	<-c.stderrDone
	c.errMu.Lock()
	c.waitErr = err
	c.errMu.Unlock()
	close(c.processExited)
	if !c.closing.Load() {
		c.fail(&ProcessError{Err: err, StderrTail: c.stderr.snapshot()})
	}
}

func (c *Client) fail(err error) {
	c.failWithKillPolicy(err, true)
}

func (c *Client) failAfterCallback(err error) {
	c.failWithKillPolicy(err, false)
}

func (c *Client) failWithKillPolicy(err error, killImmediately bool) {
	c.terminateOnce.Do(func() {
		c.errMu.Lock()
		c.terminalErr = err
		c.errMu.Unlock()
		c.fatal <- err
		close(c.done)
		c.failPending(err)
		c.closeInput()
		if killImmediately {
			c.killProcess()
			return
		}
		go func() {
			select {
			case <-c.processExited:
			case <-time.After(callbackDrainTimeout):
				c.killProcess()
			}
		}()
	})
}

func (c *Client) terminate(err error) {
	c.terminateOnce.Do(func() {
		c.errMu.Lock()
		c.terminalErr = err
		c.errMu.Unlock()
		close(c.done)
		if err == nil {
			c.failPending(ErrClosed)
		} else {
			c.fatal <- err
			c.failPending(err)
		}
	})
}

func (c *Client) failPending(err error) {
	c.pendingMu.Lock()
	pending := make([]pendingCall, 0, len(c.pending))
	for id, call := range c.pending {
		delete(c.pending, id)
		pending = append(pending, call)
	}
	c.pendingMu.Unlock()
	for _, call := range pending {
		call.result <- response{err: err}
	}
}

func (c *Client) closeInput() {
	c.stdinOnce.Do(func() { _ = c.stdin.Close() })
}

func (c *Client) killProcess() {
	c.killOnce.Do(func() {
		if c.command.Process != nil {
			_ = c.command.Process.Kill()
		}
	})
}

func readBoundedLine(reader *bufio.Reader, limit int) ([]byte, error) {
	var line []byte
	for {
		fragment, prefix, err := reader.ReadLine()
		if len(fragment) > limit-len(line) {
			return nil, ErrMessageTooLarge
		}
		line = append(line, fragment...)
		if err != nil {
			if errors.Is(err, io.EOF) && len(line) > 0 {
				return line, nil
			}
			return nil, err
		}
		if !prefix {
			return line, nil
		}
	}
}

func validateOptions(options Options) (Options, error) {
	if options.Binary == "" || !filepath.IsAbs(options.Binary) {
		return Options{}, errors.New("app-server binary must be an absolute path")
	}
	info, err := os.Stat(options.Binary)
	if err != nil {
		return Options{}, fmt.Errorf("inspect app-server binary: %w", err)
	}
	if !info.Mode().IsRegular() {
		return Options{}, errors.New("app-server binary must be a regular file")
	}
	if options.CodexHome == "" || !filepath.IsAbs(options.CodexHome) {
		return Options{}, errors.New("app-server CODEX_HOME must be an absolute path")
	}
	homeInfo, err := os.Stat(options.CodexHome)
	if err != nil {
		return Options{}, fmt.Errorf("inspect app-server CODEX_HOME: %w", err)
	}
	if !homeInfo.IsDir() {
		return Options{}, errors.New("app-server CODEX_HOME must be a directory")
	}
	for key, value := range options.Environment {
		if key == "" || strings.ContainsAny(key, "=\x00") || strings.ContainsRune(value, '\x00') {
			return Options{}, fmt.Errorf("app-server environment contains invalid key %q", key)
		}
		if environmentKeyEqual(key, "CODEX_HOME") && !samePath(value, options.CodexHome) {
			return Options{}, errors.New("app-server Environment CODEX_HOME conflicts with CodexHome")
		}
	}
	if options.ClientVersion == "" {
		options.ClientVersion = buildinfo.Version
	}
	if options.HandshakeTimeout == 0 {
		options.HandshakeTimeout = defaultHandshakeTimeout
	}
	if options.HandshakeTimeout < time.Millisecond || options.HandshakeTimeout > 5*time.Minute {
		return Options{}, errors.New("app-server handshake timeout is invalid")
	}
	if options.CloseTimeout == 0 {
		options.CloseTimeout = defaultCloseTimeout
	}
	if options.CloseTimeout < time.Millisecond || options.CloseTimeout > time.Minute {
		return Options{}, errors.New("app-server close timeout is invalid")
	}
	if options.StderrLimit == 0 {
		options.StderrLimit = defaultStderrLimit
	}
	if options.StderrLimit < 1 || options.StderrLimit > maximumStderrLimit {
		return Options{}, errors.New("app-server stderr limit is invalid")
	}
	if options.NotificationBuffer == 0 {
		options.NotificationBuffer = defaultNotificationQueue
	}
	if options.NotificationBuffer < 1 || options.NotificationBuffer > maximumNotificationQueue {
		return Options{}, errors.New("app-server notification buffer is invalid")
	}
	if options.MaxPendingCalls == 0 {
		options.MaxPendingCalls = defaultMaxPendingCalls
	}
	if options.MaxPendingCalls < 1 || options.MaxPendingCalls > maximumPendingCalls {
		return Options{}, errors.New("app-server pending call limit is invalid")
	}
	return options, nil
}

func buildEnvironment(overrides map[string]string, codexHome string) []string {
	environment := append([]string(nil), os.Environ()...)
	keys := make([]string, 0, len(overrides))
	for key := range overrides {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		environment = setEnvironment(environment, key, overrides[key])
	}
	return setEnvironment(environment, "CODEX_HOME", codexHome)
}

func setEnvironment(environment []string, key, value string) []string {
	prefix := key + "="
	filtered := slices.DeleteFunc(environment, func(entry string) bool {
		entryKey, _, found := strings.Cut(entry, "=")
		return found && environmentKeyEqual(entryKey, key)
	})
	return append(filtered, prefix+value)
}

func environmentKeyEqual(left, right string) bool {
	if runtime.GOOS == "windows" {
		return strings.EqualFold(left, right)
	}
	return left == right
}

func samePath(left, right string) bool {
	left = filepath.Clean(left)
	right = filepath.Clean(right)
	if runtime.GOOS == "windows" {
		return strings.EqualFold(left, right)
	}
	return left == right
}
