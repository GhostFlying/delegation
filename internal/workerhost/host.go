package workerhost

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/GhostFlying/delegation/internal/appserver"
	"github.com/GhostFlying/delegation/internal/codexconfig"
	"github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/identity"
	"github.com/GhostFlying/delegation/internal/store"
)

const (
	// Prompts are explicit task input from the root, not an implicit context
	// fragment. The cap keeps each initial/follow-up turn bounded while still
	// allowing a concrete delegated task specification.
	maximumPromptBytes = 8 * 1024
	workerLockCount    = 64
	stateTimeout       = 30 * time.Second
	operationTimeout   = 2 * time.Minute
	workerInstructions = "You are a managed Delegation worker. Work only on the assigned task and report results to your parent. You cannot discover devices, synchronize workspaces, or delegate recursively."
)

var (
	ErrClosed              = errors.New("worker host is closed")
	ErrWorkerFailed        = errors.New("managed worker has failed")
	ErrWorkerInterrupted   = errors.New("managed worker outcome is interrupted")
	ErrWorkerNotIdle       = errors.New("managed worker is not idle")
	ErrWorkerNotRunning    = errors.New("managed worker is not running")
	ErrMCPInjectionBlocked = errors.New("managed worker MCP injection is blocked")
	errClientRecovering    = errors.New("managed app-server is recovering")
)

var hostAuthEnvironment = []string{
	"CODEX_ACCESS_TOKEN",
	"CODEX_API_KEY",
	"OPENAI_API_KEY",
}

type application interface {
	ThreadStart(context.Context, any, any) error
	ThreadResume(context.Context, any, any) error
	MCPServerStatusList(context.Context, any, any) error
	TurnStart(context.Context, any, any) error
	TurnSteer(context.Context, any, any) error
	TurnInterrupt(context.Context, any, any) error
	Notifications() <-chan appserver.Notification
	Done() <-chan struct{}
	Err() error
	Close(context.Context) error
}

type startApplication func(context.Context, appserver.Options) (application, error)

type Options struct {
	ControllerID            string
	DeviceID                string
	PeerConfigPath          string
	DelegationBinary        string
	CodexBinary             string
	CodexEnvironment        map[string]string
	CodexUnsetEnvironment   []string
	ProviderEnvironmentFile string
	CodexHome               string
	WorkspaceRoot           string
	MaxWorkerSlots          int
	CodexConfig             map[string]any
	Store                   *store.PeerStore
	ReportError             func(error)

	startApplication startApplication
}

type SpawnRequest struct {
	TreeID        string
	AgentID       string
	ParentAgentID string
	TaskName      string
	Prompt        string
}

type FollowupRequest struct {
	OperationID string
	Key         store.WorkerKey
	Message     string
}

type SendRequest struct {
	Key       store.WorkerKey
	MessageID string
	Message   string
}

type InterruptRequest struct {
	OperationID string
	Key         store.WorkerKey
}

type StartedTurn struct {
	Worker store.WorkerReservation
}

type OperationResult struct {
	Receipt store.WorkerOperationReceipt
	Worker  store.WorkerReservation
}

type Host struct {
	controllerID             string
	deviceID                 string
	peerConfigPath           string
	delegationBinary         string
	codexBinary              string
	codexEnvironment         map[string]string
	codexUnsetEnvironment    []string
	shellExcludedEnvironment []string
	providerEnvironmentFile  string
	codexHome                string
	workspaceRoot            *os.Root
	maxWorkerSlots           int
	codexConfig              map[string]any
	state                    *store.PeerStore
	startApplication         startApplication
	reportError              func(error)
	completionEvents         chan queuedCompletion
	changes                  chan struct{}
	changesMu                sync.Mutex
	workerRevision           uint64
	completionDrains         map[application]chan struct{}
	deferredCompletions      map[application][]turnCompletedNotification
	applyCompletion          func(turnCompletedNotification) error
	background               sync.WaitGroup
	shutdownOnce             sync.Once
	shutdownDone             chan struct{}
	shutdownErr              error

	operations sync.RWMutex
	workerLock [workerLockCount]sync.Mutex

	clientMu   sync.Mutex
	client     application
	loaded     map[store.WorkerKey]string
	starting   chan struct{}
	recovering chan struct{}
	closed     bool
	fatalErr   error
	done       chan struct{}
	doneOnce   sync.Once
	monitors   sync.WaitGroup
}

func New(ctx context.Context, options Options) (*Host, error) {
	if options.Store == nil {
		return nil, errors.New("worker host store is required")
	}
	if err := identity.ValidateID(options.ControllerID); err != nil {
		return nil, fmt.Errorf("controllerId %w", err)
	}
	if err := identity.ValidateID(options.DeviceID); err != nil {
		return nil, fmt.Errorf("deviceId %w", err)
	}
	for name, path := range map[string]string{
		"peer config":       options.PeerConfigPath,
		"delegation binary": options.DelegationBinary,
		"Codex binary":      options.CodexBinary,
		"Codex home":        options.CodexHome,
		"workspace root":    options.WorkspaceRoot,
	} {
		if !filepath.IsAbs(path) {
			return nil, fmt.Errorf("%s must be an absolute path", name)
		}
	}
	if options.ProviderEnvironmentFile != "" && !filepath.IsAbs(options.ProviderEnvironmentFile) {
		return nil, errors.New("provider environment file must be an absolute path")
	}
	for name, path := range map[string]string{
		"peer config":       options.PeerConfigPath,
		"delegation binary": options.DelegationBinary,
		"Codex binary":      options.CodexBinary,
	} {
		if err := requireRegularFile(path, name); err != nil {
			return nil, err
		}
	}
	codexBinary, err := filepath.EvalSymlinks(options.CodexBinary)
	if err != nil {
		return nil, fmt.Errorf("resolve Codex binary: %w", err)
	}
	providerEnvironmentFile := ""
	if options.ProviderEnvironmentFile != "" {
		if err := requireRegularFile(options.ProviderEnvironmentFile, "provider environment file"); err != nil {
			return nil, err
		}
		resolvedEnvironmentFile, err := filepath.EvalSymlinks(options.ProviderEnvironmentFile)
		if err != nil {
			return nil, fmt.Errorf("resolve provider environment file: %w", err)
		}
		providerEnvironmentFile = resolvedEnvironmentFile
	}
	if options.MaxWorkerSlots < 1 || options.MaxWorkerSlots > config.MaximumWorkerSlots {
		return nil, fmt.Errorf("max worker slots must be from 1 through %d", config.MaximumWorkerSlots)
	}
	if err := config.ValidatePrivateDirectory(options.CodexHome); err != nil {
		return nil, fmt.Errorf("validate managed CODEX_HOME: %w", err)
	}
	if err := codexconfig.ValidateManagedHome(options.CodexHome); err != nil {
		return nil, err
	}
	if err := config.ValidatePrivateDirectory(options.WorkspaceRoot); err != nil {
		return nil, fmt.Errorf("validate managed workspace root: %w", err)
	}
	workspaceRoot, err := filepath.EvalSymlinks(options.WorkspaceRoot)
	if err != nil {
		return nil, fmt.Errorf("resolve worker workspace root: %w", err)
	}
	root, err := os.OpenRoot(workspaceRoot)
	if err != nil {
		return nil, fmt.Errorf("open worker workspace root: %w", err)
	}
	reportError := options.ReportError
	if reportError == nil {
		reportError = func(error) {}
	}
	start := options.startApplication
	if start == nil {
		start = func(ctx context.Context, options appserver.Options) (application, error) {
			return appserver.Start(ctx, options)
		}
	}
	credentialEnvironment := codexconfig.CredentialEnvironmentVariables(options.CodexConfig)
	appServerUnsetEnvironment := append(
		append([]string(nil), options.CodexUnsetEnvironment...),
		codexconfig.EnvironmentVariable, "CODEX_SQLITE_HOME",
	)
	codexEnvironment := cloneStringMap(options.CodexEnvironment)
	removeEnvironmentName(codexEnvironment, "CODEX_SQLITE_HOME")
	shellExcludedEnvironment := append(
		append([]string(nil), hostAuthEnvironment...),
		codexconfig.EnvironmentVariable,
	)
	shellExcludedEnvironment = append(
		shellExcludedEnvironment,
		credentialEnvironment...,
	)
	host := &Host{
		controllerID: options.ControllerID, deviceID: options.DeviceID,
		peerConfigPath: options.PeerConfigPath, delegationBinary: options.DelegationBinary,
		codexBinary: codexBinary, codexHome: options.CodexHome,
		codexEnvironment:         codexEnvironment,
		codexUnsetEnvironment:    uniqueEnvironmentNames(appServerUnsetEnvironment),
		shellExcludedEnvironment: uniqueEnvironmentNames(shellExcludedEnvironment),
		providerEnvironmentFile:  providerEnvironmentFile,
		workspaceRoot:            root, maxWorkerSlots: options.MaxWorkerSlots,
		codexConfig: codexconfig.Clone(options.CodexConfig), state: options.Store,
		startApplication: start, reportError: reportError,
		loaded:              make(map[store.WorkerKey]string),
		completionEvents:    make(chan queuedCompletion, options.MaxWorkerSlots),
		changes:             make(chan struct{}, 1),
		completionDrains:    make(map[application]chan struct{}),
		deferredCompletions: make(map[application][]turnCompletedNotification),
		done:                make(chan struct{}),
		shutdownDone:        make(chan struct{}),
	}
	host.applyCompletion = host.completeTurn
	if err := host.validateStoredAuthority(ctx); err != nil {
		_ = root.Close()
		return nil, err
	}
	if err := host.seedWorkerRevision(ctx); err != nil {
		_ = root.Close()
		return nil, err
	}
	if _, err := host.recordWorkerRecovery(
		host.state.RecoverWorkers(ctx, host.controllerID, host.deviceID, time.Now()),
	); err != nil {
		_ = root.Close()
		return nil, fmt.Errorf("recover managed workers: %w", err)
	}
	host.background.Add(1)
	go host.processCompletions()
	return host, nil
}

func cloneStringMap(source map[string]string) map[string]string {
	if source == nil {
		return nil
	}
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

func removeEnvironmentName(environment map[string]string, name string) {
	for key := range environment {
		if key == name || runtime.GOOS == "windows" && strings.EqualFold(key, name) {
			delete(environment, key)
		}
	}
}

func uniqueEnvironmentNames(names []string) []string {
	if runtime.GOOS != "windows" {
		slices.Sort(names)
		return slices.Compact(names)
	}
	slices.SortFunc(names, func(left, right string) int {
		return strings.Compare(strings.ToUpper(left), strings.ToUpper(right))
	})
	return slices.CompactFunc(names, strings.EqualFold)
}

// Done closes when the host is closed or can no longer recover authoritative worker state.
func (h *Host) Done() <-chan struct{} {
	return h.done
}

// Err returns the terminal host error, if any.
func (h *Host) Err() error {
	h.clientMu.Lock()
	defer h.clientMu.Unlock()
	return h.fatalErr
}

func (h *Host) fail(err error) {
	if err == nil {
		return
	}
	h.clientMu.Lock()
	if h.fatalErr == nil {
		h.fatalErr = err
		h.doneOnce.Do(func() { close(h.done) })
	}
	h.clientMu.Unlock()
}

func (h *Host) Spawn(ctx context.Context, request SpawnRequest) (StartedTurn, error) {
	principal := control.NewWorkerPrincipal(
		h.controllerID, request.TreeID, request.AgentID, request.ParentAgentID, h.deviceID,
	)
	if err := principal.Validate(); err != nil {
		return StartedTurn{}, fmt.Errorf("worker identity: %w", err)
	}
	if err := validatePrompt(request.Prompt); err != nil {
		return StartedTurn{}, err
	}
	key := store.WorkerKey{
		ControllerID: h.controllerID,
		TreeID:       request.TreeID,
		AgentID:      request.AgentID,
	}
	for {
		release, err := h.acquireOperation(ctx)
		if err != nil {
			return StartedTurn{}, err
		}
		lock := h.lockFor(key)
		lock.Lock()
		result, recovery, err := h.spawnLocked(ctx, request, key)
		lock.Unlock()
		release()
		if waitErr := h.awaitRecovery(ctx, recovery); waitErr != nil {
			return result, errors.Join(err, waitErr)
		}
		if !errors.Is(err, errClientRecovering) {
			return result, err
		}
		if err := h.waitForCurrentRecovery(ctx); err != nil {
			return StartedTurn{}, err
		}
	}
}

func (h *Host) spawnLocked(
	ctx context.Context,
	request SpawnRequest,
	key store.WorkerKey,
) (StartedTurn, <-chan struct{}, error) {
	workspacePath := h.workspacePath(key)
	desired := store.WorkerReservation{
		WorkerKey: key, ParentAgentID: request.ParentAgentID, DeviceID: h.deviceID,
		TaskName: request.TaskName, PromptDigest: promptDigest(request.Prompt),
		WorkspacePath: workspacePath, ProfileVersion: workerProfileVersion,
	}
	var worker store.WorkerReservation
	if existing, err := h.state.GetWorker(ctx, key); err == nil {
		if !sameReservation(existing, desired) {
			return StartedTurn{}, nil, store.ErrWorkerReservationConflict
		}
		if existing.Status == store.WorkerFailed {
			return StartedTurn{Worker: existing}, nil, fmt.Errorf("%w: %s", ErrWorkerFailed, existing.FailureCode)
		}
		switch existing.Status {
		case store.WorkerReserved, store.WorkerPending:
			worker = existing
		case store.WorkerRunning, store.WorkerIdle:
			if err := h.prepareWorkspace(key); err != nil {
				return StartedTurn{Worker: existing}, nil, err
			}
			return StartedTurn{Worker: existing}, nil, nil
		case store.WorkerInterrupted:
			return StartedTurn{Worker: existing}, nil, fmt.Errorf(
				"%w: explicit follow-up is required before retrying work",
				ErrWorkerInterrupted,
			)
		case store.WorkerStarting, store.WorkerPreflight, store.WorkerReady:
			return StartedTurn{Worker: existing}, nil, fmt.Errorf(
				"%w: worker remains in %s without an active operation",
				store.ErrWorkerTransition,
				existing.Status,
			)
		case store.WorkerFailed:
			panic("failed worker handled above")
		default:
			return StartedTurn{Worker: existing}, nil, fmt.Errorf("unknown worker status %q", existing.Status)
		}
	} else if !errors.Is(err, store.ErrNotFound) {
		return StartedTurn{}, nil, err
	}
	if err := h.prepareWorkspace(key); err != nil {
		return StartedTurn{}, nil, err
	}
	operationContext, cancel, err := detachedOperationContext(ctx)
	if err != nil {
		return StartedTurn{Worker: worker}, nil, err
	}
	defer cancel()
	client, err := h.ensureClient(operationContext)
	if err != nil {
		return StartedTurn{Worker: worker}, nil, err
	}
	if worker.Status == "" {
		worker, err = h.recordWorkerChange(
			h.state.ReserveWorkerStart(operationContext, desired, h.maxWorkerSlots, time.Now()),
		)
		if err != nil {
			return StartedTurn{}, nil, err
		}
	} else {
		worker, err = h.recordWorkerChange(
			h.state.BeginWorkerStart(operationContext, worker.WorkerKey, h.maxWorkerSlots, time.Now()),
		)
		if err != nil {
			return StartedTurn{Worker: worker}, nil, err
		}
	}
	if worker.CodexThreadID != "" {
		return h.retryPendingThread(operationContext, client, worker, request.Prompt)
	}
	return h.startNewThread(operationContext, client, worker, request.Prompt)
}

func (h *Host) followupLocked(
	ctx context.Context,
	request FollowupRequest,
) (StartedTurn, <-chan struct{}, error) {
	worker, err := h.state.GetWorker(ctx, request.Key)
	if err != nil {
		return StartedTurn{}, nil, err
	}
	if worker.DeviceID != h.deviceID {
		return StartedTurn{}, nil, errors.New("worker belongs to another device")
	}
	if worker.Status != store.WorkerIdle && worker.Status != store.WorkerInterrupted {
		return StartedTurn{Worker: worker}, nil, fmt.Errorf("%w: status is %s", ErrWorkerNotIdle, worker.Status)
	}
	if err := h.prepareWorkspace(worker.WorkerKey); err != nil {
		return StartedTurn{Worker: worker}, nil, err
	}
	operationContext, cancel, err := detachedOperationContext(ctx)
	if err != nil {
		return StartedTurn{Worker: worker}, nil, err
	}
	defer cancel()
	client, err := h.ensureClient(operationContext)
	if err != nil {
		return StartedTurn{Worker: worker}, nil, err
	}
	worker, err = h.recordWorkerChange(
		h.state.BeginWorkerStart(operationContext, worker.WorkerKey, h.maxWorkerSlots, time.Now()),
	)
	if err != nil {
		return StartedTurn{Worker: worker}, nil, err
	}
	if h.isLoaded(client, worker.WorkerKey, worker.CodexThreadID) {
		worker, err = h.recordWorkerChange(
			h.state.AttachWorkerThread(operationContext, worker.WorkerKey, worker.CodexThreadID, time.Now()),
		)
		if err != nil {
			return StartedTurn{Worker: worker}, h.retireClient(client, err), err
		}
		if err := h.verifyWorkerMCP(operationContext, client, worker.CodexThreadID); err != nil {
			if errors.Is(err, appserver.ErrRequestNotWritten) {
				restored, restoreErr := h.restoreWorkerAfterUnsent(worker, store.WorkerIdle, err)
				return StartedTurn{Worker: restored}, nil, restoreErr
			}
			if h.shouldRetire(client, err) {
				return StartedTurn{Worker: worker}, h.retireClient(client, err), err
			}
			recovery, failureErr := h.failWorkerMCP(client, worker.WorkerKey, err)
			return StartedTurn{Worker: worker}, recovery, failureErr
		}
		worker, err = h.recordWorkerChange(
			h.state.MarkWorkerReady(operationContext, worker.WorkerKey, time.Now()),
		)
		if err != nil {
			return StartedTurn{Worker: worker}, h.retireClient(client, err), err
		}
	} else {
		worker, recovery, err := h.resumeThread(operationContext, client, worker, store.WorkerIdle)
		if err != nil {
			return StartedTurn{Worker: worker}, recovery, err
		}
	}
	return h.startTurn(operationContext, client, worker, request.Message, store.WorkerIdle)
}

func detachedOperationContext(ctx context.Context) (context.Context, context.CancelFunc, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	operationContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), operationTimeout)
	return operationContext, cancel, nil
}

func requireRegularFile(path, description string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("inspect %s: %w", description, err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("%s must be a regular file", description)
	}
	return nil
}

func validatePrompt(prompt string) error {
	if strings.TrimSpace(prompt) == "" || len(prompt) > maximumPromptBytes || !utf8.ValidString(prompt) {
		return fmt.Errorf("worker prompt must contain from 1 through %d bytes of valid UTF-8", maximumPromptBytes)
	}
	return nil
}

func promptDigest(prompt string) string {
	digest := sha256.Sum256([]byte(prompt))
	return hex.EncodeToString(digest[:])
}

func sameReservation(stored, desired store.WorkerReservation) bool {
	return stored.WorkerKey == desired.WorkerKey &&
		stored.ParentAgentID == desired.ParentAgentID &&
		stored.DeviceID == desired.DeviceID &&
		stored.TaskName == desired.TaskName &&
		stored.PromptDigest == desired.PromptDigest &&
		stored.WorkspacePath == desired.WorkspacePath &&
		stored.ProfileVersion == desired.ProfileVersion
}
