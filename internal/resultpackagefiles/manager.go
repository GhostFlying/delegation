package resultpackagefiles

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/identity"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
)

const (
	outboxDirectoryName = ".delegation-result-outbox-v2"
	inboxDirectoryName  = ".delegation-result-inbox-v2"
	packageLockCount    = 64
)

type Options struct {
	ControllerID  string
	DeviceID      string
	WorkspaceRoot string
	Store         *store.PeerStore
}

type ReadRequest struct {
	TreeID string
	Source control.PrincipalIdentity
	Params protocol.ReadResultPackagePartParams
}

type BeginRequest struct {
	TreeID string
	Source control.PrincipalIdentity
	Params protocol.BeginResultPackageParams
}

type WriteRequest struct {
	TreeID string
	Source control.PrincipalIdentity
	Params protocol.WriteResultPackagePartParams
}

type FinishRequest struct {
	TreeID string
	Source control.PrincipalIdentity
	Params protocol.FinishResultPackageParams
}

type CancelRequest struct {
	TreeID string
	Source control.PrincipalIdentity
	Params protocol.CancelResultPackageParams
}

type AcknowledgeRequest struct {
	TreeID string
	Source control.PrincipalIdentity
	Params protocol.AcknowledgeResultPackageParams
}

type Manager struct {
	controllerID string
	deviceID     string
	state        *store.PeerStore
	workspace    *os.Root
	outbox       *os.Root
	inbox        *os.Root
	locks        [packageLockCount]sync.Mutex
	now          func() time.Time
	syncRoot     func(*os.Root) error
}

func New(ctx context.Context, options Options) (*Manager, error) {
	if options.Store == nil {
		return nil, errors.New("result package store is required")
	}
	if err := identity.ValidateID(options.ControllerID); err != nil {
		return nil, fmt.Errorf("controllerId %w", err)
	}
	if err := identity.ValidateID(options.DeviceID); err != nil {
		return nil, fmt.Errorf("deviceId %w", err)
	}
	if !filepath.IsAbs(options.WorkspaceRoot) {
		return nil, errors.New("result package workspace root must be absolute")
	}
	workspace, err := os.OpenRoot(options.WorkspaceRoot)
	if err != nil {
		return nil, fmt.Errorf("open result package workspace root: %w", err)
	}
	closeOnError := func(err error) (*Manager, error) {
		return nil, errors.Join(err, workspace.Close())
	}
	outbox, err := ensurePrivateRoot(workspace, outboxDirectoryName)
	if err != nil {
		return closeOnError(fmt.Errorf("open result package outbox: %w", err))
	}
	inbox, err := ensurePrivateRoot(workspace, inboxDirectoryName)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("open result package inbox: %w", err), outbox.Close(), workspace.Close())
	}
	if err := syncDirectory(workspace); err != nil {
		return nil, errors.Join(
			fmt.Errorf("sync result package workspace root: %w", err),
			inbox.Close(), outbox.Close(), workspace.Close(),
		)
	}
	manager := &Manager{
		controllerID: options.ControllerID,
		deviceID:     options.DeviceID,
		state:        options.Store,
		workspace:    workspace,
		outbox:       outbox,
		inbox:        inbox,
		now:          time.Now,
		syncRoot:     syncDirectory,
	}
	if err := manager.recover(ctx); err != nil {
		return nil, errors.Join(fmt.Errorf("recover result package files: %w", err), manager.Close())
	}
	return manager, nil
}

func (m *Manager) Close() error {
	if m == nil {
		return nil
	}
	return errors.Join(m.inbox.Close(), m.outbox.Close(), m.workspace.Close())
}

func (m *Manager) lock(packageID string) *sync.Mutex {
	digest := fnv.New32a()
	_, _ = digest.Write([]byte(packageID))
	return &m.locks[digest.Sum32()%packageLockCount]
}

func ensurePrivateRoot(parent *os.Root, name string) (*os.Root, error) {
	if err := parent.Mkdir(name, 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
		return nil, err
	}
	before, err := parent.Lstat(name)
	if err != nil {
		return nil, err
	}
	if !before.IsDir() || before.Mode()&os.ModeSymlink != 0 || !privateMode(before.Mode(), true) {
		return nil, errors.New("result package root must be a private directory")
	}
	root, err := parent.OpenRoot(name)
	if err != nil {
		return nil, err
	}
	directory, err := root.Open(".")
	if err != nil {
		_ = root.Close()
		return nil, err
	}
	opened, statErr := directory.Stat()
	closeErr := directory.Close()
	if statErr == nil && (!os.SameFile(before, opened) || !privateMode(opened.Mode(), true)) {
		statErr = errors.New("result package root changed while it was opened")
	}
	if statErr != nil || closeErr != nil {
		_ = root.Close()
		return nil, errors.Join(statErr, closeErr)
	}
	return root, nil
}

func privateMode(mode os.FileMode, directory bool) bool {
	if directory != mode.IsDir() || mode&os.ModeSymlink != 0 {
		return false
	}
	if runtime.GOOS == "windows" {
		return true
	}
	if directory {
		return mode.Perm() == 0o700
	}
	return mode.Perm() == 0o600
}
