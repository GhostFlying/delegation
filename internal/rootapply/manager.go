package rootapply

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/GhostFlying/delegation/internal/gitworkspace"
	"github.com/GhostFlying/delegation/internal/localbridge"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/resultpackagefiles"
)

const journalDirectoryName = ".delegation-root-applies-v1"

type PackageSource interface {
	LookupApplyManifest(
		context.Context,
		resultpackagefiles.ApplyPackageRequest,
	) (protocol.ResultManifest, error)
	MaterializeApplyWorkspace(
		context.Context,
		resultpackagefiles.MaterializeApplyPackageRequest,
		*os.Root,
	) (protocol.ResultManifest, error)
}

type Options struct {
	WorkspaceRoot string
	Runner        gitworkspace.Runner
	Packages      PackageSource
}

type Manager struct {
	workspaceRoot string
	journalPath   string
	journal       *os.Root
	runner        gitworkspace.Runner
	packages      PackageSource
	fault         func(string) error
	now           func() time.Time
	retention     journalRetention
	mu            sync.Mutex
}

const (
	faultBeforeMutation         = "beforeMutation"
	faultBeforeDestructiveWrite = "beforeDestructiveWrite"
	faultAfterMutation          = "afterMutation"
)

func New(options Options) (*Manager, error) {
	if !filepath.IsAbs(options.WorkspaceRoot) || filepath.Clean(options.WorkspaceRoot) != options.WorkspaceRoot {
		return nil, errors.New("root apply workspace root must be a normalized absolute path")
	}
	if options.Runner.Binary == "" {
		return nil, errors.New("root apply Git runner is required")
	}
	if options.Packages == nil {
		return nil, errors.New("root apply result package source is required")
	}
	workspace, err := os.OpenRoot(options.WorkspaceRoot)
	if err != nil {
		return nil, fmt.Errorf("open root apply workspace authority: %w", err)
	}
	defer workspace.Close()
	if err := workspace.Mkdir(journalDirectoryName, 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
		return nil, fmt.Errorf("create root apply journal: %w", err)
	}
	info, err := workspace.Lstat(journalDirectoryName)
	if err != nil || !privateEntry(info, true) {
		return nil, errors.New("root apply journal must be a private real directory")
	}
	journal, err := workspace.OpenRoot(journalDirectoryName)
	if err != nil {
		return nil, fmt.Errorf("open root apply journal: %w", err)
	}
	if err := syncDirectory(workspace); err != nil {
		_ = journal.Close()
		return nil, fmt.Errorf("sync root apply journal authority: %w", err)
	}
	manager := &Manager{
		workspaceRoot: options.WorkspaceRoot,
		journalPath:   filepath.Join(options.WorkspaceRoot, journalDirectoryName),
		journal:       journal,
		runner:        options.Runner,
		packages:      options.Packages,
		now:           time.Now,
		retention:     defaultJournalRetention,
	}
	if _, err := manager.maintainJournals(false); err != nil {
		_ = journal.Close()
		return nil, fmt.Errorf("maintain root apply journal: %w", err)
	}
	return manager, nil
}

func (m *Manager) Close() error {
	if m == nil || m.journal == nil {
		return nil
	}
	return m.journal.Close()
}

func (m *Manager) triggerFault(point string) error {
	if m.fault == nil {
		return nil
	}
	return m.fault(point)
}

func privateEntry(info os.FileInfo, directory bool) bool {
	if info == nil || info.IsDir() != directory || info.Mode()&os.ModeSymlink != 0 {
		return false
	}
	if runtime.GOOS == "windows" {
		return true
	}
	if directory {
		return info.Mode().Perm() == 0o700
	}
	return info.Mode().Perm() == 0o600
}

func (m *Manager) mapPackageError(err error) error {
	switch {
	case errors.Is(err, resultpackagefiles.ErrApplyPackageEvicted):
		return localbridge.ErrApplyPackageEvicted
	case errors.Is(err, resultpackagefiles.ErrApplyPackageUnavailable):
		return localbridge.ErrApplyPackageUnavailable
	default:
		return err
	}
}
