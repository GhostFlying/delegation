package rootapply

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/identity"
	"github.com/GhostFlying/delegation/internal/localbridge"
	"github.com/GhostFlying/delegation/internal/protocol"
)

const (
	journalVersion       = 2
	journalFileName      = "journal.json"
	baseBundleFileName   = "base.bundle"
	desiredOverlayName   = "desired-overlay.tar.zst"
	packageDirectoryName = "package"
	stagingDirectoryName = "staging"
	maximumJournalBytes  = 512 * 1024
)

type journalState string

const (
	journalAuthorizing      journalState = "authorizing"
	journalBuilding         journalState = "building"
	journalPrepared         journalState = "prepared"
	journalMutating         journalState = "mutating"
	journalVerifying        journalState = "verifying"
	journalCompleted        journalState = "completed"
	journalRecoveryRequired journalState = "recoveryRequired"
)

type requestBinding struct {
	ApplyID          string `json:"applyId"`
	PackageID        string `json:"packageId"`
	ControllerID     string `json:"controllerId"`
	TreeID           string `json:"treeId"`
	RootAgentID      string `json:"rootAgentId"`
	RootDeviceID     string `json:"rootDeviceId"`
	SourcePathSHA256 string `json:"sourcePathSha256"`
	GitURL           string `json:"gitUrl"`
}

type journalRecord struct {
	Version             int                                  `json:"version"`
	CreatedAt           int64                                `json:"createdAt"`
	UpdatedAt           int64                                `json:"updatedAt"`
	Request             requestBinding                       `json:"request"`
	AuthorizationParams *protocol.AuthorizeResultApplyParams `json:"authorizationParams"`
	Authorization       *protocol.AuthorizeResultApplyResult `json:"authorization"`
	Manifest            *protocol.ResultManifest             `json:"manifest"`
	Base                *protocol.WorkspaceManifest          `json:"base"`
	Desired             *protocol.WorkspaceManifest          `json:"desired"`
	DesiredData         *artifactDescriptor                  `json:"desiredData"`
	State               journalState                         `json:"state"`
	Result              *localbridge.ApplyAgentChangesResult `json:"result"`
}

type artifactDescriptor struct {
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

func (d artifactDescriptor) validate() error {
	if d.Size < 1 || d.Size > protocol.MaximumWorkspaceArtifactBytes || len(d.SHA256) != 64 {
		return errors.New("root apply artifact descriptor is invalid")
	}
	_, err := hex.DecodeString(d.SHA256)
	return err
}

type journalLease struct {
	path string
	root *os.Root
	info os.FileInfo
	now  func() time.Time
}

func bindingFor(request localbridge.ResultApplyRequest, gitURL string) requestBinding {
	return requestBinding{
		ApplyID: request.Params.ApplyID, PackageID: request.Params.PackageID,
		ControllerID: request.Root.ControllerID, TreeID: request.Root.TreeID,
		RootAgentID: request.Root.AgentID, RootDeviceID: request.Root.DeviceID,
		SourcePathSHA256: hashPath(request.Params.SourcePath), GitURL: gitURL,
	}
}

func (b requestBinding) matches(request localbridge.ResultApplyRequest) bool {
	return b.ApplyID == request.Params.ApplyID && b.PackageID == request.Params.PackageID &&
		b.ControllerID == request.Root.ControllerID && b.TreeID == request.Root.TreeID &&
		b.RootAgentID == request.Root.AgentID && b.RootDeviceID == request.Root.DeviceID &&
		b.SourcePathSHA256 == hashPath(request.Params.SourcePath)
}

func (b requestBinding) root() control.PrincipalIdentity {
	return control.PrincipalIdentity{
		ControllerID: b.ControllerID, TreeID: b.TreeID, AgentID: b.RootAgentID,
		DeviceID: b.RootDeviceID,
	}
}

func (r journalRecord) validate() error {
	if r.Version != journalVersion {
		return fmt.Errorf("unsupported root apply journal version %d", r.Version)
	}
	if r.CreatedAt < 1 || r.UpdatedAt < r.CreatedAt {
		return errors.New("root apply journal timestamps are invalid")
	}
	for _, value := range []string{
		r.Request.ApplyID, r.Request.PackageID, r.Request.ControllerID, r.Request.TreeID,
		r.Request.RootAgentID, r.Request.RootDeviceID,
	} {
		if err := identity.ValidateID(value); err != nil {
			return err
		}
	}
	if len(r.Request.SourcePathSHA256) != 64 {
		return errors.New("root apply journal path digest is invalid")
	}
	if _, err := hex.DecodeString(r.Request.SourcePathSHA256); err != nil {
		return errors.New("root apply journal path digest is invalid")
	}
	switch r.State {
	case journalAuthorizing:
		if r.Manifest == nil || r.AuthorizationParams == nil || r.Authorization != nil || r.Base != nil ||
			r.Desired != nil || r.DesiredData != nil || r.Result != nil {
			return errors.New("authorizing root apply journal contains invalid state")
		}
	case journalBuilding:
		if r.Manifest == nil || r.AuthorizationParams == nil || r.Authorization == nil || r.Base != nil ||
			r.Desired != nil || r.DesiredData != nil || r.Result != nil {
			return errors.New("building root apply journal contains invalid state")
		}
	case journalPrepared, journalMutating, journalVerifying, journalRecoveryRequired:
		if r.Manifest == nil || r.AuthorizationParams == nil || r.Authorization == nil || r.Base == nil ||
			r.Desired == nil || r.Result != nil {
			return errors.New("active root apply journal contains invalid state")
		}
		if r.Desired.Clean != (r.DesiredData == nil) {
			return errors.New("root apply journal artifacts differ from snapshot cleanliness")
		}
		for _, descriptor := range []*artifactDescriptor{r.DesiredData} {
			if descriptor != nil {
				if err := descriptor.validate(); err != nil {
					return err
				}
			}
		}
	case journalCompleted:
		if r.Result == nil || r.Manifest != nil || r.AuthorizationParams != nil ||
			r.Authorization != nil || r.Base != nil || r.Desired != nil || r.DesiredData != nil {
			return errors.New("completed root apply journal has no result")
		}
		if err := r.Result.Validate(); err != nil {
			return err
		}
		if r.Result.ApplyID != r.Request.ApplyID || r.Result.PackageID != r.Request.PackageID {
			return errors.New("completed root apply result differs from its request")
		}
	default:
		return fmt.Errorf("unsupported root apply journal state %q", r.State)
	}
	if r.Manifest != nil {
		if err := r.Manifest.Validate(); err != nil {
			return fmt.Errorf("root apply journal manifest: %w", err)
		}
		if r.Manifest.PackageID != r.Request.PackageID ||
			r.Manifest.ControllerID != r.Request.ControllerID || r.Manifest.TreeID != r.Request.TreeID {
			return errors.New("root apply journal manifest authority differs")
		}
	}
	if r.AuthorizationParams != nil {
		if err := r.AuthorizationParams.Validate(); err != nil {
			return err
		}
		if r.AuthorizationParams.ApplyID != r.Request.ApplyID ||
			r.AuthorizationParams.PackageID != r.Request.PackageID ||
			r.AuthorizationParams.SourcePathSHA256 != r.Request.SourcePathSHA256 ||
			r.AuthorizationParams.GitURL != r.Request.GitURL {
			return errors.New("root apply journal authorization request differs")
		}
	}
	if r.Authorization != nil {
		if err := r.Authorization.Validate(); err != nil {
			return err
		}
		if r.Authorization.ApplyID != r.Request.ApplyID ||
			r.Authorization.PackageID != r.Request.PackageID {
			return errors.New("root apply journal authorization differs")
		}
	}
	if r.Base != nil {
		if err := r.Base.Validate(); err != nil {
			return err
		}
	}
	if r.Desired != nil {
		if err := r.Desired.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) createJournal(applyID string) (*journalLease, error) {
	if err := identity.ValidateID(applyID); err != nil {
		return nil, err
	}
	if err := m.journal.Mkdir(applyID, 0o700); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return nil, localbridge.ErrApplyRequestConflict
		}
		return nil, err
	}
	cleanup := func(cause error) (*journalLease, error) {
		removeErr := m.journal.RemoveAll(applyID)
		if errors.Is(removeErr, fs.ErrNotExist) {
			removeErr = nil
		}
		return nil, errors.Join(cause, removeErr, syncDirectory(m.journal))
	}
	if err := syncDirectory(m.journal); err != nil {
		return cleanup(err)
	}
	lease, err := m.openJournal(applyID)
	if err != nil {
		return cleanup(err)
	}
	return lease, nil
}

func (m *Manager) openJournal(applyID string) (*journalLease, error) {
	if err := identity.ValidateID(applyID); err != nil {
		return nil, err
	}
	info, err := m.journal.Lstat(applyID)
	if err != nil {
		return nil, err
	}
	if !privateEntry(info, true) {
		return nil, localbridge.ErrApplyRecoveryRequired
	}
	root, err := m.journal.OpenRoot(applyID)
	if err != nil {
		return nil, err
	}
	lease := &journalLease{
		path: filepath.Join(m.journalPath, applyID), root: root, info: info, now: m.now,
	}
	if err := lease.verify(); err != nil {
		_ = root.Close()
		return nil, err
	}
	return lease, nil
}

func (l *journalLease) verify() error {
	info, err := os.Lstat(l.path)
	if err != nil || !privateEntry(info, true) || !os.SameFile(info, l.info) {
		return localbridge.ErrApplyRecoveryRequired
	}
	return nil
}

func (l *journalLease) close() error { return l.root.Close() }

func (l *journalLease) read() (journalRecord, error) {
	if err := l.verify(); err != nil {
		return journalRecord{}, err
	}
	info, err := l.root.Lstat(journalFileName)
	if err != nil || !privateEntry(info, false) || info.Size() < 1 || info.Size() > maximumJournalBytes {
		return journalRecord{}, localbridge.ErrApplyRecoveryRequired
	}
	file, err := l.root.Open(journalFileName)
	if err != nil {
		return journalRecord{}, err
	}
	data, readErr := io.ReadAll(io.LimitReader(file, maximumJournalBytes+1))
	closeErr := file.Close()
	if readErr != nil || closeErr != nil {
		return journalRecord{}, errors.Join(readErr, closeErr)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var record journalRecord
	if err := decoder.Decode(&record); err != nil {
		return journalRecord{}, localbridge.ErrApplyRecoveryRequired
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return journalRecord{}, localbridge.ErrApplyRecoveryRequired
	}
	if err := record.validate(); err != nil {
		return journalRecord{}, localbridge.ErrApplyRecoveryRequired
	}
	return record, nil
}

func (l *journalLease) write(record journalRecord) error {
	now := l.now().Unix()
	if record.CreatedAt == 0 {
		record.CreatedAt = now
	}
	if now < record.CreatedAt {
		now = record.CreatedAt
	}
	record.UpdatedAt = now
	if err := record.validate(); err != nil {
		return err
	}
	data, err := json.Marshal(record)
	if err != nil || len(data)+1 > maximumJournalBytes {
		return errors.New("root apply journal exceeds its byte limit")
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return err
	}
	temporary := ".journal-" + hex.EncodeToString(random[:]) + ".tmp"
	file, err := l.root.OpenFile(temporary, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	keep := false
	defer func() {
		if !keep {
			_ = l.root.Remove(temporary)
		}
	}()
	if _, err := file.Write(append(data, '\n')); err != nil {
		_ = file.Close()
		return err
	}
	if err := errors.Join(file.Sync(), file.Close()); err != nil {
		return err
	}
	if err := l.verify(); err != nil {
		return err
	}
	if err := l.root.Rename(temporary, journalFileName); err != nil {
		return err
	}
	keep = true
	return syncDirectory(l.root)
}

func (l *journalLease) compactTerminal(record journalRecord) error {
	record.AuthorizationParams = nil
	record.Authorization = nil
	record.Manifest = nil
	record.Base = nil
	record.Desired = nil
	record.DesiredData = nil
	if err := l.write(record); err != nil {
		return err
	}
	return l.compactArtifacts()
}

func (l *journalLease) compactArtifacts() error {
	entries, err := fs.ReadDir(l.root.FS(), ".")
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.Name() == journalFileName {
			continue
		}
		if err := l.root.RemoveAll(entry.Name()); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}
	return syncDirectory(l.root)
}

func journalExists(root *os.Root, applyID string) (bool, error) {
	_, err := root.Lstat(applyID)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	return false, err
}
