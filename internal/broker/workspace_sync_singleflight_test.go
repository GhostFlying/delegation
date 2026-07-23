package broker

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/connector"
	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/protocol"
)

const (
	workspaceSingleFlightCanceledSyncID = "123e4567-e89b-42d3-a456-426614174144"
	workspaceSingleFlightFailedSyncID   = "123e4567-e89b-42d3-a456-426614174145"
)

type serializedWorkspacePeer struct {
	recordingWorkspacePeer

	stateMu       sync.Mutex
	started       chan int
	firstCanceled chan struct{}
	releaseFirst  chan struct{}
	cancelFirst   bool
	failFirst     bool
	calls         int
	active        int
	maximumActive int
}

func (p *serializedWorkspacePeer) PrepareWorkspace(
	ctx context.Context,
	request connector.WorkspacePrepareRequest,
) (protocol.PrepareWorkspaceResult, error) {
	p.recordingWorkspacePeer.mu.Lock()
	p.preparations = append(p.preparations, request)
	p.recordingWorkspacePeer.mu.Unlock()

	p.stateMu.Lock()
	p.calls++
	call := p.calls
	p.active++
	p.maximumActive = max(p.maximumActive, p.active)
	p.stateMu.Unlock()
	p.started <- call
	defer func() {
		p.stateMu.Lock()
		p.active--
		p.stateMu.Unlock()
	}()

	if call == 1 {
		if p.cancelFirst {
			<-ctx.Done()
			close(p.firstCanceled)
		}
		<-p.releaseFirst
		if p.cancelFirst {
			return protocol.PrepareWorkspaceResult{}, ctx.Err()
		}
		if p.failFirst {
			return protocol.PrepareWorkspaceResult{}, errors.New("first target preparation failed")
		}
	}
	hash, err := protocol.WorkspaceManifestHash(request.Params.Manifest)
	if err != nil {
		return protocol.PrepareWorkspaceResult{}, err
	}
	return protocol.PrepareWorkspaceResult{
		WorkspaceID:  request.Params.WorkspaceID,
		Outcome:      protocol.WorkspacePrepareReady,
		Strategy:     protocol.WorkspaceStrategyDirect,
		ManifestHash: hash,
		Warnings:     append([]string(nil), request.Params.Manifest.Warnings...),
	}, nil
}

func (p *serializedWorkspacePeer) snapshotConcurrency() (int, int) {
	p.stateMu.Lock()
	defer p.stateMu.Unlock()
	return p.calls, p.maximumActive
}

func TestWorkspaceSyncSingleFlightWaitsForCanceledTargetCleanup(t *testing.T) {
	harness := newBrokerHarness(t, config.AuthModeNone, time.Second)
	gitURL := "ssh://git@example.invalid/repository.git"
	sourceManager := &recordingWorkspacePeer{
		deviceID: brokerTestDeviceID,
		manifest: workspaceRPCManifest(gitURL, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
	}
	targetManager := &serializedWorkspacePeer{
		recordingWorkspacePeer: recordingWorkspacePeer{deviceID: agentRPCTargetID},
		started:                make(chan int, 2),
		firstCanceled:          make(chan struct{}),
		releaseFirst:           make(chan struct{}),
		cancelFirst:            true,
	}
	sourceClient := startAgentRPCConnector(t, harness, brokerTestDeviceID, sourceManager)
	startAgentRPCConnector(t, harness, agentRPCTargetID, targetManager)
	sourceSession := harness.server.connection(brokerTestDeviceID)
	root, source := ensureWorkspaceSingleFlightRoot(t, sourceClient)
	params := protocol.SyncWorkspaceParams{
		SyncID: workspaceSingleFlightCanceledSyncID, TargetDeviceID: agentRPCTargetID,
		GitURL: gitURL, SourcePath: filepath.Join(t.TempDir(), "trusted", "source"),
	}

	firstContext, cancelFirst := context.WithCancel(context.Background())
	firstDone := callWorkspaceSync(sourceClient, firstContext, root, source, params)
	wantWorkspacePrepareCall(t, targetManager.started, 1)
	cancelOnlyWorkspaceSync(t, sourceSession)
	select {
	case <-targetManager.firstCanceled:
	case <-time.After(2 * time.Second):
		t.Fatal("target did not observe cancellation")
	}
	secondContext, cancelSecond := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelSecond()
	secondDone := callWorkspaceSync(sourceClient, secondContext, root, source, params)
	wantNoWorkspacePrepareCall(t, targetManager.started)
	close(targetManager.releaseFirst)
	select {
	case got := <-targetManager.started:
		if got != 2 {
			t.Fatalf("target preparation = %d, want 2", got)
		}
	case err := <-secondDone:
		inspections, _, _ := sourceManager.snapshot()
		t.Fatalf("second workspace sync ended before retry after %d inspections: %v", len(inspections), err)
	case <-time.After(2 * time.Second):
		sourceSession.asyncMu.Lock()
		asyncCount := len(sourceSession.asyncCancels)
		sourceSession.asyncMu.Unlock()
		harness.server.workspaceSyncs.mu.Lock()
		flightCount := len(harness.server.workspaceSyncs.active)
		harness.server.workspaceSyncs.mu.Unlock()
		targetSession := harness.server.connection(agentRPCTargetID)
		targetSession.pendingMu.Lock()
		peerCalls := len(targetSession.pending)
		targetSession.pendingMu.Unlock()
		calls, maximumActive := targetManager.snapshotConcurrency()
		t.Fatalf(
			"target retry did not start: async=%d flights=%d target peer calls=%d calls=%d max=%d",
			asyncCount, flightCount, peerCalls, calls, maximumActive,
		)
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("second workspace sync: %v", err)
	}
	cancelFirst()
	if err := <-firstDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("first workspace sync = %v, want cancellation", err)
	}
	if calls, maximumActive := targetManager.snapshotConcurrency(); calls != 2 || maximumActive != 1 {
		t.Fatalf("target preparations = %d calls, %d concurrently", calls, maximumActive)
	}
}

func cancelOnlyWorkspaceSync(t *testing.T, session *session) {
	t.Helper()
	session.asyncMu.Lock()
	if len(session.asyncCancels) != 1 {
		count := len(session.asyncCancels)
		session.asyncMu.Unlock()
		t.Fatalf("active asynchronous requests = %d, want one workspace sync", count)
	}
	var cancel context.CancelFunc
	for _, activeCancel := range session.asyncCancels {
		cancel = activeCancel
	}
	session.asyncMu.Unlock()
	cancel()
}

func TestWorkspaceSyncSingleFlightRetriesAfterLeaderFailure(t *testing.T) {
	harness := newBrokerHarness(t, config.AuthModeNone, time.Second)
	gitURL := "ssh://git@example.invalid/repository.git"
	sourceManager := &recordingWorkspacePeer{
		deviceID: brokerTestDeviceID,
		manifest: workspaceRPCManifest(gitURL, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
	}
	targetManager := &serializedWorkspacePeer{
		recordingWorkspacePeer: recordingWorkspacePeer{deviceID: agentRPCTargetID},
		started:                make(chan int, 2),
		releaseFirst:           make(chan struct{}),
		failFirst:              true,
	}
	sourceClient := startAgentRPCConnector(t, harness, brokerTestDeviceID, sourceManager)
	startAgentRPCConnector(t, harness, agentRPCTargetID, targetManager)
	root, source := ensureWorkspaceSingleFlightRoot(t, sourceClient)
	params := protocol.SyncWorkspaceParams{
		SyncID: workspaceSingleFlightFailedSyncID, TargetDeviceID: agentRPCTargetID,
		GitURL: gitURL, SourcePath: filepath.Join(t.TempDir(), "trusted", "source"),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	firstDone := callWorkspaceSync(sourceClient, ctx, root, source, params)
	wantWorkspacePrepareCall(t, targetManager.started, 1)
	secondDone := callWorkspaceSync(sourceClient, ctx, root, source, params)
	wantNoWorkspacePrepareCall(t, targetManager.started)
	close(targetManager.releaseFirst)
	if err := <-firstDone; err == nil {
		t.Fatal("first workspace sync unexpectedly succeeded")
	}
	wantWorkspacePrepareCall(t, targetManager.started, 2)
	if err := <-secondDone; err != nil {
		t.Fatalf("retried workspace sync: %v", err)
	}
	if calls, maximumActive := targetManager.snapshotConcurrency(); calls != 2 || maximumActive != 1 {
		t.Fatalf("target preparations = %d calls, %d concurrently", calls, maximumActive)
	}
}

func ensureWorkspaceSingleFlightRoot(
	t *testing.T,
	client *connector.Client,
) (protocol.EnsureRootTreeResult, control.PrincipalIdentity) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var root protocol.EnsureRootTreeResult
	if err := client.Call(
		ctx, protocol.MethodEnsureRootTree, "", nil,
		protocol.EnsureRootTreeParams{ExternalThreadID: agentRPCRemoteThreadID}, &root,
	); err != nil {
		t.Fatal(err)
	}
	return root, root.Principal.Identity()
}

func callWorkspaceSync(
	client *connector.Client,
	ctx context.Context,
	root protocol.EnsureRootTreeResult,
	source control.PrincipalIdentity,
	params protocol.SyncWorkspaceParams,
) <-chan error {
	done := make(chan error, 1)
	go func() {
		var result protocol.SyncWorkspaceResult
		done <- client.Call(
			ctx, protocol.MethodSyncWorkspace, root.Tree.TreeID, &source, params, &result,
		)
	}()
	return done
}

func wantWorkspacePrepareCall(t *testing.T, started <-chan int, want int) {
	t.Helper()
	select {
	case got := <-started:
		if got != want {
			t.Fatalf("target preparation = %d, want %d", got, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("target preparation %d did not start", want)
	}
}

func wantNoWorkspacePrepareCall(t *testing.T, started <-chan int) {
	t.Helper()
	select {
	case got := <-started:
		t.Fatalf("target preparation %d overlapped the active sync", got)
	case <-time.After(100 * time.Millisecond):
	}
}
