package localbridge

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/protocol"
)

const (
	bridgeApplyID     = "123e4567-e89b-42d3-a456-426614174320"
	bridgePackageID   = "123e4567-e89b-42d3-a456-426614174321"
	bridgeWorkspaceID = "123e4567-e89b-42d3-a456-426614174322"
)

type fakeResultApplyProvider struct {
	mu             sync.Mutex
	preparation    ResultApplyPreparation
	result         ApplyAgentChangesResult
	prepareRequest ResultApplyRequest
	applyRequest   ResultApplyRequest
	authorization  protocol.AuthorizeResultApplyResult
	prepareErr     error
}

func (p *fakeResultApplyProvider) PrepareResultApply(
	_ context.Context,
	request ResultApplyRequest,
) (ResultApplyPreparation, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.prepareRequest = request
	return p.preparation, p.prepareErr
}

func (p *fakeResultApplyProvider) ApplyAuthorizedResult(
	_ context.Context,
	request ResultApplyRequest,
	authorization protocol.AuthorizeResultApplyResult,
) (ApplyAgentChangesResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.applyRequest = request
	p.authorization = authorization
	return p.result, nil
}

type resultApplyBrokerBackend struct {
	mu     sync.Mutex
	calls  []protocol.AuthorizeResultApplyParams
	result protocol.AuthorizeResultApplyResult
}

func (b *resultApplyBrokerBackend) Call(
	_ context.Context,
	method, treeID string,
	source *control.PrincipalIdentity,
	params, result any,
) error {
	if method != protocol.MethodAuthorizeResultApply || treeID != bridgeTestTreeID || source == nil ||
		source.ParentAgentID != "" {
		panic("unexpected result apply broker call")
	}
	input, ok := params.(protocol.AuthorizeResultApplyParams)
	if !ok {
		panic("result apply broker call did not use authorization params")
	}
	b.mu.Lock()
	b.calls = append(b.calls, input)
	b.mu.Unlock()
	*result.(*protocol.AuthorizeResultApplyResult) = b.result
	return nil
}

func TestResultApplyKeepsRawWorkspacePathInsideLocalBridge(t *testing.T) {
	root := control.NewRootPrincipal(
		bridgeTestControllerID, bridgeTestTreeID, bridgeTestAgentID, bridgeTestDeviceID,
	).Identity()
	sourcePath := filepath.Join(t.TempDir(), "repository", "nested")
	authorizationParams := protocol.AuthorizeResultApplyParams{
		ApplyID: bridgeApplyID, PackageID: bridgePackageID,
		SourcePathSHA256: strings.Repeat("a", 64), GitURL: "ssh://git@example.invalid/repository.git",
	}
	authorization := protocol.AuthorizeResultApplyResult{
		ApplyID: bridgeApplyID, PackageID: bridgePackageID,
		ManifestSHA256: strings.Repeat("b", 64), WorkspaceID: bridgeWorkspaceID,
		BaseManifestHash: strings.Repeat("c", 64),
	}
	provider := &fakeResultApplyProvider{
		preparation: ResultApplyPreparation{Authorization: &authorizationParams},
		result: ApplyAgentChangesResult{
			ApplyID: bridgeApplyID, PackageID: bridgePackageID, Outcome: ApplyAgentChangesApplied,
		},
	}
	backend := &resultApplyBrokerBackend{result: authorization}
	client, stop := startResultApplyBridge(t, backend, provider)
	defer stop()
	params := ApplyAgentChangesParams{
		ApplyID: bridgeApplyID, PackageID: bridgePackageID, SourcePath: sourcePath,
	}
	var result ApplyAgentChangesResult
	if err := client.Call(
		context.Background(), MethodApplyAgentChanges, root.TreeID, &root, params, &result,
	); err != nil {
		t.Fatal(err)
	}
	if result != provider.result {
		t.Fatalf("local apply result = %#v", result)
	}
	provider.mu.Lock()
	if provider.prepareRequest.Params != params || provider.applyRequest.Params != params ||
		provider.authorization != authorization {
		t.Fatalf("local provider calls = prepare %#v, apply %#v, auth %#v",
			provider.prepareRequest, provider.applyRequest, provider.authorization)
	}
	provider.mu.Unlock()
	backend.mu.Lock()
	calls := append([]protocol.AuthorizeResultApplyParams(nil), backend.calls...)
	backend.mu.Unlock()
	if len(calls) != 1 || calls[0] != authorizationParams {
		t.Fatalf("broker authorization calls = %#v", calls)
	}
	encoded, err := json.Marshal(calls)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), sourcePath) || strings.Contains(string(encoded), "sourcePath\"") {
		t.Fatalf("raw root path crossed broker boundary: %s", encoded)
	}
}

func TestResultApplyRejectsWorkerPrincipalBeforeLocalProvider(t *testing.T) {
	provider := &fakeResultApplyProvider{}
	backend := &resultApplyBrokerBackend{}
	client, stop := startResultApplyBridge(t, backend, provider)
	defer stop()
	worker := control.PrincipalIdentity{
		ControllerID: bridgeTestControllerID, TreeID: bridgeTestTreeID,
		AgentID: bridgeResultWorkerID, ParentAgentID: bridgeTestAgentID, DeviceID: bridgeTestDeviceID,
	}
	var result ApplyAgentChangesResult
	err := client.Call(
		context.Background(), MethodApplyAgentChanges, worker.TreeID, &worker,
		ApplyAgentChangesParams{
			ApplyID: bridgeApplyID, PackageID: bridgePackageID, SourcePath: t.TempDir(),
		},
		&result,
	)
	assertRPCCode(t, err, protocol.ErrorForbidden)
	provider.mu.Lock()
	defer provider.mu.Unlock()
	if provider.prepareRequest != (ResultApplyRequest{}) {
		t.Fatalf("worker reached local apply provider: %#v", provider.prepareRequest)
	}
}

func TestResultApplyMapsLocalBacklogToUnavailable(t *testing.T) {
	provider := &fakeResultApplyProvider{prepareErr: ErrApplyBacklog}
	client, stop := startResultApplyBridge(t, &resultApplyBrokerBackend{}, provider)
	defer stop()
	root := control.NewRootPrincipal(
		bridgeTestControllerID, bridgeTestTreeID, bridgeTestAgentID, bridgeTestDeviceID,
	).Identity()
	var result ApplyAgentChangesResult
	err := client.Call(
		context.Background(), MethodApplyAgentChanges, root.TreeID, &root,
		ApplyAgentChangesParams{
			ApplyID: bridgeApplyID, PackageID: bridgePackageID, SourcePath: t.TempDir(),
		},
		&result,
	)
	assertRPCCode(t, err, protocol.ErrorUnavailable)
}

func startResultApplyBridge(
	t *testing.T,
	backend Backend,
	provider ResultApplyProvider,
) (*Client, func()) {
	t.Helper()
	endpoint := testEndpoint(t)
	server, err := ListenWithResultApply(
		endpoint, testServiceIdentity(), backend, &fakeWorkerAuthorizer{}, nil, nil, provider,
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	client, err := NewClient(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			if err := server.Close(); err != nil {
				t.Errorf("close result apply bridge: %v", err)
			}
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("serve result apply bridge: %v", err)
				}
			case <-time.After(time.Second):
				t.Error("result apply bridge did not stop")
			}
		})
	}
	t.Cleanup(stop)
	return client, stop
}
