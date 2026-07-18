package store

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/control"
)

const (
	treeThreadID       = "123e4567-e89b-42d3-a456-426614174040"
	treeSecondThreadID = "123e4567-e89b-42d3-a456-426614174041"
	treeThirdThreadID  = "123e4567-e89b-42d3-a456-426614174043"
)

func TestRootTreeBindingPersistsAndAuthorizesFromStore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "broker.sqlite3")
	registry, err := Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := registry.RegisterTrustedDevice(
		ctx,
		deviceDescriptor(testControllerID, testDeviceID, control.DeviceRoleController),
		time.Unix(1, 0),
	); err != nil {
		t.Fatal(err)
	}
	tree, principal, err := registry.EnsureRootTree(
		ctx, testControllerID, treeThreadID, testDeviceID, time.Unix(2, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := tree.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := principal.Validate(); err != nil {
		t.Fatal(err)
	}
	if tree.RootAgentID != principal.AgentID || !principal.Has(control.CapabilityDeviceRead) {
		t.Fatalf("root tree binding = %#v, %#v", tree, principal)
	}
	authorized, err := registry.AuthorizePrincipal(ctx, principal.Identity(), control.CapabilityDeviceRead)
	if err != nil || !reflect.DeepEqual(authorized, principal) {
		t.Fatalf("authorized principal = %#v, error %v", authorized, err)
	}
	if _, err := registry.AuthorizePrincipal(
		ctx, principal.Identity(), control.CapabilityArtifactApply,
	); !errors.Is(err, ErrAuthorizationDenied) {
		t.Fatalf("deferred capability error = %v, want authorization denial", err)
	}
	forged := principal.Identity()
	forged.DeviceID = deviceSecondID
	if _, err := registry.AuthorizePrincipal(
		ctx, forged, control.CapabilityDeviceRead,
	); !errors.Is(err, ErrAuthorizationDenied) {
		t.Fatalf("forged identity error = %v, want authorization denial", err)
	}
	forged = principal.Identity()
	forged.TreeID = treeSecondThreadID
	if _, err := registry.AuthorizePrincipal(
		ctx, forged, control.CapabilityDeviceRead,
	); !errors.Is(err, ErrAuthorizationDenied) {
		t.Fatalf("cross-tree identity error = %v, want authorization denial", err)
	}
	repeatedTree, repeatedPrincipal, err := registry.EnsureRootTree(
		ctx, testControllerID, treeThreadID, testDeviceID, time.Unix(3, 0),
	)
	if err != nil || tree != repeatedTree || !reflect.DeepEqual(principal, repeatedPrincipal) {
		t.Fatalf("repeated tree = %#v, %#v, error %v", repeatedTree, repeatedPrincipal, err)
	}
	if _, err := registry.RegisterTrustedDevice(
		ctx,
		deviceDescriptor(testControllerID, deviceSecondID, control.DeviceRoleController),
		time.Unix(3, 0),
	); err != nil {
		t.Fatal(err)
	}
	if _, _, err := registry.EnsureRootTree(
		ctx, testControllerID, treeThreadID, deviceSecondID, time.Unix(3, 0),
	); !errors.Is(err, ErrConflict) {
		t.Fatalf("root rebind error = %v, want ErrConflict", err)
	}
	if _, err := registry.RegisterTrustedDevice(
		ctx,
		deviceDescriptor(testControllerID, deviceThirdID, control.DeviceRoleWorker),
		time.Unix(3, 0),
	); err != nil {
		t.Fatal(err)
	}
	if _, _, err := registry.EnsureRootTree(
		ctx, testControllerID, treeSecondThreadID, deviceThirdID, time.Unix(3, 0),
	); !errors.Is(err, ErrConflict) {
		t.Fatalf("worker root creation error = %v, want ErrConflict", err)
	}
	if _, err := registry.MarkDeviceOffline(
		ctx, testControllerID, testDeviceID, 1, time.Unix(4, 0),
	); err != nil {
		t.Fatal(err)
	}
	if _, _, err := registry.EnsureRootTree(
		ctx, testControllerID, treeSecondThreadID, testDeviceID, time.Unix(4, 0),
	); !errors.Is(err, ErrConflict) {
		t.Fatalf("offline root creation error = %v, want ErrConflict", err)
	}
	if existing, _, err := registry.EnsureRootTree(
		ctx, testControllerID, treeThreadID, testDeviceID, time.Unix(4, 0),
	); err != nil || existing != tree {
		t.Fatalf("offline existing tree = %#v, error %v", existing, err)
	}
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}
	registry, err = Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	persistedTree, persistedPrincipal, err := registry.EnsureRootTree(
		ctx, testControllerID, treeThreadID, testDeviceID, time.Unix(5, 0),
	)
	if err != nil || persistedTree != tree || !reflect.DeepEqual(persistedPrincipal, principal) {
		t.Fatalf("persisted tree = %#v, %#v, error %v", persistedTree, persistedPrincipal, err)
	}
}

func TestConcurrentRootTreeCreationIsIdempotent(t *testing.T) {
	registry := openTestStore(t)
	if _, err := registry.RegisterTrustedDevice(
		context.Background(),
		deviceDescriptor(testControllerID, testDeviceID, control.DeviceRoleController),
		time.Unix(1, 0),
	); err != nil {
		t.Fatal(err)
	}
	type result struct {
		tree      control.Tree
		principal control.Principal
		err       error
	}
	results := make(chan result, 32)
	start := make(chan struct{})
	var wait sync.WaitGroup
	for range 32 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			tree, principal, err := registry.EnsureRootTree(
				context.Background(), testControllerID, treeThreadID, testDeviceID, time.Unix(2, 0),
			)
			results <- result{tree: tree, principal: principal, err: err}
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	var first result
	for current := range results {
		if current.err != nil {
			t.Fatal(current.err)
		}
		if first.tree.TreeID == "" {
			first = current
			continue
		}
		if current.tree != first.tree || !reflect.DeepEqual(current.principal, first.principal) {
			t.Fatalf("concurrent tree = %#v, %#v; want %#v, %#v", current.tree, current.principal, first.tree, first.principal)
		}
	}
}

func TestRootTreeCreationRollsBackAndStoredCapabilitiesFailClosed(t *testing.T) {
	registry := openTestStore(t)
	ctx := context.Background()
	if _, err := registry.RegisterTrustedDevice(
		ctx,
		deviceDescriptor(testControllerID, testDeviceID, control.DeviceRoleController),
		time.Unix(1, 0),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.db.Exec(`
CREATE TRIGGER reject_principal BEFORE INSERT ON principals
BEGIN SELECT RAISE(ABORT, 'injected principal failure'); END
`); err != nil {
		t.Fatal(err)
	}
	if _, _, err := registry.EnsureRootTree(
		ctx, testControllerID, treeThreadID, testDeviceID, time.Unix(2, 0),
	); err == nil {
		t.Fatal("root tree creation survived principal failure")
	}
	for _, table := range []string{"trees", "principals"} {
		var count int
		if err := registry.db.QueryRow("SELECT count(*) FROM " + table).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("%s retained %d rows after rollback", table, count)
		}
	}
	if _, err := registry.db.Exec("DROP TRIGGER reject_principal"); err != nil {
		t.Fatal(err)
	}
	_, principal, err := registry.EnsureRootTree(
		ctx, testControllerID, treeThreadID, testDeviceID, time.Unix(2, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	forgedAgentID := "123e4567-e89b-42d3-a456-426614174042"
	rootCapabilities, err := json.Marshal(control.RootCapabilities())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.db.Exec(`
INSERT INTO principals(
    controller_id, tree_id, agent_id, parent_agent_id, device_id, capabilities_json, created_at
) VALUES (?, ?, ?, '', ?, ?, 2)
`, testControllerID, principal.TreeID, forgedAgentID, deviceSecondID, string(rootCapabilities)); err != nil {
		t.Fatal(err)
	}
	forgedRoot := control.NewRootPrincipal(
		testControllerID, principal.TreeID, forgedAgentID, deviceSecondID,
	)
	if _, err := registry.AuthorizePrincipal(
		ctx, forgedRoot.Identity(), control.CapabilityDeviceRead,
	); !errors.Is(err, ErrAuthorizationDenied) {
		t.Fatalf("non-canonical root error = %v, want authorization denial", err)
	}
	injectedCapabilities := append(control.RootCapabilities(), control.CapabilityArtifactApply)
	slices.Sort(injectedCapabilities)
	injected, err := json.Marshal(injectedCapabilities)
	if err != nil {
		t.Fatal(err)
	}
	for _, corrupted := range []string{
		string(injected),
		"\x00" + strings.Repeat("x", maximumCapabilityJSON+1),
	} {
		if _, err := registry.db.Exec(`
UPDATE principals SET capabilities_json = ?
WHERE controller_id = ? AND tree_id = ? AND agent_id = ?
`, corrupted, principal.ControllerID, principal.TreeID, principal.AgentID); err != nil {
			t.Fatal(err)
		}
		if _, err := registry.AuthorizePrincipal(
			ctx, principal.Identity(), control.CapabilityDeviceRead,
		); err == nil {
			t.Fatal("authorization accepted corrupt stored capabilities")
		}
	}
	canonical, err := json.Marshal(control.RootCapabilities())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.db.Exec(`
UPDATE principals SET capabilities_json = ?
WHERE controller_id = ? AND tree_id = ? AND agent_id = ?
`, string(canonical), principal.ControllerID, principal.TreeID, principal.AgentID); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.AuthorizePrincipal(
		ctx, principal.Identity(), control.CapabilityDeviceRead,
	); err != nil {
		t.Fatalf("authorization after capability repair: %v", err)
	}
	if _, err := registry.db.Exec(`
DELETE FROM principals WHERE controller_id = ? AND tree_id = ? AND agent_id = ?
`, principal.ControllerID, principal.TreeID, principal.AgentID); err != nil {
		t.Fatal(err)
	}
	if _, _, err := registry.EnsureRootTree(
		ctx, testControllerID, treeThreadID, testDeviceID, time.Unix(3, 0),
	); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing root principal error = %v, want ErrNotFound", err)
	}
	var treeCount int
	if err := registry.db.QueryRow("SELECT count(*) FROM trees").Scan(&treeCount); err != nil {
		t.Fatal(err)
	}
	if treeCount != 1 {
		t.Fatalf("missing principal caused %d tree rows, want 1", treeCount)
	}
}

func TestWorkerAuthorizationRequiresParentInSameControllerTree(t *testing.T) {
	registry := openTestStore(t)
	ctx := context.Background()
	if _, err := registry.RegisterTrustedDevice(
		ctx,
		deviceDescriptor(testControllerID, testDeviceID, control.DeviceRoleController),
		time.Unix(1, 0),
	); err != nil {
		t.Fatal(err)
	}
	firstTree, firstRoot, err := registry.EnsureRootTree(
		ctx, testControllerID, treeThreadID, testDeviceID, time.Unix(2, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	_, otherTreeRoot, err := registry.EnsureRootTree(
		ctx, testControllerID, treeSecondThreadID, testDeviceID, time.Unix(2, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.RegisterTrustedDevice(
		ctx,
		deviceDescriptor(deviceSecondControllerID, testDeviceID, control.DeviceRoleController),
		time.Unix(1, 0),
	); err != nil {
		t.Fatal(err)
	}
	_, otherControllerRoot, err := registry.EnsureRootTree(
		ctx, deviceSecondControllerID, treeThirdThreadID, testDeviceID, time.Unix(2, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	worker := control.NewWorkerPrincipal(
		testControllerID,
		firstTree.TreeID,
		"123e4567-e89b-42d3-a456-426614174044",
		firstRoot.AgentID,
		deviceSecondID,
	)
	insertStoredWorker(t, registry, worker)
	if authorized, err := registry.AuthorizePrincipal(
		ctx, worker.Identity(), control.CapabilityMessageSendParent,
	); err != nil || !reflect.DeepEqual(authorized, worker) {
		t.Fatalf("authorized worker = %#v, error %v", authorized, err)
	}
	forgedClaim := worker.Identity()
	forgedClaim.ParentAgentID = otherTreeRoot.AgentID
	if _, err := registry.AuthorizePrincipal(
		ctx, forgedClaim, control.CapabilityMessageSendParent,
	); !errors.Is(err, ErrAuthorizationDenied) {
		t.Fatalf("forged worker parent error = %v, want authorization denial", err)
	}
	for index, parentID := range []string{
		"123e4567-e89b-42d3-a456-426614174045",
		otherTreeRoot.AgentID,
		otherControllerRoot.AgentID,
	} {
		candidate := control.NewWorkerPrincipal(
			testControllerID,
			firstTree.TreeID,
			[]string{
				"123e4567-e89b-42d3-a456-426614174046",
				"123e4567-e89b-42d3-a456-426614174047",
				"123e4567-e89b-42d3-a456-426614174048",
			}[index],
			parentID,
			deviceSecondID,
		)
		insertStoredWorker(t, registry, candidate)
		if _, err := registry.AuthorizePrincipal(
			ctx, candidate.Identity(), control.CapabilityMessageSendParent,
		); !errors.Is(err, ErrAuthorizationDenied) {
			t.Fatalf("worker parent %d error = %v, want authorization denial", index, err)
		}
	}
}

func insertStoredWorker(t *testing.T, registry *Store, worker control.Principal) {
	t.Helper()
	capabilities, err := json.Marshal(worker.Capabilities)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.db.Exec(`
INSERT INTO principals(
    controller_id, tree_id, agent_id, parent_agent_id, device_id, capabilities_json, created_at
) VALUES (?, ?, ?, ?, ?, ?, 3)
`,
		worker.ControllerID,
		worker.TreeID,
		worker.AgentID,
		worker.ParentAgentID,
		worker.DeviceID,
		string(capabilities),
	); err != nil {
		t.Fatal(err)
	}
}
