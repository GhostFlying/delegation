package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/identity"
	"github.com/GhostFlying/delegation/internal/protocol"
)

const (
	mailboxWorkerID       = "123e4567-e89b-42d3-a456-426614174050"
	mailboxSecondWorkerID = "123e4567-e89b-42d3-a456-426614174051"
)

func TestMailboxRoutesPersistedMessagesWithIndependentSequences(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "broker.sqlite3")
	registry, err := Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	root, worker, _ := createMailboxPrincipals(t, registry)

	first := sendStoredMessage(t, registry, worker, protocol.MessageTarget{
		Kind: protocol.MessageTargetParent,
	}, "worker to parent", time.Unix(4, 0))
	second := sendStoredMessage(t, registry, worker, protocol.MessageTarget{
		Kind: protocol.MessageTargetRoot,
	}, "worker to root", time.Unix(5, 0))
	workerDelivery := sendStoredMessage(t, registry, root, protocol.MessageTarget{
		Kind: protocol.MessageTargetAgent, AgentID: worker.AgentID,
	}, "root to worker", time.Unix(6, 0))
	if first.RecipientAgentID != root.AgentID || first.Sequence != 1 ||
		second.RecipientAgentID != root.AgentID || second.Sequence != 2 ||
		workerDelivery.RecipientAgentID != worker.AgentID || workerDelivery.Sequence != 1 {
		t.Fatalf("mailbox deliveries = %#v, %#v, %#v", first, second, workerDelivery)
	}

	rootPage, err := registry.ReadMailbox(context.Background(), root, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(rootPage.Messages) != 1 || rootPage.NextCursor != 1 ||
		rootPage.Messages[0].Source != worker.Identity() || rootPage.Messages[0].Message != "worker to parent" {
		t.Fatalf("first root mailbox page = %#v", rootPage)
	}
	rootPage, err = registry.ReadMailbox(context.Background(), root, rootPage.NextCursor, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(rootPage.Messages) != 1 || rootPage.NextCursor != 2 ||
		rootPage.Messages[0].Message != "worker to root" {
		t.Fatalf("second root mailbox page = %#v", rootPage)
	}
	empty, err := registry.ReadMailbox(context.Background(), root, rootPage.NextCursor, 1)
	if err != nil || len(empty.Messages) != 0 || empty.NextCursor != rootPage.NextCursor {
		t.Fatalf("empty root mailbox page = %#v, %v", empty, err)
	}
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}

	registry, err = Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	persisted, err := registry.ReadMailbox(context.Background(), worker, 0, 1)
	if err != nil || len(persisted.Messages) != 1 || persisted.NextCursor != 1 ||
		persisted.Messages[0].Message != "root to worker" || persisted.Messages[0].Source != root.Identity() {
		t.Fatalf("persisted worker mailbox = %#v, %v", persisted, err)
	}
}

func TestMailboxConcurrentSendAllocatesContiguousSequences(t *testing.T) {
	registry := openTestStore(t)
	root, worker, _ := createMailboxPrincipals(t, registry)
	const count = 24
	sequences := make(chan uint64, count)
	errorsChannel := make(chan error, count)
	start := make(chan struct{})
	var wait sync.WaitGroup
	for index := range count {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			messageID, err := identity.NewID()
			if err != nil {
				errorsChannel <- err
				return
			}
			delivery, err := registry.SendMailboxMessage(
				context.Background(),
				root,
				protocol.MessageTarget{Kind: protocol.MessageTargetAgent, AgentID: worker.AgentID},
				messageID,
				fmt.Sprintf("message %d", index),
				time.Unix(10, 0),
			)
			if err != nil {
				errorsChannel <- err
				return
			}
			sequences <- delivery.Sequence
		}()
	}
	close(start)
	wait.Wait()
	close(sequences)
	close(errorsChannel)
	for err := range errorsChannel {
		t.Fatal(err)
	}
	got := make([]uint64, 0, count)
	for sequence := range sequences {
		got = append(got, sequence)
	}
	slices.Sort(got)
	want := make([]uint64, count)
	for index := range count {
		want[index] = uint64(index + 1)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("mailbox sequences = %v, want %v", got, want)
	}
}

func TestMailboxSendIsIdempotentAndCursorCompactsPayload(t *testing.T) {
	registry := openTestStore(t)
	root, worker, secondWorker := createMailboxPrincipals(t, registry)
	messageID := "123e4567-e89b-42d3-a456-426614174052"
	target := protocol.MessageTarget{Kind: protocol.MessageTargetAgent, AgentID: worker.AgentID}
	first, err := registry.SendMailboxMessage(
		context.Background(), root, target, messageID, "idempotent message", time.Unix(10, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	retry, err := registry.SendMailboxMessage(
		context.Background(), root, target, messageID, "idempotent message", time.Unix(11, 0),
	)
	if err != nil || !reflect.DeepEqual(retry, first) {
		t.Fatalf("idempotent retry = %#v, %v; want %#v", retry, err, first)
	}
	if _, err := registry.SendMailboxMessage(
		context.Background(), root, target, messageID, "conflicting message", time.Unix(11, 0),
	); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflicting messageId reuse = %v, want ErrConflict", err)
	}
	if _, err := registry.SendMailboxMessage(
		context.Background(),
		root,
		protocol.MessageTarget{Kind: protocol.MessageTargetAgent, AgentID: secondWorker.AgentID},
		messageID,
		"idempotent message",
		time.Unix(11, 0),
	); !errors.Is(err, ErrConflict) {
		t.Fatalf("messageId reuse with another resolved recipient = %v, want ErrConflict", err)
	}
	sourceBoundID := "123e4567-e89b-42d3-a456-426614174055"
	if _, err := registry.SendMailboxMessage(
		context.Background(),
		worker,
		protocol.MessageTarget{Kind: protocol.MessageTargetParent},
		sourceBoundID,
		"source-bound message",
		time.Unix(11, 0),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.SendMailboxMessage(
		context.Background(),
		root,
		protocol.MessageTarget{Kind: protocol.MessageTargetAgent, AgentID: root.AgentID},
		sourceBoundID,
		"source-bound message",
		time.Unix(11, 0),
	); !errors.Is(err, ErrConflict) {
		t.Fatalf("messageId reuse from another source = %v, want ErrConflict", err)
	}

	page, err := registry.ReadMailbox(context.Background(), worker, 0, 1)
	if err != nil || len(page.Messages) != 1 || page.NextCursor != first.Sequence {
		t.Fatalf("mailbox page = %#v, %v", page, err)
	}
	acknowledged, err := registry.ReadMailbox(context.Background(), worker, page.NextCursor, 1)
	if err != nil || len(acknowledged.Messages) != 0 || acknowledged.NextCursor != first.Sequence {
		t.Fatalf("acknowledged mailbox page = %#v, %v", acknowledged, err)
	}
	if pending := pendingMailboxCount(t, registry, worker); pending != 0 {
		t.Fatalf("pending messages after cursor acknowledgement = %d, want 0", pending)
	}
	retry, err = registry.SendMailboxMessage(
		context.Background(), root, target, messageID, "idempotent message", time.Unix(12, 0),
	)
	if err != nil || !reflect.DeepEqual(retry, first) {
		t.Fatalf("post-ack idempotent retry = %#v, %v; want %#v", retry, err, first)
	}
	if pending := pendingMailboxCount(t, registry, worker); pending != 0 {
		t.Fatalf("post-ack retry recreated %d pending messages", pending)
	}
}

func TestMailboxQuotaAndBoundedReceiptWindowPreservePendingDedup(t *testing.T) {
	registry := openTestStore(t)
	root, worker, _ := createMailboxPrincipals(t, registry)
	ctx := context.Background()
	err := registry.withImmediateTransaction(ctx, func(connection *sql.Conn) error {
		if _, err := connection.ExecContext(ctx, `
INSERT INTO mailboxes(controller_id, tree_id, recipient_agent_id, last_sequence)
VALUES (?, ?, ?, ?)
`, root.ControllerID, root.TreeID, worker.AgentID, maximumMailboxReceipts); err != nil {
			return err
		}
		receiptStatement, err := connection.PrepareContext(ctx, `
INSERT INTO mailbox_receipts(
    controller_id, tree_id, recipient_agent_id, sequence, message_id,
    source_agent_id, source_parent_agent_id, source_device_id, message
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
`)
		if err != nil {
			return err
		}
		defer receiptStatement.Close()
		messageStatement, err := connection.PrepareContext(ctx, `
INSERT INTO mailbox_messages(
    controller_id, tree_id, recipient_agent_id, sequence, message_id,
    source_agent_id, source_parent_agent_id, source_device_id, message, created_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`)
		if err != nil {
			return err
		}
		defer messageStatement.Close()
		for sequence := 1; sequence <= maximumMailboxReceipts; sequence++ {
			messageID := mailboxTestMessageID(sequence)
			message := fmt.Sprintf("message %d", sequence)
			arguments := []any{
				root.ControllerID, root.TreeID, worker.AgentID, sequence, messageID,
				root.AgentID, root.ParentAgentID, root.DeviceID, message,
			}
			if _, err := receiptStatement.ExecContext(ctx, arguments...); err != nil {
				return err
			}
			if sequence > maximumPendingMailboxMessages {
				if _, err := messageStatement.ExecContext(ctx, append(arguments, int64(10))...); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if pending := pendingMailboxCount(t, registry, worker); pending != maximumPendingMailboxMessages {
		t.Fatalf("seeded pending messages = %d, want %d", pending, maximumPendingMailboxMessages)
	}
	target := protocol.MessageTarget{Kind: protocol.MessageTargetAgent, AgentID: worker.AgentID}
	if _, err := registry.SendMailboxMessage(
		ctx, root, target, "123e4567-e89b-42d3-a456-426614174053", "over quota", time.Unix(11, 0),
	); !errors.Is(err, ErrMailboxFull) {
		t.Fatalf("over-quota send = %v, want ErrMailboxFull", err)
	}
	pendingID := mailboxTestMessageID(maximumPendingMailboxMessages + 2)
	pendingMessage := fmt.Sprintf("message %d", maximumPendingMailboxMessages+2)
	retry, err := registry.SendMailboxMessage(
		ctx, root, target, pendingID, pendingMessage, time.Unix(11, 0),
	)
	if err != nil || retry.Sequence != uint64(maximumPendingMailboxMessages+2) {
		t.Fatalf("dedup at quota = %#v, %v", retry, err)
	}

	page, err := registry.ReadMailbox(ctx, worker, maximumPendingMailboxMessages+1, 1)
	if err != nil || len(page.Messages) != 1 || page.Messages[0].Sequence != uint64(maximumPendingMailboxMessages+2) {
		t.Fatalf("quota acknowledgement page = %#v, %v", page, err)
	}
	newID := "123e4567-e89b-42d3-a456-426614174054"
	delivery, err := registry.SendMailboxMessage(
		ctx, root, target, newID, "after acknowledgement", time.Unix(12, 0),
	)
	if err != nil || delivery.Sequence != uint64(maximumMailboxReceipts+1) {
		t.Fatalf("send after acknowledgement = %#v, %v", delivery, err)
	}
	var receiptCount int
	if err := registry.db.QueryRowContext(ctx, `
SELECT COUNT(*) FROM mailbox_receipts
WHERE controller_id = ? AND tree_id = ? AND recipient_agent_id = ?
`, root.ControllerID, root.TreeID, worker.AgentID).Scan(&receiptCount); err != nil {
		t.Fatal(err)
	}
	if receiptCount != maximumMailboxReceipts {
		t.Fatalf("bounded receipt count = %d, want %d", receiptCount, maximumMailboxReceipts)
	}
	var oldestCount, pendingReceiptCount int
	if err := registry.db.QueryRowContext(
		ctx, "SELECT COUNT(*) FROM mailbox_receipts WHERE message_id = ?", mailboxTestMessageID(1),
	).Scan(&oldestCount); err != nil {
		t.Fatal(err)
	}
	if err := registry.db.QueryRowContext(
		ctx, "SELECT COUNT(*) FROM mailbox_receipts WHERE message_id = ?", pendingID,
	).Scan(&pendingReceiptCount); err != nil {
		t.Fatal(err)
	}
	if oldestCount != 0 || pendingReceiptCount != 1 {
		t.Fatalf("receipt pruning kept oldest=%d pending=%d, want 0 and 1", oldestCount, pendingReceiptCount)
	}
}

func TestMailboxRejectsForgedAuthorityTargetsAndFutureCursor(t *testing.T) {
	registry := openTestStore(t)
	root, worker, secondWorker := createMailboxPrincipals(t, registry)
	forged := worker
	forged.DeviceID = testDeviceID
	messageID, err := identity.NewID()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.SendMailboxMessage(
		context.Background(),
		forged,
		protocol.MessageTarget{Kind: protocol.MessageTargetParent},
		messageID,
		"forged",
		time.Unix(4, 0),
	); !errors.Is(err, ErrAuthorizationDenied) {
		t.Fatalf("forged source error = %v, want authorization denial", err)
	}
	messageID, _ = identity.NewID()
	if _, err := registry.SendMailboxMessage(
		context.Background(),
		worker,
		protocol.MessageTarget{Kind: protocol.MessageTargetAgent, AgentID: secondWorker.AgentID},
		messageID,
		"forbidden sibling",
		time.Unix(4, 0),
	); !errors.Is(err, ErrAuthorizationDenied) {
		t.Fatalf("worker sibling target error = %v, want authorization denial", err)
	}
	messageID, _ = identity.NewID()
	if _, err := registry.SendMailboxMessage(
		context.Background(),
		root,
		protocol.MessageTarget{
			Kind:    protocol.MessageTargetAgent,
			AgentID: "123e4567-e89b-42d3-a456-426614174059",
		},
		messageID,
		"missing target",
		time.Unix(4, 0),
	); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing target error = %v, want not found", err)
	}
	valid := sendStoredMessage(t, registry, root, protocol.MessageTarget{
		Kind: protocol.MessageTargetAgent, AgentID: worker.AgentID,
	}, "valid", time.Unix(5, 0))
	if valid.Sequence != 1 {
		t.Fatalf("failed sends consumed a mailbox sequence: %#v", valid)
	}
	if _, err := registry.ReadMailbox(
		context.Background(), worker, valid.Sequence+1, 1,
	); !errors.Is(err, ErrMailboxCursorAhead) {
		t.Fatalf("future cursor error = %v, want ErrMailboxCursorAhead", err)
	}
}

func TestCreateWorkerPrincipalIsIdempotentAndBoundToStoredParentAndDevice(t *testing.T) {
	registry := openTestStore(t)
	root, worker, _ := createMailboxPrincipals(t, registry)
	repeated, err := registry.CreateWorkerPrincipal(
		context.Background(),
		worker.ControllerID,
		worker.TreeID,
		worker.AgentID,
		worker.ParentAgentID,
		worker.DeviceID,
		time.Unix(20, 0),
	)
	if err != nil || !reflect.DeepEqual(repeated, worker) {
		t.Fatalf("repeated worker = %#v, %v", repeated, err)
	}
	if _, err := registry.CreateWorkerPrincipal(
		context.Background(),
		worker.ControllerID,
		worker.TreeID,
		worker.AgentID,
		root.AgentID,
		testDeviceID,
		time.Unix(20, 0),
	); !errors.Is(err, ErrConflict) {
		t.Fatalf("worker identity conflict = %v, want ErrConflict", err)
	}
}

func createMailboxPrincipals(
	t *testing.T,
	registry *Store,
) (control.Principal, control.Principal, control.Principal) {
	t.Helper()
	ctx := context.Background()
	for _, deviceID := range []string{testDeviceID, deviceSecondID} {
		if _, err := registry.RegisterTrustedDevice(
			ctx, deviceDescriptor(testControllerID, deviceID), time.Unix(1, 0),
		); err != nil {
			t.Fatal(err)
		}
	}
	tree, root, err := registry.EnsureRootTree(
		ctx, testControllerID, treeThreadID, testDeviceID, time.Unix(2, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	worker, err := registry.CreateWorkerPrincipal(
		ctx,
		testControllerID,
		tree.TreeID,
		mailboxWorkerID,
		root.AgentID,
		deviceSecondID,
		time.Unix(3, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	secondWorker, err := registry.CreateWorkerPrincipal(
		ctx,
		testControllerID,
		tree.TreeID,
		mailboxSecondWorkerID,
		root.AgentID,
		deviceSecondID,
		time.Unix(3, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	return root, worker, secondWorker
}

func sendStoredMessage(
	t *testing.T,
	registry *Store,
	source control.Principal,
	target protocol.MessageTarget,
	message string,
	createdAt time.Time,
) MailboxDelivery {
	t.Helper()
	messageID, err := identity.NewID()
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := registry.SendMailboxMessage(
		context.Background(), source, target, messageID, message, createdAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	return delivery
}

func pendingMailboxCount(t *testing.T, registry *Store, recipient control.Principal) int {
	t.Helper()
	var count int
	if err := registry.db.QueryRowContext(context.Background(), `
SELECT COUNT(*) FROM mailbox_messages
WHERE controller_id = ? AND tree_id = ? AND recipient_agent_id = ?
`, recipient.ControllerID, recipient.TreeID, recipient.AgentID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func mailboxTestMessageID(sequence int) string {
	return fmt.Sprintf("123e4567-e89b-42d3-a456-%012x", sequence)
}
