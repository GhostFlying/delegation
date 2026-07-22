package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/identity"
	"github.com/GhostFlying/delegation/internal/protocol"
)

var (
	ErrMailboxCursorAhead       = errors.New("mailbox cursor is ahead of the stored sequence")
	ErrMailboxFull              = errors.New("mailbox pending message quota exceeded")
	ErrMailboxSequenceExhausted = errors.New("mailbox sequence is exhausted")
)

const (
	maximumPendingMailboxMessages = 1024
	// Keep one full pending quota plus a same-sized window of acknowledged
	// receipts. Pruning only acknowledged receipts bounds dedup state without
	// invalidating retries for messages that are still awaiting consumption.
	maximumMailboxReceipts = 2 * maximumPendingMailboxMessages
)

type MailboxDelivery struct {
	RecipientAgentID string
	MessageID        string
	Sequence         uint64
}

func (s *Store) SendMailboxMessage(
	ctx context.Context,
	source control.Principal,
	target protocol.MessageTarget,
	messageID, message string,
	createdAt time.Time,
) (MailboxDelivery, error) {
	if err := source.Validate(); err != nil {
		return MailboxDelivery{}, fmt.Errorf("message source: %w", err)
	}
	if err := target.Validate(); err != nil {
		return MailboxDelivery{}, err
	}
	if err := identity.ValidateID(messageID); err != nil {
		return MailboxDelivery{}, fmt.Errorf("messageId %w", err)
	}
	if err := protocol.ValidateMailboxMessage(message); err != nil {
		return MailboxDelivery{}, err
	}
	timestamp, err := unixTime(createdAt, "createdAt")
	if err != nil {
		return MailboxDelivery{}, err
	}

	var delivery MailboxDelivery
	err = s.withImmediateTransaction(ctx, func(connection *sql.Conn) error {
		storedSource, err := queryPrincipal(
			ctx, connection, source.ControllerID, source.TreeID, source.AgentID,
		)
		if err != nil || !storedSource.Matches(source.Identity()) {
			if err != nil && !errors.Is(err, ErrNotFound) {
				return err
			}
			return ErrAuthorizationDenied
		}
		recipientAgentID, err := resolveMessageRecipient(ctx, connection, storedSource, target)
		if err != nil {
			return err
		}
		if _, err := queryPrincipal(
			ctx, connection, source.ControllerID, source.TreeID, recipientAgentID,
		); err != nil {
			return err
		}
		existing, found, err := findMailboxDelivery(
			ctx, connection, source, recipientAgentID, messageID, message,
		)
		if err != nil {
			return err
		}
		if found {
			delivery = existing
			return nil
		}
		if _, err := connection.ExecContext(ctx, `
INSERT INTO mailboxes(controller_id, tree_id, recipient_agent_id, last_sequence)
VALUES (?, ?, ?, 0)
ON CONFLICT(controller_id, tree_id, recipient_agent_id) DO NOTHING
`, source.ControllerID, source.TreeID, recipientAgentID); err != nil {
			return fmt.Errorf("initialize mailbox: %w", err)
		}
		var pending int
		if err := connection.QueryRowContext(ctx, `
SELECT COUNT(*) FROM mailbox_messages
WHERE controller_id = ? AND tree_id = ? AND recipient_agent_id = ?
`, source.ControllerID, source.TreeID, recipientAgentID).Scan(&pending); err != nil {
			return fmt.Errorf("count pending mailbox messages: %w", err)
		}
		if pending >= maximumPendingMailboxMessages {
			return ErrMailboxFull
		}
		var lastSequence int64
		if err := connection.QueryRowContext(ctx, `
SELECT last_sequence FROM mailboxes
WHERE controller_id = ? AND tree_id = ? AND recipient_agent_id = ?
`, source.ControllerID, source.TreeID, recipientAgentID).Scan(&lastSequence); err != nil {
			return fmt.Errorf("load mailbox sequence: %w", err)
		}
		if lastSequence == math.MaxInt64 {
			return ErrMailboxSequenceExhausted
		}
		sequence := lastSequence + 1
		if err := pruneAcknowledgedMailboxReceipts(
			ctx,
			connection,
			source.ControllerID,
			source.TreeID,
			recipientAgentID,
		); err != nil {
			return err
		}
		if _, err := connection.ExecContext(ctx, `
UPDATE mailboxes SET last_sequence = ?
WHERE controller_id = ? AND tree_id = ? AND recipient_agent_id = ? AND last_sequence = ?
`, sequence, source.ControllerID, source.TreeID, recipientAgentID, lastSequence); err != nil {
			return fmt.Errorf("advance mailbox sequence: %w", err)
		}
		if _, err := connection.ExecContext(ctx, `
INSERT INTO mailbox_receipts(
    controller_id, tree_id, recipient_agent_id, sequence, message_id,
    source_agent_id, source_parent_agent_id, source_device_id, message
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
`,
			source.ControllerID,
			source.TreeID,
			recipientAgentID,
			sequence,
			messageID,
			source.AgentID,
			source.ParentAgentID,
			source.DeviceID,
			message,
		); err != nil {
			return fmt.Errorf("record mailbox message receipt: %w", err)
		}
		if _, err := connection.ExecContext(ctx, `
INSERT INTO mailbox_messages(
    controller_id, tree_id, recipient_agent_id, sequence, message_id,
    source_agent_id, source_parent_agent_id, source_device_id, message, created_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`,
			source.ControllerID,
			source.TreeID,
			recipientAgentID,
			sequence,
			messageID,
			source.AgentID,
			source.ParentAgentID,
			source.DeviceID,
			message,
			timestamp,
		); err != nil {
			return fmt.Errorf("enqueue mailbox message: %w", err)
		}
		delivery = MailboxDelivery{
			RecipientAgentID: recipientAgentID,
			MessageID:        messageID,
			Sequence:         uint64(sequence),
		}
		return nil
	})
	return delivery, err
}

func pruneAcknowledgedMailboxReceipts(
	ctx context.Context,
	connection *sql.Conn,
	controllerID, treeID, recipientAgentID string,
) error {
	var count int
	if err := connection.QueryRowContext(ctx, `
SELECT COUNT(*) FROM mailbox_receipts
WHERE controller_id = ? AND tree_id = ? AND recipient_agent_id = ?
`, controllerID, treeID, recipientAgentID).Scan(&count); err != nil {
		return fmt.Errorf("count mailbox receipts: %w", err)
	}
	toDelete := count - maximumMailboxReceipts + 1
	if toDelete <= 0 {
		return nil
	}
	result, err := connection.ExecContext(ctx, `
DELETE FROM mailbox_receipts
WHERE message_id IN (
    SELECT receipt.message_id
    FROM mailbox_receipts AS receipt
    LEFT JOIN mailbox_messages AS pending
      ON pending.message_id = receipt.message_id
    WHERE receipt.controller_id = ?
      AND receipt.tree_id = ?
      AND receipt.recipient_agent_id = ?
      AND pending.message_id IS NULL
    ORDER BY receipt.sequence
    LIMIT ?
)
`, controllerID, treeID, recipientAgentID, toDelete)
	if err != nil {
		return fmt.Errorf("prune acknowledged mailbox receipts: %w", err)
	}
	deleted, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("count pruned mailbox receipts: %w", err)
	}
	if deleted != int64(toDelete) {
		return errors.New("mailbox receipt window is full of pending messages")
	}
	return nil
}

func findMailboxDelivery(
	ctx context.Context,
	connection *sql.Conn,
	source control.Principal,
	recipientAgentID, messageID, message string,
) (MailboxDelivery, bool, error) {
	var (
		storedControllerID   string
		storedTreeID         string
		storedRecipientID    string
		storedSequence       int64
		storedSourceAgentID  string
		storedSourceParentID string
		storedSourceDeviceID string
		storedMessage        string
	)
	err := connection.QueryRowContext(ctx, `
SELECT controller_id, tree_id, recipient_agent_id, sequence,
       source_agent_id, source_parent_agent_id, source_device_id, message
FROM mailbox_receipts
WHERE message_id = ?
`, messageID).Scan(
		&storedControllerID,
		&storedTreeID,
		&storedRecipientID,
		&storedSequence,
		&storedSourceAgentID,
		&storedSourceParentID,
		&storedSourceDeviceID,
		&storedMessage,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return MailboxDelivery{}, false, nil
	}
	if err != nil {
		return MailboxDelivery{}, false, fmt.Errorf("find mailbox message receipt: %w", err)
	}
	if storedControllerID != source.ControllerID || storedTreeID != source.TreeID ||
		storedRecipientID != recipientAgentID || storedSourceAgentID != source.AgentID ||
		storedSourceParentID != source.ParentAgentID || storedSourceDeviceID != source.DeviceID ||
		storedMessage != message {
		return MailboxDelivery{}, false, ErrConflict
	}
	return MailboxDelivery{
		RecipientAgentID: storedRecipientID,
		MessageID:        messageID,
		Sequence:         uint64(storedSequence),
	}, true, nil
}

func resolveMessageRecipient(
	ctx context.Context,
	queryer rowQueryer,
	source control.Principal,
	target protocol.MessageTarget,
) (string, error) {
	if source.ParentAgentID == "" {
		if control.Require(source, control.CapabilityMessageTree) != nil ||
			target.Kind != protocol.MessageTargetAgent {
			return "", ErrAuthorizationDenied
		}
		return target.AgentID, nil
	}
	if control.Require(source, control.CapabilityMessageSendParent) != nil {
		return "", ErrAuthorizationDenied
	}
	switch target.Kind {
	case protocol.MessageTargetParent:
		return source.ParentAgentID, nil
	case protocol.MessageTargetRoot:
		tree, err := queryTreeByID(ctx, queryer, source.ControllerID, source.TreeID)
		if err != nil {
			return "", err
		}
		return tree.RootAgentID, nil
	case protocol.MessageTargetAgent:
		return "", ErrAuthorizationDenied
	default:
		return "", errors.New("message target was not validated")
	}
}

func (s *Store) ReadMailbox(
	ctx context.Context,
	recipient control.Principal,
	cursor uint64,
	limit int,
) (protocol.WaitMailboxResult, error) {
	if err := recipient.Validate(); err != nil {
		return protocol.WaitMailboxResult{}, fmt.Errorf("mailbox recipient: %w", err)
	}
	if cursor > math.MaxInt64 {
		return protocol.WaitMailboxResult{}, ErrMailboxCursorAhead
	}
	if limit < 1 || limit > protocol.MaximumMailboxPage {
		return protocol.WaitMailboxResult{}, fmt.Errorf(
			"mailbox page limit must be from 1 through %d",
			protocol.MaximumMailboxPage,
		)
	}
	required := control.CapabilityMessageTree
	if recipient.ParentAgentID != "" {
		required = control.CapabilityMessageReceiveSelf
	}
	if control.Require(recipient, required) != nil {
		return protocol.WaitMailboxResult{}, ErrAuthorizationDenied
	}

	result := protocol.WaitMailboxResult{
		Messages:   []protocol.MailboxMessage{},
		NextCursor: cursor,
	}
	err := s.withImmediateTransaction(ctx, func(connection *sql.Conn) error {
		storedRecipient, err := queryPrincipal(
			ctx, connection, recipient.ControllerID, recipient.TreeID, recipient.AgentID,
		)
		if err != nil || !storedRecipient.Matches(recipient.Identity()) {
			if err != nil && !errors.Is(err, ErrNotFound) {
				return err
			}
			return ErrAuthorizationDenied
		}

		var lastSequence int64
		err = connection.QueryRowContext(ctx, `
SELECT last_sequence FROM mailboxes
WHERE controller_id = ? AND tree_id = ? AND recipient_agent_id = ?
	`, recipient.ControllerID, recipient.TreeID, recipient.AgentID).Scan(&lastSequence)
		if errors.Is(err, sql.ErrNoRows) {
			if cursor != 0 {
				return ErrMailboxCursorAhead
			}
			return nil
		}
		if err != nil {
			return fmt.Errorf("load mailbox sequence: %w", err)
		}
		if cursor > uint64(lastSequence) {
			return ErrMailboxCursorAhead
		}
		if cursor != 0 {
			if _, err := connection.ExecContext(ctx, `
DELETE FROM mailbox_messages
WHERE controller_id = ? AND tree_id = ? AND recipient_agent_id = ? AND sequence <= ?
`, recipient.ControllerID, recipient.TreeID, recipient.AgentID, cursor); err != nil {
				return fmt.Errorf("compact acknowledged mailbox messages: %w", err)
			}
		}

		rows, err := connection.QueryContext(ctx, `
SELECT sequence, message_id, source_agent_id, source_parent_agent_id,
       source_device_id, message, created_at
FROM mailbox_messages
WHERE controller_id = ? AND tree_id = ? AND recipient_agent_id = ? AND sequence > ?
ORDER BY sequence
LIMIT ?
	`, recipient.ControllerID, recipient.TreeID, recipient.AgentID, cursor, limit)
		if err != nil {
			return fmt.Errorf("read mailbox messages: %w", err)
		}
		result.Messages = make([]protocol.MailboxMessage, 0, limit)
		for rows.Next() {
			var sequence int64
			var message protocol.MailboxMessage
			message.Source.ControllerID = recipient.ControllerID
			message.Source.TreeID = recipient.TreeID
			if err := rows.Scan(
				&sequence,
				&message.MessageID,
				&message.Source.AgentID,
				&message.Source.ParentAgentID,
				&message.Source.DeviceID,
				&message.Message,
				&message.CreatedAt,
			); err != nil {
				rows.Close()
				return fmt.Errorf("scan mailbox message: %w", err)
			}
			message.Sequence = uint64(sequence)
			if err := message.Validate(); err != nil {
				rows.Close()
				return fmt.Errorf("stored mailbox message is invalid: %w", err)
			}
			result.Messages = append(result.Messages, message)
			result.NextCursor = message.Sequence
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return fmt.Errorf("read mailbox messages: %w", err)
		}
		if err := rows.Close(); err != nil {
			return fmt.Errorf("close mailbox messages: %w", err)
		}
		return nil
	})
	return result, err
}
