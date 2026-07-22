package protocol

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"unicode/utf8"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/identity"
)

const (
	MethodSendMessage        = "message.send"
	MethodWaitMailbox        = "message.wait"
	MaximumMailboxPage       = 32
	MaximumMailboxWaitMillis = 25_000
	// MaximumMailboxMessageBytes is enforced before enqueue and again before
	// delivery so one message cannot permanently block a bounded mailbox page.
	MaximumMailboxMessageBytes = 1024
)

type MessageTargetKind string

const (
	MessageTargetParent MessageTargetKind = "parent"
	MessageTargetRoot   MessageTargetKind = "root"
	MessageTargetAgent  MessageTargetKind = "agent"
)

type MessageTarget struct {
	Kind    MessageTargetKind `json:"kind"`
	AgentID string            `json:"agentId,omitempty"`
}

func (t MessageTarget) Validate() error {
	switch t.Kind {
	case MessageTargetParent, MessageTargetRoot:
		if t.AgentID != "" {
			return errors.New("parent and root message targets must not contain agentId")
		}
	case MessageTargetAgent:
		if err := identity.ValidateID(t.AgentID); err != nil {
			return fmt.Errorf("message target agentId %w", err)
		}
	default:
		return fmt.Errorf("unsupported message target %q", t.Kind)
	}
	return nil
}

type SendMessageParams struct {
	MessageID string        `json:"messageId"`
	Target    MessageTarget `json:"target"`
	Message   string        `json:"message"`
}

func (p SendMessageParams) Validate() error {
	if err := identity.ValidateID(p.MessageID); err != nil {
		return fmt.Errorf("messageId %w", err)
	}
	if err := p.Target.Validate(); err != nil {
		return err
	}
	return ValidateMailboxMessage(p.Message)
}

type SendMessageResult struct {
	MessageID string `json:"messageId"`
	Sequence  uint64 `json:"sequence"`
}

type WaitMailboxParams struct {
	Cursor        uint64 `json:"cursor,omitempty"`
	TimeoutMillis int    `json:"timeoutMillis"`
	Limit         int    `json:"limit"`
}

func (p WaitMailboxParams) Validate() error {
	if p.Cursor > math.MaxInt64 {
		return errors.New("mailbox cursor exceeds the supported range")
	}
	if p.TimeoutMillis < 0 || p.TimeoutMillis > MaximumMailboxWaitMillis {
		return fmt.Errorf("timeoutMillis must be from 0 through %d", MaximumMailboxWaitMillis)
	}
	if p.Limit < 1 || p.Limit > MaximumMailboxPage {
		return fmt.Errorf("limit must be from 1 through %d", MaximumMailboxPage)
	}
	return nil
}

type MailboxMessage struct {
	MessageID string                    `json:"messageId"`
	Sequence  uint64                    `json:"sequence"`
	Source    control.PrincipalIdentity `json:"source"`
	Message   string                    `json:"message"`
	CreatedAt int64                     `json:"createdAt"`
}

func (m MailboxMessage) Validate() error {
	if err := identity.ValidateID(m.MessageID); err != nil {
		return fmt.Errorf("messageId %w", err)
	}
	if m.Sequence == 0 {
		return errors.New("message sequence must be positive")
	}
	if err := m.Source.Validate(); err != nil {
		return fmt.Errorf("message source: %w", err)
	}
	if err := ValidateMailboxMessage(m.Message); err != nil {
		return err
	}
	if m.CreatedAt < 0 {
		return errors.New("message createdAt must not be negative")
	}
	return nil
}

func ValidateMailboxMessage(message string) error {
	if strings.TrimSpace(message) == "" || len(message) > MaximumMailboxMessageBytes ||
		!utf8.ValidString(message) || strings.ContainsRune(message, '\x00') {
		return fmt.Errorf(
			"message must contain from 1 through %d bytes of valid text",
			MaximumMailboxMessageBytes,
		)
	}
	return nil
}

type WaitMailboxResult struct {
	Messages   []MailboxMessage `json:"messages"`
	NextCursor uint64           `json:"nextCursor"`
}
