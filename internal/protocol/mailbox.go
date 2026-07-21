package protocol

import "github.com/GhostFlying/delegation/internal/control"

const (
	MethodSendMessage = "message.send"
	MethodWaitMailbox = "message.wait"
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

type SendMessageParams struct {
	Target  MessageTarget `json:"target"`
	Message string        `json:"message"`
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

type MailboxMessage struct {
	MessageID string                    `json:"messageId"`
	Sequence  uint64                    `json:"sequence"`
	Source    control.PrincipalIdentity `json:"source"`
	Message   string                    `json:"message"`
	CreatedAt int64                     `json:"createdAt"`
}

type WaitMailboxResult struct {
	Messages   []MailboxMessage `json:"messages"`
	NextCursor uint64           `json:"nextCursor"`
}
