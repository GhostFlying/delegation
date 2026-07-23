package workerhost

import (
	"encoding/json"
)

type threadStartParams struct {
	CWD                   string         `json:"cwd"`
	RuntimeWorkspaceRoots []string       `json:"runtimeWorkspaceRoots"`
	ApprovalPolicy        string         `json:"approvalPolicy"`
	Config                map[string]any `json:"config"`
	ServiceName           string         `json:"serviceName"`
	ThreadSource          string         `json:"threadSource"`
	DeveloperMessage      string         `json:"developerInstructions"`
}

type threadResumeParams struct {
	ThreadID              string         `json:"threadId"`
	CWD                   string         `json:"cwd"`
	RuntimeWorkspaceRoots []string       `json:"runtimeWorkspaceRoots"`
	ApprovalPolicy        string         `json:"approvalPolicy"`
	Config                map[string]any `json:"config"`
	DeveloperMessage      string         `json:"developerInstructions"`
	ExcludeTurns          bool           `json:"excludeTurns"`
}

type threadResult struct {
	Thread struct {
		ID string `json:"id"`
	} `json:"thread"`
	CWD                     string   `json:"cwd"`
	RuntimeWorkspaceRoots   []string `json:"runtimeWorkspaceRoots"`
	ActivePermissionProfile *struct {
		ID string `json:"id"`
	} `json:"activePermissionProfile"`
}

type mcpStatusParams struct {
	Cursor   *string `json:"cursor,omitempty"`
	Limit    int     `json:"limit"`
	Detail   string  `json:"detail"`
	ThreadID string  `json:"threadId"`
}

type mcpStatusPage struct {
	Data       []mcpServerStatus `json:"data"`
	NextCursor *string           `json:"nextCursor"`
}

type mcpServerStatus struct {
	Name              string                     `json:"name"`
	Tools             map[string]json.RawMessage `json:"tools"`
	Resources         []json.RawMessage          `json:"resources"`
	ResourceTemplates []json.RawMessage          `json:"resourceTemplates"`
	AuthStatus        string                     `json:"authStatus"`
}

type turnStartParams struct {
	ThreadID string      `json:"threadId"`
	Input    []textInput `json:"input"`
}

type textInput struct {
	Type         string `json:"type"`
	Text         string `json:"text"`
	TextElements []any  `json:"text_elements"`
}

type turnStartResult struct {
	Turn turn `json:"turn"`
}

type turn struct {
	ID     string          `json:"id"`
	Status string          `json:"status"`
	Error  json.RawMessage `json:"error"`
}

type turnCompletedNotification struct {
	ThreadID string `json:"threadId"`
	Turn     turn   `json:"turn"`
}
