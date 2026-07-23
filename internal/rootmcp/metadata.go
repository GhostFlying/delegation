package rootmcp

import (
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/GhostFlying/delegation/internal/identity"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const sandboxStateMetaCapability = "codex/sandbox-state-meta"

type codexToolMetadata struct {
	ThreadID string
	CWD      string
}

func toolMetadata(request *mcp.CallToolRequest, requireCWD bool) (codexToolMetadata, error) {
	if request == nil || request.Params == nil {
		return codexToolMetadata{}, errors.New("Codex did not provide tool-call metadata")
	}
	threadValue, found := request.Params.Meta["threadId"]
	if !found {
		return codexToolMetadata{}, errors.New("Codex did not provide _meta.threadId; start a new Codex task and retry")
	}
	threadID, ok := threadValue.(string)
	if !ok {
		return codexToolMetadata{}, errors.New("Codex provided a non-string _meta.threadId")
	}
	if err := identity.ValidateID(threadID); err != nil {
		return codexToolMetadata{}, fmt.Errorf("Codex _meta.threadId %w", err)
	}
	metadata := codexToolMetadata{ThreadID: threadID}
	if !requireCWD {
		return metadata, nil
	}
	sandboxValue, found := request.Params.Meta[sandboxStateMetaCapability]
	if !found {
		return codexToolMetadata{}, errors.New("Codex did not provide trusted sandbox cwd metadata; update Codex, start a new task, and retry")
	}
	sandbox, ok := sandboxValue.(map[string]any)
	if !ok {
		return codexToolMetadata{}, errors.New("Codex provided malformed sandbox cwd metadata")
	}
	cwdValue, found := sandbox["sandboxCwd"]
	if !found {
		return codexToolMetadata{}, errors.New("Codex sandbox metadata did not contain sandboxCwd")
	}
	cwdURI, ok := cwdValue.(string)
	if !ok {
		return codexToolMetadata{}, errors.New("Codex provided a non-string sandboxCwd")
	}
	cwd, err := localPathFromFileURI(cwdURI)
	if err != nil {
		return codexToolMetadata{}, fmt.Errorf("Codex sandboxCwd: %w", err)
	}
	metadata.CWD = cwd
	return metadata, nil
}

func threadID(request *mcp.CallToolRequest) (string, error) {
	metadata, err := toolMetadata(request, false)
	return metadata.ThreadID, err
}

func localPathFromFileURI(raw string) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "file" || parsed.Opaque != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", errors.New("must be a local file URI")
	}
	if strings.ContainsRune(parsed.Path, '\x00') {
		return "", errors.New("contains a NUL path byte")
	}
	var path string
	if runtime.GOOS == "windows" {
		if parsed.Host != "" && !strings.EqualFold(parsed.Host, "localhost") {
			path = `\\` + parsed.Host + filepath.FromSlash(parsed.Path)
		} else {
			path = filepath.FromSlash(strings.TrimPrefix(parsed.Path, "/"))
		}
	} else {
		if parsed.Host != "" && parsed.Host != "localhost" {
			return "", errors.New("must not contain a remote file authority")
		}
		path = filepath.FromSlash(parsed.Path)
	}
	path = filepath.Clean(path)
	if !filepath.IsAbs(path) {
		return "", errors.New("must identify an absolute local path")
	}
	return path, nil
}
