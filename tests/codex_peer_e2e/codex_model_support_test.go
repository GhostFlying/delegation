//go:build integration

package codex_peer_e2e

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

const codexAcceptanceModel = "gpt-6.1-sol"

// Responses Lite carries its tool declarations in history rather than tools[].
func modelToolDeclarations(body map[string]any) any {
	if tools, ok := body["tools"].([]any); ok {
		return tools
	}
	var tools []any
	inputs, _ := body["input"].([]any)
	for _, value := range inputs {
		item, _ := value.(map[string]any)
		if item["type"] == "additional_tools" {
			declared, _ := item["tools"].([]any)
			tools = append(tools, declared...)
		}
	}
	return tools
}

func modelToolExposed(tools any, name string) bool {
	encoded, _ := json.Marshal(tools)
	return strings.Contains(string(encoded), `"name":"`+name+`"`) ||
		strings.Contains(string(encoded), "### `"+name+"`")
}

type codexCodeModeWriter struct {
	http.ResponseWriter
	worker bool
}

func modelResponseWriter(writer http.ResponseWriter, body map[string]any, worker bool) http.ResponseWriter {
	if body["model"] == codexAcceptanceModel {
		return &codexCodeModeWriter{ResponseWriter: writer, worker: worker}
	}
	return writer
}

func (writer *codexCodeModeWriter) Flush() {
	if flusher, ok := writer.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

// Keep the scenario scripts identical while executing their tools through the
// current model's real functions.exec interface. TraeX retains Responses calls.
func modelResponseEvent(writer http.ResponseWriter, event map[string]any) map[string]any {
	mode, ok := writer.(*codexCodeModeWriter)
	if !ok || event["type"] != "response.output_item.done" {
		return event
	}
	item, _ := event["item"].(map[string]any)
	var code string
	switch item["type"] {
	case "function_call":
		name, _ := item["name"].(string)
		if namespace, _ := item["namespace"].(string); namespace != "" {
			name = namespace + "__" + name
		}
		arguments, _ := item["arguments"].(string)
		code = fmt.Sprintf("text(await tools.%s(%s));", name, arguments)
	case "tool_search_call":
		// Discover from the actual runtime catalog, including deferred MCP tools.
		code = `text({delegation_tools: ALL_TOOLS.filter(t => t.name.startsWith("mcp__delegation")).map(t => t.name)});`
	default:
		return event
	}
	if mode.worker {
		code = `if (ALL_TOOLS.some(t => /(^|__)(spawn_agent|list_agents|list_devices|describe_device|sync_workspace|followup_task|interrupt_agent|send_input|resume_agent|close_agent|send_message|wait_agent)$/.test(t.name))) throw new Error("worker exposes recursive delegation authority");
if (!ALL_TOOLS.some(t => t.name === "mcp__delegation_worker__send_upstream_message") || !ALL_TOOLS.some(t => t.name === "mcp__delegation_worker__wait_for_upstream_message")) throw new Error("worker mailbox discovery is missing");
` + code
	}
	return map[string]any{
		"type": event["type"],
		"item": map[string]any{
			"type": "custom_tool_call", "call_id": item["call_id"],
			"namespace": "functions", "name": "exec", "input": code,
		},
	}
}

func modelOutputText(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case []any:
		var parts []string
		for _, value := range typed {
			if item, ok := value.(map[string]any); ok {
				if text, ok := item["text"].(string); ok {
					parts = append(parts, text)
				}
			}
		}
		return strings.Join(parts, "\n")
	default:
		encoded, _ := json.Marshal(value)
		return string(encoded)
	}
}
