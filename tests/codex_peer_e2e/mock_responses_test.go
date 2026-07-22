//go:build integration && linux

package codex_peer_e2e

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
)

type mockResponses struct {
	mu     sync.Mutex
	calls  map[string]int
	errors []string
}

func (m *mockResponses) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	peerLabel := request.Header.Get("x-delegation-test-peer")
	if request.Method != http.MethodPost || request.URL.Path != "/v1/responses" {
		m.fail(writer, fmt.Errorf("unexpected model request %s %s for peer %q", request.Method, request.URL.Path, peerLabel))
		return
	}
	body, err := decodeRequest(request)
	if err != nil {
		m.fail(writer, err)
		return
	}
	testCase := requestCase(body)
	if testCase == "" {
		m.fail(writer, errors.New("model request did not contain a known acceptance case"))
		return
	}
	if testCase == workerDispatchCase {
		key := "worker/" + testCase
		m.mu.Lock()
		call := m.calls[key]
		m.calls[key] = call + 1
		m.mu.Unlock()
		if call != 0 {
			m.record(fmt.Errorf("case %s received model call %d", key, call+1))
		}
		encodedTools, _ := json.Marshal(body["tools"])
		for _, forbidden := range []string{"spawn_agent", "list_agents", "list_devices", "describe_device"} {
			if bytes.Contains(encodedTools, []byte(forbidden)) {
				m.record(fmt.Errorf("case %s exposed root tool %s: %s", key, forbidden, encodedTools))
			}
		}
		writeFinalResponse(writer, key)
		return
	}
	if deviceIDs[peerLabel] == "" {
		m.fail(writer, fmt.Errorf("unexpected model peer %q", peerLabel))
		return
	}
	key := peerLabel + "/" + testCase
	m.mu.Lock()
	call := m.calls[key]
	m.calls[key] = call + 1
	m.mu.Unlock()
	if testCase == "lazy" {
		if call != 0 {
			m.record(fmt.Errorf("lazy case received model call %d", call+1))
		}
		writeFinalResponse(writer, key)
		return
	}
	suffix := strings.ReplaceAll(testCase, "-", "_")
	searchID := "search-" + suffix
	callID := "call-" + suffix
	if call == 0 {
		encodedTools, _ := json.Marshal(body["tools"])
		if !bytes.Contains(encodedTools, []byte(`"tool_search"`)) {
			m.record(fmt.Errorf("case %s model tools do not expose tool_search: %s", key, encodedTools))
		}
		writeToolSearchResponse(writer, key, searchID)
		return
	}
	if call == 1 {
		searchOutput, err := callOutputItem(body, "tool_search_output", searchID)
		encodedOutput, _ := json.Marshal(searchOutput)
		if err != nil {
			m.record(fmt.Errorf("case %s: %w", key, err))
		} else if !bytes.Contains(encodedOutput, []byte(`"mcp__delegation"`)) ||
			!bytes.Contains(encodedOutput, []byte(`"list_devices"`)) {
			m.record(fmt.Errorf("case %s tool search did not expose delegation list_devices: %s", key, encodedOutput))
		}
		writeToolCallResponse(writer, key, callID)
		return
	}
	if call != 2 {
		m.fail(writer, fmt.Errorf("case %s received unexpected model call %d", key, call+1))
		return
	}
	output, err := functionOutput(body, callID)
	if err != nil {
		m.record(fmt.Errorf("case %s: %w", key, err))
	} else if testCase == "cross-conflict" {
		if !strings.Contains(output, "bound to another delegation root device") {
			m.record(fmt.Errorf("case %s did not receive the expected root-device conflict: %s", key, output))
		}
	} else if roster, ok := findRoster(output); !ok {
		m.record(fmt.Errorf("case %s did not receive structured peer output: %s", key, output))
	} else if err := validateRoster(roster, peerLabel); err != nil {
		m.record(fmt.Errorf("case %s: %w", key, err))
	}
	writeFinalResponse(writer, key)
}

func decodeRequest(request *http.Request) (map[string]any, error) {
	var reader io.Reader = io.LimitReader(request.Body, 16<<20)
	if request.Header.Get("Content-Encoding") == "gzip" {
		compressed, err := gzip.NewReader(reader)
		if err != nil {
			return nil, fmt.Errorf("open compressed model request: %w", err)
		}
		defer compressed.Close()
		reader = compressed
	}
	var body map[string]any
	if err := json.NewDecoder(reader).Decode(&body); err != nil {
		return nil, fmt.Errorf("decode model request: %w", err)
	}
	return body, nil
}

func requestCase(body map[string]any) string {
	data, _ := json.Marshal(body)
	text := string(data)
	for _, testCase := range []string{
		workerDispatchCase, "cross-conflict", "a1-resume", "lazy", "a1", "b1", "c1", "a2",
	} {
		if testCase == workerDispatchCase && strings.Contains(text, "delegation-worker-case="+testCase) {
			return testCase
		}
		suffix := strings.ReplaceAll(testCase, "-", "_")
		if strings.Contains(text, "delegation-e2e-case="+testCase) ||
			strings.Contains(text, "search-"+suffix) || strings.Contains(text, "call-"+suffix) {
			return testCase
		}
	}
	return ""
}

func functionOutput(body map[string]any, callID string) (string, error) {
	var visit func(any) (string, bool)
	visit = func(value any) (string, bool) {
		switch typed := value.(type) {
		case map[string]any:
			if typed["type"] == "function_call_output" && typed["call_id"] == callID {
				switch output := typed["output"].(type) {
				case string:
					return output, true
				default:
					data, _ := json.Marshal(output)
					return string(data), true
				}
			}
			for _, child := range typed {
				if output, ok := visit(child); ok {
					return output, true
				}
			}
		case []any:
			for _, child := range typed {
				if output, ok := visit(child); ok {
					return output, true
				}
			}
		}
		return "", false
	}
	if output, ok := visit(body); ok {
		return output, nil
	}
	return "", fmt.Errorf("missing function_call_output for %s", callID)
}

func callOutputItem(body map[string]any, outputType, callID string) (map[string]any, error) {
	inputs, ok := body["input"].([]any)
	if !ok {
		return nil, errors.New("model request input is not an array")
	}
	for _, input := range inputs {
		item, ok := input.(map[string]any)
		if ok && item["type"] == outputType && item["call_id"] == callID {
			return item, nil
		}
	}
	return nil, fmt.Errorf("missing %s for %s", outputType, callID)
}

func findRoster(output string) (map[string]any, bool) {
	for index := strings.IndexByte(output, '{'); index >= 0; {
		var parsed any
		if json.Unmarshal([]byte(output[index:]), &parsed) == nil {
			if roster, ok := findRosterValue(parsed); ok {
				return roster, true
			}
		}
		next := strings.IndexByte(output[index+1:], '{')
		if next < 0 {
			break
		}
		index += next + 1
	}
	return nil, false
}

func findRosterValue(value any) (map[string]any, bool) {
	switch typed := value.(type) {
	case map[string]any:
		if _, ok := typed["devices"].([]any); ok {
			return typed, true
		}
		for _, child := range typed {
			if roster, ok := findRosterValue(child); ok {
				return roster, true
			}
		}
	case []any:
		for _, child := range typed {
			if roster, ok := findRosterValue(child); ok {
				return roster, true
			}
		}
	case string:
		return findRoster(typed)
	}
	return nil, false
}

func validateRoster(roster map[string]any, currentLabel string) error {
	data, _ := json.Marshal(roster)
	if containsKey(roster, "role") {
		return fmt.Errorf("peer output leaked role: %s", data)
	}
	devices, ok := roster["devices"].([]any)
	if !ok || len(devices) != 3 {
		return fmt.Errorf("peer output has %d devices, want 3: %s", len(devices), data)
	}
	seen := make(map[string]bool)
	for _, value := range devices {
		device, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("peer output contains a non-object device: %s", data)
		}
		deviceID, _ := device["deviceId"].(string)
		current, _ := device["isCurrentDevice"].(bool)
		if current != (deviceID == deviceIDs[currentLabel]) {
			return fmt.Errorf("peer %s current-device marker is wrong: %s", currentLabel, data)
		}
		if protocolVersion, _ := device["protocolVersion"].(float64); protocolVersion != 1 {
			return fmt.Errorf("peer protocol version is not 1: %s", data)
		}
		if online, _ := device["online"].(bool); !online {
			return fmt.Errorf("peer is not online: %s", data)
		}
		seen[deviceID] = true
	}
	for _, deviceID := range deviceIDs {
		if !seen[deviceID] {
			return fmt.Errorf("peer output is missing device %s: %s", deviceID, data)
		}
	}
	return nil
}

func containsKey(value any, key string) bool {
	switch typed := value.(type) {
	case map[string]any:
		if _, ok := typed[key]; ok {
			return true
		}
		for _, child := range typed {
			if containsKey(child, key) {
				return true
			}
		}
	case []any:
		for _, child := range typed {
			if containsKey(child, key) {
				return true
			}
		}
	}
	return false
}

func writeToolCallResponse(writer http.ResponseWriter, key, callID string) {
	writeSSE(writer,
		map[string]any{"type": "response.created", "response": map[string]any{"id": "resp-" + key + "-1"}},
		map[string]any{"type": "response.output_item.done", "item": map[string]any{
			"type": "function_call", "call_id": callID, "namespace": "mcp__delegation", "name": "list_devices", "arguments": "{}",
		}},
		completedEvent("resp-"+key+"-1"),
	)
}

func writeToolSearchResponse(writer http.ResponseWriter, key, searchID string) {
	writeSSE(writer,
		map[string]any{"type": "response.created", "response": map[string]any{"id": "resp-" + key + "-search"}},
		map[string]any{"type": "response.output_item.done", "item": map[string]any{
			"type": "tool_search_call", "call_id": searchID, "execution": "client",
			"arguments": map[string]any{"query": "delegation list devices peer discovery", "limit": 8},
		}},
		completedEvent("resp-"+key+"-search"),
	)
}

func writeFinalResponse(writer http.ResponseWriter, key string) {
	writeSSE(writer,
		map[string]any{"type": "response.created", "response": map[string]any{"id": "resp-" + key + "-2"}},
		map[string]any{"type": "response.output_item.done", "item": map[string]any{
			"type": "message", "role": "assistant", "id": "msg-" + key,
			"content": []map[string]any{{"type": "output_text", "text": finalText}},
		}},
		completedEvent("resp-"+key+"-2"),
	)
}

func completedEvent(id string) map[string]any {
	return map[string]any{"type": "response.completed", "response": map[string]any{
		"id": id,
		"usage": map[string]any{
			"input_tokens": 0, "input_tokens_details": nil, "output_tokens": 0,
			"output_tokens_details": nil, "total_tokens": 0,
		},
	}}
}

func writeSSE(writer http.ResponseWriter, events ...map[string]any) {
	writer.Header().Set("Content-Type", "text/event-stream")
	for _, event := range events {
		data, _ := json.Marshal(event)
		fmt.Fprintf(writer, "event: %s\ndata: %s\n\n", event["type"], data)
	}
}

func (m *mockResponses) fail(writer http.ResponseWriter, err error) {
	m.record(err)
	http.Error(writer, err.Error(), http.StatusBadRequest)
}

func (m *mockResponses) record(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.errors = append(m.errors, err.Error())
}

func (m *mockResponses) verify(t *testing.T, cases []string) {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.errors) != 0 {
		t.Fatalf("mock Responses errors: %s", strings.Join(m.errors, "\n"))
	}
	for _, testCase := range cases {
		want := 3
		label := strings.ToUpper(testCase[:1])
		if testCase == workerDispatchCase {
			label, want = "worker", 1
		} else if testCase == "lazy" {
			label, want = "A", 1
		} else if strings.HasPrefix(testCase, "a") {
			label = "A"
		} else if testCase == "cross-conflict" {
			label = "B"
		}
		if got := m.calls[label+"/"+testCase]; got != want {
			t.Fatalf("model calls for %s/%s = %d, want %d", label, testCase, got, want)
		}
	}
}

func (m *mockResponses) check(t *testing.T) {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.errors) != 0 {
		t.Fatalf("mock Responses errors: %s", strings.Join(m.errors, "\n"))
	}
}
