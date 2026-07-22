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
	"slices"
	"strings"
	"sync"
	"testing"
)

type mockResponses struct {
	mu           sync.Mutex
	calls        map[string]int
	errors       []string
	workerBlocks map[string]*workerResponseBlock
}

type workerResponseBlock struct {
	started     chan struct{}
	release     chan struct{}
	startedOnce sync.Once
	releaseOnce sync.Once
}

type rootAgentScript struct {
	tool           string
	query          string
	arguments      map[string]any
	expectedOutput []string
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
	if strings.HasPrefix(testCase, "worker-") {
		key := "worker/" + testCase
		m.mu.Lock()
		call := m.calls[key]
		m.calls[key] = call + 1
		block := m.workerBlocks[testCase]
		m.mu.Unlock()
		encodedTools, _ := json.Marshal(body["tools"])
		for _, forbidden := range []string{"spawn_agent", "list_agents", "list_devices", "describe_device"} {
			if bytes.Contains(encodedTools, []byte(forbidden)) {
				m.record(fmt.Errorf("case %s exposed root tool %s: %s", key, forbidden, encodedTools))
			}
		}
		switch testCase {
		case workerCollaborationInitial:
			m.handleWorkerCollaborationInitial(writer, request, body, key, call, block)
			return
		case workerRootMCPFollowup:
			m.handleWorkerMailboxFollowup(
				writer, body, key, testCase, call,
				managedQueuedMessageID, managedQueuedMessage, managedFollowupReplyID, managedFollowupReply,
			)
			return
		case workerCollaborationResume:
			m.handleWorkerMailboxFollowup(
				writer, body, key, testCase, call,
				managedRecoveryMessageID, managedRecoveryMessage, managedRecoveryReplyID, managedRecoveryReply,
			)
			return
		case workerSelfTarget:
			if call != 0 {
				m.record(fmt.Errorf("case %s received model call %d", key, call+1))
			}
			writeFinalResponse(writer, key)
			return
		}
		if call != 0 {
			m.record(fmt.Errorf("case %s received model call %d", key, call+1))
		}
		if block == nil {
			writeFinalResponse(writer, key)
			return
		}
		writeBlockedFinalResponse(writer, request, key, block)
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
	if script, ok := rootAgentScriptFor(testCase); ok {
		m.handleRootAgentScript(writer, body, peerLabel, key, testCase, call, script)
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

func rootAgentScriptFor(testCase string) (rootAgentScript, bool) {
	switch testCase {
	case rootMCPSpawn:
		return rootAgentScript{
			tool: "spawn_agent", query: "delegation spawn managed agent on peer",
			arguments: map[string]any{
				"spawn_id": managedRootMCPSpawn, "target_device_id": deviceIDs["A"],
				"task_name": managedRootMCPTask,
				"message":   "delegation-worker-case=" + workerRootMCPInitial + " Complete the initial turn.",
			},
			expectedOutput: []string{managedRootMCPSpawn, managedRootMCPTask, "started"},
		}, true
	case rootMCPQueue:
		return rootAgentScript{
			tool: "send_message", query: "delegation send message to idle managed agent",
			arguments: map[string]any{
				"target": managedRootMCPTask, "message_id": managedQueuedMessageID,
				"message": managedQueuedMessage,
			},
			expectedOutput: []string{managedQueuedMessageID, "queued"},
		}, true
	case rootMCPFollowup:
		return rootAgentScript{
			tool: "followup_task", query: "delegation follow up idle managed agent",
			arguments: map[string]any{
				"target": managedRootMCPTask, "operation_id": managedFollowupOperationID,
				"message": "delegation-worker-case=" + workerRootMCPFollowup +
					" Read the queued root message and acknowledge it.",
			},
			expectedOutput: []string{managedFollowupOperationID, "started"},
		}, true
	case rootMCPWaitFollowup:
		return rootAgentScript{
			tool: "wait_agent", query: "delegation wait managed agent messages",
			arguments:      map[string]any{"timeout_seconds": 20},
			expectedOutput: []string{managedFollowupReplyID, managedFollowupReply},
		}, true
	default:
		return rootAgentScript{}, false
	}
}

func (m *mockResponses) handleRootAgentScript(
	writer http.ResponseWriter,
	body map[string]any,
	peerLabel string,
	key string,
	testCase string,
	call int,
	script rootAgentScript,
) {
	if peerLabel != "C" {
		m.record(fmt.Errorf("case %s ran on peer %q, want C", key, peerLabel))
	}
	suffix := strings.ReplaceAll(testCase, "-", "_")
	searchID := "search-" + suffix
	callID := "call-" + suffix
	switch call {
	case 0:
		writeToolSearchQueryResponse(writer, key, searchID, script.query)
	case 1:
		m.validateRootToolSearch(body, key, searchID, script.tool)
		writeRootToolCallResponse(writer, key, callID, script.tool, script.arguments)
	case 2:
		output, err := functionOutput(body, callID)
		if err != nil {
			m.record(fmt.Errorf("case %s: %w", key, err))
			writeFinalResponse(writer, key)
			return
		}
		m.validateRootToolOutput(key, output, script.expectedOutput)
		writeFinalResponse(writer, key)
	default:
		m.fail(writer, fmt.Errorf("case %s received unexpected model call %d", key, call+1))
	}
}

func (m *mockResponses) validateRootToolSearch(
	body map[string]any,
	key string,
	searchID string,
	tool string,
) {
	searchOutput, err := callOutputItem(body, "tool_search_output", searchID)
	if err != nil {
		m.record(fmt.Errorf("case %s: %w", key, err))
		return
	}
	namespaces, err := toolSearchNamespaces(searchOutput)
	if err != nil {
		m.record(fmt.Errorf("case %s: %w", key, err))
		return
	}
	if tools, found := namespaces["mcp__delegation_worker"]; found {
		m.record(fmt.Errorf("case %s root tool search exposed worker namespace: %v", key, tools))
	}
	tools, found := namespaces["mcp__delegation"]
	if !found {
		m.record(fmt.Errorf("case %s root tool search omitted mcp__delegation: %v", key, namespaces))
		return
	}
	if !slices.Contains(tools, tool) {
		m.record(fmt.Errorf("case %s root tool search omitted %s: %v", key, tool, tools))
	}
}

func (m *mockResponses) validateRootToolOutput(key, output string, expected []string) {
	if !containsEvery(output, expected) {
		m.record(fmt.Errorf("case %s root tool output = %s, want markers %v", key, output, expected))
	}
}

func containsEvery(output string, expected []string) bool {
	for _, value := range expected {
		if !strings.Contains(output, value) {
			return false
		}
	}
	return true
}

func (m *mockResponses) handleWorkerCollaborationInitial(
	writer http.ResponseWriter,
	request *http.Request,
	body map[string]any,
	key string,
	call int,
	block *workerResponseBlock,
) {
	searchID := "search_worker_collaboration_initial"
	sendCallID := "call_worker_collaboration_initial_send"
	switch call {
	case 0:
		if block == nil {
			m.fail(writer, fmt.Errorf("case %s is missing its running-turn barrier", key))
			return
		}
		writeBlockedFinalResponse(writer, request, key, block)
	case 1:
		encoded, _ := json.Marshal(body)
		if !bytes.Contains(encoded, []byte(managedSteerMessage)) {
			m.record(fmt.Errorf("case %s follow-up sampling omitted steer input: %s", key, encoded))
		}
		writeWorkerToolSearchResponse(writer, key, searchID)
	case 2:
		m.validateWorkerToolSearch(body, key, searchID)
		writeWorkerToolCallResponse(writer, key, sendCallID, "send_message", map[string]any{
			"messageId": managedInitialReplyID,
			"recipient": "parent",
			"message":   managedInitialReply,
		})
	case 3:
		output, err := functionOutput(body, sendCallID)
		if err != nil {
			m.record(fmt.Errorf("case %s: %w", key, err))
		} else if !strings.Contains(output, managedInitialReplyID) {
			m.record(fmt.Errorf("case %s send_message receipt omitted message ID: %s", key, output))
		}
		writeFinalResponse(writer, key)
	default:
		m.fail(writer, fmt.Errorf("case %s received unexpected model call %d", key, call+1))
	}
}

func (m *mockResponses) handleWorkerMailboxFollowup(
	writer http.ResponseWriter,
	body map[string]any,
	key string,
	testCase string,
	call int,
	queuedMessageID string,
	queuedMessage string,
	replyMessageID string,
	replyMessage string,
) {
	suffix := strings.ReplaceAll(testCase, "-", "_")
	searchID := "search_" + suffix
	waitCallID := "call_" + suffix + "_wait"
	sendCallID := "call_" + suffix + "_send"
	switch call {
	case 0:
		writeWorkerToolSearchResponse(writer, key, searchID)
	case 1:
		m.validateWorkerToolSearch(body, key, searchID)
		writeWorkerToolCallResponse(writer, key, waitCallID, "wait_agent", map[string]any{
			"cursor": 0, "timeoutSeconds": 2,
		})
	case 2:
		output, err := functionOutput(body, waitCallID)
		if err != nil {
			m.record(fmt.Errorf("case %s: %w", key, err))
		} else if !strings.Contains(output, queuedMessageID) ||
			!strings.Contains(output, queuedMessage) {
			m.record(fmt.Errorf("case %s wait_agent omitted queued root message: %s", key, output))
		}
		writeWorkerToolCallResponse(writer, key, sendCallID, "send_message", map[string]any{
			"messageId": replyMessageID,
			"recipient": "parent",
			"message":   replyMessage,
		})
	case 3:
		output, err := functionOutput(body, sendCallID)
		if err != nil {
			m.record(fmt.Errorf("case %s: %w", key, err))
		} else if !strings.Contains(output, replyMessageID) {
			m.record(fmt.Errorf("case %s send_message receipt omitted message ID: %s", key, output))
		}
		writeFinalResponse(writer, key)
	default:
		m.fail(writer, fmt.Errorf("case %s received unexpected model call %d", key, call+1))
	}
}

func (m *mockResponses) validateWorkerToolSearch(body map[string]any, key, searchID string) {
	searchOutput, err := callOutputItem(body, "tool_search_output", searchID)
	if err != nil {
		m.record(fmt.Errorf("case %s: %w", key, err))
		return
	}
	namespaces, err := toolSearchNamespaces(searchOutput)
	if err != nil {
		m.record(fmt.Errorf("case %s: %w", key, err))
		return
	}
	names, found := namespaces["mcp__delegation_worker"]
	if len(namespaces) != 1 || !found {
		m.record(fmt.Errorf("case %s worker tool search namespaces = %v, want only mcp__delegation_worker", key, namespaces))
		return
	}
	want := map[string]bool{
		"send_message": false,
		"wait_agent":   false,
	}
	for _, name := range names {
		if _, ok := want[name]; !ok {
			m.record(fmt.Errorf("case %s worker tool search exposed unexpected tool %s", key, name))
			continue
		}
		want[name] = true
	}
	for name, found := range want {
		if !found {
			m.record(fmt.Errorf("case %s worker tool search omitted %s: %v", key, name, names))
		}
	}
	if len(names) != len(want) {
		m.record(fmt.Errorf("case %s worker tool search returned %d tools, want %d: %v", key, len(names), len(want), names))
	}
}

func toolSearchNamespaces(output map[string]any) (map[string][]string, error) {
	tools, ok := output["tools"].([]any)
	if !ok {
		return nil, errors.New("tool_search_output tools is not an array")
	}
	namespaces := make(map[string][]string, len(tools))
	for _, value := range tools {
		namespace, ok := value.(map[string]any)
		if !ok || namespace["type"] != "namespace" {
			return nil, fmt.Errorf("tool_search_output contains a non-namespace tool: %#v", value)
		}
		name, ok := namespace["name"].(string)
		if !ok || name == "" {
			return nil, fmt.Errorf("tool_search_output contains an unnamed namespace: %#v", value)
		}
		if _, duplicate := namespaces[name]; duplicate {
			return nil, fmt.Errorf("tool_search_output repeats namespace %s", name)
		}
		values, ok := namespace["tools"].([]any)
		if !ok {
			return nil, fmt.Errorf("tool_search_output namespace %s tools is not an array", name)
		}
		names := make([]string, 0, len(values))
		for _, value := range values {
			tool, ok := value.(map[string]any)
			if !ok || tool["type"] != "function" {
				return nil, fmt.Errorf("tool_search_output namespace %s contains a non-function tool: %#v", name, value)
			}
			toolName, ok := tool["name"].(string)
			if !ok || toolName == "" {
				return nil, fmt.Errorf("tool_search_output namespace %s contains an unnamed tool: %#v", name, value)
			}
			names = append(names, toolName)
		}
		namespaces[name] = names
	}
	return namespaces, nil
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
		rootMCPWaitFollowup, rootMCPFollowup, rootMCPQueue, rootMCPSpawn,
		"cross-conflict", "a1-resume", "lazy", "a1", "b1", "c1", "a2",
	} {
		suffix := strings.ReplaceAll(testCase, "-", "_")
		if strings.Contains(text, "delegation-e2e-case="+testCase) ||
			strings.Contains(text, "search-"+suffix) || strings.Contains(text, "call-"+suffix) {
			return testCase
		}
	}
	for _, testCase := range []string{
		workerCollaborationResume, workerCollaborationInitial, workerSelfTarget,
		workerRootMCPFollowup, workerRootMCPInitial,
		workerAdmissionA, workerAdmissionB, workerAdmissionRetry,
	} {
		if strings.Contains(text, "delegation-worker-case="+testCase) {
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

func writeToolSearchQueryResponse(writer http.ResponseWriter, key, searchID, query string) {
	writeSSE(writer,
		map[string]any{"type": "response.created", "response": map[string]any{"id": "resp-" + key + "-search"}},
		map[string]any{"type": "response.output_item.done", "item": map[string]any{
			"type": "tool_search_call", "call_id": searchID, "execution": "client",
			"arguments": map[string]any{"query": query, "limit": 8},
		}},
		completedEvent("resp-"+key+"-search"),
	)
}

func writeRootToolCallResponse(
	writer http.ResponseWriter,
	key string,
	callID string,
	name string,
	arguments map[string]any,
) {
	encoded, _ := json.Marshal(arguments)
	writeSSE(writer,
		map[string]any{"type": "response.created", "response": map[string]any{"id": "resp-" + key + "-" + name}},
		map[string]any{"type": "response.output_item.done", "item": map[string]any{
			"type": "function_call", "call_id": callID,
			"namespace": "mcp__delegation", "name": name, "arguments": string(encoded),
		}},
		completedEvent("resp-"+key+"-"+name),
	)
}

func writeWorkerToolSearchResponse(writer http.ResponseWriter, key, searchID string) {
	writeSSE(writer,
		map[string]any{"type": "response.created", "response": map[string]any{"id": "resp-" + key + "-search"}},
		map[string]any{"type": "response.output_item.done", "item": map[string]any{
			"type": "tool_search_call", "call_id": searchID, "execution": "client",
			"arguments": map[string]any{
				"query": "delegation worker send_message wait_agent parent mailbox", "limit": 8,
			},
		}},
		completedEvent("resp-"+key+"-search"),
	)
}

func writeWorkerToolCallResponse(
	writer http.ResponseWriter,
	key string,
	callID string,
	name string,
	arguments map[string]any,
) {
	encoded, _ := json.Marshal(arguments)
	writeSSE(writer,
		map[string]any{"type": "response.created", "response": map[string]any{"id": "resp-" + key + "-" + name}},
		map[string]any{"type": "response.output_item.done", "item": map[string]any{
			"type": "function_call", "call_id": callID,
			"namespace": "mcp__delegation_worker", "name": name, "arguments": string(encoded),
		}},
		completedEvent("resp-"+key+"-"+name),
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

func writeBlockedFinalResponse(
	writer http.ResponseWriter,
	request *http.Request,
	key string,
	block *workerResponseBlock,
) {
	writer.Header().Set("Content-Type", "text/event-stream")
	writeSSE(writer, map[string]any{
		"type": "response.created", "response": map[string]any{"id": "resp-" + key + "-2"},
	})
	if flusher, ok := writer.(http.Flusher); ok {
		flusher.Flush()
	}
	block.startedOnce.Do(func() { close(block.started) })
	select {
	case <-request.Context().Done():
		return
	case <-block.release:
	}
	writeSSE(writer,
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
		if strings.HasPrefix(testCase, "worker-") {
			label, want = "worker", 1
			if testCase == workerRootMCPFollowup || testCase == workerCollaborationResume ||
				testCase == workerCollaborationInitial {
				want = 4
			}
		} else if strings.HasPrefix(testCase, "root-mcp-") {
			label = "C"
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

func (m *mockResponses) blockWorker(testCase string) (<-chan struct{}, func()) {
	block := &workerResponseBlock{started: make(chan struct{}), release: make(chan struct{})}
	m.mu.Lock()
	if m.workerBlocks == nil {
		m.workerBlocks = make(map[string]*workerResponseBlock)
	}
	m.workerBlocks[testCase] = block
	m.mu.Unlock()
	return block.started, func() {
		block.releaseOnce.Do(func() { close(block.release) })
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
