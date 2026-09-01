//go:build integration && live && (linux || darwin)

package codex_peer_e2e

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/identity"
)

const traeXLiveFinalText = "delegation-traex-live-ok"

func runTraeXLive(
	t *testing.T, environment []string, binary string, args ...string,
) (string, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary, args...)
	command.Env = environment
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	err := command.Run()
	if ctx.Err() != nil {
		t.Fatalf(
			"command timed out: %s %s\nstdout: %s\nstderr: %s",
			binary, strings.Join(args, " "), stdout.String(), stderr.String(),
		)
	}
	if err != nil {
		t.Fatalf(
			"command failed: %s %s: %v\nstdout: %s\nstderr: %s",
			binary, strings.Join(args, " "), err, stdout.String(), stderr.String(),
		)
	}
	return stdout.String(), stderr.String()
}

func newTraeXLiveIdentity(t *testing.T) string {
	t.Helper()
	value, err := identity.NewID()
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func decodeTraeXLiveRequest(request *http.Request) (map[string]any, error) {
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

func writeTraeXLiveFinalResponse(writer http.ResponseWriter, key string) {
	writeTraeXLiveSSE(writer,
		map[string]any{
			"type":     "response.created",
			"response": map[string]any{"id": "resp-" + key + "-2"},
		},
		map[string]any{
			"type": "response.output_item.done",
			"item": map[string]any{
				"type": "message", "role": "assistant", "id": "msg-" + key,
				"content": []map[string]any{{
					"type": "output_text", "text": traeXLiveFinalText,
				}},
			},
		},
		map[string]any{
			"type": "response.completed",
			"response": map[string]any{
				"id": "resp-" + key + "-2",
				"usage": map[string]any{
					"input_tokens": 0, "input_tokens_details": nil,
					"output_tokens": 0, "output_tokens_details": nil,
					"total_tokens": 0,
				},
			},
		},
	)
}

func writeTraeXLiveSSE(writer http.ResponseWriter, events ...map[string]any) {
	writer.Header().Set("Content-Type", "text/event-stream")
	for _, event := range events {
		data, _ := json.Marshal(event)
		fmt.Fprintf(writer, "event: %s\ndata: %s\n\n", event["type"], data)
	}
}
