package statuspage

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

func TestJSONStatusReturnsOneAggregateSnapshot(t *testing.T) {
	want := testSnapshot()
	calls := 0
	handler := NewHandler(func(context.Context) (Snapshot, error) {
		calls++
		return want, nil
	})

	response := requestStatus(t, handler, http.MethodGet, JSONPath)
	assertStatusHeaders(t, response, "application/json; charset=utf-8")
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %q", response.Code, response.Body.String())
	}
	if calls != 1 {
		t.Fatalf("provider calls = %d, want 1", calls)
	}
	var got Snapshot
	if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("snapshot = %#v, want %#v", got, want)
	}
	for _, forbidden := range []string{"prompt", "taskName", "gitUrl", "path", "deviceId"} {
		if strings.Contains(response.Body.String(), forbidden) {
			t.Errorf("JSON contains forbidden field %q", forbidden)
		}
	}
}

func TestHTMLStatusIsAccessibleEscapedAndOmitsControllerID(t *testing.T) {
	snapshot := testSnapshot()
	snapshot.Version = "v1<&\"'><script>alert(1)</script>"
	snapshot.ControllerID = "private-controller-id"
	handler := NewHandler(func(context.Context) (Snapshot, error) { return snapshot, nil })

	response := requestStatus(t, handler, http.MethodGet, HTMLPath)
	assertStatusHeaders(t, response, "text/html; charset=utf-8")
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %q", response.Code, response.Body.String())
	}
	body := response.Body.String()
	for _, required := range []string{
		`<html lang="en">`,
		`<main>`,
		`<caption>Broker operational status</caption>`,
		`<th scope="col">Metric</th>`,
		`<th scope="row">Registered</th>`,
		`<th colspan="2" scope="rowgroup">Devices</th>`,
		`v1&lt;&amp;&#34;&#39;&gt;&lt;script&gt;alert(1)&lt;/script&gt;`,
		`<tr><th scope="row">Registered</th><td>9</td></tr>`,
		`<tr><th scope="row">Online</th><td>8</td></tr>`,
		`<tr><th scope="row">Connected</th><td>7</td></tr>`,
		`<tr><th scope="row">Sync-ready</th><td>6</td></tr>`,
		`<tr><th scope="row">Pending</th><td>5</td></tr>`,
		`<tr><th scope="row">Started</th><td>4</td></tr>`,
		`<tr><th scope="row">Failed</th><td>3</td></tr>`,
		`<tr><th scope="row">Lifetime started</th><td>200</td></tr>`,
		`<tr><th scope="row">Running turns</th><td>2</td></tr>`,
		`<tr><th scope="row">Occupied slots</th><td>1</td></tr>`,
		`<tr><th scope="row">Lifetime turns</th><td>300</td></tr>`,
		`<tr><th scope="row">Trees</th><td>10</td></tr>`,
		`<tr><th scope="row">Available</th><td>11</td></tr>`,
		`<tr><th scope="row">Unchanged</th><td>12</td></tr>`,
		`<tr><th scope="row">Capture failed</th><td>13</td></tr>`,
	} {
		if !strings.Contains(body, required) {
			t.Errorf("body missing %q", required)
		}
	}
	for _, forbidden := range []string{"private-controller-id", "<script>", "http://", "https://", " src="} {
		if strings.Contains(body, forbidden) {
			t.Errorf("body contains forbidden text %q", forbidden)
		}
	}

	stylePattern := regexp.MustCompile(`(?s)<style>(.*?)</style>`)
	match := stylePattern.FindStringSubmatch(body)
	if len(match) != 2 {
		t.Fatal("HTML does not contain one inline style block")
	}
	digest := sha256.Sum256([]byte(match[1]))
	wantSource := "'sha256-" + base64.StdEncoding.EncodeToString(digest[:]) + "'"
	if !strings.Contains(response.Header().Get("Content-Security-Policy"), wantSource) {
		t.Fatalf("CSP does not authorize only the rendered style: %q", response.Header().Get("Content-Security-Policy"))
	}
}

func TestJSONStatusOmitsEmptyControllerID(t *testing.T) {
	snapshot := testSnapshot()
	snapshot.ControllerID = ""
	handler := NewHandler(func(context.Context) (Snapshot, error) { return snapshot, nil })

	response := requestStatus(t, handler, http.MethodGet, JSONPath)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %q", response.Code, response.Body.String())
	}
	var document map[string]json.RawMessage
	if err := json.Unmarshal(response.Body.Bytes(), &document); err != nil {
		t.Fatal(err)
	}
	if _, exists := document["controllerId"]; exists {
		t.Fatal("empty controllerId was included in JSON")
	}
}

func TestFormatUptimeDoesNotOverflow(t *testing.T) {
	tests := []struct {
		seconds uint64
		want    string
	}{
		{seconds: 0, want: "0s"},
		{seconds: 59, want: "59s"},
		{seconds: 60, want: "1m 0s"},
		{seconds: 3723, want: "1h 2m 3s"},
		{seconds: 90061, want: "1d 1h 1m 1s"},
		{seconds: ^uint64(0), want: "213503982334601d 7h 0m 15s"},
	}
	for _, test := range tests {
		if got := formatUptime(test.seconds); got != test.want {
			t.Errorf("formatUptime(%d) = %q, want %q", test.seconds, got, test.want)
		}
	}
}

func TestStatusRoutesMethodsAndHead(t *testing.T) {
	snapshot := testSnapshot()
	calls := 0
	handler := NewHandler(func(context.Context) (Snapshot, error) {
		calls++
		return snapshot, nil
	})

	tests := []struct {
		name              string
		method            string
		path              string
		wantStatus        int
		wantCalls         int
		wantBody          string
		wantAllow         string
		wantType          string
		wantNonzeroLength bool
	}{
		{
			name: "unknown path", method: http.MethodGet, path: "/status/",
			wantStatus: http.StatusNotFound, wantBody: notFoundBody, wantType: "text/plain; charset=utf-8",
		},
		{
			name: "method rejected", method: http.MethodPost, path: HTMLPath,
			wantStatus: http.StatusMethodNotAllowed, wantBody: methodNotAllowedBody,
			wantAllow: "GET, HEAD", wantType: "text/plain; charset=utf-8",
		},
		{
			name: "HTML head", method: http.MethodHead, path: HTMLPath,
			wantStatus: http.StatusOK, wantCalls: 1, wantType: "text/html; charset=utf-8", wantNonzeroLength: true,
		},
		{
			name: "JSON head", method: http.MethodHead, path: JSONPath,
			wantStatus: http.StatusOK, wantCalls: 1, wantType: "application/json; charset=utf-8", wantNonzeroLength: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			before := calls
			response := requestStatus(t, handler, test.method, test.path)
			assertStatusHeaders(t, response, test.wantType)
			if response.Code != test.wantStatus {
				t.Fatalf("status = %d, want %d", response.Code, test.wantStatus)
			}
			if calls-before != test.wantCalls {
				t.Fatalf("provider calls = %d, want %d", calls-before, test.wantCalls)
			}
			if response.Body.String() != test.wantBody {
				t.Fatalf("body = %q, want %q", response.Body.String(), test.wantBody)
			}
			if response.Header().Get("Allow") != test.wantAllow {
				t.Fatalf("Allow = %q, want %q", response.Header().Get("Allow"), test.wantAllow)
			}
			length, err := strconv.Atoi(response.Header().Get("Content-Length"))
			if err != nil {
				t.Fatalf("Content-Length is invalid: %v", err)
			}
			if test.wantNonzeroLength && length == 0 {
				t.Fatal("HEAD response has zero representation length")
			}
		})
	}
}

func TestStatusProviderFailuresReturnBoundedError(t *testing.T) {
	secret := strings.Repeat("sensitive provider failure ", 1000)
	tests := []struct {
		name     string
		provider Provider
	}{
		{name: "nil provider"},
		{
			name: "provider error",
			provider: func(context.Context) (Snapshot, error) {
				return Snapshot{}, errors.New(secret)
			},
		},
		{
			name: "oversized snapshot",
			provider: func(context.Context) (Snapshot, error) {
				return Snapshot{Version: strings.Repeat("x", maximumTextBytes+1)}, nil
			},
		},
		{
			name: "non-printing snapshot text",
			provider: func(context.Context) (Snapshot, error) {
				return Snapshot{Version: "version\nsecret"}, nil
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := requestStatus(t, NewHandler(test.provider), http.MethodGet, JSONPath)
			assertStatusHeaders(t, response, "text/plain; charset=utf-8")
			if response.Code != http.StatusServiceUnavailable {
				t.Fatalf("status = %d, want %d", response.Code, http.StatusServiceUnavailable)
			}
			if response.Body.String() != statusUnavailableBody {
				t.Fatalf("body = %q, want bounded error", response.Body.String())
			}
			if strings.Contains(response.Body.String(), secret) {
				t.Fatal("response leaked provider error")
			}
		})
	}
}

func testSnapshot() Snapshot {
	return Snapshot{
		Version:       "0.2.0",
		UptimeSeconds: 3723,
		ControllerID:  "123e4567-e89b-42d3-a456-426614174100",
		Devices: DeviceCounts{
			Registered: 9,
			Online:     8,
			Connected:  7,
			SyncReady:  6,
		},
		Dispatch: DispatchCounts{
			Pending:         5,
			Started:         4,
			Failed:          3,
			LifetimeStarted: 200,
		},
		RunningTurns:  2,
		OccupiedSlots: 1,
		LifetimeTurns: 300,
		Trees:         10,
		Artifacts: ArtifactCounts{
			Available:     11,
			Unchanged:     12,
			CaptureFailed: 13,
		},
	}
}

func requestStatus(t *testing.T, handler http.Handler, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, path, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func assertStatusHeaders(t *testing.T, response *httptest.ResponseRecorder, contentType string) {
	t.Helper()
	if response.Header().Get("Content-Type") != contentType {
		t.Errorf("Content-Type = %q, want %q", response.Header().Get("Content-Type"), contentType)
	}
	if response.Header().Get("Cache-Control") != "no-store" {
		t.Errorf("Cache-Control = %q", response.Header().Get("Cache-Control"))
	}
	if response.Header().Get("X-Content-Type-Options") != "nosniff" {
		t.Errorf("X-Content-Type-Options = %q", response.Header().Get("X-Content-Type-Options"))
	}
	if response.Header().Get("Referrer-Policy") != "no-referrer" {
		t.Errorf("Referrer-Policy = %q", response.Header().Get("Referrer-Policy"))
	}
	csp := response.Header().Get("Content-Security-Policy")
	for _, directive := range []string{"default-src 'none'", "base-uri 'none'", "form-action 'none'", "frame-ancestors 'none'"} {
		if !strings.Contains(csp, directive) {
			t.Errorf("Content-Security-Policy %q missing %q", csp, directive)
		}
	}
	if contentLength := response.Header().Get("Content-Length"); contentLength == "" {
		t.Error("Content-Length is missing")
	}
}
