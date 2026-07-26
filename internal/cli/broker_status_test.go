package cli

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/GhostFlying/delegation/internal/statuspage"
)

type statusHTTPDoerFunc func(*http.Request) (*http.Response, error)

func (f statusHTTPDoerFunc) Do(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestReadBrokerStatusUsesBoundedLoopbackJSON(t *testing.T) {
	want := statuspage.Snapshot{
		Version: "0.2.0-test", ControllerID: statusTestControllerID,
		Devices:      statuspage.DeviceCounts{Registered: 3, Online: 2, Connected: 2, SyncReady: 1},
		Dispatch:     statuspage.DispatchCounts{Pending: 1, Started: 2, Failed: 3, LifetimeStarted: 4},
		RunningTurns: 1, OccupiedSlots: 2, LifetimeTurns: 5, Trees: 6,
		Artifacts: statuspage.ArtifactCounts{Available: 7, Unchanged: 8, CaptureFailed: 9},
		Results:   statuspage.ResultCounts{DeliveryPending: 10, Delivered: 12, SourceAcknowledged: 11},
	}
	body := `{"version":"0.2.0-test","uptimeSeconds":0,"controllerId":"` + statusTestControllerID +
		`","devices":{"registered":3,"online":2,"connected":2,"syncReady":1},` +
		`"dispatch":{"pending":1,"started":2,"failed":3,"lifetimeStarted":4},` +
		`"runningTurns":1,"occupiedSlots":2,"lifetimeTurns":5,"trees":6,` +
		`"artifacts":{"available":7,"unchanged":8,"captureFailed":9},` +
		`"results":{"deliveryPending":10,"delivered":12,"sourceAcknowledged":11}}`
	client := statusHTTPDoerFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodGet || request.URL.String() != "http://127.0.0.1:8788/v1/status" ||
			request.Header.Get("Authorization") != "" {
			t.Fatalf("broker status request = %#v", request)
		}
		return statusHTTPResponse(http.StatusOK, "application/json; charset=utf-8", body), nil
	})
	got, err := readBrokerStatusWithClient(
		context.Background(), "127.0.0.1:8788", client,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("broker status = %#v, want %#v", got, want)
	}
}

func TestReadBrokerStatusRejectsInvalidResponses(t *testing.T) {
	tests := []struct {
		name        string
		status      int
		contentType string
		body        string
	}{
		{name: "HTTP status", status: http.StatusUnauthorized, contentType: "application/json", body: `{}`},
		{name: "content type", status: http.StatusOK, contentType: "text/html", body: `{}`},
		{name: "malformed JSON", status: http.StatusOK, contentType: "application/json", body: `{`},
		{
			name: "oversized", status: http.StatusOK, contentType: "application/json",
			body: strings.Repeat("x", maximumBrokerStatusResponse+1),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := statusHTTPDoerFunc(func(*http.Request) (*http.Response, error) {
				return statusHTTPResponse(test.status, test.contentType, test.body), nil
			})
			if _, err := readBrokerStatusWithClient(
				context.Background(), "127.0.0.1:8788", client,
			); err == nil {
				t.Fatal("readBrokerStatusWithClient() accepted an invalid response")
			}
		})
	}
}

func TestStatusCommandRendersBrokerSnapshot(t *testing.T) {
	configPath, cfg := writeStatusTestConfig(t, "broker")
	snapshot := statuspage.Snapshot{
		Version: "0.2.0-test", UptimeSeconds: 61, ControllerID: cfg.ControllerID,
		Devices:      statuspage.DeviceCounts{Registered: 3, Online: 2, Connected: 2, SyncReady: 1},
		Dispatch:     statuspage.DispatchCounts{Pending: 1, Started: 2, Failed: 3, LifetimeStarted: 4},
		RunningTurns: 1, OccupiedSlots: 2, LifetimeTurns: 5, Trees: 6,
		Artifacts: statuspage.ArtifactCounts{Available: 7, Unchanged: 8, CaptureFailed: 9},
		Results:   statuspage.ResultCounts{DeliveryPending: 10, Delivered: 12, SourceAcknowledged: 11},
	}
	readBroker := func(_ context.Context, address string) (statuspage.Snapshot, error) {
		if address != cfg.Broker.StatusListen {
			t.Fatalf("broker status address = %q", address)
		}
		return snapshot, nil
	}
	for _, jsonOutput := range []bool{false, true} {
		args := []string{"--config", configPath}
		if jsonOutput {
			args = append(args, "--json")
		}
		var stdout bytes.Buffer
		var stderr bytes.Buffer
		code := runStatusWithReaders(args, &stdout, &stderr, nil, readBroker)
		if code != 0 || stderr.Len() != 0 {
			t.Fatalf("broker status code = %d, stderr = %q", code, stderr.String())
		}
		if jsonOutput {
			if !strings.Contains(stdout.String(), `"sourceAcknowledged":11`) {
				t.Fatalf("broker JSON status = %q", stdout.String())
			}
		} else {
			for _, text := range []string{
				"delegation broker status\n", "lifetime started: 4\n",
				"running turns: 1\n", "capture failed: 9\n",
				"delivery pending: 10\n", "source acknowledged: 11\n",
			} {
				if !strings.Contains(stdout.String(), text) {
					t.Fatalf("broker human status missing %q: %q", text, stdout.String())
				}
			}
		}
	}
}

func statusHTTPResponse(status int, contentType string, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": []string{contentType}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}
