package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"

	"github.com/GhostFlying/delegation/internal/statuspage"
)

const maximumBrokerStatusResponse = 16 * 1024

type statusHTTPDoer interface {
	Do(*http.Request) (*http.Response, error)
}

func readBrokerStatus(ctx context.Context, address string) (statuspage.Snapshot, error) {
	client := &http.Client{
		Timeout: peerStatusReadTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	return readBrokerStatusWithClient(ctx, address, client)
}

func readBrokerStatusWithClient(
	ctx context.Context,
	address string,
	client statusHTTPDoer,
) (statuspage.Snapshot, error) {
	if client == nil {
		return statuspage.Snapshot{}, errors.New("broker status HTTP client is required")
	}
	request, err := http.NewRequestWithContext(
		ctx, http.MethodGet, "http://"+address+statuspage.JSONPath, nil,
	)
	if err != nil {
		return statuspage.Snapshot{}, fmt.Errorf("build broker status request: %w", err)
	}
	response, err := client.Do(request)
	if err != nil {
		return statuspage.Snapshot{}, fmt.Errorf("request broker status: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return statuspage.Snapshot{}, fmt.Errorf("broker status returned HTTP %d", response.StatusCode)
	}
	mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return statuspage.Snapshot{}, errors.New("broker status returned an invalid content type")
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maximumBrokerStatusResponse+1))
	if err != nil {
		return statuspage.Snapshot{}, fmt.Errorf("read broker status: %w", err)
	}
	if len(body) > maximumBrokerStatusResponse {
		return statuspage.Snapshot{}, errors.New("broker status response exceeds the size limit")
	}
	var snapshot statuspage.Snapshot
	if err := json.Unmarshal(body, &snapshot); err != nil {
		return statuspage.Snapshot{}, fmt.Errorf("decode broker status: %w", err)
	}
	if err := snapshot.Validate(); err != nil {
		return statuspage.Snapshot{}, fmt.Errorf("validate broker status: %w", err)
	}
	return snapshot, nil
}

func writeBrokerStatus(
	stdout io.Writer,
	stderr io.Writer,
	status statuspage.Snapshot,
	jsonOutput bool,
) int {
	var output []byte
	if jsonOutput {
		var err error
		output, err = json.Marshal(status)
		if err != nil {
			return writeFixedStatusError(stderr, statusOutputError, 1)
		}
		output = append(output, '\n')
	} else {
		var rendered bytes.Buffer
		fmt.Fprintln(&rendered, "delegation broker status")
		fmt.Fprintf(&rendered, "version: %s\n", status.Version)
		fmt.Fprintf(&rendered, "uptime seconds: %d\n", status.UptimeSeconds)
		fmt.Fprintln(&rendered, "devices:")
		fmt.Fprintf(&rendered, "  registered: %d\n", status.Devices.Registered)
		fmt.Fprintf(&rendered, "  online: %d\n", status.Devices.Online)
		fmt.Fprintf(&rendered, "  connected: %d\n", status.Devices.Connected)
		fmt.Fprintf(&rendered, "  sync ready: %d\n", status.Devices.SyncReady)
		fmt.Fprintln(&rendered, "dispatches:")
		fmt.Fprintf(&rendered, "  pending: %d\n", status.Dispatch.Pending)
		fmt.Fprintf(&rendered, "  started: %d\n", status.Dispatch.Started)
		fmt.Fprintf(&rendered, "  failed: %d\n", status.Dispatch.Failed)
		fmt.Fprintf(&rendered, "  lifetime started: %d\n", status.Dispatch.LifetimeStarted)
		fmt.Fprintf(&rendered, "running turns: %d\n", status.RunningTurns)
		fmt.Fprintf(&rendered, "occupied worker slots: %d\n", status.OccupiedSlots)
		fmt.Fprintf(&rendered, "lifetime turns: %d\n", status.LifetimeTurns)
		fmt.Fprintf(&rendered, "trees: %d\n", status.Trees)
		fmt.Fprintln(&rendered, "artifacts:")
		fmt.Fprintf(&rendered, "  available: %d\n", status.Artifacts.Available)
		fmt.Fprintf(&rendered, "  unchanged: %d\n", status.Artifacts.Unchanged)
		fmt.Fprintf(&rendered, "  capture failed: %d\n", status.Artifacts.CaptureFailed)
		fmt.Fprintln(&rendered, "results:")
		fmt.Fprintf(&rendered, "  delivery pending: %d\n", status.Results.DeliveryPending)
		fmt.Fprintf(&rendered, "  delivered: %d\n", status.Results.Delivered)
		fmt.Fprintf(&rendered, "  source acknowledged: %d\n", status.Results.SourceAcknowledged)
		fmt.Fprintf(&rendered, "  source released: %d\n", status.Results.SourceReleased)
		output = rendered.Bytes()
	}
	if len(output) == 0 || len(output) > maximumStatusOutput {
		return writeFixedStatusError(stderr, statusOutputError, 1)
	}
	if _, err := io.Copy(stdout, bytes.NewReader(output)); err != nil {
		return writeFixedStatusError(stderr, statusOutputError, 1)
	}
	return 0
}
