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

	"github.com/GhostFlying/delegation/internal/localbridge"
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
	// A successful response comes from the running broker even when upgrading
	// from an older runtime that did not serialize serviceRunning.
	snapshot.ServiceRunning = true
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
		fmt.Fprintf(&rendered, "transport: %s\n", status.Transport)
		if status.TailscaleHostname != "" {
			fmt.Fprintf(&rendered, "tailscale hostname: %s\n", status.TailscaleHostname)
		}
		fmt.Fprintf(&rendered, "service running: %t\n", status.ServiceRunning)
		writeUpgradeStatus(&rendered, fromStatusPageUpgrade(status.Upgrade))
		writeControllerUpgradeStatus(&rendered, status.ControllerUpgrade)
		if status.ServiceRunning {
			fmt.Fprintf(&rendered, "uptime seconds: %d\n", status.UptimeSeconds)
			fmt.Fprintln(&rendered, "devices:")
			fmt.Fprintf(&rendered, "  registered: %d\n", status.Devices.Registered)
			fmt.Fprintf(&rendered, "  online: %d\n", status.Devices.Online)
			fmt.Fprintf(&rendered, "  connected: %d\n", status.Devices.Connected)
			fmt.Fprintf(&rendered, "  sync ready: %d\n", status.Devices.SyncReady)
			fmt.Fprintf(&rendered, "  worker ready: %d\n", status.Devices.WorkerReady)
			fmt.Fprintf(&rendered, "  dispatchable: %d\n", status.Devices.Dispatchable)
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
			fmt.Fprintf(&rendered, "  details retained: %d\n", status.Results.DetailsRetained)
			fmt.Fprintf(&rendered, "  lifetime delivered: %d\n", status.Results.Delivered)
			fmt.Fprintf(&rendered, "  lifetime source acknowledged: %d\n", status.Results.SourceAcknowledged)
			fmt.Fprintf(&rendered, "  lifetime source released: %d\n", status.Results.SourceReleased)
			fmt.Fprintf(&rendered, "  lifetime details compacted: %d\n", status.Results.DetailsCompacted)
		}
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

func writeControllerUpgradeStatus(rendered *bytes.Buffer, upgrade *statuspage.ControllerUpgrade) {
	if upgrade == nil {
		return
	}
	fmt.Fprintln(rendered, "controller upgrade:")
	fmt.Fprintf(rendered, "  transaction: %s\n", upgrade.TransactionID)
	fmt.Fprintf(rendered, "  state: %s\n", upgrade.State)
	fmt.Fprintf(rendered, "  version: %s -> %s\n", upgrade.SourceVersion, upgrade.TargetVersion)
	fmt.Fprintf(rendered, "  global commit: %t\n", upgrade.GlobalCommit)
	fmt.Fprintf(rendered, "  completion deadline: %d\n", upgrade.CompletionDeadline)
	fmt.Fprintf(rendered, "  participants total: %d\n", upgrade.Participants.Total)
	fmt.Fprintf(rendered, "  participants qualified: %d\n", upgrade.Participants.Qualified)
	fmt.Fprintf(rendered, "  participants intervention required: %d\n",
		upgrade.Participants.InterventionRequired)
	if upgrade.BrokerFailureCode != "" {
		fmt.Fprintf(rendered, "  broker failure: %s\n", upgrade.BrokerFailureCode)
	}
	if upgrade.FailureCode != "" {
		fmt.Fprintf(rendered, "  failure: %s\n", upgrade.FailureCode)
	}
}

func toStatusPageUpgrade(upgrade *localbridge.UpgradeSnapshot) *statuspage.Upgrade {
	if upgrade == nil {
		return nil
	}
	return &statuspage.Upgrade{
		TransactionID: upgrade.TransactionID, State: upgrade.State,
		SourceVersion: upgrade.SourceVersion, TargetVersion: upgrade.TargetVersion,
		CommitAuthorized: upgrade.CommitAuthorized, FailureCode: upgrade.FailureCode,
		UpdatedAt: upgrade.UpdatedAt,
	}
}

func fromStatusPageUpgrade(upgrade *statuspage.Upgrade) *localbridge.UpgradeSnapshot {
	if upgrade == nil {
		return nil
	}
	return &localbridge.UpgradeSnapshot{
		TransactionID: upgrade.TransactionID, State: upgrade.State,
		SourceVersion: upgrade.SourceVersion, TargetVersion: upgrade.TargetVersion,
		CommitAuthorized: upgrade.CommitAuthorized, FailureCode: upgrade.FailureCode,
		UpdatedAt: upgrade.UpdatedAt,
	}
}
