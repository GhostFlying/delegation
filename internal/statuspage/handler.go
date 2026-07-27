package statuspage

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"html/template"
	"net"
	"net/http"
	"strconv"
	"strings"
)

const (
	// HTMLPath is the server-rendered status endpoint.
	HTMLPath = "/status"
	// JSONPath is the machine-readable status endpoint.
	JSONPath = "/v1/status"

	notFoundBody          = "not found\n"
	methodNotAllowedBody  = "method not allowed\n"
	misdirectedBody       = "misdirected request\n"
	statusUnavailableBody = "status unavailable\n"

	statusStyle = `:root{color-scheme:light dark;font-family:ui-monospace,SFMono-Regular,Consolas,"Liberation Mono",monospace;letter-spacing:0}body{margin:0;padding:2rem;line-height:1.5}main{max-width:52rem;margin:0 auto;overflow-x:auto}h1{font-size:1.5rem;margin:0}.meta{margin:.25rem 0 1.5rem;color:#666}table{width:100%;border-collapse:collapse;font-variant-numeric:tabular-nums}caption{text-align:left;padding:.5rem 0;font-weight:600}th,td{padding:.55rem .5rem;border-bottom:1px solid #8886;text-align:left}td{text-align:right}.group th{padding-top:1.1rem;font-size:.85rem;text-transform:uppercase}@media(prefers-color-scheme:dark){.meta{color:#aaa}}`
)

var (
	statusTemplate = template.Must(template.New("status").Parse(statusDocument))
	statusCSP      = buildCSP(statusStyle)
)

const statusDocument = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Delegation broker status</title>
<style>` + statusStyle + `</style>
</head>
<body>
<main>
<h1>Delegation broker</h1>
<p class="meta">{{if .Version}}Version {{.Version}} | {{end}}Uptime {{.Uptime}}</p>
<table>
<caption>Broker operational status</caption>
<thead><tr><th scope="col">Metric</th><th scope="col">Count</th></tr></thead>
<tbody>
<tr class="group"><th colspan="2" scope="rowgroup">Devices</th></tr>
<tr><th scope="row">Registered</th><td>{{.Devices.Registered}}</td></tr>
<tr><th scope="row">Online</th><td>{{.Devices.Online}}</td></tr>
<tr><th scope="row">Connected</th><td>{{.Devices.Connected}}</td></tr>
<tr><th scope="row">Sync-ready</th><td>{{.Devices.SyncReady}}</td></tr>
</tbody>
<tbody>
<tr class="group"><th colspan="2" scope="rowgroup">Dispatch</th></tr>
<tr><th scope="row">Pending</th><td>{{.Dispatch.Pending}}</td></tr>
<tr><th scope="row">Started</th><td>{{.Dispatch.Started}}</td></tr>
<tr><th scope="row">Failed</th><td>{{.Dispatch.Failed}}</td></tr>
<tr><th scope="row">Lifetime started</th><td>{{.Dispatch.LifetimeStarted}}</td></tr>
</tbody>
<tbody>
<tr class="group"><th colspan="2" scope="rowgroup">Work</th></tr>
<tr><th scope="row">Running turns</th><td>{{.RunningTurns}}</td></tr>
<tr><th scope="row">Occupied slots</th><td>{{.OccupiedSlots}}</td></tr>
<tr><th scope="row">Lifetime turns</th><td>{{.LifetimeTurns}}</td></tr>
<tr><th scope="row">Trees</th><td>{{.Trees}}</td></tr>
</tbody>
<tbody>
<tr class="group"><th colspan="2" scope="rowgroup">Artifacts</th></tr>
<tr><th scope="row">Available</th><td>{{.Artifacts.Available}}</td></tr>
<tr><th scope="row">Unchanged</th><td>{{.Artifacts.Unchanged}}</td></tr>
<tr><th scope="row">Capture failed</th><td>{{.Artifacts.CaptureFailed}}</td></tr>
</tbody>
<tbody>
<tr class="group"><th colspan="2" scope="rowgroup">Results</th></tr>
<tr><th scope="row">Delivery pending</th><td>{{.Results.DeliveryPending}}</td></tr>
<tr><th scope="row">Details retained</th><td>{{.Results.DetailsRetained}}</td></tr>
<tr><th scope="row">Delivered (lifetime)</th><td>{{.Results.Delivered}}</td></tr>
<tr><th scope="row">Source acknowledged (lifetime)</th><td>{{.Results.SourceAcknowledged}}</td></tr>
<tr><th scope="row">Source released (lifetime)</th><td>{{.Results.SourceReleased}}</td></tr>
<tr><th scope="row">Details compacted (lifetime)</th><td>{{.Results.DetailsCompacted}}</td></tr>
</tbody>
</table>
</main>
</body>
</html>
`

type handler struct {
	provider Provider
}

type pageData struct {
	Version       string
	Uptime        string
	Devices       DeviceCounts
	Dispatch      DispatchCounts
	RunningTurns  uint64
	OccupiedSlots uint64
	LifetimeTurns uint64
	Trees         uint64
	Artifacts     ArtifactCounts
	Results       ResultCounts
}

// NewHandler returns an HTTP handler for the aggregate HTML and JSON status
// endpoints. Listener selection and authentication remain the caller's
// responsibility.
func NewHandler(provider Provider) http.Handler {
	return &handler{provider: provider}
}

func (h *handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	setSecurityHeaders(writer.Header())
	if !loopbackRequestHost(request.Host) {
		writeResponse(
			writer, request.Method, http.StatusMisdirectedRequest,
			"text/plain; charset=utf-8", []byte(misdirectedBody),
		)
		return
	}

	switch request.URL.Path {
	case HTMLPath, JSONPath:
	default:
		writeResponse(writer, request.Method, http.StatusNotFound, "text/plain; charset=utf-8", []byte(notFoundBody))
		return
	}

	if request.Method != http.MethodGet && request.Method != http.MethodHead {
		writer.Header().Set("Allow", http.MethodGet+", "+http.MethodHead)
		writeResponse(writer, request.Method, http.StatusMethodNotAllowed, "text/plain; charset=utf-8", []byte(methodNotAllowedBody))
		return
	}

	if h.provider == nil {
		writeResponse(writer, request.Method, http.StatusServiceUnavailable, "text/plain; charset=utf-8", []byte(statusUnavailableBody))
		return
	}
	snapshot, err := h.provider(request.Context())
	if err != nil || snapshot.Validate() != nil {
		writeResponse(writer, request.Method, http.StatusServiceUnavailable, "text/plain; charset=utf-8", []byte(statusUnavailableBody))
		return
	}

	var contentType string
	var body []byte
	switch request.URL.Path {
	case HTMLPath:
		contentType = "text/html; charset=utf-8"
		body, err = renderHTML(snapshot)
	case JSONPath:
		contentType = "application/json; charset=utf-8"
		body, err = json.Marshal(snapshot)
		body = append(body, '\n')
	}
	if err != nil {
		writeResponse(writer, request.Method, http.StatusServiceUnavailable, "text/plain; charset=utf-8", []byte(statusUnavailableBody))
		return
	}
	writeResponse(writer, request.Method, http.StatusOK, contentType, body)
}

func loopbackRequestHost(authority string) bool {
	if authority == "" || strings.ContainsAny(authority, "/@?#") {
		return false
	}
	host := authority
	if parsedHost, _, err := net.SplitHostPort(authority); err == nil {
		host = parsedHost
	} else if strings.HasPrefix(authority, "[") && strings.HasSuffix(authority, "]") {
		host = strings.TrimSuffix(strings.TrimPrefix(authority, "["), "]")
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func renderHTML(snapshot Snapshot) ([]byte, error) {
	data := pageData{
		Version:       snapshot.Version,
		Uptime:        formatUptime(snapshot.UptimeSeconds),
		Devices:       snapshot.Devices,
		Dispatch:      snapshot.Dispatch,
		RunningTurns:  snapshot.RunningTurns,
		OccupiedSlots: snapshot.OccupiedSlots,
		LifetimeTurns: snapshot.LifetimeTurns,
		Trees:         snapshot.Trees,
		Artifacts:     snapshot.Artifacts,
		Results:       snapshot.Results,
	}
	var body bytes.Buffer
	if err := statusTemplate.Execute(&body, data); err != nil {
		return nil, err
	}
	return body.Bytes(), nil
}

func formatUptime(seconds uint64) string {
	const (
		secondsPerMinute = 60
		secondsPerHour   = 60 * secondsPerMinute
		secondsPerDay    = 24 * secondsPerHour
	)
	days := seconds / secondsPerDay
	seconds %= secondsPerDay
	hours := seconds / secondsPerHour
	seconds %= secondsPerHour
	minutes := seconds / secondsPerMinute
	seconds %= secondsPerMinute

	if days > 0 {
		return strconv.FormatUint(days, 10) + "d " + strconv.FormatUint(hours, 10) + "h " +
			strconv.FormatUint(minutes, 10) + "m " + strconv.FormatUint(seconds, 10) + "s"
	}
	if hours > 0 {
		return strconv.FormatUint(hours, 10) + "h " + strconv.FormatUint(minutes, 10) + "m " +
			strconv.FormatUint(seconds, 10) + "s"
	}
	if minutes > 0 {
		return strconv.FormatUint(minutes, 10) + "m " + strconv.FormatUint(seconds, 10) + "s"
	}
	return strconv.FormatUint(seconds, 10) + "s"
}

func setSecurityHeaders(header http.Header) {
	header.Set("Cache-Control", "no-store")
	header.Set("Content-Security-Policy", statusCSP)
	header.Set("Referrer-Policy", "no-referrer")
	header.Set("X-Content-Type-Options", "nosniff")
}

func buildCSP(style string) string {
	digest := sha256.Sum256([]byte(style))
	encoded := base64.StdEncoding.EncodeToString(digest[:])
	return "default-src 'none'; style-src 'sha256-" + encoded + "'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'"
}

func writeResponse(writer http.ResponseWriter, method string, status int, contentType string, body []byte) {
	writer.Header().Set("Content-Type", contentType)
	writer.Header().Set("Content-Length", strconv.Itoa(len(body)))
	writer.WriteHeader(status)
	if method != http.MethodHead {
		_, _ = writer.Write(body)
	}
}
