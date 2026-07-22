package userservice

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"path"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	markerPrefix = "delegation-managed:v1:"

	MarkerBroker = markerPrefix + "broker"
	MarkerPeer   = markerPrefix + "peer"

	SystemdBrokerUnitName = "delegation-broker.service"
	SystemdPeerUnitName   = "delegation-peer.service"
	LaunchAgentBrokerName = "com.github.ghostflying.delegation.broker"
	LaunchAgentPeerName   = "com.github.ghostflying.delegation.peer"
	ScheduledTaskBroker   = `\Delegation Broker`
	ScheduledTaskPeer     = `\Delegation Peer`
)

type ServiceRole string

const (
	ServiceRoleBroker ServiceRole = "broker"
	ServiceRolePeer   ServiceRole = "peer"
)

type serviceSpec struct {
	role        ServiceRole
	marker      string
	systemdUnit string
	launchAgent string
	scheduled   string
	description string
}

func specFor(role ServiceRole) (serviceSpec, error) {
	switch role {
	case ServiceRoleBroker:
		return serviceSpec{
			role: role, marker: MarkerBroker,
			systemdUnit: SystemdBrokerUnitName,
			launchAgent: LaunchAgentBrokerName,
			scheduled:   ScheduledTaskBroker, description: "Delegation broker",
		}, nil
	case ServiceRolePeer:
		return serviceSpec{
			role: role, marker: MarkerPeer,
			systemdUnit: SystemdPeerUnitName,
			launchAgent: LaunchAgentPeerName,
			scheduled:   ScheduledTaskPeer, description: "Delegation peer",
		}, nil
	default:
		return serviceSpec{}, fmt.Errorf("unsupported service role %q", role)
	}
}

type Kind string

const (
	KindSystemd       Kind = "systemdUser"
	KindLaunchAgent   Kind = "launchAgent"
	KindScheduledTask Kind = "scheduledTask"
)

type Descriptor struct {
	Kind    Kind
	Name    string
	Content []byte
}

type Invocation struct {
	BinaryPath      string
	ConfigPath      string
	EnvironmentFile string
}

func RenderSystemd(role ServiceRole, invocation Invocation) (Descriptor, error) {
	spec, err := specFor(role)
	if err != nil {
		return Descriptor{}, err
	}
	if err := validateInvocation(role, invocation, false); err != nil {
		return Descriptor{}, err
	}
	environmentArgument := ""
	if invocation.EnvironmentFile != "" {
		environmentArgument = " --environment-file " + systemdQuote(invocation.EnvironmentFile)
	}
	content := fmt.Sprintf(`# %s
[Unit]
Description=%s

[Service]
Type=exec
ExecStart=%s service run --config %s%s
Restart=on-failure
RestartSec=5
UMask=0077

[Install]
WantedBy=default.target
`, spec.marker, spec.description, systemdQuote(invocation.BinaryPath), systemdQuote(invocation.ConfigPath), environmentArgument)
	return Descriptor{Kind: KindSystemd, Name: spec.systemdUnit, Content: []byte(content)}, nil
}

func RenderLaunchAgent(role ServiceRole, invocation Invocation) (Descriptor, error) {
	spec, err := specFor(role)
	if err != nil {
		return Descriptor{}, err
	}
	if err := validateInvocation(role, invocation, false); err != nil {
		return Descriptor{}, err
	}
	binaryXML, err := escapeXML(invocation.BinaryPath)
	if err != nil {
		return Descriptor{}, err
	}
	configXML, err := escapeXML(invocation.ConfigPath)
	if err != nil {
		return Descriptor{}, err
	}
	environmentXML := ""
	if invocation.EnvironmentFile != "" {
		escaped, err := escapeXML(invocation.EnvironmentFile)
		if err != nil {
			return Descriptor{}, err
		}
		environmentXML = fmt.Sprintf("    <string>--environment-file</string>\n    <string>%s</string>\n", escaped)
	}
	content := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>%s</string>
  <key>Description</key>
  <string>%s</string>
  <key>ProgramArguments</key>
  <array>
    <string>%s</string>
    <string>service</string>
    <string>run</string>
    <string>--config</string>
    <string>%s</string>
%s
  </array>
  <key>Disabled</key>
  <true/>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <dict>
    <key>SuccessfulExit</key>
    <false/>
  </dict>
  <key>ThrottleInterval</key>
  <integer>5</integer>
</dict>
</plist>
`, spec.launchAgent, spec.marker, binaryXML, configXML, strings.TrimSuffix(environmentXML, "\n"))
	return Descriptor{Kind: KindLaunchAgent, Name: spec.launchAgent, Content: []byte(content)}, nil
}

func RenderScheduledTask(
	role ServiceRole,
	invocation Invocation,
	userSID string,
	escapeArg func(string) string,
) (Descriptor, error) {
	spec, err := specFor(role)
	if err != nil {
		return Descriptor{}, err
	}
	taskPath := spec.scheduled
	if err := validateInvocation(role, invocation, true); err != nil {
		return Descriptor{}, err
	}
	if err := validateTextPaths(taskPath); err != nil {
		return Descriptor{}, err
	}
	if strings.TrimSpace(userSID) == "" || len(taskPath) < 2 || taskPath[0] != '\\' ||
		strings.HasPrefix(taskPath, `\\`) || strings.HasSuffix(taskPath, `\`) ||
		strings.Contains(taskPath, "/") || strings.Contains(taskPath[1:], `\\`) || escapeArg == nil {
		return Descriptor{}, errors.New("Windows user SID, absolute task path, and argument escaper are required")
	}
	binaryXML, err := escapeXML(invocation.BinaryPath)
	if err != nil {
		return Descriptor{}, err
	}
	sidXML, err := escapeXML(userSID)
	if err != nil {
		return Descriptor{}, err
	}
	taskPathXML, err := escapeXML(taskPath)
	if err != nil {
		return Descriptor{}, err
	}
	arguments := strings.Join([]string{
		escapeArg("service"),
		escapeArg("run"),
		escapeArg("--config"),
		escapeArg(invocation.ConfigPath),
	}, " ")
	if invocation.EnvironmentFile != "" {
		arguments += " " + escapeArg("--environment-file") + " " + escapeArg(invocation.EnvironmentFile)
	}
	argumentsXML, err := escapeXML(arguments)
	if err != nil {
		return Descriptor{}, err
	}
	// Omit values that Task Scheduler exports as defaults, while pinning effective non-default behavior.
	content := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.4" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo>
    <Description>%s</Description>
    <URI>%s</URI>
  </RegistrationInfo>
  <Triggers>
    <LogonTrigger>
      <UserId>%s</UserId>
    </LogonTrigger>
  </Triggers>
  <Principals>
    <Principal id="Author">
      <UserId>%s</UserId>
      <LogonType>InteractiveToken</LogonType>
    </Principal>
  </Principals>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <StartWhenAvailable>true</StartWhenAvailable>
    <IdleSettings>
      <StopOnIdleEnd>false</StopOnIdleEnd>
      <RestartOnIdle>false</RestartOnIdle>
    </IdleSettings>
    <Enabled>false</Enabled>
    <ExecutionTimeLimit>PT0S</ExecutionTimeLimit>
    <UseUnifiedSchedulingEngine>true</UseUnifiedSchedulingEngine>
  </Settings>
  <Actions Context="Author">
    <Exec>
      <Command>%s</Command>
      <Arguments>%s</Arguments>
    </Exec>
  </Actions>
</Task>
`, spec.marker, taskPathXML, sidXML, sidXML, binaryXML, argumentsXML)
	encoded, err := encodeTaskXMLUTF16LE(content)
	if err != nil {
		return Descriptor{}, err
	}
	return Descriptor{Kind: KindScheduledTask, Name: taskPath, Content: encoded}, nil
}

func validateInvocation(role ServiceRole, invocation Invocation, windowsPaths bool) error {
	paths := []string{invocation.BinaryPath, invocation.ConfigPath}
	if role == ServiceRolePeer {
		if invocation.EnvironmentFile == "" {
			return errors.New("peer service environment file is required")
		}
		paths = append(paths, invocation.EnvironmentFile)
	} else if role == ServiceRoleBroker {
		if invocation.EnvironmentFile != "" {
			return errors.New("broker service must not use a peer environment file")
		}
	} else {
		return fmt.Errorf("unsupported service role %q", role)
	}
	if windowsPaths {
		for _, candidate := range paths {
			if !windowsAbsolute(candidate) {
				return errors.New("service paths must be absolute Windows paths")
			}
		}
		return validateTextPaths(paths...)
	}
	return validatePOSIXPaths(paths...)
}

func validatePOSIXPaths(paths ...string) error {
	for _, value := range paths {
		if !path.IsAbs(value) {
			return errors.New("service binary and config paths must be absolute")
		}
	}
	return validateTextPaths(paths...)
}

func validateTextPaths(paths ...string) error {
	for _, path := range paths {
		if !utf8.ValidString(path) {
			return errors.New("service paths must be valid UTF-8")
		}
		if strings.IndexFunc(path, unicode.IsControl) >= 0 {
			return errors.New("service paths must not contain control characters")
		}
	}
	return nil
}

func windowsAbsolute(path string) bool {
	if strings.HasPrefix(path, `\\`) {
		if strings.HasPrefix(path, `\\.\`) {
			return false
		}
		parts := strings.FieldsFunc(strings.TrimPrefix(path, `\\`), func(char rune) bool {
			return char == '\\' || char == '/'
		})
		return len(parts) >= 2
	}
	return len(path) >= 3 && ((path[0] >= 'A' && path[0] <= 'Z') || (path[0] >= 'a' && path[0] <= 'z')) &&
		path[1] == ':' && (path[2] == '\\' || path[2] == '/')
}

func systemdQuote(value string) string {
	replacer := strings.NewReplacer(`\`, `\\`, `"`, `\"`, `%`, `%%`, `$`, `$$`)
	return `"` + replacer.Replace(value) + `"`
}

func escapeXML(value string) (string, error) {
	var escaped bytes.Buffer
	if err := xml.EscapeText(&escaped, []byte(value)); err != nil {
		return "", fmt.Errorf("escape service XML: %w", err)
	}
	return escaped.String(), nil
}
