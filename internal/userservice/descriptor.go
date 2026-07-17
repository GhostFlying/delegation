package userservice

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	Marker          = "delegation-managed:v1"
	SystemdUnitName = "delegation.service"
	LaunchAgentName = "com.github.ghostflying.delegation"
	ScheduledTask   = "Delegation Connector"
)

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

func RenderSystemd(binaryPath, configPath string) (Descriptor, error) {
	if err := validatePOSIXPaths(binaryPath, configPath); err != nil {
		return Descriptor{}, err
	}
	content := fmt.Sprintf(`# %s
# Prepared by Delegation M0; enablement starts in M1.
[Unit]
Description=Delegation connector

[Service]
Type=exec
ExecStart=%s service run --config %s
Restart=on-failure
RestartSec=5
UMask=0077

[Install]
WantedBy=default.target
`, Marker, systemdQuote(binaryPath), systemdQuote(configPath))
	return Descriptor{Kind: KindSystemd, Name: SystemdUnitName, Content: []byte(content)}, nil
}

func RenderLaunchAgent(binaryPath, configPath string) (Descriptor, error) {
	if err := validatePOSIXPaths(binaryPath, configPath); err != nil {
		return Descriptor{}, err
	}
	binaryXML, err := escapeXML(binaryPath)
	if err != nil {
		return Descriptor{}, err
	}
	configXML, err := escapeXML(configPath)
	if err != nil {
		return Descriptor{}, err
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
`, LaunchAgentName, Marker, binaryXML, configXML)
	return Descriptor{Kind: KindLaunchAgent, Name: LaunchAgentName, Content: []byte(content)}, nil
}

func RenderScheduledTask(binaryPath, configPath, userSID string, escapeArg func(string) string) (Descriptor, error) {
	if !windowsAbsolute(binaryPath) || !windowsAbsolute(configPath) {
		return Descriptor{}, errors.New("service binary and config paths must be absolute Windows paths")
	}
	if err := validateTextPaths(binaryPath, configPath); err != nil {
		return Descriptor{}, err
	}
	if strings.TrimSpace(userSID) == "" || escapeArg == nil {
		return Descriptor{}, errors.New("Windows user SID and argument escaper are required")
	}
	binaryXML, err := escapeXML(binaryPath)
	if err != nil {
		return Descriptor{}, err
	}
	sidXML, err := escapeXML(userSID)
	if err != nil {
		return Descriptor{}, err
	}
	arguments := strings.Join([]string{
		escapeArg("service"),
		escapeArg("run"),
		escapeArg("--config"),
		escapeArg(configPath),
	}, " ")
	argumentsXML, err := escapeXML(arguments)
	if err != nil {
		return Descriptor{}, err
	}
	content := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<Task version="1.4" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo>
    <Description>%s</Description>
    <URI>\Delegation\Connector</URI>
  </RegistrationInfo>
  <Triggers>
    <LogonTrigger>
      <Enabled>true</Enabled>
      <UserId>%s</UserId>
    </LogonTrigger>
  </Triggers>
  <Principals>
    <Principal id="Author">
      <UserId>%s</UserId>
      <LogonType>InteractiveToken</LogonType>
      <RunLevel>LeastPrivilege</RunLevel>
    </Principal>
  </Principals>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <AllowHardTerminate>true</AllowHardTerminate>
    <StartWhenAvailable>true</StartWhenAvailable>
    <RunOnlyIfNetworkAvailable>false</RunOnlyIfNetworkAvailable>
    <IdleSettings>
      <StopOnIdleEnd>false</StopOnIdleEnd>
      <RestartOnIdle>false</RestartOnIdle>
    </IdleSettings>
    <AllowStartOnDemand>true</AllowStartOnDemand>
    <Enabled>false</Enabled>
    <Hidden>false</Hidden>
    <RunOnlyIfIdle>false</RunOnlyIfIdle>
    <WakeToRun>false</WakeToRun>
    <ExecutionTimeLimit>PT0S</ExecutionTimeLimit>
    <Priority>7</Priority>
  </Settings>
  <Actions Context="Author">
    <Exec>
      <Command>%s</Command>
      <Arguments>%s</Arguments>
    </Exec>
  </Actions>
</Task>
`, Marker, sidXML, sidXML, binaryXML, argumentsXML)
	return Descriptor{Kind: KindScheduledTask, Name: ScheduledTask, Content: []byte(content)}, nil
}

func validatePOSIXPaths(paths ...string) error {
	for _, path := range paths {
		if !filepath.IsAbs(path) {
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
