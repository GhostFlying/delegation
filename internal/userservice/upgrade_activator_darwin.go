//go:build darwin

package userservice

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
)

func platformPrepareUpgradeActivator(plan UpgradeActivatorPlan) (UpgradeActivatorPlan, error) {
	if plan.UserIdentity != strconv.Itoa(os.Geteuid()) {
		return UpgradeActivatorPlan{}, errors.New("upgrade activator user identity changed")
	}
	plan.Kind = KindLaunchAgent
	plan.Name = "com.github.ghostflying.delegation.upgrade." + plan.TransactionID
	if filepath.Base(plan.DefinitionPath) != plan.Name+".plist" {
		return UpgradeActivatorPlan{}, errors.New("LaunchAgent activator definition name is not canonical")
	}
	label, err := escapeXML(plan.Name)
	if err != nil {
		return UpgradeActivatorPlan{}, err
	}
	binary, err := escapeXML(plan.BinaryPath)
	if err != nil {
		return UpgradeActivatorPlan{}, err
	}
	root, err := escapeXML(plan.UpgradeRoot)
	if err != nil {
		return UpgradeActivatorPlan{}, err
	}
	transactionID, err := escapeXML(plan.TransactionID)
	if err != nil {
		return UpgradeActivatorPlan{}, err
	}
	plan.Definition = []byte(fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>%s</string>
  <key>Description</key>
  <string>delegation-managed-upgrade:v1:%s</string>
  <key>ProgramArguments</key>
  <array>
    <string>%s</string>
    <string>service</string>
    <string>upgrade-activate</string>
    <string>--upgrade-root</string>
    <string>%s</string>
    <string>--transaction-id</string>
    <string>%s</string>
  </array>
  <key>RunAtLoad</key>
  <false/>
  <key>KeepAlive</key>
  <dict>
    <key>SuccessfulExit</key>
    <false/>
  </dict>
  <key>ThrottleInterval</key>
  <integer>5</integer>
  <key>ProcessType</key>
  <string>Background</string>
</dict>
</plist>
`, label, transactionID, binary, root, transactionID))
	return plan, nil
}

func platformInstallUpgradeActivator(ctx context.Context, plan UpgradeActivatorPlan) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	status, loaded, err := printLaunchAgent(launchAgentActivatorTarget(plan))
	if err != nil {
		return err
	}
	if loaded {
		if !launchAgentActivatorMatches(status, plan) {
			return errors.New("LaunchAgent upgrade activator label is occupied by another definition")
		}
		return nil
	}
	domain := fmt.Sprintf("gui/%d", os.Geteuid())
	result, runErr := runLaunchctl("bootstrap", domain, plan.DefinitionPath)
	if runErr != nil || result.ExitCode != 0 {
		return errors.Join(runErr, commandFailure("bootstrap upgrade activator", result))
	}
	status, loaded, err = printLaunchAgent(launchAgentActivatorTarget(plan))
	if err != nil || !loaded || !launchAgentActivatorMatches(status, plan) {
		return errors.Join(err, errors.New("LaunchAgent upgrade activator registration differs"))
	}
	return nil
}

func platformLaunchUpgradeActivator(ctx context.Context, plan UpgradeActivatorPlan) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	target := launchAgentActivatorTarget(plan)
	status, loaded, err := printLaunchAgent(target)
	if err != nil {
		return err
	}
	if !loaded || !launchAgentActivatorMatches(status, plan) {
		return errors.New("LaunchAgent upgrade activator identity differs")
	}
	result, runErr := runLaunchctl("kickstart", target)
	if runErr != nil || result.ExitCode != 0 {
		return errors.Join(runErr, commandFailure("start LaunchAgent upgrade activator", result))
	}
	return nil
}

func platformRemoveUpgradeActivator(ctx context.Context, plan UpgradeActivatorPlan) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	target := launchAgentActivatorTarget(plan)
	status, loaded, err := printLaunchAgent(target)
	if err != nil {
		return err
	}
	if !loaded {
		return nil
	}
	if !launchAgentActivatorMatches(status, plan) {
		return errors.New("LaunchAgent upgrade activator identity differs")
	}
	result, runErr := runLaunchctl("bootout", target)
	if runErr != nil || result.ExitCode != 0 {
		return errors.Join(runErr, commandFailure("remove LaunchAgent upgrade activator", result))
	}
	return nil
}

func launchAgentActivatorTarget(plan UpgradeActivatorPlan) string {
	return fmt.Sprintf("gui/%d/%s", os.Geteuid(), plan.Name)
}

func launchAgentActivatorMatches(status launchAgentStatus, plan UpgradeActivatorPlan) bool {
	wantArguments := []string{
		plan.BinaryPath, "service", "upgrade-activate", "--upgrade-root", plan.UpgradeRoot,
		"--transaction-id", plan.TransactionID,
	}
	return filepath.Clean(status.Path) == filepath.Clean(plan.DefinitionPath) &&
		status.Program != "" && filepath.Clean(status.Program) == filepath.Clean(plan.BinaryPath) &&
		status.ArgumentsPresent && slices.Equal(status.Arguments, wantArguments)
}
