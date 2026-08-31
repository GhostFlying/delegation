//go:build windows

package userservice

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"golang.org/x/sys/windows"
)

func platformPrepareUpgradeActivator(plan UpgradeActivatorPlan) (UpgradeActivatorPlan, error) {
	sid, err := windowsUserSID()
	if err != nil {
		return UpgradeActivatorPlan{}, err
	}
	if !windowsTaskUserIDsEqualValue(plan.UserIdentity, sid) {
		return UpgradeActivatorPlan{}, errors.New("upgrade activator user identity changed")
	}
	plan.Kind = KindScheduledTask
	plan.Name = `\Delegation Upgrade ` + plan.TransactionID
	if filepath.Base(plan.DefinitionPath) != "delegation-upgrade-"+plan.TransactionID+".xml" {
		return UpgradeActivatorPlan{}, errors.New("Scheduled Task activator definition name is not canonical")
	}
	command, err := escapeXML(plan.BinaryPath)
	if err != nil {
		return UpgradeActivatorPlan{}, err
	}
	user, err := escapeXML(sid)
	if err != nil {
		return UpgradeActivatorPlan{}, err
	}
	name, err := escapeXML(plan.Name)
	if err != nil {
		return UpgradeActivatorPlan{}, err
	}
	arguments, err := escapeXML(strings.Join([]string{
		windows.EscapeArg("service"), windows.EscapeArg("upgrade-activate"),
		windows.EscapeArg("--upgrade-root"), windows.EscapeArg(plan.UpgradeRoot),
		windows.EscapeArg("--transaction-id"), windows.EscapeArg(plan.TransactionID),
	}, " "))
	if err != nil {
		return UpgradeActivatorPlan{}, err
	}
	marker, err := escapeXML("delegation-managed-upgrade:v1:" + plan.TransactionID)
	if err != nil {
		return UpgradeActivatorPlan{}, err
	}
	content := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.4" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo><Description>%s</Description><URI>%s</URI></RegistrationInfo>
  <Triggers/>
  <Principals><Principal id="Author"><UserId>%s</UserId><LogonType>InteractiveToken</LogonType></Principal></Principals>
  <Settings><MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy><DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries><StopIfGoingOnBatteries>false</StopIfGoingOnBatteries><StartWhenAvailable>true</StartWhenAvailable><RestartOnFailure><Interval>PT1M</Interval><Count>255</Count></RestartOnFailure><Enabled>false</Enabled><ExecutionTimeLimit>PT0S</ExecutionTimeLimit></Settings>
  <Actions Context="Author"><Exec><Command>%s</Command><Arguments>%s</Arguments></Exec></Actions>
</Task>
`, marker, name, user, command, arguments)
	plan.Definition, err = encodeTaskXMLUTF16LE(content)
	return plan, err
}

func platformInstallUpgradeActivator(ctx context.Context, plan UpgradeActivatorPlan) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	query, err := runTaskCommand("/Query", "/TN", plan.Name, "/XML")
	if err != nil {
		return err
	}
	if query.ExitCode == 0 {
		return scheduledActivatorDefinitionMatches(plan, query.Output)
	}
	result, err := runTaskCommand("/Create", "/TN", plan.Name, "/XML", plan.DefinitionPath)
	if err != nil || result.ExitCode != 0 {
		if matchErr := requireScheduledActivator(plan, false); matchErr != nil {
			return errors.Join(err, taskCommandFailure("register scheduled upgrade activator", result), matchErr)
		}
	}
	return requireScheduledActivator(plan, false)
}

func platformLaunchUpgradeActivator(ctx context.Context, plan UpgradeActivatorPlan) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := requireScheduledActivator(plan, false); err != nil {
		return err
	}
	result, err := runTaskCommand("/Change", "/TN", plan.Name, "/ENABLE")
	if err != nil || result.ExitCode != 0 {
		return errors.Join(err, taskCommandFailure("enable scheduled upgrade activator", result))
	}
	result, err = runTaskCommand("/Run", "/TN", plan.Name)
	if err != nil || result.ExitCode != 0 {
		return errors.Join(err, taskCommandFailure("start scheduled upgrade activator", result))
	}
	return nil
}

func platformRemoveUpgradeActivator(ctx context.Context, plan UpgradeActivatorPlan) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	query, err := runTaskCommand("/Query", "/TN", plan.Name, "/XML")
	if err != nil {
		return err
	}
	if query.ExitCode != 0 {
		return nil
	}
	if err := scheduledActivatorDefinitionMatches(plan, query.Output); err != nil {
		return err
	}
	result, err := runTaskCommand("/Delete", "/F", "/TN", plan.Name)
	if err != nil || result.ExitCode != 0 {
		return errors.Join(err, taskCommandFailure("delete scheduled upgrade activator", result))
	}
	return nil
}

func requireScheduledActivator(plan UpgradeActivatorPlan, enabled bool) error {
	query, err := runTaskCommand("/Query", "/TN", plan.Name, "/XML")
	if err != nil || query.ExitCode != 0 {
		return errors.Join(err, taskCommandFailure("query scheduled upgrade activator", query))
	}
	current, err := parseTaskDefinition(query.Output)
	if err != nil {
		return err
	}
	want, err := parseTaskDefinition(plan.Definition)
	if err != nil {
		return err
	}
	want.Enabled = enabled
	equal, err := scheduledActivatorDefinitionsEquivalent(want, current)
	if err != nil || !equal {
		return errors.Join(err, errors.New("scheduled upgrade activator identity differs"))
	}
	return nil
}

func scheduledActivatorDefinitionMatches(plan UpgradeActivatorPlan, data []byte) error {
	current, err := parseTaskDefinition(data)
	if err != nil {
		return err
	}
	want, err := parseTaskDefinition(plan.Definition)
	if err != nil {
		return err
	}
	want.Enabled = current.Enabled
	equal, err := scheduledActivatorDefinitionsEquivalent(want, current)
	if err != nil || !equal {
		return errors.Join(err, errors.New("scheduled upgrade activator identity differs"))
	}
	return nil
}

func scheduledActivatorDefinitionsEquivalent(desired, existing taskDefinition) (bool, error) {
	if desired.Description != existing.Description || desired.URI != existing.URI ||
		desired.Enabled != existing.Enabled || desired.TriggerUserID != existing.TriggerUserID ||
		desired.Triggers != existing.Triggers || desired.Principals != existing.Principals ||
		desired.Settings != existing.Settings || desired.Actions != existing.Actions {
		return false, nil
	}
	return windowsTaskUserIDsEqual(desired.PrincipalUserID, existing.PrincipalUserID)
}

func windowsTaskUserIDsEqualValue(left, right string) bool {
	equal, err := windowsTaskUserIDsEqual(left, right)
	return err == nil && equal
}
