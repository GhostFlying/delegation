//go:build linux

package userservice

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func platformPrepareUpgradeActivator(plan UpgradeActivatorPlan) (UpgradeActivatorPlan, error) {
	if plan.UserIdentity != strconv.Itoa(os.Geteuid()) {
		return UpgradeActivatorPlan{}, errors.New("upgrade activator user identity changed")
	}
	plan.Kind = KindSystemd
	plan.Name = "delegation-upgrade-" + plan.TransactionID + ".service"
	if filepath.Base(plan.DefinitionPath) != plan.Name {
		return UpgradeActivatorPlan{}, errors.New("systemd activator definition name is not canonical")
	}
	plan.Definition = []byte(fmt.Sprintf(`# delegation-managed-upgrade:v1:%s
[Unit]
Description=Delegation local upgrade activator

[Service]
Type=oneshot
ExecStart=%s service upgrade-activate --upgrade-root %s --transaction-id %s
Restart=on-failure
RestartSec=5
UMask=0077
`, plan.TransactionID, systemdQuote(plan.BinaryPath), systemdQuote(plan.UpgradeRoot),
		systemdQuote(plan.TransactionID)))
	return plan, nil
}

func platformInstallUpgradeActivator(ctx context.Context, plan UpgradeActivatorPlan) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	linked, err := runSystemctl("--user", "--no-ask-password", "link", plan.DefinitionPath)
	if err != nil || linked.ExitCode != 0 {
		if _, match, _ := systemdActivatorIdentity(plan); !match {
			return errors.Join(err, commandFailure("link systemd upgrade activator", linked))
		}
	}
	reloaded, err := runSystemctl("--user", "--no-ask-password", "daemon-reload")
	if err != nil || reloaded.ExitCode != 0 {
		return errors.Join(err, commandFailure("reload systemd upgrade activator", reloaded))
	}
	_, matched, err := systemdActivatorIdentity(plan)
	if err != nil {
		return err
	}
	if !matched {
		return errors.New("systemd upgrade activator is loaded from an unexpected path")
	}
	return nil
}

func platformLaunchUpgradeActivator(ctx context.Context, plan UpgradeActivatorPlan) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, matched, err := systemdActivatorIdentity(plan)
	if err != nil || !matched {
		return errors.Join(err, errors.New("systemd upgrade activator identity does not match"))
	}
	started, err := runSystemctl(
		"--user", "--no-ask-password", "start", "--no-block", plan.Name,
	)
	if err != nil || started.ExitCode != 0 {
		return errors.Join(err, commandFailure("start systemd upgrade activator", started))
	}
	return nil
}

func platformRemoveUpgradeActivator(ctx context.Context, plan UpgradeActivatorPlan) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	present, matched, err := systemdActivatorIdentity(plan)
	if err != nil {
		return err
	}
	if present && !matched {
		return errors.New("systemd upgrade activator name is occupied by another definition")
	}
	if matched {
		reverted, runErr := runSystemctl("--user", "--no-ask-password", "revert", plan.Name)
		if runErr != nil || reverted.ExitCode != 0 {
			return errors.Join(runErr, commandFailure("unlink systemd upgrade activator", reverted))
		}
	}
	reloaded, runErr := runSystemctl("--user", "--no-ask-password", "daemon-reload")
	if runErr != nil || reloaded.ExitCode != 0 {
		return errors.Join(runErr, commandFailure("reload removed systemd upgrade activator", reloaded))
	}
	if stillPresent, _, matchErr := systemdActivatorIdentity(plan); matchErr != nil {
		return matchErr
	} else if stillPresent {
		return errors.New("systemd upgrade activator name remained occupied after removal")
	}
	return nil
}

func systemdActivatorIdentity(plan UpgradeActivatorPlan) (bool, bool, error) {
	result, err := runSystemctl(
		"--user", "--no-ask-password", "show", plan.Name,
		"--property=LoadState", "--property=FragmentPath", "--property=DropInPaths",
	)
	if err != nil || result.ExitCode != 0 {
		return false, false, errors.Join(err, commandFailure("inspect systemd upgrade activator", result))
	}
	properties := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(string(result.Output)), "\n") {
		key, value, found := strings.Cut(line, "=")
		if !found || key == "" {
			return false, false, errors.New("systemd returned malformed activator identity")
		}
		if _, duplicate := properties[key]; duplicate {
			return false, false, errors.New("systemd returned duplicate activator identity")
		}
		properties[key] = value
	}
	if len(properties) != 3 {
		return false, false, errors.New("systemd omitted activator identity properties")
	}
	if properties["LoadState"] == "not-found" && properties["FragmentPath"] == "" &&
		properties["DropInPaths"] == "" {
		return false, false, nil
	}
	matched := properties["LoadState"] == "loaded" &&
		filepath.Clean(properties["FragmentPath"]) == filepath.Clean(plan.DefinitionPath) &&
		strings.TrimSpace(properties["DropInPaths"]) == ""
	return true, matched, nil
}
