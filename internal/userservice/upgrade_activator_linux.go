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
	"syscall"
)

var linuxActivatorOwnedByCurrentUser = func(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == uint32(os.Geteuid())
}

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
	linkErr := errors.Join(err, commandFailure("link systemd upgrade activator", linked))
	reloaded, err := runSystemctl("--user", "--no-ask-password", "daemon-reload")
	if err != nil || reloaded.ExitCode != 0 {
		return errors.Join(linkErr, err, commandFailure("reload systemd upgrade activator", reloaded))
	}
	_, matched, _, err := systemdActivatorIdentity(plan)
	if err != nil {
		return errors.Join(linkErr, err)
	}
	if !matched {
		return errors.Join(linkErr, errors.New("systemd upgrade activator is loaded from an unexpected path"))
	}
	return nil
}

func platformLaunchUpgradeActivator(ctx context.Context, plan UpgradeActivatorPlan) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, matched, _, err := systemdActivatorIdentity(plan)
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
	reloaded, runErr := runSystemctl("--user", "--no-ask-password", "daemon-reload")
	if runErr != nil || reloaded.ExitCode != 0 {
		return errors.Join(runErr, commandFailure("reload systemd upgrade activator before removal", reloaded))
	}
	present, matched, linkPath, err := systemdActivatorIdentity(plan)
	if err != nil {
		return err
	}
	if present && !matched {
		return errors.New("systemd upgrade activator name is occupied by another definition")
	}
	var disableErr error
	removedLinkPath := ""
	if matched {
		removedLinkPath = linkPath
		disabled, runErr := runSystemctl("--user", "--no-ask-password", "disable", plan.Name)
		disableErr = errors.Join(runErr, commandFailure("unlink systemd upgrade activator", disabled))
	}
	reloaded, runErr = runSystemctl("--user", "--no-ask-password", "daemon-reload")
	if runErr != nil || reloaded.ExitCode != 0 {
		return errors.Join(disableErr, runErr, commandFailure("reload removed systemd upgrade activator", reloaded))
	}
	if stillPresent, _, _, matchErr := systemdActivatorIdentity(plan); matchErr != nil {
		return errors.Join(disableErr, matchErr)
	} else if stillPresent {
		return errors.Join(disableErr, errors.New("systemd upgrade activator name remained occupied after removal"))
	}
	if removedLinkPath != "" {
		if stillPresent, _, inspectErr := systemdActivatorLinkIdentity(plan, removedLinkPath); inspectErr != nil {
			return errors.Join(disableErr, inspectErr)
		} else if stillPresent {
			return errors.Join(disableErr, errors.New("systemd upgrade activator link remained after removal"))
		}
	}
	return nil
}

func systemdActivatorIdentity(plan UpgradeActivatorPlan) (bool, bool, string, error) {
	result, err := runSystemctl(
		"--user", "--no-ask-password", "show", plan.Name,
		"--property=LoadState", "--property=FragmentPath", "--property=DropInPaths",
		"--property=UnitFileState",
	)
	if err != nil || result.ExitCode != 0 {
		return false, false, "", errors.Join(err, commandFailure("inspect systemd upgrade activator", result))
	}
	properties := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(string(result.Output)), "\n") {
		key, value, found := strings.Cut(line, "=")
		if !found || key == "" {
			return false, false, "", errors.New("systemd returned malformed activator identity")
		}
		if _, duplicate := properties[key]; duplicate {
			return false, false, "", errors.New("systemd returned duplicate activator identity")
		}
		properties[key] = value
	}
	if len(properties) != 4 {
		return false, false, "", errors.New("systemd omitted activator identity properties")
	}
	if properties["LoadState"] == "not-found" && properties["FragmentPath"] == "" &&
		properties["DropInPaths"] == "" && properties["UnitFileState"] == "" {
		return false, false, "", nil
	}
	linkPath := properties["FragmentPath"]
	if properties["LoadState"] != "loaded" || linkPath == "" || !filepath.IsAbs(linkPath) ||
		filepath.Clean(linkPath) != linkPath || filepath.Base(linkPath) != plan.Name ||
		strings.TrimSpace(properties["DropInPaths"]) != "" ||
		properties["UnitFileState"] != "linked" {
		return true, false, linkPath, nil
	}
	present, matched, err := systemdActivatorLinkIdentity(plan, linkPath)
	if err != nil {
		return true, false, linkPath, err
	}
	return true, present && matched, linkPath, nil
}

func systemdActivatorLinkIdentity(
	plan UpgradeActivatorPlan, linkPath string,
) (bool, bool, error) {
	linkInfo, err := os.Lstat(linkPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, false, nil
		}
		return false, false, fmt.Errorf("inspect systemd activator link: %w", err)
	}
	if linkInfo.Mode()&os.ModeSymlink == 0 || !linuxActivatorOwnedByCurrentUser(linkInfo) {
		return true, false, nil
	}
	target, err := os.Readlink(linkPath)
	if err != nil {
		return true, false, fmt.Errorf("read systemd activator link: %w", err)
	}
	if target != plan.DefinitionPath {
		return true, false, nil
	}
	definitionInfo, err := os.Lstat(plan.DefinitionPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return true, false, nil
		}
		return true, false, fmt.Errorf("inspect systemd activator definition: %w", err)
	}
	matched := definitionInfo.Mode().IsRegular() && definitionInfo.Mode().Perm() == 0o600 &&
		linuxActivatorOwnedByCurrentUser(definitionInfo)
	return true, matched, nil
}
