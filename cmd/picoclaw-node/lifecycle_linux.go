//go:build linux

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

const managedSystemdUnitMarker = "# Managed by ForgeClaw picoclaw-node lifecycle v1"

type systemdRunResult struct {
	Output   string
	ExitCode int
}

type systemdRunner func(context.Context, bool, ...string) (systemdRunResult, error)

type systemdLifecycle struct {
	system  bool
	unitDir string
	run     systemdRunner
}

type systemdUnitState struct {
	exists  bool
	managed bool
}

type systemdUnitProperties struct {
	loadState    string
	activeState  string
	fragmentPath string
	dropInPaths  string
}

func newPlatformServiceLifecycle(system bool) (serviceLifecycle, error) {
	unitDir := "/etc/systemd/system"
	if !system {
		configDir, err := os.UserConfigDir()
		if err != nil {
			return nil, fmt.Errorf("resolve user config directory: %w", err)
		}
		unitDir = filepath.Join(configDir, "systemd", "user")
	}
	return &systemdLifecycle{system: system, unitDir: unitDir, run: runSystemctl}, nil
}

func (lifecycle *systemdLifecycle) Status(
	ctx context.Context,
	request lifecycleRequest,
) (lifecycleStatus, error) {
	status := lifecycle.baseStatus(request.Instance)
	unit, err := captureSystemdUnit(status.UnitPath)
	if err != nil {
		return lifecycleStatus{}, err
	}
	if !unit.exists {
		properties, queryErr := lifecycle.queryUnitProperties(ctx, status.Service)
		if queryErr != nil {
			return lifecycleStatus{}, queryErr
		}
		if !properties.missing() {
			return lifecycleStatus{}, resolvedSystemdServiceError(status.Service, properties)
		}
		return status, nil
	}
	if !unit.managed {
		return lifecycleStatus{}, unownedSystemdUnitError(status.UnitPath)
	}
	properties, err := lifecycle.queryUnitProperties(ctx, status.Service)
	if err != nil {
		return lifecycleStatus{}, err
	}
	if !properties.missing() && !properties.resolvesTo(status.UnitPath) {
		return lifecycleStatus{}, resolvedSystemdServiceError(status.Service, properties)
	}
	status.Installed = true
	status.State = properties.activeState
	status.Active = status.State == "active"
	return status, nil
}

func (lifecycle *systemdLifecycle) baseStatus(instance string) lifecycleStatus {
	service := "picoclaw-node-" + instance + ".service"
	scope := "user"
	if lifecycle.system {
		scope = "system"
	}
	return lifecycleStatus{
		Instance: instance,
		Manager:  "systemd",
		Scope:    scope,
		Service:  service,
		UnitPath: filepath.Join(lifecycle.unitDir, service),
		State:    "not-installed",
	}
}

func (lifecycle *systemdLifecycle) queryUnitProperties(
	ctx context.Context,
	service string,
) (systemdUnitProperties, error) {
	args := []string{
		"show", service,
		"--property=LoadState",
		"--property=ActiveState",
		"--property=FragmentPath",
		"--property=DropInPaths",
		"--property=NeedDaemonReload",
	}
	result, err := lifecycle.run(ctx, lifecycle.system, args...)
	if err != nil {
		return systemdUnitProperties{}, err
	}
	if result.ExitCode != 0 {
		return systemdUnitProperties{}, systemdCommandError(result, args...)
	}
	properties := make(map[string]string, 5)
	for _, line := range strings.Split(result.Output, "\n") {
		key, value, found := strings.Cut(line, "=")
		if !found || (key != "LoadState" && key != "ActiveState" &&
			key != "FragmentPath" && key != "DropInPaths" && key != "NeedDaemonReload") {
			continue
		}
		if _, duplicate := properties[key]; duplicate {
			return systemdUnitProperties{}, fmt.Errorf("systemctl show returned duplicate %s", key)
		}
		properties[key] = value
	}
	loadState, hasLoadState := properties["LoadState"]
	activeState, hasActiveState := properties["ActiveState"]
	fragmentPath, hasFragmentPath := properties["FragmentPath"]
	dropInPaths, hasDropInPaths := properties["DropInPaths"]
	needDaemonReload, hasNeedDaemonReload := properties["NeedDaemonReload"]
	if !hasLoadState || !hasActiveState || !hasFragmentPath || !hasDropInPaths || !hasNeedDaemonReload {
		return systemdUnitProperties{}, errors.New(
			"systemctl show omitted LoadState, ActiveState, FragmentPath, DropInPaths, or NeedDaemonReload",
		)
	}
	if needDaemonReload != "yes" && needDaemonReload != "no" {
		return systemdUnitProperties{}, fmt.Errorf(
			"systemctl show returned invalid NeedDaemonReload %q",
			needDaemonReload,
		)
	}
	if needDaemonReload == "yes" {
		return systemdUnitProperties{}, fmt.Errorf(
			"refusing systemd service %s while daemon reload is pending",
			service,
		)
	}
	return systemdUnitProperties{
		loadState: loadState, activeState: activeState,
		fragmentPath: fragmentPath, dropInPaths: dropInPaths,
	}, nil
}

func captureSystemdUnit(path string) (systemdUnitState, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return systemdUnitState{}, nil
	}
	if err != nil {
		return systemdUnitState{}, fmt.Errorf("inspect existing systemd unit: %w", err)
	}
	if !info.Mode().IsRegular() || info.Size() > 1024*1024 {
		return systemdUnitState{}, errors.New("existing systemd unit is not a bounded regular file")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return systemdUnitState{}, fmt.Errorf("read existing systemd unit: %w", err)
	}
	return systemdUnitState{
		exists:  true,
		managed: hasSystemdUnitMarker(data),
	}, nil
}

func hasSystemdUnitMarker(data []byte) bool {
	for _, line := range bytes.Split(data, []byte("\n")) {
		if string(line) == managedSystemdUnitMarker {
			return true
		}
	}
	return false
}

func unownedSystemdUnitError(path string) error {
	return fmt.Errorf("refusing to manage unowned systemd unit %s", path)
}

func (properties systemdUnitProperties) missing() bool {
	return properties.loadState == "not-found" && properties.activeState == "inactive" &&
		properties.fragmentPath == "" && properties.dropInPaths == ""
}

func (properties systemdUnitProperties) resolvesTo(path string) bool {
	return properties.loadState == "loaded" && properties.fragmentPath != "" &&
		filepath.Clean(properties.fragmentPath) == filepath.Clean(path) &&
		properties.dropInPaths == ""
}

func resolvedSystemdServiceError(service string, properties systemdUnitProperties) error {
	return fmt.Errorf(
		"refusing systemd service %s resolved outside its managed unit "+
			"(load state %q, active state %q, fragment %q, drop-ins %q)",
		service,
		properties.loadState,
		properties.activeState,
		properties.fragmentPath,
		properties.dropInPaths,
	)
}

func runSystemctl(ctx context.Context, system bool, args ...string) (systemdRunResult, error) {
	commandArgs := make([]string, 0, len(args)+1)
	if !system {
		commandArgs = append(commandArgs, "--user")
	}
	commandArgs = append(commandArgs, args...)
	command := exec.CommandContext(ctx, "systemctl", commandArgs...)
	output, err := command.CombinedOutput()
	result := systemdRunResult{Output: strings.TrimSpace(string(output))}
	if err == nil {
		return result, nil
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return systemdRunResult{}, fmt.Errorf("run systemctl: %w", ctxErr)
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		result.ExitCode = exitErr.ExitCode()
		return result, nil
	}
	return systemdRunResult{}, fmt.Errorf("run systemctl: %w", err)
}

func systemdCommandError(result systemdRunResult, args ...string) error {
	detail := strings.TrimSpace(result.Output)
	if detail == "" {
		detail = "no output"
	}
	return fmt.Errorf("systemctl %s failed with exit code %d: %s", strings.Join(args, " "), result.ExitCode, detail)
}
