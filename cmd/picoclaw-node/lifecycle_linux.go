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
	"unicode"

	"github.com/sipeed/picoclaw/pkg/fileutil"
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

type systemdUnitBackup struct {
	exists  bool
	data    []byte
	mode    os.FileMode
	managed bool
	enabled bool
	active  bool
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

func (lifecycle *systemdLifecycle) Install(
	ctx context.Context,
	request lifecycleRequest,
) (lifecycleStatus, error) {
	status := lifecycle.baseStatus(request.Instance)
	backup, err := captureSystemdUnit(status.UnitPath)
	if err != nil {
		return lifecycleStatus{}, err
	}
	if backup.exists && !backup.managed {
		return lifecycleStatus{}, unownedSystemdUnitError(status.UnitPath)
	}
	if backup.exists {
		backup.active, err = lifecycle.isActive(ctx, status.Service)
		if err != nil {
			return lifecycleStatus{}, fmt.Errorf("inspect existing service activity: %w", err)
		}
		backup.enabled, err = lifecycle.isEnabled(ctx, status.Service)
		if err != nil {
			return lifecycleStatus{}, fmt.Errorf("inspect existing service enablement: %w", err)
		}
	}
	unit, err := renderSystemdUnit(request, lifecycle.system)
	if err != nil {
		return lifecycleStatus{}, err
	}
	if err = fileutil.WriteFileAtomic(status.UnitPath, []byte(unit), 0o644); err != nil {
		return lifecycleStatus{}, fmt.Errorf("write systemd unit: %w", err)
	}
	if err = lifecycle.requireSuccess(ctx, "daemon-reload"); err != nil {
		return lifecycleStatus{}, lifecycle.rollbackInstall(status, backup, false, err)
	}
	if backup.active {
		if err = lifecycle.requireSuccess(ctx, "enable", status.Service); err != nil {
			return lifecycleStatus{}, lifecycle.rollbackInstall(status, backup, false, err)
		}
		if err = lifecycle.requireSuccess(ctx, "restart", status.Service); err != nil {
			return lifecycleStatus{}, lifecycle.rollbackInstall(status, backup, true, err)
		}
	} else if err = lifecycle.requireSuccess(ctx, "enable", "--now", status.Service); err != nil {
		return lifecycleStatus{}, lifecycle.rollbackInstall(status, backup, true, err)
	}
	current, err := lifecycle.Status(ctx, request)
	if err != nil {
		return lifecycleStatus{}, lifecycle.rollbackInstall(status, backup, true, err)
	}
	if !current.Active {
		stateErr := fmt.Errorf("systemd service %s entered state %q", status.Service, current.State)
		return lifecycleStatus{}, lifecycle.rollbackInstall(status, backup, true, stateErr)
	}
	return current, nil
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
		return status, nil
	}
	if !unit.managed {
		return lifecycleStatus{}, unownedSystemdUnitError(status.UnitPath)
	}
	status.Installed = true
	result, err := lifecycle.run(ctx, lifecycle.system, "is-active", status.Service)
	if err != nil {
		return lifecycleStatus{}, err
	}
	status.State = strings.TrimSpace(result.Output)
	if result.ExitCode == 0 {
		status.Active = status.State == "active" || status.State == "reloading" ||
			status.State == "activating"
		return status, nil
	}
	if result.ExitCode == 3 && status.State != "" {
		return status, nil
	}
	return lifecycleStatus{}, systemdCommandError(result, "is-active", status.Service)
}

func (lifecycle *systemdLifecycle) Uninstall(
	ctx context.Context,
	request lifecycleRequest,
) (lifecycleStatus, error) {
	status := lifecycle.baseStatus(request.Instance)
	unit, err := captureSystemdUnit(status.UnitPath)
	if err != nil {
		return lifecycleStatus{}, err
	}
	if !unit.exists {
		if reloadErr := lifecycle.requireSuccess(ctx, "daemon-reload"); reloadErr != nil {
			return lifecycleStatus{}, reloadErr
		}
		status.State = "not-installed"
		return status, nil
	}
	if !unit.managed {
		return lifecycleStatus{}, unownedSystemdUnitError(status.UnitPath)
	}
	if err := lifecycle.requireSuccess(ctx, "disable", "--now", status.Service); err != nil {
		return lifecycleStatus{}, err
	}
	if err := os.Remove(status.UnitPath); err != nil {
		return lifecycleStatus{}, fmt.Errorf("remove systemd unit: %w", err)
	}
	if err := lifecycle.requireSuccess(ctx, "daemon-reload"); err != nil {
		return lifecycleStatus{}, err
	}
	status.State = "removed"
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

func (lifecycle *systemdLifecycle) requireSuccess(ctx context.Context, args ...string) error {
	result, err := lifecycle.run(ctx, lifecycle.system, args...)
	if err != nil {
		return err
	}
	if result.ExitCode != 0 {
		return systemdCommandError(result, args...)
	}
	return nil
}

func (lifecycle *systemdLifecycle) isActive(ctx context.Context, service string) (bool, error) {
	result, err := lifecycle.run(ctx, lifecycle.system, "is-active", service)
	if err != nil {
		return false, err
	}
	state := strings.TrimSpace(result.Output)
	if result.ExitCode == 0 {
		return state == "active" || state == "reloading" || state == "activating", nil
	}
	if result.ExitCode == 3 && state != "" {
		return false, nil
	}
	return false, systemdCommandError(result, "is-active", service)
}

func (lifecycle *systemdLifecycle) isEnabled(ctx context.Context, service string) (bool, error) {
	result, err := lifecycle.run(ctx, lifecycle.system, "is-enabled", service)
	if err != nil {
		return false, err
	}
	if result.ExitCode == 0 {
		return true, nil
	}
	switch strings.TrimSpace(result.Output) {
	case "disabled", "static", "indirect", "masked", "masked-runtime":
		return false, nil
	default:
		return false, systemdCommandError(result, "is-enabled", service)
	}
}

func (lifecycle *systemdLifecycle) rollbackInstall(
	status lifecycleStatus,
	backup systemdUnitBackup,
	startAttempted bool,
	cause error,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), serviceCommandTimeout)
	defer cancel()
	errorsSeen := []error{cause}
	if startAttempted {
		if err := lifecycle.requireSuccess(ctx, "disable", "--now", status.Service); err != nil {
			errorsSeen = append(errorsSeen, fmt.Errorf("rollback service disable: %w", err))
		}
	}
	if err := restoreSystemdUnit(status.UnitPath, backup); err != nil {
		errorsSeen = append(errorsSeen, err)
	}
	if err := lifecycle.requireSuccess(ctx, "daemon-reload"); err != nil {
		errorsSeen = append(errorsSeen, fmt.Errorf("rollback daemon reload: %w", err))
		return errors.Join(errorsSeen...)
	}
	if backup.enabled && backup.active {
		if err := lifecycle.requireSuccess(ctx, "enable", "--now", status.Service); err != nil {
			errorsSeen = append(errorsSeen, fmt.Errorf("restore enabled active service: %w", err))
		}
	} else if backup.enabled {
		if err := lifecycle.requireSuccess(ctx, "enable", status.Service); err != nil {
			errorsSeen = append(errorsSeen, fmt.Errorf("restore enabled service: %w", err))
		}
	} else if backup.active {
		if err := lifecycle.requireSuccess(ctx, "start", status.Service); err != nil {
			errorsSeen = append(errorsSeen, fmt.Errorf("restore active service: %w", err))
		}
	}
	return errors.Join(errorsSeen...)
}

func captureSystemdUnit(path string) (systemdUnitBackup, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return systemdUnitBackup{}, nil
	}
	if err != nil {
		return systemdUnitBackup{}, fmt.Errorf("inspect existing systemd unit: %w", err)
	}
	if !info.Mode().IsRegular() || info.Size() > 1024*1024 {
		return systemdUnitBackup{}, errors.New("existing systemd unit is not a bounded regular file")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return systemdUnitBackup{}, fmt.Errorf("read existing systemd unit: %w", err)
	}
	return systemdUnitBackup{
		exists:  true,
		data:    data,
		mode:    info.Mode().Perm(),
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

func restoreSystemdUnit(path string, backup systemdUnitBackup) error {
	if backup.exists {
		if err := fileutil.WriteFileAtomic(path, backup.data, backup.mode); err != nil {
			return fmt.Errorf("restore previous systemd unit: %w", err)
		}
		return nil
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove failed systemd unit: %w", err)
	}
	return nil
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

func renderSystemdUnit(request lifecycleRequest, system bool) (string, error) {
	if !filepath.IsAbs(request.ExecutablePath) || !filepath.IsAbs(request.ConfigPath) {
		return "", errors.New("systemd executable and config paths must be absolute")
	}
	executable, err := quoteSystemdArgument(request.ExecutablePath)
	if err != nil {
		return "", fmt.Errorf("quote executable path: %w", err)
	}
	configPath, err := quoteSystemdArgument(request.ConfigPath)
	if err != nil {
		return "", fmt.Errorf("quote config path: %w", err)
	}
	target := "default.target"
	serviceUser := ""
	if system {
		if !serviceAccountPattern.MatchString(request.ServiceUser) {
			return "", errors.New("systemd system unit requires a valid service user")
		}
		target = "multi-user.target"
		serviceUser = "User=" + request.ServiceUser + "\n"
	}
	return fmt.Sprintf(`%s
[Unit]
Description=ForgeClaw node companion (%s)
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
%sExecStart=%s run --config %s
Restart=on-failure
RestartSec=5s
NoNewPrivileges=true

[Install]
WantedBy=%s
`, managedSystemdUnitMarker, request.Instance, serviceUser, executable, configPath, target), nil
}

func quoteSystemdArgument(value string) (string, error) {
	if value == "" || strings.IndexFunc(value, unicode.IsControl) >= 0 {
		return "", errors.New("argument is empty or contains control characters")
	}
	value = strings.ReplaceAll(value, "%", "%%")
	value = strings.ReplaceAll(value, "$", "$$")
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, `"`, `\"`)
	return `"` + value + `"`, nil
}
