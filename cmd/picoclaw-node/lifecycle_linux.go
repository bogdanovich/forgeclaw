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
	"time"
	"unicode"

	"github.com/sipeed/picoclaw/pkg/fileutil"
)

const managedSystemdUnitMarker = "# Managed by ForgeClaw picoclaw-node lifecycle v1"

const (
	systemdReadinessAttempts    = 8
	systemdReadinessConsecutive = 3
	defaultReadinessInterval    = 250 * time.Millisecond
)

type systemdRunResult struct {
	Output   string
	ExitCode int
}

type systemdRunner func(context.Context, bool, ...string) (systemdRunResult, error)

type systemdLifecycle struct {
	system            bool
	unitDir           string
	run               systemdRunner
	writeFile         func(string, []byte, os.FileMode) error
	readinessInterval time.Duration
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
	return &systemdLifecycle{
		system:            system,
		unitDir:           unitDir,
		run:               runSystemctl,
		writeFile:         fileutil.WriteFileAtomic,
		readinessInterval: defaultReadinessInterval,
	}, nil
}

func (lifecycle *systemdLifecycle) Install(
	ctx context.Context,
	request lifecycleRequest,
) (lifecycleStatus, error) {
	status := lifecycle.baseStatus(request.Instance)
	unitState, err := captureSystemdUnit(status.UnitPath)
	if err != nil {
		return lifecycleStatus{}, err
	}
	if unitState.exists {
		if !unitState.managed {
			return lifecycleStatus{}, unownedSystemdUnitError(status.UnitPath)
		}
		return lifecycleStatus{}, fmt.Errorf(
			"managed systemd unit %s already exists; install is create-only",
			status.UnitPath,
		)
	}
	if err = lifecycle.requireUnitNameAvailable(ctx, status.Service); err != nil {
		return lifecycleStatus{}, err
	}
	unit, err := renderSystemdUnit(request, lifecycle.system)
	if err != nil {
		return lifecycleStatus{}, err
	}
	if err = lifecycle.writeUnit(status.UnitPath, []byte(unit), 0o644); err != nil {
		writeErr := fmt.Errorf("write systemd unit: %w", err)
		if fileutil.IsCommittedWriteError(err) {
			return lifecycleStatus{}, lifecycle.rollbackCreatedUnit(status, false, writeErr)
		}
		return lifecycleStatus{}, writeErr
	}
	if err = lifecycle.requireSuccess(ctx, "daemon-reload"); err != nil {
		return lifecycleStatus{}, lifecycle.rollbackCreatedUnit(status, false, err)
	}
	if err = lifecycle.requireSuccess(ctx, "enable", "--now", status.Service); err != nil {
		return lifecycleStatus{}, lifecycle.rollbackCreatedUnit(status, true, err)
	}
	current, err := lifecycle.waitForActive(ctx, request)
	if err != nil {
		return lifecycleStatus{}, lifecycle.rollbackCreatedUnit(status, true, err)
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
	status.Active = status.State == "active" || status.State == "reloading" ||
		status.State == "activating"
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

func (lifecycle *systemdLifecycle) requireUnitNameAvailable(
	ctx context.Context,
	service string,
) error {
	properties, err := lifecycle.queryUnitProperties(ctx, service)
	if err != nil {
		return err
	}
	if properties.missing() {
		return nil
	}
	return resolvedSystemdServiceError(service, properties)
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
	}
	result, err := lifecycle.run(ctx, lifecycle.system, args...)
	if err != nil {
		return systemdUnitProperties{}, err
	}
	if result.ExitCode != 0 {
		return systemdUnitProperties{}, systemdCommandError(result, args...)
	}
	properties := make(map[string]string, 4)
	for _, line := range strings.Split(result.Output, "\n") {
		key, value, found := strings.Cut(line, "=")
		if !found || (key != "LoadState" && key != "ActiveState" &&
			key != "FragmentPath" && key != "DropInPaths") {
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
	if !hasLoadState || !hasActiveState || !hasFragmentPath || !hasDropInPaths {
		return systemdUnitProperties{}, errors.New(
			"systemctl show omitted LoadState, ActiveState, FragmentPath, or DropInPaths",
		)
	}
	return systemdUnitProperties{
		loadState: loadState, activeState: activeState,
		fragmentPath: fragmentPath, dropInPaths: dropInPaths,
	}, nil
}

func (lifecycle *systemdLifecycle) writeUnit(path string, data []byte, mode os.FileMode) error {
	if lifecycle.writeFile != nil {
		return lifecycle.writeFile(path, data, mode)
	}
	return fileutil.WriteFileAtomic(path, data, mode)
}

func (lifecycle *systemdLifecycle) waitForActive(
	ctx context.Context,
	request lifecycleRequest,
) (lifecycleStatus, error) {
	consecutive := 0
	lastState := "unknown"
	for attempt := 0; attempt < systemdReadinessAttempts; attempt++ {
		status, err := lifecycle.Status(ctx, request)
		if err != nil {
			return lifecycleStatus{}, err
		}
		lastState = status.State
		if status.State == "active" {
			consecutive++
			if consecutive == systemdReadinessConsecutive {
				return status, nil
			}
		} else {
			consecutive = 0
			if status.State != "activating" && status.State != "reloading" {
				return lifecycleStatus{}, fmt.Errorf(
					"systemd service %s entered state %q",
					status.Service,
					status.State,
				)
			}
		}
		if attempt+1 < systemdReadinessAttempts {
			err = lifecycle.waitReadinessInterval(ctx)
		}
		if err != nil {
			return lifecycleStatus{}, err
		}
	}
	return lifecycleStatus{}, fmt.Errorf(
		"systemd service %s did not stabilize in active state; last state %q",
		lifecycle.baseStatus(request.Instance).Service,
		lastState,
	)
}

func (lifecycle *systemdLifecycle) waitReadinessInterval(ctx context.Context) error {
	if lifecycle.readinessInterval <= 0 {
		return nil
	}
	timer := time.NewTimer(lifecycle.readinessInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (lifecycle *systemdLifecycle) rollbackCreatedUnit(
	status lifecycleStatus,
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
	if err := os.Remove(status.UnitPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		errorsSeen = append(errorsSeen, fmt.Errorf("remove failed systemd unit: %w", err))
	}
	if err := lifecycle.requireSuccess(ctx, "daemon-reload"); err != nil {
		errorsSeen = append(errorsSeen, fmt.Errorf("rollback daemon reload: %w", err))
	}
	return errors.Join(errorsSeen...)
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
