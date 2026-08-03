//go:build linux

package companion

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/nodes"
)

const (
	maxSystemdStatusOutputBytes = 16 * 1024
	maxSystemdStderrBytes       = 4 * 1024
	maxSystemdLogCaptureBytes   = 2 * nodes.MaxServiceLogBytes
)

var (
	systemctlCandidates  = []string{"/usr/bin/systemctl", "/bin/systemctl"}
	journalctlCandidates = []string{"/usr/bin/journalctl", "/bin/journalctl"}
)

type systemdServiceManager struct {
	profiles    map[string]systemdProfile
	descriptors []nodes.CommandDescriptor
	runner      systemdProcessRunner
	now         func() time.Time
	enforcement serviceEnforcement
}

type systemdProfile struct {
	services  map[string]ServicePolicyEntry
	logLimits nodes.ServiceLogLimits
}

type systemdProcessRunner struct {
	systemctl commandExecutable
	journal   commandExecutable
	env       []string
}

type commandExecutable struct {
	path     string
	prefix   []string
	file     *os.File
	identity string
}

type systemdProcessResult struct {
	stdout    []byte
	exitCode  int
	truncated bool
}

type boundedProcessBuffer struct {
	buffer    bytes.Buffer
	remaining int
	truncated bool
	onLimit   func()
	limitOnce sync.Once
}

var errSystemdOutputLimit = errors.New("systemd process output limit reached")

func NewSystemdServiceManager(policies ServicePolicies) (ServiceManager, error) {
	normalized, err := normalizeServicePolicies(policies)
	if err != nil {
		return nil, err
	}
	needStatus, needLogs := serviceReadRequirements(normalized)
	if !needStatus && !needLogs {
		return nil, errors.New("enabled service policies grant no read command")
	}
	runner := systemdProcessRunner{env: fixedSystemdEnvironment()}
	if needStatus {
		runner.systemctl.path, err = resolveTrustedSystemdExecutable(systemctlCandidates)
		if err != nil {
			return nil, fmt.Errorf("resolve systemctl: %w", err)
		}
	}
	if needLogs {
		runner.journal.path, err = resolveTrustedSystemdExecutable(journalctlCandidates)
		if err != nil {
			return nil, fmt.Errorf("resolve journalctl: %w", err)
		}
	}
	return newSystemdServiceManager(normalized, runner, time.Now)
}

func newSystemdServiceManager(
	policies ServicePolicies,
	runner systemdProcessRunner,
	now func() time.Time,
) (*systemdServiceManager, error) {
	needStatus, needLogs := serviceReadRequirements(policies)
	return newSystemdServiceManagerWithEnforcement(
		policies,
		runner,
		now,
		serviceEnforcement{status: needStatus, logs: needLogs},
	)
}

func newSystemdServiceManagerWithEnforcement(
	policies ServicePolicies,
	runner systemdProcessRunner,
	now func() time.Time,
	enforcement serviceEnforcement,
) (*systemdServiceManager, error) {
	if now == nil {
		return nil, errors.New("systemd service manager clock is required")
	}
	if enforcement.empty() {
		return nil, errors.New("systemd service manager enforcement is empty")
	}
	profiles := make(map[string]systemdProfile)
	for alias, profile := range policies {
		if !profile.Enabled {
			continue
		}
		services := make(map[string]ServicePolicyEntry)
		for serviceAlias, service := range profile.Services {
			if !(enforcement.status && service.Status) &&
				!(enforcement.logs && service.Logs) &&
				!(enforcement.actions && len(service.Actions) > 0) {
				continue
			}
			services[serviceAlias] = service
		}
		if len(services) > 0 {
			profiles[alias] = systemdProfile{
				services:  services,
				logLimits: profile.LogLimits,
			}
		}
	}
	if len(profiles) == 0 {
		return nil, errors.New("systemd service manager has no enforced profile")
	}
	needStatus, needLogs := serviceReadRequirements(policies)
	needStatus = needStatus && enforcement.status
	needLogs = needLogs && enforcement.logs
	needActions := serviceActionRequired(policies) && enforcement.actions
	if (needStatus && runner.systemctl.path == "") || (needLogs && runner.journal.path == "") {
		return nil, errors.New("systemd service manager has an incomplete process backend")
	}
	if needActions && runner.systemctl.path == "" {
		return nil, errors.New("systemd service manager has no action process backend")
	}
	descriptors, err := serviceCapabilityDescriptors(
		policies,
		serviceEnforcement{status: needStatus, logs: needLogs, actions: needActions},
		"linux",
	)
	if err != nil {
		return nil, fmt.Errorf("build systemd read descriptors: %w", err)
	}
	return &systemdServiceManager{
		profiles:    profiles,
		descriptors: descriptors,
		runner:      runner,
		now:         now,
		enforcement: serviceEnforcement{status: needStatus, logs: needLogs, actions: needActions},
	}, nil
}

func (manager *systemdServiceManager) Descriptors() []nodes.CommandDescriptor {
	if manager == nil {
		return nil
	}
	return cloneCatalog(nodes.CapabilityCatalog{Commands: manager.descriptors}).Commands
}

func serviceReadRequirements(policies ServicePolicies) (bool, bool) {
	needStatus := false
	needLogs := false
	for _, profile := range policies {
		if !profile.Enabled {
			continue
		}
		for _, service := range profile.Services {
			needStatus = needStatus || service.Status
			needLogs = needLogs || service.Logs
		}
	}
	return needStatus, needLogs
}

func serviceActionRequired(policies ServicePolicies) bool {
	for _, profile := range policies {
		if !profile.Enabled {
			continue
		}
		for _, service := range profile.Services {
			if len(service.Actions) > 0 {
				return true
			}
		}
	}
	return false
}

func (manager *systemdServiceManager) Status(
	ctx context.Context,
	request ServiceStatusRequest,
) (ServiceStatus, error) {
	service, err := manager.resolve(request.Profile, request.Service, false)
	if err != nil {
		return ServiceStatus{}, err
	}
	if !manager.enforcement.status || !service.Status || manager.runner.systemctl.path == "" {
		return ServiceStatus{}, &ServiceManagerError{Code: "command_denied"}
	}
	args := []string{
		"--system",
		"--no-pager",
		"--no-ask-password",
		"--plain",
		"show",
		"--property=LoadState",
		"--property=ActiveState",
		"--property=SubState",
		"--property=UnitFileState",
		service.Unit,
	}
	result, runErr := manager.runner.run(ctx, manager.runner.systemctl, args, maxSystemdStatusOutputBytes)
	if contextErr := ctx.Err(); contextErr != nil {
		return ServiceStatus{}, fmt.Errorf("%w: %v", errCommandCancellationConfirmed, contextErr)
	}
	observedAt := manager.now().Unix()
	if observedAt <= 0 {
		return ServiceStatus{}, &ServiceManagerError{Code: "clock_invalid"}
	}
	if runErr != nil {
		// Manager availability is part of the bounded status contract, not a
		// transport error and never includes the process failure text.
		//nolint:nilerr
		return unavailableServiceStatus(request.Service, observedAt, "manager_unavailable"), nil
	}
	if result.truncated {
		return unavailableServiceStatus(request.Service, observedAt, "output_limit"), nil
	}
	properties, parseErr := parseSystemdStatus(result.stdout)
	if parseErr != nil {
		// Malformed manager output is represented by a fixed safe status code;
		// no raw output crosses the service-manager boundary.
		//nolint:nilerr
		return unavailableServiceStatus(request.Service, observedAt, "output_invalid"), nil
	}
	status := ServiceStatus{
		Service:     request.Service,
		LoadState:   normalizeSystemdLoadState(properties["LoadState"]),
		ActiveState: normalizeSystemdActiveState(properties["ActiveState"]),
		Substate:    normalizeSystemdSubstate(properties["SubState"]),
		Enabled:     normalizeSystemdEnabledState(properties["UnitFileState"]),
		ObservedAt:  observedAt,
	}
	if status.LoadState == "not_found" {
		status.Code = "not_found"
	} else if result.exitCode != 0 {
		status.Code = "query_failed"
		status.ActiveState = "unknown"
		status.Substate = "unknown"
		status.Enabled = "unknown"
	}
	return status, nil
}

func (manager *systemdServiceManager) Logs(
	ctx context.Context,
	request ServiceLogRequest,
) (ServiceLogs, error) {
	service, err := manager.resolve(request.Profile, request.Service, true)
	if err != nil {
		return ServiceLogs{}, err
	}
	profile := manager.profiles[request.Profile]
	if !manager.enforcement.logs || !service.Logs || manager.runner.journal.path == "" {
		return ServiceLogs{}, &ServiceManagerError{Code: "command_denied"}
	}
	entries, sinceSeconds, boundErr := boundedServiceLogRequest(request, profile.logLimits)
	if boundErr != nil {
		return ServiceLogs{}, boundErr
	}
	now := manager.now()
	if now.Unix() <= 0 {
		return ServiceLogs{}, &ServiceManagerError{Code: "clock_invalid"}
	}
	since := now.Add(-time.Duration(sinceSeconds) * time.Second).Unix()
	args := []string{
		"--no-pager",
		"--quiet",
		"--output=json",
		"--output-fields=__REALTIME_TIMESTAMP,PRIORITY,MESSAGE",
		"--unit=" + service.Unit,
		"--since=@" + strconv.FormatInt(since, 10),
		"--lines=" + strconv.Itoa(entries),
	}
	captureLimit := profile.logLimits.BytesMax + entries*1024
	captureLimit = max(captureLimit, 64*1024)
	captureLimit = min(captureLimit, maxSystemdLogCaptureBytes)
	result, runErr := manager.runner.run(ctx, manager.runner.journal, args, captureLimit)
	if contextErr := ctx.Err(); contextErr != nil {
		return ServiceLogs{}, fmt.Errorf("%w: %v", errCommandCancellationConfirmed, contextErr)
	}
	if runErr != nil || result.exitCode != 0 {
		return ServiceLogs{}, &ServiceManagerError{Code: "logs_unavailable"}
	}
	raw := result.stdout
	if result.truncated {
		if boundary := bytes.LastIndexByte(raw, '\n'); boundary >= 0 {
			raw = raw[:boundary]
		} else {
			raw = nil
		}
	}
	logs, parseErr := parseSystemdLogs(request.Service, raw, entries, profile.logLimits.BytesMax)
	if parseErr != nil {
		return ServiceLogs{}, parseErr
	}
	logs.Truncated = logs.Truncated || result.truncated
	if !serviceLogsFit(logs, profile.logLimits.BytesMax) {
		return ServiceLogs{}, &ServiceManagerError{Code: "output_limit_too_small"}
	}
	return logs, nil
}

func (manager *systemdServiceManager) resolve(
	profileAlias string,
	serviceAlias string,
	logs bool,
) (ServicePolicyEntry, error) {
	if err := (nodes.Alias(profileAlias)).Validate(); err != nil {
		return ServicePolicyEntry{}, &ServiceManagerError{Code: "command_denied"}
	}
	if err := (nodes.Alias(serviceAlias)).Validate(); err != nil {
		return ServicePolicyEntry{}, &ServiceManagerError{Code: "command_denied"}
	}
	profile, found := manager.profiles[profileAlias]
	if !found {
		return ServicePolicyEntry{}, &ServiceManagerError{Code: "command_denied"}
	}
	service, found := profile.services[serviceAlias]
	if !found || (logs && !service.Logs) || (!logs && !service.Status) {
		return ServicePolicyEntry{}, &ServiceManagerError{Code: "command_denied"}
	}
	return service, nil
}

func (runner *systemdProcessRunner) run(
	ctx context.Context,
	executable commandExecutable,
	args []string,
	outputLimit int,
) (systemdProcessResult, error) {
	if executable.path == "" || outputLimit <= 0 {
		return systemdProcessResult{}, errors.New("systemd process backend is unavailable")
	}
	commandArgs := append(append([]string(nil), executable.prefix...), args...)
	processCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	command := exec.CommandContext(processCtx, executable.executionPath(), commandArgs...)
	executable.attach(command)
	command.Env = append([]string(nil), runner.env...)
	stdout := &boundedProcessBuffer{
		remaining: outputLimit,
		onLimit:   func() { cancel(errSystemdOutputLimit) },
	}
	stderr := &boundedProcessBuffer{
		remaining: maxSystemdStderrBytes,
		onLimit:   func() { cancel(errSystemdOutputLimit) },
	}
	command.Stdout = stdout
	command.Stderr = stderr
	command.WaitDelay = time.Second
	err := command.Run()
	if ctx.Err() != nil {
		return systemdProcessResult{}, ctx.Err()
	}
	result := systemdProcessResult{
		stdout:    stdout.buffer.Bytes(),
		truncated: stdout.truncated || stderr.truncated,
	}
	if errors.Is(context.Cause(processCtx), errSystemdOutputLimit) {
		return result, nil
	}
	if err == nil {
		return result, nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		result.exitCode = exitErr.ExitCode()
		return result, nil
	}
	return systemdProcessResult{}, errors.New("systemd process could not start")
}

func (executable *commandExecutable) executionPath() string {
	if executable.file != nil {
		return "/proc/self/fd/3"
	}
	return executable.path
}

func (executable *commandExecutable) attach(command *exec.Cmd) {
	if executable.file != nil {
		command.ExtraFiles = []*os.File{executable.file}
	}
}

func (executable *commandExecutable) close() error {
	if executable == nil || executable.file == nil {
		return nil
	}
	err := executable.file.Close()
	executable.file = nil
	return err
}

func (runner *systemdProcessRunner) close() error {
	if runner == nil {
		return nil
	}
	result := runner.systemctl.close()
	if err := runner.journal.close(); err != nil && result == nil {
		result = err
	}
	return result
}

func (writer *boundedProcessBuffer) Write(data []byte) (int, error) {
	written := len(data)
	if writer.remaining <= 0 {
		writer.truncated = writer.truncated || len(data) > 0
		if writer.truncated && writer.onLimit != nil {
			writer.limitOnce.Do(writer.onLimit)
		}
		return written, nil
	}
	keep := min(len(data), writer.remaining)
	_, _ = writer.buffer.Write(data[:keep])
	writer.remaining -= keep
	writer.truncated = writer.truncated || keep < len(data)
	if writer.truncated && writer.onLimit != nil {
		writer.limitOnce.Do(writer.onLimit)
	}
	return written, nil
}

func fixedSystemdEnvironment() []string {
	return []string{
		"HOME=/nonexistent",
		"LANG=C",
		"LC_ALL=C",
		"PATH=/usr/sbin:/usr/bin:/sbin:/bin",
		"SYSTEMD_COLORS=0",
		"SYSTEMD_ASK_PASSWORD_AGENT=0",
		"SYSTEMD_PAGER=cat",
	}
}

func resolveTrustedSystemdExecutable(candidates []string) (string, error) {
	for _, candidate := range candidates {
		path, err := trustedSystemdExecutable(candidate)
		if err == nil {
			return path, nil
		}
	}
	return "", errors.New("trusted executable is unavailable")
}

func trustedSystemdExecutable(path string) (string, error) {
	if !strings.HasPrefix(path, "/") {
		return "", errors.New("systemd executable must be absolute")
	}
	resolved, err := filepathEvalSymlinks(path)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(resolved)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 || info.Mode().Perm()&0o022 != 0 {
		return "", errors.New("systemd executable is not a trusted regular file")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != 0 {
		return "", errors.New("systemd executable is not root-owned")
	}
	return resolved, nil
}

var filepathEvalSymlinks = func(path string) (string, error) {
	return filepath.EvalSymlinks(path)
}
