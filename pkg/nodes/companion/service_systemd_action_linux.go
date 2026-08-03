//go:build linux

package companion

import (
	"bytes"
	"context"
	"errors"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/nodes"
)

const maxSystemdActionOutputBytes = 16 * 1024

func (manager *systemdServiceManager) Action(
	ctx context.Context,
	request ServiceActionRequest,
) (ServiceActionResult, error) {
	return manager.executeAction(ctx, request, func() bool { return true })
}

func (manager *systemdServiceManager) executeAction(
	ctx context.Context,
	request ServiceActionRequest,
	accept serviceActionAcceptor,
) (ServiceActionResult, error) {
	service, err := manager.resolveAction(request)
	if err != nil {
		return ServiceActionResult{}, err
	}
	if accept == nil {
		return ServiceActionResult{}, errors.New("systemd action acceptance callback is required")
	}
	if manager.now().Unix() <= 0 {
		return ServiceActionResult{}, &ServiceManagerError{Code: "clock_invalid"}
	}
	var priorActivation uint64
	if request.Action == nodes.ServiceActionRestart {
		priorActivation, err = manager.activationIdentity(ctx, service.Unit)
		if err != nil {
			return ServiceActionResult{}, &ServiceManagerError{Code: "verification_unavailable"}
		}
	}
	args := []string{
		"--system",
		"--no-pager",
		"--no-ask-password",
		"--plain",
		string(request.Action),
		service.Unit,
	}
	acceptedAt := int64(0)
	result, accepted, runErr := manager.runner.runAccepted(
		ctx,
		manager.runner.systemctl,
		args,
		maxSystemdActionOutputBytes,
		func() bool {
			acceptedAt = manager.now().Unix()
			if acceptedAt <= 0 {
				return false
			}
			if !accept() {
				return false
			}
			return true
		},
	)
	if !accepted {
		return ServiceActionResult{
			Service: request.Service,
			Action:  request.Action,
			State:   "canceled",
			Code:    "canceled_before_acceptance",
		}, nil
	}
	actionResult := ServiceActionResult{
		Service:    request.Service,
		Action:     request.Action,
		State:      "unknown",
		AcceptedAt: acceptedAt,
	}
	if runErr != nil || result.exitCode != 0 || result.truncated {
		actionResult.Code = "manager_outcome_unknown"
		// Manager handoff already occurred, so process failure is an explicit
		// uncertain outcome rather than a retryable transport error.
		//nolint:nilerr
		return actionResult, nil
	}
	status, statusErr := manager.Status(ctx, ServiceStatusRequest{
		Profile: request.Profile,
		Service: request.Service,
	})
	if statusErr != nil {
		actionResult.Code = "verification_unavailable"
		// The accepted mutation cannot be disproved after verification loss.
		//nolint:nilerr
		return actionResult, nil
	}
	actionResult.Status = &status
	if !serviceActionStatusMatches(request.Action, status) {
		actionResult.Code = "verification_failed"
		return actionResult, nil
	}
	if request.Action == nodes.ServiceActionRestart {
		activation, activationErr := manager.activationIdentity(ctx, service.Unit)
		if activationErr != nil || activation == 0 || activation <= priorActivation {
			actionResult.Code = "restart_not_proven"
			// A completed restart requires positive activation identity proof.
			//nolint:nilerr
			return actionResult, nil
		}
	}
	actionResult.State = "completed"
	return actionResult, nil
}

func (manager *systemdServiceManager) resolveAction(
	request ServiceActionRequest,
) (ServicePolicyEntry, error) {
	if !manager.enforcement.actions || !request.Action.Valid() {
		return ServicePolicyEntry{}, &ServiceManagerError{Code: "command_denied"}
	}
	if err := (nodes.Alias(request.Profile)).Validate(); err != nil {
		return ServicePolicyEntry{}, &ServiceManagerError{Code: "command_denied"}
	}
	if err := (nodes.Alias(request.Service)).Validate(); err != nil {
		return ServicePolicyEntry{}, &ServiceManagerError{Code: "command_denied"}
	}
	profile, found := manager.profiles[request.Profile]
	if !found {
		return ServicePolicyEntry{}, &ServiceManagerError{Code: "command_denied"}
	}
	service, found := profile.services[request.Service]
	if !found {
		return ServicePolicyEntry{}, &ServiceManagerError{Code: "command_denied"}
	}
	for _, allowed := range service.Actions {
		if allowed == request.Action {
			return service, nil
		}
	}
	return ServicePolicyEntry{}, &ServiceManagerError{Code: "command_denied"}
}

func (manager *systemdServiceManager) activationIdentity(
	ctx context.Context,
	unit string,
) (uint64, error) {
	result, err := manager.runner.run(
		ctx,
		manager.runner.systemctl,
		[]string{
			"--system",
			"--no-pager",
			"--no-ask-password",
			"--plain",
			"show",
			"--property=ActiveEnterTimestampMonotonic",
			"--value",
			unit,
		},
		maxSystemdStatusOutputBytes,
	)
	if err != nil || result.exitCode != 0 || result.truncated {
		return 0, errors.New("systemd activation identity is unavailable")
	}
	value := strings.TrimSpace(string(result.stdout))
	if value == "" || strings.ContainsAny(value, "\r\n\t ") {
		return 0, errors.New("systemd activation identity is malformed")
	}
	identity, parseErr := strconv.ParseUint(value, 10, 64)
	if parseErr != nil {
		return 0, errors.New("systemd activation identity is malformed")
	}
	return identity, nil
}

func (runner systemdProcessRunner) runAccepted(
	ctx context.Context,
	executable commandExecutable,
	args []string,
	outputLimit int,
	accept serviceActionAcceptor,
) (systemdProcessResult, bool, error) {
	if executable.path == "" || outputLimit <= 0 || accept == nil {
		return systemdProcessResult{}, false, errors.New("systemd action backend is unavailable")
	}
	commandArgs := append(append([]string(nil), executable.prefix...), args...)
	processContext, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	command := exec.CommandContext(processContext, executable.executionPath(), commandArgs...)
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
	if ctx.Err() != nil {
		return systemdProcessResult{}, false, ctx.Err()
	}
	if !accept() {
		return systemdProcessResult{}, false, nil
	}
	if err := command.Start(); err != nil {
		return systemdProcessResult{}, true, errors.New("systemd action could not start")
	}
	err := command.Wait()
	result := systemdProcessResult{
		stdout:    bytes.Clone(stdout.buffer.Bytes()),
		truncated: stdout.truncated || stderr.truncated,
	}
	if ctx.Err() != nil {
		return result, true, ctx.Err()
	}
	if errors.Is(context.Cause(processContext), errSystemdOutputLimit) {
		return result, true, nil
	}
	if err == nil {
		return result, true, nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		result.exitCode = exitErr.ExitCode()
		return result, true, nil
	}
	return result, true, errors.New("systemd action wait failed")
}
