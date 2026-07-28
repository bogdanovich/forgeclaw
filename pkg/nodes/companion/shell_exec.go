package companion

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"os/exec"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/bogdanovich/mintclaw/pkg/nodes"
)

type shellExecInput struct {
	Profile        string            `json:"profile"`
	Script         string            `json:"script"`
	CWD            string            `json:"cwd"`
	Env            map[string]string `json:"env"`
	TimeoutSeconds float64           `json:"timeout_seconds"`
}

type preparedShellExec struct {
	profileAlias   string
	profile        OwnerShellProfile
	script         string
	cwd            string
	environment    []string
	timeoutSeconds int
	outputBytes    int
}

type shellExecOutput struct {
	ExitCode   int     `json:"exit_code"`
	Signal     string  `json:"signal"`
	Stdout     string  `json:"stdout"`
	Stderr     string  `json:"stderr"`
	Truncated  bool    `json:"truncated"`
	DurationMS float64 `json:"duration_ms"`
}

type shellExecHandler struct {
	policy      OwnerShellPolicy
	concurrency map[string]chan struct{}
}

func newShellExecHandler(policy OwnerShellPolicy) *shellExecHandler {
	concurrency := make(map[string]chan struct{}, len(policy.Profiles))
	for alias, profile := range policy.Profiles {
		concurrency[alias] = make(chan struct{}, profile.Limits.ConcurrentCommands)
	}
	return &shellExecHandler{policy: policy, concurrency: concurrency}
}

func (*shellExecHandler) descriptor() nodes.CommandDescriptor {
	return nodes.CommandDescriptor{
		Name: "shell.exec.v1",
		InputSchema: json.RawMessage(
			`{"type":"object","required":["profile","script","cwd","env","timeout_seconds"],"properties":{"profile":{"type":"string","minLength":1,"maxLength":64},"script":{"type":"string","minLength":1,"maxLength":65536},"cwd":{"type":"string","minLength":1,"maxLength":64},"env":{"type":"object","maxProperties":64,"additionalProperties":{"type":"string","maxLength":8192}},"timeout_seconds":{"type":"integer","minimum":1,"maximum":3600}},"additionalProperties":false}`,
		),
		OutputSchema: json.RawMessage(
			`{"type":"object","required":["exit_code","signal","stdout","stderr","truncated","duration_ms"],"properties":{"exit_code":{"type":"integer"},"signal":{"type":"string","maxLength":32},"stdout":{"type":"string"},"stderr":{"type":"string"},"truncated":{"type":"boolean"},"duration_ms":{"type":"integer","minimum":0}},"additionalProperties":false}`,
		),
		Risk:           nodes.RiskPrivileged,
		SupportsCancel: true,
	}
}

func (handler *shellExecHandler) authorize(plan nodes.ExecutionPlan) error {
	_, err := handler.prepare(plan.Input, plan.TimeoutSeconds, plan.OutputLimitBytes)
	return err
}

func (handler *shellExecHandler) prepare(
	raw json.RawMessage,
	planTimeoutSeconds int,
	planOutputBytes int,
) (preparedShellExec, error) {
	var input shellExecInput
	if err := decodeStrictJSON(raw, &input); err != nil {
		return preparedShellExec{}, errors.New("invalid shell.exec input")
	}
	profile, exists := handler.policy.Profiles[input.Profile]
	if !handler.policy.Enabled || !exists {
		return preparedShellExec{}, errors.New("shell.exec profile is unavailable")
	}
	if input.Script == "" || !utf8.ValidString(input.Script) ||
		strings.ContainsRune(input.Script, 0) ||
		len(input.Script) > profile.Limits.CommandBytes ||
		input.TimeoutSeconds <= 0 ||
		input.TimeoutSeconds > float64(planTimeoutSeconds) ||
		input.TimeoutSeconds > float64(profile.Limits.TimeoutSeconds) ||
		math.Trunc(input.TimeoutSeconds) != input.TimeoutSeconds ||
		planOutputBytes <= 0 ||
		planOutputBytes > profile.Limits.OutputBytes ||
		input.Env == nil ||
		len(input.Env) > maxSystemExecEnvNames {
		return preparedShellExec{}, errors.New("shell.exec input exceeds profile limits")
	}
	cwd, err := profile.resolveWorkingScope(input.CWD)
	if err != nil {
		return preparedShellExec{}, err
	}
	environment, err := profile.buildOwnerShellEnvironment(input.Env)
	if err != nil {
		return preparedShellExec{}, err
	}
	return preparedShellExec{
		profileAlias:   input.Profile,
		profile:        profile,
		script:         input.Script,
		cwd:            cwd,
		environment:    environment,
		timeoutSeconds: int(input.TimeoutSeconds),
		outputBytes:    planOutputBytes,
	}, nil
}

func (profile OwnerShellProfile) buildOwnerShellEnvironment(input map[string]string) ([]string, error) {
	values := make(map[string]string, len(profile.FixedEnvironment)+len(input))
	for name, value := range profile.FixedEnvironment {
		values[name] = value
	}
	totalBytes := 0
	for name, value := range input {
		canonical, allowed := profile.environmentSet[systemExecEnvKey(name)]
		if !allowed ||
			(canonical != name && runtimeEnvironmentCaseSensitive()) ||
			strings.ContainsRune(value, 0) {
			return nil, errors.New("shell.exec environment override is not allowed")
		}
		values[canonical] = value
	}
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	slices.Sort(names)
	environment := make([]string, 0, len(names))
	for _, name := range names {
		value := values[name]
		totalBytes += len(name) + len(value) + 1
		if totalBytes > maxOwnerShellEnvBytes {
			return nil, errors.New("shell.exec environment is too large")
		}
		environment = append(environment, name+"="+value)
	}
	return environment, nil
}

func (handler *shellExecHandler) execute(
	ctx context.Context,
	invocation commandInvocation,
) (any, error) {
	prepared, err := handler.prepare(
		invocation.Input,
		invocation.TimeoutSeconds,
		invocation.OutputLimitBytes,
	)
	if err != nil {
		return nil, newCommandFailure("COMMAND_DENIED", "shell.exec input denied", err)
	}
	slot := handler.concurrency[prepared.profileAlias]
	select {
	case slot <- struct{}{}:
		defer func() { <-slot }()
	default:
		return nil, newCommandFailure(
			"CONCURRENCY_LIMIT",
			"shell.exec profile concurrency limit reached",
			errors.New("shell.exec concurrency limit reached"),
		)
	}
	execCtx, deadlineCancel := context.WithTimeout(
		ctx,
		time.Duration(prepared.timeoutSeconds)*time.Second,
	)
	defer deadlineCancel()
	if contextErr := execCtx.Err(); contextErr != nil {
		// No process has been started, so cancellation is already proven.
		return nil, shellExecContextFailure(execCtx, contextErr, true)
	}

	arguments := []string{"-c", prepared.script}
	if prepared.profile.Shell.Login {
		arguments[0] = "-lc"
	}
	output := newBoundedSystemExecOutput(prepared.outputBytes)
	command := exec.Command(prepared.profile.Shell.Path, arguments...)
	command.Dir = prepared.cwd
	command.Env = prepared.environment
	command.Stdout = output.stdoutWriter()
	command.Stderr = output.stderrWriter()
	prepareOwnedShellProcess(command)
	startedAt := time.Now()
	if startErr := command.Start(); startErr != nil {
		return nil, newCommandFailure("START_FAILED", "shell.exec failed to start", startErr)
	}
	waitErr, terminated, terminationProven := waitOwnedShellProcess(execCtx, command)
	if terminated {
		return nil, shellExecContextFailure(execCtx, waitErr, terminationProven)
	}
	exitCode, signal, err := ownedShellExit(waitErr)
	if err != nil {
		return nil, newCommandFailure("WAIT_FAILED", "shell.exec wait failed", err)
	}
	result, err := shellExecResult(
		output,
		exitCode,
		signal,
		float64(time.Since(startedAt).Milliseconds()),
		prepared.outputBytes,
	)
	if err != nil {
		return nil, newCommandFailure(
			"OUTPUT_LIMIT_TOO_SMALL",
			"shell.exec output limit is too small",
			err,
		)
	}
	return result, nil
}

func waitOwnedShellProcess(
	ctx context.Context,
	command *exec.Cmd,
) (error, bool, bool) {
	done := make(chan error, 1)
	go func() {
		done <- command.Wait()
	}()
	select {
	case err := <-done:
		return err, false, true
	case <-ctx.Done():
		select {
		case err := <-done:
			return err, false, true
		default:
		}
		_ = terminateOwnedShellProcess(command)
		waitErr := <-done
		return waitErr, true, ownedShellProcessTreeGone(command)
	}
}

func shellExecContextFailure(
	ctx context.Context,
	fallback error,
	terminationProven bool,
) error {
	cause := context.Cause(ctx)
	if errors.Is(cause, errCancellationRequested) && terminationProven {
		return newCommandFailure(
			"EXECUTION_CANCELED",
			"shell.exec canceled",
			errors.Join(errCommandCancellationConfirmed, cause),
		)
	}
	if errors.Is(cause, context.DeadlineExceeded) {
		return newCommandFailure("TIMEOUT", "shell.exec timed out", cause)
	}
	if cause == nil {
		cause = fallback
	}
	return newCommandFailure("EXECUTION_CANCELED", "shell.exec cancellation outcome is unknown", cause)
}

func shellExecResult(
	output *boundedSystemExecOutput,
	exitCode int,
	signal string,
	durationMS float64,
	limit int,
) (shellExecOutput, error) {
	output.mu.Lock()
	result := shellExecOutput{
		ExitCode:   exitCode,
		Signal:     signal,
		Stdout:     strings.ToValidUTF8(output.stdout.String(), "\uFFFD"),
		Stderr:     strings.ToValidUTF8(output.stderr.String(), "\uFFFD"),
		Truncated:  output.truncated,
		DurationMS: durationMS,
	}
	output.mu.Unlock()
	for {
		encoded, err := json.Marshal(result)
		if err != nil {
			return shellExecOutput{}, err
		}
		if len(encoded) <= limit {
			return result, nil
		}
		result.Truncated = true
		switch {
		case len(result.Stdout) == 0 && len(result.Stderr) == 0:
			return shellExecOutput{}, errors.New("output envelope exceeds limit")
		case len(result.Stdout) >= len(result.Stderr):
			result.Stdout = truncateSystemExecText(result.Stdout)
		default:
			result.Stderr = truncateSystemExecText(result.Stderr)
		}
	}
}
