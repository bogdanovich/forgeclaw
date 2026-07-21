package companion

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/sipeed/picoclaw/pkg/nodes"
)

const (
	maxSystemExecArgv       = 128
	maxSystemExecArgument   = 4096
	maxSystemExecArgvBytes  = 64 * 1024
	maxSystemExecEnvValue   = 16 * 1024
	maxSystemExecEnvBytes   = 64 * 1024
	maxSystemExecWorkingDir = 4096
)

type commandFailureError struct {
	failure nodes.InvocationFailure
	cause   error
}

func (failure *commandFailureError) Error() string {
	return failure.cause.Error()
}

func (failure *commandFailureError) Unwrap() error {
	return failure.cause
}

func newCommandFailure(code, message string, cause error) error {
	return &commandFailureError{
		failure: nodes.InvocationFailure{Code: code, Message: message},
		cause:   cause,
	}
}

type systemExecInput struct {
	Argv           []string          `json:"argv"`
	CWD            string            `json:"cwd"`
	TimeoutSeconds float64           `json:"timeout_seconds"`
	Env            map[string]string `json:"env"`
}

type preparedSystemExec struct {
	executable     string
	args           []string
	cwd            string
	timeoutSeconds int
	env            []string
}

type systemExecOutput struct {
	ExitCode  int    `json:"exit_code"`
	Stdout    string `json:"stdout"`
	Stderr    string `json:"stderr"`
	Truncated bool   `json:"truncated"`
}

type systemExecHandler struct {
	policy SystemExecPolicy
}

func newSystemExecHandler(policy SystemExecPolicy) *systemExecHandler {
	return &systemExecHandler{policy: policy}
}

func (*systemExecHandler) descriptor() nodes.CommandDescriptor {
	return nodes.CommandDescriptor{
		Name: "system.exec.v1",
		InputSchema: json.RawMessage(
			`{"type":"object","required":["argv","cwd","timeout_seconds","env"],"properties":{"argv":{"type":"array","minItems":1,"maxItems":128,"items":{"type":"string","minLength":1,"maxLength":4096}},"cwd":{"type":"string","minLength":1,"maxLength":4096},"timeout_seconds":{"type":"integer","minimum":1,"maximum":3600},"env":{"type":"object","maxProperties":64,"additionalProperties":{"type":"string","maxLength":16384}}},"additionalProperties":false}`,
		),
		OutputSchema: json.RawMessage(
			`{"type":"object","required":["exit_code","stdout","stderr","truncated"],"properties":{"exit_code":{"type":"integer"},"stdout":{"type":"string"},"stderr":{"type":"string"},"truncated":{"type":"boolean"}},"additionalProperties":false}`,
		),
		Risk:           nodes.RiskWrite,
		SupportsCancel: true,
	}
}

func (handler *systemExecHandler) authorize(plan nodes.ExecutionPlan) error {
	_, err := handler.prepare(plan.Input, plan.TimeoutSeconds)
	return err
}

func (handler *systemExecHandler) execute(
	ctx context.Context,
	invocation commandInvocation,
) (any, error) {
	prepared, err := handler.prepare(invocation.Input, invocation.TimeoutSeconds)
	if err != nil {
		return nil, newCommandFailure("COMMAND_DENIED", "system.exec input denied", err)
	}
	execCtx := ctx
	deadlineCancel := func() {}
	if prepared.timeoutSeconds < invocation.TimeoutSeconds {
		execCtx, deadlineCancel = context.WithTimeout(
			ctx,
			time.Duration(prepared.timeoutSeconds)*time.Second,
		)
	}
	defer deadlineCancel()
	if contextErr := execCtx.Err(); contextErr != nil {
		return nil, systemExecContextFailure(execCtx, contextErr)
	}

	output := newBoundedSystemExecOutput(invocation.OutputLimitBytes)
	command := exec.Command(prepared.executable, prepared.args...)
	command.Dir = prepared.cwd
	command.Env = prepared.env
	command.Stdout = output.stdoutWriter()
	command.Stderr = output.stderrWriter()
	process, err := startSystemExecProcess(command)
	if err != nil {
		return nil, newCommandFailure("START_FAILED", "system.exec failed to start", err)
	}
	defer process.close()
	waitErr, terminated := waitSystemExecProcess(execCtx, process)
	if terminated {
		cause := context.Cause(execCtx)
		if errors.Is(cause, errCancellationRequested) {
			return nil, fmt.Errorf("%w: process terminated", errCommandCancellationConfirmed)
		}
		return nil, systemExecContextFailure(execCtx, waitErr)
	}

	exitCode := 0
	if waitErr != nil {
		var exitErr *exec.ExitError
		if !errors.As(waitErr, &exitErr) {
			return nil, newCommandFailure("WAIT_FAILED", "system.exec wait failed", waitErr)
		}
		exitCode = exitErr.ExitCode()
	}
	result, fitErr := output.result(exitCode, invocation.OutputLimitBytes)
	if fitErr != nil {
		return nil, newCommandFailure(
			"OUTPUT_LIMIT_TOO_SMALL",
			"system.exec output limit is too small",
			fitErr,
		)
	}
	return result, nil
}

func (handler *systemExecHandler) prepare(
	raw json.RawMessage,
	planTimeoutSeconds int,
) (preparedSystemExec, error) {
	var input systemExecInput
	if err := decodeStrictJSON(raw, &input); err != nil {
		return preparedSystemExec{}, errors.New("invalid system.exec input")
	}
	if len(input.Argv) == 0 || len(input.Argv) > maxSystemExecArgv ||
		input.TimeoutSeconds <= 0 || input.TimeoutSeconds > float64(planTimeoutSeconds) ||
		math.Trunc(input.TimeoutSeconds) != input.TimeoutSeconds ||
		len(input.CWD) == 0 || len(input.CWD) > maxSystemExecWorkingDir ||
		input.Env == nil || len(input.Env) > maxSystemExecEnvNames {
		return preparedSystemExec{}, errors.New("system.exec input exceeds policy bounds")
	}
	argvBytes := 0
	for _, argument := range input.Argv {
		if argument == "" || len(argument) > maxSystemExecArgument || strings.ContainsRune(argument, 0) {
			return preparedSystemExec{}, errors.New("system.exec argument is invalid")
		}
		argvBytes += len(argument)
		if argvBytes > maxSystemExecArgvBytes {
			return preparedSystemExec{}, errors.New("system.exec argv is too large")
		}
	}
	executable, err := handler.policy.resolveExecutable(input.Argv[0])
	if err != nil {
		return preparedSystemExec{}, err
	}
	cwd, err := handler.policy.resolveWorkingDirectory(input.CWD)
	if err != nil {
		return preparedSystemExec{}, err
	}
	environment, err := handler.policy.buildEnvironment(input.Env)
	if err != nil {
		return preparedSystemExec{}, err
	}
	return preparedSystemExec{
		executable:     executable,
		args:           append([]string(nil), input.Argv[1:]...),
		cwd:            cwd,
		timeoutSeconds: int(input.TimeoutSeconds),
		env:            environment,
	}, nil
}

func (policy SystemExecPolicy) buildEnvironment(input map[string]string) ([]string, error) {
	values := make(map[string]string, len(policy.Environment))
	for _, name := range policy.Environment {
		if value, ok := os.LookupEnv(name); ok {
			values[name] = value
		}
	}
	for name, value := range input {
		canonicalName, allowed := policy.environmentSet[systemExecEnvKey(name)]
		if !allowed || (canonicalName != name && runtimeEnvironmentCaseSensitive()) ||
			len(value) > maxSystemExecEnvValue || strings.ContainsRune(value, 0) {
			return nil, errors.New("system.exec environment override is not allowed")
		}
		values[canonicalName] = value
	}
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	slices.Sort(names)
	environment := make([]string, 0, len(names))
	totalBytes := 0
	for _, name := range names {
		value := values[name]
		if len(value) > maxSystemExecEnvValue || strings.ContainsRune(value, 0) {
			return nil, errors.New("system.exec inherited environment is invalid")
		}
		totalBytes += len(name) + len(value) + 1
		if totalBytes > maxSystemExecEnvBytes {
			return nil, errors.New("system.exec environment is too large")
		}
		environment = append(environment, name+"="+value)
	}
	if environment == nil {
		environment = []string{}
	}
	return environment, nil
}

func runtimeEnvironmentCaseSensitive() bool {
	return systemExecEnvKey("Path") != systemExecEnvKey("PATH")
}

func systemExecContextFailure(ctx context.Context, fallback error) error {
	cause := context.Cause(ctx)
	if errors.Is(cause, context.DeadlineExceeded) {
		return newCommandFailure("TIMEOUT", "system.exec timed out", cause)
	}
	if cause == nil {
		cause = fallback
	}
	return newCommandFailure("EXECUTION_CANCELED", "system.exec context canceled", cause)
}

func waitSystemExecProcess(ctx context.Context, process systemExecProcess) (error, bool) {
	done := make(chan error, 1)
	go func() {
		done <- process.wait()
	}()
	select {
	case err := <-done:
		return err, false
	case <-ctx.Done():
		select {
		case err := <-done:
			return err, false
		default:
		}
		terminateErr := process.terminate()
		waitErr := <-done
		return waitErr, terminateErr == nil && process.terminationConfirmed()
	}
}

type boundedSystemExecOutput struct {
	mu        sync.Mutex
	remaining int
	truncated bool
	stdout    bytes.Buffer
	stderr    bytes.Buffer
}

type boundedSystemExecWriter struct {
	output *boundedSystemExecOutput
	stderr bool
}

func newBoundedSystemExecOutput(limit int) *boundedSystemExecOutput {
	if limit < 0 {
		limit = 0
	}
	return &boundedSystemExecOutput{remaining: limit}
}

func (output *boundedSystemExecOutput) stdoutWriter() *boundedSystemExecWriter {
	return &boundedSystemExecWriter{output: output}
}

func (output *boundedSystemExecOutput) stderrWriter() *boundedSystemExecWriter {
	return &boundedSystemExecWriter{output: output, stderr: true}
}

func (writer *boundedSystemExecWriter) Write(data []byte) (int, error) {
	writer.output.mu.Lock()
	defer writer.output.mu.Unlock()
	accepted := min(len(data), writer.output.remaining)
	if writer.stderr {
		_, _ = writer.output.stderr.Write(data[:accepted])
	} else {
		_, _ = writer.output.stdout.Write(data[:accepted])
	}
	writer.output.remaining -= accepted
	if accepted != len(data) {
		writer.output.truncated = true
	}
	return len(data), nil
}

func (output *boundedSystemExecOutput) result(
	exitCode int,
	limit int,
) (systemExecOutput, error) {
	output.mu.Lock()
	result := systemExecOutput{
		ExitCode:  exitCode,
		Stdout:    strings.ToValidUTF8(output.stdout.String(), "\uFFFD"),
		Stderr:    strings.ToValidUTF8(output.stderr.String(), "\uFFFD"),
		Truncated: output.truncated,
	}
	output.mu.Unlock()
	for {
		encoded, err := json.Marshal(result)
		if err != nil {
			return systemExecOutput{}, err
		}
		if len(encoded) <= limit {
			return result, nil
		}
		result.Truncated = true
		switch {
		case len(result.Stdout) == 0 && len(result.Stderr) == 0:
			return systemExecOutput{}, errors.New("output envelope exceeds limit")
		case len(result.Stdout) >= len(result.Stderr):
			result.Stdout = truncateSystemExecText(result.Stdout)
		default:
			result.Stderr = truncateSystemExecText(result.Stderr)
		}
	}
}

func truncateSystemExecText(value string) string {
	if len(value) <= 1 {
		return ""
	}
	value = value[:len(value)/2]
	for len(value) > 0 && !utf8.ValidString(value) {
		value = value[:len(value)-1]
	}
	return value
}
