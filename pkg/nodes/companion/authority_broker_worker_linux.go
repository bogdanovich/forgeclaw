//go:build linux

package companion

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const (
	authorityBrokerControlFD    = 3
	authorityBrokerCleanupLimit = 10 * time.Second
)

type authorityBrokerWorkerRequest struct {
	ShellPath           string   `json:"shell_path"`
	ShellArguments      []string `json:"shell_arguments"`
	WorkingDirectory    string   `json:"working_directory"`
	Environment         []string `json:"environment"`
	UID                 uint32   `json:"uid"`
	GID                 uint32   `json:"gid"`
	SupplementaryGroups []uint32 `json:"supplementary_groups"`
	TimeoutSeconds      int      `json:"timeout_seconds"`
	OutputBytesMax      int      `json:"output_bytes_max"`
}

type authorityBrokerWorkerResponse struct {
	OK       bool              `json:"ok"`
	Canceled bool              `json:"canceled,omitempty"`
	Result   ShellBrokerResult `json:"result"`
}

type authorityBrokerProcessRunner struct {
	executable  string
	arguments   []string
	environment []string
}

func newAuthorityBrokerProcessRunner(executable string) (*authorityBrokerProcessRunner, error) {
	executable, err := resolveAuthorityBrokerPath("", executable, true)
	if err != nil {
		return nil, fmt.Errorf("resolve authority broker executable: %w", err)
	}
	return &authorityBrokerProcessRunner{
		executable: executable,
		arguments:  []string{AuthorityBrokerWorkerArgument},
	}, nil
}

func (runner *authorityBrokerProcessRunner) Execute(
	ctx context.Context,
	prepared preparedAuthorityBrokerExecution,
	request ShellBrokerRequest,
) (ShellBrokerResult, error) {
	controlRead, controlWrite, err := os.Pipe()
	if err != nil {
		return ShellBrokerResult{}, fmt.Errorf("create authority broker control pipe: %w", err)
	}
	defer controlRead.Close()
	defer controlWrite.Close()
	command := exec.Command(runner.executable, runner.arguments...)
	if runner.environment != nil {
		command.Env = append([]string(nil), runner.environment...)
	}
	stdin, err := command.StdinPipe()
	if err != nil {
		return ShellBrokerResult{}, err
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		return ShellBrokerResult{}, err
	}
	command.Stderr = io.Discard
	command.ExtraFiles = []*os.File{controlRead}
	if err := command.Start(); err != nil {
		return ShellBrokerResult{}, fmt.Errorf("start authority broker worker: %w", err)
	}
	_ = controlRead.Close()
	workerRequest := authorityBrokerWorkerRequest{
		ShellPath: prepared.shellPath, ShellArguments: prepared.shellArguments,
		WorkingDirectory: prepared.workingDirectory, Environment: prepared.environment,
		UID: prepared.profile.UID, GID: prepared.profile.GID,
		SupplementaryGroups: prepared.profile.SupplementaryGroups,
		TimeoutSeconds:      request.TimeoutSeconds, OutputBytesMax: request.OutputBytesMax,
	}
	if err := writeAuthorityBrokerFrame(stdin, workerRequest); err != nil {
		_ = stdin.Close()
		_ = command.Process.Kill()
		_ = command.Wait()
		return ShellBrokerResult{}, fmt.Errorf("write authority broker worker request: %w", err)
	}
	if err := stdin.Close(); err != nil {
		_ = command.Process.Kill()
		_ = command.Wait()
		return ShellBrokerResult{}, err
	}
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = controlWrite.Close()
		case <-done:
		}
	}()
	var response authorityBrokerWorkerResponse
	readErr := readAuthorityBrokerFrame(stdout, &response)
	close(done)
	_ = controlWrite.Close()
	waitErr := command.Wait()
	if readErr != nil {
		return ShellBrokerResult{}, fmt.Errorf("read authority broker worker response: %w", readErr)
	}
	if waitErr != nil {
		return ShellBrokerResult{}, fmt.Errorf("authority broker worker failed: %w", waitErr)
	}
	if response.Canceled {
		return ShellBrokerResult{}, ErrShellBrokerCancellationConfirmed
	}
	if !response.OK {
		return ShellBrokerResult{}, errors.New("authority broker worker execution failed")
	}
	return response.Result, nil
}

func RunAuthorityBrokerWorker(ctx context.Context, requireRoot bool) error {
	if requireRoot && os.Geteuid() != 0 {
		return errors.New("authority broker worker must run as root")
	}
	var request authorityBrokerWorkerRequest
	if err := readAuthorityBrokerFrame(os.Stdin, &request); err != nil {
		return fmt.Errorf("read authority broker worker request: %w", err)
	}
	response, err := executeAuthorityBrokerWorker(ctx, request, authorityBrokerControlFD)
	if writeErr := writeAuthorityBrokerFrame(os.Stdout, response); writeErr != nil {
		return writeErr
	}
	return err
}

func executeAuthorityBrokerWorker(
	parent context.Context,
	request authorityBrokerWorkerRequest,
	controlFD uintptr,
) (authorityBrokerWorkerResponse, error) {
	if request.ShellPath == "" ||
		len(request.ShellArguments) == 0 ||
		request.WorkingDirectory == "" ||
		request.TimeoutSeconds <= 0 ||
		request.TimeoutSeconds > 3600 ||
		request.OutputBytesMax <= 0 ||
		request.OutputBytesMax > 128*1024 {
		return authorityBrokerWorkerResponse{}, errors.New("authority broker worker request is invalid")
	}
	if err := unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0); err != nil {
		return authorityBrokerWorkerResponse{}, fmt.Errorf("enable authority broker subreaper: %w", err)
	}
	executeContext, cancel := context.WithTimeout(
		parent,
		time.Duration(request.TimeoutSeconds)*time.Second,
	)
	defer cancel()
	control := os.NewFile(controlFD, "authority-broker-control")
	if control == nil {
		return authorityBrokerWorkerResponse{}, errors.New("authority broker control pipe is unavailable")
	}
	defer control.Close()
	go func() {
		var signal [1]byte
		_, _ = control.Read(signal[:])
		cancel()
	}()
	output := newAuthorityBrokerCapture(request.OutputBytesMax)
	command := exec.Command(request.ShellPath, request.ShellArguments...)
	command.Dir = request.WorkingDirectory
	command.Env = append([]string(nil), request.Environment...)
	command.Stdout = output
	command.Stderr = output.stderrWriter()
	// A descendant may retain inherited capture descriptors after the shell
	// exits. Bound Cmd.Wait independently, then terminate the owned subreaper
	// domain before accepting completion.
	command.WaitDelay = 250 * time.Millisecond
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if os.Geteuid() == 0 {
		command.SysProcAttr.Credential = &syscall.Credential{
			Uid:         request.UID,
			Gid:         request.GID,
			Groups:      append([]uint32(nil), request.SupplementaryGroups...),
			NoSetGroups: false,
		}
	} else if request.UID != uint32(os.Geteuid()) ||
		request.GID != uint32(os.Getegid()) ||
		len(request.SupplementaryGroups) != 0 {
		return authorityBrokerWorkerResponse{}, errors.New(
			"unprivileged authority broker fixture cannot change identity",
		)
	}
	startedAt := time.Now()
	if err := command.Start(); err != nil {
		return authorityBrokerWorkerResponse{}, fmt.Errorf("start authority broker shell: %w", err)
	}
	processGroup := command.Process.Pid
	waitDone := make(chan error, 1)
	go func() { waitDone <- command.Wait() }()
	var waitErr error
	canceled := false
	select {
	case waitErr = <-waitDone:
	case <-executeContext.Done():
		canceled = true
		_ = unix.Kill(-processGroup, unix.SIGKILL)
		waitErr = <-waitDone
	}
	if err := terminateAuthorityBrokerDescendants(processGroup); err != nil {
		return authorityBrokerWorkerResponse{}, err
	}
	completedAt := time.Now()
	if canceled {
		return authorityBrokerWorkerResponse{Canceled: true}, nil
	}
	exitCode, signal, err := authorityBrokerExit(waitErr)
	if err != nil {
		return authorityBrokerWorkerResponse{}, err
	}
	stdout, stderr, truncated := output.result()
	return authorityBrokerWorkerResponse{
		OK: true,
		Result: ShellBrokerResult{
			ExitCode: exitCode, Stdout: stdout, Stderr: stderr,
			Signal: signal, Truncated: truncated,
			StartedAt: startedAt.UnixMilli(), CompletedAt: completedAt.UnixMilli(),
		},
	}, nil
}

func terminateAuthorityBrokerDescendants(processGroup int) error {
	_ = unix.Kill(-processGroup, unix.SIGKILL)
	deadline := time.Now().Add(authorityBrokerCleanupLimit)
	for {
		var status unix.WaitStatus
		pid, err := unix.Wait4(-1, &status, unix.WNOHANG, nil)
		switch {
		case errors.Is(err, unix.ECHILD):
			return nil
		case err != nil:
			return fmt.Errorf("reap authority broker process domain: %w", err)
		case pid > 0:
			continue
		}
		children, readErr := os.ReadFile(
			"/proc/self/task/" + strconv.Itoa(os.Getpid()) + "/children",
		)
		if readErr != nil {
			return fmt.Errorf("inspect authority broker process domain: %w", readErr)
		}
		for _, field := range strings.Fields(string(children)) {
			child, parseErr := strconv.Atoi(field)
			if parseErr != nil {
				return fmt.Errorf("parse authority broker child: %w", parseErr)
			}
			_ = unix.Kill(child, unix.SIGKILL)
		}
		if time.Now().After(deadline) {
			return errors.New("authority broker process domain did not terminate")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func authorityBrokerExit(waitErr error) (int, string, error) {
	if waitErr == nil || errors.Is(waitErr, exec.ErrWaitDelay) {
		return 0, "", nil
	}
	var exitError *exec.ExitError
	if !errors.As(waitErr, &exitError) {
		return 0, "", fmt.Errorf("wait authority broker shell: %w", waitErr)
	}
	status, ok := exitError.Sys().(syscall.WaitStatus)
	if !ok {
		return exitError.ExitCode(), "", nil
	}
	signal := ""
	if status.Signaled() {
		signal = status.Signal().String()
	}
	return exitError.ExitCode(), signal, nil
}

type authorityBrokerCapture struct {
	mu        sync.Mutex
	remaining int
	stdout    []byte
	stderr    []byte
	truncated bool
	stderrOut authorityBrokerCaptureWriter
}

type authorityBrokerCaptureWriter struct {
	capture *authorityBrokerCapture
	stderr  bool
}

func newAuthorityBrokerCapture(limit int) *authorityBrokerCapture {
	capture := &authorityBrokerCapture{remaining: limit}
	capture.stderrOut = authorityBrokerCaptureWriter{capture: capture, stderr: true}
	return capture
}

func (capture *authorityBrokerCapture) Write(data []byte) (int, error) {
	return capture.write(data, false)
}

func (capture *authorityBrokerCapture) stderrWriter() io.Writer {
	return &capture.stderrOut
}

func (writer *authorityBrokerCaptureWriter) Write(data []byte) (int, error) {
	return writer.capture.write(data, writer.stderr)
}

func (capture *authorityBrokerCapture) write(data []byte, stderr bool) (int, error) {
	capture.mu.Lock()
	defer capture.mu.Unlock()
	accepted := min(len(data), capture.remaining)
	if stderr {
		capture.stderr = append(capture.stderr, data[:accepted]...)
	} else {
		capture.stdout = append(capture.stdout, data[:accepted]...)
	}
	capture.remaining -= accepted
	if accepted < len(data) {
		capture.truncated = true
	}
	return len(data), nil
}

func (capture *authorityBrokerCapture) result() (string, string, bool) {
	capture.mu.Lock()
	defer capture.mu.Unlock()
	return string(capture.stdout), string(capture.stderr), capture.truncated
}
