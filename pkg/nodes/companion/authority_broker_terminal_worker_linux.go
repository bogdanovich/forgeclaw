//go:build linux

package companion

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"syscall"
	"time"

	"github.com/creack/pty"
	"golang.org/x/sys/unix"
)

type authorityBrokerTerminalWorkerRequest struct {
	TerminalID          string   `json:"terminal_id"`
	ShellPath           string   `json:"shell_path"`
	ShellArguments      []string `json:"shell_arguments"`
	WorkingDirectory    string   `json:"working_directory"`
	Environment         []string `json:"environment"`
	UID                 uint32   `json:"uid"`
	GID                 uint32   `json:"gid"`
	SupplementaryGroups []uint32 `json:"supplementary_groups"`
	Columns             int      `json:"columns"`
	Rows                int      `json:"rows"`
	IdleSeconds         int      `json:"idle_seconds"`
	LifetimeSeconds     int      `json:"lifetime_seconds"`
	BufferBytes         int      `json:"buffer_bytes"`
}

type terminalPTYRead struct {
	data []byte
	err  error
}

func (runner *authorityBrokerProcessRunner) Terminal(
	ctx context.Context,
	prepared preparedAuthorityBrokerTerminal,
	request TerminalBrokerOpenRequest,
	terminalID string,
	controls <-chan TerminalBrokerControl,
	events chan<- TerminalBrokerEvent,
) error {
	controlRead, controlWrite, err := os.Pipe()
	if err != nil {
		return fmt.Errorf("create terminal worker control pipe: %w", err)
	}
	defer controlRead.Close()
	defer controlWrite.Close()
	command := exec.Command(runner.executable, runner.arguments...)
	if runner.environment != nil {
		command.Env = append([]string(nil), runner.environment...)
	}
	stdin, err := command.StdinPipe()
	if err != nil {
		return err
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		return err
	}
	command.Stderr = io.Discard
	command.ExtraFiles = []*os.File{controlRead}
	if err := command.Start(); err != nil {
		return fmt.Errorf("start authority broker terminal worker: %w", err)
	}
	_ = controlRead.Close()
	workerRequest := authorityBrokerTerminalWorkerRequest{
		TerminalID: terminalID,
		ShellPath:  prepared.shellPath, ShellArguments: prepared.shellArguments,
		WorkingDirectory: prepared.workingDirectory, Environment: prepared.environment,
		UID: prepared.profile.UID, GID: prepared.profile.GID,
		SupplementaryGroups: prepared.profile.SupplementaryGroups,
		Columns:             request.Columns, Rows: request.Rows,
		IdleSeconds: request.IdleSeconds, LifetimeSeconds: request.LifetimeSeconds,
		BufferBytes: request.BufferBytes,
	}
	if err := writeAuthorityBrokerFrame(stdin, authorityBrokerWorkerEnvelope{
		Version:  AuthorityBrokerProtocolVersion,
		Action:   authorityBrokerActionTerminal,
		Terminal: &workerRequest,
	}); err != nil {
		_ = stdin.Close()
		_ = command.Process.Kill()
		_ = command.Wait()
		return fmt.Errorf("write authority broker terminal worker request: %w", err)
	}
	go func() {
		for {
			select {
			case control, ok := <-controls:
				if !ok {
					_ = stdin.Close()
					return
				}
				if err := writeAuthorityBrokerFrame(stdin, control); err != nil {
					_ = stdin.Close()
					return
				}
			case <-ctx.Done():
				_ = stdin.Close()
				return
			}
		}
	}()
	workerDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = controlWrite.Close()
		case <-workerDone:
		}
	}()
	for {
		var event TerminalBrokerEvent
		if err := readAuthorityBrokerFrame(stdout, &event); err != nil {
			close(workerDone)
			_ = controlWrite.Close()
			_ = stdin.Close()
			waitErr := command.Wait()
			if ctx.Err() != nil && waitErr == nil {
				return nil
			}
			return fmt.Errorf("%w: read terminal worker event: %v", ErrTerminalOutcomeUnknown, err)
		}
		if err := event.validate(); err != nil || event.TerminalID != terminalID {
			close(workerDone)
			_ = controlWrite.Close()
			_ = stdin.Close()
			_ = command.Process.Kill()
			_ = command.Wait()
			return fmt.Errorf("%w: invalid terminal worker event", ErrTerminalOutcomeUnknown)
		}
		select {
		case events <- event:
		case <-ctx.Done():
			close(workerDone)
			_ = controlWrite.Close()
			_ = stdin.Close()
			_ = command.Wait()
			return nil
		}
		if event.Type == TerminalEventClosed || event.Type == TerminalEventUnknown {
			break
		}
	}
	close(workerDone)
	_ = controlWrite.Close()
	_ = stdin.Close()
	if err := command.Wait(); err != nil {
		return fmt.Errorf("%w: terminal worker failed: %v", ErrTerminalOutcomeUnknown, err)
	}
	return nil
}

func runAuthorityBrokerTerminalWorker(
	parent context.Context,
	request authorityBrokerTerminalWorkerRequest,
	controls io.Reader,
	events io.Writer,
	controlFD uintptr,
) error {
	if err := request.validate(); err != nil {
		return err
	}
	if err := unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0); err != nil {
		return fmt.Errorf("enable terminal worker subreaper: %w", err)
	}
	lifetimeContext, cancelLifetime := context.WithTimeout(
		parent,
		time.Duration(request.LifetimeSeconds)*time.Second,
	)
	defer cancelLifetime()
	control := os.NewFile(controlFD, "authority-broker-terminal-control")
	if control == nil {
		return errors.New("authority broker terminal control pipe is unavailable")
	}
	defer control.Close()
	disconnected := make(chan struct{}, 1)
	go func() {
		var signal [1]byte
		_, _ = control.Read(signal[:])
		disconnected <- struct{}{}
	}()
	controlFrames := make(chan TerminalBrokerControl, 1)
	controlErrors := make(chan error, 1)
	go func() {
		for {
			var frame TerminalBrokerControl
			if err := readAuthorityBrokerFrame(controls, &frame); err != nil {
				controlErrors <- err
				return
			}
			controlFrames <- frame
		}
	}()
	command := exec.Command(request.ShellPath, request.ShellArguments...)
	command.Dir = request.WorkingDirectory
	command.Env = append([]string(nil), request.Environment...)
	attributes := &syscall.SysProcAttr{Setsid: true, Setctty: true}
	if os.Geteuid() == 0 {
		attributes.Credential = &syscall.Credential{
			Uid: request.UID, Gid: request.GID,
			Groups: append([]uint32(nil), request.SupplementaryGroups...),
		}
	} else if request.UID != uint32(os.Geteuid()) ||
		request.GID != uint32(os.Getegid()) ||
		len(request.SupplementaryGroups) != 0 {
		return errors.New("unprivileged terminal fixture cannot change identity")
	}
	terminal, startErr := pty.StartWithAttrs(command, &pty.Winsize{
		Cols: uint16(request.Columns), Rows: uint16(request.Rows),
	}, attributes)
	if startErr != nil {
		return fmt.Errorf("start authority broker terminal: %w", startErr)
	}
	defer terminal.Close()
	processGroup := command.Process.Pid
	startedAt := time.Now().UnixMilli()
	if err := writeTerminalWorkerEvent(events, TerminalBrokerEvent{
		Type: TerminalEventOpened, TerminalID: request.TerminalID,
		State: "live", StartedAt: startedAt,
	}); err != nil {
		_ = unix.Kill(-processGroup, unix.SIGKILL)
		_ = command.Wait()
		_ = terminateAuthorityBrokerDescendants(processGroup)
		return err
	}
	readCapacity := max(1, request.BufferBytes/MaxTerminalFrameBytes)
	output := make(chan terminalPTYRead, readCapacity)
	overflow := make(chan struct{}, 1)
	go func() {
		for {
			buffer := make([]byte, MaxTerminalFrameBytes)
			count, readErr := terminal.Read(buffer)
			frame := terminalPTYRead{data: append([]byte(nil), buffer[:count]...), err: readErr}
			select {
			case output <- frame:
			default:
				_ = unix.Kill(-processGroup, unix.SIGKILL)
				overflow <- struct{}{}
				return
			}
			if readErr != nil {
				return
			}
		}
	}()
	waitDone := make(chan error, 1)
	go func() { waitDone <- command.Wait() }()
	idleTimer := time.NewTimer(time.Duration(request.IdleSeconds) * time.Second)
	defer idleTimer.Stop()
	resetIdle := func() {
		if !idleTimer.Stop() {
			select {
			case <-idleTimer.C:
			default:
			}
		}
		idleTimer.Reset(time.Duration(request.IdleSeconds) * time.Second)
	}
	var (
		cursor          uint64
		lastSequence    uint64
		lastKey         string
		closeReason     = TerminalCloseNatural
		waitErr         error
		cleanupErr      error
		processExited   bool
		ptyClosed       bool
		eventSinkFailed bool
	)
	controlsChannel := (<-chan TerminalBrokerControl)(controlFrames)
	controlErrorsChannel := (<-chan error)(controlErrors)
	disconnectedChannel := (<-chan struct{})(disconnected)
	idleChannel := idleTimer.C
	lifetimeChannel := lifetimeContext.Done()
	overflowChannel := (<-chan struct{})(overflow)
	shutdown := func(reason string) {
		if controlsChannel == nil {
			return
		}
		closeReason = reason
		controlsChannel = nil
		controlErrorsChannel = nil
		disconnectedChannel = nil
		idleChannel = nil
		lifetimeChannel = nil
		overflowChannel = nil
		_ = unix.Kill(-processGroup, unix.SIGKILL)
	}
	for !processExited || !ptyClosed {
		select {
		case frame := <-output:
			if len(frame.data) > 0 {
				cursor += uint64(len(frame.data))
				if eventErr := writeTerminalWorkerEvent(events, TerminalBrokerEvent{
					Type: TerminalEventOutput, TerminalID: request.TerminalID,
					Cursor: cursor, DataBase64: base64.StdEncoding.EncodeToString(frame.data),
				}); eventErr != nil {
					eventSinkFailed = true
					shutdown(TerminalCloseDisconnected)
				}
				if controlsChannel != nil {
					resetIdle()
				}
			}
			if frame.err != nil {
				ptyClosed = true
			}
		case frame := <-controlsChannel:
			data, validationErr := frame.validate()
			if validationErr != nil ||
				frame.Sequence > lastSequence+1 ||
				frame.Sequence < lastSequence ||
				(frame.Sequence == lastSequence && frame.IdempotencyKey != lastKey) {
				if eventErr := writeTerminalWorkerEvent(events, TerminalBrokerEvent{
					Type: TerminalEventDenied, TerminalID: request.TerminalID,
					State: "live", Reason: "invalid_sequence",
				}); eventErr != nil {
					eventSinkFailed = true
					shutdown(TerminalCloseDisconnected)
				}
				continue
			}
			if frame.Sequence == lastSequence {
				if ackErr := writeTerminalWorkerAck(
					events,
					request.TerminalID,
					lastSequence,
				); ackErr != nil {
					eventSinkFailed = true
					shutdown(TerminalCloseDisconnected)
				}
				continue
			}
			lastSequence = frame.Sequence
			lastKey = frame.IdempotencyKey
			var operationErr error
			switch {
			case len(data) > 0:
				_, operationErr = terminal.Write(data)
			case frame.Resize != nil:
				operationErr = pty.Setsize(terminal, &pty.Winsize{
					Cols: uint16(frame.Resize.Columns), Rows: uint16(frame.Resize.Rows),
				})
			case frame.Signal != "":
				operationErr = unix.Kill(-processGroup, terminalSignal(frame.Signal))
			case frame.Close:
				closeReason = TerminalCloseRequested
			}
			if operationErr != nil {
				shutdown(TerminalCloseDisconnected)
				continue
			}
			if ackErr := writeTerminalWorkerAck(
				events,
				request.TerminalID,
				lastSequence,
			); ackErr != nil {
				eventSinkFailed = true
				shutdown(TerminalCloseDisconnected)
				continue
			}
			resetIdle()
			if frame.Close {
				shutdown(TerminalCloseRequested)
			}
		case waitErr = <-waitDone:
			processExited = true
			cleanupErr = terminateAuthorityBrokerDescendants(processGroup)
		case <-idleChannel:
			shutdown(TerminalCloseIdleTimeout)
		case <-lifetimeChannel:
			shutdown(TerminalCloseLifetime)
		case <-disconnectedChannel:
			shutdown(TerminalCloseDisconnected)
		case <-controlErrorsChannel:
			shutdown(TerminalCloseDisconnected)
		case <-overflowChannel:
			shutdown(TerminalCloseOutputOverflow)
		}
	}
	if cleanupErr == nil {
		cleanupErr = terminateAuthorityBrokerDescendants(processGroup)
	}
	if cleanupErr != nil {
		_ = writeTerminalWorkerEvent(events, TerminalBrokerEvent{
			Type: TerminalEventUnknown, TerminalID: request.TerminalID,
			State: "unknown", Reason: closeReason, StartedAt: startedAt,
		})
		return fmt.Errorf("%w: %v", ErrTerminalOutcomeUnknown, cleanupErr)
	}
	if eventSinkFailed {
		return ErrTerminalOutcomeUnknown
	}
	exitCode, signalName, exitErr := authorityBrokerExit(waitErr)
	if exitErr != nil {
		return exitErr
	}
	return writeTerminalWorkerEvent(events, TerminalBrokerEvent{
		Type: TerminalEventClosed, TerminalID: request.TerminalID,
		State: "closed", Reason: closeReason, ExitCode: exitCode, Signal: signalName,
		StartedAt: startedAt, CompletedAt: time.Now().UnixMilli(),
		TerminationConfirmed: true,
	})
}

func (request authorityBrokerTerminalWorkerRequest) validate() error {
	if !terminalIdentifierPattern.MatchString(request.TerminalID) ||
		request.ShellPath == "" ||
		len(request.ShellArguments) == 0 ||
		request.WorkingDirectory == "" ||
		!validTerminalSize(request.Columns, request.Rows) ||
		request.IdleSeconds <= 0 ||
		request.IdleSeconds > MaxTerminalIdleSeconds ||
		request.LifetimeSeconds < request.IdleSeconds ||
		request.LifetimeSeconds > MaxTerminalLifetimeSeconds ||
		request.BufferBytes <= 0 ||
		request.BufferBytes > MaxTerminalBufferBytes {
		return errors.New("authority broker terminal worker request is invalid")
	}
	return nil
}

func writeTerminalWorkerAck(writer io.Writer, terminalID string, sequence uint64) error {
	return writeTerminalWorkerEvent(writer, TerminalBrokerEvent{
		Type: TerminalEventAck, TerminalID: terminalID,
		State: "live", AcceptedSequence: sequence,
	})
}

func writeTerminalWorkerEvent(writer io.Writer, event TerminalBrokerEvent) error {
	event.Version = AuthorityBrokerProtocolVersion
	if err := event.validate(); err != nil {
		return err
	}
	return writeAuthorityBrokerFrame(writer, event)
}

func terminalSignal(name string) unix.Signal {
	switch name {
	case "INT":
		return unix.SIGINT
	case "TERM":
		return unix.SIGTERM
	case "HUP":
		return unix.SIGHUP
	default:
		return unix.SIGKILL
	}
}
