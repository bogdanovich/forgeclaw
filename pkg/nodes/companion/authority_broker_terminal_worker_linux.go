//go:build linux

package companion

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
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

type terminalPTYWrite struct {
	control  TerminalBrokerControl
	digest   [sha256.Size]byte
	expected int
	written  int
	err      error
}

type terminalPTYBuffer struct {
	mu       sync.Mutex
	frames   []terminalPTYRead
	bytes    int
	limit    int
	closed   bool
	overflow bool
	notify   chan struct{}
}

func newTerminalPTYBuffer(limit int) *terminalPTYBuffer {
	return &terminalPTYBuffer{
		limit:  limit,
		notify: make(chan struct{}, 1),
	}
}

func (buffer *terminalPTYBuffer) push(frame terminalPTYRead) bool {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	if buffer.closed {
		return false
	}
	if len(frame.data) > buffer.limit-buffer.bytes {
		buffer.closed = true
		buffer.overflow = true
		buffer.signal()
		return false
	}
	if len(frame.data) > 0 || frame.err != nil {
		buffer.frames = append(buffer.frames, frame)
		buffer.bytes += len(frame.data)
	}
	if frame.err != nil {
		buffer.closed = true
	}
	buffer.signal()
	return true
}

func (buffer *terminalPTYBuffer) pop() (terminalPTYRead, bool, bool, bool) {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	var frame terminalPTYRead
	hasFrame := len(buffer.frames) > 0
	if hasFrame {
		frame = buffer.frames[0]
		buffer.frames[0] = terminalPTYRead{}
		buffer.frames = buffer.frames[1:]
		buffer.bytes -= len(frame.data)
	}
	done := buffer.closed && len(buffer.frames) == 0
	if len(buffer.frames) > 0 {
		buffer.signal()
	}
	return frame, hasFrame, buffer.overflow, done
}

func (buffer *terminalPTYBuffer) signal() {
	select {
	case buffer.notify <- struct{}{}:
	default:
	}
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
			_ = command.Process.Kill()
			waitErr := waitAuthorityBrokerTerminalWorker(command, stdout, true)
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
			_ = waitAuthorityBrokerTerminalWorker(command, stdout, true)
			return fmt.Errorf("%w: invalid terminal worker event", ErrTerminalOutcomeUnknown)
		}
		select {
		case events <- event:
		case <-ctx.Done():
			close(workerDone)
			_ = controlWrite.Close()
			_ = stdin.Close()
			return waitAuthorityBrokerTerminalWorker(command, stdout, true)
		}
		if event.Type == TerminalEventClosed || event.Type == TerminalEventUnknown {
			break
		}
	}
	close(workerDone)
	_ = controlWrite.Close()
	_ = stdin.Close()
	if err := waitAuthorityBrokerTerminalWorker(command, stdout, false); err != nil {
		return fmt.Errorf("%w: terminal worker failed: %v", ErrTerminalOutcomeUnknown, err)
	}
	return nil
}

func waitAuthorityBrokerTerminalWorker(
	command *exec.Cmd,
	stdout io.ReadCloser,
	drain bool,
) error {
	if drain {
		go func() {
			_, _ = io.Copy(io.Discard, stdout)
		}()
	}
	waitDone := make(chan error, 1)
	go func() { waitDone <- command.Wait() }()
	timer := time.NewTimer(authorityBrokerCleanupLimit)
	defer timer.Stop()
	select {
	case err := <-waitDone:
		return err
	case <-timer.C:
		_ = command.Process.Kill()
		select {
		case err := <-waitDone:
			return err
		case <-time.After(authorityBrokerCleanupLimit):
			return errors.New("authority broker terminal worker did not terminate")
		}
	}
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
	terminalFD := int(terminal.Fd())
	if err := unix.SetNonblock(terminalFD, true); err != nil {
		_ = unix.Kill(-command.Process.Pid, unix.SIGKILL)
		_ = command.Wait()
		return fmt.Errorf("make authority broker terminal nonblocking: %w", err)
	}
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
	output := newTerminalPTYBuffer(request.BufferBytes)
	go func() {
		for {
			frame := readTerminalPTY(terminalFD)
			if !output.push(frame) {
				_ = unix.Kill(-processGroup, unix.SIGKILL)
				return
			}
			if frame.err != nil {
				return
			}
		}
	}()
	terminalShutdown := make(chan struct{})
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
		lastDigest      [sha256.Size]byte
		closeReason     = TerminalCloseNatural
		waitErr         error
		cleanupErr      error
		processExited   bool
		ptyClosed       bool
		writeInFlight   bool
		shuttingDown    bool
		eventSinkFailed bool
		controlUnknown  bool
	)
	writeDone := make(chan terminalPTYWrite, 1)
	controlsChannel := (<-chan TerminalBrokerControl)(controlFrames)
	controlErrorsChannel := (<-chan error)(controlErrors)
	disconnectedChannel := (<-chan struct{})(disconnected)
	idleChannel := idleTimer.C
	lifetimeChannel := lifetimeContext.Done()
	shutdown := func(reason string) {
		if shuttingDown {
			return
		}
		shuttingDown = true
		closeReason = reason
		controlsChannel = nil
		controlErrorsChannel = nil
		disconnectedChannel = nil
		idleChannel = nil
		lifetimeChannel = nil
		close(terminalShutdown)
		_ = unix.Kill(-processGroup, unix.SIGKILL)
	}
	for !processExited || !ptyClosed || writeInFlight {
		select {
		case <-output.notify:
			for {
				frame, hasFrame, didOverflow, done := output.pop()
				if didOverflow {
					shutdown(TerminalCloseOutputOverflow)
				}
				if hasFrame && len(frame.data) > 0 {
					cursor += uint64(len(frame.data))
					if eventErr := writeTerminalWorkerEvent(events, TerminalBrokerEvent{
						Type: TerminalEventOutput, TerminalID: request.TerminalID,
						Cursor: cursor, DataBase64: base64.StdEncoding.EncodeToString(frame.data),
					}); eventErr != nil {
						eventSinkFailed = true
						shutdown(TerminalCloseDisconnected)
					}
					if !shuttingDown {
						resetIdle()
					}
				}
				if done {
					ptyClosed = true
				}
				if !hasFrame {
					break
				}
			}
		case frame := <-controlsChannel:
			data, validationErr := frame.validate()
			frameDigest, digestErr := terminalControlDigest(frame)
			if validationErr != nil ||
				digestErr != nil ||
				frame.Sequence > lastSequence+1 ||
				frame.Sequence < lastSequence ||
				(frame.Sequence == lastSequence &&
					(frame.IdempotencyKey != lastKey || frameDigest != lastDigest)) {
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
			if len(data) > 0 {
				writeInFlight = true
				controlsChannel = nil
				go func(
					control TerminalBrokerControl,
					digest [sha256.Size]byte,
					input []byte,
				) {
					written, writeErr := writeTerminalPTY(
						terminalFD,
						input,
						terminalShutdown,
					)
					writeDone <- terminalPTYWrite{
						control: control, digest: digest,
						expected: len(input), written: written, err: writeErr,
					}
				}(frame, frameDigest, append([]byte(nil), data...))
				continue
			}
			var operationErr error
			switch {
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
			lastSequence = frame.Sequence
			lastKey = frame.IdempotencyKey
			lastDigest = frameDigest
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
		case result := <-writeDone:
			writeInFlight = false
			if shuttingDown {
				continue
			}
			if result.err != nil || result.written != result.expected {
				controlUnknown = true
				shutdown(TerminalCloseDisconnected)
				continue
			}
			lastSequence = result.control.Sequence
			lastKey = result.control.IdempotencyKey
			lastDigest = result.digest
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
			controlsChannel = controlFrames
		case waitErr = <-waitDone:
			processExited = true
			cleanupErr = terminateAuthorityBrokerDescendants(processGroup)
			shutdown(TerminalCloseNatural)
		case <-idleChannel:
			shutdown(TerminalCloseIdleTimeout)
		case <-lifetimeChannel:
			shutdown(TerminalCloseLifetime)
		case <-disconnectedChannel:
			shutdown(TerminalCloseDisconnected)
		case <-controlErrorsChannel:
			shutdown(TerminalCloseDisconnected)
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
	if controlUnknown {
		_ = writeTerminalWorkerEvent(events, TerminalBrokerEvent{
			Type: TerminalEventUnknown, TerminalID: request.TerminalID,
			State: "unknown", Reason: "input_outcome_unknown", StartedAt: startedAt,
		})
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

func readTerminalPTY(descriptor int) terminalPTYRead {
	for {
		poll := []unix.PollFd{{
			Fd:     int32(descriptor),
			Events: unix.POLLIN | unix.POLLHUP | unix.POLLERR,
		}}
		if _, err := unix.Poll(poll, -1); err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			return terminalPTYRead{err: err}
		}
		buffer := make([]byte, MaxTerminalFrameBytes)
		count, err := unix.Read(descriptor, buffer)
		if errors.Is(err, unix.EAGAIN) {
			continue
		}
		if count == 0 && err == nil {
			err = io.EOF
		}
		return terminalPTYRead{
			data: append([]byte(nil), buffer[:max(0, count)]...),
			err:  err,
		}
	}
}

func writeTerminalPTY(
	descriptor int,
	data []byte,
	shutdown <-chan struct{},
) (int, error) {
	written := 0
	for written < len(data) {
		select {
		case <-shutdown:
			return written, context.Canceled
		default:
		}
		poll := []unix.PollFd{{
			Fd:     int32(descriptor),
			Events: unix.POLLOUT | unix.POLLHUP | unix.POLLERR,
		}}
		ready, err := unix.Poll(poll, 50)
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			return written, err
		}
		if ready == 0 {
			continue
		}
		count, err := unix.Write(descriptor, data[written:])
		written += max(0, count)
		if errors.Is(err, unix.EAGAIN) {
			continue
		}
		if err != nil {
			return written, err
		}
	}
	return written, nil
}

func terminalControlDigest(control TerminalBrokerControl) ([sha256.Size]byte, error) {
	encoded, err := json.Marshal(control)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	return sha256.Sum256(encoded), nil
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
