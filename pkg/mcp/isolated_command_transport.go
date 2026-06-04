package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	sdkmcp "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/sipeed/picoclaw/pkg/isolation"
	"github.com/sipeed/picoclaw/pkg/logger"
)

var isolatedCommandTerminateDuration = 5 * time.Second

// isolatedCommandTransport mirrors the SDK command transport but routes
// process startup through pkg/isolation so Windows post-start hooks run too.
type isolatedCommandTransport struct {
	ServerName        string
	Command           *exec.Cmd
	TerminateDuration time.Duration
}

func (t *isolatedCommandTransport) Connect(ctx context.Context) (sdkmcp.Connection, error) {
	stdout, err := t.Command.StdoutPipe()
	if err != nil {
		return nil, err
	}
	stdout = io.NopCloser(stdout)
	stdin, err := t.Command.StdinPipe()
	if err != nil {
		return nil, err
	}
	stderr, err := t.Command.StderrPipe()
	if err != nil {
		return nil, err
	}
	if err := isolation.Start(t.Command); err != nil {
		return nil, err
	}
	logger.InfoCF("mcp", "MCP stdio process started",
		map[string]any{
			"server":  t.ServerName,
			"command": t.Command.Path,
			"pid":     t.Command.Process.Pid,
		})
	go logStdioProcessStderr(ctx, t.ServerName, t.Command.Path, stderr)
	waitCh := make(chan error, 1)
	go func() {
		err := t.Command.Wait()
		fields := map[string]any{
			"server":  t.ServerName,
			"command": t.Command.Path,
			"pid":     t.Command.Process.Pid,
		}
		if err != nil {
			fields["error"] = err.Error()
			logger.WarnCF("mcp", "MCP stdio process exited with error", fields)
		} else {
			logger.InfoCF("mcp", "MCP stdio process exited", fields)
		}
		waitCh <- err
	}()
	td := t.TerminateDuration
	if td <= 0 {
		td = isolatedCommandTerminateDuration
	}
	return newIsolatedIOConn(
		&isolatedPipeRWC{cmd: t.Command, stdout: stdout, stdin: stdin, waitCh: waitCh, terminateDuration: td},
	), nil
}

type isolatedPipeRWC struct {
	cmd               *exec.Cmd
	stdout            io.ReadCloser
	stdin             io.WriteCloser
	waitCh            <-chan error
	terminateDuration time.Duration
}

func (s *isolatedPipeRWC) Read(p []byte) (n int, err error) {
	return s.stdout.Read(p)
}

func (s *isolatedPipeRWC) Write(p []byte) (n int, err error) {
	return s.stdin.Write(p)
}

func (s *isolatedPipeRWC) Close() error {
	if err := s.stdin.Close(); err != nil {
		return fmt.Errorf("closing stdin: %v", err)
	}
	wait := func() (error, bool) {
		select {
		case err := <-s.waitCh:
			return err, true
		case <-time.After(s.terminateDuration):
		}
		return nil, false
	}
	if err, ok := wait(); ok {
		return err
	}
	if err := s.cmd.Process.Signal(syscall.SIGTERM); err == nil {
		if err, ok := wait(); ok {
			return err
		}
	}
	if err := s.cmd.Process.Kill(); err != nil {
		return err
	}
	if err, ok := wait(); ok {
		return err
	}
	return fmt.Errorf("unresponsive subprocess")
}

func logStdioProcessStderr(ctx context.Context, serverName, command string, stderr io.Reader) {
	scanner := bufio.NewScanner(stderr)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		logger.InfoCF("mcp", "MCP stdio stderr",
			map[string]any{
				"server":  serverName,
				"command": command,
				"line":    line,
			})
	}
	if err := scanner.Err(); err != nil && ctx.Err() == nil {
		logger.WarnCF("mcp", "MCP stdio stderr reader failed",
			map[string]any{
				"server":  serverName,
				"command": command,
				"error":   err.Error(),
			})
	}
}

type isolatedIOConn struct {
	writeMu   sync.Mutex
	rwc       io.ReadWriteCloser
	incoming  <-chan isolatedMsgOrErr
	queue     []jsonrpc.Message
	closeOnce sync.Once
	closed    chan struct{}
	closeErr  error
}

type isolatedMsgOrErr struct {
	msg json.RawMessage
	err error
}

func newIsolatedIOConn(rwc io.ReadWriteCloser) *isolatedIOConn {
	incoming := make(chan isolatedMsgOrErr)
	closed := make(chan struct{})
	go func() {
		dec := json.NewDecoder(rwc)
		for {
			var raw json.RawMessage
			err := dec.Decode(&raw)
			if err == nil {
				var tr [1]byte
				if n, readErr := dec.Buffered().Read(tr[:]); n > 0 {
					if tr[0] != '\n' && tr[0] != '\r' {
						err = fmt.Errorf("invalid trailing data at the end of stream")
					}
				} else if readErr != nil && readErr != io.EOF {
					err = readErr
				}
			}
			select {
			case incoming <- isolatedMsgOrErr{msg: raw, err: err}:
			case <-closed:
				return
			}
			if err != nil {
				return
			}
		}
	}()
	return &isolatedIOConn{rwc: rwc, incoming: incoming, closed: closed}
}

func (c *isolatedIOConn) SessionID() string { return "" }

func (c *isolatedIOConn) Read(ctx context.Context) (jsonrpc.Message, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	if len(c.queue) > 0 {
		next := c.queue[0]
		c.queue = c.queue[1:]
		return next, nil
	}
	var raw json.RawMessage
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case v := <-c.incoming:
		if v.err != nil {
			return nil, v.err
		}
		raw = v.msg
	case <-c.closed:
		return nil, io.EOF
	}
	msgs, err := readIsolatedBatch(raw)
	if err != nil {
		return nil, err
	}
	c.queue = msgs[1:]
	return msgs[0], nil
}

func readIsolatedBatch(data []byte) ([]jsonrpc.Message, error) {
	var rawBatch []json.RawMessage
	if err := json.Unmarshal(data, &rawBatch); err == nil {
		if len(rawBatch) == 0 {
			return nil, fmt.Errorf("empty batch")
		}
		msgs := make([]jsonrpc.Message, 0, len(rawBatch))
		for _, raw := range rawBatch {
			msg, err := jsonrpc.DecodeMessage(raw)
			if err != nil {
				return nil, err
			}
			msgs = append(msgs, msg)
		}
		return msgs, nil
	}
	msg, err := jsonrpc.DecodeMessage(data)
	if err != nil {
		return nil, err
	}
	return []jsonrpc.Message{msg}, nil
}

func (c *isolatedIOConn) Write(ctx context.Context, msg jsonrpc.Message) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	data, err := jsonrpc.EncodeMessage(msg)
	if err != nil {
		return fmt.Errorf("marshaling message: %v", err)
	}
	data = append(data, '\n')
	_, err = c.rwc.Write(data)
	return err
}

func (c *isolatedIOConn) Close() error {
	c.closeOnce.Do(func() {
		c.closeErr = c.rwc.Close()
		close(c.closed)
	})
	return c.closeErr
}

var (
	_ sdkmcp.Transport  = (*isolatedCommandTransport)(nil)
	_ sdkmcp.Connection = (*isolatedIOConn)(nil)
)
