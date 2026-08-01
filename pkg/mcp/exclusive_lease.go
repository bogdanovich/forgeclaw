package mcp

import (
	"errors"
	"fmt"
	"os"
	"sync"
)

var errExclusiveLeaseBusy = errors.New("exclusive lease busy")

// ExclusiveLeaseBusyError classifies a configured MCP server lease that is
// already held by another cooperating process.
type ExclusiveLeaseBusyError struct {
	Server string
}

func (e *ExclusiveLeaseBusyError) Error() string {
	if e != nil && e.Server != "" {
		return fmt.Sprintf("MCP server %s exclusive lease is busy", e.Server)
	}
	return "MCP server exclusive lease is busy"
}

func (e *ExclusiveLeaseBusyError) Unwrap() error {
	return errExclusiveLeaseBusy
}

type exclusiveServerLease struct {
	file *os.File
	once sync.Once
}

func acquireExclusiveServerLease(serverName, path string) (*exclusiveServerLease, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open MCP server exclusive lease: %w", withoutPath(err))
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("secure MCP server exclusive lease: %w", withoutPath(err))
	}
	if err := tryAcquireExclusiveFileLock(file); err != nil {
		_ = file.Close()
		if errors.Is(err, errExclusiveLeaseBusy) {
			return nil, &ExclusiveLeaseBusyError{Server: serverName}
		}
		return nil, fmt.Errorf("lock MCP server exclusive lease: %w", err)
	}
	return &exclusiveServerLease{file: file}, nil
}

func withoutPath(err error) error {
	var pathErr *os.PathError
	if errors.As(err, &pathErr) {
		return pathErr.Err
	}
	return err
}

func (l *exclusiveServerLease) release() {
	if l == nil {
		return
	}
	l.once.Do(func() {
		_ = releaseExclusiveFileLock(l.file)
		_ = l.file.Close()
	})
}
