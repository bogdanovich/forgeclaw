//go:build linux || darwin

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sys/unix"
)

type unixLifecycleLock struct {
	file *os.File
}

func acquireUnixLifecycleLock(
	ctx context.Context,
	directoryFD int,
	directoryPath string,
	name string,
	manager string,
	retryInterval time.Duration,
) (*unixLifecycleLock, error) {
	fd, err := unix.Openat(
		directoryFD,
		name,
		unix.O_CREAT|unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW,
		0o600,
	)
	if err != nil {
		return nil, fmt.Errorf("open %s install lock: %w", manager, err)
	}
	file := os.NewFile(uintptr(fd), filepath.Join(directoryPath, name))
	var stat unix.Stat_t
	if err = unix.Fstat(fd, &stat); err != nil || stat.Mode&unix.S_IFMT != unix.S_IFREG {
		_ = file.Close()
		if err != nil {
			return nil, fmt.Errorf("inspect %s install lock: %w", manager, err)
		}
		return nil, fmt.Errorf("%s install lock is not a regular file", manager)
	}
	for {
		err = unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			return &unixLifecycleLock{file: file}, nil
		}
		if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EAGAIN) {
			_ = file.Close()
			return nil, fmt.Errorf("lock %s service: %w", manager, err)
		}
		if err = waitLifecycleContext(ctx, retryInterval); err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("wait for %s install lock: %w", manager, err)
		}
	}
}

func (lock *unixLifecycleLock) Close() error {
	if lock == nil || lock.file == nil {
		return nil
	}
	err := unix.Flock(int(lock.file.Fd()), unix.LOCK_UN)
	return errors.Join(err, lock.file.Close())
}

func waitLifecycleContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
