//go:build linux

package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode"

	"golang.org/x/sys/unix"
)

const (
	installTransactionMarker = "# ForgeClaw install transaction: "
	systemdRestartDelay      = 5 * time.Second
	systemdReadinessAttempts = 10
	systemdReadinessStable   = 3
	defaultReadinessInterval = 3 * time.Second
	defaultLockRetryInterval = 50 * time.Millisecond
	maximumManagedUnitSize   = 1024 * 1024
)

type publishedSystemdUnit struct {
	path   string
	data   []byte
	device uint64
	inode  uint64
}

type systemdPublisher func(string, []byte, os.FileMode) (publishedSystemdUnit, error)

type systemdUnitLock struct {
	file *os.File
}

func (lifecycle *systemdLifecycle) Install(
	ctx context.Context,
	request lifecycleRequest,
) (lifecycleStatus, error) {
	status := lifecycle.baseStatus(request.Instance)
	if err := ensureSystemdUnitDirectory(lifecycle.unitDir, lifecycle.system); err != nil {
		return lifecycleStatus{}, err
	}
	lock, err := acquireSystemdUnitLock(ctx, lifecycle.unitDir, status.Service)
	if err != nil {
		return lifecycleStatus{}, err
	}
	defer func() { _ = lock.Close() }()

	unitState, err := captureSystemdUnit(status.UnitPath)
	if err != nil {
		return lifecycleStatus{}, err
	}
	if unitState.exists {
		if !unitState.managed {
			return lifecycleStatus{}, unownedSystemdUnitError(status.UnitPath)
		}
		return lifecycleStatus{}, fmt.Errorf(
			"managed systemd unit %s already exists; install is create-only",
			status.UnitPath,
		)
	}
	if err = lifecycle.requireUnitNameAvailable(ctx, status.Service); err != nil {
		return lifecycleStatus{}, err
	}

	transactionID, err := newInstallTransactionID()
	if err != nil {
		return lifecycleStatus{}, err
	}
	unit, err := renderSystemdUnit(request, lifecycle.system, transactionID)
	if err != nil {
		return lifecycleStatus{}, err
	}
	publish := lifecycle.publish
	if publish == nil {
		publish = publishSystemdUnit
	}
	publication, err := publish(status.UnitPath, []byte(unit), 0o644)
	if err != nil {
		cause := fmt.Errorf("publish systemd unit: %w", err)
		if publication.path != "" {
			return lifecycleStatus{}, lifecycle.rollbackInstall(publication, status, false, false, cause)
		}
		return lifecycleStatus{}, cause
	}
	if err = lifecycle.requireSuccess(ctx, "daemon-reload"); err != nil {
		return lifecycleStatus{}, lifecycle.rollbackInstall(publication, status, false, false, err)
	}
	if err = lifecycle.requirePublishedUnitLoaded(ctx, publication, status); err != nil {
		return lifecycleStatus{}, lifecycle.rollbackInstall(publication, status, false, false, err)
	}
	if err = requirePublishedSystemdUnit(publication); err != nil {
		return lifecycleStatus{}, lifecycle.rollbackInstall(publication, status, false, false, err)
	}
	if err = lifecycle.requireSuccess(ctx, "start", status.Service); err != nil {
		return lifecycleStatus{}, lifecycle.rollbackInstall(publication, status, true, false, err)
	}
	current, err := lifecycle.waitForActive(ctx, request, publication)
	if err != nil {
		return lifecycleStatus{}, lifecycle.rollbackInstall(publication, status, true, false, err)
	}
	if err = requirePublishedSystemdUnit(publication); err != nil {
		return lifecycleStatus{}, lifecycle.rollbackInstall(publication, status, true, false, err)
	}
	if err = lifecycle.requireSuccess(ctx, "enable", status.Service); err != nil {
		return lifecycleStatus{}, lifecycle.rollbackInstall(publication, status, true, true, err)
	}
	return current, nil
}

func (lifecycle *systemdLifecycle) requireSuccess(ctx context.Context, args ...string) error {
	result, err := lifecycle.run(ctx, lifecycle.system, args...)
	if err != nil {
		return err
	}
	if result.ExitCode != 0 {
		return systemdCommandError(result, args...)
	}
	return nil
}

func (lifecycle *systemdLifecycle) requireUnitNameAvailable(ctx context.Context, service string) error {
	properties, err := lifecycle.queryUnitProperties(ctx, service)
	if err != nil {
		return err
	}
	if properties.missing() {
		return nil
	}
	return resolvedSystemdServiceError(service, properties)
}

func (lifecycle *systemdLifecycle) requirePublishedUnitLoaded(
	ctx context.Context,
	publication publishedSystemdUnit,
	status lifecycleStatus,
) error {
	if err := requirePublishedSystemdUnit(publication); err != nil {
		return err
	}
	properties, err := lifecycle.queryUnitProperties(ctx, status.Service)
	if err != nil {
		return err
	}
	if !properties.resolvesTo(status.UnitPath) {
		return resolvedSystemdServiceError(status.Service, properties)
	}
	return requirePublishedSystemdUnit(publication)
}

func (lifecycle *systemdLifecycle) waitForActive(
	ctx context.Context,
	request lifecycleRequest,
	publication publishedSystemdUnit,
) (lifecycleStatus, error) {
	stable := 0
	lastState := "unknown"
	for attempt := 0; attempt < systemdReadinessAttempts; attempt++ {
		if err := requirePublishedSystemdUnit(publication); err != nil {
			return lifecycleStatus{}, err
		}
		status, err := lifecycle.Status(ctx, request)
		if err != nil {
			return lifecycleStatus{}, err
		}
		lastState = status.State
		if err = requirePublishedSystemdUnit(publication); err != nil {
			return lifecycleStatus{}, err
		}
		if status.Active {
			stable++
			if stable == systemdReadinessStable {
				return status, nil
			}
		} else {
			stable = 0
			if status.State != "activating" && status.State != "reloading" {
				return lifecycleStatus{}, fmt.Errorf(
					"systemd service %s entered state %q",
					status.Service,
					status.State,
				)
			}
		}
		if attempt+1 < systemdReadinessAttempts {
			if err = waitContext(ctx, lifecycle.readinessInterval); err != nil {
				return lifecycleStatus{}, err
			}
		}
	}
	return lifecycleStatus{}, fmt.Errorf(
		"systemd service %s did not stabilize in active state; last state %q",
		lifecycle.baseStatus(request.Instance).Service,
		lastState,
	)
}

func (lifecycle *systemdLifecycle) rollbackInstall(
	publication publishedSystemdUnit,
	status lifecycleStatus,
	startAttempted bool,
	enableAttempted bool,
	cause error,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), serviceCommandTimeout)
	defer cancel()
	errorsSeen := []error{cause}
	if startAttempted {
		if err := lifecycle.requirePublishedUnitLoaded(ctx, publication, status); err != nil {
			return errors.Join(cause, fmt.Errorf("rollback service ownership: %w", err))
		}
	}
	quarantined, err := quarantinePublishedSystemdUnit(publication)
	if err != nil {
		return errors.Join(cause, err)
	}
	if enableAttempted {
		if err := lifecycle.requireSuccess(ctx, "disable", "--now", status.Service); err != nil {
			errorsSeen = append(errorsSeen, fmt.Errorf("rollback service disable: %w", err))
		}
	} else if startAttempted {
		if err := lifecycle.requireSuccess(ctx, "stop", status.Service); err != nil {
			errorsSeen = append(errorsSeen, fmt.Errorf("rollback service stop: %w", err))
		}
	}
	if err := removeQuarantinedSystemdUnit(quarantined); err != nil {
		errorsSeen = append(errorsSeen, fmt.Errorf("remove failed systemd unit: %w", err))
	}
	if err := lifecycle.requireSuccess(ctx, "daemon-reload"); err != nil {
		errorsSeen = append(errorsSeen, fmt.Errorf("rollback daemon reload: %w", err))
	}
	return errors.Join(errorsSeen...)
}

func ensureSystemdUnitDirectory(path string, system bool) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) && !system {
		if err = os.MkdirAll(path, 0o755); err != nil {
			return fmt.Errorf("create systemd user unit directory: %w", err)
		}
		info, err = os.Lstat(path)
	}
	if err != nil {
		return fmt.Errorf("inspect systemd unit directory: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("systemd unit directory is not a real directory")
	}
	return nil
}

func acquireSystemdUnitLock(
	ctx context.Context,
	unitDir string,
	service string,
) (*systemdUnitLock, error) {
	path := filepath.Join(unitDir, "."+service+".install.lock")
	fd, err := unix.Open(path, unix.O_CREAT|unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open systemd install lock: %w", err)
	}
	file := os.NewFile(uintptr(fd), path)
	var stat unix.Stat_t
	if err = unix.Fstat(fd, &stat); err != nil || stat.Mode&unix.S_IFMT != unix.S_IFREG {
		_ = file.Close()
		if err != nil {
			return nil, fmt.Errorf("inspect systemd install lock: %w", err)
		}
		return nil, errors.New("systemd install lock is not a regular file")
	}
	for {
		err = unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			return &systemdUnitLock{file: file}, nil
		}
		if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EAGAIN) {
			_ = file.Close()
			return nil, fmt.Errorf("lock systemd service: %w", err)
		}
		if err = waitContext(ctx, defaultLockRetryInterval); err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("wait for systemd install lock: %w", err)
		}
	}
}

func (lock *systemdUnitLock) Close() error {
	if lock == nil || lock.file == nil {
		return nil
	}
	err := unix.Flock(int(lock.file.Fd()), unix.LOCK_UN)
	return errors.Join(err, lock.file.Close())
}

func waitContext(ctx context.Context, delay time.Duration) error {
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

func publishSystemdUnit(
	path string,
	data []byte,
	mode os.FileMode,
) (publication publishedSystemdUnit, retErr error) {
	temp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return publication, err
	}
	tempPath := temp.Name()
	defer func() {
		if cleanupErr := os.Remove(tempPath); cleanupErr != nil && !errors.Is(cleanupErr, os.ErrNotExist) {
			retErr = errors.Join(retErr, fmt.Errorf("remove temporary unit: %w", cleanupErr))
		}
	}()
	if err = temp.Chmod(mode); err == nil {
		_, err = temp.Write(data)
	}
	if err == nil {
		err = temp.Sync()
	}
	var stat unix.Stat_t
	if err == nil {
		err = unix.Fstat(int(temp.Fd()), &stat)
	}
	closeErr := temp.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return publication, err
	}
	if err = os.Link(tempPath, path); err != nil {
		if errors.Is(err, os.ErrExist) {
			return publication, fmt.Errorf("systemd unit %s appeared during install", path)
		}
		return publication, err
	}
	publication = publishedSystemdUnit{
		path: path, data: append([]byte(nil), data...),
		device: stat.Dev, inode: stat.Ino,
	}
	if err = os.Remove(tempPath); err != nil {
		return publication, err
	}
	if err = syncDirectory(filepath.Dir(path)); err != nil {
		return publication, err
	}
	return publication, nil
}

func publishedSystemdUnitMatches(publication publishedSystemdUnit) (bool, error) {
	fd, err := unix.Open(publication.path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if errors.Is(err, unix.ENOENT) || errors.Is(err, unix.ELOOP) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	file := os.NewFile(uintptr(fd), publication.path)
	defer file.Close()
	var stat unix.Stat_t
	if err = unix.Fstat(fd, &stat); err != nil {
		return false, err
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Dev != publication.device ||
		stat.Ino != publication.inode || stat.Size != int64(len(publication.data)) {
		return false, nil
	}
	data, err := io.ReadAll(io.LimitReader(file, maximumManagedUnitSize+1))
	if err != nil {
		return false, err
	}
	return bytes.Equal(data, publication.data), nil
}

func requirePublishedSystemdUnit(publication publishedSystemdUnit) error {
	owned, err := publishedSystemdUnitMatches(publication)
	if err != nil {
		return fmt.Errorf("verify installed systemd unit: %w", err)
	}
	if !owned {
		return errors.New("systemd unit no longer matches the installed transaction")
	}
	return nil
}

func quarantinePublishedSystemdUnit(publication publishedSystemdUnit) (publishedSystemdUnit, error) {
	id, err := newInstallTransactionID()
	if err != nil {
		return publishedSystemdUnit{}, err
	}
	quarantinePath := publication.path + ".rollback-" + id
	if err = unix.Renameat2(
		unix.AT_FDCWD,
		publication.path,
		unix.AT_FDCWD,
		quarantinePath,
		unix.RENAME_NOREPLACE,
	); err != nil {
		return publishedSystemdUnit{}, fmt.Errorf("quarantine failed systemd unit: %w", err)
	}
	quarantined := publication
	quarantined.path = quarantinePath
	owned, verifyErr := publishedSystemdUnitMatches(quarantined)
	if verifyErr == nil && owned {
		return quarantined, nil
	}
	restoreErr := unix.Renameat2(
		unix.AT_FDCWD,
		quarantinePath,
		unix.AT_FDCWD,
		publication.path,
		unix.RENAME_NOREPLACE,
	)
	ownershipErr := errors.New("rollback refused: systemd unit is no longer the installed transaction")
	if verifyErr != nil {
		ownershipErr = fmt.Errorf("verify quarantined systemd unit: %w", verifyErr)
	}
	if restoreErr != nil {
		ownershipErr = errors.Join(
			ownershipErr,
			fmt.Errorf("restore foreign systemd unit from %s: %w", quarantinePath, restoreErr),
		)
	}
	return publishedSystemdUnit{}, ownershipErr
}

func removeQuarantinedSystemdUnit(publication publishedSystemdUnit) error {
	owned, err := publishedSystemdUnitMatches(publication)
	if err != nil {
		return err
	}
	if !owned {
		return errors.New("rollback refused: quarantined unit no longer matches the installed transaction")
	}
	if err = os.Remove(publication.path); err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(publication.path))
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func newInstallTransactionID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("create install transaction identity: %w", err)
	}
	return hex.EncodeToString(value[:]), nil
}

func renderSystemdUnit(request lifecycleRequest, system bool, transactionID string) (string, error) {
	if !nodeInstancePattern.MatchString(request.Instance) {
		return "", errors.New("systemd unit requires a valid service instance")
	}
	if !filepath.IsAbs(request.ExecutablePath) || !filepath.IsAbs(request.ConfigPath) {
		return "", errors.New("systemd executable and config paths must be absolute")
	}
	if len(transactionID) != 32 {
		return "", errors.New("systemd unit requires an install transaction identity")
	}
	if _, err := hex.DecodeString(transactionID); err != nil {
		return "", errors.New("systemd unit requires a valid install transaction identity")
	}
	executable, err := quoteSystemdArgument(request.ExecutablePath)
	if err != nil {
		return "", fmt.Errorf("quote executable path: %w", err)
	}
	configPath, err := quoteSystemdArgument(request.ConfigPath)
	if err != nil {
		return "", fmt.Errorf("quote config path: %w", err)
	}
	target := "default.target"
	serviceUser := ""
	if system {
		if !serviceAccountPattern.MatchString(request.ServiceUser) {
			return "", errors.New("systemd system unit requires a valid service user")
		}
		target = "multi-user.target"
		serviceUser = "User=" + request.ServiceUser + "\n"
	}
	return fmt.Sprintf(`%s
%s%s
[Unit]
Description=ForgeClaw node companion (%s)
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
%sExecStart=%s run --config %s
Restart=on-failure
RestartSec=%s
NoNewPrivileges=true

[Install]
WantedBy=%s
`, managedSystemdUnitMarker, installTransactionMarker, transactionID, request.Instance,
		serviceUser, executable, configPath, systemdRestartDelay, target), nil
}

func quoteSystemdArgument(value string) (string, error) {
	if value == "" || strings.IndexFunc(value, unicode.IsControl) >= 0 {
		return "", errors.New("argument is empty or contains control characters")
	}
	value = strings.ReplaceAll(value, "%", "%%")
	value = strings.ReplaceAll(value, "$", "$$")
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, `"`, `\"`)
	return `"` + value + `"`, nil
}
