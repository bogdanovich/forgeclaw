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
	directory *systemdUnitDirectory
	name      string
	data      []byte
	device    uint64
	inode     uint64
}

type systemdPublisher func(*systemdUnitDirectory, string, []byte, os.FileMode) (publishedSystemdUnit, error)

type systemdUnitDirectory struct {
	file   *os.File
	path   string
	device uint64
	inode  uint64
}

type systemdUnitLock struct {
	file *os.File
}

func (lifecycle *systemdLifecycle) Install(
	ctx context.Context,
	request lifecycleRequest,
) (lifecycleStatus, error) {
	status := lifecycle.baseStatus(request.Instance)
	directory, err := openSystemdUnitDirectory(lifecycle.unitDir, lifecycle.system)
	if err != nil {
		return lifecycleStatus{}, err
	}
	defer directory.Close()
	lock, err := acquireSystemdUnitLock(ctx, directory, status.Service)
	if err != nil {
		return lifecycleStatus{}, err
	}
	defer func() { _ = lock.Close() }()

	unitState, err := captureSystemdUnitAt(directory, status.Service)
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
	publication, err := publish(directory, status.Service, []byte(unit), 0o644)
	if err != nil {
		cause := fmt.Errorf("publish systemd unit: %w", err)
		if publication.name != "" {
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
	if enableAttempted {
		if err := lifecycle.requireSuccess(ctx, "disable", "--now", status.Service); err != nil {
			return errors.Join(cause, fmt.Errorf("rollback service disable: %w", err))
		}
	} else if startAttempted {
		if err := lifecycle.requireSuccess(ctx, "stop", status.Service); err != nil {
			return errors.Join(cause, fmt.Errorf("rollback service stop: %w", err))
		}
	}
	if startAttempted {
		if err := requirePublishedSystemdUnit(publication); err != nil {
			return errors.Join(cause, fmt.Errorf("rollback service ownership after stop: %w", err))
		}
	}
	quarantined, err := quarantinePublishedSystemdUnit(publication)
	if err != nil {
		return errors.Join(cause, err)
	}
	if err := removeQuarantinedSystemdUnit(quarantined); err != nil {
		errorsSeen = append(errorsSeen, fmt.Errorf("remove failed systemd unit: %w", err))
	}
	if err := lifecycle.requireSuccess(ctx, "daemon-reload"); err != nil {
		errorsSeen = append(errorsSeen, fmt.Errorf("rollback daemon reload: %w", err))
	}
	return errors.Join(errorsSeen...)
}

func openSystemdUnitDirectory(path string, system bool) (*systemdUnitDirectory, error) {
	_, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) && !system {
		if err = os.MkdirAll(path, 0o755); err != nil {
			return nil, fmt.Errorf("create systemd user unit directory: %w", err)
		}
	}
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fmt.Errorf("open systemd unit directory: %w", err)
	}
	file := os.NewFile(uintptr(fd), path)
	var stat unix.Stat_t
	if err = unix.Fstat(fd, &stat); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("inspect systemd unit directory: %w", err)
	}
	if err = validateSystemdUnitDirectory(stat, uint32(os.Geteuid())); err != nil {
		_ = file.Close()
		return nil, err
	}
	return &systemdUnitDirectory{
		file: file, path: path, device: stat.Dev, inode: stat.Ino,
	}, nil
}

func validateSystemdUnitDirectory(stat unix.Stat_t, expectedUID uint32) error {
	if stat.Mode&unix.S_IFMT != unix.S_IFDIR {
		return fmt.Errorf("systemd unit directory has file type mode %#o, want directory", stat.Mode&unix.S_IFMT)
	}
	if stat.Uid != expectedUID {
		return fmt.Errorf("systemd unit directory is owned by uid %d, want uid %d", stat.Uid, expectedUID)
	}
	if stat.Mode&0o022 != 0 {
		return fmt.Errorf("systemd unit directory mode %#o is group/world writable", stat.Mode&0o7777)
	}
	return nil
}

func (directory *systemdUnitDirectory) Close() error {
	if directory == nil || directory.file == nil {
		return nil
	}
	return directory.file.Close()
}

func (directory *systemdUnitDirectory) fd() int {
	return int(directory.file.Fd())
}

func (directory *systemdUnitDirectory) matchesPath() (bool, error) {
	fd, err := unix.Open(
		directory.path,
		unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW,
		0,
	)
	if errors.Is(err, unix.ENOENT) || errors.Is(err, unix.ELOOP) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer unix.Close(fd)
	var stat unix.Stat_t
	if err = unix.Fstat(fd, &stat); err != nil {
		return false, err
	}
	return stat.Dev == directory.device && stat.Ino == directory.inode, nil
}

func captureSystemdUnitAt(directory *systemdUnitDirectory, name string) (systemdUnitState, error) {
	fd, err := unix.Openat(directory.fd(), name, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if errors.Is(err, unix.ENOENT) {
		return systemdUnitState{}, nil
	}
	if err != nil {
		return systemdUnitState{}, fmt.Errorf("inspect existing systemd unit: %w", err)
	}
	file := os.NewFile(uintptr(fd), filepath.Join(directory.path, name))
	defer file.Close()
	var stat unix.Stat_t
	if err = unix.Fstat(fd, &stat); err != nil {
		return systemdUnitState{}, fmt.Errorf("inspect existing systemd unit: %w", err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Size > maximumManagedUnitSize {
		return systemdUnitState{}, errors.New("existing systemd unit is not a bounded regular file")
	}
	data, err := io.ReadAll(io.LimitReader(file, maximumManagedUnitSize+1))
	if err != nil {
		return systemdUnitState{}, fmt.Errorf("read existing systemd unit: %w", err)
	}
	return systemdUnitState{exists: true, managed: hasSystemdUnitMarker(data)}, nil
}

func acquireSystemdUnitLock(
	ctx context.Context,
	directory *systemdUnitDirectory,
	service string,
) (*systemdUnitLock, error) {
	name := "." + service + ".install.lock"
	fd, err := unix.Openat(
		directory.fd(),
		name,
		unix.O_CREAT|unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW,
		0o600,
	)
	if err != nil {
		return nil, fmt.Errorf("open systemd install lock: %w", err)
	}
	file := os.NewFile(uintptr(fd), filepath.Join(directory.path, name))
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
	directory *systemdUnitDirectory,
	name string,
	data []byte,
	mode os.FileMode,
) (publication publishedSystemdUnit, retErr error) {
	id, err := newInstallTransactionID()
	if err != nil {
		return publication, err
	}
	tempName := "." + name + ".tmp-" + id
	fd, err := unix.Openat(
		directory.fd(),
		tempName,
		unix.O_CREAT|unix.O_EXCL|unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW,
		0o600,
	)
	if err != nil {
		return publication, err
	}
	temp := os.NewFile(uintptr(fd), filepath.Join(directory.path, tempName))
	defer func() {
		cleanupErr := unix.Unlinkat(directory.fd(), tempName, 0)
		if cleanupErr != nil && !errors.Is(cleanupErr, unix.ENOENT) {
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
		err = unix.Fstat(fd, &stat)
	}
	closeErr := temp.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return publication, err
	}
	if err = unix.Linkat(directory.fd(), tempName, directory.fd(), name, 0); err != nil {
		if errors.Is(err, unix.EEXIST) {
			return publication, fmt.Errorf(
				"systemd unit %s appeared during install",
				filepath.Join(directory.path, name),
			)
		}
		return publication, err
	}
	publication = publishedSystemdUnit{
		directory: directory, name: name, data: append([]byte(nil), data...),
		device: stat.Dev, inode: stat.Ino,
	}
	if err = unix.Unlinkat(directory.fd(), tempName, 0); err != nil {
		return publication, err
	}
	if err = unix.Fsync(directory.fd()); err != nil {
		return publication, err
	}
	return publication, nil
}

func publishedSystemdUnitMatches(publication publishedSystemdUnit) (bool, error) {
	directoryMatches, err := publication.directory.matchesPath()
	if err != nil || !directoryMatches {
		return false, err
	}
	fd, err := unix.Openat(
		publication.directory.fd(),
		publication.name,
		unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW,
		0,
	)
	if errors.Is(err, unix.ENOENT) || errors.Is(err, unix.ELOOP) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	file := os.NewFile(uintptr(fd), filepath.Join(publication.directory.path, publication.name))
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
	quarantineName := publication.name + ".rollback-" + id
	if err = unix.Renameat2(
		publication.directory.fd(),
		publication.name,
		publication.directory.fd(),
		quarantineName,
		unix.RENAME_NOREPLACE,
	); err != nil {
		return publishedSystemdUnit{}, fmt.Errorf("quarantine failed systemd unit: %w", err)
	}
	quarantined := publication
	quarantined.name = quarantineName
	owned, verifyErr := publishedSystemdUnitMatches(quarantined)
	if verifyErr == nil && owned {
		return quarantined, nil
	}
	restoreErr := unix.Renameat2(
		publication.directory.fd(),
		quarantineName,
		publication.directory.fd(),
		publication.name,
		unix.RENAME_NOREPLACE,
	)
	ownershipErr := errors.New("rollback refused: systemd unit is no longer the installed transaction")
	if verifyErr != nil {
		ownershipErr = fmt.Errorf("verify quarantined systemd unit: %w", verifyErr)
	}
	if restoreErr != nil {
		ownershipErr = errors.Join(
			ownershipErr,
			fmt.Errorf(
				"restore foreign systemd unit from %s: %w",
				filepath.Join(publication.directory.path, quarantineName),
				restoreErr,
			),
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
	if err = unix.Unlinkat(publication.directory.fd(), publication.name, 0); err != nil {
		return err
	}
	return unix.Fsync(publication.directory.fd())
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
