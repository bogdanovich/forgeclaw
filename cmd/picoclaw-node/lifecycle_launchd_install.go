//go:build linux || darwin

package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

const (
	launchdInstallTransactionMarker = "ForgeClaw install transaction: "
	launchdReadinessAttempts        = 10
	launchdReadinessStable          = 3
	launchdReadinessInterval        = 3 * time.Second
	launchdLockRetryInterval        = 50 * time.Millisecond
	maximumManagedPlistSize         = 1024 * 1024
)

type launchdPlistDirectory struct {
	file     *os.File
	path     string
	identity os.FileInfo
}

type launchdInstallLock = unixLifecycleLock

type publishedLaunchdPlist struct {
	directory *launchdPlistDirectory
	name      string
	data      []byte
	identity  os.FileInfo
}

func (lifecycle *launchdLifecycle) Install(
	ctx context.Context,
	request lifecycleRequest,
) (lifecycleStatus, error) {
	status := lifecycle.baseStatus(request.Instance)
	directory, err := openLaunchdPlistDirectory(lifecycle.plistDir, lifecycle.system)
	if err != nil {
		return lifecycleStatus{}, err
	}
	defer directory.Close()

	lock, err := acquireLaunchdInstallLock(ctx, directory, status.Service)
	if err != nil {
		return lifecycleStatus{}, err
	}
	defer func() { _ = lock.Close() }()

	existing, err := captureLaunchdPlistAt(directory, filepath.Base(status.UnitPath))
	if err != nil {
		return lifecycleStatus{}, err
	}
	if existing.exists {
		if !existing.managed {
			return lifecycleStatus{}, fmt.Errorf("refusing to manage unowned launchd plist %s", status.UnitPath)
		}
		return lifecycleStatus{}, fmt.Errorf(
			"managed launchd plist %s already exists; install is create-only",
			status.UnitPath,
		)
	}

	domain, err := lifecycle.requireInstallTargetAvailable(ctx, status.Service)
	if err != nil {
		return lifecycleStatus{}, err
	}
	transactionID, err := newLaunchdTransactionID()
	if err != nil {
		return lifecycleStatus{}, err
	}
	plist, err := renderLaunchdPlist(request, lifecycle.system, status.Service, transactionID)
	if err != nil {
		return lifecycleStatus{}, err
	}
	publication, err := publishLaunchdPlist(
		directory,
		filepath.Base(status.UnitPath),
		[]byte(plist),
		0o600,
	)
	if err != nil {
		cause := fmt.Errorf("publish launchd plist: %w", err)
		if publication.name != "" {
			return lifecycleStatus{}, lifecycle.rollbackInstall(publication, domain, status, false, cause)
		}
		return lifecycleStatus{}, cause
	}
	if err = requirePublishedLaunchdPlist(publication); err != nil {
		return lifecycleStatus{}, lifecycle.rollbackInstall(publication, domain, status, false, err)
	}
	if _, loaded, queryErr := lifecycle.queryJob(ctx, status.Service); queryErr != nil {
		return lifecycleStatus{}, lifecycle.rollbackInstall(publication, domain, status, false, queryErr)
	} else if loaded {
		return lifecycleStatus{}, lifecycle.rollbackInstall(
			publication,
			domain,
			status,
			false,
			fmt.Errorf("launchd service %s appeared during install", status.Service),
		)
	}
	if err = lifecycle.requireSuccess(ctx, "bootstrap", domain, status.UnitPath); err != nil {
		return lifecycleStatus{}, lifecycle.rollbackInstall(publication, domain, status, true, err)
	}
	current, err := lifecycle.waitForRunning(ctx, request, publication)
	if err != nil {
		return lifecycleStatus{}, lifecycle.rollbackInstall(publication, domain, status, true, err)
	}
	if err = requirePublishedLaunchdPlist(publication); err != nil {
		return lifecycleStatus{}, lifecycle.rollbackInstall(publication, domain, status, true, err)
	}
	return current, nil
}

func (lifecycle *launchdLifecycle) requireSuccess(ctx context.Context, args ...string) error {
	result, err := lifecycle.run(ctx, args...)
	if err != nil {
		return err
	}
	if result.ExitCode != 0 {
		return fmt.Errorf(
			"launchctl %s failed with exit code %d: %s",
			strings.Join(args, " "),
			result.ExitCode,
			result.Output,
		)
	}
	return nil
}

func (lifecycle *launchdLifecycle) requireInstallTargetAvailable(
	ctx context.Context,
	service string,
) (string, error) {
	availableDomain := ""
	for _, domain := range lifecycle.domains {
		target := domain + "/" + service
		result, err := lifecycle.run(ctx, "print", target)
		if err != nil {
			return "", err
		}
		if result.ExitCode == 0 {
			if _, parseErr := parseLaunchdJob(target, service, result.Output); parseErr != nil {
				return "", parseErr
			}
			return "", fmt.Errorf("launchd service %s is already loaded", service)
		}
		if launchdJobMissing(result) {
			if availableDomain == "" {
				availableDomain = domain
			}
			continue
		}
		if launchdOptionalDomainMissing(domain, result) {
			continue
		}
		return "", fmt.Errorf(
			"launchctl print %s failed with exit code %d: %s",
			target,
			result.ExitCode,
			result.Output,
		)
	}
	if availableDomain == "" {
		return "", fmt.Errorf("launchd service %s has no available installation domain", service)
	}
	return availableDomain, nil
}

func (lifecycle *launchdLifecycle) waitForRunning(
	ctx context.Context,
	request lifecycleRequest,
	publication publishedLaunchdPlist,
) (lifecycleStatus, error) {
	attempts := lifecycle.readinessAttempts
	if attempts == 0 {
		attempts = launchdReadinessAttempts
	}
	stableTarget := lifecycle.readinessStable
	if stableTarget == 0 {
		stableTarget = launchdReadinessStable
	}
	interval := lifecycle.readinessInterval
	if interval == 0 {
		interval = launchdReadinessInterval
	}
	stable := 0
	var current lifecycleStatus
	for attempt := 0; attempt < attempts; attempt++ {
		if err := requirePublishedLaunchdPlist(publication); err != nil {
			return lifecycleStatus{}, err
		}
		var err error
		current, err = lifecycle.Status(ctx, request)
		if err != nil {
			return lifecycleStatus{}, err
		}
		if current.Active && current.State == "running" {
			stable++
			if stable >= stableTarget {
				return current, nil
			}
		} else {
			stable = 0
		}
		if attempt+1 < attempts {
			if err = waitLifecycleContext(ctx, interval); err != nil {
				return lifecycleStatus{}, err
			}
		}
	}
	return lifecycleStatus{}, fmt.Errorf(
		"launchd service %s did not remain running for %d observations",
		current.Service,
		stableTarget,
	)
}

func (lifecycle *launchdLifecycle) rollbackInstall(
	publication publishedLaunchdPlist,
	domain string,
	status lifecycleStatus,
	bootstrapAttempted bool,
	cause error,
) error {
	rollbackCtx, cancel := context.WithTimeout(context.Background(), serviceCommandTimeout)
	defer cancel()
	errorsSeen := []error{cause}

	if bootstrapAttempted {
		job, loaded, err := lifecycle.queryJob(rollbackCtx, status.Service)
		if err != nil {
			return errors.Join(cause, fmt.Errorf("inspect launchd service during rollback: %w", err))
		}
		if loaded {
			if filepath.Clean(job.path) != filepath.Clean(status.UnitPath) {
				return errors.Join(
					cause,
					fmt.Errorf("rollback refused: launchd service %s resolves to %q", status.Service, job.path),
				)
			}
			if err = lifecycle.requireSuccess(
				rollbackCtx,
				"bootout",
				domain+"/"+status.Service,
			); err != nil {
				return errors.Join(cause, fmt.Errorf("bootout failed launchd service: %w", err))
			}
			if _, stillLoaded, verifyErr := lifecycle.queryJob(rollbackCtx, status.Service); verifyErr != nil {
				return errors.Join(cause, fmt.Errorf("verify launchd bootout: %w", verifyErr))
			} else if stillLoaded {
				return errors.Join(cause, errors.New("launchd service remained loaded after bootout"))
			}
		}
	}
	if err := removePublishedLaunchdPlist(publication); err != nil {
		errorsSeen = append(errorsSeen, fmt.Errorf("remove failed launchd plist: %w", err))
	}
	return errors.Join(errorsSeen...)
}

func openLaunchdPlistDirectory(path string, system bool) (*launchdPlistDirectory, error) {
	if !filepath.IsAbs(path) {
		return nil, errors.New("launchd plist directory must be absolute")
	}
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fmt.Errorf("open launchd plist directory: %w", err)
	}
	file := os.NewFile(uintptr(fd), path)
	var stat unix.Stat_t
	if err = unix.Fstat(fd, &stat); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("inspect launchd plist directory: %w", err)
	}
	expectedUID := uint32(os.Geteuid())
	if system {
		expectedUID = 0
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFDIR || stat.Uid != expectedUID || stat.Mode&0o022 != 0 {
		_ = file.Close()
		return nil, fmt.Errorf(
			"untrusted launchd plist directory %s: owner=%d mode=%#o",
			path,
			stat.Uid,
			stat.Mode&0o7777,
		)
	}
	identity, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("capture launchd plist directory identity: %w", err)
	}
	return &launchdPlistDirectory{
		file: file, path: path, identity: identity,
	}, nil
}

func (directory *launchdPlistDirectory) Close() error {
	if directory == nil || directory.file == nil {
		return nil
	}
	return directory.file.Close()
}

func (directory *launchdPlistDirectory) fd() int {
	return int(directory.file.Fd())
}

func (directory *launchdPlistDirectory) matchesPath() (bool, error) {
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
	file := os.NewFile(uintptr(fd), directory.path)
	defer file.Close()
	identity, err := file.Stat()
	if err != nil {
		return false, err
	}
	return os.SameFile(directory.identity, identity), nil
}

func acquireLaunchdInstallLock(
	ctx context.Context,
	directory *launchdPlistDirectory,
	service string,
) (*launchdInstallLock, error) {
	name := "." + service + ".install.lock"
	return acquireUnixLifecycleLock(
		ctx,
		directory.fd(),
		directory.path,
		name,
		"launchd",
		launchdLockRetryInterval,
	)
}

func captureLaunchdPlistAt(
	directory *launchdPlistDirectory,
	name string,
) (launchdPlistState, error) {
	fd, err := unix.Openat(directory.fd(), name, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if errors.Is(err, unix.ENOENT) {
		return launchdPlistState{}, nil
	}
	if err != nil {
		return launchdPlistState{}, fmt.Errorf("inspect existing launchd plist: %w", err)
	}
	file := os.NewFile(uintptr(fd), filepath.Join(directory.path, name))
	defer file.Close()
	var stat unix.Stat_t
	if err = unix.Fstat(fd, &stat); err != nil {
		return launchdPlistState{}, fmt.Errorf("inspect existing launchd plist: %w", err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Size > maximumManagedPlistSize {
		return launchdPlistState{}, errors.New("existing launchd plist is not a bounded regular file")
	}
	data, err := io.ReadAll(io.LimitReader(file, maximumManagedPlistSize+1))
	if err != nil {
		return launchdPlistState{}, fmt.Errorf("read existing launchd plist: %w", err)
	}
	state := launchdPlistState{exists: true, managed: hasLaunchdPlistMarker(data)}
	if state.managed {
		state.label, err = parseLaunchdPlistLabel(data)
	}
	return state, err
}

func publishLaunchdPlist(
	directory *launchdPlistDirectory,
	name string,
	data []byte,
	mode os.FileMode,
) (publication publishedLaunchdPlist, retErr error) {
	id, err := newLaunchdTransactionID()
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
			retErr = errors.Join(retErr, fmt.Errorf("remove temporary plist: %w", cleanupErr))
		}
	}()
	if err = temp.Chmod(mode); err == nil {
		_, err = temp.Write(data)
	}
	if err == nil {
		err = temp.Sync()
	}
	var identity os.FileInfo
	if err == nil {
		identity, err = temp.Stat()
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
				"launchd plist %s appeared during install",
				filepath.Join(directory.path, name),
			)
		}
		return publication, err
	}
	publication = publishedLaunchdPlist{
		directory: directory,
		name:      name,
		data:      append([]byte(nil), data...),
		identity:  identity,
	}
	if err = unix.Unlinkat(directory.fd(), tempName, 0); err != nil {
		return publication, err
	}
	if err = unix.Fsync(directory.fd()); err != nil {
		return publication, err
	}
	return publication, nil
}

func publishedLaunchdPlistMatches(publication publishedLaunchdPlist) (bool, error) {
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
	stat, err := file.Stat()
	if err != nil {
		return false, err
	}
	if !stat.Mode().IsRegular() || !os.SameFile(stat, publication.identity) ||
		stat.Size() != int64(len(publication.data)) {
		return false, nil
	}
	data, err := io.ReadAll(io.LimitReader(file, maximumManagedPlistSize+1))
	if err != nil {
		return false, err
	}
	return bytes.Equal(data, publication.data), nil
}

func requirePublishedLaunchdPlist(publication publishedLaunchdPlist) error {
	owned, err := publishedLaunchdPlistMatches(publication)
	if err != nil {
		return fmt.Errorf("verify installed launchd plist: %w", err)
	}
	if !owned {
		return errors.New("launchd plist no longer matches the installed transaction")
	}
	return nil
}

func removePublishedLaunchdPlist(publication publishedLaunchdPlist) error {
	id, err := newLaunchdTransactionID()
	if err != nil {
		return err
	}
	quarantineName := publication.name + ".rollback-" + id
	if err = renameLaunchdNoReplace(
		publication.directory.fd(),
		publication.name,
		quarantineName,
	); err != nil {
		return fmt.Errorf("quarantine failed launchd plist: %w", err)
	}
	quarantined := publication
	quarantined.name = quarantineName
	owned, verifyErr := publishedLaunchdPlistMatches(quarantined)
	if verifyErr != nil || !owned {
		restoreErr := renameLaunchdNoReplace(
			publication.directory.fd(),
			quarantineName,
			publication.name,
		)
		ownershipErr := errors.New("rollback refused: launchd plist is no longer the installed transaction")
		if verifyErr != nil {
			ownershipErr = fmt.Errorf("verify quarantined launchd plist: %w", verifyErr)
		}
		return errors.Join(ownershipErr, restoreErr)
	}
	if err = unix.Unlinkat(publication.directory.fd(), quarantineName, 0); err != nil {
		return err
	}
	return unix.Fsync(publication.directory.fd())
}

func renderLaunchdPlist(
	request lifecycleRequest,
	system bool,
	service string,
	transactionID string,
) (string, error) {
	if !nodeInstancePattern.MatchString(request.Instance) || service == "" {
		return "", errors.New("launchd plist requires a valid service identity")
	}
	if !filepath.IsAbs(request.ExecutablePath) || !filepath.IsAbs(request.ConfigPath) {
		return "", errors.New("launchd executable and config paths must be absolute")
	}
	if len(transactionID) != 32 {
		return "", errors.New("launchd plist requires an install transaction identity")
	}
	if _, err := hex.DecodeString(transactionID); err != nil {
		return "", errors.New("launchd plist requires a valid install transaction identity")
	}
	if system && !serviceAccountPattern.MatchString(request.ServiceUser) {
		return "", errors.New("launchd system plist requires a valid service user")
	}

	var body strings.Builder
	body.WriteString(xml.Header)
	body.WriteString("<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" ")
	body.WriteString("\"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n")
	body.WriteString(managedLaunchdPlistMarker + "\n")
	body.WriteString("<!-- " + launchdInstallTransactionMarker + transactionID + " -->\n")
	body.WriteString("<plist version=\"1.0\">\n<dict>\n")
	writeLaunchdString(&body, "Label", service)
	body.WriteString("\t<key>ProgramArguments</key>\n\t<array>\n")
	for _, argument := range []string{request.ExecutablePath, "run", "--config", request.ConfigPath} {
		body.WriteString("\t\t<string>")
		if err := xml.EscapeText(&body, []byte(argument)); err != nil {
			return "", err
		}
		body.WriteString("</string>\n")
	}
	body.WriteString("\t</array>\n")
	if system {
		writeLaunchdString(&body, "UserName", request.ServiceUser)
	}
	body.WriteString("\t<key>RunAtLoad</key>\n\t<true/>\n")
	body.WriteString("\t<key>KeepAlive</key>\n\t<true/>\n")
	writeLaunchdString(&body, "ProcessType", "Background")
	body.WriteString("</dict>\n</plist>\n")
	return body.String(), nil
}

func writeLaunchdString(writer *strings.Builder, key, value string) {
	writer.WriteString("\t<key>")
	_ = xml.EscapeText(writer, []byte(key))
	writer.WriteString("</key>\n\t<string>")
	_ = xml.EscapeText(writer, []byte(value))
	writer.WriteString("</string>\n")
}

func newLaunchdTransactionID() (string, error) {
	var value [16]byte
	if _, err := io.ReadFull(rand.Reader, value[:]); err != nil {
		return "", fmt.Errorf("create launchd transaction identity: %w", err)
	}
	return hex.EncodeToString(value[:]), nil
}
