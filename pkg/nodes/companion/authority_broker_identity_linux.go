//go:build linux

package companion

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/sys/unix"
)

const (
	authorityBrokerProcRoot        = "/proc"
	authorityBrokerCgroupRoot      = "/sys/fs/cgroup"
	authorityBrokerCgroupReadBytes = 64 * 1024
)

var authorityBrokerDelegationXattrs = []string{
	"user.delegate",
	"trusted.delegate",
	"system.posix_acl_access",
}

type authorityBrokerCompanionIdentity interface {
	Authorize(int32, string) bool
	Close() error
}

type authorityBrokerCgroupIdentity struct {
	mu          sync.Mutex
	cgroup      string
	procRoot    string
	cgroupFD    int
	processesFD int
	pid         int32
	pidfd       int
}

func newAuthorityBrokerCgroupIdentity(
	cgroup string,
) (*authorityBrokerCgroupIdentity, error) {
	return openAuthorityBrokerCgroupIdentity(
		cgroup,
		authorityBrokerProcRoot,
		authorityBrokerCgroupRoot,
	)
}

func openAuthorityBrokerCgroupIdentity(
	cgroup string,
	procRoot string,
	cgroupRoot string,
) (*authorityBrokerCgroupIdentity, error) {
	rootFD, err := unix.Open(
		cgroupRoot,
		unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW,
		0,
	)
	if err != nil {
		return nil, fmt.Errorf("open authority broker cgroup root: %w", err)
	}
	if validationErr := validateAuthorityBrokerCgroupFD(
		rootFD,
		unix.S_IFDIR,
	); validationErr != nil {
		_ = unix.Close(rootFD)
		return nil, fmt.Errorf(
			"validate authority broker cgroup root: %w",
			validationErr,
		)
	}
	currentFD := rootFD
	for _, component := range strings.Split(strings.TrimPrefix(cgroup, "/"), "/") {
		nextFD, openErr := unix.Openat(
			currentFD,
			component,
			unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW,
			0,
		)
		if currentFD != rootFD {
			_ = unix.Close(currentFD)
		}
		if openErr != nil {
			_ = unix.Close(rootFD)
			return nil, fmt.Errorf("open authority broker companion cgroup: %w", openErr)
		}
		currentFD = nextFD
		if validationErr := validateAuthorityBrokerCgroupFD(
			currentFD,
			unix.S_IFDIR,
		); validationErr != nil {
			_ = unix.Close(currentFD)
			_ = unix.Close(rootFD)
			return nil, fmt.Errorf(
				"validate authority broker companion cgroup: %w",
				validationErr,
			)
		}
	}
	if currentFD == rootFD {
		_ = unix.Close(rootFD)
		return nil, errors.New("authority broker companion cgroup is invalid")
	}
	_ = unix.Close(rootFD)
	processesFD, err := unix.Openat(
		currentFD,
		"cgroup.procs",
		unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW,
		0,
	)
	if err != nil {
		_ = unix.Close(currentFD)
		return nil, fmt.Errorf("open authority broker cgroup membership: %w", err)
	}
	if validationErr := validateAuthorityBrokerCgroupFD(
		processesFD,
		unix.S_IFREG,
	); validationErr != nil {
		_ = unix.Close(processesFD)
		_ = unix.Close(currentFD)
		return nil, fmt.Errorf(
			"validate authority broker cgroup membership: %w",
			validationErr,
		)
	}
	return &authorityBrokerCgroupIdentity{
		cgroup: cgroup, procRoot: procRoot,
		cgroupFD: currentFD, processesFD: processesFD, pidfd: -1,
	}, nil
}

func validateAuthorityBrokerCgroupFD(descriptor int, expectedType uint32) error {
	var stat unix.Stat_t
	if err := unix.Fstat(descriptor, &stat); err != nil {
		return err
	}
	if stat.Uid != 0 ||
		stat.Gid != 0 ||
		stat.Mode&unix.S_IFMT != expectedType ||
		stat.Mode&0o022 != 0 {
		return errors.New("cgroup object is not root-owned and non-writable")
	}
	for _, attribute := range authorityBrokerDelegationXattrs {
		exists, err := authorityBrokerFDHasXattr(descriptor, attribute)
		if err != nil {
			return err
		}
		if exists {
			return fmt.Errorf("cgroup object carries delegation metadata %q", attribute)
		}
	}
	return nil
}

func authorityBrokerFDHasXattr(descriptor int, attribute string) (bool, error) {
	_, err := unix.Fgetxattr(descriptor, attribute, nil)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, unix.ENODATA) || errors.Is(err, unix.ENOTSUP) {
		return false, nil
	}
	return false, err
}

func newAuthorityBrokerPIDIdentity(pid int32) (*authorityBrokerCgroupIdentity, error) {
	pidfd, err := unix.PidfdOpen(int(pid), 0)
	if err != nil {
		return nil, err
	}
	return &authorityBrokerCgroupIdentity{
		cgroupFD: -1, processesFD: -1, pid: pid, pidfd: pidfd,
	}, nil
}

func (identity *authorityBrokerCgroupIdentity) Authorize(peerPID int32, action string) bool {
	if identity == nil || peerPID <= 0 {
		return false
	}
	identity.mu.Lock()
	defer identity.mu.Unlock()
	if identity.pidfd >= 0 {
		return peerPID == identity.pid &&
			unix.PidfdSendSignal(identity.pidfd, 0, nil, 0) == nil
	}
	if action != authorityBrokerActionSnapshot ||
		!identity.peerOwnsCgroup(peerPID) {
		return false
	}
	pidfd, err := unix.PidfdOpen(int(peerPID), 0)
	if err != nil {
		return false
	}
	if !identity.peerOwnsCgroup(peerPID) {
		_ = unix.Close(pidfd)
		return false
	}
	identity.pid = peerPID
	identity.pidfd = pidfd
	return true
}

func (identity *authorityBrokerCgroupIdentity) peerOwnsCgroup(peerPID int32) bool {
	cgroup, err := authorityBrokerProcessCgroup(peerPID, identity.procRoot)
	if err != nil || cgroup != identity.cgroup {
		return false
	}
	buffer := make([]byte, authorityBrokerCgroupReadBytes)
	count, err := unix.Pread(identity.processesFD, buffer, 0)
	if err != nil || count <= 0 || count == len(buffer) {
		return false
	}
	fields := strings.Fields(string(buffer[:count]))
	return len(fields) == 1 && fields[0] == strconv.FormatInt(int64(peerPID), 10)
}

func (identity *authorityBrokerCgroupIdentity) Close() error {
	if identity == nil {
		return nil
	}
	identity.mu.Lock()
	defer identity.mu.Unlock()
	var result error
	for _, descriptor := range []*int{
		&identity.pidfd,
		&identity.processesFD,
		&identity.cgroupFD,
	} {
		if *descriptor < 0 {
			continue
		}
		if err := unix.Close(*descriptor); err != nil && result == nil {
			result = err
		}
		*descriptor = -1
	}
	return result
}

func authorityBrokerProcessCgroup(pid int32, procRoot string) (string, error) {
	raw, err := os.ReadFile(
		filepath.Join(procRoot, strconv.FormatInt(int64(pid), 10), "cgroup"),
	)
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.SplitN(line, ":", 3)
		if len(fields) == 3 && fields[0] == "0" && fields[1] == "" {
			cgroup := strings.TrimSpace(fields[2])
			if cgroup == "" || !strings.HasPrefix(cgroup, "/") {
				break
			}
			return cgroup, nil
		}
	}
	return "", errors.New("companion process is not in a cgroup v2 domain")
}
