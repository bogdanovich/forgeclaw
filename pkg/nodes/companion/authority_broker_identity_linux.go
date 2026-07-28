//go:build linux

package companion

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/sys/unix"
)

const (
	authorityBrokerProcRoot   = "/proc"
	authorityBrokerCgroupRoot = "/sys/fs/cgroup"
)

type authorityBrokerCompanionIdentity interface {
	Authorize(int32, string) bool
	Close() error
}

type authorityBrokerCgroupIdentity struct {
	mu         sync.Mutex
	cgroup     string
	procRoot   string
	cgroupRoot string
	pid        int32
	pidfd      int
}

func newAuthorityBrokerCgroupIdentity(cgroup string) *authorityBrokerCgroupIdentity {
	return &authorityBrokerCgroupIdentity{
		cgroup: cgroup, procRoot: authorityBrokerProcRoot,
		cgroupRoot: authorityBrokerCgroupRoot, pidfd: -1,
	}
}

func newAuthorityBrokerPIDIdentity(pid int32) (*authorityBrokerCgroupIdentity, error) {
	pidfd, err := unix.PidfdOpen(int(pid), 0)
	if err != nil {
		return nil, err
	}
	return &authorityBrokerCgroupIdentity{pid: pid, pidfd: pidfd}, nil
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
		!authorityBrokerPeerOwnsCgroup(
			peerPID,
			identity.cgroup,
			identity.procRoot,
			identity.cgroupRoot,
		) {
		return false
	}
	pidfd, err := unix.PidfdOpen(int(peerPID), 0)
	if err != nil {
		return false
	}
	if !authorityBrokerPeerOwnsCgroup(
		peerPID,
		identity.cgroup,
		identity.procRoot,
		identity.cgroupRoot,
	) {
		_ = unix.Close(pidfd)
		return false
	}
	identity.pid = peerPID
	identity.pidfd = pidfd
	return true
}

func (identity *authorityBrokerCgroupIdentity) Close() error {
	if identity == nil {
		return nil
	}
	identity.mu.Lock()
	defer identity.mu.Unlock()
	if identity.pidfd < 0 {
		return nil
	}
	err := unix.Close(identity.pidfd)
	identity.pidfd = -1
	return err
}

func authorityBrokerPeerOwnsCgroup(
	peerPID int32,
	expected string,
	procRoot string,
	cgroupRoot string,
) bool {
	cgroup, err := authorityBrokerProcessCgroup(peerPID, procRoot)
	if err != nil || cgroup != expected {
		return false
	}
	processes, err := os.ReadFile(
		filepath.Join(cgroupRoot, strings.TrimPrefix(expected, "/"), "cgroup.procs"),
	)
	if err != nil {
		return false
	}
	fields := strings.Fields(string(processes))
	return len(fields) == 1 && fields[0] == strconv.FormatInt(int64(peerPID), 10)
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
