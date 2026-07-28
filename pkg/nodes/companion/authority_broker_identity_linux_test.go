//go:build linux

package companion

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"

	"golang.org/x/sys/unix"
)

func TestAuthorityBrokerCgroupIdentityRequiresSoleRootControlledProcess(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("root-controlled cgroup proof requires root")
	}
	fixture := newAuthorityBrokerCgroupFixture(t)
	identity, err := openAuthorityBrokerCgroupIdentity(
		fixture.controlGroup,
		fixture.procRoot,
		fixture.cgroupRoot,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := identity.Close(); err != nil {
			t.Errorf("close identity: %v", err)
		}
	})
	if !identity.Authorize(fixture.pid, authorityBrokerActionSnapshot) {
		t.Fatal("sole process in root-controlled cgroup was rejected")
	}
	if identity.Authorize(fixture.pid+1, authorityBrokerActionSnapshot) {
		t.Fatal("different process reused established companion authority")
	}

	if err := os.WriteFile(
		fixture.processesPath,
		[]byte(strconv.Itoa(int(fixture.pid))+"\n999999\n"),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	unclaimed, err := openAuthorityBrokerCgroupIdentity(
		fixture.controlGroup,
		fixture.procRoot,
		fixture.cgroupRoot,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer unclaimed.Close()
	if unclaimed.Authorize(fixture.pid, authorityBrokerActionSnapshot) {
		t.Fatal("non-sole process claimed companion authority")
	}
}

func TestAuthorityBrokerCgroupIdentityRetainsMembershipDescriptor(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("retained cgroup descriptor proof requires root")
	}
	fixture := newAuthorityBrokerCgroupFixture(t)
	identity, err := openAuthorityBrokerCgroupIdentity(
		fixture.controlGroup,
		fixture.procRoot,
		fixture.cgroupRoot,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer identity.Close()
	moved := fixture.controlDirectory + "-moved"
	if err := os.Rename(fixture.controlDirectory, moved); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(fixture.controlDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(fixture.controlDirectory, "cgroup.procs"),
		[]byte("999999\n"),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	if !identity.Authorize(fixture.pid, authorityBrokerActionSnapshot) {
		t.Fatal("identity stopped using its retained membership descriptor")
	}
}

func TestAuthorityBrokerCgroupIdentityRejectsDelegatedDomain(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("delegated cgroup rejection requires root")
	}
	tests := []struct {
		name   string
		mutate func(*testing.T, authorityBrokerCgroupFixture)
	}{
		{
			name: "writable directory",
			mutate: func(t *testing.T, fixture authorityBrokerCgroupFixture) {
				t.Helper()
				if err := os.Chmod(fixture.controlDirectory, 0o770); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "writable membership",
			mutate: func(t *testing.T, fixture authorityBrokerCgroupFixture) {
				t.Helper()
				if err := os.Chmod(fixture.processesPath, 0o620); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "delegation xattr",
			mutate: func(t *testing.T, fixture authorityBrokerCgroupFixture) {
				t.Helper()
				if err := unix.Setxattr(
					fixture.controlDirectory,
					"user.delegate",
					[]byte("1"),
					0,
				); err != nil {
					if errors.Is(err, unix.ENOTSUP) {
						t.Skip("filesystem does not support delegation xattrs")
					}
					t.Fatal(err)
				}
			},
		},
		{
			name: "symlinked component",
			mutate: func(t *testing.T, fixture authorityBrokerCgroupFixture) {
				t.Helper()
				component := filepath.Join(fixture.cgroupRoot, "system.slice")
				moved := component + "-real"
				if err := os.Rename(component, moved); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(moved, component); err != nil {
					t.Fatal(err)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newAuthorityBrokerCgroupFixture(t)
			test.mutate(t, fixture)
			if identity, err := openAuthorityBrokerCgroupIdentity(
				fixture.controlGroup,
				fixture.procRoot,
				fixture.cgroupRoot,
			); err == nil {
				_ = identity.Close()
				t.Fatal("delegated companion cgroup was accepted")
			}
		})
	}
}

func TestAuthorityBrokerCgroupIdentityOpensKernelDomain(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("kernel cgroup ownership proof requires root")
	}
	cgroup, err := authorityBrokerProcessCgroup(
		int32(os.Getpid()),
		authorityBrokerProcRoot,
	)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := newAuthorityBrokerCgroupIdentity(cgroup)
	if err != nil {
		t.Fatal(err)
	}
	if err := identity.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestAuthorityBrokerPIDIdentityFailsClosedAfterExit(t *testing.T) {
	command := exec.Command("/bin/sleep", "30")
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	identity, err := newAuthorityBrokerPIDIdentity(int32(command.Process.Pid))
	if err != nil {
		_ = command.Process.Kill()
		_ = command.Wait()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = identity.Close()
		_ = command.Process.Kill()
		_ = command.Wait()
	})
	if !identity.Authorize(int32(command.Process.Pid), authorityBrokerActionSnapshot) {
		t.Fatal("live pidfd identity was rejected")
	}
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err == nil {
		t.Fatal("killed identity process exited successfully")
	}
	if identity.Authorize(int32(command.Process.Pid), authorityBrokerActionExecute) {
		t.Fatal("exited pidfd identity remained authorized")
	}
}

type authorityBrokerCgroupFixture struct {
	procRoot         string
	cgroupRoot       string
	controlGroup     string
	controlDirectory string
	processesPath    string
	pid              int32
}

func newAuthorityBrokerCgroupFixture(t *testing.T) authorityBrokerCgroupFixture {
	t.Helper()
	base := authorityBrokerServiceTestDir(t)
	fixture := authorityBrokerCgroupFixture{
		procRoot:     filepath.Join(base, "proc"),
		cgroupRoot:   filepath.Join(base, "cgroup"),
		controlGroup: "/system.slice/mintclaw-node.service",
		pid:          int32(os.Getpid()),
	}
	processDirectory := filepath.Join(
		fixture.procRoot,
		strconv.Itoa(int(fixture.pid)),
	)
	fixture.controlDirectory = filepath.Join(
		fixture.cgroupRoot,
		"system.slice",
		"mintclaw-node.service",
	)
	if err := os.MkdirAll(processDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(fixture.controlDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(processDirectory, "cgroup"),
		[]byte("0::"+fixture.controlGroup+"\n"),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	fixture.processesPath = filepath.Join(fixture.controlDirectory, "cgroup.procs")
	if err := os.WriteFile(
		fixture.processesPath,
		[]byte(strconv.Itoa(int(fixture.pid))+"\n"),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	return fixture
}
