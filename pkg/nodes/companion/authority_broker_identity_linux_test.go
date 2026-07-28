//go:build linux

package companion

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
)

func TestAuthorityBrokerCgroupIdentityRequiresSoleRootControlledProcess(t *testing.T) {
	base := t.TempDir()
	procRoot := filepath.Join(base, "proc")
	cgroupRoot := filepath.Join(base, "cgroup")
	pid := int32(os.Getpid())
	processDirectory := filepath.Join(procRoot, strconv.Itoa(int(pid)))
	controlGroup := "/system.slice/mintclaw-node.service"
	controlDirectory := filepath.Join(cgroupRoot, "system.slice", "mintclaw-node.service")
	if err := os.MkdirAll(processDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(controlDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(processDirectory, "cgroup"),
		[]byte("0::"+controlGroup+"\n"),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	processesPath := filepath.Join(controlDirectory, "cgroup.procs")
	if err := os.WriteFile(
		processesPath,
		[]byte(strconv.Itoa(int(pid))+"\n"),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	identity := &authorityBrokerCgroupIdentity{
		cgroup: controlGroup, procRoot: procRoot, cgroupRoot: cgroupRoot, pidfd: -1,
	}
	t.Cleanup(func() {
		if err := identity.Close(); err != nil {
			t.Errorf("close identity: %v", err)
		}
	})
	if !identity.Authorize(pid, authorityBrokerActionSnapshot) {
		t.Fatal("sole process in root-controlled cgroup was rejected")
	}
	if identity.Authorize(pid+1, authorityBrokerActionSnapshot) {
		t.Fatal("different process reused established companion authority")
	}

	unclaimed := &authorityBrokerCgroupIdentity{
		cgroup: controlGroup, procRoot: procRoot, cgroupRoot: cgroupRoot, pidfd: -1,
	}
	if err := os.WriteFile(
		processesPath,
		[]byte(strconv.Itoa(int(pid))+"\n999999\n"),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	if unclaimed.Authorize(pid, authorityBrokerActionSnapshot) {
		t.Fatal("non-sole process claimed companion authority")
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
