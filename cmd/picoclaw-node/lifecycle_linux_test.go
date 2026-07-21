//go:build linux

package main

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

type systemdCall struct {
	system bool
	args   []string
}

func TestSystemdLifecycleInstallAndUninstall(t *testing.T) {
	var calls []systemdCall
	lifecycle := &systemdLifecycle{
		unitDir: t.TempDir(),
		run: func(_ context.Context, system bool, args ...string) (systemdRunResult, error) {
			calls = append(calls, systemdCall{system: system, args: append([]string(nil), args...)})
			if reflect.DeepEqual(args, []string{"is-active", "picoclaw-node-main.service"}) {
				return systemdRunResult{Output: "active"}, nil
			}
			return systemdRunResult{}, nil
		},
	}
	request := lifecycleRequest{
		Instance:       "main",
		ConfigPath:     "/home/test/node config/config%main.json",
		ExecutablePath: "/home/test/bin/picoclaw node",
	}
	status, err := lifecycle.Install(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if !status.Installed || !status.Active || status.State != "active" || status.Scope != "user" {
		t.Fatalf("install status = %#v", status)
	}
	data, err := os.ReadFile(status.UnitPath)
	if err != nil {
		t.Fatal(err)
	}
	unit := string(data)
	execStart := `ExecStart="/home/test/bin/picoclaw node" run --config ` +
		`"/home/test/node config/config%%main.json"`
	if !strings.Contains(unit, execStart) || !strings.Contains(unit, "WantedBy=default.target") ||
		!strings.Contains(unit, "NoNewPrivileges=true") || strings.Contains(unit, "User=") {
		t.Fatalf("unexpected systemd unit:\n%s", unit)
	}
	wantInstallCalls := []systemdCall{
		{args: []string{"daemon-reload"}},
		{args: []string{"enable", "--now", "picoclaw-node-main.service"}},
		{args: []string{"is-active", "picoclaw-node-main.service"}},
	}
	if !reflect.DeepEqual(calls, wantInstallCalls) {
		t.Fatalf("install calls = %#v, want %#v", calls, wantInstallCalls)
	}

	calls = nil
	status, err = lifecycle.Uninstall(t.Context(), lifecycleRequest{Instance: "main"})
	if err != nil {
		t.Fatal(err)
	}
	if status.Installed || status.Active || status.State != "removed" {
		t.Fatalf("uninstall status = %#v", status)
	}
	if _, err := os.Stat(status.UnitPath); !os.IsNotExist(err) {
		t.Fatalf("unit still exists: %v", err)
	}
	wantUninstallCalls := []systemdCall{
		{args: []string{"disable", "--now", "picoclaw-node-main.service"}},
		{args: []string{"daemon-reload"}},
	}
	if !reflect.DeepEqual(calls, wantUninstallCalls) {
		t.Fatalf("uninstall calls = %#v, want %#v", calls, wantUninstallCalls)
	}
}

func TestSystemdLifecycleStatus(t *testing.T) {
	unitDir := t.TempDir()
	unitPath := filepath.Join(unitDir, "picoclaw-node-main.service")
	if err := os.WriteFile(unitPath, []byte("test"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name    string
		result  systemdRunResult
		active  bool
		wantErr bool
	}{
		{name: "active", result: systemdRunResult{Output: "active"}, active: true},
		{name: "activating", result: systemdRunResult{Output: "activating"}},
		{name: "inactive", result: systemdRunResult{Output: "inactive", ExitCode: 3}},
		{name: "unexpected", result: systemdRunResult{Output: "unknown", ExitCode: 4}, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			lifecycle := &systemdLifecycle{
				unitDir: unitDir,
				run: func(_ context.Context, system bool, args ...string) (systemdRunResult, error) {
					if system || !reflect.DeepEqual(args, []string{"is-active", "picoclaw-node-main.service"}) {
						t.Fatalf("status call = system:%t args:%v", system, args)
					}
					return test.result, nil
				},
			}
			status, err := lifecycle.Status(t.Context(), lifecycleRequest{Instance: "main"})
			if (err != nil) != test.wantErr {
				t.Fatalf("Status() error = %v", err)
			}
			unexpectedStatus := !status.Installed || status.Active != test.active ||
				status.State != test.result.Output
			if !test.wantErr && unexpectedStatus {
				t.Fatalf("status = %#v", status)
			}
		})
	}
}

func TestSystemdLifecycleUninstallMissingUnitReloadsManager(t *testing.T) {
	var calls []systemdCall
	lifecycle := &systemdLifecycle{
		unitDir: t.TempDir(),
		run: func(_ context.Context, system bool, args ...string) (systemdRunResult, error) {
			calls = append(calls, systemdCall{system: system, args: append([]string(nil), args...)})
			return systemdRunResult{}, nil
		},
	}
	status, err := lifecycle.Uninstall(t.Context(), lifecycleRequest{Instance: "main"})
	if err != nil {
		t.Fatal(err)
	}
	if status.Installed || status.State != "not-installed" {
		t.Fatalf("uninstall status = %#v", status)
	}
	want := []systemdCall{{args: []string{"daemon-reload"}}}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("uninstall calls = %#v, want %#v", calls, want)
	}
}

func TestRenderSystemdSystemUnit(t *testing.T) {
	unit, err := renderSystemdUnit(lifecycleRequest{
		Instance:       "vpn",
		ConfigPath:     "/etc/forgeclaw/node.json",
		ExecutablePath: "/usr/local/bin/picoclaw-node",
		ServiceUser:    "forgeclaw-node",
	}, true)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(unit, "WantedBy=multi-user.target") ||
		!strings.Contains(unit, "User=forgeclaw-node") {
		t.Fatalf("system unit has wrong target:\n%s", unit)
	}
	if _, err := quoteSystemdArgument("bad\npath"); err == nil {
		t.Fatal("systemd argument accepted a newline")
	}
	if _, err := renderSystemdUnit(lifecycleRequest{
		Instance: "vpn", ConfigPath: "relative.json", ExecutablePath: "/bin/picoclaw-node",
	}, false); err == nil {
		t.Fatal("systemd unit accepted a relative config path")
	}
	if _, err := renderSystemdUnit(lifecycleRequest{
		Instance: "vpn", ConfigPath: "/etc/node.json", ExecutablePath: "/bin/picoclaw-node",
	}, true); err == nil {
		t.Fatal("systemd system unit accepted an empty service user")
	}
}
