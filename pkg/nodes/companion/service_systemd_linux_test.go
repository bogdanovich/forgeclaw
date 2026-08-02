//go:build linux

package companion

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/nodes"
)

const systemdReadHelperEnvironment = "MINTCLAW_SYSTEMD_READ_HELPER"

func TestSystemdServiceManagerReadsStrictProfileAlias(t *testing.T) {
	manager := testSystemdServiceManager(t, "status", "logs")
	descriptors := manager.Descriptors()
	if len(descriptors) != 2 || descriptors[0].Name != "service.status.v1" ||
		descriptors[1].Name != "service.logs.v1" ||
		descriptors[1].ModelContract.OutputBytesMax != 4096 {
		t.Fatalf("read descriptors = %#v", descriptors)
	}
	encodedDescriptors, err := json.Marshal(descriptors)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encodedDescriptors), "wg-quick@wg0.service") ||
		strings.Contains(string(encodedDescriptors), "service.action.v1") {
		t.Fatalf("read descriptors leaked hidden authority: %s", encodedDescriptors)
	}
	descriptors[0].ServiceProfiles[0].Services[0].Alias = "mutated"
	if manager.Descriptors()[0].ServiceProfiles[0].Services[0].Alias != "vpn" {
		t.Fatal("descriptor caller mutated retained systemd authority")
	}
	status, err := manager.Status(t.Context(), ServiceStatusRequest{
		Profile: "server-services",
		Service: "vpn",
	})
	if err != nil {
		t.Fatal(err)
	}
	if status != (ServiceStatus{
		Service:     "vpn",
		LoadState:   "loaded",
		ActiveState: "active",
		Substate:    "running",
		Enabled:     "enabled",
		ObservedAt:  1_700_000_000,
	}) {
		t.Fatalf("status = %#v", status)
	}
	statusJSON, err := json.Marshal(status)
	if err != nil {
		t.Fatal(err)
	}
	statusDescriptor := manager.Descriptors()[0]
	if _, err = nodes.ValidateInvocationOutput(statusDescriptor, statusJSON, 4096); err != nil {
		t.Fatalf("status output contract: %v", err)
	}

	for _, request := range []ServiceStatusRequest{
		{Profile: "other-services", Service: "vpn"},
		{Profile: "server-services", Service: "neighbor"},
		{Profile: "server-services", Service: "../neighbor.service"},
	} {
		_, statusErr := manager.Status(t.Context(), request)
		var managerErr *ServiceManagerError
		if !errors.As(statusErr, &managerErr) || managerErr.Code != "command_denied" {
			t.Fatalf("Status(%#v) error = %v", request, statusErr)
		}
	}
}

func TestSystemdServiceManagerRequiresCompleteReadEnforcement(t *testing.T) {
	profile := servicePolicyFixture()
	policies, err := normalizeServicePolicies(ServicePolicies{"server-services": profile})
	if err != nil {
		t.Fatal(err)
	}
	_, err = newSystemdServiceManager(
		policies,
		systemdProcessRunner{journal: commandExecutable{path: os.Args[0]}},
		time.Now,
	)
	if err == nil || !strings.Contains(err.Error(), "incomplete process backend") {
		t.Fatalf("incomplete read backend error = %v", err)
	}

	actionOnly := servicePolicyFixture()
	service := actionOnly.Services["vpn"]
	service.Status = false
	service.Logs = false
	actionOnly.Services["vpn"] = service
	actionPolicies, err := normalizeServicePolicies(ServicePolicies{"server-services": actionOnly})
	if err != nil {
		t.Fatal(err)
	}
	_, err = newSystemdServiceManager(
		actionPolicies,
		systemdProcessRunner{systemctl: commandExecutable{path: os.Args[0]}},
		time.Now,
	)
	if err == nil || !strings.Contains(err.Error(), "no readable profile") {
		t.Fatalf("action-only read backend error = %v", err)
	}
}

func TestNewSystemdServiceManagerRejectsUnusableLogOutputBudget(t *testing.T) {
	profile := servicePolicyFixture()
	profile.LogLimits = nodes.ServiceLogLimits{EntriesMax: 1, BytesMax: 1, AgeSecondsMax: 1}
	_, err := NewSystemdServiceManager(ServicePolicies{"server-services": profile})
	if err == nil || !strings.Contains(err.Error(), "mandatory result") {
		t.Fatalf("unusable systemd log constructor error = %v", err)
	}
}

func TestSystemdServiceManagerBoundsAndNormalizesLogs(t *testing.T) {
	manager := testSystemdServiceManager(t, "status", "logs")
	logs, err := manager.Logs(t.Context(), ServiceLogRequest{
		Profile:      "server-services",
		Service:      "vpn",
		Entries:      999,
		SinceSeconds: 999,
	})
	if err != nil {
		t.Fatal(err)
	}
	if logs.Service != "vpn" || logs.Truncated || len(logs.Records) != 2 {
		t.Fatalf("logs = %#v", logs)
	}
	if logs.Records[0] != (ServiceLogRecord{
		Timestamp: 1_700_000_001,
		Severity:  "warning",
		Message:   "first�line",
	}) || logs.Records[1] != (ServiceLogRecord{
		Timestamp: 1_700_000_002,
		Severity:  "info",
		Message:   "second",
	}) {
		t.Fatalf("normalized records = %#v", logs.Records)
	}
	data, marshalErr := json.Marshal(logs)
	if marshalErr != nil || len(data) > 4096 {
		t.Fatalf("bounded logs = %d bytes, error %v", len(data), marshalErr)
	}
	descriptors := manager.Descriptors()
	if _, err = nodes.ValidateInvocationOutput(descriptors[1], data, 4096); err != nil {
		t.Fatalf("logs output contract: %v", err)
	}
}

func TestSystemdServiceManagerReturnsEmptyLogsAtExactOutputBudget(t *testing.T) {
	profile := servicePolicyFixture()
	minimum, err := json.Marshal(ServiceLogs{
		Service:   "vpn",
		Records:   []ServiceLogRecord{},
		Truncated: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	profile.LogLimits = nodes.ServiceLogLimits{
		EntriesMax: 2, BytesMax: len(minimum), AgeSecondsMax: 60,
	}
	policies, err := normalizeServicePolicies(ServicePolicies{"server-services": profile})
	if err != nil {
		t.Fatal(err)
	}
	manager, err := newSystemdServiceManager(
		policies,
		systemdProcessRunner{
			systemctl: commandExecutable{
				path:   os.Args[0],
				prefix: []string{"-test.run=^TestSystemdReadProcessHelper$", "--", "status"},
			},
			journal: commandExecutable{
				path:   os.Args[0],
				prefix: []string{"-test.run=^TestSystemdReadProcessHelper$", "--", "empty"},
			},
			env: append(fixedSystemdEnvironment(), systemdReadHelperEnvironment+"=1"),
		},
		func() time.Time { return time.Unix(1_700_000_000, 0) },
	)
	if err != nil {
		t.Fatal(err)
	}
	logs, err := manager.Logs(t.Context(), ServiceLogRequest{
		Profile: "server-services",
		Service: "vpn",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(logs.Records) != 0 || logs.Truncated {
		t.Fatalf("empty logs = %#v", logs)
	}
	data, err := json.Marshal(logs)
	if err != nil || len(data) != len(minimum) {
		t.Fatalf("empty logs output = %d bytes, error %v", len(data), err)
	}
}

func TestSystemdServiceManagerDoesNotExposeProcessFailure(t *testing.T) {
	manager := testSystemdServiceManager(t, "fail", "fail")
	status, err := manager.Status(t.Context(), ServiceStatusRequest{
		Profile: "server-services",
		Service: "vpn",
	})
	if err != nil || status.Code != "output_invalid" {
		t.Fatalf("failed status = %#v, error %v", status, err)
	}
	logs, logsErr := manager.Logs(t.Context(), ServiceLogRequest{
		Profile: "server-services",
		Service: "vpn",
	})
	if logsErr == nil || strings.Contains(logsErr.Error(), "secret-manager-detail") ||
		logs.Service != "" || len(logs.Records) != 0 || logs.Truncated {
		t.Fatalf("failed logs = %#v, error %v", logs, logsErr)
	}
}

func TestSystemdServiceManagerCancellationTerminatesProcess(t *testing.T) {
	for _, operation := range []string{"status", "logs"} {
		t.Run(operation, func(t *testing.T) {
			manager := testSystemdServiceManager(t, "block", "block")
			marker := t.TempDir() + "/started"
			manager.runner.env = append(manager.runner.env, "MINTCLAW_SYSTEMD_READ_MARKER="+marker)
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan error, 1)
			go func() {
				if operation == "status" {
					_, err := manager.Status(ctx, ServiceStatusRequest{
						Profile: "server-services",
						Service: "vpn",
					})
					done <- err
					return
				}
				_, err := manager.Logs(ctx, ServiceLogRequest{
					Profile: "server-services",
					Service: "vpn",
				})
				done <- err
			}()
			waitForSystemdReadMarker(t, marker)
			cancel()
			var err error
			select {
			case err = <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("systemd read helper did not stop after cancellation")
			}
			if !errors.Is(err, errCommandCancellationConfirmed) {
				t.Fatalf("%s cancellation error = %v", operation, err)
			}
		})
	}
}

func TestSystemdServiceManagerStopsProcessAtOutputCeiling(t *testing.T) {
	manager := testSystemdServiceManager(t, "flood", "flood")
	statusCtx, cancelStatus := context.WithTimeout(t.Context(), time.Second)
	defer cancelStatus()
	status, err := manager.Status(statusCtx, ServiceStatusRequest{
		Profile: "server-services",
		Service: "vpn",
	})
	if err != nil || status.Code != "output_limit" {
		t.Fatalf("status output ceiling = %#v, error %v", status, err)
	}

	logsCtx, cancelLogs := context.WithTimeout(t.Context(), time.Second)
	defer cancelLogs()
	logs, err := manager.Logs(logsCtx, ServiceLogRequest{
		Profile: "server-services",
		Service: "vpn",
	})
	if err != nil || !logs.Truncated || len(logs.Records) != 0 {
		t.Fatalf("logs output ceiling = %#v, error %v", logs, err)
	}
}

func TestSystemdServiceManagerRejectsMalformedLogBoundsBeforeProcess(t *testing.T) {
	manager := testSystemdServiceManager(t, "status", "fail")
	_, err := manager.Logs(t.Context(), ServiceLogRequest{
		Profile:      "server-services",
		Service:      "vpn",
		Entries:      -1,
		SinceSeconds: -1,
	})
	var managerErr *ServiceManagerError
	if !errors.As(err, &managerErr) || managerErr.Code != "input_invalid" {
		t.Fatalf("malformed log bounds error = %v", err)
	}
}

func TestSystemdReadProcessHelper(t *testing.T) {
	if os.Getenv(systemdReadHelperEnvironment) != "1" {
		return
	}
	separator := slices.Index(os.Args, "--")
	if separator < 0 || separator+1 >= len(os.Args) {
		os.Exit(90)
	}
	mode := os.Args[separator+1]
	args := os.Args[separator+2:]
	switch mode {
	case "status":
		want := []string{
			"--system", "--no-pager", "--no-ask-password", "--plain", "show",
			"--property=LoadState", "--property=ActiveState",
			"--property=SubState", "--property=UnitFileState",
			"wg-quick@wg0.service",
		}
		if !slices.Equal(args, want) || os.Getenv("LC_ALL") != "C" || os.Getenv("SYSTEMD_PAGER") != "cat" {
			os.Exit(91)
		}
		fmt.Print("LoadState=loaded\nActiveState=active\nSubState=running\nUnitFileState=enabled\n")
	case "logs":
		want := []string{
			"--no-pager", "--quiet", "--output=json",
			"--output-fields=__REALTIME_TIMESTAMP,PRIORITY,MESSAGE",
			"--unit=wg-quick@wg0.service", "--since=@1699999940", "--lines=2",
		}
		if !slices.Equal(args, want) {
			os.Exit(92)
		}
		fmt.Print("{\"__REALTIME_TIMESTAMP\":\"1700000002000000\",\"PRIORITY\":\"6\",\"MESSAGE\":\"second\"}\n")
		fmt.Print(
			"{\"__REALTIME_TIMESTAMP\":\"1700000001000000\",\"PRIORITY\":\"4\",\"MESSAGE\":\"first\\u0001line\"}\n",
		)
	case "empty":
		want := []string{
			"--no-pager", "--quiet", "--output=json",
			"--output-fields=__REALTIME_TIMESTAMP,PRIORITY,MESSAGE",
			"--unit=wg-quick@wg0.service", "--since=@1699999940", "--lines=2",
		}
		if !slices.Equal(args, want) {
			os.Exit(92)
		}
	case "fail":
		fmt.Fprint(os.Stderr, "secret-manager-detail wg-quick@wg0.service")
		os.Exit(4)
	case "block":
		marker := os.Getenv("MINTCLAW_SYSTEMD_READ_MARKER")
		if marker == "" || os.WriteFile(marker, []byte("started"), 0o600) != nil {
			os.Exit(94)
		}
		time.Sleep(10 * time.Second)
	case "flood":
		fmt.Print(strings.Repeat("x", 1024*1024))
		time.Sleep(10 * time.Second)
	default:
		os.Exit(93)
	}
	os.Exit(0)
}

func waitForSystemdReadMarker(t *testing.T, path string) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := os.Stat(path); err == nil {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("systemd read helper did not report process start")
		case <-ticker.C:
		}
	}
}

func testSystemdServiceManager(
	t *testing.T,
	statusMode string,
	logsMode string,
) *systemdServiceManager {
	t.Helper()
	profile := servicePolicyFixture()
	profile.LogLimits = nodes.ServiceLogLimits{EntriesMax: 2, BytesMax: 4096, AgeSecondsMax: 60}
	policies, err := normalizeServicePolicies(ServicePolicies{"server-services": profile})
	if err != nil {
		t.Fatal(err)
	}
	runner := systemdProcessRunner{
		systemctl: commandExecutable{
			path:   os.Args[0],
			prefix: []string{"-test.run=^TestSystemdReadProcessHelper$", "--", statusMode},
		},
		journal: commandExecutable{
			path:   os.Args[0],
			prefix: []string{"-test.run=^TestSystemdReadProcessHelper$", "--", logsMode},
		},
		env: append(fixedSystemdEnvironment(), systemdReadHelperEnvironment+"=1"),
	}
	manager, err := newSystemdServiceManager(
		policies,
		runner,
		func() time.Time { return time.Unix(1_700_000_000, 0) },
	)
	if err != nil {
		t.Fatal(err)
	}
	return manager
}
