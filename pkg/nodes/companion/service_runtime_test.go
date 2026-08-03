package companion

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/nodes"
)

type fakeServiceController struct {
	descriptors []nodes.CommandDescriptor

	mu            sync.Mutex
	statusRequest ServiceStatusRequest
	logsRequest   ServiceLogRequest
	logsResult    *ServiceLogs
	actionRequest ServiceActionRequest
	actionCalls   int
	actionResult  ServiceActionResult
	blockAction   bool
	actionStarted chan struct{}
}

func (manager *fakeServiceController) Descriptors() []nodes.CommandDescriptor {
	return cloneCatalog(nodes.CapabilityCatalog{Commands: manager.descriptors}).Commands
}

func (manager *fakeServiceController) Status(
	_ context.Context,
	request ServiceStatusRequest,
) (ServiceStatus, error) {
	manager.mu.Lock()
	manager.statusRequest = request
	manager.mu.Unlock()
	return serviceRuntimeStatus(request.Service), nil
}

func (manager *fakeServiceController) Logs(
	_ context.Context,
	request ServiceLogRequest,
) (ServiceLogs, error) {
	manager.mu.Lock()
	manager.logsRequest = request
	result := manager.logsResult
	manager.mu.Unlock()
	if result != nil {
		return *result, nil
	}
	return ServiceLogs{
		Service: request.Service,
		Records: []ServiceLogRecord{{Timestamp: 100, Severity: "info", Message: "bounded result"}},
	}, nil
}

func (manager *fakeServiceController) Action(
	ctx context.Context,
	request ServiceActionRequest,
) (ServiceActionResult, error) {
	manager.mu.Lock()
	manager.actionRequest = request
	manager.actionCalls++
	started := manager.actionStarted
	blocked := manager.blockAction
	result := manager.actionResult
	manager.mu.Unlock()
	if started != nil {
		select {
		case <-started:
		default:
			close(started)
		}
	}
	if blocked {
		<-ctx.Done()
		return ServiceActionResult{}, &ServiceManagerError{Code: "request_canceled"}
	}
	return result, nil
}

func TestServiceLogsRespectInvocationOutputLimit(t *testing.T) {
	manager := newFakeServiceController()
	manager.logsResult = &ServiceLogs{
		Service: "vpn",
		Records: []ServiceLogRecord{
			{Timestamp: 100, Message: strings.Repeat("a", 80)},
			{Timestamp: 101, Message: strings.Repeat("b", 80)},
		},
	}
	runtime := newServiceTestRuntime(t, manager)
	plan := testRuntimePlan(
		t,
		runtime,
		"service.logs.v1",
		json.RawMessage(`{"service":"vpn","entries":4,"since_seconds":60}`),
	)
	handler := runtime.handlers["service.logs.v1"].(*serviceCommandHandler)
	result, err := handler.execute(t.Context(), commandInvocation{
		Plan: plan, Input: plan.Input, TimeoutSeconds: plan.TimeoutSeconds,
		OutputLimitBytes: 96,
	})
	if err != nil {
		t.Fatal(err)
	}
	logs, ok := result.(ServiceLogs)
	if !ok || !logs.Truncated || len(logs.Records) != 0 {
		t.Fatalf("bounded service logs = %#v", result)
	}
	encoded, err := json.Marshal(logs)
	if err != nil || len(encoded) > 96 {
		t.Fatalf("bounded service logs bytes = %d, error %v", len(encoded), err)
	}
	if _, err := boundServiceLogs(*manager.logsResult, 1); err == nil {
		t.Fatal("service logs fit an impossible output limit")
	}
}

func TestRuntimeExecutesTargetBoundServiceCommands(t *testing.T) {
	manager := newFakeServiceController()
	runtime := newServiceTestRuntime(t, manager)

	status, err := runtime.Invoke(
		t.Context(),
		testRuntimePlan(t, runtime, "service.status.v1", json.RawMessage(`{"service":"vpn"}`)),
	)
	if err != nil || !strings.Contains(string(status), `"active_state":"active"`) {
		t.Fatalf("status result = %s, error %v", status, err)
	}
	logsPlan := testRuntimePlan(
		t,
		runtime,
		"service.logs.v1",
		json.RawMessage(`{"service":"vpn","entries":4,"since_seconds":60}`),
	)
	if _, prepareErr := runtime.handlers["service.logs.v1"].(*serviceCommandHandler).prepare(
		logsPlan,
	); prepareErr != nil {
		t.Fatalf("prepare logs request: %v", prepareErr)
	}
	logs, err := runtime.Invoke(t.Context(), logsPlan)
	if err != nil || !strings.Contains(string(logs), `"message":"bounded result"`) {
		t.Fatalf("logs result = %s, error %v", logs, err)
	}
	actionPlan := testRuntimePlan(
		t,
		runtime,
		"service.action.v1",
		json.RawMessage(`{"service":"vpn","action":"restart"}`),
	)
	first, err := runtime.Invoke(t.Context(), actionPlan)
	if err != nil || !strings.Contains(string(first), `"state":"completed"`) {
		t.Fatalf("action result = %s, error %v", first, err)
	}
	second, err := runtime.Invoke(t.Context(), actionPlan)
	if err != nil || string(first) != string(second) {
		t.Fatalf("action replay result = %s, error %v", second, err)
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if manager.statusRequest.Profile != "server-services" ||
		manager.logsRequest.Profile != "server-services" ||
		manager.actionRequest.Profile != "server-services" ||
		manager.actionCalls != 1 {
		t.Fatalf("service manager requests = %#v %#v %#v, action calls %d",
			manager.statusRequest,
			manager.logsRequest,
			manager.actionRequest,
			manager.actionCalls,
		)
	}
}

func TestRuntimePersistsUnknownServiceActionWithoutReplay(t *testing.T) {
	manager := newFakeServiceController()
	manager.actionResult = ServiceActionResult{
		Service: "vpn", Action: nodes.ServiceActionRestart,
		State: "unknown", AcceptedAt: 100, Code: "helper_response_lost",
	}
	runtime := newServiceTestRuntime(t, manager)
	plan := testRuntimePlan(
		t,
		runtime,
		"service.action.v1",
		json.RawMessage(`{"service":"vpn","action":"restart"}`),
	)
	if _, err := runtime.Invoke(t.Context(), plan); !errors.Is(err, ErrInvocationOutcomeUnknown) {
		t.Fatalf("first unknown action error = %v", err)
	}
	if _, err := runtime.Invoke(t.Context(), plan); !errors.Is(err, ErrInvocationOutcomeUnknown) {
		t.Fatalf("replayed unknown action error = %v", err)
	}
	record, found, err := runtime.Invocation(plan.InvocationID)
	if err != nil || !found || record.State != nodes.InvocationUnknown {
		t.Fatalf("unknown action record = %#v, found %v, error %v", record, found, err)
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if manager.actionCalls != 1 {
		t.Fatalf("uncertain action executions = %d", manager.actionCalls)
	}
}

func TestRuntimePreservesUnknownAfterAcceptedActionOutputFailure(t *testing.T) {
	manager := newFakeServiceController()
	runtime := newServiceTestRuntime(t, manager)
	minimum, err := json.Marshal(ServiceActionResult{
		Service: "vpn", Action: nodes.ServiceActionRestart,
		State: "unknown", AcceptedAt: 1, Code: "unknown",
	})
	if err != nil {
		t.Fatal(err)
	}
	completed, err := json.Marshal(manager.actionResult)
	if err != nil || len(completed) <= len(minimum) {
		t.Fatalf("action output fixture sizes = (%d, %d), error %v", len(minimum), len(completed), err)
	}
	plan := testRuntimePlanAtWithOutputLimit(
		t,
		runtime,
		"service.action.v1",
		json.RawMessage(`{"service":"vpn","action":"restart"}`),
		time.Now(),
		time.Minute,
		len(minimum),
	)
	if _, err := runtime.Invoke(t.Context(), plan); !errors.Is(err, ErrInvocationOutcomeUnknown) {
		t.Fatalf("accepted action output error = %v", err)
	}
	if _, err := runtime.Invoke(t.Context(), plan); !errors.Is(err, ErrInvocationOutcomeUnknown) {
		t.Fatalf("duplicate accepted action output error = %v", err)
	}
	record, found, err := runtime.Invocation(plan.InvocationID)
	if err != nil || !found || record.State != nodes.InvocationUnknown {
		t.Fatalf("accepted action output record = %#v, found %v, error %v", record, found, err)
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if manager.actionCalls != 1 {
		t.Fatalf("accepted action output executions = %d", manager.actionCalls)
	}
}

func TestRuntimeRejectsImpossibleActionOutputBeforeController(t *testing.T) {
	manager := newFakeServiceController()
	runtime := newServiceTestRuntime(t, manager)
	plan := testRuntimePlanAtWithOutputLimit(
		t,
		runtime,
		"service.action.v1",
		json.RawMessage(`{"service":"vpn","action":"restart"}`),
		time.Now(),
		time.Minute,
		1,
	)
	if _, err := runtime.Invoke(t.Context(), plan); err == nil || errors.Is(err, ErrInvocationOutcomeUnknown) {
		t.Fatalf("impossible action output error = %v", err)
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if manager.actionCalls != 0 {
		t.Fatalf("impossible action output reached controller: %d", manager.actionCalls)
	}
}

func TestRuntimeConfirmsPreAcceptanceServiceCancellation(t *testing.T) {
	manager := newFakeServiceController()
	manager.blockAction = true
	manager.actionStarted = make(chan struct{})
	runtime := newServiceTestRuntime(t, manager)
	plan := testRuntimePlan(
		t,
		runtime,
		"service.action.v1",
		json.RawMessage(`{"service":"vpn","action":"restart"}`),
	)
	done := make(chan error, 1)
	go func() {
		_, err := runtime.Invoke(t.Context(), plan)
		done <- err
	}()
	select {
	case <-manager.actionStarted:
	case <-time.After(time.Second):
		t.Fatal("service action did not start")
	}
	if _, err := runtime.Cancel(nodes.InvocationCancelRequest{InvocationID: plan.InvocationID}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, ErrInvocationCanceled) {
			t.Fatalf("canceled service action error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("service cancellation did not finish")
	}
}

func TestRuntimeDoesNotAdvertiseLocallyDeniedServiceCapability(t *testing.T) {
	manager := newFakeServiceController()
	policy := testRuntimePolicy(nil)
	runtime, err := NewRuntime(
		nodes.ID("node_test"),
		"test",
		policy,
		newMemoryInvocationLedger(),
		WithServiceManager(manager),
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, descriptor := range runtime.Catalog().Commands {
		if nodes.IsServiceCommand(descriptor.Name) {
			t.Fatalf("locally denied service command advertised: %s", descriptor.Name)
		}
	}
}

func newFakeServiceController() *fakeServiceController {
	manager := &fakeServiceController{descriptors: serviceRuntimeDescriptors()}
	manager.actionResult = ServiceActionResult{
		Service: "vpn", Action: nodes.ServiceActionRestart, State: "completed", AcceptedAt: 100,
		Status: func() *ServiceStatus {
			status := serviceRuntimeStatus("vpn")
			return &status
		}(),
	}
	return manager
}

func newServiceTestRuntime(t *testing.T, manager ServiceManager) *Runtime {
	t.Helper()
	policy := testRuntimePolicy([]string{
		"service.status.v1", "service.logs.v1", "service.action.v1",
	})
	policy.MaximumRisk = nodes.RiskPrivileged
	runtime, err := NewRuntime(
		nodes.ID("node_test"),
		"test",
		policy,
		newMemoryInvocationLedger(),
		WithServiceManager(manager),
	)
	if err != nil {
		t.Fatal(err)
	}
	return runtime
}

func serviceRuntimeDescriptors() []nodes.CommandDescriptor {
	profile := nodes.ServiceProfileDescriptor{
		Alias: "server-services", Revision: "server-services-v1", Manager: "systemd",
		Services: []nodes.ServiceDescriptor{{
			Alias: "vpn", Status: true, Logs: true,
			Actions: []nodes.ServiceAction{nodes.ServiceActionRestart},
		}},
		LogLimits:      nodes.ServiceLogLimits{EntriesMax: 100, BytesMax: 4096, AgeSecondsMax: 3600},
		ActionApproval: "required",
	}
	descriptors := make([]nodes.CommandDescriptor, 0, 3)
	for _, command := range []string{"service.status.v1", "service.logs.v1", "service.action.v1"} {
		projected := profile
		projected.Services = nodes.CloneServiceProfileDescriptors([]nodes.ServiceProfileDescriptor{profile})[0].Services
		service := &projected.Services[0]
		switch command {
		case "service.status.v1":
			service.Logs = false
			service.Actions = nil
		case "service.logs.v1":
			service.Status = false
			service.Actions = nil
		case "service.action.v1":
			service.Status = false
			service.Logs = false
		}
		risk := nodes.RiskRead
		approval := ""
		if command == "service.action.v1" {
			risk = nodes.RiskPrivileged
			approval = "each_command"
		}
		descriptors = append(descriptors, nodes.CommandDescriptor{
			Name: command,
			InputSchema: nodes.ServiceCommandInputSchema(
				command,
				[]nodes.ServiceProfileDescriptor{projected},
			),
			OutputSchema:     nodes.ServiceCommandOutputSchema(command),
			Risk:             risk,
			SupportsCancel:   true,
			SupportsProgress: true,
			ModelContract: &nodes.CommandModelContract{
				Availability: nodes.ModelUnavailable, TimeoutSecondsMax: 30,
				OutputBytesMax: 4096, ResultKind: "json",
				AuthorityDigest: strings.Repeat("a", 64), ApprovalMode: approval,
				Guidance: []string{}, Examples: []json.RawMessage{},
			},
			ServiceProfiles: []nodes.ServiceProfileDescriptor{projected},
		})
	}
	return descriptors
}

func serviceRuntimeStatus(service string) ServiceStatus {
	return ServiceStatus{
		Service: service, LoadState: "loaded", ActiveState: "active",
		Substate: "running", Enabled: "enabled", ObservedAt: 100,
	}
}
