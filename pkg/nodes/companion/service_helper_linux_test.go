//go:build linux

package companion

import (
	"context"
	"errors"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/nodes"
)

type testServiceHelperIdentity struct {
	mu      sync.Mutex
	allowed bool
	actions []string
}

func (identity *testServiceHelperIdentity) Authorize(_ int32, action string) bool {
	identity.mu.Lock()
	defer identity.mu.Unlock()
	identity.actions = append(identity.actions, action)
	return identity.allowed
}

func (*testServiceHelperIdentity) Close() error { return nil }

type testServiceHelperManager struct {
	descriptors []nodes.CommandDescriptor
}

func (manager *testServiceHelperManager) Descriptors() []nodes.CommandDescriptor {
	return cloneCatalog(nodes.CapabilityCatalog{Commands: manager.descriptors}).Commands
}

func (*testServiceHelperManager) Status(
	context.Context,
	ServiceStatusRequest,
) (ServiceStatus, error) {
	return ServiceStatus{
		Service: "vpn", LoadState: "loaded", ActiveState: "active",
		Substate: "running", Enabled: "enabled", ObservedAt: 1,
	}, nil
}

func (*testServiceHelperManager) Logs(
	context.Context,
	ServiceLogRequest,
) (ServiceLogs, error) {
	return ServiceLogs{Service: "vpn", Records: []ServiceLogRecord{}, Truncated: false}, nil
}

func (manager *testServiceHelperManager) Action(
	ctx context.Context,
	request ServiceActionRequest,
) (ServiceActionResult, error) {
	return manager.executeAction(ctx, request, func() bool { return true })
}

func (*testServiceHelperManager) executeAction(
	_ context.Context,
	request ServiceActionRequest,
	accept serviceActionAcceptor,
) (ServiceActionResult, error) {
	if !accept() {
		return ServiceActionResult{
			Service: request.Service, Action: request.Action,
			State: "canceled", Code: "canceled_before_acceptance",
		}, nil
	}
	status := ServiceStatus{
		Service: "vpn", LoadState: "loaded", ActiveState: "active",
		Substate: "running", Enabled: "enabled", ObservedAt: 2,
	}
	return ServiceActionResult{
		Service: request.Service, Action: request.Action,
		State: "completed", AcceptedAt: 1, Status: &status,
	}, nil
}

func TestServiceHelperClientServerRoundTrip(t *testing.T) {
	config := serviceHelperLinuxConfigFixture(t)
	descriptors, err := config.Descriptors()
	if err != nil {
		t.Fatal(err)
	}
	identity := &testServiceHelperIdentity{allowed: true}
	server, err := newServiceHelperServer(
		config,
		&testServiceHelperManager{descriptors: descriptors},
		identity,
		time.Now,
	)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: config.SocketPath, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	listener.SetUnlinkOnClose(true)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, listener) }()
	client, err := newServiceHelperClient(
		t.Context(),
		config.SocketPath,
		uint32(os.Geteuid()),
		uint32(os.Getegid()),
	)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	projected := client.Descriptors()
	projected[0].ServiceProfiles[0].Services[0].Alias = "mutated"
	if client.Descriptors()[0].ServiceProfiles[0].Services[0].Alias != "vpn" {
		t.Fatal("service helper descriptor caller mutated retained snapshot")
	}
	t.Cleanup(func() {
		_ = client.Close()
		cancel()
		select {
		case serveErr := <-done:
			if serveErr != nil {
				t.Errorf("service helper server shutdown: %v", serveErr)
			}
		case <-time.After(5 * time.Second):
			t.Error("service helper server did not stop")
		}
	})
	status, err := client.Status(t.Context(), ServiceStatusRequest{
		Profile: "server-services", Service: "vpn",
	})
	if err != nil || status.ActiveState != "active" {
		t.Fatalf("helper status = %#v, error %v", status, err)
	}
	logs, err := client.Logs(t.Context(), ServiceLogRequest{
		Profile: "server-services", Service: "vpn", Entries: 1, SinceSeconds: 1,
	})
	if err != nil || logs.Service != "vpn" || logs.Records == nil {
		t.Fatalf("helper logs = %#v, error %v", logs, err)
	}
	action, err := client.Action(t.Context(), ServiceActionRequest{
		Profile: "server-services", Service: "vpn", Action: nodes.ServiceActionRestart,
	})
	if err != nil || action.State != "completed" || action.Status == nil {
		t.Fatalf("helper action = %#v, error %v", action, err)
	}
	identity.mu.Lock()
	actions := append([]string(nil), identity.actions...)
	identity.mu.Unlock()
	if len(actions) != 4 || actions[0] != authorityBrokerActionSnapshot {
		t.Fatalf("service helper peer actions = %#v", actions)
	}
}

func TestServiceHelperClientRejectsStaleSnapshotAndWrongAction(t *testing.T) {
	client, stop := testServiceHelperClient(t)
	defer stop()
	_, err := client.Action(t.Context(), ServiceActionRequest{
		Profile: "server-services", Service: "vpn", Action: nodes.ServiceActionStop,
	})
	var managerErr *ServiceManagerError
	if !errors.As(err, &managerErr) || managerErr.Code != "command_denied" {
		t.Fatalf("wrong service action error = %v", err)
	}
	client.snapshotDigest = strings.Repeat("b", 64)
	_, err = client.Status(t.Context(), ServiceStatusRequest{
		Profile: "server-services", Service: "vpn",
	})
	if !errors.As(err, &managerErr) || managerErr.Code != "helper_denied" {
		t.Fatalf("stale service helper snapshot error = %v", err)
	}
}

func TestServiceHelperServerRejectsWrongPeerBoundary(t *testing.T) {
	config := serviceHelperLinuxConfigFixture(t)
	descriptors, err := config.Descriptors()
	if err != nil {
		t.Fatal(err)
	}
	server, err := newServiceHelperServer(
		config,
		&testServiceHelperManager{descriptors: descriptors},
		&testServiceHelperIdentity{allowed: false},
		time.Now,
	)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: config.SocketPath, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	listener.SetUnlinkOnClose(true)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, listener) }()
	_, err = newServiceHelperClient(
		t.Context(), config.SocketPath, uint32(os.Geteuid()), uint32(os.Getegid()),
	)
	if err == nil {
		cancel()
		t.Fatal("service helper accepted a peer outside its cgroup identity")
	}
	cancel()
	if serveErr := <-done; serveErr != nil {
		t.Fatal(serveErr)
	}
}

func TestServiceHelperClientReportsUnboundActionResponseAsUnknown(t *testing.T) {
	tests := []struct {
		name    string
		respond func(net.Conn, serviceHelperRequest) error
	}{
		{name: "lost", respond: func(net.Conn, serviceHelperRequest) error { return nil }},
		{name: "wrong request", respond: func(connection net.Conn, request serviceHelperRequest) error {
			return writeServiceHelperResponse(connection, completedServiceActionResponse(request.RequestID+"x"))
		}},
		{name: "wrong kind", respond: func(connection net.Conn, request serviceHelperRequest) error {
			status := activeServiceStatus()
			return writeServiceHelperResponse(connection, serviceHelperResponse{
				Version: ServiceHelperProtocolVersion, Kind: serviceHelperRequestStatus,
				RequestID: request.RequestID, Status: &status,
			})
		}},
		{name: "invalid proof", respond: func(connection net.Conn, request serviceHelperRequest) error {
			response := completedServiceActionResponse(request.RequestID)
			response.Action.Status = nil
			return writeAuthorityBrokerFrame(connection, response)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := serviceHelperLinuxConfigFixture(t)
			snapshot, err := newServiceHelperSnapshot(config)
			if err != nil {
				t.Fatal(err)
			}
			listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: config.SocketPath, Net: "unix"})
			if err != nil {
				t.Fatal(err)
			}
			listener.SetUnlinkOnClose(true)
			done := make(chan error, 1)
			go serveServiceHelperActionResponse(listener, snapshot, test.respond, done)
			client, err := newServiceHelperClient(
				t.Context(), config.SocketPath, uint32(os.Geteuid()), uint32(os.Getegid()),
			)
			if err != nil {
				t.Fatal(err)
			}
			result, err := client.Action(t.Context(), ServiceActionRequest{
				Profile: "server-services", Service: "vpn", Action: nodes.ServiceActionRestart,
			})
			if err != nil || result.State != "unknown" || result.Code != "helper_response_lost" {
				t.Fatalf("unbound helper action response = %#v, error %v", result, err)
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func serveServiceHelperActionResponse(
	listener *net.UnixListener,
	snapshot serviceHelperSnapshot,
	respond func(net.Conn, serviceHelperRequest) error,
	done chan<- error,
) {
	defer listener.Close()
	connection, err := listener.AcceptUnix()
	if err == nil {
		var request serviceHelperRequest
		request, err = readServiceHelperRequest(connection)
		if err == nil {
			err = writeServiceHelperResponse(connection, serviceHelperResponse{
				Version: ServiceHelperProtocolVersion, Kind: serviceHelperRequestSnapshot,
				RequestID: request.RequestID, Snapshot: &snapshot,
			})
		}
		_ = connection.Close()
	}
	if err == nil {
		connection, err = listener.AcceptUnix()
	}
	if err == nil {
		var request serviceHelperRequest
		request, err = readServiceHelperRequest(connection)
		if err == nil && request.Kind != serviceHelperRequestAction {
			err = errors.New("unexpected service helper request")
		}
		if err == nil {
			err = respond(connection, request)
		}
		_ = connection.Close()
	}
	done <- err
}

func activeServiceStatus() ServiceStatus {
	return ServiceStatus{
		Service: "vpn", LoadState: "loaded", ActiveState: "active",
		Substate: "running", Enabled: "enabled", ObservedAt: 1,
	}
}

func completedServiceActionResponse(requestID string) serviceHelperResponse {
	status := activeServiceStatus()
	return serviceHelperResponse{
		Version: ServiceHelperProtocolVersion, Kind: serviceHelperRequestAction,
		RequestID: requestID,
		Action: &ServiceActionResult{
			Service: "vpn", Action: nodes.ServiceActionRestart,
			State: "completed", AcceptedAt: 1, Status: &status,
		},
	}
}

func TestServiceHelperCancellationBoundary(t *testing.T) {
	server := &serviceHelperServer{
		now:         time.Now,
		active:      make(map[string]*serviceHelperActiveRequest),
		preCanceled: make(map[string]int64),
	}
	preContext, preCancel := context.WithCancel(t.Context())
	pre, ok := server.register(
		"pre", time.Now().Add(time.Minute).Unix(), preCancel, serviceHelperRequestAction,
	)
	if !ok {
		t.Fatal("register pre-acceptance action")
	}
	server.cancel("pre", time.Now().Add(time.Minute).Unix())
	if preContext.Err() == nil || server.accept("pre", pre) {
		t.Fatal("pre-acceptance cancellation did not win")
	}
	postContext, postCancel := context.WithCancel(t.Context())
	post, ok := server.register(
		"post", time.Now().Add(time.Minute).Unix(), postCancel, serviceHelperRequestAction,
	)
	if !ok || !server.accept("post", post) {
		t.Fatal("accept post-acceptance action")
	}
	server.cancel("post", time.Now().Add(time.Minute).Unix())
	if postContext.Err() != nil {
		t.Fatal("post-acceptance cancellation interrupted mutation")
	}
	postCancel()
}

func testServiceHelperClient(t *testing.T) (*ServiceHelperClient, func()) {
	t.Helper()
	config := serviceHelperLinuxConfigFixture(t)
	descriptors, err := config.Descriptors()
	if err != nil {
		t.Fatal(err)
	}
	server, err := newServiceHelperServer(
		config,
		&testServiceHelperManager{descriptors: descriptors},
		&testServiceHelperIdentity{allowed: true},
		time.Now,
	)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: config.SocketPath, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	listener.SetUnlinkOnClose(true)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, listener) }()
	client, err := newServiceHelperClient(
		t.Context(), config.SocketPath, uint32(os.Geteuid()), uint32(os.Getegid()),
	)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	return client, func() {
		_ = client.Close()
		cancel()
		<-done
	}
}

func serviceHelperLinuxConfigFixture(t *testing.T) ServiceHelperServiceConfig {
	t.Helper()
	config := serviceHelperConfigFixture(t)
	config.SocketPath = t.TempDir() + "/service-helper.sock"
	config.AllowedUID = uint32(os.Geteuid())
	config.AllowedGID = uint32(os.Getegid())
	config.normalized = true
	return config
}
