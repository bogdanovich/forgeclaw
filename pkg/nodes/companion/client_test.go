package companion

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sipeed/picoclaw/pkg/nodes"
	nodews "github.com/sipeed/picoclaw/pkg/nodes/ws"
)

func TestClientAuthenticatesPinnedWSSIdentity(t *testing.T) {
	registry, handler := testGatewayAdmission(t)
	server := httptest.NewTLSServer(handler)
	defer server.Close()
	identity := testIdentity(t)
	client := testClientForServer(t, server, identity, ReconnectConfig{})

	result, err := client.Authenticate(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if result.NodeID != identity.ID || result.State != nodes.StatePendingPairing {
		t.Fatalf("Authenticate() = %#v", result)
	}
	pending, exists, err := registry.Pending(identity.ID)
	if err != nil || !exists || pending.Node.ID != identity.ID {
		t.Fatalf("Pending() = %#v, exists %v, error %v", pending, exists, err)
	}
}

func TestClientReconnectsAfterPendingAdmission(t *testing.T) {
	_, admission := testGatewayAdmission(t)
	var requests atomic.Int32
	server := httptest.NewTLSServer(
		http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			requests.Add(1)
			admission.ServeHTTP(writer, request)
		}),
	)
	defer server.Close()
	client := testClientForServer(
		t,
		server,
		testIdentity(t),
		ReconnectConfig{PendingDelaySeconds: 1},
	)
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- client.Run(ctx) }()
	for requests.Load() < 2 {
		select {
		case err := <-done:
			t.Fatalf("Run() stopped before reconnect: %v", err)
		case <-time.After(25 * time.Millisecond):
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run() error = %v", err)
	}
}

func TestClientKeepsApprovedSessionUntilContextCancellation(t *testing.T) {
	registry, admission := testGatewayAdmission(t)
	server := httptest.NewTLSServer(admission)
	defer server.Close()
	identity := testIdentity(t)
	client := testClientForServer(t, server, identity, ReconnectConfig{})

	result, err := client.Authenticate(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Approve(result.NodeID, nodes.PairingApproval{At: time.Now().Unix()}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- client.Run(ctx) }()
	waitForNodeState(t, registry, identity.ID, nodes.StateConnected)
	select {
	case err := <-done:
		t.Fatalf("Run() stopped while approved session was live: %v", err)
	default:
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run() cancellation error = %v", err)
	}
	waitForNodeState(t, registry, identity.ID, nodes.StateDisconnected)
}

func TestClientExecutesCorrelatedInvocationOverAuthenticatedSession(t *testing.T) {
	registry, admission := testGatewayAdmission(t)
	server := httptest.NewTLSServer(admission)
	defer server.Close()
	identity := testIdentity(t)
	policy := testRuntimePolicy([]string{"node.info.v1"})
	ledgerPath := filepath.Join(t.TempDir(), "invocations.json")
	ledger, err := NewFileInvocationLedger(ledgerPath, 8, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ledger.Close)
	commandRuntime, err := NewRuntime(identity.ID, "test", policy, ledger)
	if err != nil {
		t.Fatal(err)
	}
	client := testRuntimeClientForServer(t, server, identity, commandRuntime)
	result, err := client.Authenticate(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, approveErr := registry.Approve(result.NodeID, nodes.PairingApproval{
		AllowedCommands: []string{"node.info.v1"},
		At:              time.Now().Unix(),
	}); approveErr != nil {
		t.Fatal(approveErr)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- client.Run(ctx) }()
	waitForNodeState(t, registry, identity.ID, nodes.StateConnected)

	registration, exists, err := registry.Registration(identity.ID)
	if err != nil || !exists {
		t.Fatalf("Registration() = exists %v, error %v", exists, err)
	}
	descriptor, err := registration.ApprovedCommand("node.info.v1")
	if err != nil {
		t.Fatal(err)
	}
	plan, err := nodes.PrepareExecutionPlan(nodes.InvocationRequest{
		InvocationID:     "inv_transport",
		IdempotencyKey:   "idem_transport",
		NodeID:           identity.ID,
		CatalogHash:      registration.Snapshot.CatalogHash,
		Command:          descriptor.Name,
		Input:            json.RawMessage(`{}`),
		AgentID:          "agent_test",
		SessionID:        "session_test",
		ActorID:          "actor_test",
		TimeoutSeconds:   5,
		OutputLimitBytes: 4096,
	}, descriptor, LocalExecutor, policy.Revision, time.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	output, err := admission.Invoke(t.Context(), identity.ID, plan)
	if err != nil {
		t.Fatal(err)
	}
	var info struct {
		NodeID nodes.ID `json:"node_id"`
	}
	if decodeErr := json.Unmarshal(output, &info); decodeErr != nil {
		t.Fatal(decodeErr)
	}
	if info.NodeID != identity.ID {
		t.Fatalf("node.info node_id = %q", info.NodeID)
	}
	cancel()
	if runErr := <-done; runErr != nil {
		t.Fatal(runErr)
	}
	waitForNodeState(t, registry, identity.ID, nodes.StateDisconnected)

	ledger.Close()
	reloadedLedger, err := NewFileInvocationLedger(ledgerPath, 8, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reloadedLedger.Close)
	reloadedRuntime, err := NewRuntime(identity.ID, "test", policy, reloadedLedger)
	if err != nil {
		t.Fatal(err)
	}
	reconnectedClient := testRuntimeClientForServer(t, server, identity, reloadedRuntime)
	reconnectCtx, reconnectCancel := context.WithCancel(t.Context())
	reconnectDone := make(chan error, 1)
	go func() { reconnectDone <- reconnectedClient.Run(reconnectCtx) }()
	waitForNodeState(t, registry, identity.ID, nodes.StateConnected)

	record, err := admission.Invocation(t.Context(), identity.ID, plan.InvocationID)
	if err != nil {
		t.Fatal(err)
	}
	if record.State != nodes.InvocationSucceeded || record.PlanHash != plan.PlanHash ||
		string(record.Result) != string(output) {
		t.Fatalf("durable invocation record = %#v", record)
	}
	reconnectCancel()
	if runErr := <-reconnectDone; runErr != nil {
		t.Fatal(runErr)
	}
}

func TestClientDispatchesInvocationsConcurrentlyAndServesQueries(t *testing.T) {
	registry, admission := testGatewayAdmission(t)
	server := httptest.NewTLSServer(admission)
	defer server.Close()
	identity := testIdentity(t)
	policy := testRuntimePolicy([]string{"test.block.v1"})
	commandRuntime, err := NewRuntime(identity.ID, "test", policy, newMemoryInvocationLedger())
	if err != nil {
		t.Fatal(err)
	}
	handler := newBlockingHandler()
	descriptor := handler.descriptor()
	commandRuntime.handlers[descriptor.Name] = handler
	commandRuntime.catalog.Commands = append(commandRuntime.catalog.Commands, descriptor)
	client := testRuntimeClientForServer(t, server, identity, commandRuntime)
	result, err := client.Authenticate(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, approveErr := registry.Approve(result.NodeID, nodes.PairingApproval{
		AllowedCommands: []string{descriptor.Name},
		At:              time.Now().Unix(),
	}); approveErr != nil {
		t.Fatal(approveErr)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- client.Run(ctx) }()
	waitForNodeState(t, registry, identity.ID, nodes.StateConnected)

	first := testTransportPlan(t, commandRuntime, descriptor, "first")
	second := testTransportPlan(t, commandRuntime, descriptor, "second")
	results := make(chan error, 2)
	for _, plan := range []nodes.ExecutionPlan{first, second} {
		go func() {
			_, invokeErr := admission.Invoke(t.Context(), identity.ID, plan)
			results <- invokeErr
		}()
	}
	for range 2 {
		select {
		case <-handler.started:
		case <-time.After(3 * time.Second):
			t.Fatal("concurrent invocation did not start")
		}
	}

	queryCtx, queryCancel := context.WithTimeout(t.Context(), time.Second)
	record, err := admission.Invocation(queryCtx, identity.ID, first.InvocationID)
	queryCancel()
	if err != nil {
		t.Fatalf("query running invocation: %v", err)
	}
	if record.State != nodes.InvocationRunning {
		t.Fatalf("running invocation state = %q", record.State)
	}
	close(handler.release)
	for range 2 {
		if invokeErr := <-results; invokeErr != nil {
			t.Fatalf("Invoke() error = %v", invokeErr)
		}
	}
	cancel()
	if runErr := <-done; runErr != nil {
		t.Fatal(runErr)
	}
}

func TestClientCancelsInvocationOverAuthenticatedSession(t *testing.T) {
	registry, admission := testGatewayAdmission(t)
	server := httptest.NewTLSServer(admission)
	defer server.Close()
	identity := testIdentity(t)
	policy := testRuntimePolicy([]string{"test.block.v1"})
	commandRuntime, err := NewRuntime(identity.ID, "test", policy, newMemoryInvocationLedger())
	if err != nil {
		t.Fatal(err)
	}
	handler := newBlockingHandler()
	descriptor := handler.descriptor()
	commandRuntime.handlers[descriptor.Name] = handler
	commandRuntime.catalog.Commands = append(commandRuntime.catalog.Commands, descriptor)
	client := testRuntimeClientForServer(t, server, identity, commandRuntime)
	result, err := client.Authenticate(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, approveErr := registry.Approve(result.NodeID, nodes.PairingApproval{
		AllowedCommands: []string{descriptor.Name},
		At:              time.Now().Unix(),
	}); approveErr != nil {
		t.Fatal(approveErr)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- client.Run(ctx) }()
	waitForNodeState(t, registry, identity.ID, nodes.StateConnected)

	plan := testTransportPlan(t, commandRuntime, descriptor, "cancel")
	invokeDone := make(chan error, 1)
	go func() {
		_, invokeErr := admission.Invoke(t.Context(), identity.ID, plan)
		invokeDone <- invokeErr
	}()
	select {
	case <-handler.started:
	case <-time.After(3 * time.Second):
		t.Fatal("invocation did not start")
	}
	record, err := admission.CancelInvocation(t.Context(), identity.ID, plan.InvocationID)
	if err != nil {
		t.Fatal(err)
	}
	if record.State != nodes.InvocationRunning || record.Cancellation == nil ||
		record.Cancellation.TerminationConfirmed {
		t.Fatalf("cancellation acknowledgement = %#v", record)
	}
	if invokeErr := <-invokeDone; invokeErr == nil ||
		!strings.Contains(invokeErr.Error(), "INVOCATION_CANCELED") {
		t.Fatalf("canceled Invoke() error = %v", invokeErr)
	}
	record, err = admission.Invocation(t.Context(), identity.ID, plan.InvocationID)
	if err != nil {
		t.Fatal(err)
	}
	if record.State != nodes.InvocationCanceled || record.Cancellation == nil ||
		!record.Cancellation.TerminationConfirmed {
		t.Fatalf("terminal cancellation record = %#v", record)
	}
	cancel()
	if runErr := <-done; runErr != nil {
		t.Fatal(runErr)
	}
}

func TestRuntimeConcurrentDuplicateExecutesOnce(t *testing.T) {
	commandRuntime, err := NewRuntime(
		nodes.ID("node_test"),
		"test",
		testRuntimePolicy([]string{"test.block.v1"}),
		newMemoryInvocationLedger(),
	)
	if err != nil {
		t.Fatal(err)
	}
	handler := newBlockingHandler()
	descriptor := handler.descriptor()
	commandRuntime.handlers[descriptor.Name] = handler
	commandRuntime.catalog.Commands = append(commandRuntime.catalog.Commands, descriptor)
	plan := testTransportPlan(t, commandRuntime, descriptor, "duplicate")
	results := make(chan error, 2)
	for range 2 {
		go func() {
			_, invokeErr := commandRuntime.Invoke(t.Context(), plan)
			results <- invokeErr
		}()
	}
	select {
	case <-handler.started:
	case <-time.After(time.Second):
		t.Fatal("invocation did not start")
	}
	select {
	case <-handler.started:
		t.Fatal("duplicate invocation executed concurrently")
	case <-time.After(50 * time.Millisecond):
	}
	close(handler.release)
	var successes, unknown int
	for range 2 {
		switch invokeErr := <-results; {
		case invokeErr == nil:
			successes++
		case errors.Is(invokeErr, ErrInvocationOutcomeUnknown):
			unknown++
		default:
			t.Fatalf("duplicate Invoke() error = %v", invokeErr)
		}
	}
	if successes != 1 || unknown != 1 || handler.executions.Load() != 1 {
		t.Fatalf(
			"duplicate results: successes=%d unknown=%d executions=%d",
			successes,
			unknown,
			handler.executions.Load(),
		)
	}
}

type blockingHandler struct {
	started    chan struct{}
	release    chan struct{}
	executions atomic.Int32
}

func newBlockingHandler() *blockingHandler {
	return &blockingHandler{
		started: make(chan struct{}, maxConcurrentInvocations),
		release: make(chan struct{}),
	}
}

func (*blockingHandler) descriptor() nodes.CommandDescriptor {
	return nodes.CommandDescriptor{
		Name:        "test.block.v1",
		InputSchema: json.RawMessage(`{"type":"object","additionalProperties":false}`),
		OutputSchema: json.RawMessage(
			`{"type":"object","required":["ok"],"properties":{"ok":{"type":"boolean"}},"additionalProperties":false}`,
		),
		Risk:           nodes.RiskRead,
		SupportsCancel: true,
	}
}

func (handler *blockingHandler) execute(ctx context.Context, _ commandInvocation) (any, error) {
	handler.executions.Add(1)
	handler.started <- struct{}{}
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("%w: %v", errCommandCancellationConfirmed, ctx.Err())
	case <-handler.release:
		return map[string]bool{"ok": true}, nil
	}
}

func testTransportPlan(
	t *testing.T,
	commandRuntime *Runtime,
	descriptor nodes.CommandDescriptor,
	suffix string,
) nodes.ExecutionPlan {
	t.Helper()
	catalogHash, err := commandRuntime.Catalog().Hash()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := nodes.PrepareExecutionPlan(nodes.InvocationRequest{
		InvocationID:     "inv_" + suffix,
		IdempotencyKey:   "idem_" + suffix,
		NodeID:           commandRuntime.nodeID,
		CatalogHash:      catalogHash,
		Command:          descriptor.Name,
		Input:            json.RawMessage(`{}`),
		AgentID:          "agent_test",
		SessionID:        "session_test",
		ActorID:          "actor_test",
		TimeoutSeconds:   5,
		OutputLimitBytes: 4096,
	}, descriptor, LocalExecutor, commandRuntime.policy.Revision, time.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func TestDuplicateCompanionsBackOffInsteadOfRapidlyFlapping(t *testing.T) {
	registry, admission := testGatewayAdmission(t)
	var requests atomic.Int32
	server := httptest.NewTLSServer(
		http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			requests.Add(1)
			admission.ServeHTTP(writer, request)
		}),
	)
	defer server.Close()
	identity := testIdentity(t)
	bootstrap := testClientForServer(t, server, identity, ReconnectConfig{})
	result, err := bootstrap.Authenticate(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Approve(result.NodeID, nodes.PairingApproval{At: time.Now().Unix()}); err != nil {
		t.Fatal(err)
	}

	first := testClientForServer(t, server, identity, ReconnectConfig{})
	second := testClientForServer(t, server, identity, ReconnectConfig{})
	for _, client := range []*Client{first, second} {
		client.config.minReconnectDelay = 5 * time.Millisecond
		client.config.maxReconnectDelay = 80 * time.Millisecond
		client.stableWindow = time.Second
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 2)
	go func() { done <- first.Run(ctx) }()
	waitForNodeState(t, registry, identity.ID, nodes.StateConnected)
	go func() { done <- second.Run(ctx) }()
	time.Sleep(400 * time.Millisecond)
	cancel()
	for range 2 {
		if err := <-done; err != nil {
			t.Fatalf("Run() error = %v", err)
		}
	}
	closeCtx, closeCancel := context.WithTimeout(t.Context(), time.Second)
	defer closeCancel()
	if err := admission.Close(closeCtx); err != nil {
		t.Fatalf("close admission: %v", err)
	}
	if count := requests.Load(); count < 4 || count > 30 {
		t.Fatalf("duplicate companion admission requests = %d", count)
	}
}

func TestClientRejectsWrongCertificatePin(t *testing.T) {
	_, handler := testGatewayAdmission(t)
	server := httptest.NewTLSServer(handler)
	defer server.Close()
	identity := testIdentity(t)
	cfg, err := (Config{
		GatewayURL: strings.Replace(server.URL, "https://", "wss://", 1) + GatewayPath,
		StateDir:   filepath.Dir(filepath.Join(t.TempDir(), "state")),
		TLS:        TLSConfig{CertificateSHA256: strings.Repeat("00", sha256.Size)},
	}).Normalize(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(
		cfg,
		identity,
		"test",
		nodes.CapabilityCatalog{},
		slog.New(slog.DiscardHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Authenticate(t.Context()); err == nil {
		t.Fatal("Authenticate() accepted the wrong gateway certificate pin")
	}
}

func TestClientAuthenticatesThroughHTTPConnectProxy(t *testing.T) {
	_, handler := testGatewayAdmission(t)
	backend := httptest.NewTLSServer(handler)
	defer backend.Close()
	proxy, requests := testConnectProxy(t, backend.Listener.Addr().String())
	defer proxy.Close()
	client := testClientForServer(t, backend, testIdentity(t), ReconnectConfig{})
	if client.dialer.Proxy == nil {
		t.Fatal("node WebSocket dialer does not preserve environment proxy support")
	}
	proxyURL, err := url.Parse(proxy.URL)
	if err != nil {
		t.Fatal(err)
	}
	client.dialer.Proxy = http.ProxyURL(proxyURL)
	if _, err := client.Authenticate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if requests.Load() != 1 {
		t.Fatalf("CONNECT proxy requests = %d", requests.Load())
	}
}

func testGatewayAdmission(t *testing.T) (*nodes.FileRegistry, *nodews.AdmissionHandler) {
	t.Helper()
	registry, err := nodes.NewFileRegistry(filepath.Join(t.TempDir(), "registry.json"), 8)
	if err != nil {
		t.Fatal(err)
	}
	authenticator, err := nodes.NewAuthenticator(registry, nodes.AdmissionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := nodews.NewAdmissionHandler(authenticator, nodews.AdmissionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	return registry, handler
}

func testIdentity(t *testing.T) Identity {
	t.Helper()
	identity, err := LoadOrCreateIdentity(filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	return identity
}

func testClientForServer(
	t *testing.T,
	server *httptest.Server,
	identity Identity,
	reconnect ReconnectConfig,
) *Client {
	t.Helper()
	fingerprint := sha256.Sum256(server.Certificate().Raw)
	cfg, err := (Config{
		GatewayURL: strings.Replace(server.URL, "https://", "wss://", 1) + GatewayPath,
		StateDir:   filepath.Join(t.TempDir(), "state"),
		TLS:        TLSConfig{CertificateSHA256: hex.EncodeToString(fingerprint[:])},
		Reconnect:  reconnect,
	}).Normalize(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(
		cfg,
		identity,
		"test",
		nodes.CapabilityCatalog{},
		slog.New(slog.DiscardHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func testRuntimeClientForServer(
	t *testing.T,
	server *httptest.Server,
	identity Identity,
	runtime *Runtime,
) *Client {
	t.Helper()
	fingerprint := sha256.Sum256(server.Certificate().Raw)
	cfg, err := (Config{
		GatewayURL: strings.Replace(server.URL, "https://", "wss://", 1) + GatewayPath,
		StateDir:   filepath.Join(t.TempDir(), "state"),
		TLS:        TLSConfig{CertificateSHA256: hex.EncodeToString(fingerprint[:])},
		Policy:     runtime.policy,
	}).Normalize(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewClientWithRuntime(
		cfg,
		identity,
		"test",
		runtime,
		slog.New(slog.DiscardHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func testConnectProxy(t *testing.T, backendAddress string) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	requests := &atomic.Int32{}
	proxy := httptest.NewServer(
		http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			if request.Method != http.MethodConnect {
				http.Error(writer, "CONNECT required", http.StatusMethodNotAllowed)
				return
			}
			backend, err := net.Dial("tcp", backendAddress)
			if err != nil {
				http.Error(writer, "backend unavailable", http.StatusBadGateway)
				return
			}
			hijacker, ok := writer.(http.Hijacker)
			if !ok {
				backend.Close()
				http.Error(writer, "hijacking unavailable", http.StatusInternalServerError)
				return
			}
			client, _, err := hijacker.Hijack()
			if err != nil {
				backend.Close()
				return
			}
			requests.Add(1)
			if _, err := fmt.Fprint(client, "HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
				client.Close()
				backend.Close()
				return
			}
			defer client.Close()
			defer backend.Close()
			copyDone := make(chan struct{})
			go func() {
				_, _ = io.Copy(backend, client)
				if connection, ok := backend.(*net.TCPConn); ok {
					_ = connection.CloseWrite()
				}
				close(copyDone)
			}()
			_, _ = io.Copy(client, backend)
			<-copyDone
		}),
	)
	return proxy, requests
}

func waitForNodeState(
	t *testing.T,
	registry *nodes.FileRegistry,
	id nodes.ID,
	want nodes.State,
) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		registration, exists, err := registry.Registration(id)
		if err != nil {
			t.Fatal(err)
		}
		if exists && registration.Snapshot.State == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	registration, exists, err := registry.Registration(id)
	t.Fatalf("node state = %#v, exists %v, error %v; want %q", registration, exists, err, want)
}
