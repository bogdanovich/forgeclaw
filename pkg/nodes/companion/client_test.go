package companion

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		admission.ServeHTTP(writer, request)
	}))
	defer server.Close()
	client := testClientForServer(t, server, testIdentity(t), ReconnectConfig{PendingDelaySeconds: 1})
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

func testConnectProxy(t *testing.T, backendAddress string) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	requests := &atomic.Int32{}
	proxy := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
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
	}))
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
