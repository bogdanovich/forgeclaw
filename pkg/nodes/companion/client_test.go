package companion

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"net/http"
	"net/http/httptest"
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
