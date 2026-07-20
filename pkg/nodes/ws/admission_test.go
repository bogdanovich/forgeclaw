package ws

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/sipeed/picoclaw/pkg/nodes"
	"github.com/sipeed/picoclaw/pkg/nodes/protocol"
)

func TestAdmissionPersistsSignedIdentityOverWSS(t *testing.T) {
	registry, handler := testAdmissionHandler(t, false)
	server := httptest.NewTLSServer(handler)
	defer server.Close()
	transport, ok := server.Client().Transport.(*http.Transport)
	if !ok {
		t.Fatalf("test server transport = %T", server.Client().Transport)
	}
	dialer := websocket.Dialer{TLSClientConfig: transport.TLSClientConfig.Clone()}
	connection, handshakeResponse, err := dialer.Dial("wss"+strings.TrimPrefix(server.URL, "https"), nil)
	if handshakeResponse != nil && handshakeResponse.Body != nil {
		defer handshakeResponse.Body.Close()
	}
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()

	challenge := readChallenge(t, connection)
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := nodes.NewIdentityProof(
		privateKey, challenge.Nonce, nodes.ProtocolV1, nodes.ProtocolV1,
		"v0.1.0", "linux", "amd64", nodes.CapabilityCatalog{},
	)
	if err != nil {
		t.Fatal(err)
	}
	params, err := json.Marshal(proof)
	if err != nil {
		t.Fatal(err)
	}
	requestData, err := protocol.Encode(protocol.Envelope{
		Type:   protocol.FrameRequest,
		ID:     "req_auth",
		Method: "node.authenticate",
		Params: params,
	})
	if err != nil {
		t.Fatal(err)
	}
	if writeErr := connection.WriteMessage(websocket.TextMessage, requestData); writeErr != nil {
		t.Fatal(writeErr)
	}
	_, responseData, err := connection.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	response, err := protocol.Decode(responseData)
	if err != nil {
		t.Fatal(err)
	}
	if response.OK == nil || !*response.OK {
		t.Fatalf("authentication response = %#v", response)
	}
	var result nodes.AdmissionResult
	if unmarshalErr := json.Unmarshal(response.Result, &result); unmarshalErr != nil {
		t.Fatal(unmarshalErr)
	}
	if result.NodeID != proof.NodeID || result.State != nodes.StatePendingPairing {
		t.Fatalf("authentication result = %#v", result)
	}
	pending, exists, err := registry.Pending(proof.NodeID)
	if err != nil || !exists || pending.Node.State != nodes.StatePendingPairing {
		t.Fatalf("Pending() = %#v, exists %v, error %v", pending, exists, err)
	}
}

func TestAdmissionRejectsPlaintextByDefault(t *testing.T) {
	_, handler := testAdmissionHandler(t, false)
	server := httptest.NewServer(handler)
	defer server.Close()
	connection, response, err := websocket.DefaultDialer.Dial(
		"ws"+strings.TrimPrefix(server.URL, "http"), nil,
	)
	if response != nil && response.Body != nil {
		defer response.Body.Close()
	}
	if connection != nil {
		connection.Close()
	}
	if err == nil {
		t.Fatal("plaintext WebSocket admission succeeded")
	}
	if response == nil || response.StatusCode != http.StatusUpgradeRequired {
		t.Fatalf("response = %#v", response)
	}
}

func TestAdmissionAllowsExplicitLoopbackDevelopment(t *testing.T) {
	_, handler := testAdmissionHandler(t, true)
	server := httptest.NewServer(handler)
	defer server.Close()
	connection, response, err := websocket.DefaultDialer.Dial(
		"ws"+strings.TrimPrefix(server.URL, "http"), nil,
	)
	if response != nil && response.Body != nil {
		defer response.Body.Close()
	}
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	challenge := readChallenge(t, connection)
	if challenge.Nonce == "" {
		t.Fatal("development connection received empty challenge")
	}
}

func TestAdmissionHeartbeatDisconnectsRevokedLiveSession(t *testing.T) {
	registry, handler := testAdmissionHandlerWithConfig(t, AdmissionConfig{
		HeartbeatPeriod: 10 * time.Millisecond,
		LivenessTimeout: 100 * time.Millisecond,
	})
	server := httptest.NewTLSServer(handler)
	defer server.Close()
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	pendingConnection := dialTestAdmission(t, server)
	pending := authenticateTestConnection(t, pendingConnection, privateKey)
	_ = pendingConnection.Close()
	if _, err := registry.Approve(pending.NodeID, nodes.PairingApproval{At: time.Now().Unix()}); err != nil {
		t.Fatal(err)
	}

	activeConnection := dialTestAdmission(t, server)
	connected := authenticateTestConnection(t, activeConnection, privateKey)
	if connected.State != nodes.StateConnected {
		t.Fatalf("approved admission state = %q", connected.State)
	}
	if _, err := registry.Revoke(connected.NodeID, nodes.Revocation{
		Reason: "test revocation",
		At:     time.Now().Unix(),
	}); err != nil {
		t.Fatal(err)
	}
	_ = activeConnection.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, err := activeConnection.ReadMessage(); err == nil {
		t.Fatal("revoked live session remained connected after heartbeat")
	}
	registration, exists, err := registry.Registration(connected.NodeID)
	if err != nil || !exists {
		t.Fatalf("Registration() = exists %v, error %v", exists, err)
	}
	if registration.Snapshot.State != nodes.StateRevoked {
		t.Fatalf("revoked node state = %q", registration.Snapshot.State)
	}
}

func TestAdmissionDoesNotTrustForwardedProtoFromRemotePeer(t *testing.T) {
	_, handler := testAdmissionHandler(t, false)
	request := httptest.NewRequest(http.MethodGet, "http://gateway.example/nodes/v1/ws", nil)
	request.RemoteAddr = "192.0.2.20:12345"
	request.Header.Set("X-Forwarded-Proto", "https")
	if handler.secureRequest(request) {
		t.Fatal("remote peer spoofed secure transport with X-Forwarded-Proto")
	}
}

func TestAdmissionDoesNotTrustForwardedProtoFromLoopbackPeer(t *testing.T) {
	_, handler := testAdmissionHandler(t, false)
	request := httptest.NewRequest(http.MethodGet, "http://gateway.example/nodes/v1/ws", nil)
	request.RemoteAddr = "127.0.0.1:12345"
	request.Header.Set("X-Forwarded-Proto", "https")
	if handler.secureRequest(request) {
		t.Fatal("loopback peer spoofed secure transport with X-Forwarded-Proto")
	}
}

func testAdmissionHandler(t *testing.T, allowPlaintext bool) (*nodes.FileRegistry, *AdmissionHandler) {
	t.Helper()
	return testAdmissionHandlerWithConfig(t, AdmissionConfig{
		AllowLoopbackPlaintext: allowPlaintext,
	})
}

func testAdmissionHandlerWithConfig(
	t *testing.T,
	cfg AdmissionConfig,
) (*nodes.FileRegistry, *AdmissionHandler) {
	t.Helper()
	registry, err := nodes.NewFileRegistry(filepath.Join(t.TempDir(), "registry.json"), 4)
	if err != nil {
		t.Fatal(err)
	}
	authenticator, err := nodes.NewAuthenticator(registry, nodes.AdmissionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewAdmissionHandler(authenticator, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return registry, handler
}

func dialTestAdmission(t *testing.T, server *httptest.Server) *websocket.Conn {
	t.Helper()
	transport, ok := server.Client().Transport.(*http.Transport)
	if !ok {
		t.Fatalf("test server transport = %T", server.Client().Transport)
	}
	dialer := websocket.Dialer{TLSClientConfig: transport.TLSClientConfig.Clone()}
	connection, response, err := dialer.Dial(
		"wss"+strings.TrimPrefix(server.URL, "https"),
		nil,
	)
	if response != nil && response.Body != nil {
		defer response.Body.Close()
	}
	if err != nil {
		t.Fatal(err)
	}
	return connection
}

func authenticateTestConnection(
	t *testing.T,
	connection *websocket.Conn,
	privateKey ed25519.PrivateKey,
) nodes.AdmissionResult {
	t.Helper()
	challenge := readChallenge(t, connection)
	proof, err := nodes.NewIdentityProof(
		privateKey, challenge.Nonce, nodes.ProtocolV1, nodes.ProtocolV1,
		"v0.1.0", "linux", "amd64", nodes.CapabilityCatalog{},
	)
	if err != nil {
		t.Fatal(err)
	}
	params, err := json.Marshal(proof)
	if err != nil {
		t.Fatal(err)
	}
	requestData, err := protocol.Encode(protocol.Envelope{
		Type:   protocol.FrameRequest,
		ID:     "req_auth",
		Method: "node.authenticate",
		Params: params,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := connection.WriteMessage(websocket.TextMessage, requestData); err != nil {
		t.Fatal(err)
	}
	_, responseData, err := connection.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	response, err := protocol.Decode(responseData)
	if err != nil {
		t.Fatal(err)
	}
	if response.OK == nil || !*response.OK {
		t.Fatalf("authentication response = %#v", response)
	}
	var result nodes.AdmissionResult
	if err := json.Unmarshal(response.Result, &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func readChallenge(t *testing.T, connection *websocket.Conn) nodes.Challenge {
	t.Helper()
	messageType, data, err := connection.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if messageType != websocket.TextMessage {
		t.Fatalf("challenge message type = %d", messageType)
	}
	envelope, err := protocol.Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	if envelope.Type != protocol.FrameEvent || envelope.Event != "node.challenge" {
		t.Fatalf("challenge envelope = %#v", envelope)
	}
	var challenge nodes.Challenge
	if err := json.Unmarshal(envelope.Payload, &challenge); err != nil {
		t.Fatal(err)
	}
	return challenge
}
