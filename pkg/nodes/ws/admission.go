package ws

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/websocket"

	"github.com/sipeed/picoclaw/pkg/nodes"
	"github.com/sipeed/picoclaw/pkg/nodes/protocol"
)

const (
	Path                   = "/nodes/v1/ws"
	DefaultHandshakeWindow = 15 * time.Second
	DefaultHeartbeatPeriod = 20 * time.Second
	DefaultLivenessTimeout = 60 * time.Second
)

type AdmissionConfig struct {
	AllowLoopbackPlaintext bool
	HandshakeWindow        time.Duration
	HeartbeatPeriod        time.Duration
	LivenessTimeout        time.Duration
	Sessions               *SessionHub
}

type AdmissionHandler struct {
	authenticator          *nodes.Authenticator
	allowLoopbackPlaintext bool
	handshakeWindow        time.Duration
	heartbeatPeriod        time.Duration
	livenessTimeout        time.Duration
	sessions               *SessionHub
	upgrader               websocket.Upgrader
}

func NewAdmissionHandler(
	authenticator *nodes.Authenticator,
	cfg AdmissionConfig,
) (*AdmissionHandler, error) {
	if authenticator == nil {
		return nil, errors.New("node authenticator is required")
	}
	if cfg.HandshakeWindow <= 0 {
		cfg.HandshakeWindow = DefaultHandshakeWindow
	}
	if cfg.HeartbeatPeriod <= 0 {
		cfg.HeartbeatPeriod = DefaultHeartbeatPeriod
	}
	if cfg.LivenessTimeout <= 0 {
		cfg.LivenessTimeout = DefaultLivenessTimeout
	}
	if cfg.LivenessTimeout <= cfg.HeartbeatPeriod {
		return nil, errors.New("node liveness timeout must exceed heartbeat period")
	}
	if cfg.Sessions == nil {
		cfg.Sessions = NewSessionHub()
	}
	return &AdmissionHandler{
		authenticator:          authenticator,
		allowLoopbackPlaintext: cfg.AllowLoopbackPlaintext,
		handshakeWindow:        cfg.HandshakeWindow,
		heartbeatPeriod:        cfg.HeartbeatPeriod,
		livenessTimeout:        cfg.LivenessTimeout,
		sessions:               cfg.Sessions,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(*http.Request) bool { return true },
		},
	}, nil
}

func (handler *AdmissionHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writer.Header().Set("Allow", http.MethodGet)
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !handler.secureRequest(request) {
		http.Error(writer, "secure WebSocket transport required", http.StatusUpgradeRequired)
		return
	}
	connection, upgradeErr := handler.upgrader.Upgrade(writer, request, nil)
	if upgradeErr != nil {
		return
	}
	defer connection.Close()
	connection.SetReadLimit(protocol.MaxFrameSize)
	deadline := time.Now().Add(handler.handshakeWindow)
	if deadlineErr := connection.SetReadDeadline(deadline); deadlineErr != nil {
		return
	}
	if deadlineErr := connection.SetWriteDeadline(deadline); deadlineErr != nil {
		return
	}

	challenge, err := handler.authenticator.IssueChallenge()
	if err != nil {
		handler.closeWithError(connection, websocket.CloseTryAgainLater, "admission unavailable")
		return
	}
	defer handler.authenticator.DiscardChallenge(challenge.Nonce)
	challengePayload, err := json.Marshal(challenge)
	if err != nil || handler.writeEnvelope(connection, protocol.Envelope{
		Type:    protocol.FrameEvent,
		Event:   "node.challenge",
		Payload: challengePayload,
	}) != nil {
		return
	}

	messageType, data, err := connection.ReadMessage()
	if err != nil || messageType != websocket.TextMessage {
		handler.closeWithError(connection, websocket.CloseUnsupportedData, "text authentication frame required")
		return
	}
	envelope, err := protocol.Decode(data)
	if err != nil || envelope.Type != protocol.FrameRequest || envelope.Method != "node.authenticate" {
		handler.closeWithError(connection, websocket.ClosePolicyViolation, "invalid authentication request")
		return
	}
	var proof nodes.IdentityProof
	decoder := json.NewDecoder(bytes.NewReader(envelope.Params))
	decoder.DisallowUnknownFields()
	if decodeErr := decoder.Decode(&proof); decodeErr != nil {
		handler.writeAdmissionError(connection, envelope.ID, "AUTH_INVALID", "invalid identity proof")
		return
	}
	if trailingErr := decoder.Decode(new(any)); !errors.Is(trailingErr, io.EOF) {
		handler.writeAdmissionError(connection, envelope.ID, "AUTH_INVALID", "invalid identity proof")
		return
	}
	if proof.Nonce != challenge.Nonce {
		handler.writeAdmissionError(connection, envelope.ID, "AUTH_INVALID", "challenge mismatch")
		return
	}
	admission, err := handler.authenticator.Authenticate(proof)
	if err != nil {
		handler.writeAdmissionError(connection, envelope.ID, "AUTH_FAILED", "identity verification failed")
		return
	}
	result := admission.Result
	var release func() bool
	if result.State == nodes.StateConnected {
		release, err = handler.sessions.Claim(result.NodeID, connection, func() error {
			return handler.authenticator.Connect(admission)
		})
		if err != nil {
			handler.writeAdmissionError(connection, envelope.ID, "SESSION_UNAVAILABLE", "node session unavailable")
			return
		}
	}
	responseData, err := json.Marshal(result)
	if err != nil {
		handler.releaseSession(result.NodeID, release, "encode admission response failed")
		return
	}
	ok := true
	if writeErr := handler.writeEnvelope(connection, protocol.Envelope{
		Type:   protocol.FrameResponse,
		ID:     envelope.ID,
		OK:     &ok,
		Result: responseData,
	}); writeErr != nil || result.State != nodes.StateConnected {
		if writeErr != nil {
			handler.releaseSession(result.NodeID, release, "send admission response failed")
		}
		return
	}
	handler.serveSession(connection, result.NodeID, release)
}

// Close terminates all generations that share this handler's session hub.
// Gateway reloads intentionally keep the hub alive; shutdown closes it.
func (handler *AdmissionHandler) Close() {
	handler.sessions.Close()
}

func (handler *AdmissionHandler) serveSession(
	connection *websocket.Conn,
	nodeID nodes.ID,
	release func() bool,
) {
	if err := connection.SetReadDeadline(time.Now().Add(handler.livenessTimeout)); err != nil {
		return
	}
	if err := connection.SetWriteDeadline(time.Time{}); err != nil {
		return
	}
	connection.SetPongHandler(func(string) error {
		if err := handler.authenticator.Heartbeat(nodeID); err != nil {
			return err
		}
		return connection.SetReadDeadline(time.Now().Add(handler.livenessTimeout))
	})

	done := make(chan struct{})
	go handler.sendHeartbeats(connection, done)
	defer close(done)
	defer handler.releaseSession(nodeID, release, "transport connection closed")

	for {
		messageType, _, err := connection.ReadMessage()
		if err != nil {
			return
		}
		if messageType == websocket.TextMessage || messageType == websocket.BinaryMessage {
			handler.closeWithError(connection, websocket.ClosePolicyViolation, "command protocol is not enabled")
			return
		}
	}
}

func (handler *AdmissionHandler) releaseSession(
	nodeID nodes.ID,
	release func() bool,
	reason string,
) {
	if release != nil && release() {
		_ = handler.authenticator.Disconnect(nodeID, reason)
	}
}

func (handler *AdmissionHandler) sendHeartbeats(connection *websocket.Conn, done <-chan struct{}) {
	ticker := time.NewTicker(handler.heartbeatPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case now := <-ticker.C:
			if err := connection.WriteControl(
				websocket.PingMessage,
				[]byte(now.UTC().Format(time.RFC3339Nano)),
				now.Add(handler.heartbeatPeriod),
			); err != nil {
				_ = connection.Close()
				return
			}
		}
	}
}

func (handler *AdmissionHandler) secureRequest(request *http.Request) bool {
	if request.TLS != nil {
		return true
	}
	remoteHost, _, err := net.SplitHostPort(request.RemoteAddr)
	if err != nil {
		remoteHost = request.RemoteAddr
	}
	remoteIP := net.ParseIP(strings.Trim(remoteHost, "[]"))
	if remoteIP == nil || !remoteIP.IsLoopback() {
		return false
	}
	return handler.allowLoopbackPlaintext
}

func (handler *AdmissionHandler) writeAdmissionError(
	connection *websocket.Conn,
	requestID, code, message string,
) {
	ok := false
	_ = handler.writeEnvelope(connection, protocol.Envelope{
		Type: protocol.FrameResponse,
		ID:   requestID,
		OK:   &ok,
		Error: &protocol.Error{
			Code:    code,
			Message: message,
		},
	})
}

func (handler *AdmissionHandler) writeEnvelope(
	connection *websocket.Conn,
	envelope protocol.Envelope,
) error {
	data, err := protocol.Encode(envelope)
	if err != nil {
		return err
	}
	return connection.WriteMessage(websocket.TextMessage, data)
}

func (handler *AdmissionHandler) closeWithError(
	connection *websocket.Conn,
	code int,
	message string,
) {
	_ = connection.WriteControl(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(code, fmt.Sprintf("node admission: %s", message)),
		time.Now().Add(time.Second),
	)
}
