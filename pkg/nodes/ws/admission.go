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
)

type AdmissionConfig struct {
	AllowLoopbackPlaintext bool
	HandshakeWindow        time.Duration
}

type AdmissionHandler struct {
	authenticator          *nodes.Authenticator
	allowLoopbackPlaintext bool
	handshakeWindow        time.Duration
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
	return &AdmissionHandler{
		authenticator:          authenticator,
		allowLoopbackPlaintext: cfg.AllowLoopbackPlaintext,
		handshakeWindow:        cfg.HandshakeWindow,
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
	result, err := handler.authenticator.Admit(proof)
	if err != nil {
		handler.writeAdmissionError(connection, envelope.ID, "AUTH_FAILED", "identity verification failed")
		return
	}
	responseData, err := json.Marshal(result)
	if err != nil {
		return
	}
	ok := true
	_ = handler.writeEnvelope(connection, protocol.Envelope{
		Type:   protocol.FrameResponse,
		ID:     envelope.ID,
		OK:     &ok,
		Result: responseData,
	})
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
