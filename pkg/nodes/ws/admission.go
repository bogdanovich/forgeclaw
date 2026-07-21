package ws

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
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
	releaseTransport, trackErr := handler.sessions.TrackTransport(connection)
	if trackErr != nil {
		return
	}
	defer releaseTransport()
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
		handler.closeWithError(
			connection,
			websocket.CloseUnsupportedData,
			"text authentication frame required",
		)
		return
	}
	envelope, err := protocol.Decode(data)
	if err != nil || envelope.Type != protocol.FrameRequest ||
		envelope.Method != "node.authenticate" {
		handler.closeWithError(
			connection,
			websocket.ClosePolicyViolation,
			"invalid authentication request",
		)
		return
	}
	var proof nodes.IdentityProof
	decoder := json.NewDecoder(bytes.NewReader(envelope.Params))
	decoder.DisallowUnknownFields()
	if decodeErr := decoder.Decode(&proof); decodeErr != nil {
		handler.writeAdmissionError(
			connection,
			envelope.ID,
			"AUTH_INVALID",
			"invalid identity proof",
		)
		return
	}
	if trailingErr := decoder.Decode(new(any)); !errors.Is(trailingErr, io.EOF) {
		handler.writeAdmissionError(
			connection,
			envelope.ID,
			"AUTH_INVALID",
			"invalid identity proof",
		)
		return
	}
	if proof.Nonce != challenge.Nonce {
		handler.writeAdmissionError(connection, envelope.ID, "AUTH_INVALID", "challenge mismatch")
		return
	}
	admission, err := handler.authenticator.Authenticate(proof)
	if err != nil {
		handler.writeAdmissionError(
			connection,
			envelope.ID,
			"AUTH_FAILED",
			"identity verification failed",
		)
		return
	}
	result := admission.Result
	var release func() (bool, error)
	var session *peer
	if result.State == nodes.StateConnected {
		session = newPeer(connection)
		release, err = handler.sessions.Claim(
			result.NodeID,
			session,
			func() error { return handler.authenticator.Connect(admission) },
			func() error {
				return handler.authenticator.Disconnect(
					result.NodeID,
					"transport connection closed",
				)
			},
		)
		if err != nil {
			handler.writeAdmissionError(
				connection,
				envelope.ID,
				"SESSION_UNAVAILABLE",
				"node session unavailable",
			)
			return
		}
	}
	if session != nil {
		defer session.Close()
	}
	responseData, err := json.Marshal(result)
	if err != nil {
		handler.releaseSession(result.NodeID, release)
		return
	}
	ok := true
	writeResponse := handler.writeEnvelope
	if session != nil {
		writeResponse = func(_ *websocket.Conn, envelope protocol.Envelope) error {
			writeCtx, cancel := context.WithDeadline(request.Context(), deadline)
			defer cancel()
			return session.writeEnvelope(writeCtx, envelope)
		}
	}
	if writeErr := writeResponse(connection, protocol.Envelope{
		Type:   protocol.FrameResponse,
		ID:     envelope.ID,
		OK:     &ok,
		Result: responseData,
	}); writeErr != nil || result.State != nodes.StateConnected {
		if writeErr != nil {
			handler.releaseSession(result.NodeID, release)
		}
		return
	}
	if err := handler.prepareSession(session, result.NodeID); err != nil {
		handler.releaseSession(result.NodeID, release)
		return
	}
	session.markReady()
	handler.serveSession(session, result.NodeID, release)
}

// Close terminates all generations that share this handler's session hub.
// Gateway reloads intentionally keep the hub alive; shutdown closes it.
func (handler *AdmissionHandler) Close(ctx context.Context) error {
	return handler.sessions.Close(ctx)
}

func (handler *AdmissionHandler) serveSession(
	session *peer,
	nodeID nodes.ID,
	release func() (bool, error),
) {
	defer handler.releaseSession(nodeID, release)
	connection := session.connection

	done := make(chan struct{})
	go handler.sendHeartbeats(session, done)
	defer close(done)

	for {
		messageType, data, err := connection.ReadMessage()
		if err != nil {
			return
		}
		if messageType == websocket.BinaryMessage {
			_ = session.writeControl(websocket.CloseMessage, websocket.FormatCloseMessage(
				websocket.CloseUnsupportedData, "node admission: text command frame required",
			), time.Now().Add(time.Second))
			return
		}
		if messageType == websocket.TextMessage {
			envelope, decodeErr := protocol.Decode(data)
			if decodeErr != nil || envelope.Type != protocol.FrameResponse ||
				session.handleResponse(envelope) != nil {
				_ = session.writeControl(websocket.CloseMessage, websocket.FormatCloseMessage(
					websocket.ClosePolicyViolation, "node admission: unexpected command response",
				), time.Now().Add(time.Second))
				return
			}
		}
	}
}

func (handler *AdmissionHandler) prepareSession(session *peer, nodeID nodes.ID) error {
	connection := session.connection
	if err := connection.SetReadDeadline(time.Now().Add(handler.livenessTimeout)); err != nil {
		return err
	}
	if err := connection.SetWriteDeadline(time.Time{}); err != nil {
		return err
	}
	connection.SetPongHandler(func(string) error {
		if err := handler.authenticator.Heartbeat(nodeID); err != nil {
			return err
		}
		return connection.SetReadDeadline(time.Now().Add(handler.livenessTimeout))
	})
	return nil
}

// Invoke checks the durable pairing command surface and dispatches a prepared
// plan. Agent approval and durable invocation records are added before this
// transport boundary is exposed to tools.
func (handler *AdmissionHandler) Invoke(
	ctx context.Context,
	nodeID nodes.ID,
	plan nodes.ExecutionPlan,
) (json.RawMessage, error) {
	approval, err := handler.authenticator.ApprovedCommand(nodeID, plan.Command)
	if err != nil {
		return nil, err
	}
	if validationErr := plan.Validate(); validationErr != nil {
		return nil, validationErr
	}
	if plan.NodeID != nodeID || plan.Risk != approval.Descriptor.Risk ||
		plan.CatalogHash != approval.CatalogHash {
		return nil, fmt.Errorf(
			"%w: execution plan does not match approved command",
			nodes.ErrCommandDenied,
		)
	}
	params, err := json.Marshal(plan)
	if err != nil {
		return nil, fmt.Errorf("encode node execution plan: %w", err)
	}
	response, err := handler.sessions.RequestWithIdempotencyKey(
		ctx,
		nodeID,
		"node.invoke",
		params,
		plan.IdempotencyKey,
	)
	if err != nil {
		return nil, err
	}
	if response.OK == nil {
		return nil, errors.New("node returned a malformed invocation response")
	}
	if !*response.OK {
		return nil, fmt.Errorf(
			"node invocation failed (%s): %s",
			response.Error.Code,
			response.Error.Message,
		)
	}
	return validateInvocationResult(approval.Descriptor, plan, response.Result)
}

// Invocation returns the companion's durable record for reconnect recovery.
// It never retries or replays the command.
func (handler *AdmissionHandler) Invocation(
	ctx context.Context,
	nodeID nodes.ID,
	invocationID string,
) (nodes.InvocationRecord, error) {
	query := nodes.InvocationQuery{InvocationID: invocationID}
	if err := query.Validate(); err != nil {
		return nodes.InvocationRecord{}, err
	}
	params, err := json.Marshal(query)
	if err != nil {
		return nodes.InvocationRecord{}, fmt.Errorf("encode invocation query: %w", err)
	}
	response, err := handler.sessions.Request(ctx, nodeID, "node.invoke.get", params)
	if err != nil {
		return nodes.InvocationRecord{}, err
	}
	if response.OK == nil {
		return nodes.InvocationRecord{}, errors.New("node returned a malformed invocation query response")
	}
	if !*response.OK {
		return nodes.InvocationRecord{}, fmt.Errorf(
			"node invocation query failed (%s): %s",
			response.Error.Code,
			response.Error.Message,
		)
	}
	var record nodes.InvocationRecord
	decoder := json.NewDecoder(bytes.NewReader(response.Result))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&record); err != nil {
		return nodes.InvocationRecord{}, fmt.Errorf("decode invocation query result: %w", err)
	}
	if trailingErr := decoder.Decode(new(any)); !errors.Is(trailingErr, io.EOF) {
		return nodes.InvocationRecord{}, errors.New("decode invocation query result: trailing data")
	}
	if err := record.Validate(); err != nil {
		return nodes.InvocationRecord{}, err
	}
	if record.NodeID != nodeID || record.InvocationID != invocationID {
		return nodes.InvocationRecord{}, errors.New("node returned an unrelated invocation record")
	}
	return record, nil
}

func validateInvocationResult(
	descriptor nodes.CommandDescriptor,
	plan nodes.ExecutionPlan,
	result json.RawMessage,
) (json.RawMessage, error) {
	return nodes.ValidateInvocationOutput(descriptor, result, plan.OutputLimitBytes)
}

func (handler *AdmissionHandler) releaseSession(
	nodeID nodes.ID,
	release func() (bool, error),
) {
	if release != nil {
		if _, err := release(); err != nil {
			slog.Warn("persist node disconnect", "node_id", nodeID, "error", err)
		}
	}
}

func (handler *AdmissionHandler) sendHeartbeats(session *peer, done <-chan struct{}) {
	ticker := time.NewTicker(handler.heartbeatPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case now := <-ticker.C:
			if err := session.writeControl(
				websocket.PingMessage,
				[]byte(now.UTC().Format(time.RFC3339Nano)),
				now.Add(handler.heartbeatPeriod),
			); err != nil {
				_ = session.Close()
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
