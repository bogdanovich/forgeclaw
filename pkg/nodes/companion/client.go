package companion

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"net/http"
	"runtime"
	"time"

	"github.com/gorilla/websocket"

	"github.com/sipeed/picoclaw/pkg/nodes"
	"github.com/sipeed/picoclaw/pkg/nodes/protocol"
)

var ErrIncompatibleGateway = errors.New("node gateway protocol is incompatible")

const defaultStableSessionWindow = 30 * time.Second

type Client struct {
	config        Config
	identity      Identity
	clientVersion string
	catalog       nodes.CapabilityCatalog
	runtime       *Runtime
	logger        *slog.Logger
	dialer        websocket.Dialer
	stableWindow  time.Duration
}

func NewClient(
	cfg Config,
	identity Identity,
	clientVersion string,
	catalog nodes.CapabilityCatalog,
	logger *slog.Logger,
) (*Client, error) {
	return newClient(cfg, identity, clientVersion, catalog, nil, logger)
}

func NewClientWithRuntime(
	cfg Config,
	identity Identity,
	clientVersion string,
	runtime *Runtime,
	logger *slog.Logger,
) (*Client, error) {
	if runtime == nil {
		return nil, errors.New("node command runtime is required")
	}
	if runtime.nodeID != identity.ID {
		return nil, errors.New("node command runtime identity does not match client identity")
	}
	return newClient(cfg, identity, clientVersion, runtime.Catalog(), runtime, logger)
}

func newClient(
	cfg Config,
	identity Identity,
	clientVersion string,
	catalog nodes.CapabilityCatalog,
	commandRuntime *Runtime,
	logger *slog.Logger,
) (*Client, error) {
	if cfg.minReconnectDelay <= 0 || cfg.maxReconnectDelay < cfg.minReconnectDelay ||
		cfg.pendingRetryDelay <= 0 {
		return nil, errors.New("normalized node configuration is required")
	}
	if len(identity.PrivateKey) != ed25519.PrivateKeySize || identity.ID == "" {
		return nil, errors.New("valid node identity is required")
	}
	derivedIdentity, err := identityFromPrivateKey(identity.PrivateKey)
	if err != nil || derivedIdentity.ID != identity.ID {
		return nil, errors.New("node identity ID does not match its private key")
	}
	if clientVersion == "" || len(clientVersion) > nodes.MaxClientVersionLength {
		return nil, errors.New("valid node client version is required")
	}
	if catalogErr := catalog.Validate(); catalogErr != nil {
		return nil, catalogErr
	}
	if logger == nil {
		logger = slog.Default()
	}
	tlsConfig, err := buildTLSConfig(cfg.TLS)
	if err != nil {
		return nil, err
	}
	return &Client{
		config: cfg,
		identity: Identity{
			ID:         identity.ID,
			PrivateKey: append(ed25519.PrivateKey(nil), identity.PrivateKey...),
		},
		clientVersion: clientVersion,
		catalog:       cloneCatalog(catalog),
		runtime:       commandRuntime,
		logger:        logger,
		stableWindow:  defaultStableSessionWindow,
		dialer: websocket.Dialer{
			HandshakeTimeout: DefaultHandshakeTimeout,
			TLSClientConfig:  tlsConfig,
			Proxy:            http.ProxyFromEnvironment,
		},
	}, nil
}

func cloneCatalog(catalog nodes.CapabilityCatalog) nodes.CapabilityCatalog {
	result := nodes.CapabilityCatalog{
		Commands: append([]nodes.CommandDescriptor(nil), catalog.Commands...),
	}
	for index := range result.Commands {
		result.Commands[index].InputSchema = append(
			json.RawMessage(nil),
			catalog.Commands[index].InputSchema...,
		)
		result.Commands[index].OutputSchema = append(
			json.RawMessage(nil),
			catalog.Commands[index].OutputSchema...,
		)
	}
	return result
}

func (client *Client) Run(ctx context.Context) error {
	backoff := client.config.minReconnectDelay
	for {
		connection, result, err := client.connectAndAuthenticate(ctx)
		if err == nil {
			client.logger.Info(
				"node admission completed",
				"node_id",
				result.NodeID,
				"state",
				result.State,
			)
			if result.State == nodes.StatePendingPairing {
				backoff = client.config.minReconnectDelay
				_ = connection.Close()
				if waitErr := waitForContext(ctx, client.config.pendingRetryDelay); waitErr != nil {
					return normalizeRunExit(waitErr)
				}
				continue
			}
			connectedAt := time.Now()
			err = client.serveConnected(ctx, connection)
			if time.Since(connectedAt) >= client.stableWindow {
				backoff = client.config.minReconnectDelay
			}
		}
		if ctx.Err() != nil {
			return normalizeRunExit(ctx.Err())
		}
		client.logger.Warn("node admission failed", "node_id", client.identity.ID, "error", err)
		if waitErr := waitForContext(ctx, jitterDelay(backoff)); waitErr != nil {
			return normalizeRunExit(waitErr)
		}
		backoff = min(backoff*2, client.config.maxReconnectDelay)
	}
}

func (client *Client) Authenticate(ctx context.Context) (nodes.AdmissionResult, error) {
	connection, result, err := client.connectAndAuthenticate(ctx)
	if connection != nil {
		_ = connection.Close()
	}
	return result, err
}

func (client *Client) connectAndAuthenticate(
	ctx context.Context,
) (*websocket.Conn, nodes.AdmissionResult, error) {
	connection, response, err := client.dialer.DialContext(ctx, client.config.GatewayURL, nil)
	closeResponse(response)
	if err != nil {
		return nil, nodes.AdmissionResult{}, fmt.Errorf("connect to node gateway: %w", err)
	}
	connected := false
	defer func() {
		if !connected {
			_ = connection.Close()
		}
	}()
	stopContextClose := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer func() {
		if stopContextClose() {
			return
		}
		_ = connection.Close()
	}()
	connection.SetReadLimit(protocol.MaxFrameSize)
	deadline := time.Now().Add(DefaultHandshakeTimeout)
	if contextDeadline, ok := ctx.Deadline(); ok && contextDeadline.Before(deadline) {
		deadline = contextDeadline
	}
	if deadlineErr := connection.SetReadDeadline(deadline); deadlineErr != nil {
		return nil, nodes.AdmissionResult{}, fmt.Errorf(
			"set node handshake deadline: %w",
			deadlineErr,
		)
	}
	if deadlineErr := connection.SetWriteDeadline(deadline); deadlineErr != nil {
		return nil, nodes.AdmissionResult{}, fmt.Errorf(
			"set node handshake deadline: %w",
			deadlineErr,
		)
	}

	challenge, err := readChallenge(connection)
	if err != nil {
		return nil, nodes.AdmissionResult{}, err
	}
	proof, err := nodes.NewIdentityProof(
		client.identity.PrivateKey,
		challenge.Nonce,
		nodes.ProtocolV1,
		nodes.ProtocolV1,
		client.clientVersion,
		runtime.GOOS,
		runtime.GOARCH,
		client.catalog,
	)
	if err != nil {
		return nil, nodes.AdmissionResult{}, fmt.Errorf("create node identity proof: %w", err)
	}
	params, err := json.Marshal(proof)
	if err != nil {
		return nil, nodes.AdmissionResult{}, fmt.Errorf("encode node identity proof: %w", err)
	}
	requestID, err := randomRequestID()
	if err != nil {
		return nil, nodes.AdmissionResult{}, err
	}
	requestData, err := protocol.Encode(protocol.Envelope{
		Type:   protocol.FrameRequest,
		ID:     requestID,
		Method: "node.authenticate",
		Params: params,
	})
	if err != nil {
		return nil, nodes.AdmissionResult{}, err
	}
	if writeErr := connection.WriteMessage(websocket.TextMessage, requestData); writeErr != nil {
		return nil, nodes.AdmissionResult{}, fmt.Errorf("send node identity proof: %w", writeErr)
	}

	messageType, responseData, err := connection.ReadMessage()
	if err != nil {
		return nil, nodes.AdmissionResult{}, fmt.Errorf("read node admission result: %w", err)
	}
	if messageType != websocket.TextMessage {
		return nil, nodes.AdmissionResult{}, errors.New(
			"node gateway returned a non-text admission frame",
		)
	}
	envelope, err := protocol.Decode(responseData)
	if err != nil {
		return nil, nodes.AdmissionResult{}, err
	}
	if envelope.Type != protocol.FrameResponse || envelope.ID != requestID {
		return nil, nodes.AdmissionResult{}, errors.New(
			"node gateway returned an unrelated admission response",
		)
	}
	if envelope.OK == nil || !*envelope.OK {
		if envelope.Error == nil {
			return nil, nodes.AdmissionResult{}, errors.New("node gateway rejected admission")
		}
		return nil, nodes.AdmissionResult{}, fmt.Errorf(
			"node gateway rejected admission (%s): %s",
			envelope.Error.Code,
			envelope.Error.Message,
		)
	}
	var result nodes.AdmissionResult
	if err := decodeStrictJSON(envelope.Result, &result); err != nil {
		return nil, nodes.AdmissionResult{}, fmt.Errorf("decode node admission result: %w", err)
	}
	if result.NodeID != client.identity.ID ||
		(result.State != nodes.StatePendingPairing && result.State != nodes.StateConnected) {
		return nil, nodes.AdmissionResult{}, errors.New(
			"node gateway returned an invalid admission identity or state",
		)
	}
	connected = true
	return connection, result, nil
}

func (client *Client) serveConnected(ctx context.Context, connection *websocket.Conn) error {
	defer connection.Close()
	stopContextClose := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stopContextClose()
	if err := connection.SetWriteDeadline(time.Time{}); err != nil {
		return err
	}
	if err := connection.SetReadDeadline(time.Now().Add(DefaultGatewayLiveness)); err != nil {
		return err
	}
	connection.SetPingHandler(func(message string) error {
		if err := connection.SetReadDeadline(time.Now().Add(DefaultGatewayLiveness)); err != nil {
			return err
		}
		return connection.WriteControl(
			websocket.PongMessage,
			[]byte(message),
			time.Now().Add(DefaultHandshakeTimeout),
		)
	})
	for {
		messageType, data, err := connection.ReadMessage()
		if err != nil {
			return fmt.Errorf("node gateway session ended: %w", err)
		}
		if messageType == websocket.BinaryMessage {
			return errors.New("node gateway sent a non-text command frame")
		}
		if messageType == websocket.TextMessage {
			if requestErr := client.handleRequest(ctx, connection, data); requestErr != nil {
				return requestErr
			}
		}
	}
}

func (client *Client) handleRequest(
	ctx context.Context,
	connection *websocket.Conn,
	data []byte,
) error {
	envelope, decodeErr := protocol.Decode(data)
	if decodeErr != nil || envelope.Type != protocol.FrameRequest {
		return errors.New("node gateway sent an invalid command request")
	}
	switch envelope.Method {
	case "node.invoke":
		return client.handleInvoke(ctx, connection, envelope)
	case "node.invoke.get":
		return client.handleInvocationQuery(connection, envelope)
	default:
		return client.writeCommandError(
			connection,
			envelope.ID,
			"METHOD_NOT_FOUND",
			"unsupported node method",
		)
	}
}

func (client *Client) handleInvoke(
	ctx context.Context,
	connection *websocket.Conn,
	envelope protocol.Envelope,
) error {
	if client.runtime == nil {
		return client.writeCommandError(
			connection,
			envelope.ID,
			"COMMAND_UNAVAILABLE",
			"node command runtime is disabled",
		)
	}
	var plan nodes.ExecutionPlan
	if planErr := decodeStrictJSON(envelope.Params, &plan); planErr != nil {
		return client.writeCommandError(
			connection,
			envelope.ID,
			"INVALID_PLAN",
			"invalid execution plan",
		)
	}
	if envelope.IdempotencyKey == "" || envelope.IdempotencyKey != plan.IdempotencyKey {
		return client.writeCommandError(
			connection,
			envelope.ID,
			"INVALID_PLAN",
			"invocation idempotency key mismatch",
		)
	}
	result, err := client.runtime.Invoke(ctx, plan)
	if err != nil {
		code := "EXECUTION_FAILED"
		message := "node command failed"
		switch {
		case errors.Is(err, nodes.ErrCommandDenied), errors.Is(err, nodes.ErrInvalidInvocation):
			code = "COMMAND_DENIED"
			message = "node command denied"
		case errors.Is(err, ErrInvocationConflict):
			code = "IDEMPOTENCY_CONFLICT"
			message = "invocation idempotency conflict"
		case errors.Is(err, ErrInvocationOutcomeUnknown):
			code = "INVOCATION_UNKNOWN"
			message = "invocation outcome is unknown"
		}
		return client.writeCommandError(connection, envelope.ID, code, message)
	}
	ok := true
	return writeEnvelope(connection, protocol.Envelope{
		Type:   protocol.FrameResponse,
		ID:     envelope.ID,
		OK:     &ok,
		Result: result,
	})
}

func (client *Client) handleInvocationQuery(
	connection *websocket.Conn,
	envelope protocol.Envelope,
) error {
	if client.runtime == nil {
		return client.writeCommandError(
			connection,
			envelope.ID,
			"COMMAND_UNAVAILABLE",
			"node command runtime is disabled",
		)
	}
	if envelope.IdempotencyKey != "" {
		return client.writeCommandError(
			connection,
			envelope.ID,
			"INVALID_QUERY",
			"invocation query cannot carry an idempotency key",
		)
	}
	var query nodes.InvocationQuery
	if err := decodeStrictJSON(envelope.Params, &query); err != nil || query.Validate() != nil {
		return client.writeCommandError(
			connection,
			envelope.ID,
			"INVALID_QUERY",
			"invalid invocation query",
		)
	}
	record, found, lookupErr := client.runtime.Invocation(query.InvocationID)
	if lookupErr != nil {
		return client.writeCommandError(
			connection,
			envelope.ID,
			"LEDGER_UNAVAILABLE",
			"invocation ledger is unavailable",
		)
	}
	if !found {
		return client.writeCommandError(
			connection,
			envelope.ID,
			"INVOCATION_NOT_FOUND",
			"invocation record not found",
		)
	}
	result, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode invocation query result: %w", err)
	}
	ok := true
	return writeEnvelope(connection, protocol.Envelope{
		Type:   protocol.FrameResponse,
		ID:     envelope.ID,
		OK:     &ok,
		Result: result,
	})
}

func (client *Client) writeCommandError(
	connection *websocket.Conn,
	requestID, code, message string,
) error {
	ok := false
	return writeEnvelope(connection, protocol.Envelope{
		Type: protocol.FrameResponse,
		ID:   requestID,
		OK:   &ok,
		Error: &protocol.Error{
			Code:    code,
			Message: message,
		},
	})
}

func writeEnvelope(connection *websocket.Conn, envelope protocol.Envelope) error {
	data, err := protocol.Encode(envelope)
	if err != nil {
		return err
	}
	return connection.WriteMessage(websocket.TextMessage, data)
}

func readChallenge(connection *websocket.Conn) (nodes.Challenge, error) {
	messageType, data, err := connection.ReadMessage()
	if err != nil {
		return nodes.Challenge{}, fmt.Errorf("read node admission challenge: %w", err)
	}
	if messageType != websocket.TextMessage {
		return nodes.Challenge{}, errors.New("node gateway returned a non-text challenge frame")
	}
	envelope, err := protocol.Decode(data)
	if err != nil {
		return nodes.Challenge{}, err
	}
	if envelope.Type != protocol.FrameEvent || envelope.Event != "node.challenge" {
		return nodes.Challenge{}, errors.New("node gateway returned an unexpected challenge frame")
	}
	var challenge nodes.Challenge
	if err := decodeStrictJSON(envelope.Payload, &challenge); err != nil {
		return nodes.Challenge{}, fmt.Errorf("decode node admission challenge: %w", err)
	}
	nonce, nonceErr := base64.RawURLEncoding.DecodeString(challenge.Nonce)
	if nonceErr != nil || len(nonce) != 32 {
		return nodes.Challenge{}, errors.New("node gateway returned a malformed admission nonce")
	}
	if challenge.MinProtocol > nodes.ProtocolV1 || challenge.MaxProtocol < nodes.ProtocolV1 {
		return nodes.Challenge{}, ErrIncompatibleGateway
	}
	if challenge.ExpiresAt <= time.Now().Unix() {
		return nodes.Challenge{}, errors.New("node admission challenge is expired")
	}
	return challenge, nil
}

func randomRequestID() (string, error) {
	value := make([]byte, 12)
	if _, err := rand.Read(value); err != nil {
		return "", fmt.Errorf("generate node request ID: %w", err)
	}
	return "req_" + base64.RawURLEncoding.EncodeToString(value), nil
}

func waitForContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func normalizeRunExit(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return nil
	}
	return err
}

func jitterDelay(delay time.Duration) time.Duration {
	if delay <= 1 {
		return delay
	}
	span := delay / 2
	jitter, err := rand.Int(rand.Reader, big.NewInt(int64(span)+1))
	if err != nil {
		return delay
	}
	return span + time.Duration(jitter.Int64())
}

func closeResponse(response *http.Response) {
	if response != nil && response.Body != nil {
		response.Body.Close()
	}
}
