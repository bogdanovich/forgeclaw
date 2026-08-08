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
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/bogdanovich/mintclaw/pkg/nodes"
	"github.com/bogdanovich/mintclaw/pkg/nodes/protocol"
)

var ErrIncompatibleGateway = errors.New("node gateway protocol is incompatible")

const (
	defaultStableSessionWindow = 30 * time.Second
	maxConcurrentInvocations   = 16
)

type Client struct {
	config          Config
	identity        Identity
	clientVersion   string
	catalog         nodes.CapabilityCatalog
	runtime         *Runtime
	logger          *slog.Logger
	dialer          websocket.Dialer
	stableWindow    time.Duration
	invokeSlots     chan struct{}
	transferHandler TransferFrameHandler
	attachmentsMu   sync.Mutex
	attachments     map[string]*TerminalAttachment
}

type TransferFrameHandler interface {
	HandleTransferFrame(
		context.Context,
		protocol.TransferFrame,
		func(protocol.TransferFrame) error,
	) error
}

type connectedWorkers struct {
	requests sync.WaitGroup
	events   sync.WaitGroup
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

func NewClientWithRuntimeAndTransferHandler(
	cfg Config,
	identity Identity,
	clientVersion string,
	runtime *Runtime,
	handler TransferFrameHandler,
	logger *slog.Logger,
) (*Client, error) {
	if handler == nil {
		return nil, errors.New("node transfer frame handler is required")
	}
	client, err := NewClientWithRuntime(cfg, identity, clientVersion, runtime, logger)
	if err != nil {
		return nil, err
	}
	client.transferHandler = handler
	return client, nil
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
		invokeSlots:   make(chan struct{}, maxConcurrentInvocations),
		attachments:   make(map[string]*TerminalAttachment),
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
		result.Commands[index].ModelContract = cloneModelContract(
			catalog.Commands[index].ModelContract,
		)
		result.Commands[index].FileProfiles = cloneFileProfileDescriptors(
			catalog.Commands[index].FileProfiles,
		)
		result.Commands[index].ServiceProfiles = nodes.CloneServiceProfileDescriptors(
			catalog.Commands[index].ServiceProfiles,
		)
		result.Commands[index].UpdateProfiles = nodes.CloneUpdateProfileDescriptors(
			catalog.Commands[index].UpdateProfiles,
		)
	}
	return result
}

func cloneModelContract(
	contract *nodes.CommandModelContract,
) *nodes.CommandModelContract {
	if contract == nil {
		return nil
	}
	result := *contract
	result.Constraints.ExecutableAliases = append(
		[]string(nil),
		contract.Constraints.ExecutableAliases...,
	)
	result.Constraints.ProfileAliases = append(
		[]string(nil),
		contract.Constraints.ProfileAliases...,
	)
	result.Constraints.WorkingScopes = append(
		[]string(nil),
		contract.Constraints.WorkingScopes...,
	)
	result.Constraints.EnvironmentNames = append(
		[]string(nil),
		contract.Constraints.EnvironmentNames...,
	)
	result.Guidance = make([]string, len(contract.Guidance))
	copy(result.Guidance, contract.Guidance)
	result.Examples = make([]json.RawMessage, len(contract.Examples))
	for index := range contract.Examples {
		result.Examples[index] = append(json.RawMessage(nil), contract.Examples[index]...)
	}
	return &result
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
	proof, err := client.identityProof(challenge)
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

func (client *Client) identityProof(challenge nodes.Challenge) (nodes.IdentityProof, error) {
	if challenge.MinProtocol > nodes.ProtocolV1 || challenge.MaxProtocol < nodes.ProtocolV1 {
		return nodes.IdentityProof{}, ErrIncompatibleGateway
	}
	profile := nodes.ExecutionProfile{}
	if client.runtime != nil {
		profile = client.runtime.ExecutionProfile()
	}
	return nodes.NewIdentityProof(
		client.identity.PrivateKey,
		challenge.Nonce,
		nodes.ProtocolV1,
		nodes.ProtocolV1,
		client.clientVersion,
		runtime.GOOS,
		runtime.GOARCH,
		client.catalog,
		profile,
	)
}

func (client *Client) serveConnected(ctx context.Context, connection *websocket.Conn) error {
	connectionCtx, cancelConnection := context.WithCancel(ctx)
	workers := &connectedWorkers{}
	defer func() {
		cancelConnection()
		_ = connection.Close()
		workers.requests.Wait()
		client.disconnectTerminals()
		workers.events.Wait()
	}()
	stopContextClose := context.AfterFunc(connectionCtx, func() { _ = connection.Close() })
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
	writer := &connectedWriter{connection: connection}
	workerFailure := make(chan error, 1)
	for {
		messageType, data, err := connection.ReadMessage()
		if err != nil {
			select {
			case workerErr := <-workerFailure:
				return workerErr
			default:
			}
			return fmt.Errorf("node gateway session ended: %w", err)
		}
		if messageType == websocket.BinaryMessage {
			if client.transferHandler == nil {
				return errors.New("node gateway sent a transfer frame while transfers are disabled")
			}
			frame, decodeErr := protocol.DecodeTransferFrame(data)
			if decodeErr != nil {
				return errors.New("node gateway sent an invalid transfer frame")
			}
			if handleErr := client.transferHandler.HandleTransferFrame(
				connectionCtx,
				frame,
				func(response protocol.TransferFrame) error {
					if !frame.SameBinding(response) {
						return protocol.ErrInvalidTransferFrame
					}
					return writer.writeTransferFrame(response)
				},
			); handleErr != nil {
				return handleErr
			}
			continue
		}
		if messageType == websocket.TextMessage {
			envelope, decodeErr := decodeCommandRequest(data)
			if decodeErr != nil {
				return decodeErr
			}
			if requestErr := client.dispatchRequest(
				connectionCtx,
				writer,
				envelope,
				workerFailure,
				workers,
			); requestErr != nil {
				return requestErr
			}
		}
	}
}

func decodeCommandRequest(data []byte) (protocol.Envelope, error) {
	envelope, decodeErr := protocol.Decode(data)
	if decodeErr != nil || envelope.Type != protocol.FrameRequest {
		return protocol.Envelope{}, errors.New("node gateway sent an invalid command request")
	}
	return envelope, nil
}

func (client *Client) dispatchRequest(
	ctx context.Context,
	writer *connectedWriter,
	envelope protocol.Envelope,
	workerFailure chan<- error,
	workers *connectedWorkers,
) error {
	if !client.concurrentRequest(envelope.Method) {
		return client.handleRequest(ctx, writer, envelope, workers)
	}
	select {
	case client.invokeSlots <- struct{}{}:
	default:
		return client.writeCommandError(
			writer,
			envelope.ID,
			"NODE_BUSY",
			"node invocation concurrency limit reached",
		)
	}
	workers.requests.Add(1)
	go func() {
		defer workers.requests.Done()
		defer func() { <-client.invokeSlots }()
		if err := client.handleRequest(ctx, writer, envelope, workers); err != nil {
			select {
			case workerFailure <- err:
			default:
			}
			_ = writer.connection.Close()
		}
	}()
	return nil
}

func (client *Client) concurrentRequest(method string) bool {
	switch method {
	case "node.invoke", "node.terminal.open", "node.terminal.attach", "node.terminal.control",
		"node.terminal.detach":
		return true
	default:
		return false
	}
}

func (client *Client) handleRequest(
	ctx context.Context,
	writer *connectedWriter,
	envelope protocol.Envelope,
	workers *connectedWorkers,
) error {
	switch envelope.Method {
	case "node.invoke":
		return client.handleInvoke(ctx, writer, envelope)
	case "node.invoke.get":
		return client.handleInvocationQuery(writer, envelope)
	case "node.invoke.cancel":
		return client.handleInvocationCancel(writer, envelope)
	case "node.terminal.open":
		return client.handleTerminalOpen(ctx, writer, envelope)
	case "node.terminal.attach":
		return client.handleTerminalAttach(writer, envelope, workers)
	case "node.terminal.control":
		return client.handleTerminalControl(ctx, writer, envelope)
	case "node.terminal.status":
		return client.handleTerminalStatus(writer, envelope)
	case "node.terminal.detach":
		return client.handleTerminalDetach(writer, envelope)
	default:
		return client.writeCommandError(
			writer,
			envelope.ID,
			"METHOD_NOT_FOUND",
			"unsupported node method",
		)
	}
}

type terminalTransportEvent struct {
	TerminalID string              `json:"terminal_id"`
	Event      TerminalBrokerEvent `json:"event"`
}

func (client *Client) handleTerminalOpen(
	ctx context.Context,
	writer *connectedWriter,
	envelope protocol.Envelope,
) error {
	if client.runtime == nil || client.runtime.terminals == nil {
		return client.writeCommandError(
			writer,
			envelope.ID,
			"TERMINAL_UNAVAILABLE",
			"node terminal runtime is disabled",
		)
	}
	var plan nodes.TerminalOpenPlan
	if err := decodeStrictJSON(envelope.Params, &plan); err != nil ||
		envelope.IdempotencyKey == "" ||
		envelope.IdempotencyKey != plan.IdempotencyKey {
		return client.writeCommandError(
			writer,
			envelope.ID,
			"INVALID_TERMINAL_PLAN",
			"invalid terminal open plan",
		)
	}
	metadata, err := client.runtime.terminals.Open(ctx, plan)
	if err != nil {
		code := "TERMINAL_OPEN_FAILED"
		message := "terminal open failed"
		if errors.Is(err, nodes.ErrCommandDenied) || errors.Is(err, nodes.ErrInvalidTerminal) {
			code = "TERMINAL_DENIED"
			message = "terminal open denied"
		} else if errors.Is(err, ErrTerminalOpenConflict) {
			code = "IDEMPOTENCY_CONFLICT"
			message = "terminal open conflicts with existing session"
		}
		return client.writeCommandError(writer, envelope.ID, code, message)
	}
	return writeTerminalMetadata(writer, envelope.ID, metadata)
}

func (client *Client) handleTerminalAttach(
	writer *connectedWriter,
	envelope protocol.Envelope,
	workers *connectedWorkers,
) error {
	if client.runtime == nil || client.runtime.terminals == nil {
		return client.writeCommandError(
			writer,
			envelope.ID,
			"TERMINAL_UNAVAILABLE",
			"node terminal runtime is disabled",
		)
	}
	if envelope.IdempotencyKey != "" {
		return client.writeCommandError(
			writer,
			envelope.ID,
			"INVALID_TERMINAL_ATTACH",
			"terminal attach cannot carry an idempotency key",
		)
	}
	var request nodes.TerminalSessionRequest
	if err := decodeStrictJSON(envelope.Params, &request); err != nil {
		return client.writeCommandError(
			writer,
			envelope.ID,
			"INVALID_TERMINAL_ATTACH",
			"invalid terminal attach request",
		)
	}
	attachment, err := client.runtime.terminals.Attach(request)
	if err != nil {
		return client.writeTerminalAccessError(writer, envelope.ID, err)
	}
	metadata, err := client.runtime.terminals.Status(request)
	if err != nil {
		_ = attachment.Close()
		return client.writeTerminalAccessError(writer, envelope.ID, err)
	}
	client.attachmentsMu.Lock()
	if client.attachments[request.TerminalID] != nil {
		client.attachmentsMu.Unlock()
		_ = attachment.Close()
		return client.writeCommandError(
			writer,
			envelope.ID,
			"TERMINAL_ALREADY_ATTACHED",
			"terminal session was already attached",
		)
	}
	client.attachments[request.TerminalID] = attachment
	client.attachmentsMu.Unlock()
	if err := writeTerminalMetadata(writer, envelope.ID, metadata); err != nil {
		client.removeAttachment(request.TerminalID, attachment)
		_ = attachment.Close()
		return err
	}
	workers.events.Add(1)
	go client.forwardTerminalEvents(
		writer,
		request.TerminalID,
		attachment,
		workers,
	)
	return nil
}

func (client *Client) handleTerminalControl(
	ctx context.Context,
	writer *connectedWriter,
	envelope protocol.Envelope,
) error {
	var request nodes.TerminalControlRequest
	if err := decodeStrictJSON(envelope.Params, &request); err != nil ||
		envelope.IdempotencyKey == "" ||
		envelope.IdempotencyKey != request.IdempotencyKey {
		return client.writeCommandError(
			writer,
			envelope.ID,
			"INVALID_TERMINAL_CONTROL",
			"invalid terminal control request",
		)
	}
	client.attachmentsMu.Lock()
	attachment := client.attachments[request.TerminalID]
	client.attachmentsMu.Unlock()
	if attachment == nil {
		return client.writeCommandError(
			writer,
			envelope.ID,
			"TERMINAL_NOT_ATTACHED",
			"terminal session is not attached",
		)
	}
	if err := attachment.Send(ctx, request); err != nil {
		return client.writeTerminalAccessError(writer, envelope.ID, err)
	}
	result, err := json.Marshal(map[string]any{
		"terminal_id": request.TerminalID,
		"sequence":    request.Sequence,
		"dispatched":  true,
	})
	if err != nil {
		return err
	}
	ok := true
	return writer.writeEnvelope(protocol.Envelope{
		Type: protocol.FrameResponse, ID: envelope.ID, OK: &ok, Result: result,
	})
}

func (client *Client) handleTerminalStatus(
	writer *connectedWriter,
	envelope protocol.Envelope,
) error {
	if client.runtime == nil || client.runtime.terminals == nil {
		return client.writeCommandError(
			writer,
			envelope.ID,
			"TERMINAL_UNAVAILABLE",
			"node terminal runtime is disabled",
		)
	}
	if envelope.IdempotencyKey != "" {
		return client.writeCommandError(
			writer,
			envelope.ID,
			"INVALID_TERMINAL_STATUS",
			"terminal status cannot carry an idempotency key",
		)
	}
	var request nodes.TerminalSessionRequest
	if err := decodeStrictJSON(envelope.Params, &request); err != nil {
		return client.writeCommandError(
			writer,
			envelope.ID,
			"INVALID_TERMINAL_STATUS",
			"invalid terminal status request",
		)
	}
	metadata, err := client.runtime.terminals.Status(request)
	if err != nil {
		return client.writeTerminalAccessError(writer, envelope.ID, err)
	}
	return writeTerminalMetadata(writer, envelope.ID, metadata)
}

func (client *Client) handleTerminalDetach(
	writer *connectedWriter,
	envelope protocol.Envelope,
) error {
	if client.runtime == nil || client.runtime.terminals == nil {
		return client.writeCommandError(
			writer,
			envelope.ID,
			"TERMINAL_UNAVAILABLE",
			"node terminal runtime is disabled",
		)
	}
	if envelope.IdempotencyKey != "" {
		return client.writeCommandError(
			writer,
			envelope.ID,
			"INVALID_TERMINAL_DETACH",
			"terminal detach cannot carry an idempotency key",
		)
	}
	var request nodes.TerminalSessionRequest
	if err := decodeStrictJSON(envelope.Params, &request); err != nil ||
		request.Validate() != nil {
		return client.writeCommandError(
			writer,
			envelope.ID,
			"INVALID_TERMINAL_DETACH",
			"invalid terminal detach request",
		)
	}
	client.attachmentsMu.Lock()
	attachment := client.attachments[request.TerminalID]
	client.attachmentsMu.Unlock()
	if attachment == nil {
		metadata, err := client.runtime.terminals.Status(request)
		if err != nil {
			return client.writeTerminalAccessError(writer, envelope.ID, err)
		}
		if metadata.State == TerminalSessionClosed ||
			metadata.State == TerminalSessionUnknown {
			return writeTerminalMetadata(writer, envelope.ID, metadata)
		}
		return client.writeCommandError(
			writer,
			envelope.ID,
			"TERMINAL_NOT_ATTACHED",
			"terminal session is not attached",
		)
	}
	if !terminalOwnersEqual(attachment.owner, request.Owner) {
		return client.writeTerminalAccessError(writer, envelope.ID, ErrTerminalOwnerMismatch)
	}
	if err := attachment.Close(); err != nil {
		return client.writeTerminalAccessError(writer, envelope.ID, err)
	}
	client.removeAttachment(request.TerminalID, attachment)
	metadata, err := client.runtime.terminals.Status(request)
	if err != nil {
		return client.writeTerminalAccessError(writer, envelope.ID, err)
	}
	return writeTerminalMetadata(writer, envelope.ID, metadata)
}

func (client *Client) forwardTerminalEvents(
	writer *connectedWriter,
	terminalID string,
	attachment *TerminalAttachment,
	workers *connectedWorkers,
) {
	defer workers.events.Done()
	defer client.removeAttachment(terminalID, attachment)
	defer attachment.Close()
	for event := range attachment.Events() {
		payload, err := json.Marshal(terminalTransportEvent{
			TerminalID: terminalID,
			Event:      event,
		})
		if err != nil {
			_ = writer.connection.Close()
			return
		}
		if err := writer.writeEnvelope(protocol.Envelope{
			Type: protocol.FrameEvent, Event: "node.terminal.event", Payload: payload,
		}); err != nil {
			_ = writer.connection.Close()
			return
		}
	}
}

func (client *Client) removeAttachment(
	terminalID string,
	attachment *TerminalAttachment,
) {
	client.attachmentsMu.Lock()
	if client.attachments[terminalID] == attachment {
		delete(client.attachments, terminalID)
	}
	client.attachmentsMu.Unlock()
}

func (client *Client) disconnectTerminals() {
	client.attachmentsMu.Lock()
	attachments := make([]*TerminalAttachment, 0, len(client.attachments))
	for terminalID, attachment := range client.attachments {
		attachments = append(attachments, attachment)
		delete(client.attachments, terminalID)
	}
	client.attachmentsMu.Unlock()
	var closed sync.WaitGroup
	for _, attachment := range attachments {
		closed.Add(1)
		go func() {
			defer closed.Done()
			_ = attachment.Close()
		}()
	}
	closed.Wait()
	if client.runtime != nil && client.runtime.terminals != nil {
		_ = client.runtime.terminals.Disconnect()
	}
}

func (client *Client) writeTerminalAccessError(
	writer *connectedWriter,
	requestID string,
	err error,
) error {
	code := "TERMINAL_FAILED"
	message := "terminal operation failed"
	switch {
	case errors.Is(err, ErrTerminalOwnerMismatch):
		code = "TERMINAL_DENIED"
		message = "terminal operation denied"
	case errors.Is(err, ErrTerminalNotFound):
		code = "TERMINAL_NOT_FOUND"
		message = "terminal session not found"
	case errors.Is(err, ErrTerminalAlreadyAttached):
		code = "TERMINAL_ALREADY_ATTACHED"
		message = "terminal session was already attached"
	}
	return client.writeCommandError(writer, requestID, code, message)
}

func writeTerminalMetadata(
	writer *connectedWriter,
	requestID string,
	metadata nodes.TerminalMetadata,
) error {
	result, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("encode terminal metadata: %w", err)
	}
	ok := true
	return writer.writeEnvelope(protocol.Envelope{
		Type: protocol.FrameResponse, ID: requestID, OK: &ok, Result: result,
	})
}

func (client *Client) handleInvoke(
	ctx context.Context,
	writer *connectedWriter,
	envelope protocol.Envelope,
) error {
	if client.runtime == nil {
		return client.writeCommandError(
			writer,
			envelope.ID,
			"COMMAND_UNAVAILABLE",
			"node command runtime is disabled",
		)
	}
	var plan nodes.ExecutionPlan
	if planErr := decodeStrictJSON(envelope.Params, &plan); planErr != nil {
		return client.writeCommandError(
			writer,
			envelope.ID,
			"INVALID_PLAN",
			"invalid execution plan",
		)
	}
	if envelope.IdempotencyKey == "" || envelope.IdempotencyKey != plan.IdempotencyKey {
		return client.writeCommandError(
			writer,
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
		case errors.Is(err, ErrCommandUnavailable):
			code = "COMMAND_UNAVAILABLE"
			message = "node command unavailable"
		case errors.Is(err, ErrInvocationConflict):
			code = "IDEMPOTENCY_CONFLICT"
			message = "invocation idempotency conflict"
		case errors.Is(err, ErrInvocationOutcomeUnknown):
			code = "INVOCATION_UNKNOWN"
			message = "invocation outcome is unknown"
		case errors.Is(err, ErrInvocationCanceled):
			code = "INVOCATION_CANCELED"
			message = "node command canceled"
		}
		client.logger.Warn(
			"node invocation rejected",
			"invocation_id", plan.InvocationID,
			"command", plan.Command,
			"code", code,
			"reason", invocationRejectionReason(err),
		)
		return client.writeCommandError(writer, envelope.ID, code, message)
	}
	ok := true
	return writer.writeEnvelope(protocol.Envelope{
		Type:   protocol.FrameResponse,
		ID:     envelope.ID,
		OK:     &ok,
		Result: result,
	})
}

func invocationRejectionReason(err error) string {
	var inputDenied *commandInputDeniedError
	switch {
	case errors.As(err, &inputDenied):
		return "command_input_denied"
	case errors.Is(err, nodes.ErrCommandDenied):
		return "plan_authorization_denied"
	case errors.Is(err, nodes.ErrInvalidInvocation):
		return "invalid_invocation"
	case errors.Is(err, ErrCommandUnavailable):
		return "command_unavailable"
	case errors.Is(err, ErrInvocationConflict):
		return "idempotency_conflict"
	case errors.Is(err, ErrInvocationOutcomeUnknown):
		return "outcome_unknown"
	case errors.Is(err, ErrInvocationCanceled):
		return "canceled"
	default:
		return "execution_failed"
	}
}

func (client *Client) handleInvocationQuery(
	writer *connectedWriter,
	envelope protocol.Envelope,
) error {
	if client.runtime == nil {
		return client.writeCommandError(
			writer,
			envelope.ID,
			"COMMAND_UNAVAILABLE",
			"node command runtime is disabled",
		)
	}
	if envelope.IdempotencyKey != "" {
		return client.writeCommandError(
			writer,
			envelope.ID,
			"INVALID_QUERY",
			"invocation query cannot carry an idempotency key",
		)
	}
	var query nodes.InvocationQuery
	if err := decodeStrictJSON(envelope.Params, &query); err != nil || query.Validate() != nil {
		return client.writeCommandError(
			writer,
			envelope.ID,
			"INVALID_QUERY",
			"invalid invocation query",
		)
	}
	record, found, lookupErr := client.runtime.Invocation(query.InvocationID)
	if lookupErr != nil {
		return client.writeCommandError(
			writer,
			envelope.ID,
			"LEDGER_UNAVAILABLE",
			"invocation ledger is unavailable",
		)
	}
	if !found {
		return client.writeCommandError(
			writer,
			envelope.ID,
			"INVOCATION_NOT_FOUND",
			"invocation record not found",
		)
	}
	return writeInvocationRecord(writer, envelope.ID, record)
}

func (client *Client) handleInvocationCancel(
	writer *connectedWriter,
	envelope protocol.Envelope,
) error {
	if client.runtime == nil {
		return client.writeCommandError(
			writer,
			envelope.ID,
			"COMMAND_UNAVAILABLE",
			"node command runtime is disabled",
		)
	}
	if envelope.IdempotencyKey != "" {
		return client.writeCommandError(
			writer,
			envelope.ID,
			"INVALID_CANCEL",
			"invocation cancellation cannot carry an idempotency key",
		)
	}
	var request nodes.InvocationCancelRequest
	if err := decodeStrictJSON(envelope.Params, &request); err != nil || request.Validate() != nil {
		return client.writeCommandError(
			writer,
			envelope.ID,
			"INVALID_CANCEL",
			"invalid invocation cancellation request",
		)
	}
	record, err := client.runtime.Cancel(request)
	if err != nil {
		code := "CANCELLATION_FAILED"
		message := "invocation cancellation failed"
		switch {
		case errors.Is(err, ErrInvocationNotFound):
			code = "INVOCATION_NOT_FOUND"
			message = "invocation record not found"
		case errors.Is(err, ErrCancellationUnsupported):
			code = "CANCELLATION_UNSUPPORTED"
			message = "node command does not support cancellation"
		case errors.Is(err, ErrInvocationOutcomeUnknown):
			code = "INVOCATION_UNKNOWN"
			message = "invocation outcome is unknown"
		}
		return client.writeCommandError(writer, envelope.ID, code, message)
	}
	return writeInvocationRecord(writer, envelope.ID, record)
}

func writeInvocationRecord(
	writer *connectedWriter,
	requestID string,
	record nodes.InvocationRecord,
) error {
	result, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode invocation record: %w", err)
	}
	ok := true
	return writer.writeEnvelope(protocol.Envelope{
		Type:   protocol.FrameResponse,
		ID:     requestID,
		OK:     &ok,
		Result: result,
	})
}

func (client *Client) writeCommandError(
	writer *connectedWriter,
	requestID, code, message string,
) error {
	ok := false
	return writer.writeEnvelope(protocol.Envelope{
		Type: protocol.FrameResponse,
		ID:   requestID,
		OK:   &ok,
		Error: &protocol.Error{
			Code:    code,
			Message: message,
		},
	})
}

type connectedWriter struct {
	connection *websocket.Conn
	mu         sync.Mutex
}

func (writer *connectedWriter) writeEnvelope(envelope protocol.Envelope) error {
	data, err := protocol.Encode(envelope)
	if err != nil {
		return err
	}
	writer.mu.Lock()
	defer writer.mu.Unlock()
	if err := writer.connection.SetWriteDeadline(time.Now().Add(DefaultHandshakeTimeout)); err != nil {
		return err
	}
	return writer.connection.WriteMessage(websocket.TextMessage, data)
}

func (writer *connectedWriter) writeTransferFrame(
	frame protocol.TransferFrame,
) error {
	data, err := protocol.EncodeTransferFrame(frame)
	if err != nil {
		return err
	}
	writer.mu.Lock()
	defer writer.mu.Unlock()
	if err := writer.connection.SetWriteDeadline(time.Now().Add(DefaultHandshakeTimeout)); err != nil {
		return err
	}
	return writer.connection.WriteMessage(websocket.BinaryMessage, data)
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
