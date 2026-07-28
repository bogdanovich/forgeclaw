package gateway

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/bogdanovich/mintclaw/pkg/nodes"
)

const (
	nodeTerminalOperatorPath      = "/nodes/v1/terminal/ws"
	nodeTerminalOperatorReadLimit = 64 * 1024
	nodeTerminalOperatorWriteWait = 15 * time.Second
)

type nodeTerminalOperatorStream interface {
	Receive(context.Context) (nodes.TerminalEvent, error)
	Control(context.Context, nodes.TerminalControlRequest) error
	Close(context.Context) error
}

type nodeTerminalOperatorSource interface {
	attachOperatorTerminal(
		context.Context,
		nodes.TerminalOwner,
		string,
	) (nodeTerminalOperatorStream, nodes.TerminalMetadata, error)
	terminalOperatorStatus(
		context.Context,
		nodes.TerminalOwner,
		string,
	) (nodes.TerminalMetadata, error)
	closeOperatorTerminal(
		context.Context,
		nodes.TerminalOwner,
		string,
	) (nodes.TerminalMetadata, error)
}

type nodeTerminalOperatorBinding struct {
	owner      nodes.TerminalOwner
	source     nodeTerminalOperatorSource
	terminalID string
	expires    time.Time
}

type nodeTerminalOperatorSession struct {
	mu         sync.Mutex
	owner      nodes.TerminalOwner
	terminalID string
	stream     nodeTerminalOperatorStream
	next       uint64
	closed     bool
	finished   chan struct{}
}

type nodeTerminalOperatorHub struct {
	mu       sync.Mutex
	token    string
	pending  map[string]nodeTerminalOperatorBinding
	claimed  map[string]nodeTerminalOperatorBinding
	active   map[string]*nodeTerminalOperatorSession
	closed   bool
	ctx      context.Context
	cancel   context.CancelFunc
	upgrader websocket.Upgrader
}

type nodeTerminalOperatorRequest struct {
	Version        int    `json:"version"`
	Type           string `json:"type"`
	Sequence       uint64 `json:"sequence"`
	IdempotencyKey string `json:"idempotency_key"`
	InputBase64    string `json:"input_base64,omitempty"`
	Columns        int    `json:"columns,omitempty"`
	Rows           int    `json:"rows,omitempty"`
	Signal         string `json:"signal,omitempty"`
}

type nodeTerminalOperatorAttached struct {
	Version    int    `json:"version"`
	Type       string `json:"type"`
	TerminalID string `json:"terminal_id"`
	State      string `json:"state"`
}

func newNodeTerminalOperatorHub(token string, allowOrigins []string) *nodeTerminalOperatorHub {
	ctx, cancel := context.WithCancel(context.Background())
	checkOrigin := func(request *http.Request) bool {
		if len(allowOrigins) == 0 {
			return true
		}
		origin := request.Header.Get("Origin")
		for _, allowed := range allowOrigins {
			if allowed == "*" || allowed == origin {
				return true
			}
		}
		return false
	}
	return &nodeTerminalOperatorHub{
		token:   token,
		pending: make(map[string]nodeTerminalOperatorBinding),
		claimed: make(map[string]nodeTerminalOperatorBinding),
		active:  make(map[string]*nodeTerminalOperatorSession),
		ctx:     ctx,
		cancel:  cancel,
		upgrader: websocket.Upgrader{
			CheckOrigin: checkOrigin,
		},
	}
}

func terminalOperatorKey(operatorSessionID, terminalID string) string {
	return operatorSessionID + "\x00" + terminalID
}

func (hub *nodeTerminalOperatorHub) bind(
	source nodeTerminalOperatorSource,
	owner nodes.TerminalOwner,
	terminalID string,
	operatorSessionID string,
) error {
	if hub == nil || source == nil || strings.TrimSpace(hub.token) == "" {
		return errors.New("authenticated terminal operator transport is unavailable")
	}
	if strings.TrimSpace(operatorSessionID) == "" {
		return errors.New("operator session identity is required")
	}
	if err := (nodes.TerminalSessionRequest{
		TerminalID: terminalID,
		Owner:      owner,
	}).Validate(); err != nil {
		return err
	}
	key := terminalOperatorKey(operatorSessionID, terminalID)
	hub.mu.Lock()
	defer hub.mu.Unlock()
	if hub.closed {
		return errors.New("authenticated terminal operator transport is unavailable")
	}
	hub.pruneLocked(time.Now())
	if hub.terminalBoundLocked(terminalID) {
		return nodes.ErrGatewayTerminalConflict
	}
	hub.pending[key] = nodeTerminalOperatorBinding{
		owner: owner, source: source, terminalID: terminalID,
		expires: time.Now().Add(30 * time.Second),
	}
	return nil
}

func (hub *nodeTerminalOperatorHub) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	if hub == nil || !hub.authenticate(request) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	operatorSessionID := strings.TrimSpace(request.URL.Query().Get("session_id"))
	terminalID := strings.TrimSpace(request.URL.Query().Get("terminal_id"))
	key := terminalOperatorKey(operatorSessionID, terminalID)
	binding, found := hub.claim(key, time.Now())
	if !found {
		http.Error(w, "terminal unavailable", http.StatusNotFound)
		return
	}
	responseHeader := http.Header{}
	if protocol := hub.matchedSubprotocol(request); protocol != "" {
		responseHeader.Set("Sec-WebSocket-Protocol", protocol)
	}
	connection, err := hub.upgrader.Upgrade(w, request, responseHeader)
	if err != nil {
		hub.restore(key, binding)
		return
	}
	hub.serveConnection(connection, key, terminalID, binding)
}

func (hub *nodeTerminalOperatorHub) serveConnection(
	connection *websocket.Conn,
	key string,
	terminalID string,
	binding nodeTerminalOperatorBinding,
) {
	defer connection.Close()
	attachCtx, attachCancel := context.WithDeadline(
		hub.ctx,
		binding.expires,
	)
	stream, metadata, err := binding.source.attachOperatorTerminal(
		attachCtx,
		binding.owner,
		terminalID,
	)
	attachCancel()
	if err != nil {
		hub.releaseClaim(key)
		return
	}
	session := &nodeTerminalOperatorSession{
		owner: binding.owner, terminalID: terminalID, stream: stream,
		next: 1, finished: make(chan struct{}),
	}
	if !hub.activate(key, session) {
		closeCtx, closeCancel := context.WithTimeout(
			context.Background(),
			nodeAdmissionDrainTimeout,
		)
		_ = stream.Close(closeCtx)
		closeCancel()
		return
	}
	defer func() {
		session.shutdown()
		hub.deactivate(key, session)
		statusCtx, statusCancel := context.WithTimeout(
			context.Background(),
			nodeAdmissionDrainTimeout,
		)
		_, _ = binding.source.terminalOperatorStatus(
			statusCtx,
			binding.owner,
			terminalID,
		)
		statusCancel()
	}()
	connection.SetReadLimit(nodeTerminalOperatorReadLimit)
	if err := writeTerminalOperatorJSON(connection, nodeTerminalOperatorAttached{
		Version: nodes.TerminalProtocolVersion, Type: "attached",
		TerminalID: terminalID, State: metadata.State,
	}); err != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	readErr := make(chan error, 1)
	events := make(chan nodes.TerminalEvent)
	receiveErr := make(chan error, 1)
	go func() {
		readErr <- session.readControls(ctx, connection)
	}()
	go func() {
		for {
			event, receiveEventErr := stream.Receive(ctx)
			if receiveEventErr != nil {
				receiveErr <- receiveEventErr
				return
			}
			select {
			case events <- event:
			case <-ctx.Done():
				return
			}
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case <-session.finished:
			return
		case <-readErr:
			return
		case <-receiveErr:
			return
		case event := <-events:
			if err := writeTerminalOperatorJSON(connection, event); err != nil {
				return
			}
			if event.Type == "closed" || event.Type == "unknown" {
				return
			}
		}
	}
}

func (session *nodeTerminalOperatorSession) readControls(
	ctx context.Context,
	connection *websocket.Conn,
) error {
	for {
		var request nodeTerminalOperatorRequest
		if err := connection.ReadJSON(&request); err != nil {
			return err
		}
		if err := session.control(ctx, request); err != nil {
			return err
		}
	}
}

func (session *nodeTerminalOperatorSession) control(
	ctx context.Context,
	request nodeTerminalOperatorRequest,
) error {
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.closed || session.stream == nil {
		return errors.New("operator terminal session is closed")
	}
	if request.Version != nodes.TerminalProtocolVersion || request.Sequence != session.next {
		return nodes.ErrInvalidTerminal
	}
	control := nodes.TerminalControlRequest{
		TerminalSessionRequest: nodes.TerminalSessionRequest{
			TerminalID: session.terminalID,
			Owner:      session.owner,
		},
		Sequence:       request.Sequence,
		IdempotencyKey: request.IdempotencyKey,
	}
	switch request.Type {
	case "input":
		control.InputBase64 = request.InputBase64
	case "resize":
		control.Columns = request.Columns
		control.Rows = request.Rows
	case "signal":
		control.Signal = request.Signal
	case "close":
		control.Close = true
	default:
		return nodes.ErrInvalidTerminal
	}
	session.next++
	if err := session.stream.Control(ctx, control); err != nil {
		return err
	}
	return nil
}

func (session *nodeTerminalOperatorSession) signal(
	ctx context.Context,
	terminalID string,
	signal string,
) error {
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.closed || session.stream == nil {
		return errors.New("operator terminal session is closed")
	}
	sequence := session.next
	session.next++
	stream := session.stream
	owner := session.owner
	return stream.Control(ctx, nodes.TerminalControlRequest{
		TerminalSessionRequest: nodes.TerminalSessionRequest{
			TerminalID: terminalID,
			Owner:      owner,
		},
		Sequence:       sequence,
		IdempotencyKey: fmt.Sprintf("model_signal_%d", sequence),
		Signal:         signal,
	})
}

func (session *nodeTerminalOperatorSession) closeTerminal(
	ctx context.Context,
	terminalID string,
) error {
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.closed || session.stream == nil {
		return errors.New("operator terminal session is closed")
	}
	sequence := session.next
	session.next++
	stream := session.stream
	owner := session.owner
	return stream.Control(ctx, nodes.TerminalControlRequest{
		TerminalSessionRequest: nodes.TerminalSessionRequest{
			TerminalID: terminalID,
			Owner:      owner,
		},
		Sequence:       sequence,
		IdempotencyKey: fmt.Sprintf("model_close_%d", sequence),
		Close:          true,
	})
}

func (session *nodeTerminalOperatorSession) shutdown() {
	stream := session.beginShutdown()
	if stream == nil {
		return
	}
	closeCtx, closeCancel := context.WithTimeout(
		context.Background(),
		nodeAdmissionDrainTimeout,
	)
	_ = stream.Close(closeCtx)
	closeCancel()
}

func (session *nodeTerminalOperatorSession) beginShutdown() nodeTerminalOperatorStream {
	session.mu.Lock()
	if session.closed {
		session.mu.Unlock()
		return nil
	}
	session.closed = true
	stream := session.stream
	session.stream = nil
	close(session.finished)
	session.mu.Unlock()
	return stream
}

func (hub *nodeTerminalOperatorHub) signal(
	ctx context.Context,
	owner nodes.TerminalOwner,
	terminalID string,
	signal string,
) error {
	session := hub.ownedActive(owner, terminalID)
	if session == nil {
		return nodes.ErrGatewayTerminalConflict
	}
	return session.signal(ctx, terminalID, signal)
}

func (hub *nodeTerminalOperatorHub) closeTerminal(
	ctx context.Context,
	owner nodes.TerminalOwner,
	terminalID string,
) error {
	session := hub.ownedActive(owner, terminalID)
	if session == nil {
		return nodes.ErrGatewayTerminalNotFound
	}
	if err := session.closeTerminal(ctx, terminalID); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-session.finished:
		return nil
	}
}

func (hub *nodeTerminalOperatorHub) ownedActive(
	owner nodes.TerminalOwner,
	terminalID string,
) *nodeTerminalOperatorSession {
	if hub == nil {
		return nil
	}
	hub.mu.Lock()
	defer hub.mu.Unlock()
	for key, session := range hub.active {
		if strings.HasSuffix(key, "\x00"+terminalID) && session.owner == owner {
			return session
		}
	}
	return nil
}

func (hub *nodeTerminalOperatorHub) claim(
	key string,
	now time.Time,
) (nodeTerminalOperatorBinding, bool) {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	if hub.closed {
		return nodeTerminalOperatorBinding{}, false
	}
	hub.pruneLocked(now)
	binding, found := hub.pending[key]
	if !found {
		return nodeTerminalOperatorBinding{}, false
	}
	delete(hub.pending, key)
	hub.claimed[key] = binding
	return binding, true
}

func (hub *nodeTerminalOperatorHub) restore(
	key string,
	binding nodeTerminalOperatorBinding,
) {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	delete(hub.claimed, key)
	if !hub.closed && time.Now().Before(binding.expires) {
		hub.pending[key] = binding
	}
}

func (hub *nodeTerminalOperatorHub) releaseClaim(key string) {
	hub.mu.Lock()
	delete(hub.claimed, key)
	hub.mu.Unlock()
}

func (hub *nodeTerminalOperatorHub) activate(
	key string,
	session *nodeTerminalOperatorSession,
) bool {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	if hub.closed {
		delete(hub.claimed, key)
		return false
	}
	if _, claimed := hub.claimed[key]; !claimed {
		return false
	}
	if _, exists := hub.active[key]; exists {
		delete(hub.claimed, key)
		return false
	}
	delete(hub.claimed, key)
	hub.active[key] = session
	return true
}

func (hub *nodeTerminalOperatorHub) unbind(
	owner nodes.TerminalOwner,
	terminalID string,
) {
	if hub == nil {
		return
	}
	suffix := "\x00" + terminalID
	hub.mu.Lock()
	defer hub.mu.Unlock()
	for key, binding := range hub.pending {
		if strings.HasSuffix(key, suffix) && binding.owner == owner {
			delete(hub.pending, key)
		}
	}
	for key, binding := range hub.claimed {
		if strings.HasSuffix(key, suffix) && binding.owner == owner {
			delete(hub.claimed, key)
		}
	}
}

func (hub *nodeTerminalOperatorHub) deactivate(
	key string,
	session *nodeTerminalOperatorSession,
) {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	if hub.active[key] == session {
		delete(hub.active, key)
	}
}

func (hub *nodeTerminalOperatorHub) pruneLocked(now time.Time) {
	for key, binding := range hub.pending {
		if !now.Before(binding.expires) {
			delete(hub.pending, key)
		}
	}
}

func (hub *nodeTerminalOperatorHub) terminalBoundLocked(terminalID string) bool {
	suffix := "\x00" + terminalID
	for key := range hub.pending {
		if strings.HasSuffix(key, suffix) {
			return true
		}
	}
	for key := range hub.claimed {
		if strings.HasSuffix(key, suffix) {
			return true
		}
	}
	for key := range hub.active {
		if strings.HasSuffix(key, suffix) {
			return true
		}
	}
	return false
}

func (hub *nodeTerminalOperatorHub) shutdown() {
	if hub == nil {
		return
	}
	hub.mu.Lock()
	if hub.closed {
		hub.mu.Unlock()
		return
	}
	hub.closed = true
	hub.cancel()
	sessions := make([]*nodeTerminalOperatorSession, 0, len(hub.active))
	pending := make([]nodeTerminalOperatorBinding, 0, len(hub.pending))
	for _, session := range hub.active {
		sessions = append(sessions, session)
	}
	for _, binding := range hub.pending {
		pending = append(pending, binding)
	}
	clear(hub.pending)
	clear(hub.claimed)
	clear(hub.active)
	hub.mu.Unlock()
	cleanupCtx, cleanupCancel := context.WithTimeout(
		context.Background(),
		nodeAdmissionDrainTimeout,
	)
	defer cleanupCancel()
	var cleanup sync.WaitGroup
	for _, session := range sessions {
		stream := session.beginShutdown()
		if stream == nil {
			continue
		}
		cleanup.Add(1)
		go func() {
			defer cleanup.Done()
			_ = stream.Close(cleanupCtx)
		}()
	}
	for _, binding := range pending {
		cleanup.Add(1)
		go func() {
			defer cleanup.Done()
			_, _ = binding.source.closeOperatorTerminal(
				cleanupCtx,
				binding.owner,
				binding.terminalID,
			)
		}()
	}
	done := make(chan struct{})
	go func() {
		cleanup.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-cleanupCtx.Done():
	}
}

func (hub *nodeTerminalOperatorHub) authenticate(request *http.Request) bool {
	if request == nil || strings.TrimSpace(hub.token) == "" {
		return false
	}
	if token, found := strings.CutPrefix(request.Header.Get("Authorization"), "Bearer "); found &&
		constantTimeStringEqual(token, hub.token) {
		return true
	}
	return hub.matchedSubprotocol(request) != ""
}

func (hub *nodeTerminalOperatorHub) matchedSubprotocol(request *http.Request) string {
	for _, protocol := range websocket.Subprotocols(request) {
		if token, found := strings.CutPrefix(protocol, "token."); found &&
			constantTimeStringEqual(token, hub.token) {
			return protocol
		}
	}
	return ""
}

func constantTimeStringEqual(left, right string) bool {
	if len(left) != len(right) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(left), []byte(right)) == 1
}

func writeTerminalOperatorJSON(connection *websocket.Conn, value any) error {
	if err := connection.SetWriteDeadline(time.Now().Add(nodeTerminalOperatorWriteWait)); err != nil {
		return err
	}
	defer connection.SetWriteDeadline(time.Time{})
	return connection.WriteJSON(value)
}
