package mintclaw

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"github.com/bogdanovich/mintclaw/pkg/bus"
	"github.com/bogdanovich/mintclaw/pkg/channels"
	"github.com/bogdanovich/mintclaw/pkg/config"
	"github.com/bogdanovich/mintclaw/pkg/identity"
	"github.com/bogdanovich/mintclaw/pkg/logger"
	"github.com/bogdanovich/mintclaw/pkg/utils"
)

// mintclawConn represents a single WebSocket connection.
type mintclawConn struct {
	id         string
	conn       *websocket.Conn
	sessionID  string
	writeOnce  sync.Once
	writeLock  chan struct{}
	queueMu    sync.Mutex
	writeQueue chan mintclawWriteRequest
	closeCh    chan struct{}
	closed     atomic.Bool
	cancel     context.CancelFunc // cancels per-connection goroutines (e.g. pingLoop)
}

const (
	mintclawWriteTimeout   = 15 * time.Second
	mintclawWriteQueueSize = 32
)

var errMintClawWriteQueueFull = errors.New("mintclaw connection write queue full")

type mintclawWriteRequest struct {
	ctx     context.Context
	cancel  context.CancelFunc
	writeFn func() error
	result  chan error
}

var allowedInlineImageMIMETypes = map[string]struct{}{
	"image/jpeg": {},
	"image/png":  {},
	"image/gif":  {},
	"image/webp": {},
	"image/bmp":  {},
}

func outboundMessageIsThought(msg bus.OutboundMessage) bool {
	return bus.OutboundMetadataFromMessage(msg).IsThought()
}

func outboundMessageIsToolCalls(msg bus.OutboundMessage) bool {
	return bus.OutboundMetadataFromMessage(msg).IsToolCalls()
}

func (pc *mintclawConn) write(ctx context.Context, writeFn func() error) error {
	if pc.closed.Load() {
		return fmt.Errorf("connection closed")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	writeCtx, cancel := context.WithTimeout(ctx, mintclawWriteTimeout)
	defer cancel()

	pc.writeOnce.Do(func() {
		pc.writeLock = make(chan struct{}, 1)
		pc.writeLock <- struct{}{}
	})
	select {
	case <-writeCtx.Done():
		return writeCtx.Err()
	case <-pc.writeLock:
	}
	defer func() { pc.writeLock <- struct{}{} }()

	if pc.closed.Load() {
		return fmt.Errorf("connection closed")
	}
	if err := writeCtx.Err(); err != nil {
		return err
	}
	deadline, _ := writeCtx.Deadline()
	if err := pc.conn.SetWriteDeadline(deadline); err != nil {
		return err
	}
	defer pc.conn.SetWriteDeadline(time.Time{})

	var writeState atomic.Uint32
	writeFinished := make(chan struct{})
	watcherDone := make(chan struct{})
	go func() {
		defer close(watcherDone)
		select {
		case <-writeCtx.Done():
			if writeState.CompareAndSwap(0, 2) {
				pc.close()
			}
		case <-writeFinished:
		}
	}()

	err := writeFn()
	if writeState.CompareAndSwap(0, 1) {
		close(writeFinished)
	}
	<-watcherDone
	if err != nil {
		if timeoutErr, ok := err.(net.Error); ok && timeoutErr.Timeout() {
			pc.close()
			return context.DeadlineExceeded
		}
		if ctxErr := writeCtx.Err(); ctxErr != nil {
			if ctxErr == context.DeadlineExceeded {
				pc.close()
			}
			return ctxErr
		}
		return err
	}
	// A successful WebSocket write is treated as delivered even if cancellation
	// won the connection-lifecycle race. Reporting an error here could retry a
	// frame that the peer already received; the closed connection is used only
	// to prevent future writes after the ambiguous cancellation boundary.
	return nil
}

// writeJSON sends a JSON message with context-aware serialization and a
// bounded socket deadline. Gorilla permits only one concurrent writer.
func (pc *mintclawConn) writeJSON(ctx context.Context, v any) error {
	return pc.write(ctx, func() error {
		return pc.conn.WriteJSON(v)
	})
}

func (pc *mintclawConn) writeMessage(ctx context.Context, messageType int, data []byte) error {
	return pc.write(ctx, func() error {
		return pc.conn.WriteMessage(messageType, data)
	})
}

func (pc *mintclawConn) enqueueWrite(ctx context.Context, writeFn func() error) <-chan error {
	if ctx == nil {
		ctx = context.Background()
	}
	result := make(chan error, 1)
	writeCtx, cancel := context.WithTimeout(ctx, mintclawWriteTimeout)
	request := mintclawWriteRequest{ctx: writeCtx, cancel: cancel, writeFn: writeFn, result: result}

	pc.queueMu.Lock()
	if pc.closed.Load() {
		pc.queueMu.Unlock()
		cancel()
		result <- fmt.Errorf("connection closed")
		return result
	}
	if pc.writeQueue == nil {
		pc.writeQueue = make(chan mintclawWriteRequest, mintclawWriteQueueSize)
		pc.closeCh = make(chan struct{})
		go pc.runWriteQueue(pc.writeQueue, pc.closeCh)
	}
	select {
	case pc.writeQueue <- request:
		pc.queueMu.Unlock()
	case <-pc.closeCh:
		pc.queueMu.Unlock()
		cancel()
		result <- fmt.Errorf("connection closed")
	default:
		pc.queueMu.Unlock()
		cancel()
		result <- errMintClawWriteQueueFull
		pc.close()
	}
	return result
}

func (pc *mintclawConn) runWriteQueue(queue <-chan mintclawWriteRequest, closeCh <-chan struct{}) {
	for {
		if pc.closed.Load() {
			pc.failQueuedWrites(queue)
			return
		}
		select {
		case <-closeCh:
			pc.failQueuedWrites(queue)
			return
		case request := <-queue:
			err := pc.write(request.ctx, request.writeFn)
			request.cancel()
			request.result <- err
			if err != nil || pc.closed.Load() {
				pc.close()
				pc.failQueuedWrites(queue)
				return
			}
		}
	}
}

func (pc *mintclawConn) failQueuedWrites(queue <-chan mintclawWriteRequest) {
	for {
		select {
		case request := <-queue:
			request.cancel()
			request.result <- fmt.Errorf("connection closed")
		default:
			return
		}
	}
}

func (pc *mintclawConn) enqueueJSON(ctx context.Context, v any) <-chan error {
	return pc.enqueueWrite(ctx, func() error {
		return pc.conn.WriteJSON(v)
	})
}

// close closes the connection.
func (pc *mintclawConn) close() {
	pc.queueMu.Lock()
	if !pc.closed.CompareAndSwap(false, true) {
		pc.queueMu.Unlock()
		return
	}
	closeCh := pc.closeCh
	cancel := pc.cancel
	conn := pc.conn
	if closeCh != nil {
		close(closeCh)
	}
	pc.queueMu.Unlock()
	if cancel != nil {
		cancel()
	}
	if conn != nil {
		_ = conn.Close()
	}
}

// MintClawChannel implements the native MintClaw Protocol WebSocket channel.
// It serves as the reference implementation for all optional capability interfaces.
type MintClawChannel struct {
	*channels.BaseChannel
	bc                 *config.Channel
	config             *config.MintClawSettings
	upgrader           websocket.Upgrader
	connections        map[string]*mintclawConn            // connID -> *mintclawConn
	sessionConnections map[string]map[string]*mintclawConn // sessionID -> connID -> *mintclawConn
	connsMu            sync.RWMutex
	ctx                context.Context
	cancel             context.CancelFunc
	// broadcastFn lets tests intercept outbound broadcasts. nil routes to active connections.
	broadcastFn func(ctx context.Context, chatID string, msg MintClawMessage) error
}

// NewMintClawChannel creates a new MintClaw Protocol channel.
func NewMintClawChannel(
	bc *config.Channel,
	cfg *config.MintClawSettings,
	messageBus *bus.MessageBus,
) (*MintClawChannel, error) {
	if cfg.Token.String() == "" {
		return nil, fmt.Errorf("mintclaw token is required")
	}

	base := channels.NewBaseChannel("mintclaw", cfg, messageBus, bc.AllowFrom)

	allowOrigins := cfg.AllowOrigins
	checkOrigin := func(r *http.Request) bool {
		if len(allowOrigins) == 0 {
			return true // allow all if not configured
		}
		origin := r.Header.Get("Origin")
		for _, allowed := range allowOrigins {
			if allowed == "*" || allowed == origin {
				return true
			}
		}
		return false
	}

	ch := &MintClawChannel{
		BaseChannel: base,
		bc:          bc,
		config:      cfg,
		upgrader: websocket.Upgrader{
			CheckOrigin:     checkOrigin,
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
		},
		connections:        make(map[string]*mintclawConn),
		sessionConnections: make(map[string]map[string]*mintclawConn),
	}
	return ch, nil
}

// createAndAddConnection checks MaxConnections and registers a connection atomically.
func (c *MintClawChannel) createAndAddConnection(
	conn *websocket.Conn,
	sessionID string,
	maxConns int,
) (*mintclawConn, error) {
	c.connsMu.Lock()
	defer c.connsMu.Unlock()
	if len(c.connections) >= maxConns {
		return nil, channels.ErrTemporary
	}

	var connID string
	for {
		connID = uuid.New().String()
		if _, exists := c.connections[connID]; !exists {
			break
		}
	}

	pc := &mintclawConn{
		id:        connID,
		conn:      conn,
		sessionID: sessionID,
	}

	c.connections[pc.id] = pc
	bySession, ok := c.sessionConnections[pc.sessionID]
	if !ok {
		bySession = make(map[string]*mintclawConn)
		c.sessionConnections[pc.sessionID] = bySession
	}
	bySession[pc.id] = pc

	return pc, nil
}

// removeConnection deletes a connection from indexes and returns it when found.
func (c *MintClawChannel) removeConnection(connID string) *mintclawConn {
	c.connsMu.Lock()
	defer c.connsMu.Unlock()

	pc, ok := c.connections[connID]
	if !ok {
		return nil
	}

	delete(c.connections, connID)
	if bySession, ok := c.sessionConnections[pc.sessionID]; ok {
		delete(bySession, connID)
		if len(bySession) == 0 {
			delete(c.sessionConnections, pc.sessionID)
		}
	}

	return pc
}

// takeAllConnections snapshots and clears all connection indexes.
func (c *MintClawChannel) takeAllConnections() []*mintclawConn {
	c.connsMu.Lock()
	defer c.connsMu.Unlock()

	all := make([]*mintclawConn, 0, len(c.connections))
	for _, pc := range c.connections {
		all = append(all, pc)
	}
	clear(c.connections)
	clear(c.sessionConnections)

	return all
}

// sessionConnectionsSnapshot returns all active connections for a session.
func (c *MintClawChannel) sessionConnectionsSnapshot(sessionID string) []*mintclawConn {
	c.connsMu.RLock()
	defer c.connsMu.RUnlock()

	bySession, ok := c.sessionConnections[sessionID]
	if !ok || len(bySession) == 0 {
		return nil
	}

	conns := make([]*mintclawConn, 0, len(bySession))
	for _, pc := range bySession {
		conns = append(conns, pc)
	}
	return conns
}

// currentConnCount returns a lock-protected snapshot of active connection count.
func (c *MintClawChannel) currentConnCount() int {
	c.connsMu.RLock()
	defer c.connsMu.RUnlock()
	return len(c.connections)
}

// Start implements Channel.
func (c *MintClawChannel) Start(ctx context.Context) error {
	logger.InfoC("mintclaw", "Starting MintClaw Protocol channel")
	c.ctx, c.cancel = context.WithCancel(ctx)
	c.SetRunning(true)
	logger.InfoC("mintclaw", "MintClaw Protocol channel started")
	return nil
}

// Stop implements Channel.
func (c *MintClawChannel) Stop(ctx context.Context) error {
	logger.InfoC("mintclaw", "Stopping MintClaw Protocol channel")
	c.SetRunning(false)

	// Close all connections
	for _, pc := range c.takeAllConnections() {
		pc.close()
	}

	if c.cancel != nil {
		c.cancel()
	}
	logger.InfoC("mintclaw", "MintClaw Protocol channel stopped")
	return nil
}

// WebhookPath implements channels.WebhookHandler.
func (c *MintClawChannel) WebhookPath() string { return "/mintclaw/" }

// ServeHTTP implements http.Handler for the shared HTTP server.
func (c *MintClawChannel) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/mintclaw")

	switch path {
	case "/ws", "/ws/":
		c.handleWebSocket(w, r)
	default:
		if strings.HasPrefix(path, "/media/") {
			c.handleMediaDownload(w, r)
			return
		}
		http.NotFound(w, r)
	}
}

// Send implements Channel — sends a message to the appropriate WebSocket connection.
func (c *MintClawChannel) Send(ctx context.Context, msg bus.OutboundMessage) ([]string, error) {
	if !c.IsRunning() {
		return nil, channels.ErrNotRunning
	}
	isThought := outboundMessageIsThought(msg)
	isToolCalls := outboundMessageIsToolCalls(msg)
	msgID := uuid.New().String()

	payload := map[string]any{
		PayloadKeyContent: msg.Content,
		"message_id":      msgID,
	}
	if modelName := strings.TrimSpace(msg.Context.Raw[PayloadKeyModelName]); modelName != "" {
		payload[PayloadKeyModelName] = modelName
	}
	switch {
	case isThought:
		payload[PayloadKeyKind] = MessageKindThought

		// This field is kept solely for compatibility with legacy mintclaw clients that
		// do not yet support the newer "kind" field.
		// DO NOT use it for any purpose other than legacy client compatibility.
		payload[PayloadKeyThought] = true

	case isToolCalls:
		payload[PayloadKeyKind] = MessageKindToolCalls
		if toolCalls, ok := mintclawToolCallsPayload(msg); ok {
			payload[PayloadKeyToolCalls] = toolCalls
		}
	}
	setContextUsagePayload(payload, msg.ContextUsage)
	outMsg := newMessage(TypeMessageCreate, payload)

	if err := c.broadcastToSessionContext(ctx, msg.ChatID, outMsg); err != nil {
		return nil, err
	}
	return []string{msgID}, nil
}

// EditMessage implements channels.MessageEditor.
func (c *MintClawChannel) EditMessage(ctx context.Context, chatID string, messageID string, content string) error {
	return c.editMessage(ctx, chatID, messageID, content, nil)
}

func (c *MintClawChannel) EditMessageWithPayload(
	ctx context.Context,
	chatID string,
	messageID string,
	payload map[string]any,
) error {
	return c.editMessagePayload(ctx, chatID, messageID, payload, nil)
}

// DeleteMessage implements channels.MessageDeleter.
func (c *MintClawChannel) DeleteMessage(ctx context.Context, chatID string, messageID string) error {
	outMsg := newMessage(TypeMessageDelete, map[string]any{
		"message_id": messageID,
	})
	return c.broadcastToSessionContext(ctx, chatID, outMsg)
}

// StartTyping implements channels.TypingCapable.
func (c *MintClawChannel) StartTyping(ctx context.Context, chatID string) (func(), error) {
	startMsg := newMessage(TypeTypingStart, nil)
	if err := c.broadcast(ctx, chatID, startMsg); err != nil {
		return func() {}, err
	}
	return func() {
		stopMsg := newMessage(TypeTypingStop, nil)
		_ = c.broadcast(c.ctx, chatID, stopMsg)
	}, nil
}

// SendPlaceholder implements channels.PlaceholderCapable.
// It sends a placeholder message via the MintClaw Protocol that will later be
// edited to the actual response via EditMessage (channels.MessageEditor).
func (c *MintClawChannel) SendPlaceholder(ctx context.Context, chatID string) (string, error) {
	if !c.bc.Placeholder.Enabled {
		return "", nil
	}

	text := c.bc.Placeholder.GetRandomText()

	msgID := uuid.New().String()
	outMsg := newMessage(TypeMessageCreate, map[string]any{
		PayloadKeyContent:     text,
		PayloadKeyPlaceholder: true,
		"message_id":          msgID,
	})

	if err := c.broadcast(ctx, chatID, outMsg); err != nil {
		return "", err
	}

	return msgID, nil
}

// BeginStream implements channels.StreamingCapable for MintClaw WebUI.
func (c *MintClawChannel) BeginStream(ctx context.Context, chatID string) (channels.Streamer, error) {
	if c == nil || c.config == nil || !c.config.Streaming.Enabled {
		return nil, fmt.Errorf("streaming disabled in config")
	}
	if !c.IsRunning() {
		return nil, channels.ErrNotRunning
	}
	streamCfg := c.config.Streaming.WithDefaults(0, 1)
	return &mintclawStreamer{
		channel:          c,
		chatID:           chatID,
		throttleInterval: time.Duration(streamCfg.ThrottleSeconds) * time.Second,
		minGrowth:        streamCfg.MinGrowthChars,
	}, nil
}

type mintclawStreamer struct {
	channel          *MintClawChannel
	chatID           string
	modelName        string
	turnInputTokens  int
	turnOutputTokens int
	messageID        string
	reasoningID      string
	throttleInterval time.Duration
	minGrowth        int
	lastLen          int
	lastAt           time.Time
	lastContent      string
	reasoningLastLen int
	reasoningLastAt  time.Time
	reasoningContent string
	mu               sync.Mutex
}

func (s *mintclawStreamer) SetModelName(modelName string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.modelName = strings.TrimSpace(modelName)
}

// SetTurnUsage records the real per-turn LLM token usage to emit on finalize.
func (s *mintclawStreamer) SetTurnUsage(inputTokens, outputTokens int) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.turnInputTokens = inputTokens
	s.turnOutputTokens = outputTokens
}

func (s *mintclawStreamer) Update(ctx context.Context, content string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.updateLocked(ctx, content, false, nil)
}

func (s *mintclawStreamer) Finalize(ctx context.Context, content string) error {
	return s.FinalizeWithContext(ctx, content, nil)
}

func (s *mintclawStreamer) FinalizeWithContext(
	ctx context.Context,
	content string,
	contextUsage *bus.ContextUsage,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.updateLocked(ctx, content, true, contextUsage)
}

func (s *mintclawStreamer) UpdateReasoning(ctx context.Context, content string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.updateReasoningLocked(ctx, content, false)
}

func (s *mintclawStreamer) FinalizeReasoning(ctx context.Context, content string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.updateReasoningLocked(ctx, content, true)
}

func (s *mintclawStreamer) Cancel(ctx context.Context) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.channel == nil || s.messageID == "" {
		if s.channel != nil && s.reasoningID != "" {
			_ = s.channel.DeleteMessage(ctx, s.chatID, s.reasoningID)
			s.reasoningID = ""
		}
		return
	}
	_ = s.channel.DeleteMessage(ctx, s.chatID, s.messageID)
	s.messageID = ""
	if s.reasoningID != "" {
		_ = s.channel.DeleteMessage(ctx, s.chatID, s.reasoningID)
		s.reasoningID = ""
	}
}

func (s *mintclawStreamer) updateLocked(
	ctx context.Context,
	content string,
	force bool,
	contextUsage *bus.ContextUsage,
) error {
	if s == nil || s.channel == nil {
		return fmt.Errorf("streamer is not initialized")
	}
	if strings.TrimSpace(content) == "" && s.messageID == "" {
		return nil
	}

	now := time.Now()
	contentLen := len([]rune(content))
	if s.messageID != "" && !force {
		growth := contentLen - s.lastLen
		if now.Sub(s.lastAt) < s.throttleInterval || growth < s.minGrowth {
			return nil
		}
	}

	return s.sendLocked(ctx, content, contextUsage)
}

func (s *mintclawStreamer) updateReasoningLocked(ctx context.Context, content string, force bool) error {
	if s == nil || s.channel == nil {
		return fmt.Errorf("streamer is not initialized")
	}
	if strings.TrimSpace(content) == "" && s.reasoningID == "" {
		return nil
	}

	now := time.Now()
	contentLen := len([]rune(content))
	if s.reasoningID != "" && !force {
		growth := contentLen - s.reasoningLastLen
		if now.Sub(s.reasoningLastAt) < s.throttleInterval || growth < s.minGrowth {
			return nil
		}
	}

	return s.sendReasoningLocked(ctx, content)
}

func (s *mintclawStreamer) sendLocked(ctx context.Context, content string, contextUsage *bus.ContextUsage) error {
	now := time.Now()
	contentLen := len([]rune(content))

	if s.messageID == "" {
		messageID := uuid.New().String()
		payload := map[string]any{
			PayloadKeyContent: content,
			"message_id":      messageID,
		}
		if s.modelName != "" {
			payload[PayloadKeyModelName] = s.modelName
		}
		setContextUsagePayload(payload, contextUsage)
		setTurnUsagePayload(payload, s.turnInputTokens, s.turnOutputTokens)
		outMsg := newMessage(TypeMessageCreate, payload)
		if err := s.channel.broadcast(ctx, s.chatID, outMsg); err != nil {
			return err
		}
		s.messageID = messageID
	} else if content != s.lastContent || contextUsage != nil {
		payload := map[string]any{
			PayloadKeyContent: content,
			"message_id":      s.messageID,
		}
		if s.modelName != "" {
			payload[PayloadKeyModelName] = s.modelName
		}
		setTurnUsagePayload(payload, s.turnInputTokens, s.turnOutputTokens)
		if err := s.channel.editMessagePayload(ctx, s.chatID, s.messageID, payload, contextUsage); err != nil {
			return err
		}
	}

	s.lastContent = content
	s.lastLen = contentLen
	s.lastAt = now
	return nil
}

func (s *mintclawStreamer) sendReasoningLocked(ctx context.Context, content string) error {
	now := time.Now()
	contentLen := len([]rune(content))

	if s.reasoningID == "" {
		reasoningID := uuid.New().String()
		payload := map[string]any{
			PayloadKeyContent: content,
			"message_id":      reasoningID,
			PayloadKeyKind:    MessageKindThought,
			PayloadKeyThought: true,
		}
		if s.modelName != "" {
			payload[PayloadKeyModelName] = s.modelName
		}
		outMsg := newMessage(TypeMessageCreate, payload)
		if err := s.channel.broadcast(ctx, s.chatID, outMsg); err != nil {
			return err
		}
		s.reasoningID = reasoningID
	} else if content != s.reasoningContent {
		payload := map[string]any{
			PayloadKeyContent: content,
			"message_id":      s.reasoningID,
			PayloadKeyKind:    MessageKindThought,
			PayloadKeyThought: true,
		}
		if s.modelName != "" {
			payload[PayloadKeyModelName] = s.modelName
		}
		outMsg := newMessage(TypeMessageUpdate, payload)
		if err := s.channel.broadcast(ctx, s.chatID, outMsg); err != nil {
			return err
		}
	}

	s.reasoningContent = content
	s.reasoningLastLen = contentLen
	s.reasoningLastAt = now
	return nil
}

// SendMedia implements channels.MediaSender for the MintClaw web UI.
// Media is delivered as a normal assistant message carrying structured
// attachments plus an authenticated same-origin download URL.
func (c *MintClawChannel) SendMedia(ctx context.Context, msg bus.OutboundMediaMessage) ([]string, error) {
	if !c.IsRunning() {
		return nil, channels.ErrNotRunning
	}
	store := c.GetMediaStore()
	if store == nil {
		return nil, fmt.Errorf("no media store available: %w", channels.ErrSendFailed)
	}

	attachments := make([]map[string]any, 0, len(msg.Parts))
	caption := ""

	for _, part := range msg.Parts {
		localPath, meta, err := store.ResolveWithMeta(part.Ref)
		if err != nil {
			logger.ErrorCF("mintclaw", "Failed to resolve media ref", map[string]any{
				"ref":   part.Ref,
				"error": err.Error(),
			})
			continue
		}

		filename := strings.TrimSpace(part.Filename)
		if filename == "" {
			filename = strings.TrimSpace(meta.Filename)
		}
		if filename == "" {
			filename = filepath.Base(localPath)
		}

		contentType := strings.TrimSpace(part.ContentType)
		if contentType == "" {
			contentType = strings.TrimSpace(meta.ContentType)
		}
		if contentType == "" {
			contentType = "application/octet-stream"
		}

		attachmentType := strings.TrimSpace(part.Type)
		if attachmentType == "" {
			attachmentType = mintclawInferAttachmentType(filename, contentType)
		}

		attachmentURL, err := mintclawDownloadURLForRef(part.Ref)
		if err != nil {
			logger.ErrorCF("mintclaw", "Failed to build media download URL", map[string]any{
				"ref":   part.Ref,
				"error": err.Error(),
			})
			continue
		}

		attachments = append(attachments, map[string]any{
			"type":         attachmentType,
			"url":          attachmentURL,
			"filename":     filename,
			"content_type": contentType,
		})
		if caption == "" {
			caption = strings.TrimSpace(part.Caption)
		}
	}

	if len(attachments) == 0 {
		return nil, fmt.Errorf("no deliverable media parts: %w", channels.ErrSendFailed)
	}

	msgID := uuid.New().String()
	outMsg := newMessage(TypeMessageCreate, map[string]any{
		PayloadKeyContent: caption,
		"attachments":     attachments,
		"message_id":      msgID,
	})
	if modelName := strings.TrimSpace(msg.Context.Raw[PayloadKeyModelName]); modelName != "" {
		outMsg.Payload[PayloadKeyModelName] = modelName
	}

	if err := c.broadcast(ctx, msg.ChatID, outMsg); err != nil {
		return nil, err
	}
	return []string{msgID}, nil
}

func mintclawDownloadURLForRef(ref string) (string, error) {
	refID, err := mintclawMediaRefID(ref)
	if err != nil {
		return "", err
	}
	return "/mintclaw/media/" + url.PathEscape(refID), nil
}

func mintclawMediaRefID(ref string) (string, error) {
	refID := strings.TrimSpace(strings.TrimPrefix(ref, "media://"))
	if refID == "" || strings.Contains(refID, "/") {
		return "", fmt.Errorf("invalid media ref %q", ref)
	}
	return refID, nil
}

func mintclawInferAttachmentType(filename, contentType string) string {
	contentType = strings.ToLower(strings.TrimSpace(contentType))
	filename = strings.ToLower(strings.TrimSpace(filename))

	switch {
	case strings.HasPrefix(contentType, "image/"):
		return "image"
	case strings.HasPrefix(contentType, "audio/"):
		return "audio"
	case strings.HasPrefix(contentType, "video/"):
		return "video"
	}

	switch ext := filepath.Ext(filename); ext {
	case ".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".svg":
		return "image"
	case ".mp3", ".wav", ".ogg", ".m4a", ".flac", ".aac", ".wma", ".opus":
		return "audio"
	case ".mp4", ".avi", ".mov", ".webm", ".mkv":
		return "video"
	default:
		return "file"
	}
}

func mintclawAllowsInlineDisplay(filename, contentType string) bool {
	contentType = strings.ToLower(strings.TrimSpace(contentType))
	filename = strings.ToLower(strings.TrimSpace(filename))

	if strings.Contains(contentType, "svg") || filepath.Ext(filename) == ".svg" {
		return false
	}

	return mintclawInferAttachmentType(filename, contentType) == "image"
}

func (c *MintClawChannel) handleMediaDownload(w http.ResponseWriter, r *http.Request) {
	if !c.IsRunning() {
		http.Error(w, "channel not running", http.StatusServiceUnavailable)
		return
	}
	if !c.authenticate(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	refID := strings.TrimSpace(strings.TrimPrefix(strings.TrimPrefix(r.URL.Path, "/mintclaw/media/"), "/"))
	if refID == "" {
		http.NotFound(w, r)
		return
	}

	store := c.GetMediaStore()
	if store == nil {
		http.Error(w, "media store unavailable", http.StatusServiceUnavailable)
		return
	}

	localPath, meta, err := store.ResolveWithMeta("media://" + refID)
	if err != nil {
		http.NotFound(w, r)
		return
	}

	file, err := os.Open(localPath)
	if err != nil {
		http.Error(w, "failed to open media", http.StatusInternalServerError)
		return
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		http.Error(w, "failed to stat media", http.StatusInternalServerError)
		return
	}

	filename := strings.TrimSpace(meta.Filename)
	if filename == "" {
		filename = filepath.Base(localPath)
	}
	contentType := strings.TrimSpace(meta.ContentType)
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	dispositionType := "attachment"
	if mintclawAllowsInlineDisplay(filename, contentType) {
		dispositionType = "inline"
	}

	if cd := mime.FormatMediaType(dispositionType, map[string]string{"filename": filename}); cd != "" {
		w.Header().Set("Content-Disposition", cd)
	}
	w.Header().Set("Content-Type", contentType)
	http.ServeContent(w, r, filename, info.ModTime(), file)
}

// broadcast routes through broadcastFn when set (tests), else active connections.
func (c *MintClawChannel) broadcast(ctx context.Context, chatID string, msg MintClawMessage) error {
	if c.broadcastFn != nil {
		return c.broadcastFn(ctx, chatID, msg)
	}
	return c.broadcastToSessionContext(ctx, chatID, msg)
}

// broadcastToSession sends a message to all connections with a matching session.
func (c *MintClawChannel) broadcastToSession(chatID string, msg MintClawMessage) error {
	return c.broadcastToSessionContext(context.Background(), chatID, msg)
}

func (c *MintClawChannel) broadcastToSessionContext(ctx context.Context, chatID string, msg MintClawMessage) error {
	// chatID format: "mintclaw:<sessionID>"
	sessionID := strings.TrimPrefix(chatID, "mintclaw:")
	msg.SessionID = sessionID
	return c.broadcastToConnectionsContext(ctx, sessionID, msg, c.sessionConnectionsSnapshot(sessionID))
}

func (c *MintClawChannel) broadcastToConnectionsContext(
	ctx context.Context,
	sessionID string,
	msg MintClawMessage,
	connections []*mintclawConn,
) error {
	msg.SessionID = sessionID
	type connectionWriteResult struct {
		err error
	}
	results := make(chan connectionWriteResult, len(connections))
	for _, pc := range connections {
		pending := pc.enqueueJSON(ctx, msg)
		go func(pc *mintclawConn, pending <-chan error) {
			err := <-pending
			if err != nil {
				logger.DebugCF("mintclaw", "Write to connection failed", map[string]any{
					"conn_id": pc.id,
					"error":   err.Error(),
				})
			}
			results <- connectionWriteResult{err: err}
		}(pc, pending)
	}

	var canceled, timedOut bool
	for range connections {
		result := <-results
		if result.err != nil {
			canceled = canceled || errors.Is(result.err, context.Canceled)
			timedOut = timedOut || errors.Is(result.err, context.DeadlineExceeded)
		} else {
			// Delivery to any active peer satisfies the session contract. Other
			// already-started writes remain bounded and continue best-effort.
			return nil
		}
	}

	err := fmt.Errorf("no active connections for session %s: %w", sessionID, channels.ErrSendFailed)
	if canceled {
		err = errors.Join(err, context.Canceled)
	}
	if timedOut {
		err = errors.Join(err, context.DeadlineExceeded)
	}
	return err
}

// handleWebSocket upgrades the HTTP connection and manages the WebSocket lifecycle.
func (c *MintClawChannel) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	if !c.IsRunning() {
		http.Error(w, "channel not running", http.StatusServiceUnavailable)
		return
	}

	// Authenticate
	if !c.authenticate(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	// Check connection limit
	maxConns := c.config.MaxConnections
	if maxConns <= 0 {
		maxConns = 100
	}
	if c.currentConnCount() >= maxConns {
		http.Error(w, "too many connections", http.StatusServiceUnavailable)
		return
	}

	// Echo the matched subprotocol back so the browser accepts the upgrade.
	var responseHeader http.Header
	if proto := c.matchedSubprotocol(r); proto != "" {
		responseHeader = http.Header{"Sec-WebSocket-Protocol": {proto}}
	}

	conn, err := c.upgrader.Upgrade(w, r, responseHeader)
	if err != nil {
		logger.ErrorCF("mintclaw", "WebSocket upgrade failed", map[string]any{
			"error": err.Error(),
		})
		return
	}

	// Determine session ID from query param or generate one
	sessionID := r.URL.Query().Get("session_id")
	if sessionID == "" {
		sessionID = uuid.New().String()
	}

	pc, err := c.createAndAddConnection(conn, sessionID, maxConns)
	if err != nil {
		_ = conn.WriteControl(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseTryAgainLater, "too many connections"),
			time.Now().Add(2*time.Second),
		)
		_ = conn.Close()
		return
	}

	logger.InfoCF("mintclaw", "WebSocket client connected", map[string]any{
		"conn_id":    pc.id,
		"session_id": sessionID,
	})

	go c.readLoop(pc)
}

// authenticate checks the request for a valid token:
//  1. Authorization: Bearer <token> header
//  2. Sec-WebSocket-Protocol "token.<value>" (for browsers that can't set headers)
//  3. Query parameter "token" (only when AllowTokenQuery is on)
func (c *MintClawChannel) authenticate(r *http.Request) bool {
	token := c.config.Token.String()
	if token == "" {
		return false
	}

	// Check Authorization header
	auth := r.Header.Get("Authorization")
	if after, ok := strings.CutPrefix(auth, "Bearer "); ok {
		if after == token {
			return true
		}
	}

	// Check Sec-WebSocket-Protocol subprotocol ("token.<value>")
	if c.matchedSubprotocol(r) != "" {
		return true
	}

	// Check query parameter only when explicitly allowed
	if c.config.AllowTokenQuery {
		if r.URL.Query().Get("token") == token {
			return true
		}
	}

	return false
}

// matchedSubprotocol returns the "token.<value>" subprotocol that matches
// the configured token, or "" if none do.
func (c *MintClawChannel) matchedSubprotocol(r *http.Request) string {
	token := c.config.Token.String()
	for _, proto := range websocket.Subprotocols(r) {
		if after, ok := strings.CutPrefix(proto, "token."); ok && after == token {
			return proto
		}
	}
	return ""
}

// readLoop reads messages from a WebSocket connection.
func (c *MintClawChannel) readLoop(pc *mintclawConn) {
	defer func() {
		pc.close()
		if removed := c.removeConnection(pc.id); removed != nil {
			logger.InfoCF("mintclaw", "WebSocket client disconnected", map[string]any{
				"conn_id":    removed.id,
				"session_id": removed.sessionID,
			})
		}
	}()

	readTimeout := time.Duration(c.config.ReadTimeout) * time.Second
	if readTimeout <= 0 {
		readTimeout = 60 * time.Second
	}

	_ = pc.conn.SetReadDeadline(time.Now().Add(readTimeout))
	pc.conn.SetPongHandler(func(appData string) error {
		_ = pc.conn.SetReadDeadline(time.Now().Add(readTimeout))
		return nil
	})

	// Start ping ticker
	pingInterval := time.Duration(c.config.PingInterval) * time.Second
	if pingInterval <= 0 {
		pingInterval = 30 * time.Second
	}
	go c.pingLoop(pc, pingInterval)

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		_, rawMsg, err := pc.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				logger.DebugCF("mintclaw", "WebSocket read error", map[string]any{
					"conn_id": pc.id,
					"error":   err.Error(),
				})
			}
			return
		}

		_ = pc.conn.SetReadDeadline(time.Now().Add(readTimeout))

		var msg MintClawMessage
		if err := json.Unmarshal(rawMsg, &msg); err != nil {
			errMsg := newError("invalid_message", "failed to parse message")
			pc.writeJSON(c.ctx, errMsg)
			continue
		}

		c.handleMessage(pc, msg)
	}
}

// pingLoop sends periodic ping frames to keep the connection alive.
func (c *MintClawChannel) pingLoop(pc *mintclawConn, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			if pc.closed.Load() {
				return
			}
			err := pc.writeMessage(c.ctx, websocket.PingMessage, nil)
			if err != nil {
				return
			}
		}
	}
}

// handleMessage processes an inbound MintClaw Protocol message.
func (c *MintClawChannel) handleMessage(pc *mintclawConn, msg MintClawMessage) {
	switch msg.Type {
	case TypePing:
		pong := newMessage(TypePong, nil)
		pong.ID = msg.ID
		pc.writeJSON(c.ctx, pong)

	case TypeMessageSend:
		c.handleMessageSend(pc, msg)

	case TypeMediaSend:
		c.handleMessageSend(pc, msg)

	default:
		errMsg := newError("unknown_type", fmt.Sprintf("unknown message type: %s", msg.Type))
		pc.writeJSON(c.ctx, errMsg)
	}
}

// handleMessageSend processes an inbound message.send from a client.
func (c *MintClawChannel) handleMessageSend(pc *mintclawConn, msg MintClawMessage) {
	content, _ := msg.Payload["content"].(string)
	media, err := parseInlineImageMedia(msg.Payload)
	if err != nil {
		errMsg := newErrorWithPayload("invalid_media", err.Error(), map[string]any{
			"request_id": msg.ID,
		})
		pc.writeJSON(c.ctx, errMsg)
		return
	}

	if strings.TrimSpace(content) == "" && len(media) == 0 {
		errMsg := newErrorWithPayload("empty_content", "message content is empty", map[string]any{
			"request_id": msg.ID,
		})
		pc.writeJSON(c.ctx, errMsg)
		return
	}

	sessionID := msg.SessionID
	if sessionID == "" {
		sessionID = pc.sessionID
	}

	chatID := "mintclaw:" + sessionID
	senderID := "mintclaw-user"

	metadata := map[string]string{
		"platform":   "mintclaw",
		"session_id": sessionID,
		"conn_id":    pc.id,
	}

	logger.DebugCF("mintclaw", "Received message", map[string]any{
		"session_id": sessionID,
		"preview":    truncate(content, 50),
		"media":      len(media),
	})

	sender := bus.SenderInfo{
		Platform:    "mintclaw",
		PlatformID:  senderID,
		CanonicalID: identity.BuildCanonicalID("mintclaw", senderID),
	}

	if !c.IsAllowedSender(sender) {
		return
	}

	inboundCtx := bus.InboundContext{
		Channel:   "mintclaw",
		ChatID:    chatID,
		ChatType:  "direct",
		SenderID:  senderID,
		MessageID: msg.ID,
		Raw:       metadata,
	}

	c.HandleInboundContext(c.ctx, chatID, content, media, inboundCtx, sender)
}

// truncate truncates a string to maxLen runes.
func truncate(s string, maxLen int) string {
	runes := []rune(s)
	if len(runes) <= maxLen {
		return s
	}
	return string(runes[:maxLen]) + "..."
}

func parseInlineImageMedia(payload map[string]any) ([]string, error) {
	if len(payload) == 0 {
		return nil, nil
	}

	media, err := parseInlineImageValues(payload["media"])
	if err != nil {
		return nil, err
	}

	attachments, err := parseInlineImageAttachments(payload["attachments"])
	if err != nil {
		return nil, err
	}
	media = append(media, attachments...)

	return media, nil
}

func parseInlineImageValues(raw any) ([]string, error) {
	if raw == nil {
		return nil, nil
	}
	switch values := raw.(type) {
	case []any:
		media := make([]string, 0, len(values))
		for i, item := range values {
			value, err := inlineImageValue(item)
			if err != nil {
				return nil, fmt.Errorf("media[%d]: %w", i, err)
			}
			if err := validateInlineImageDataURL(value); err != nil {
				return nil, fmt.Errorf("media[%d]: %w", i, err)
			}
			media = append(media, value)
		}
		return media, nil
	case []string:
		media := make([]string, 0, len(values))
		for i, value := range values {
			value = strings.TrimSpace(value)
			if err := validateInlineImageDataURL(value); err != nil {
				return nil, fmt.Errorf("media[%d]: %w", i, err)
			}
			media = append(media, value)
		}
		return media, nil
	case string:
		value := strings.TrimSpace(values)
		if err := validateInlineImageDataURL(value); err != nil {
			return nil, err
		}
		return []string{value}, nil
	default:
		return nil, fmt.Errorf("media must be a string or array of strings")
	}
}

func parseInlineImageAttachments(raw any) ([]string, error) {
	if raw == nil {
		return nil, nil
	}

	values, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("attachments must be an array")
	}

	media := make([]string, 0, len(values))
	for i, item := range values {
		attachment, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("attachments[%d]: attachment must be an object", i)
		}

		attachmentType, _ := attachment["type"].(string)
		attachmentType = strings.ToLower(strings.TrimSpace(attachmentType))
		if attachmentType != "" && attachmentType != "image" {
			continue
		}

		value, err := inlineImageValue(attachment)
		if err != nil {
			if attachmentType == "image" {
				return nil, fmt.Errorf("attachments[%d]: %w", i, err)
			}
			continue
		}
		if !strings.HasPrefix(value, "data:") {
			continue
		}
		if err := validateInlineImageDataURL(value); err != nil {
			return nil, fmt.Errorf("attachments[%d]: %w", i, err)
		}
		media = append(media, value)
	}
	return media, nil
}

func inlineImageValue(item any) (string, error) {
	switch value := item.(type) {
	case string:
		value = strings.TrimSpace(value)
		if value == "" {
			return "", fmt.Errorf("image payload is empty")
		}
		return value, nil
	case map[string]any:
		for _, key := range []string{"url", "data_url"} {
			if raw, ok := value[key].(string); ok && strings.TrimSpace(raw) != "" {
				return strings.TrimSpace(raw), nil
			}
		}
		return "", fmt.Errorf("image payload must include url or data_url")
	default:
		return "", fmt.Errorf("image payload must be a string or object")
	}
}

func validateInlineImageDataURL(mediaURL string) error {
	if mediaURL == "" {
		return fmt.Errorf("image payload is empty")
	}
	if !strings.HasPrefix(mediaURL, "data:image/") {
		return fmt.Errorf("only inline image data URLs are supported")
	}

	header, data, found := strings.Cut(mediaURL, ",")
	if !found || strings.TrimSpace(data) == "" {
		return fmt.Errorf("image data URL is malformed")
	}
	if !strings.Contains(header, ";base64") {
		return fmt.Errorf("image data URL must be base64 encoded")
	}
	mimeType, _, _ := strings.Cut(strings.TrimPrefix(header, "data:"), ";")
	if _, ok := allowedInlineImageMIMETypes[mimeType]; !ok {
		return fmt.Errorf("unsupported image format: %s", mimeType)
	}

	data = strings.TrimSpace(data)
	if base64.StdEncoding.DecodedLen(len(data)) > config.DefaultMaxMediaSize {
		return fmt.Errorf("image exceeds %d byte limit", config.DefaultMaxMediaSize)
	}
	if _, err := base64.StdEncoding.DecodeString(data); err != nil {
		return fmt.Errorf("invalid base64 image data")
	}

	return nil
}

// setContextUsagePayload adds context window usage stats to a mintclaw payload.
func setContextUsagePayload(payload map[string]any, u *bus.ContextUsage) {
	if u == nil {
		return
	}
	payload["context_usage"] = map[string]any{
		"used_tokens":         u.UsedTokens,
		"total_tokens":        u.TotalTokens,
		"history_tokens":      u.HistoryTokens,
		"compress_at_tokens":  u.CompressAtTokens,
		"summarize_at_tokens": u.SummarizeAtTokens,
		"used_percent":        u.UsedPercent,
	}
}

// setTurnUsagePayload attaches real per-turn LLM token usage to the payload.
// Input and output are kept separate (billed at different rates); total is a
// convenience sum. Omitted entirely when both counts are zero.
func setTurnUsagePayload(payload map[string]any, inputTokens, outputTokens int) {
	if inputTokens <= 0 && outputTokens <= 0 {
		return
	}
	payload[PayloadKeyUsage] = map[string]any{
		"input_tokens":  inputTokens,
		"output_tokens": outputTokens,
		"total_tokens":  inputTokens + outputTokens,
	}
}

func mintclawToolCallsPayload(msg bus.OutboundMessage) ([]utils.VisibleToolCall, bool) {
	raw := strings.TrimSpace(msg.Context.Raw[PayloadKeyToolCalls])
	if raw == "" {
		return nil, false
	}

	var toolCalls []utils.VisibleToolCall
	if err := json.Unmarshal([]byte(raw), &toolCalls); err != nil || len(toolCalls) == 0 {
		return nil, false
	}
	return toolCalls, true
}

func (c *MintClawChannel) editMessage(
	ctx context.Context,
	chatID string,
	messageID string,
	content string,
	contextUsage *bus.ContextUsage,
) error {
	return c.editMessagePayload(ctx, chatID, messageID, map[string]any{
		PayloadKeyContent: content,
	}, contextUsage)
}

func (c *MintClawChannel) editMessagePayload(
	ctx context.Context,
	chatID string,
	messageID string,
	payload map[string]any,
	contextUsage *bus.ContextUsage,
) error {
	if payload == nil {
		payload = map[string]any{}
	}
	normalized := make(map[string]any, len(payload)+1)
	for key, value := range payload {
		normalized[key] = value
	}
	if _, ok := normalized[PayloadKeyContent]; !ok {
		normalized[PayloadKeyContent] = ""
	}
	normalized["message_id"] = messageID
	setContextUsagePayload(normalized, contextUsage)
	outMsg := newMessage(TypeMessageUpdate, normalized)
	return c.broadcastToSessionContext(ctx, chatID, outMsg)
}
