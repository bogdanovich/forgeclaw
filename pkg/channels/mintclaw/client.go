package mintclaw

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"github.com/bogdanovich/mintclaw/pkg/bus"
	"github.com/bogdanovich/mintclaw/pkg/channels"
	"github.com/bogdanovich/mintclaw/pkg/config"
	"github.com/bogdanovich/mintclaw/pkg/identity"
	"github.com/bogdanovich/mintclaw/pkg/logger"
)

// MintClawClientChannel connects to a remote MintClaw Protocol WebSocket server.
type MintClawClientChannel struct {
	*channels.BaseChannel
	config *config.MintClawClientSettings
	conn   *mintclawConn
	mu     sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc
}

// NewMintClawClientChannel creates a new MintClaw Protocol client channel.
func NewMintClawClientChannel(
	bc *config.Channel,
	cfg *config.MintClawClientSettings,
	messageBus *bus.MessageBus,
) (*MintClawClientChannel, error) {
	if cfg.URL == "" {
		return nil, fmt.Errorf("mintclaw_client url is required")
	}

	base := channels.NewBaseChannel("mintclaw_client", cfg, messageBus, bc.AllowFrom)

	return &MintClawClientChannel{
		BaseChannel: base,
		config:      cfg,
	}, nil
}

// Start dials the remote server and begins reading.
func (c *MintClawClientChannel) Start(ctx context.Context) error {
	logger.InfoC("mintclaw_client", "Starting MintClaw Client channel")
	c.ctx, c.cancel = context.WithCancel(ctx)

	if err := c.dial(); err != nil {
		c.cancel()
		return fmt.Errorf("mintclaw_client initial connect: %w", err)
	}

	c.SetRunning(true)
	go c.reconnectLoop()

	logger.InfoCF("mintclaw_client", "Connected", map[string]any{"url": c.config.URL})
	return nil
}

// Stop closes the connection.
func (c *MintClawClientChannel) Stop(ctx context.Context) error {
	logger.InfoC("mintclaw_client", "Stopping MintClaw Client channel")
	c.SetRunning(false)
	if c.cancel != nil {
		c.cancel()
	}
	c.mu.Lock()
	if c.conn != nil {
		c.conn.close()
	}
	c.mu.Unlock()
	logger.InfoC("mintclaw_client", "MintClaw Client channel stopped")
	return nil
}

func (c *MintClawClientChannel) dial() error {
	header := http.Header{}
	if c.config.Token.String() != "" {
		header.Set("Authorization", "Bearer "+c.config.Token.String())
	}

	ws, resp, err := websocket.DefaultDialer.DialContext(c.ctx, c.config.URL, header)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	if err != nil {
		return err
	}

	connCtx, connCancel := context.WithCancel(c.ctx)

	pc := &mintclawConn{
		id:        uuid.New().String(),
		conn:      ws,
		sessionID: c.config.SessionID,
		cancel:    connCancel,
	}
	if pc.sessionID == "" {
		pc.sessionID = uuid.New().String()
	}

	c.mu.Lock()
	c.conn = pc
	c.mu.Unlock()

	go c.readLoop(connCtx, pc)
	return nil
}

// reconnectLoop re-dials when the connection drops.
func (c *MintClawClientChannel) reconnectLoop() {
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		c.mu.Lock()
		pc := c.conn
		c.mu.Unlock()

		if pc == nil || pc.closed.Load() {
			backoff := 5 * time.Second
			logger.InfoC("mintclaw_client", "Reconnecting...")
			if err := c.dial(); err != nil {
				logger.WarnCF("mintclaw_client", "Reconnect failed", map[string]any{
					"error": err.Error(),
				})
				select {
				case <-c.ctx.Done():
					return
				case <-time.After(backoff):
				}
				continue
			}
			logger.InfoC("mintclaw_client", "Reconnected")
		}

		select {
		case <-c.ctx.Done():
			return
		case <-time.After(1 * time.Second):
		}
	}
}

func (c *MintClawClientChannel) readLoop(connCtx context.Context, pc *mintclawConn) {
	defer pc.close()

	readTimeout := time.Duration(c.config.ReadTimeout) * time.Second
	if readTimeout <= 0 {
		readTimeout = 60 * time.Second
	}

	_ = pc.conn.SetReadDeadline(time.Now().Add(readTimeout))
	pc.conn.SetPongHandler(func(string) error {
		return pc.conn.SetReadDeadline(time.Now().Add(readTimeout))
	})

	pingInterval := time.Duration(c.config.PingInterval) * time.Second
	if pingInterval <= 0 {
		pingInterval = 30 * time.Second
	}
	go c.pingLoop(connCtx, pc, pingInterval)

	for {
		select {
		case <-connCtx.Done():
			return
		default:
		}

		_, raw, err := pc.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(
				err,
				websocket.CloseGoingAway,
				websocket.CloseNormalClosure,
			) {
				logger.DebugCF("mintclaw_client", "Read error", map[string]any{
					"error": err.Error(),
				})
			}
			return
		}

		_ = pc.conn.SetReadDeadline(time.Now().Add(readTimeout))

		var msg MintClawMessage
		if err := json.Unmarshal(raw, &msg); err != nil {
			continue
		}

		c.handleInbound(pc, msg)
	}
}

func (c *MintClawClientChannel) pingLoop(connCtx context.Context, pc *mintclawConn, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-connCtx.Done():
			return
		case <-ticker.C:
			if pc.closed.Load() {
				return
			}
			err := pc.writeMessage(connCtx, websocket.PingMessage, nil)
			if err != nil {
				return
			}
		}
	}
}

// handleInbound processes messages from the remote server.
// In client mode the server sends message.create (responses) and the client
// sends message.send (user input). We treat message.create from the server
// as inbound user messages to feed into the agent loop.
func (c *MintClawClientChannel) handleInbound(pc *mintclawConn, msg MintClawMessage) {
	switch msg.Type {
	case TypePong:
		// response to our ping, ignore
	case TypeMessageCreate:
		// Server sent us a message — treat as inbound
		c.handleServerMessage(pc, msg)
	case TypeMediaCreate:
		c.handleServerMessage(pc, msg)
	default:
		logger.DebugCF("mintclaw_client", "Ignoring message type", map[string]any{
			"type": msg.Type,
		})
	}
}

func (c *MintClawClientChannel) handleServerMessage(pc *mintclawConn, msg MintClawMessage) {
	if isThoughtPayload(msg.Payload) {
		return
	}

	content, _ := msg.Payload[PayloadKeyContent].(string)
	media, err := parseInlineImageMedia(msg.Payload)
	if err != nil {
		logger.WarnCF("mintclaw_client", "Ignoring invalid media payload", map[string]any{
			"error": err.Error(),
		})
		if strings.TrimSpace(content) == "" {
			return
		}
		media = nil
	}
	if strings.TrimSpace(content) == "" && len(media) == 0 {
		return
	}

	sessionID := msg.SessionID
	if sessionID == "" {
		sessionID = pc.sessionID
	}

	chatID := "mintclaw_client:" + sessionID
	senderID := "mintclaw-remote"
	sender := bus.SenderInfo{
		Platform:    "mintclaw_client",
		PlatformID:  senderID,
		CanonicalID: identity.BuildCanonicalID("mintclaw_client", senderID),
	}

	if !c.IsAllowedSender(sender) {
		return
	}

	inboundCtx := bus.InboundContext{
		Channel:   "mintclaw_client",
		ChatID:    chatID,
		ChatType:  "direct",
		SenderID:  senderID,
		MessageID: msg.ID,
		Raw: map[string]string{
			"platform":   "mintclaw_client",
			"session_id": sessionID,
		},
	}

	c.HandleInboundContext(c.ctx, chatID, content, media, inboundCtx, sender)
}

// Send sends a message to the remote server.
func (c *MintClawClientChannel) Send(ctx context.Context, msg bus.OutboundMessage) ([]string, error) {
	if !c.IsRunning() {
		return nil, channels.ErrNotRunning
	}
	c.mu.Lock()
	pc := c.conn
	c.mu.Unlock()
	if pc == nil || pc.closed.Load() {
		return nil, channels.ErrSendFailed
	}

	outMsg := newMessage(TypeMessageSend, map[string]any{
		PayloadKeyContent: msg.Content,
	})
	outMsg.SessionID = strings.TrimPrefix(msg.ChatID, "mintclaw_client:")
	return nil, pc.writeJSON(ctx, outMsg)
}

// StartTyping implements channels.TypingCapable.
func (c *MintClawClientChannel) StartTyping(ctx context.Context, chatID string) (func(), error) {
	c.mu.Lock()
	pc := c.conn
	c.mu.Unlock()
	if pc == nil || pc.closed.Load() {
		return func() {}, nil
	}

	startMsg := newMessage(TypeTypingStart, nil)
	startMsg.SessionID = strings.TrimPrefix(chatID, "mintclaw_client:")
	if err := pc.writeJSON(ctx, startMsg); err != nil {
		return func() {}, err
	}
	return func() {
		c.mu.Lock()
		currentPC := c.conn
		c.mu.Unlock()
		if currentPC == nil {
			return
		}
		stopMsg := newMessage(TypeTypingStop, nil)
		stopMsg.SessionID = strings.TrimPrefix(chatID, "mintclaw_client:")
		currentPC.writeJSON(c.ctx, stopMsg)
	}, nil
}
