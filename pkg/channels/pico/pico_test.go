package pico

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/channels"
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/media"
)

func newTestPicoChannel(t *testing.T) *PicoChannel {
	t.Helper()

	bc := &config.Channel{Type: config.ChannelPico, Enabled: true}
	cfg := &config.PicoSettings{}
	cfg.SetToken("test-token")
	ch, err := NewPicoChannel(bc, cfg, bus.NewMessageBus())
	if err != nil {
		t.Fatalf("NewPicoChannel: %v", err)
	}

	ch.ctx = context.Background()
	return ch
}

func TestHandleMessageSend_ForwardsMessageMetadata(t *testing.T) {
	msgBus := bus.NewMessageBus()
	bc := &config.Channel{Type: config.ChannelPico, Enabled: true}
	cfg := &config.PicoSettings{}
	cfg.SetToken("test-token")
	ch, err := NewPicoChannel(bc, cfg, msgBus)
	if err != nil {
		t.Fatalf("NewPicoChannel: %v", err)
	}
	ch.ctx = context.Background()

	ch.handleMessageSend(&picoConn{id: "conn-1", sessionID: "sess-1"}, PicoMessage{
		Type:      TypeMessageSend,
		ID:        "msg-1",
		SessionID: "sess-1",
		Payload: map[string]any{
			PayloadKeyContent: "hello",
		},
	})

	select {
	case inbound := <-msgBus.InboundChan():
		if inbound.Content != "hello" {
			t.Fatalf("content = %q, want hello", inbound.Content)
		}
		if got := inbound.Context.Raw["session_id"]; got != "sess-1" {
			t.Fatalf("session_id raw = %q, want sess-1", got)
		}
	case <-time.After(time.Second):
		t.Fatal("expected inbound pico message")
	}
}

func TestSend_ThoughtMessageIncludesMetadata(t *testing.T) {
	ch := newTestPicoChannel(t)

	if err := ch.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer ch.Stop(context.Background())

	clientConn, received, cleanup := newTestPicoWebSocket(t)
	defer cleanup()
	ch.addConnForTest(&picoConn{id: "conn-1", conn: clientConn, sessionID: "sess-1"})

	if _, err := ch.Send(context.Background(), bus.OutboundMessage{
		ChatID:  "pico:sess-1",
		Content: "thinking trace",
		Context: bus.InboundContext{
			Channel: "pico",
			ChatID:  "pico:sess-1",
			Raw: map[string]string{
				"message_kind":      MessageKindThought,
				PayloadKeyModelName: "gpt-5.4-mini",
			},
		},
	}); err != nil {
		t.Fatalf("Send(thought) error = %v", err)
	}

	select {
	case msg := <-received:
		if msg.Type != TypeMessageCreate {
			t.Fatalf("thought message type = %q, want %q", msg.Type, TypeMessageCreate)
		}
		payload := msg.Payload
		if got := payload[PayloadKeyContent]; got != "thinking trace" {
			t.Fatalf("thought content = %#v, want %q", got, "thinking trace")
		}
		if got := payload[PayloadKeyKind]; got != MessageKindThought {
			t.Fatalf("thought kind = %#v, want %q", got, MessageKindThought)
		}
		if got := payload[PayloadKeyModelName]; got != "gpt-5.4-mini" {
			t.Fatalf("thought model_name = %#v, want %q", got, "gpt-5.4-mini")
		}
		if got := payload["message_id"]; got == nil || got == "" {
			t.Fatalf("thought message_id = %#v, want non-empty id", got)
		}
	case <-time.After(time.Second):
		t.Fatal("expected thought message to be delivered")
	}
}

func TestSend_ToolCallsMessageIncludesModelName(t *testing.T) {
	ch := newTestPicoChannel(t)

	if err := ch.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer ch.Stop(context.Background())

	clientConn, received, cleanup := newTestPicoWebSocket(t)
	defer cleanup()
	ch.addConnForTest(&picoConn{id: "conn-1", conn: clientConn, sessionID: "sess-1"})

	if _, err := ch.Send(context.Background(), bus.OutboundMessage{
		ChatID:  "pico:sess-1",
		Content: "",
		Context: bus.InboundContext{
			Channel: "pico",
			ChatID:  "pico:sess-1",
			Raw: map[string]string{
				"message_kind":      MessageKindToolCalls,
				PayloadKeyModelName: "gpt-5.4",
				PayloadKeyToolCalls: `[{"id":"call_1","type":"function","function":{"name":"read_file","arguments":"{\"path\":\"README.md\"}"}}]`,
			},
		},
	}); err != nil {
		t.Fatalf("Send(tool_calls) error = %v", err)
	}

	select {
	case msg := <-received:
		if msg.Type != TypeMessageCreate {
			t.Fatalf("tool_calls message type = %q, want %q", msg.Type, TypeMessageCreate)
		}
		payload := msg.Payload
		if got := payload[PayloadKeyKind]; got != MessageKindToolCalls {
			t.Fatalf("tool_calls kind = %#v, want %q", got, MessageKindToolCalls)
		}
		if got := payload[PayloadKeyModelName]; got != "gpt-5.4" {
			t.Fatalf("tool_calls model_name = %#v, want %q", got, "gpt-5.4")
		}
		if _, ok := payload[PayloadKeyToolCalls].([]any); !ok {
			t.Fatalf("tool_calls payload = %#v, want parsed array", payload[PayloadKeyToolCalls])
		}
	case <-time.After(time.Second):
		t.Fatal("expected tool_calls message to be delivered")
	}
}

func TestSendPlaceholder_EmitsNormalMessageWithoutKind(t *testing.T) {
	ch := newTestPicoChannel(t)
	ch.bc.Placeholder.Enabled = true

	if err := ch.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer ch.Stop(context.Background())

	clientConn, received, cleanup := newTestPicoWebSocket(t)
	defer cleanup()
	ch.addConnForTest(&picoConn{id: "conn-1", conn: clientConn, sessionID: "sess-1"})

	msgID, err := ch.SendPlaceholder(context.Background(), "pico:sess-1")
	if err != nil {
		t.Fatalf("SendPlaceholder() error = %v", err)
	}
	if msgID == "" {
		t.Fatal("expected placeholder message id")
	}

	select {
	case msg := <-received:
		if msg.Type != TypeMessageCreate {
			t.Fatalf("placeholder message type = %q, want %q", msg.Type, TypeMessageCreate)
		}
		payload := msg.Payload
		if got := payload["message_id"]; got != msgID {
			t.Fatalf("placeholder message_id = %#v, want %q", got, msgID)
		}
		if got := payload[PayloadKeyContent]; got != "Thinking..." {
			t.Fatalf("placeholder content = %#v, want %q", got, "Thinking...")
		}
		if got := payload[PayloadKeyPlaceholder]; got != true {
			t.Fatalf("placeholder marker = %#v, want true", got)
		}
		if got, ok := payload[PayloadKeyKind]; ok {
			t.Fatalf("placeholder kind = %#v, want absent", got)
		}
	case <-time.After(time.Second):
		t.Fatal("expected placeholder message to be delivered")
	}
}

func TestBeginStream_CreatesAndUpdatesSameMessage(t *testing.T) {
	ch := newTestPicoChannel(t)
	ch.config.Streaming = config.StreamingConfig{
		Enabled:         true,
		ThrottleSeconds: 1,
		MinGrowthChars:  1,
	}
	if err := ch.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer ch.Stop(context.Background())

	clientConn, received, cleanup := newTestPicoWebSocket(t)
	defer cleanup()
	ch.addConnForTest(&picoConn{id: "conn-1", conn: clientConn, sessionID: "sess-1"})

	streamer, err := ch.BeginStream(context.Background(), "pico:sess-1")
	if err != nil {
		t.Fatalf("BeginStream() error = %v", err)
	}
	if setter, ok := streamer.(interface{ SetModelName(modelName string) }); ok {
		setter.SetModelName("gpt-5.4")
	}
	if err := streamer.Update(context.Background(), "hello"); err != nil {
		t.Fatalf("Update(first) error = %v", err)
	}
	first := mustReceivePicoMessage(t, received)
	if first.Type != TypeMessageCreate {
		t.Fatalf("first type = %q, want %q", first.Type, TypeMessageCreate)
	}
	msgID, _ := first.Payload["message_id"].(string)
	if msgID == "" {
		t.Fatalf("first message_id = %#v, want non-empty", first.Payload["message_id"])
	}
	if got := first.Payload[PayloadKeyContent]; got != "hello" {
		t.Fatalf("first content = %#v, want hello", got)
	}
	if got := first.Payload[PayloadKeyModelName]; got != "gpt-5.4" {
		t.Fatalf("first model_name = %#v, want %q", got, "gpt-5.4")
	}

	rawStreamer := streamer.(*picoStreamer)
	rawStreamer.mu.Lock()
	rawStreamer.lastAt = time.Now().Add(-2 * time.Second)
	rawStreamer.mu.Unlock()
	secondContent := "hello world with enough growth to pass the default streaming threshold"
	if err := streamer.Update(context.Background(), secondContent); err != nil {
		t.Fatalf("Update(second) error = %v", err)
	}
	second := mustReceivePicoMessage(t, received)
	if second.Type != TypeMessageUpdate {
		t.Fatalf("second type = %q, want %q", second.Type, TypeMessageUpdate)
	}
	if got := second.Payload["message_id"]; got != msgID {
		t.Fatalf("second message_id = %#v, want %q", got, msgID)
	}
	if got := second.Payload[PayloadKeyContent]; got != secondContent {
		t.Fatalf("second content = %#v, want %q", got, secondContent)
	}
	if got := second.Payload[PayloadKeyModelName]; got != "gpt-5.4" {
		t.Fatalf("second model_name = %#v, want %q", got, "gpt-5.4")
	}
}

func TestBeginStream_DefaultStreamingShowsSmallIncrements(t *testing.T) {
	ch := newTestPicoChannel(t)
	ch.config.Streaming = config.StreamingConfig{Enabled: true}
	if err := ch.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer ch.Stop(context.Background())

	clientConn, received, cleanup := newTestPicoWebSocket(t)
	defer cleanup()
	ch.addConnForTest(&picoConn{id: "conn-1", conn: clientConn, sessionID: "sess-1"})

	streamer, err := ch.BeginStream(context.Background(), "pico:sess-1")
	if err != nil {
		t.Fatalf("BeginStream() error = %v", err)
	}
	if err := streamer.Update(context.Background(), "h"); err != nil {
		t.Fatalf("Update(first) error = %v", err)
	}
	first := mustReceivePicoMessage(t, received)
	if first.Type != TypeMessageCreate {
		t.Fatalf("first type = %q, want %q", first.Type, TypeMessageCreate)
	}
	msgID, _ := first.Payload["message_id"].(string)
	if msgID == "" {
		t.Fatalf("first message_id = %#v, want non-empty", first.Payload["message_id"])
	}

	if err := streamer.Update(context.Background(), "he"); err != nil {
		t.Fatalf("Update(second) error = %v", err)
	}
	second := mustReceivePicoMessage(t, received)
	if second.Type != TypeMessageUpdate {
		t.Fatalf("second type = %q, want %q", second.Type, TypeMessageUpdate)
	}
	if got := second.Payload["message_id"]; got != msgID {
		t.Fatalf("second message_id = %#v, want %q", got, msgID)
	}
	if got := second.Payload[PayloadKeyContent]; got != "he" {
		t.Fatalf("second content = %#v, want he", got)
	}
}

func TestBeginStream_StreamsReasoningAsThoughtUpdates(t *testing.T) {
	ch := newTestPicoChannel(t)
	ch.config.Streaming = config.StreamingConfig{Enabled: true}
	if err := ch.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer ch.Stop(context.Background())

	clientConn, received, cleanup := newTestPicoWebSocket(t)
	defer cleanup()
	ch.addConnForTest(&picoConn{id: "conn-1", conn: clientConn, sessionID: "sess-1"})

	streamer, err := ch.BeginStream(context.Background(), "pico:sess-1")
	if err != nil {
		t.Fatalf("BeginStream() error = %v", err)
	}
	reasoningStreamer, ok := streamer.(bus.ReasoningStreamer)
	if !ok {
		t.Fatal("pico stream should support reasoning updates")
	}
	if setter, ok := streamer.(interface{ SetModelName(modelName string) }); ok {
		setter.SetModelName("gpt-5.4-mini")
	}
	if err := reasoningStreamer.UpdateReasoning(context.Background(), "thinking"); err != nil {
		t.Fatalf("UpdateReasoning(first) error = %v", err)
	}
	first := mustReceivePicoMessage(t, received)
	if first.Type != TypeMessageCreate {
		t.Fatalf("first type = %q, want %q", first.Type, TypeMessageCreate)
	}
	msgID, _ := first.Payload["message_id"].(string)
	if msgID == "" {
		t.Fatalf("first message_id = %#v, want non-empty", first.Payload["message_id"])
	}
	if got := first.Payload[PayloadKeyKind]; got != MessageKindThought {
		t.Fatalf("first kind = %#v, want %q", got, MessageKindThought)
	}
	if got := first.Payload[PayloadKeyContent]; got != "thinking" {
		t.Fatalf("first content = %#v, want thinking", got)
	}
	if got := first.Payload[PayloadKeyModelName]; got != "gpt-5.4-mini" {
		t.Fatalf("first model_name = %#v, want %q", got, "gpt-5.4-mini")
	}

	if err := reasoningStreamer.UpdateReasoning(context.Background(), "thinking more"); err != nil {
		t.Fatalf("UpdateReasoning(second) error = %v", err)
	}
	second := mustReceivePicoMessage(t, received)
	if second.Type != TypeMessageUpdate {
		t.Fatalf("second type = %q, want %q", second.Type, TypeMessageUpdate)
	}
	if got := second.Payload["message_id"]; got != msgID {
		t.Fatalf("second message_id = %#v, want %q", got, msgID)
	}
	if got := second.Payload[PayloadKeyKind]; got != MessageKindThought {
		t.Fatalf("second kind = %#v, want %q", got, MessageKindThought)
	}
	if got := second.Payload[PayloadKeyContent]; got != "thinking more" {
		t.Fatalf("second content = %#v, want thinking more", got)
	}
	if got := second.Payload[PayloadKeyModelName]; got != "gpt-5.4-mini" {
		t.Fatalf("second model_name = %#v, want %q", got, "gpt-5.4-mini")
	}
}

func TestBeginStream_ThrottlesIntermediateUpdatesAndFinalFlushes(t *testing.T) {
	ch := newTestPicoChannel(t)
	ch.config.Streaming = config.StreamingConfig{
		Enabled:         true,
		ThrottleSeconds: 60,
		MinGrowthChars:  100,
	}
	if err := ch.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer ch.Stop(context.Background())

	clientConn, received, cleanup := newTestPicoWebSocket(t)
	defer cleanup()
	ch.addConnForTest(&picoConn{id: "conn-1", conn: clientConn, sessionID: "sess-1"})

	streamer, err := ch.BeginStream(context.Background(), "pico:sess-1")
	if err != nil {
		t.Fatalf("BeginStream() error = %v", err)
	}
	if err := streamer.Update(context.Background(), "first"); err != nil {
		t.Fatalf("Update(first) error = %v", err)
	}
	if err := streamer.Update(context.Background(), "first plus short growth"); err != nil {
		t.Fatalf("Update(throttled) error = %v", err)
	}
	if err := streamer.Update(context.Background(), "first"+strings.Repeat("x", 120)); err != nil {
		t.Fatalf("Update(enough growth too soon) error = %v", err)
	}

	first := mustReceivePicoMessage(t, received)
	if first.Type != TypeMessageCreate {
		t.Fatalf("first type = %q, want %q", first.Type, TypeMessageCreate)
	}
	msgID, _ := first.Payload["message_id"].(string)
	assertNoPicoMessage(t, received)

	rawStreamer := streamer.(*picoStreamer)
	rawStreamer.mu.Lock()
	rawStreamer.lastAt = time.Now().Add(-61 * time.Second)
	rawStreamer.mu.Unlock()
	if err := streamer.Update(context.Background(), "first plus small growth"); err != nil {
		t.Fatalf("Update(enough time too little growth) error = %v", err)
	}
	assertNoPicoMessage(t, received)

	if err := streamer.Finalize(context.Background(), "first plus final text"); err != nil {
		t.Fatalf("Finalize() error = %v", err)
	}
	final := mustReceivePicoMessage(t, received)
	if final.Type != TypeMessageUpdate {
		t.Fatalf("final type = %q, want %q", final.Type, TypeMessageUpdate)
	}
	if got := final.Payload["message_id"]; got != msgID {
		t.Fatalf("final message_id = %#v, want %q", got, msgID)
	}
	if got := final.Payload[PayloadKeyContent]; got != "first plus final text" {
		t.Fatalf("final content = %#v, want final text", got)
	}
	assertNoPicoMessage(t, received)
}

func TestBeginStream_FinalizeIncludesContextUsage(t *testing.T) {
	ch := newTestPicoChannel(t)
	ch.config.Streaming = config.StreamingConfig{
		Enabled:         true,
		ThrottleSeconds: 0,
		MinGrowthChars:  0,
	}
	if err := ch.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer ch.Stop(context.Background())

	clientConn, received, cleanup := newTestPicoWebSocket(t)
	defer cleanup()
	ch.addConnForTest(&picoConn{id: "conn-1", conn: clientConn, sessionID: "sess-1"})

	streamer, err := ch.BeginStream(context.Background(), "pico:sess-1")
	if err != nil {
		t.Fatalf("BeginStream() error = %v", err)
	}
	if setter, ok := streamer.(interface{ SetModelName(modelName string) }); ok {
		setter.SetModelName("gpt-5.4")
	}
	if err := streamer.Update(context.Background(), "partial"); err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	first := mustReceivePicoMessage(t, received)
	msgID, _ := first.Payload["message_id"].(string)

	contextStreamer, ok := streamer.(interface {
		FinalizeWithContext(ctx context.Context, content string, usage *bus.ContextUsage) error
	})
	if !ok {
		t.Fatal("streamer should support FinalizeWithContext")
	}
	if err := contextStreamer.FinalizeWithContext(context.Background(), "final", &bus.ContextUsage{
		UsedTokens:        10,
		TotalTokens:       100,
		HistoryTokens:     5,
		CompressAtTokens:  80,
		SummarizeAtTokens: 60,
		UsedPercent:       10,
	}); err != nil {
		t.Fatalf("FinalizeWithContext() error = %v", err)
	}

	final := mustReceivePicoMessage(t, received)
	if final.Type != TypeMessageUpdate {
		t.Fatalf("final type = %q, want %q", final.Type, TypeMessageUpdate)
	}
	if got := final.Payload["message_id"]; got != msgID {
		t.Fatalf("final message_id = %#v, want %q", got, msgID)
	}
	if got := final.Payload[PayloadKeyModelName]; got != "gpt-5.4" {
		t.Fatalf("final model_name = %#v, want %q", got, "gpt-5.4")
	}
	rawUsage, ok := final.Payload["context_usage"].(map[string]any)
	if !ok {
		t.Fatalf("final context_usage = %#v, want map", final.Payload["context_usage"])
	}
	if got := rawUsage["used_tokens"]; got != float64(10) {
		t.Fatalf("used_tokens = %#v, want 10", got)
	}
	if got := rawUsage["history_tokens"]; got != float64(5) {
		t.Fatalf("history_tokens = %#v, want 5", got)
	}
	if got := rawUsage["summarize_at_tokens"]; got != float64(60) {
		t.Fatalf("summarize_at_tokens = %#v, want 60", got)
	}
}

func TestCreateAndAddConnection_RespectsMaxConnectionsConcurrently(t *testing.T) {
	ch := newTestPicoChannel(t)

	const (
		maxConns   = 5
		goroutines = 64
		sessionID  = "session-a"
	)

	var wg sync.WaitGroup
	var mu sync.Mutex
	successCount := 0
	errCount := 0

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()

			pc, err := ch.createAndAddConnection(nil, sessionID, maxConns)
			mu.Lock()
			defer mu.Unlock()

			if err == nil {
				successCount++
				if pc == nil {
					t.Errorf("pc is nil on success")
				}
				return
			}
			if !errors.Is(err, channels.ErrTemporary) {
				t.Errorf("unexpected error: %v", err)
				return
			}
			errCount++
		}()
	}
	wg.Wait()

	if successCount > maxConns {
		t.Fatalf("successCount=%d > maxConns=%d", successCount, maxConns)
	}
	if successCount+errCount != goroutines {
		t.Fatalf("success=%d err=%d total=%d want=%d", successCount, errCount, successCount+errCount, goroutines)
	}
	if got := ch.currentConnCount(); got != maxConns {
		t.Fatalf("currentConnCount=%d want=%d", got, maxConns)
	}
}

func TestRemoveConnection_CleansBothIndexes(t *testing.T) {
	ch := newTestPicoChannel(t)

	pc, err := ch.createAndAddConnection(nil, "session-cleanup", 10)
	if err != nil {
		t.Fatalf("createAndAddConnection: %v", err)
	}

	removed := ch.removeConnection(pc.id)
	if removed == nil {
		t.Fatal("removeConnection returned nil")
	}

	ch.connsMu.RLock()
	defer ch.connsMu.RUnlock()

	if _, ok := ch.connections[pc.id]; ok {
		t.Fatalf("connID %s still exists in connections", pc.id)
	}
	if _, ok := ch.sessionConnections[pc.sessionID]; ok {
		t.Fatalf("session %s still exists in sessionConnections", pc.sessionID)
	}
	if got := len(ch.connections); got != 0 {
		t.Fatalf("len(connections)=%d want=0", got)
	}
}

func TestBroadcastToSession_TargetsOnlyRequestedSession(t *testing.T) {
	ch := newTestPicoChannel(t)

	target := &picoConn{id: "target", sessionID: "s-target"}
	target.closed.Store(true)
	ch.addConnForTest(target)

	other := &picoConn{id: "other", sessionID: "s-other"}
	ch.addConnForTest(other)

	err := ch.broadcastToSession("pico:s-target", newMessage(TypeMessageCreate, map[string]any{"content": "hello"}))
	if err == nil {
		t.Fatal("expected send failure due to closed target connection")
	}
	if !errors.Is(err, channels.ErrSendFailed) {
		t.Fatalf("expected ErrSendFailed, got %v", err)
	}
}

func TestBroadcastToConnections_StalledPeerDoesNotStarveHealthyPeer(t *testing.T) {
	ch := newTestPicoChannel(t)
	healthyConn, received, cleanup := newTestPicoWebSocket(t)
	defer cleanup()
	healthy := &picoConn{id: "healthy", conn: healthyConn, sessionID: "sess-1"}
	stalled := &picoConn{id: "stalled", sessionID: "sess-1"}
	stalled.writeOnce.Do(func() {
		stalled.writeLock = make(chan struct{}, 1)
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	started := time.Now()
	err := ch.broadcastToConnectionsContext(
		ctx,
		"sess-1",
		newMessage(TypeMessageCreate, map[string]any{"content": "hello"}),
		[]*picoConn{stalled, healthy},
	)
	if err != nil {
		t.Fatalf("broadcastToConnectionsContext() error = %v", err)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("broadcast returned after %v, want first-success completion", elapsed)
	}
	if err := ctx.Err(); err != nil {
		t.Fatalf("caller context expired before successful broadcast returned: %v", err)
	}
	select {
	case msg := <-received:
		if msg.SessionID != "sess-1" || msg.Payload["content"] != "hello" {
			t.Fatalf("received message = %+v", msg)
		}
	case <-time.After(time.Second):
		t.Fatal("healthy peer did not receive broadcast")
	}
}

func TestBroadcastToConnections_PreservesContextFailure(t *testing.T) {
	tests := []struct {
		name    string
		context func(*testing.T) context.Context
		wantErr error
	}{
		{
			name: "canceled",
			context: func(t *testing.T) context.Context {
				t.Helper()
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			},
			wantErr: context.Canceled,
		},
		{
			name: "deadline",
			context: func(t *testing.T) context.Context {
				t.Helper()
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
				t.Cleanup(cancel)
				return ctx
			},
			wantErr: context.DeadlineExceeded,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stalled := &picoConn{id: "stalled", sessionID: "sess-1"}
			stalled.writeOnce.Do(func() {
				stalled.writeLock = make(chan struct{}, 1)
			})

			err := newTestPicoChannel(t).broadcastToConnectionsContext(
				tt.context(t),
				"sess-1",
				newMessage(TypeMessageCreate, map[string]any{"content": "hello"}),
				[]*picoConn{stalled},
			)
			if !errors.Is(err, channels.ErrSendFailed) {
				t.Fatalf("error = %v, want ErrSendFailed", err)
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestPicoConnEnqueueWrite_PreservesSubmissionOrder(t *testing.T) {
	conn, _, cleanup := newTestPicoWebSocket(t)
	defer cleanup()
	pc := &picoConn{conn: conn}
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var mu sync.Mutex
	var order []string

	first := pc.enqueueWrite(context.Background(), func() error {
		close(firstStarted)
		<-releaseFirst
		mu.Lock()
		order = append(order, "create")
		mu.Unlock()
		return nil
	})
	<-firstStarted
	second := pc.enqueueWrite(context.Background(), func() error {
		mu.Lock()
		order = append(order, "update")
		mu.Unlock()
		return nil
	})

	select {
	case err := <-second:
		t.Fatalf("second write completed before first: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseFirst)
	if err := <-first; err != nil {
		t.Fatalf("first write error = %v", err)
	}
	if err := <-second; err != nil {
		t.Fatalf("second write error = %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if got := strings.Join(order, ","); got != "create,update" {
		t.Fatalf("write order = %q, want create,update", got)
	}
}

func TestPicoConnEnqueueWrite_QueueFullClosesConnection(t *testing.T) {
	conn, _, cleanup := newTestPicoWebSocket(t)
	defer cleanup()
	pc := &picoConn{conn: conn}
	blockerStarted := make(chan struct{})
	releaseBlocker := make(chan struct{})
	blocker := pc.enqueueWrite(context.Background(), func() error {
		close(blockerStarted)
		<-releaseBlocker
		return nil
	})
	<-blockerStarted

	pending := make([]<-chan error, 0, picoWriteQueueSize)
	for range picoWriteQueueSize {
		pending = append(pending, pc.enqueueWrite(context.Background(), func() error { return nil }))
	}
	overflow := pc.enqueueWrite(context.Background(), func() error { return nil })
	if err := <-overflow; !errors.Is(err, errPicoWriteQueueFull) {
		t.Fatalf("overflow error = %v, want queue full", err)
	}
	if !pc.closed.Load() {
		t.Fatal("queue overflow left connection open")
	}

	close(releaseBlocker)
	if err := <-blocker; err != nil {
		t.Fatalf("blocker error = %v", err)
	}
	for i, result := range pending {
		if err := <-result; err == nil {
			t.Fatalf("pending write %d succeeded after queue overflow", i)
		}
	}
}

func TestBroadcastToConnections_FirstSuccessPreservesSlowPeerOrder(t *testing.T) {
	fastConn, _, fastCleanup := newTestPicoWebSocket(t)
	defer fastCleanup()
	slowConn, slowReceived, slowCleanup := newTestPicoWebSocket(t)
	defer slowCleanup()
	fast := &picoConn{id: "fast", conn: fastConn, sessionID: "sess-1"}
	slow := &picoConn{id: "slow", conn: slowConn, sessionID: "sess-1"}

	blockerStarted := make(chan struct{})
	releaseBlocker := make(chan struct{})
	blocker := slow.enqueueWrite(context.Background(), func() error {
		close(blockerStarted)
		<-releaseBlocker
		return nil
	})
	<-blockerStarted

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	connections := []*picoConn{slow, fast}
	if err := newTestPicoChannel(t).broadcastToConnectionsContext(
		ctx,
		"sess-1",
		newMessage(TypeMessageCreate, map[string]any{"content": "first"}),
		connections,
	); err != nil {
		t.Fatalf("create broadcast error = %v", err)
	}
	if err := newTestPicoChannel(t).broadcastToConnectionsContext(
		ctx,
		"sess-1",
		newMessage(TypeMessageUpdate, map[string]any{"content": "second"}),
		connections,
	); err != nil {
		t.Fatalf("update broadcast error = %v", err)
	}

	close(releaseBlocker)
	if err := <-blocker; err != nil {
		t.Fatalf("slow-peer blocker error = %v", err)
	}
	first := mustReceivePicoMessage(t, slowReceived)
	second := mustReceivePicoMessage(t, slowReceived)
	if first.Type != TypeMessageCreate || second.Type != TypeMessageUpdate {
		t.Fatalf("slow peer frame order = [%q, %q], want create then update", first.Type, second.Type)
	}
}

func TestBroadcastToConnections_CanceledQueuedCreateClosesSlowPeer(t *testing.T) {
	fastConn, _, fastCleanup := newTestPicoWebSocket(t)
	defer fastCleanup()
	slowConn, slowReceived, slowCleanup := newTestPicoWebSocket(t)
	defer slowCleanup()
	fast := &picoConn{id: "fast", conn: fastConn, sessionID: "sess-1"}
	slow := &picoConn{id: "slow", conn: slowConn, sessionID: "sess-1"}

	blockerStarted := make(chan struct{})
	releaseBlocker := make(chan struct{})
	blocker := slow.enqueueWrite(context.Background(), func() error {
		close(blockerStarted)
		<-releaseBlocker
		return nil
	})
	<-blockerStarted

	ctx, cancel := context.WithCancel(context.Background())
	if err := newTestPicoChannel(t).broadcastToConnectionsContext(
		ctx,
		"sess-1",
		newMessage(TypeMessageCreate, map[string]any{"content": "first"}),
		[]*picoConn{slow, fast},
	); err != nil {
		t.Fatalf("create broadcast error = %v", err)
	}
	cancel()
	close(releaseBlocker)
	if err := <-blocker; err != nil {
		t.Fatalf("slow-peer blocker error = %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for !slow.closed.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !slow.closed.Load() {
		t.Fatal("canceled queued create left slow peer open")
	}

	err := newTestPicoChannel(t).broadcastToConnectionsContext(
		context.Background(),
		"sess-1",
		newMessage(TypeMessageUpdate, map[string]any{"content": "second"}),
		[]*picoConn{slow},
	)
	if !errors.Is(err, channels.ErrSendFailed) {
		t.Fatalf("update broadcast error = %v, want ErrSendFailed", err)
	}
	assertNoPicoMessage(t, slowReceived)
}

func TestPicoConnWriteJSON_DeadlineInterruptsBlockedSocket(t *testing.T) {
	pc, cleanup := newBlockedPicoConn(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	started := time.Now()
	err := pc.writeJSON(ctx, map[string]any{
		"content": strings.Repeat("x", 8<<20),
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("writeJSON() error = %v, want context deadline exceeded", err)
	}
	if !pc.closed.Load() {
		t.Fatal("writeJSON() left a timed-out WebSocket connection reusable")
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("writeJSON() returned after %v, want bounded socket write", elapsed)
	}
}

func TestPicoConnWriteJSON_CancellationInterruptsActiveWrite(t *testing.T) {
	pc, cleanup := newBlockedPicoConn(t)
	defer cleanup()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	writeStarted := make(chan struct{})
	payload := map[string]any{"content": strings.Repeat("x", 8<<20)}
	go func() {
		result <- pc.write(ctx, func() error {
			close(writeStarted)
			return pc.conn.WriteJSON(payload)
		})
	}()

	<-writeStarted
	select {
	case err := <-result:
		t.Fatalf("write() completed before cancellation with %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	started := time.Now()
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("writeJSON() error = %v, want context canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("writeJSON() did not stop after context cancellation")
	}
	if !pc.closed.Load() {
		t.Fatal("writeJSON() left a canceled active WebSocket write reusable")
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("writeJSON() returned after %v, want prompt cancellation", elapsed)
	}
}

func newBlockedPicoConn(t *testing.T) (*picoConn, func()) {
	t.Helper()
	accepted := make(chan struct{})
	release := make(chan struct{})
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("Upgrade() error = %v", err)
			return
		}
		defer conn.Close()
		close(accepted)
		<-release
	}))

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		close(release)
		server.Close()
		t.Fatalf("Dial() error = %v", err)
	}
	resp.Body.Close()
	cleanup := func() {
		conn.Close()
		close(release)
		server.Close()
	}
	<-accepted

	if tcpConn, ok := conn.UnderlyingConn().(*net.TCPConn); ok {
		if writeBufferErr := tcpConn.SetWriteBuffer(1024); writeBufferErr != nil {
			cleanup()
			t.Fatalf("SetWriteBuffer() error = %v", writeBufferErr)
		}
	}
	return &picoConn{conn: conn}, cleanup
}

func TestPicoConnWriteJSON_DeadlineWaitingForWriterKeepsConnectionOpen(t *testing.T) {
	pc := &picoConn{}
	pc.writeOnce.Do(func() {
		pc.writeLock = make(chan struct{}, 1)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	err := pc.writeJSON(ctx, map[string]any{"content": "not written"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("writeJSON() error = %v, want context deadline exceeded", err)
	}
	if pc.closed.Load() {
		t.Fatal("writeJSON() closed a connection before acquiring the writer")
	}
}

func TestPicoConnWrite_RejectsConnectionClosedWhileWaitingForWriter(t *testing.T) {
	pc := &picoConn{}
	pc.writeOnce.Do(func() {
		pc.writeLock = make(chan struct{}, 1)
	})
	called := atomic.Bool{}
	result := make(chan error, 1)
	go func() {
		result <- pc.write(context.Background(), func() error {
			called.Store(true)
			return nil
		})
	}()

	pc.closed.Store(true)
	pc.writeLock <- struct{}{}
	err := <-result
	if err == nil || !strings.Contains(err.Error(), "connection closed") {
		t.Fatalf("write() error = %v, want connection closed", err)
	}
	if called.Load() {
		t.Fatal("write() invoked callback after connection closed while waiting")
	}
}

func TestPicoConnWrite_SuccessWinsCancellationResult(t *testing.T) {
	conn, _, cleanup := newTestPicoWebSocket(t)
	defer cleanup()
	pc := &picoConn{conn: conn}
	ctx, cancel := context.WithCancel(context.Background())

	err := pc.write(ctx, func() error {
		cancel()
		deadline := time.Now().Add(time.Second)
		for !pc.closed.Load() && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if !pc.closed.Load() {
			t.Fatal("cancellation did not win the active-write race")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("write() error = %v after successful callback, want nil", err)
	}
}

func TestSendMedia_ResolvesMediaBeforeDelivery(t *testing.T) {
	ch := newTestPicoChannel(t)
	store := media.NewFileMediaStore()
	ch.SetMediaStore(store)

	if err := ch.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer ch.Stop(context.Background())

	localPath := filepath.Join(t.TempDir(), "report.txt")
	if writeErr := os.WriteFile(localPath, []byte("attachment body"), 0o600); writeErr != nil {
		t.Fatalf("WriteFile() error = %v", writeErr)
	}

	ref, err := store.Store(localPath, media.MediaMeta{
		Filename:    "report.txt",
		ContentType: "text/plain",
	}, "test-scope")
	if err != nil {
		t.Fatalf("Store() error = %v", err)
	}

	closedConn := &picoConn{id: "closed", sessionID: "sess-1"}
	closedConn.closed.Store(true)
	ch.addConnForTest(closedConn)

	_, err = ch.SendMedia(context.Background(), bus.OutboundMediaMessage{
		ChatID: "pico:sess-1",
		Parts: []bus.MediaPart{{
			Ref:         ref,
			Type:        "file",
			Filename:    "report.txt",
			ContentType: "text/plain",
		}},
	})
	if !errors.Is(err, channels.ErrSendFailed) {
		t.Fatalf("SendMedia() error = %v, want ErrSendFailed", err)
	}
}

func TestContextBearingBroadcastsPropagateCallerContext(t *testing.T) {
	type contextKey struct{}
	marker := &struct{}{}
	ctx := context.WithValue(context.Background(), contextKey{}, marker)
	ch := newTestPicoChannel(t)
	ch.SetRunning(true)
	ch.bc.Placeholder.Enabled = true

	var calls int
	ch.broadcastFn = func(gotCtx context.Context, _ string, _ PicoMessage) error {
		calls++
		if gotCtx == nil || gotCtx.Value(contextKey{}) != marker {
			t.Fatal("broadcast did not receive the caller context")
		}
		return nil
	}

	if _, err := ch.StartTyping(ctx, "pico:sess-1"); err != nil {
		t.Fatalf("StartTyping() error = %v", err)
	}
	if _, err := ch.SendPlaceholder(ctx, "pico:sess-1"); err != nil {
		t.Fatalf("SendPlaceholder() error = %v", err)
	}

	streamer := &picoStreamer{channel: ch, chatID: "pico:sess-1"}
	streamer.mu.Lock()
	err := streamer.sendLocked(ctx, "answer", nil)
	streamer.mu.Unlock()
	if err != nil {
		t.Fatalf("sendLocked() error = %v", err)
	}
	streamer.mu.Lock()
	err = streamer.sendReasoningLocked(ctx, "reasoning")
	streamer.mu.Unlock()
	if err != nil {
		t.Fatalf("sendReasoningLocked() error = %v", err)
	}

	store := media.NewFileMediaStore()
	ch.SetMediaStore(store)
	localPath := filepath.Join(t.TempDir(), "report.txt")
	if writeErr := os.WriteFile(localPath, []byte("attachment body"), 0o600); writeErr != nil {
		t.Fatalf("WriteFile() error = %v", writeErr)
	}
	ref, err := store.Store(localPath, media.MediaMeta{
		Filename:    "report.txt",
		ContentType: "text/plain",
	}, "test-scope")
	if err != nil {
		t.Fatalf("Store() error = %v", err)
	}
	if _, err := ch.SendMedia(ctx, bus.OutboundMediaMessage{
		ChatID: "pico:sess-1",
		Parts: []bus.MediaPart{{
			Ref:         ref,
			Type:        "file",
			Filename:    "report.txt",
			ContentType: "text/plain",
		}},
	}); err != nil {
		t.Fatalf("SendMedia() error = %v", err)
	}

	if calls != 5 {
		t.Fatalf("broadcast calls = %d, want 5", calls)
	}
}

func TestPicoStreamerFailedCreateRetriesAsCreate(t *testing.T) {
	tests := []struct {
		name       string
		storedID   func(*picoStreamer) string
		sendLocked func(*picoStreamer, context.Context, string) error
	}{
		{
			name:     "answer",
			storedID: func(s *picoStreamer) string { return s.messageID },
			sendLocked: func(s *picoStreamer, ctx context.Context, content string) error {
				return s.sendLocked(ctx, content, nil)
			},
		},
		{
			name:     "reasoning",
			storedID: func(s *picoStreamer) string { return s.reasoningID },
			sendLocked: func(s *picoStreamer, ctx context.Context, content string) error {
				return s.sendReasoningLocked(ctx, content)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ch := newTestPicoChannel(t)
			var broadcasts []PicoMessage
			ch.broadcastFn = func(_ context.Context, _ string, msg PicoMessage) error {
				broadcasts = append(broadcasts, msg)
				if len(broadcasts) == 1 {
					return context.Canceled
				}
				return nil
			}
			streamer := &picoStreamer{channel: ch, chatID: "pico:sess-1"}

			if err := tt.sendLocked(streamer, context.Background(), "first"); !errors.Is(err, context.Canceled) {
				t.Fatalf("first send error = %v, want context canceled", err)
			}
			if got := tt.storedID(streamer); got != "" {
				t.Fatalf("stored ID after failed create = %q, want empty", got)
			}
			if err := tt.sendLocked(streamer, context.Background(), "retry"); err != nil {
				t.Fatalf("retry send error = %v", err)
			}
			if len(broadcasts) != 2 {
				t.Fatalf("broadcast count = %d, want 2", len(broadcasts))
			}
			if broadcasts[0].Type != TypeMessageCreate || broadcasts[1].Type != TypeMessageCreate {
				t.Fatalf("broadcast types = [%q, %q], want two creates", broadcasts[0].Type, broadcasts[1].Type)
			}
			if got := tt.storedID(streamer); got == "" {
				t.Fatal("stored ID after successful retry is empty")
			}
		})
	}
}

func TestSendMedia_IncludesCaptionAndAttachmentsInSinglePayload(t *testing.T) {
	ch := newTestPicoChannel(t)
	store := media.NewFileMediaStore()
	ch.SetMediaStore(store)

	if err := ch.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer ch.Stop(context.Background())

	clientConn, received, cleanup := newTestPicoWebSocket(t)
	defer cleanup()
	ch.addConnForTest(&picoConn{id: "conn-1", conn: clientConn, sessionID: "sess-1"})

	localPath := filepath.Join(t.TempDir(), "photo.png")
	if err := os.WriteFile(localPath, []byte("png-body"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	ref, err := store.Store(localPath, media.MediaMeta{
		Filename:    "photo.png",
		ContentType: "image/png",
	}, "test-scope")
	if err != nil {
		t.Fatalf("Store() error = %v", err)
	}

	_, err = ch.SendMedia(context.Background(), bus.OutboundMediaMessage{
		ChatID: "pico:sess-1",
		Parts: []bus.MediaPart{{
			Ref:         ref,
			Type:        "image",
			Filename:    "photo.png",
			ContentType: "image/png",
			Caption:     "recipe translation",
		}},
	})
	if err != nil {
		t.Fatalf("SendMedia() error = %v", err)
	}

	select {
	case msg := <-received:
		if msg.Type != TypeMessageCreate {
			t.Fatalf("message type = %q, want %q", msg.Type, TypeMessageCreate)
		}
		payload := msg.Payload
		if got := payload[PayloadKeyContent]; got != "recipe translation" {
			t.Fatalf("content = %#v, want %q", got, "recipe translation")
		}
		rawAttachments, ok := payload["attachments"].([]any)
		if !ok || len(rawAttachments) != 1 {
			t.Fatalf("attachments = %#v, want 1 attachment", payload["attachments"])
		}
		attachment, ok := rawAttachments[0].(map[string]any)
		if !ok {
			t.Fatalf("attachment = %#v, want map", rawAttachments[0])
		}
		if got := attachment["type"]; got != "image" {
			t.Fatalf("attachment type = %#v, want image", got)
		}
		if got := attachment["filename"]; got != "photo.png" {
			t.Fatalf("attachment filename = %#v, want photo.png", got)
		}
	case <-time.After(time.Second):
		t.Fatal("expected media payload to be delivered")
	}
}

func TestSendMedia_UsesCaptionFromFirstDeliveredAttachment(t *testing.T) {
	ch := newTestPicoChannel(t)
	store := media.NewFileMediaStore()
	ch.SetMediaStore(store)

	if err := ch.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer ch.Stop(context.Background())

	clientConn, received, cleanup := newTestPicoWebSocket(t)
	defer cleanup()
	ch.addConnForTest(&picoConn{id: "conn-1", conn: clientConn, sessionID: "sess-1"})

	localPath := filepath.Join(t.TempDir(), "photo.png")
	if err := os.WriteFile(localPath, []byte("png-body"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	goodRef, err := store.Store(localPath, media.MediaMeta{
		Filename:    "photo.png",
		ContentType: "image/png",
	}, "test-scope")
	if err != nil {
		t.Fatalf("Store() error = %v", err)
	}

	_, err = ch.SendMedia(context.Background(), bus.OutboundMediaMessage{
		ChatID: "pico:sess-1",
		Parts: []bus.MediaPart{
			{
				Ref:         "media://missing-ref",
				Type:        "image",
				Filename:    "missing.png",
				ContentType: "image/png",
				Caption:     "should be skipped",
			},
			{
				Ref:         goodRef,
				Type:        "image",
				Filename:    "photo.png",
				ContentType: "image/png",
				Caption:     "delivered caption",
			},
		},
	})
	if err != nil {
		t.Fatalf("SendMedia() error = %v", err)
	}

	select {
	case msg := <-received:
		if msg.Type != TypeMessageCreate {
			t.Fatalf("message type = %q, want %q", msg.Type, TypeMessageCreate)
		}
		if got := msg.Payload[PayloadKeyContent]; got != "delivered caption" {
			t.Fatalf("content = %#v, want %q", got, "delivered caption")
		}
	case <-time.After(time.Second):
		t.Fatal("expected media message to be delivered")
	}
}

func TestSendMedia_DoesNotPromoteCaptionFromSkippedAttachment(t *testing.T) {
	ch := newTestPicoChannel(t)
	store := media.NewFileMediaStore()
	ch.SetMediaStore(store)

	if err := ch.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer ch.Stop(context.Background())

	clientConn, received, cleanup := newTestPicoWebSocket(t)
	defer cleanup()
	ch.addConnForTest(&picoConn{id: "conn-1", conn: clientConn, sessionID: "sess-1"})

	localPath := filepath.Join(t.TempDir(), "photo.png")
	if err := os.WriteFile(localPath, []byte("png-body"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	goodRef, err := store.Store(localPath, media.MediaMeta{
		Filename:    "photo.png",
		ContentType: "image/png",
	}, "test-scope")
	if err != nil {
		t.Fatalf("Store() error = %v", err)
	}

	_, err = ch.SendMedia(context.Background(), bus.OutboundMediaMessage{
		ChatID: "pico:sess-1",
		Parts: []bus.MediaPart{
			{
				Ref:         "media://missing-ref",
				Type:        "image",
				Filename:    "missing.png",
				ContentType: "image/png",
				Caption:     "should not leak",
			},
			{
				Ref:         goodRef,
				Type:        "image",
				Filename:    "photo.png",
				ContentType: "image/png",
				Caption:     "   ",
			},
		},
	})
	if err != nil {
		t.Fatalf("SendMedia() error = %v", err)
	}

	select {
	case msg := <-received:
		if msg.Type != TypeMessageCreate {
			t.Fatalf("message type = %q, want %q", msg.Type, TypeMessageCreate)
		}
		if got := msg.Payload[PayloadKeyContent]; got != "" {
			t.Fatalf("content = %#v, want empty", got)
		}
	case <-time.After(time.Second):
		t.Fatal("expected media message to be delivered")
	}
}

func TestPicoDownloadURLForRef(t *testing.T) {
	got, err := picoDownloadURLForRef("media://attachment-1")
	if err != nil {
		t.Fatalf("picoDownloadURLForRef() error = %v", err)
	}
	if got != "/pico/media/attachment-1" {
		t.Fatalf("picoDownloadURLForRef() = %q, want %q", got, "/pico/media/attachment-1")
	}
}

func TestHandleMediaDownload_ServesStoredFile(t *testing.T) {
	ch := newTestPicoChannel(t)
	store := media.NewFileMediaStore()
	ch.SetMediaStore(store)

	if err := ch.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer ch.Stop(context.Background())

	localPath := filepath.Join(t.TempDir(), "report.txt")
	if err := os.WriteFile(localPath, []byte("downloadable"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	ref, err := store.Store(localPath, media.MediaMeta{
		Filename:    "report.txt",
		ContentType: "text/plain",
	}, "test-scope")
	if err != nil {
		t.Fatalf("Store() error = %v", err)
	}

	refID := strings.TrimPrefix(ref, "media://")
	req := httptest.NewRequest("GET", "/pico/media/"+refID, nil)
	req.Header.Set("Authorization", "Bearer test-token")
	rec := httptest.NewRecorder()

	ch.ServeHTTP(rec, req)

	if rec.Code != 200 {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if body := rec.Body.String(); body != "downloadable" {
		t.Fatalf("body = %q, want %q", body, "downloadable")
	}
	if got := rec.Header().Get("Content-Type"); got != "text/plain" {
		t.Fatalf("Content-Type = %q, want %q", got, "text/plain")
	}
}

func mustReceivePicoMessage(t *testing.T, received <-chan PicoMessage) PicoMessage {
	t.Helper()
	select {
	case msg := <-received:
		return msg
	case <-time.After(time.Second):
		t.Fatal("expected pico message")
	}
	return PicoMessage{}
}

func assertNoPicoMessage(t *testing.T, received <-chan PicoMessage) {
	t.Helper()
	select {
	case msg := <-received:
		t.Fatalf("unexpected pico message: %+v", msg)
	case <-time.After(150 * time.Millisecond):
	}
}

func (c *PicoChannel) addConnForTest(pc *picoConn) {
	c.connsMu.Lock()
	defer c.connsMu.Unlock()
	if c.connections == nil {
		c.connections = make(map[string]*picoConn)
	}
	if c.sessionConnections == nil {
		c.sessionConnections = make(map[string]map[string]*picoConn)
	}
	if _, exists := c.connections[pc.id]; exists {
		panic(fmt.Sprintf("duplicate conn id in test: %s", pc.id))
	}
	c.connections[pc.id] = pc
	bySession, ok := c.sessionConnections[pc.sessionID]
	if !ok {
		bySession = make(map[string]*picoConn)
		c.sessionConnections[pc.sessionID] = bySession
	}
	bySession[pc.id] = pc
}

func newTestPicoWebSocket(t *testing.T) (*websocket.Conn, <-chan PicoMessage, func()) {
	t.Helper()

	received := make(chan PicoMessage, 4)
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("Upgrade() error = %v", err)
			return
		}
		defer conn.Close()
		for {
			var msg PicoMessage
			if err := conn.ReadJSON(&msg); err != nil {
				return
			}
			received <- msg
		}
	}))

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	clientConn, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		server.Close()
		t.Fatalf("Dial() error = %v", err)
	}

	cleanup := func() {
		clientConn.Close()
		server.Close()
	}
	defer resp.Body.Close()
	return clientConn, received, cleanup
}
