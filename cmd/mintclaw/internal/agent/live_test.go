package agent

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	channelmintclaw "github.com/bogdanovich/mintclaw/pkg/channels/mintclaw"
)

func TestRunLiveReturnsCorrelatedFinalFromOneRequest(t *testing.T) {
	var requests atomic.Int32
	server := liveTestServer(
		t,
		"test-token",
		func(connection *websocket.Conn, request channelmintclaw.MintClawMessage) {
			requests.Add(1)
			_ = connection.WriteJSON(channelmintclaw.MintClawMessage{
				Type:      channelmintclaw.TypeMessageCreate,
				SessionID: "another-session",
				Payload: map[string]any{
					channelmintclaw.PayloadKeyContent: "must be ignored",
					channelmintclaw.PayloadKeyFinal:   true,
				},
			})
			_ = connection.WriteJSON(channelmintclaw.MintClawMessage{
				Type:      channelmintclaw.TypeMessageCreate,
				SessionID: request.SessionID,
				Payload: map[string]any{
					channelmintclaw.PayloadKeyContent: "wrong request must be ignored",
					channelmintclaw.PayloadKeyFinal:   true,
					"request_id":                      "another-request",
				},
			})
			_ = connection.WriteJSON(channelmintclaw.MintClawMessage{
				Type:      channelmintclaw.TypeMessageCreate,
				SessionID: request.SessionID,
				Payload: map[string]any{
					channelmintclaw.PayloadKeyContent: "thinking",
					channelmintclaw.PayloadKeyKind:    channelmintclaw.MessageKindThought,
					"request_id":                      request.ID,
				},
			})
			_ = connection.WriteJSON(channelmintclaw.MintClawMessage{
				Type:      channelmintclaw.TypeMessageUpdate,
				SessionID: request.SessionID,
				Payload: map[string]any{
					channelmintclaw.PayloadKeyContent:    "MINTCLAW_LIVE_OK",
					channelmintclaw.PayloadKeyFinal:      true,
					channelmintclaw.PayloadKeyAgentID:    "main",
					channelmintclaw.PayloadKeySessionKey: "sk_v1_live",
					"request_id":                         request.ID,
					channelmintclaw.PayloadKeyTraceScopes: []map[string]string{{
						"workspace": "/srv/mintclaw/workspace",
						"turn_id":   "turn-live-1",
					}},
				},
			})
		},
	)

	result, err := runLive(t.Context(), liveOptions{
		ConfigPath: liveTestConfig(t, server.URL, "test-token"),
		Message:    "reply with MINTCLAW_LIVE_OK",
		SessionID:  "live-test-session",
		Timeout:    time.Second,
	})
	if err != nil {
		t.Fatalf("runLive() error = %v", err)
	}
	if requests.Load() != 1 {
		t.Fatalf("requests = %d, want 1", requests.Load())
	}
	if result.Outcome != "success" || result.Response != "MINTCLAW_LIVE_OK" ||
		result.AgentID != "main" || result.SessionKey != "sk_v1_live" ||
		result.TraceScope.TurnID != "turn-live-1" || result.RequestID == "" {
		t.Fatalf("result = %#v", result)
	}
}

func TestRunLiveReturnsApprovalRequired(t *testing.T) {
	server := liveTestServer(
		t,
		"test-token",
		func(connection *websocket.Conn, request channelmintclaw.MintClawMessage) {
			_ = connection.WriteJSON(channelmintclaw.MintClawMessage{
				Type:      channelmintclaw.TypeMessageCreate,
				SessionID: request.SessionID,
				Payload: map[string]any{
					channelmintclaw.PayloadKeyContent:            "Approve protected action",
					channelmintclaw.PayloadKeyInteraction:        "approval",
					channelmintclaw.PayloadKeyControls:           "prompt",
					channelmintclaw.PayloadKeyInteractionID:      "interaction-approval-1",
					channelmintclaw.PayloadKeyInteractionShortID: "abc123",
					"request_id": request.ID,
				},
			})
		},
	)
	result, err := runLive(t.Context(), liveOptions{
		ConfigPath: liveTestConfig(t, server.URL, "test-token"),
		Message:    "protected operation",
		Timeout:    time.Second,
	})
	if err != nil || result.Outcome != "approval_required" ||
		result.InteractionID != "interaction-approval-1" || result.InteractionShortID != "abc123" {
		t.Fatalf("runLive() = (%#v, %v)", result, err)
	}
}

func TestRunLiveTimesOutWithoutReplay(t *testing.T) {
	var requests atomic.Int32
	server := liveTestServer(t, "test-token", func(_ *websocket.Conn, _ channelmintclaw.MintClawMessage) {
		requests.Add(1)
		time.Sleep(100 * time.Millisecond)
	})
	result, err := runLive(t.Context(), liveOptions{
		ConfigPath: liveTestConfig(t, server.URL, "test-token"),
		Message:    "wait",
		Timeout:    20 * time.Millisecond,
	})
	if err == nil || result.Outcome != "timeout" {
		t.Fatalf("runLive() = (%#v, %v)", result, err)
	}
	if requests.Load() != 1 {
		t.Fatalf("requests = %d, want no replay", requests.Load())
	}
}

func TestRunLiveClassifiesDisconnectWithoutReplay(t *testing.T) {
	var requests atomic.Int32
	server := liveTestServer(t, "test-token", func(_ *websocket.Conn, _ channelmintclaw.MintClawMessage) {
		requests.Add(1)
	})
	result, err := runLive(t.Context(), liveOptions{
		ConfigPath: liveTestConfig(t, server.URL, "test-token"),
		Message:    "disconnect",
		Timeout:    time.Second,
	})
	if err == nil || result.Outcome != "disconnected" {
		t.Fatalf("runLive() = (%#v, %v)", result, err)
	}
	if requests.Load() != 1 {
		t.Fatalf("requests = %d, want no replay", requests.Load())
	}
}

func TestRunLiveClassifiesOversizedFrameAsOutputLimit(t *testing.T) {
	server := liveTestServer(
		t,
		"test-token",
		func(connection *websocket.Conn, request channelmintclaw.MintClawMessage) {
			_ = connection.WriteJSON(channelmintclaw.MintClawMessage{
				Type:      channelmintclaw.TypeMessageCreate,
				SessionID: request.SessionID,
				Payload: map[string]any{
					channelmintclaw.PayloadKeyContent: strings.Repeat("x", liveMaxOutputBytes+128*1024),
					channelmintclaw.PayloadKeyFinal:   true,
					"request_id":                      request.ID,
				},
			})
		},
	)
	result, err := runLive(t.Context(), liveOptions{
		ConfigPath: liveTestConfig(t, server.URL, "test-token"),
		Message:    "oversized response",
		Timeout:    5 * time.Second,
	})
	if err == nil || result.Outcome != "output_limit" {
		t.Fatalf("runLive() = (%#v, %v), want output_limit", result, err)
	}
}

func TestRunLiveClassifiesAuthenticationWithoutLeakingToken(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	}))
	defer server.Close()
	const secret = "do-not-leak-this-token"
	result, err := runLive(t.Context(), liveOptions{
		ConfigPath: liveTestConfig(t, server.URL, secret),
		Message:    "hello",
		Timeout:    time.Second,
	})
	if err == nil || result.Outcome != "authentication_failed" {
		t.Fatalf("runLive() = (%#v, %v)", result, err)
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("error leaked token: %v", err)
	}
}

func TestRunLiveClassifiesCallerCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	result, err := runLive(ctx, liveOptions{
		ConfigPath: liveTestConfig(t, "http://127.0.0.1:1", "test-token"),
		Message:    "hello",
		Timeout:    time.Second,
	})
	if err == nil || result.Outcome != "canceled" {
		t.Fatalf("runLive() = (%#v, %v)", result, err)
	}
}

func liveTestServer(
	t *testing.T,
	token string,
	handle func(*websocket.Conn, channelmintclaw.MintClawMessage),
) *httptest.Server {
	t.Helper()
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/mintclaw/ws" || request.Header.Get("Authorization") != "Bearer "+token {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		connection, err := upgrader.Upgrade(w, request, nil)
		if err != nil {
			return
		}
		defer connection.Close()
		var message channelmintclaw.MintClawMessage
		if connection.ReadJSON(&message) != nil {
			return
		}
		handle(connection, message)
	}))
	t.Cleanup(server.Close)
	return server
}

func liveTestConfig(t *testing.T, serverURL, token string) string {
	t.Helper()
	parsed, err := url.Parse(serverURL)
	if err != nil {
		t.Fatal(err)
	}
	host := parsed.Hostname()
	port := parsed.Port()
	path := filepath.Join(t.TempDir(), "config.json")
	content := `{"version":3,"gateway":{"host":` + strconvQuote(host) + `,"port":` + port +
		`},"channel_list":{"mintclaw":{"type":"mintclaw","enabled":true,"settings":{"token":` +
		strconvQuote(token) + `}}}}`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func strconvQuote(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `\"`) + `"`
}
