package nodes

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/bogdanovich/mintclaw/pkg/config"
	nodepkg "github.com/bogdanovich/mintclaw/pkg/nodes"
)

func TestRunTerminalSmokeCompletesAttachedLifecycle(t *testing.T) {
	const (
		token      = "terminal-smoke-token"
		terminalID = "terminal_0123456789abcdef0123456789abcdef"
		sessionID  = "terminal-smoke-"
	)
	var chatConnected atomic.Bool
	var operatorConnected atomic.Bool
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	mux := http.NewServeMux()
	mux.HandleFunc(terminalChatPath, func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer "+token ||
			request.Header.Get("Origin") != "https://launcher.example.test" ||
			!strings.HasPrefix(request.URL.Query().Get("session_id"), sessionID) {
			http.Error(writer, "unauthorized", http.StatusUnauthorized)
			return
		}
		connection, err := upgrader.Upgrade(writer, request, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer connection.Close()
		var message terminalChatMessage
		if err := connection.ReadJSON(&message); err != nil {
			t.Error(err)
			return
		}
		content, _ := message.Payload["content"].(string)
		if message.Type != "message.send" ||
			message.SessionID != request.URL.Query().Get("session_id") ||
			!strings.Contains(content, `target "vpn-smoke"`) ||
			!strings.Contains(content, `profile "owner-test"`) {
			t.Errorf("unexpected terminal open request: %#v", message)
			return
		}
		chatConnected.Store(true)
		if err := connection.WriteJSON(terminalChatMessage{
			Type:    "message.create",
			Payload: map[string]any{"content": "TERMINAL_ID=" + terminalID},
		}); err != nil {
			t.Error(err)
		}
	})
	mux.HandleFunc(terminalOperatorPath, func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer "+token ||
			request.Header.Get("Origin") != "https://launcher.example.test" ||
			!strings.HasPrefix(request.URL.Query().Get("session_id"), sessionID) ||
			request.URL.Query().Get("terminal_id") != terminalID {
			http.Error(writer, "unauthorized", http.StatusUnauthorized)
			return
		}
		connection, err := upgrader.Upgrade(writer, request, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer connection.Close()
		operatorConnected.Store(true)
		if err := connection.WriteJSON(terminalOperatorAttached{
			Version: nodepkg.TerminalProtocolVersion, Type: "attached",
			TerminalID: terminalID, State: "live",
		}); err != nil {
			t.Error(err)
			return
		}
		var resize terminalOperatorControl
		if err := connection.ReadJSON(&resize); err != nil {
			t.Error(err)
			return
		}
		if resize.Type != "resize" || resize.Sequence != 1 ||
			resize.Columns != 100 || resize.Rows != 31 {
			t.Errorf("unexpected resize: %#v", resize)
			return
		}
		var input terminalOperatorControl
		if err := connection.ReadJSON(&input); err != nil {
			t.Error(err)
			return
		}
		script, err := base64.StdEncoding.Strict().DecodeString(input.InputBase64)
		if err != nil || input.Type != "input" || input.Sequence != 2 ||
			strings.Contains(string(script), terminalSmokeMarker) {
			t.Errorf("unexpected smoke input: %#v, %q, %v", input, script, err)
			return
		}
		outputBytes := []byte("\x1b[?2004l\r\nMINTCLAW_PTY_OK UID=1001 SIZE=31 100\r\n")
		output := base64.StdEncoding.EncodeToString(outputBytes)
		if err := connection.WriteJSON(nodepkg.TerminalEvent{
			Version: nodepkg.TerminalProtocolVersion,
			Type:    "output", TerminalID: terminalID,
			Cursor: uint64(len(outputBytes)), DataBase64: output,
		}); err != nil {
			t.Error(err)
			return
		}
		var closeRequest terminalOperatorControl
		if err := connection.ReadJSON(&closeRequest); err != nil {
			t.Error(err)
			return
		}
		if closeRequest.Type != "close" || closeRequest.Sequence != 3 {
			t.Errorf("unexpected close: %#v", closeRequest)
			return
		}
		if err := connection.WriteJSON(nodepkg.TerminalEvent{
			Version: nodepkg.TerminalProtocolVersion,
			Type:    "closed", TerminalID: terminalID,
			State: "closed", Reason: "close",
			StartedAt: 1, CompletedAt: 2, TerminationConfirmed: true,
		}); err != nil {
			t.Error(err)
		}
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	cfg := terminalSmokeTestConfig(t, server.URL, token)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := runTerminalSmoke(ctx, cfg, terminalSmokeOptions{
		Target: "vpn-smoke", Profile: "owner-test", WorkingScope: "workspace",
		Columns: 100, Rows: 31,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !chatConnected.Load() || !operatorConnected.Load() {
		t.Fatal("smoke did not use both authenticated websocket surfaces")
	}
	if result.Target != "vpn-smoke" ||
		result.Profile != "owner-test" ||
		result.TerminalID != terminalID ||
		result.UID != 1001 ||
		result.Rows != 31 ||
		result.Columns != 100 ||
		result.Marker != terminalSmokeMarker ||
		result.State != "closed" ||
		result.CloseReason != "close" {
		t.Fatalf("smoke result = %#v", result)
	}
}

func TestRunTerminalSmokeRequiresEnabledTerminalAndToken(t *testing.T) {
	cfg := config.DefaultConfig()
	_, err := runTerminalSmoke(context.Background(), cfg, terminalSmokeOptions{
		Target: "vpn-smoke", Profile: "owner-test", WorkingScope: "workspace",
		Columns: 100, Rows: 31,
	})
	if err == nil || !strings.Contains(err.Error(), "terminal_enabled") {
		t.Fatalf("disabled terminal error = %v", err)
	}

	cfg.Nodes.Enabled = true
	cfg.Nodes.TerminalEnabled = true
	_, err = runTerminalSmoke(context.Background(), cfg, terminalSmokeOptions{
		Target: "vpn-smoke", Profile: "owner-test", WorkingScope: "workspace",
		Columns: 100, Rows: 31,
	})
	if err == nil || !strings.Contains(err.Error(), "MintClaw channel") {
		t.Fatalf("missing channel error = %v", err)
	}
}

func TestLocalGatewayWebSocketURLRejectsRemoteTokenTransport(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Gateway.Host = "gateway.example.com"
	cfg.Gateway.Port = 18790
	if _, err := localGatewayWebSocketURL(cfg); err == nil ||
		!strings.Contains(err.Error(), "loopback") {
		t.Fatalf("remote gateway error = %v", err)
	}

	cfg.Gateway.Host = "0.0.0.0"
	endpoint, err := localGatewayWebSocketURL(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if endpoint.Hostname() != "127.0.0.1" {
		t.Fatalf("unspecified gateway endpoint = %s", endpoint)
	}

	cfg.Gateway.Host = "::1,127.0.0.1"
	endpoint, err = localGatewayWebSocketURL(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if endpoint.Hostname() != "::1" {
		t.Fatalf("multi-host gateway endpoint = %s", endpoint)
	}
}

func terminalSmokeTestConfig(t *testing.T, serverURL string, token string) *config.Config {
	t.Helper()
	parsed, err := url.Parse(serverURL)
	if err != nil {
		t.Fatal(err)
	}
	host, portValue, err := net.SplitHostPort(parsed.Host)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portValue)
	if err != nil {
		t.Fatal(err)
	}
	settings, err := json.Marshal(map[string]any{
		"token":         token,
		"allow_origins": []string{"https://launcher.example.test"},
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.DefaultConfig()
	cfg.Gateway.Host = host
	cfg.Gateway.Port = port
	cfg.Nodes.Enabled = true
	cfg.Nodes.TerminalEnabled = true
	cfg.Channels[config.ChannelMintClaw] = &config.Channel{
		Enabled: true, Type: config.ChannelMintClaw, Settings: config.RawNode(settings),
	}
	return cfg
}
