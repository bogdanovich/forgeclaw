package nodes

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"golang.org/x/term"

	"github.com/bogdanovich/mintclaw/pkg/config"
	nodepkg "github.com/bogdanovich/mintclaw/pkg/nodes"
)

type fakeLocalTerminal struct {
	mu       sync.Mutex
	columns  int
	rows     int
	madeRaw  int
	restored int
}

func handleTerminalTestServer(
	mux *http.ServeMux,
	open http.HandlerFunc,
	operator http.HandlerFunc,
) {
	mux.HandleFunc(terminalOperatorPath, func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodPost {
			open(writer, request)
			return
		}
		operator(writer, request)
	})
}

func (*fakeLocalTerminal) IsTerminal(int) bool { return true }
func (terminal *fakeLocalTerminal) GetSize(int) (int, int, error) {
	terminal.mu.Lock()
	defer terminal.mu.Unlock()
	return terminal.columns, terminal.rows, nil
}

func (terminal *fakeLocalTerminal) MakeRaw(int) (*term.State, error) {
	terminal.mu.Lock()
	defer terminal.mu.Unlock()
	terminal.madeRaw++
	return &term.State{}, nil
}

func (terminal *fakeLocalTerminal) Restore(int, *term.State) error {
	terminal.mu.Lock()
	defer terminal.mu.Unlock()
	terminal.restored++
	return nil
}

func (terminal *fakeLocalTerminal) resize(columns, rows int) {
	terminal.mu.Lock()
	terminal.columns = columns
	terminal.rows = rows
	terminal.mu.Unlock()
}

func TestRunTerminalSmokeCompletesAttachedLifecycle(t *testing.T) {
	const (
		token      = "terminal-smoke-token"
		terminalID = "terminal_0123456789abcdef0123456789abcdef"
		sessionID  = "terminal-operator-"
	)
	var openConnected atomic.Bool
	var operatorConnected atomic.Bool
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	mux := http.NewServeMux()
	handleTerminalTestServer(mux, func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer "+token ||
			request.Header.Get("Origin") != "https://launcher.example.test" ||
			request.Method != http.MethodPost {
			http.Error(writer, "unauthorized", http.StatusUnauthorized)
			return
		}
		var open terminalOpenRequest
		if err := json.NewDecoder(request.Body).Decode(&open); err != nil {
			t.Error(err)
			return
		}
		if !strings.HasPrefix(open.SessionID, sessionID) ||
			open.Target != "vpn-smoke" || open.Profile != "owner-test" ||
			open.WorkingScope != "workspace" || open.Columns != 100 || open.Rows != 31 {
			t.Errorf("unexpected terminal open request: %#v", open)
			return
		}
		openConnected.Store(true)
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusCreated)
		if err := json.NewEncoder(writer).Encode(terminalOpenResult{
			TerminalID: terminalID,
			State:      string(nodepkg.GatewayTerminalPendingAttach),
		}); err != nil {
			t.Error(err)
		}
	}, func(writer http.ResponseWriter, request *http.Request) {
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
		if err := connection.WriteJSON(nodepkg.TerminalEvent{
			Version: nodepkg.TerminalProtocolVersion,
			Type:    "ack", TerminalID: terminalID,
			AcceptedSequence: 1, State: "live",
		}); err != nil {
			t.Error(err)
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
			Type:    "ack", TerminalID: terminalID,
			AcceptedSequence: 3, State: "live",
		}); err != nil {
			t.Error(err)
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
	if !openConnected.Load() || !operatorConnected.Load() {
		t.Fatal("smoke did not use authenticated open and operator surfaces")
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

func TestReadTerminalSmokeOutputRequiresResizeAndCloseProof(t *testing.T) {
	const terminalID = "terminal_0123456789abcdef0123456789abcdef"
	options := terminalSmokeOptions{Columns: 100, Rows: 31}
	tests := []struct {
		name       string
		sendEvents func(*testing.T, *websocket.Conn)
	}{
		{
			name: "missing resize acknowledgement",
			sendEvents: func(t *testing.T, connection *websocket.Conn) {
				t.Helper()
				writeTerminalSmokeOutput(t, connection, terminalID)
				writeTerminalClosed(t, connection, terminalID, "close")
			},
		},
		{
			name: "natural exit after marker",
			sendEvents: func(t *testing.T, connection *websocket.Conn) {
				t.Helper()
				writeTerminalAck(t, connection, terminalID, 1)
				writeTerminalSmokeOutput(t, connection, terminalID)
				var closeRequest terminalOperatorControl
				if err := connection.ReadJSON(&closeRequest); err != nil {
					t.Fatal(err)
				}
				writeTerminalAck(t, connection, terminalID, 3)
				writeTerminalClosed(t, connection, terminalID, "exit")
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				connection, err := upgrader.Upgrade(writer, request, nil)
				if err != nil {
					t.Error(err)
					return
				}
				defer connection.Close()
				test.sendEvents(t, connection)
			}))
			defer server.Close()
			endpoint := "ws" + strings.TrimPrefix(server.URL, "http")
			connection, _, err := websocket.DefaultDialer.Dial(endpoint, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer connection.Close()
			_, err = readTerminalSmokeOutput(connection, options, terminalID)
			if err == nil || !strings.Contains(err.Error(), "requested close") {
				t.Fatalf("close proof error = %v", err)
			}
		})
	}
}

func TestRunInteractiveTerminalForwardsBytesResizeAndRestoresRawMode(t *testing.T) {
	const (
		token      = "terminal-open-token"
		terminalID = "terminal_0123456789abcdef0123456789abcdef"
	)
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	inputReceived := make(chan struct{})
	resizeReceived := make(chan struct{})
	escapeReady := make(chan struct{})
	mux := http.NewServeMux()
	handleTerminalTestServer(mux, func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(writer).Encode(terminalOpenResult{
			TerminalID: terminalID,
			State:      string(nodepkg.GatewayTerminalPendingAttach),
		})
	}, func(writer http.ResponseWriter, request *http.Request) {
		connection, err := upgrader.Upgrade(writer, request, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer connection.Close()
		if err := connection.WriteJSON(terminalOperatorAttached{
			Version: nodepkg.TerminalProtocolVersion, Type: "attached",
			TerminalID: terminalID, State: "live",
		}); err != nil {
			t.Error(err)
			return
		}
		var initial terminalOperatorControl
		if err := connection.ReadJSON(&initial); err != nil {
			t.Error(err)
			return
		}
		if initial.Type != "resize" || initial.Sequence != 1 ||
			initial.Columns != 90 || initial.Rows != 25 {
			t.Errorf("initial resize = %#v", initial)
			return
		}
		var input terminalOperatorControl
		if err := connection.ReadJSON(&input); err != nil {
			t.Error(err)
			return
		}
		inputBytes, err := base64.StdEncoding.Strict().DecodeString(input.InputBase64)
		if err != nil || input.Type != "input" || input.Sequence != 2 || string(inputBytes) != "printf ok\n" {
			t.Errorf("terminal input = %#v, %q, %v", input, inputBytes, err)
			return
		}
		close(inputReceived)
		output := []byte("REMOTE_OUTPUT")
		if err := connection.WriteJSON(nodepkg.TerminalEvent{
			Version: nodepkg.TerminalProtocolVersion, Type: "output", TerminalID: terminalID,
			Cursor: uint64(len(output)), DataBase64: base64.StdEncoding.EncodeToString(output),
		}); err != nil {
			t.Error(err)
			return
		}
		var resize terminalOperatorControl
		if err := connection.ReadJSON(&resize); err != nil {
			t.Error(err)
			return
		}
		if resize.Type != "resize" || resize.Sequence != 3 ||
			resize.Columns != 120 || resize.Rows != 40 {
			t.Errorf("SIGWINCH resize = %#v", resize)
			return
		}
		close(resizeReceived)
		close(escapeReady)
		var closeRequest terminalOperatorControl
		if err := connection.ReadJSON(&closeRequest); err != nil {
			t.Error(err)
			return
		}
		if closeRequest.Type != "close" || closeRequest.Sequence != 4 {
			t.Errorf("local escape close = %#v", closeRequest)
			return
		}
		_ = connection.WriteJSON(nodepkg.TerminalEvent{
			Version: nodepkg.TerminalProtocolVersion, Type: "closed", TerminalID: terminalID,
			State: "closed", Reason: "close", StartedAt: 1, CompletedAt: 2,
			TerminationConfirmed: true,
		})
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	cfg := terminalSmokeTestConfig(t, server.URL, token)
	stdin, inputWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer stdin.Close()
	defer inputWriter.Close()
	local := &fakeLocalTerminal{columns: 90, rows: 25}
	resizeSignals := make(chan os.Signal, 1)
	terminationSignals := make(chan os.Signal, 1)
	var output bytes.Buffer
	var status bytes.Buffer
	done := make(chan error, 1)
	go func() {
		done <- runInteractiveTerminal(
			t.Context(), cfg,
			terminalOpenOptions{Target: "vpn-smoke", Profile: "owner-test", WorkingScope: "workspace"},
			stdin, &output, &status, local, resizeSignals, terminationSignals,
		)
	}()
	if _, err := inputWriter.Write([]byte("printf ok\n")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-inputReceived:
	case <-time.After(5 * time.Second):
		t.Fatal("interactive input was not forwarded")
	}
	local.resize(120, 40)
	resizeSignals <- os.Interrupt
	select {
	case <-resizeReceived:
	case <-time.After(5 * time.Second):
		t.Fatal("interactive resize was not forwarded")
	}
	<-escapeReady
	if _, err := inputWriter.Write([]byte{terminalLocalEscape}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("interactive terminal did not close")
	}
	local.mu.Lock()
	madeRaw, restored := local.madeRaw, local.restored
	local.mu.Unlock()
	if madeRaw != 1 || restored != 1 || output.String() != "REMOTE_OUTPUT" ||
		!strings.Contains(status.String(), "Attached") ||
		!strings.Contains(status.String(), "Ctrl+]") {
		t.Fatalf(
			"interactive result raw=%d restore=%d output=%q status=%q",
			madeRaw,
			restored,
			output.String(),
			status.String(),
		)
	}
}

func TestRunInteractiveTerminalRestoresRawModeOnProtocolDenial(t *testing.T) {
	const terminalID = "terminal_0123456789abcdef0123456789abcdef"
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	mux := http.NewServeMux()
	handleTerminalTestServer(mux, func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(writer).Encode(terminalOpenResult{
			TerminalID: terminalID,
			State:      string(nodepkg.GatewayTerminalPendingAttach),
		})
	}, func(writer http.ResponseWriter, request *http.Request) {
		connection, err := upgrader.Upgrade(writer, request, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer connection.Close()
		_ = connection.WriteJSON(terminalOperatorAttached{
			Version: nodepkg.TerminalProtocolVersion, Type: "attached",
			TerminalID: terminalID, State: "live",
		})
		var initial terminalOperatorControl
		if err := connection.ReadJSON(&initial); err != nil {
			t.Error(err)
			return
		}
		_ = connection.WriteJSON(nodepkg.TerminalEvent{
			Version: nodepkg.TerminalProtocolVersion, Type: "denied", TerminalID: terminalID,
			State: "live", Reason: "input_denied",
		})
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	cfg := terminalSmokeTestConfig(t, server.URL, "terminal-token")
	stdin, inputWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer stdin.Close()
	defer inputWriter.Close()
	local := &fakeLocalTerminal{columns: 90, rows: 25}
	err = runInteractiveTerminal(
		t.Context(), cfg,
		terminalOpenOptions{Target: "vpn-smoke", Profile: "owner-test", WorkingScope: "workspace"},
		stdin, io.Discard, io.Discard, local,
		make(chan os.Signal), make(chan os.Signal),
	)
	if err == nil || !strings.Contains(err.Error(), "denied") {
		t.Fatalf("protocol denial error = %v", err)
	}
	local.mu.Lock()
	madeRaw, restored := local.madeRaw, local.restored
	local.mu.Unlock()
	if madeRaw != 1 || restored != 1 {
		t.Fatalf("protocol denial raw=%d restore=%d", madeRaw, restored)
	}
}

func writeTerminalAck(t *testing.T, connection *websocket.Conn, terminalID string, sequence uint64) {
	t.Helper()
	if err := connection.WriteJSON(nodepkg.TerminalEvent{
		Version: nodepkg.TerminalProtocolVersion,
		Type:    "ack", TerminalID: terminalID,
		AcceptedSequence: sequence, State: "live",
	}); err != nil {
		t.Fatal(err)
	}
}

func writeTerminalSmokeOutput(t *testing.T, connection *websocket.Conn, terminalID string) {
	t.Helper()
	outputBytes := []byte("MINTCLAW_PTY_OK UID=1001 SIZE=31 100\r\n")
	if err := connection.WriteJSON(nodepkg.TerminalEvent{
		Version: nodepkg.TerminalProtocolVersion,
		Type:    "output", TerminalID: terminalID,
		Cursor:     uint64(len(outputBytes)),
		DataBase64: base64.StdEncoding.EncodeToString(outputBytes),
	}); err != nil {
		t.Fatal(err)
	}
}

func writeTerminalClosed(t *testing.T, connection *websocket.Conn, terminalID string, reason string) {
	t.Helper()
	if err := connection.WriteJSON(nodepkg.TerminalEvent{
		Version: nodepkg.TerminalProtocolVersion,
		Type:    "closed", TerminalID: terminalID,
		State: "closed", Reason: reason,
		StartedAt: 1, CompletedAt: 2, TerminationConfirmed: true,
	}); err != nil {
		t.Fatal(err)
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

func TestRunTerminalSmokeFailsFastWithExplicitOpenError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != terminalOperatorOpenPath {
			t.Fatalf("unexpected request path %q", request.URL.Path)
		}
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(writer).Encode(terminalOpenError{Error: "TARGET_UNAVAILABLE"})
	}))
	defer server.Close()
	var progress []string
	_, err := runTerminalSmoke(
		t.Context(),
		terminalSmokeTestConfig(t, server.URL, "terminal-token"),
		terminalSmokeOptions{
			Target: "vpn-smoke", Profile: "owner-test", WorkingScope: "workspace",
			Columns: 100, Rows: 31,
			Progress: func(message string) { progress = append(progress, message) },
		},
	)
	if err == nil || !strings.Contains(err.Error(), "TARGET_UNAVAILABLE") ||
		len(progress) != 1 || !strings.Contains(progress[0], "Opening") {
		t.Fatalf("smoke error = %v, progress = %#v", err, progress)
	}
}

func TestLocalGatewayURLRejectsRemoteTokenTransport(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Gateway.Host = "gateway.example.com"
	cfg.Gateway.Port = 18790
	if _, err := localGatewayURL(cfg); err == nil ||
		!strings.Contains(err.Error(), "loopback") {
		t.Fatalf("remote gateway error = %v", err)
	}

	cfg.Gateway.Host = "0.0.0.0"
	endpoint, err := localGatewayURL(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if endpoint.Hostname() != "127.0.0.1" {
		t.Fatalf("unspecified gateway endpoint = %s", endpoint)
	}

	cfg.Gateway.Host = "::1,127.0.0.1"
	endpoint, err = localGatewayURL(cfg)
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
