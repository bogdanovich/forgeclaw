package nodes

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/spf13/cobra"

	"github.com/bogdanovich/mintclaw/pkg/config"
	nodepkg "github.com/bogdanovich/mintclaw/pkg/nodes"
)

const (
	terminalChatPath     = "/mintclaw/ws"
	terminalOperatorPath = "/nodes/v1/terminal/ws"
	terminalSmokeMarker  = "MINTCLAW_PTY_OK"
)

var (
	terminalIDPattern     = regexp.MustCompile(`terminal_[0-9a-f]{32}`)
	terminalOutputPattern = regexp.MustCompile(
		`MINTCLAW_PTY_OK UID=([0-9]+) SIZE=([0-9]+) ([0-9]+)`,
	)
)

type terminalConfigLoader func() (*config.Config, error)

type terminalOperatorCredentials struct {
	Token  string
	Origin string
}

type terminalSmokeOptions struct {
	Target       string
	Profile      string
	WorkingScope string
	Columns      int
	Rows         int
	Timeout      time.Duration
}

type terminalSmokeResult struct {
	Target      string `json:"target"`
	Profile     string `json:"profile"`
	TerminalID  string `json:"terminal_id"`
	UID         int    `json:"uid"`
	Rows        int    `json:"rows"`
	Columns     int    `json:"columns"`
	Marker      string `json:"marker"`
	State       string `json:"state"`
	CloseReason string `json:"close_reason,omitempty"`
}

type terminalChatMessage struct {
	Type      string         `json:"type"`
	ID        string         `json:"id,omitempty"`
	SessionID string         `json:"session_id,omitempty"`
	Payload   map[string]any `json:"payload,omitempty"`
}

type terminalOperatorControl struct {
	Version        int    `json:"version"`
	Type           string `json:"type"`
	Sequence       uint64 `json:"sequence"`
	IdempotencyKey string `json:"idempotency_key"`
	InputBase64    string `json:"input_base64,omitempty"`
	Columns        int    `json:"columns,omitempty"`
	Rows           int    `json:"rows,omitempty"`
}

type terminalOperatorAttached struct {
	Version    int    `json:"version"`
	Type       string `json:"type"`
	TerminalID string `json:"terminal_id"`
	State      string `json:"state"`
}

func newTerminalCommand(load terminalConfigLoader) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "terminal",
		Short: "Test and operate attached node terminals",
		Args:  cobra.NoArgs,
	}
	cmd.AddCommand(newTerminalSmokeCommand(load))
	return cmd
}

func newTerminalSmokeCommand(load terminalConfigLoader) *cobra.Command {
	options := terminalSmokeOptions{
		Columns: 100,
		Rows:    31,
		Timeout: 2 * time.Minute,
	}
	var jsonOutput bool
	cmd := &cobra.Command{
		Use:   "smoke",
		Short: "Run a safe end-to-end PTY lifecycle check",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := load()
			if err != nil {
				return err
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), options.Timeout)
			defer cancel()
			result, err := runTerminalSmoke(ctx, cfg, options)
			if err != nil {
				return err
			}
			if jsonOutput {
				return writeJSON(cmd.OutOrStdout(), result)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Target: %s\n", result.Target)
			fmt.Fprintf(cmd.OutOrStdout(), "Profile: %s\n", result.Profile)
			fmt.Fprintf(cmd.OutOrStdout(), "UID: %d\n", result.UID)
			fmt.Fprintf(cmd.OutOrStdout(), "Size: %dx%d\n", result.Columns, result.Rows)
			fmt.Fprintf(cmd.OutOrStdout(), "Marker: %s\n", result.Marker)
			fmt.Fprintf(cmd.OutOrStdout(), "State: %s\n", result.State)
			return nil
		},
	}
	cmd.Flags().StringVar(&options.Target, "target", "", "Visible node target name")
	cmd.Flags().StringVar(&options.Profile, "profile", "", "Owner shell profile alias")
	cmd.Flags().StringVar(&options.WorkingScope, "working-scope", "", "Configured working-scope alias")
	cmd.Flags().IntVar(&options.Columns, "columns", options.Columns, "PTY columns for the resize check")
	cmd.Flags().IntVar(&options.Rows, "rows", options.Rows, "PTY rows for the resize check")
	cmd.Flags().DurationVar(&options.Timeout, "timeout", options.Timeout, "Overall smoke-test timeout")
	cmd.Flags().BoolVar(&jsonOutput, "json", false, "Emit stable JSON output")
	_ = cmd.MarkFlagRequired("target")
	_ = cmd.MarkFlagRequired("profile")
	_ = cmd.MarkFlagRequired("working-scope")
	return cmd
}

func runTerminalSmoke(
	ctx context.Context,
	cfg *config.Config,
	options terminalSmokeOptions,
) (terminalSmokeResult, error) {
	if cfg == nil {
		return terminalSmokeResult{}, errors.New("terminal smoke requires configuration")
	}
	if !cfg.Nodes.Enabled || !cfg.Nodes.TerminalEnabled {
		return terminalSmokeResult{}, errors.New(
			"terminal smoke requires nodes.enabled and nodes.terminal_enabled",
		)
	}
	if strings.TrimSpace(options.Target) == "" ||
		strings.TrimSpace(options.Profile) == "" ||
		strings.TrimSpace(options.WorkingScope) == "" {
		return terminalSmokeResult{}, errors.New("target, profile, and working scope are required")
	}
	if options.Columns < 20 || options.Columns > 400 ||
		options.Rows < 5 || options.Rows > 200 {
		return terminalSmokeResult{}, errors.New("terminal size is outside supported bounds")
	}
	credentials, err := mintClawOperatorCredentials(cfg)
	if err != nil {
		return terminalSmokeResult{}, err
	}
	baseURL, err := localGatewayWebSocketURL(cfg)
	if err != nil {
		return terminalSmokeResult{}, err
	}
	sessionID := "terminal-smoke-" + uuid.NewString()
	header := http.Header{
		"Authorization": []string{"Bearer " + credentials.Token},
	}
	if credentials.Origin != "" {
		header.Set("Origin", credentials.Origin)
	}
	chatURL := *baseURL
	chatURL.Path = terminalChatPath
	query := chatURL.Query()
	query.Set("session_id", sessionID)
	chatURL.RawQuery = query.Encode()
	chat, err := dialTerminalWebSocket(ctx, chatURL.String(), header)
	if err != nil {
		return terminalSmokeResult{}, fmt.Errorf("connect authenticated MintClaw session: %w", err)
	}
	defer chat.Close()
	if deadline, ok := ctx.Deadline(); ok {
		_ = chat.SetReadDeadline(deadline)
		_ = chat.SetWriteDeadline(deadline)
	}
	requestID := uuid.NewString()
	prompt := terminalSmokeOpenPrompt(options)
	writeErr := chat.WriteJSON(terminalChatMessage{
		Type:      "message.send",
		ID:        requestID,
		SessionID: sessionID,
		Payload:   map[string]any{"content": prompt},
	})
	if writeErr != nil {
		return terminalSmokeResult{}, fmt.Errorf("request terminal open: %w", writeErr)
	}
	terminalID, err := waitForTerminalID(chat)
	if err != nil {
		return terminalSmokeResult{}, err
	}
	operatorURL := *baseURL
	operatorURL.Path = terminalOperatorPath
	query = operatorURL.Query()
	query.Set("session_id", sessionID)
	query.Set("terminal_id", terminalID)
	operatorURL.RawQuery = query.Encode()
	operator, err := dialTerminalWebSocket(ctx, operatorURL.String(), header)
	if err != nil {
		return terminalSmokeResult{}, fmt.Errorf("attach operator terminal: %w", err)
	}
	defer operator.Close()
	if deadline, ok := ctx.Deadline(); ok {
		_ = operator.SetReadDeadline(deadline)
		_ = operator.SetWriteDeadline(deadline)
	}
	var attached terminalOperatorAttached
	readErr := operator.ReadJSON(&attached)
	if readErr != nil {
		return terminalSmokeResult{}, fmt.Errorf("read terminal attachment: %w", readErr)
	}
	if attached.Version != nodepkg.TerminalProtocolVersion ||
		attached.Type != "attached" ||
		attached.TerminalID != terminalID ||
		attached.State != "live" {
		return terminalSmokeResult{}, errors.New("gateway returned an invalid terminal attachment")
	}
	writeErr = operator.WriteJSON(terminalOperatorControl{
		Version:        nodepkg.TerminalProtocolVersion,
		Type:           "resize",
		Sequence:       1,
		IdempotencyKey: "terminal_smoke_resize_1",
		Columns:        options.Columns,
		Rows:           options.Rows,
	})
	if writeErr != nil {
		return terminalSmokeResult{}, fmt.Errorf("resize terminal: %w", writeErr)
	}
	script := "stty -echo; printf 'MINTCLAW_%s UID=%s SIZE=%s\\n' " +
		"'PTY_OK' \"$(id -u)\" \"$(stty size)\"\n"
	writeErr = operator.WriteJSON(terminalOperatorControl{
		Version:        nodepkg.TerminalProtocolVersion,
		Type:           "input",
		Sequence:       2,
		IdempotencyKey: "terminal_smoke_input_2",
		InputBase64:    base64.StdEncoding.EncodeToString([]byte(script)),
	})
	if writeErr != nil {
		return terminalSmokeResult{}, fmt.Errorf("write terminal input: %w", writeErr)
	}
	result, err := readTerminalSmokeOutput(operator, options, terminalID)
	if err != nil {
		return terminalSmokeResult{}, err
	}
	result.Target = options.Target
	result.Profile = options.Profile
	result.TerminalID = terminalID
	return result, nil
}

func dialTerminalWebSocket(
	ctx context.Context,
	endpoint string,
	header http.Header,
) (*websocket.Conn, error) {
	connection, response, err := websocket.DefaultDialer.DialContext(ctx, endpoint, header)
	if err == nil {
		return connection, nil
	}
	if response != nil && response.Body != nil {
		_ = response.Body.Close()
	}
	return nil, err
}

func terminalSmokeOpenPrompt(options terminalSmokeOptions) string {
	return fmt.Sprintf(
		"Use only nodes_terminal. Discover target %q, then open one attached terminal "+
			"with profile %q, working_scope %q, columns %d, rows %d, and the returned "+
			"discovery_revision. Return exactly TERMINAL_ID=<terminal_id> and nothing else. "+
			"Do not use shell.exec, system exec, or any file tool.",
		options.Target,
		options.Profile,
		options.WorkingScope,
		options.Columns,
		options.Rows,
	)
}

func waitForTerminalID(connection *websocket.Conn) (string, error) {
	for {
		var message terminalChatMessage
		if err := connection.ReadJSON(&message); err != nil {
			return "", fmt.Errorf("wait for terminal open: %w", err)
		}
		content, _ := message.Payload["content"].(string)
		if terminalID := terminalIDPattern.FindString(content); terminalID != "" {
			return terminalID, nil
		}
		if message.Type == "error" {
			return "", errors.New("MintClaw session rejected terminal open")
		}
	}
}

func readTerminalSmokeOutput(
	connection *websocket.Conn,
	options terminalSmokeOptions,
	terminalID string,
) (terminalSmokeResult, error) {
	var output strings.Builder
	var cursor uint64
	closeSent := false
	var parsed terminalSmokeResult
	for {
		var event nodepkg.TerminalEvent
		if err := connection.ReadJSON(&event); err != nil {
			return terminalSmokeResult{}, fmt.Errorf("read terminal event: %w", err)
		}
		if _, err := event.Validate(); err != nil {
			return terminalSmokeResult{}, errors.New("terminal returned an invalid event")
		}
		if event.TerminalID != terminalID {
			return terminalSmokeResult{}, errors.New("terminal event identity changed")
		}
		switch event.Type {
		case "output":
			data, err := base64.StdEncoding.Strict().DecodeString(event.DataBase64)
			if err != nil {
				return terminalSmokeResult{}, errors.New("terminal returned invalid base64 output")
			}
			if output.Len()+len(data) > nodepkg.MaxTerminalTransportBuffer {
				return terminalSmokeResult{}, errors.New("terminal smoke output exceeded transport limit")
			}
			cursor += uint64(len(data))
			if event.Cursor != cursor {
				return terminalSmokeResult{}, errors.New("terminal output cursor is discontinuous")
			}
			output.Write(data)
			if !closeSent {
				match := terminalOutputPattern.FindStringSubmatch(output.String())
				if len(match) == 4 {
					uid, uidErr := strconv.Atoi(match[1])
					rows, rowsErr := strconv.Atoi(match[2])
					columns, columnsErr := strconv.Atoi(match[3])
					if uidErr != nil || rowsErr != nil || columnsErr != nil ||
						rows != options.Rows || columns != options.Columns {
						return terminalSmokeResult{}, errors.New("terminal output did not confirm requested size")
					}
					parsed.UID = uid
					parsed.Rows = rows
					parsed.Columns = columns
					parsed.Marker = terminalSmokeMarker
					if err := connection.WriteJSON(terminalOperatorControl{
						Version:        nodepkg.TerminalProtocolVersion,
						Type:           "close",
						Sequence:       3,
						IdempotencyKey: "terminal_smoke_close_3",
					}); err != nil {
						return terminalSmokeResult{}, fmt.Errorf("close terminal: %w", err)
					}
					closeSent = true
				}
			}
		case "closed":
			if !closeSent || !event.TerminationConfirmed {
				return terminalSmokeResult{}, errors.New("terminal closed before smoke completion")
			}
			parsed.State = event.State
			parsed.CloseReason = event.Reason
			return parsed, nil
		case "unknown", "denied":
			return terminalSmokeResult{}, fmt.Errorf("terminal entered %s state", event.Type)
		}
	}
}

func mintClawOperatorCredentials(cfg *config.Config) (terminalOperatorCredentials, error) {
	channel := cfg.Channels.GetByType(config.ChannelMintClaw)
	if channel == nil || !channel.Enabled {
		return terminalOperatorCredentials{}, errors.New(
			"enabled MintClaw channel is required for terminal smoke",
		)
	}
	decoded, err := channel.GetDecoded()
	if err != nil {
		return terminalOperatorCredentials{}, fmt.Errorf("decode MintClaw channel: %w", err)
	}
	settings, ok := decoded.(*config.MintClawSettings)
	if !ok || strings.TrimSpace(settings.Token.String()) == "" {
		return terminalOperatorCredentials{}, errors.New(
			"MintClaw channel token is required for terminal smoke",
		)
	}
	origin := ""
	for _, allowed := range settings.AllowOrigins {
		allowed = strings.TrimSpace(allowed)
		if allowed != "" && allowed != "*" {
			origin = allowed
			break
		}
	}
	return terminalOperatorCredentials{
		Token:  strings.TrimSpace(settings.Token.String()),
		Origin: origin,
	}, nil
}

func localGatewayWebSocketURL(cfg *config.Config) (*url.URL, error) {
	host := strings.TrimSpace(cfg.Gateway.Host)
	if host == "" || host == "0.0.0.0" || host == "::" || host == "[::]" {
		host = "127.0.0.1"
	}
	normalizedHost := strings.Trim(host, "[]")
	if !strings.EqualFold(normalizedHost, "localhost") {
		parsed := net.ParseIP(normalizedHost)
		if parsed == nil || !parsed.IsLoopback() {
			return nil, errors.New(
				"terminal smoke must run on the gateway host through a loopback address",
			)
		}
	}
	if cfg.Gateway.Port <= 0 {
		return nil, errors.New("gateway port is required for terminal smoke")
	}
	return &url.URL{
		Scheme: "ws",
		Host:   net.JoinHostPort(normalizedHost, strconv.Itoa(cfg.Gateway.Port)),
	}, nil
}
