package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/spf13/cobra"

	"github.com/bogdanovich/mintclaw/pkg/bus"
	channelmintclaw "github.com/bogdanovich/mintclaw/pkg/channels/mintclaw"
	"github.com/bogdanovich/mintclaw/pkg/config"
	runtimeevents "github.com/bogdanovich/mintclaw/pkg/events"
	"github.com/bogdanovich/mintclaw/pkg/netbind"
	"github.com/bogdanovich/mintclaw/pkg/routing"
)

const (
	liveProtocolVersion = 1
	liveMaxInputBytes   = 64 * 1024
	liveMaxOutputBytes  = 1024 * 1024
)

type liveConfigPath func() string

type liveOptions struct {
	ConfigPath string
	Message    string
	SessionID  string
	Timeout    time.Duration
	JSON       bool
	Progress   func(string)
}

type liveResult struct {
	Version            int                      `json:"version"`
	Outcome            string                   `json:"outcome"`
	ActorID            string                   `json:"actor_id"`
	AgentID            string                   `json:"agent_id,omitempty"`
	SessionID          string                   `json:"session_id"`
	SessionKey         string                   `json:"session_key,omitempty"`
	RequestID          string                   `json:"request_id"`
	TraceScope         runtimeevents.TraceScope `json:"trace_scope,omitempty"`
	InteractionID      string                   `json:"interaction_id,omitempty"`
	InteractionShortID string                   `json:"interaction_short_id,omitempty"`
	Response           string                   `json:"response,omitempty"`
	DurationMS         int64                    `json:"duration_ms"`
}

type liveRunError struct {
	cause error
}

func (e *liveRunError) Error() string { return e.cause.Error() }
func (e *liveRunError) Unwrap() error { return e.cause }

func newLiveCommand(defaultConfig liveConfigPath) *cobra.Command {
	options := liveOptions{Timeout: 2 * time.Minute}
	cmd := &cobra.Command{
		Use:   "live",
		Short: "Send one bounded request through the running gateway agent",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if strings.TrimSpace(options.ConfigPath) == "" {
				options.ConfigPath = defaultConfig()
			}
			if !options.JSON {
				options.Progress = func(message string) {
					fmt.Fprintln(cmd.ErrOrStderr(), message)
				}
			}
			result, err := runLive(cmd.Context(), options)
			if options.JSON {
				if writeErr := writeLiveJSON(cmd.OutOrStdout(), result); writeErr != nil {
					return writeErr
				}
			} else {
				writeLiveText(cmd.OutOrStdout(), result)
			}
			return err
		},
	}
	cmd.Flags().StringVar(&options.ConfigPath, "config", "", "Path to config.json (default: active MintClaw config)")
	cmd.Flags().StringVarP(&options.Message, "message", "m", "", "Single message to send to the live agent")
	cmd.Flags().StringVar(&options.SessionID, "session", "", "MintClaw protocol session ID (default: isolated UUID)")
	cmd.Flags().DurationVar(&options.Timeout, "timeout", options.Timeout, "Overall request timeout")
	cmd.Flags().BoolVar(&options.JSON, "json", false, "Emit stable JSON output")
	_ = cmd.MarkFlagRequired("message")
	return cmd
}

func runLive(parent context.Context, options liveOptions) (result liveResult, err error) {
	started := time.Now()
	result = liveResult{
		Version:   liveProtocolVersion,
		Outcome:   "internal_error",
		ActorID:   "mintclaw-user",
		SessionID: strings.TrimSpace(options.SessionID),
		RequestID: uuid.NewString(),
	}
	if result.SessionID == "" {
		result.SessionID = "live-smoke-" + uuid.NewString()
	}
	defer func() { result.DurationMS = time.Since(started).Milliseconds() }()
	progress := options.Progress
	if progress == nil {
		progress = func(string) {}
	}

	message := strings.TrimSpace(options.Message)
	if message == "" {
		return result, &liveRunError{cause: errors.New("live message is required")}
	}
	if len([]byte(message)) > liveMaxInputBytes {
		return result, &liveRunError{cause: fmt.Errorf("live message exceeds %d bytes", liveMaxInputBytes)}
	}
	if options.Timeout <= 0 {
		return result, &liveRunError{cause: errors.New("live timeout must be positive")}
	}

	cfg, loadErr := config.LoadConfig(strings.TrimSpace(options.ConfigPath))
	if loadErr != nil {
		return result, &liveRunError{cause: fmt.Errorf("load config: %w", loadErr)}
	}
	result.AgentID = configuredDefaultAgentID(cfg)
	settings, credentialsErr := liveChannelSettings(cfg)
	if credentialsErr != nil {
		result.Outcome = "unavailable"
		return result, &liveRunError{cause: credentialsErr}
	}
	endpoint, endpointErr := liveGatewayURL(cfg, result.SessionID)
	if endpointErr != nil {
		result.Outcome = "unavailable"
		return result, &liveRunError{cause: endpointErr}
	}
	progress("Connecting to the running MintClaw gateway...")

	ctx, cancel := context.WithTimeout(parent, options.Timeout)
	defer cancel()
	header := http.Header{"Authorization": []string{"Bearer " + settings.Token.String()}}
	connection, response, dialErr := websocket.DefaultDialer.DialContext(ctx, endpoint.String(), header)
	if response != nil && response.Body != nil {
		_ = response.Body.Close()
	}
	if dialErr != nil {
		result.Outcome = classifyLiveDialError(ctx, response)
		return result, &liveRunError{cause: fmt.Errorf("connect to live gateway: %w", dialErr)}
	}
	defer connection.Close()
	stopCloseOnCancel := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stopCloseOnCancel()
	connection.SetReadLimit(liveMaxOutputBytes + 64*1024)
	if deadline, ok := ctx.Deadline(); ok {
		_ = connection.SetReadDeadline(deadline)
		_ = connection.SetWriteDeadline(deadline)
	}

	request := channelmintclaw.MintClawMessage{
		Type:      channelmintclaw.TypeMessageSend,
		ID:        result.RequestID,
		SessionID: result.SessionID,
		Timestamp: time.Now().UnixMilli(),
		Payload: map[string]any{
			channelmintclaw.PayloadKeyContent: message,
		},
	}
	if writeErr := connection.WriteJSON(request); writeErr != nil {
		result.Outcome = classifyLiveIOError(ctx, writeErr)
		return result, &liveRunError{cause: fmt.Errorf("send live request: %w", writeErr)}
	}
	progress("Request accepted; waiting for one correlated terminal outcome...")

	for {
		var incoming channelmintclaw.MintClawMessage
		if readErr := connection.ReadJSON(&incoming); readErr != nil {
			result.Outcome = classifyLiveIOError(ctx, readErr)
			return result, &liveRunError{cause: fmt.Errorf("read live response: %w", readErr)}
		}
		if incoming.SessionID != "" && incoming.SessionID != result.SessionID {
			continue
		}
		if incoming.Type == channelmintclaw.TypeError {
			if requestID, _ := incoming.Payload["request_id"].(string); requestID != "" &&
				requestID != result.RequestID {
				continue
			}
			result.Outcome = "protocol_error"
			message, _ := incoming.Payload["message"].(string)
			return result, &liveRunError{cause: fmt.Errorf("live gateway rejected request: %s", message)}
		}
		if incoming.Type != channelmintclaw.TypeMessageCreate && incoming.Type != channelmintclaw.TypeMessageUpdate {
			continue
		}
		requestID, _ := incoming.Payload[bus.OutboundMetadataKeyRequestID].(string)
		if strings.TrimSpace(requestID) != result.RequestID {
			continue
		}
		applyLiveIdentity(&result, incoming.Payload)
		if isLiveAuxiliary(incoming.Payload) {
			continue
		}
		content, _ := incoming.Payload[channelmintclaw.PayloadKeyContent].(string)
		if len([]byte(content)) > liveMaxOutputBytes {
			result.Outcome = "output_limit"
			return result, &liveRunError{cause: fmt.Errorf("live response exceeds %d bytes", liveMaxOutputBytes)}
		}
		result.Response = content
		if liveApprovalRequired(incoming.Payload) {
			result.Outcome = "approval_required"
			return result, nil
		}
		if liveFinal(incoming.Payload) {
			result.Outcome = "success"
			return result, nil
		}
	}
}

func liveChannelSettings(cfg *config.Config) (*config.MintClawSettings, error) {
	channel := cfg.Channels.GetByType(config.ChannelMintClaw)
	if channel == nil || !channel.Enabled {
		return nil, errors.New("enabled MintClaw channel is required for live agent smoke")
	}
	decoded, err := channel.GetDecoded()
	if err != nil {
		return nil, fmt.Errorf("decode MintClaw channel: %w", err)
	}
	settings, ok := decoded.(*config.MintClawSettings)
	if !ok || strings.TrimSpace(settings.Token.String()) == "" {
		return nil, errors.New("MintClaw channel token is required for live agent smoke")
	}
	return settings, nil
}

func liveGatewayURL(cfg *config.Config, sessionID string) (*url.URL, error) {
	plan, err := netbind.BuildPlan(cfg.Gateway.Host, netbind.DefaultLoopback)
	if err != nil {
		return nil, fmt.Errorf("resolve gateway host: %w", err)
	}
	if !netbind.IsLoopbackHost(plan.ProbeHost) {
		return nil, errors.New("live agent smoke must run on the gateway host through loopback")
	}
	if cfg.Gateway.Port <= 0 {
		return nil, errors.New("gateway port is required for live agent smoke")
	}
	endpoint := &url.URL{
		Scheme: "ws",
		Host:   net.JoinHostPort(plan.ProbeHost, strconv.Itoa(cfg.Gateway.Port)),
		Path:   "/mintclaw/ws",
	}
	query := endpoint.Query()
	query.Set("session_id", sessionID)
	endpoint.RawQuery = query.Encode()
	return endpoint, nil
}

func configuredDefaultAgentID(cfg *config.Config) string {
	for _, candidate := range cfg.Agents.List {
		if candidate.Default && strings.TrimSpace(candidate.ID) != "" {
			return strings.TrimSpace(candidate.ID)
		}
	}
	if len(cfg.Agents.List) > 0 && strings.TrimSpace(cfg.Agents.List[0].ID) != "" {
		return strings.TrimSpace(cfg.Agents.List[0].ID)
	}
	return routing.DefaultAgentID
}

func applyLiveIdentity(result *liveResult, payload map[string]any) {
	if agentID, _ := payload[channelmintclaw.PayloadKeyAgentID].(string); strings.TrimSpace(agentID) != "" {
		result.AgentID = strings.TrimSpace(agentID)
	}
	if sessionKey, _ := payload[channelmintclaw.PayloadKeySessionKey].(string); strings.TrimSpace(sessionKey) != "" {
		result.SessionKey = strings.TrimSpace(sessionKey)
	}
	if interactionID, _ := payload[channelmintclaw.PayloadKeyInteractionID].(string); interactionID != "" {
		result.InteractionID = strings.TrimSpace(interactionID)
	}
	if shortID, _ := payload[channelmintclaw.PayloadKeyInteractionShortID].(string); shortID != "" {
		result.InteractionShortID = strings.TrimSpace(shortID)
	}
	raw, ok := payload[channelmintclaw.PayloadKeyTraceScopes]
	if !ok {
		return
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return
	}
	var scopes []runtimeevents.TraceScope
	if json.Unmarshal(encoded, &scopes) == nil && len(scopes) > 0 {
		result.TraceScope = scopes[0]
	}
}

func isLiveAuxiliary(payload map[string]any) bool {
	kind, _ := payload[channelmintclaw.PayloadKeyKind].(string)
	placeholder, _ := payload[channelmintclaw.PayloadKeyPlaceholder].(bool)
	return placeholder || kind == channelmintclaw.MessageKindThought || kind == channelmintclaw.MessageKindToolCalls
}

func liveApprovalRequired(payload map[string]any) bool {
	interaction, _ := payload[channelmintclaw.PayloadKeyInteraction].(string)
	controls, _ := payload[channelmintclaw.PayloadKeyControls].(string)
	return interaction == "approval" && controls == "prompt"
}

func liveFinal(payload map[string]any) bool {
	final, _ := payload[channelmintclaw.PayloadKeyFinal].(bool)
	kind, _ := payload[channelmintclaw.PayloadKeyKind].(string)
	outbound, _ := payload[channelmintclaw.PayloadKeyOutbound].(string)
	return final || kind == channelmintclaw.MessageKindFinalReply || outbound == "final"
}

func classifyLiveDialError(ctx context.Context, response *http.Response) string {
	if errors.Is(ctx.Err(), context.Canceled) {
		return "canceled"
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return "timeout"
	}
	if response != nil &&
		(response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden) {
		return "authentication_failed"
	}
	return "unavailable"
}

func classifyLiveIOError(ctx context.Context, err error) string {
	if errors.Is(err, websocket.ErrReadLimit) {
		return "output_limit"
	}
	if errors.Is(ctx.Err(), context.Canceled) || errors.Is(err, context.Canceled) {
		return "canceled"
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "timeout"
	}
	return "disconnected"
}

func writeLiveJSON(writer io.Writer, result liveResult) error {
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	return encoder.Encode(result)
}

func writeLiveText(writer io.Writer, result liveResult) {
	fmt.Fprintf(writer, "Outcome: %s\n", result.Outcome)
	fmt.Fprintf(writer, "Actor: %s\n", result.ActorID)
	fmt.Fprintf(writer, "Agent: %s\n", result.AgentID)
	fmt.Fprintf(writer, "Session: %s\n", result.SessionID)
	fmt.Fprintf(writer, "Request: %s\n", result.RequestID)
	if result.TraceScope.TurnID != "" {
		fmt.Fprintf(writer, "Turn: %s\n", result.TraceScope.TurnID)
	}
	if result.InteractionID != "" {
		fmt.Fprintf(writer, "Interaction: %s (%s)\n", result.InteractionID, result.InteractionShortID)
	}
	if result.Response != "" {
		fmt.Fprintf(writer, "Response: %s\n", result.Response)
	}
}
