package mintclaw

import (
	"strings"
	"time"
)

// Protocol message types.
const (
	// TypeMessageSend is sent from client to server.
	TypeMessageSend = "message.send"
	TypeMediaSend   = "media.send"
	TypePing        = "ping"

	// TypeMessageCreate is sent from server to client.
	TypeMessageCreate = "message.create"
	TypeMessageUpdate = "message.update"
	TypeMessageDelete = "message.delete"
	TypeMediaCreate   = "media.create"
	TypeTypingStart   = "typing.start"
	TypeTypingStop    = "typing.stop"
	TypeError         = "error"
	TypePong          = "pong"

	PayloadKeyContent            = "content"
	PayloadKeyThought            = "thought"
	PayloadKeyKind               = "kind"
	PayloadKeyPlaceholder        = "placeholder"
	PayloadKeyToolCalls          = "tool_calls"
	PayloadKeyModelName          = "model_name"
	PayloadKeyUsage              = "usage"
	PayloadKeyFinal              = "final"
	PayloadKeyAgentID            = "agent_id"
	PayloadKeySessionKey         = "session_key"
	PayloadKeyTraceScopes        = "trace_scopes"
	PayloadKeyInteraction        = "interaction_kind"
	PayloadKeyControls           = "interaction_controls"
	PayloadKeyOutbound           = "outbound_kind"
	PayloadKeyInteractionID      = "interaction_id"
	PayloadKeyInteractionShortID = "interaction_short_id"

	MessageKindThought    = "thought"
	MessageKindToolCalls  = "tool_calls"
	MessageKindFinalReply = "final_reply"
)

// MintClawMessage is the wire format for all MintClaw Protocol messages.
type MintClawMessage struct {
	Type      string         `json:"type"`
	ID        string         `json:"id,omitempty"`
	SessionID string         `json:"session_id,omitempty"`
	Timestamp int64          `json:"timestamp,omitempty"`
	Payload   map[string]any `json:"payload,omitempty"`
}

// newMessage creates a MintClawMessage with the given type and payload.
func newMessage(msgType string, payload map[string]any) MintClawMessage {
	return MintClawMessage{
		Type:      msgType,
		Timestamp: time.Now().UnixMilli(),
		Payload:   payload,
	}
}

func isThoughtPayload(payload map[string]any) bool {
	kind, _ := payload[PayloadKeyKind].(string)
	if strings.EqualFold(strings.TrimSpace(kind), MessageKindThought) {
		return true
	}

	// Keep mintclaw_client inbound-compatible with legacy servers that still send
	// the pre-kind boolean thought marker.
	thought, _ := payload[PayloadKeyThought].(bool)
	return thought
}

func newErrorWithPayload(code, message string, extra map[string]any) MintClawMessage {
	payload := map[string]any{
		"code":    code,
		"message": message,
	}
	for key, value := range extra {
		payload[key] = value
	}
	return newMessage(TypeError, payload)
}

// newError creates an error MintClawMessage.
func newError(code, message string) MintClawMessage {
	return newErrorWithPayload(code, message, nil)
}
