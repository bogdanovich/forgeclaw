package bus

import (
	"strconv"
	"strings"
)

const (
	OutboundMetadataKeyMessageKind  = "message_kind"
	OutboundMetadataKeyToolCalls    = "tool_calls"
	OutboundMetadataKeyOutboundKind = "outbound_kind"
	OutboundMetadataKeyModelName    = "model_name"
	OutboundMetadataKeyDefaultModel = "default_model_name"
	OutboundMetadataKeyUsageInput   = "usage_input_tokens"
	OutboundMetadataKeyUsageOutput  = "usage_output_tokens"
	OutboundMetadataKeyUsageTotal   = "usage_total_tokens"

	OutboundMessageKindThought      = "thought"
	OutboundMessageKindToolFeedback = "tool_feedback"
	OutboundMessageKindToolCalls    = "tool_calls"
	OutboundMessageKindFinalReply   = "final_reply"

	OutboundKindFinal = "final"
)

// OutboundMetadata is the typed form of the cross-package outbound metadata
// stored in InboundContext.Raw for wire/backward compatibility.
type OutboundMetadata struct {
	MessageKind       string
	ToolCalls         string
	OutboundKind      string
	ModelName         string
	DefaultModelName  string
	UsageInputTokens  int
	UsageOutputTokens int
	UsageTotalTokens  int
}

func OutboundMetadataFromMessage(msg OutboundMessage) OutboundMetadata {
	return OutboundMetadataFromRaw(msg.Context.Raw)
}

func OutboundMetadataFromContext(ctx InboundContext) OutboundMetadata {
	return OutboundMetadataFromRaw(ctx.Raw)
}

func OutboundMetadataFromRaw(raw map[string]string) OutboundMetadata {
	if len(raw) == 0 {
		return OutboundMetadata{}
	}
	return OutboundMetadata{
		MessageKind:       strings.TrimSpace(raw[OutboundMetadataKeyMessageKind]),
		ToolCalls:         strings.TrimSpace(raw[OutboundMetadataKeyToolCalls]),
		OutboundKind:      strings.TrimSpace(raw[OutboundMetadataKeyOutboundKind]),
		ModelName:         strings.TrimSpace(raw[OutboundMetadataKeyModelName]),
		DefaultModelName:  strings.TrimSpace(raw[OutboundMetadataKeyDefaultModel]),
		UsageInputTokens:  parseOutboundMetadataInt(raw[OutboundMetadataKeyUsageInput]),
		UsageOutputTokens: parseOutboundMetadataInt(raw[OutboundMetadataKeyUsageOutput]),
		UsageTotalTokens:  parseOutboundMetadataInt(raw[OutboundMetadataKeyUsageTotal]),
	}
}

func (m OutboundMetadata) ApplyToContext(ctx *InboundContext) {
	if ctx == nil {
		return
	}
	rawCount := len(ctx.Raw)
	if strings.TrimSpace(m.MessageKind) != "" {
		rawCount++
	}
	if strings.TrimSpace(m.ToolCalls) != "" {
		rawCount++
	}
	if strings.TrimSpace(m.OutboundKind) != "" {
		rawCount++
	}
	if strings.TrimSpace(m.ModelName) != "" {
		rawCount++
	}
	if strings.TrimSpace(m.DefaultModelName) != "" {
		rawCount++
	}
	if m.UsageInputTokens > 0 {
		rawCount++
	}
	if m.UsageOutputTokens > 0 {
		rawCount++
	}
	if m.UsageTotalTokens > 0 {
		rawCount++
	}
	if rawCount == 0 {
		return
	}
	if ctx.Raw == nil {
		ctx.Raw = make(map[string]string, rawCount)
	}
	setOutboundMetadataString(ctx.Raw, OutboundMetadataKeyMessageKind, m.MessageKind)
	setOutboundMetadataString(ctx.Raw, OutboundMetadataKeyToolCalls, m.ToolCalls)
	setOutboundMetadataString(ctx.Raw, OutboundMetadataKeyOutboundKind, m.OutboundKind)
	setOutboundMetadataString(ctx.Raw, OutboundMetadataKeyModelName, m.ModelName)
	setOutboundMetadataString(ctx.Raw, OutboundMetadataKeyDefaultModel, m.DefaultModelName)
	setOutboundMetadataInt(ctx.Raw, OutboundMetadataKeyUsageInput, m.UsageInputTokens)
	setOutboundMetadataInt(ctx.Raw, OutboundMetadataKeyUsageOutput, m.UsageOutputTokens)
	setOutboundMetadataInt(ctx.Raw, OutboundMetadataKeyUsageTotal, m.UsageTotalTokens)
}

func (m OutboundMetadata) IsToolFeedback() bool {
	return strings.EqualFold(m.MessageKind, OutboundMessageKindToolFeedback)
}

func (m OutboundMetadata) IsThought() bool {
	return strings.EqualFold(m.MessageKind, OutboundMessageKindThought)
}

func (m OutboundMetadata) IsToolCalls() bool {
	return strings.EqualFold(m.MessageKind, OutboundMessageKindToolCalls)
}

func (m OutboundMetadata) IsFinalReply() bool {
	return strings.EqualFold(m.MessageKind, OutboundMessageKindFinalReply)
}

func (m OutboundMetadata) HasAuxiliaryKind() bool {
	return strings.TrimSpace(m.MessageKind) != ""
}

func (m OutboundMetadata) IsFinal() bool {
	return strings.EqualFold(m.OutboundKind, OutboundKindFinal)
}

func (m OutboundMetadata) BypassesPlaceholderEdit() bool {
	return m.IsThought() || m.IsToolCalls() || m.IsFinalReply()
}

func parseOutboundMetadataInt(raw string) int {
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || value < 0 {
		return 0
	}
	return value
}

func setOutboundMetadataString(raw map[string]string, key, value string) {
	value = strings.TrimSpace(value)
	if value == "" {
		return
	}
	raw[key] = value
}

func setOutboundMetadataInt(raw map[string]string, key string, value int) {
	if value <= 0 {
		return
	}
	raw[key] = strconv.Itoa(value)
}
