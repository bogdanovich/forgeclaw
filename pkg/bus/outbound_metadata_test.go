package bus

import "testing"

func TestOutboundMetadataApplyToContextAndReadBack(t *testing.T) {
	ctx := InboundContext{
		Raw: map[string]string{
			"existing": "kept",
		},
	}
	OutboundMetadata{
		MessageKind:       " final_reply ",
		ToolCalls:         " calls ",
		OutboundKind:      " final ",
		ModelName:         " fallback-model ",
		DefaultModelName:  " primary-model ",
		UsageInputTokens:  10252,
		UsageOutputTokens: 4500,
		UsageTotalTokens:  14752,
	}.ApplyToContext(&ctx)

	if got := ctx.Raw["existing"]; got != "kept" {
		t.Fatalf("existing raw value = %q, want kept", got)
	}
	if got := ctx.Raw[OutboundMetadataKeyMessageKind]; got != OutboundMessageKindFinalReply {
		t.Fatalf("message kind raw = %q, want %q", got, OutboundMessageKindFinalReply)
	}

	metadata := OutboundMetadataFromContext(ctx)
	if metadata.MessageKind != OutboundMessageKindFinalReply ||
		metadata.ToolCalls != "calls" ||
		metadata.OutboundKind != OutboundKindFinal ||
		metadata.ModelName != "fallback-model" ||
		metadata.DefaultModelName != "primary-model" ||
		metadata.UsageInputTokens != 10252 ||
		metadata.UsageOutputTokens != 4500 ||
		metadata.UsageTotalTokens != 14752 {
		t.Fatalf("metadata round trip = %#v", metadata)
	}
	if !metadata.IsFinal() {
		t.Fatal("expected metadata to be final")
	}
	if !metadata.BypassesPlaceholderEdit() {
		t.Fatal("expected final_reply to bypass placeholder edit")
	}
}

func TestOutboundMetadataInteractionControls(t *testing.T) {
	var ctx InboundContext
	OutboundMetadata{
		InteractionKind:     OutboundInteractionApproval,
		InteractionControls: OutboundInteractionControlsPrompt,
	}.ApplyToContext(&ctx)

	metadata := OutboundMetadataFromContext(ctx)
	if !metadata.IsApprovalPrompt() || metadata.RemovesInteractionControls() {
		t.Fatalf("approval prompt metadata = %#v", metadata)
	}
	if !metadata.BypassesPlaceholderEdit() {
		t.Fatal("approval prompt must bypass metadata-loss placeholder edits")
	}

	ctx = InboundContext{}
	OutboundMetadata{
		InteractionKind:     OutboundInteractionApproval,
		InteractionControls: OutboundInteractionControlsRemove,
	}.ApplyToContext(&ctx)
	metadata = OutboundMetadataFromContext(ctx)
	if metadata.IsApprovalPrompt() || !metadata.RemovesInteractionControls() {
		t.Fatalf("approval removal metadata = %#v", metadata)
	}
}

func TestOutboundMetadataInterimKind(t *testing.T) {
	var ctx InboundContext
	OutboundMetadata{OutboundKind: " INTERIM "}.ApplyToContext(&ctx)

	metadata := OutboundMetadataFromContext(ctx)
	if !metadata.IsInterim() || metadata.IsFinal() {
		t.Fatalf("interim metadata = %#v", metadata)
	}
}

func TestOutboundMetadataFromRawSanitizesUsage(t *testing.T) {
	metadata := OutboundMetadataFromRaw(map[string]string{
		OutboundMetadataKeyUsageInput:  "-1",
		OutboundMetadataKeyUsageOutput: "not-a-number",
		OutboundMetadataKeyUsageTotal:  "10",
	})

	if metadata.UsageInputTokens != 0 ||
		metadata.UsageOutputTokens != 0 ||
		metadata.UsageTotalTokens != 10 {
		t.Fatalf("usage tokens = %#v", metadata)
	}
}

func TestOutboundMetadataApplyToContextIgnoresZeroValues(t *testing.T) {
	var ctx InboundContext
	OutboundMetadata{}.ApplyToContext(&ctx)
	if ctx.Raw != nil {
		t.Fatalf("empty metadata created raw map: %#v", ctx.Raw)
	}

	OutboundMetadata{
		MessageKind:       OutboundMessageKindToolFeedback,
		UsageInputTokens:  -1,
		UsageOutputTokens: 0,
	}.ApplyToContext(&ctx)
	if got := ctx.Raw[OutboundMetadataKeyMessageKind]; got != OutboundMessageKindToolFeedback {
		t.Fatalf("message kind raw = %q, want %q", got, OutboundMessageKindToolFeedback)
	}
	if _, ok := ctx.Raw[OutboundMetadataKeyUsageInput]; ok {
		t.Fatal("negative usage input should not be serialized")
	}
	if metadata := OutboundMetadataFromContext(ctx); !metadata.IsToolFeedback() {
		t.Fatalf("expected tool feedback metadata, got %#v", metadata)
	}
}
