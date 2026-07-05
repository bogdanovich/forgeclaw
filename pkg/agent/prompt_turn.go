package agent

import (
	"fmt"
	"strings"
	"time"

	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/providers"
)

func promptBuildRequestForTurn(
	ts *turnState,
	history []providers.Message,
	summary string,
	currentMessage string,
	media []string,
	cfg *config.Config,
) PromptBuildRequest {
	req := PromptBuildRequest{
		History:           history,
		Summary:           summary,
		CurrentMessage:    currentMessage,
		Media:             append([]string(nil), media...),
		Channel:           ts.channel,
		ChatID:            ts.chatID,
		SenderID:          ts.opts.Dispatch.SenderID(),
		SenderDisplayName: ts.opts.SenderDisplayName,
		ReplyToMessageID:  ts.opts.Dispatch.ReplyToMessageID(),
		ActiveSkills:      activeSkillNames(ts.agent, ts.opts),
		Overlays:          promptOverlaysForOptions(ts.opts),
	}
	hasCallableTools := true
	if ts.profile.Enabled {
		hasCallableTools = turnProfileHasCallableTools(ts.profile, ts.agent.Tools.ToProviderDefs()) ||
			turnProfileNativeSearchCallable(cfg, ts.profile, ts.agent)
	}
	if turnProfileSystemPromptOff(ts.profile) {
		req.SuppressDefaultSystemPrompt = true
		req.SuppressSkillContext = true
		req.ToolUseFallback = hasCallableTools
	}
	if ts.profile.Enabled && !hasCallableTools {
		req.SuppressToolUseRule = true
	}
	if turnProfileSkillsOff(ts.profile) {
		req.SuppressSkillContext = true
	}
	if turnProfileCustomSkills(ts.profile) {
		req.AllowedSkills = append([]string(nil), ts.profile.AllowedSkills...)
	}
	if ts.profile.Enabled && ts.profile.ToolsMode == config.TurnProfileModeCustom {
		req.AllowedTools = append([]string(nil), ts.profile.AllowedTools...)
	}
	return req
}

func turnProfileNativeSearchCallable(
	cfg *config.Config,
	profile config.EffectiveTurnProfile,
	agent *AgentInstance,
) bool {
	if cfg == nil || agent == nil {
		return false
	}
	if !cfg.Tools.IsToolEnabled("web") || !cfg.Tools.Web.PreferNative {
		return false
	}
	if !turnProfileToolAllowed(profile, "web_search") {
		return false
	}
	nativeProvider, ok := agent.Provider.(providers.NativeSearchCapable)
	return ok && nativeProvider.SupportsNativeSearch()
}

func promptBuildRequestForProcessOptions(
	cfg *config.Config,
	agent *AgentInstance,
	opts processOptions,
	history []providers.Message,
	summary string,
	currentMessage string,
	media []string,
) PromptBuildRequest {
	req := PromptBuildRequest{
		History:           history,
		Summary:           summary,
		CurrentMessage:    currentMessage,
		Media:             append([]string(nil), media...),
		Channel:           opts.Channel,
		ChatID:            opts.ChatID,
		SenderID:          opts.SenderID,
		SenderDisplayName: opts.SenderDisplayName,
		ReplyToMessageID:  opts.ReplyToMessageID,
		ActiveSkills:      activeSkillNames(agent, opts),
		Overlays:          promptOverlaysForOptions(opts),
	}
	profile := opts.TurnProfile
	hasCallableTools := true
	if profile.Enabled && agent != nil {
		var toolDefs []providers.ToolDefinition
		if agent.Tools != nil {
			toolDefs = agent.Tools.ToProviderDefs()
		}
		hasCallableTools = turnProfileHasCallableTools(profile, toolDefs) ||
			turnProfileNativeSearchCallable(cfg, profile, agent)
	}
	if turnProfileSystemPromptOff(profile) {
		req.SuppressDefaultSystemPrompt = true
		req.SuppressSkillContext = true
		req.ToolUseFallback = hasCallableTools
	}
	if profile.Enabled && !hasCallableTools {
		req.SuppressToolUseRule = true
	}
	if turnProfileSkillsOff(profile) {
		req.SuppressSkillContext = true
	}
	if turnProfileCustomSkills(profile) {
		req.AllowedSkills = append([]string(nil), profile.AllowedSkills...)
	}
	if profile.Enabled && profile.ToolsMode == config.TurnProfileModeCustom {
		req.AllowedTools = append([]string(nil), profile.AllowedTools...)
	}
	return req
}

func promptOverlaysForOptions(opts processOptions) []PromptPart {
	systemPrompt := strings.TrimSpace(opts.SystemPromptOverride)
	if systemPrompt == "" {
		return nil
	}

	return []PromptPart{
		{
			ID:      "instruction.subturn_profile",
			Layer:   PromptLayerInstruction,
			Slot:    PromptSlotWorkspace,
			Source:  PromptSource{ID: PromptSourceSubTurnProfile, Name: "subturn.profile"},
			Title:   "SubTurn System Instructions",
			Content: systemPrompt,
			Stable:  false,
			Cache:   PromptCacheNone,
		},
	}
}

func promptContentBlock(part PromptPart, cache *providers.CacheControl) providers.ContentBlock {
	if cache == nil {
		cache = cacheControlForPromptPart(part)
	}
	return providers.ContentBlock{
		Type:         "text",
		Text:         part.Content,
		CacheControl: cache,
		PromptLayer:  string(part.Layer),
		PromptSlot:   string(part.Slot),
		PromptSource: string(part.Source.ID),
	}
}

func cacheControlForPromptPart(part PromptPart) *providers.CacheControl {
	switch part.Cache {
	case PromptCacheEphemeral:
		return &providers.CacheControl{Type: "ephemeral"}
	default:
		return nil
	}
}

func promptMessageWithMetadata(
	msg providers.Message,
	layer PromptLayer,
	slot PromptSlot,
	source PromptSourceID,
) providers.Message {
	msg.PromptLayer = string(layer)
	msg.PromptSlot = string(slot)
	msg.PromptSource = string(source)
	return msg
}

func promptMessageWithDefaultMetadata(
	msg providers.Message,
	layer PromptLayer,
	slot PromptSlot,
	source PromptSourceID,
) providers.Message {
	if strings.TrimSpace(msg.PromptSource) != "" {
		return msg
	}
	return promptMessageWithMetadata(msg, layer, slot, source)
}

func userPromptMessage(content string, media []string) providers.Message {
	msg := providers.Message{
		Role:    "user",
		Content: content,
	}
	if len(media) > 0 {
		msg.Media = append([]string(nil), media...)
	}
	return promptMessageWithMetadata(msg, PromptLayerTurn, PromptSlotMessage, PromptSourceUserMessage)
}

func currentTurnUserPromptMessage(
	content string,
	media []string,
	replyToMessageID string,
	history []providers.Message,
	now time.Time,
) providers.Message {
	relation := classifyCurrentTurnRelation(currentTurnRelationInput{
		Content:          content,
		Media:            media,
		ReplyToMessageID: replyToMessageID,
		History:          history,
		Now:              now,
	})
	content = strings.TrimSpace(content)
	if relation.MediaOnly {
		lines := []string{
			"[New user message with attached media only]",
			"No text or caption was provided with this message.",
			"Treat the attached media as the current turn input.",
		}
		switch relation.Kind {
		case currentTurnRelationReplyToMessage:
			lines = append(
				lines,
				"This message was sent as a reply to an earlier chat message, so use that quoted/reply context when relevant.",
			)
		case currentTurnRelationAdjacentFollowupMedia:
			lines = append(
				lines,
				"This media-only message arrived shortly after the user's previous message and likely adds context to it.",
				"Use the most recent user message as companion context unless the new media clearly starts a different request.",
			)
		default:
			lines = append(
				lines,
				"Do not assume it continues the previous request unless the user explicitly referenced earlier context.",
			)
		}
		content = strings.Join(lines, "\n")
	}
	return userPromptMessage(content, media)
}

func toolImageFollowUpPromptMessage(media []string) providers.Message {
	msg := providers.Message{
		Role:    "user",
		Content: "[Loaded image from tool result above]",
	}
	if len(media) > 0 {
		msg.Media = append([]string(nil), media...)
	}
	return promptMessageWithMetadata(msg, PromptLayerTurn, PromptSlotToolResult, PromptSourceToolResult)
}

func steeringPromptMessage(msg providers.Message) providers.Message {
	return promptMessageWithDefaultMetadata(msg, PromptLayerTurn, PromptSlotSteering, PromptSourceSteering)
}

func providerPromptMessageForTurn(msg providers.Message) providers.Message {
	if msg.PromptSlot != string(PromptSlotSteering) {
		return msg
	}
	msg.Content = formatSteeringPromptContent(msg.Content, len(msg.Media) > 0)
	return msg
}

func formatSteeringPromptContent(content string, hasMedia bool) string {
	content = strings.TrimSpace(content)
	if content == "" && hasMedia {
		content = "(no text; attached media is part of this mid-turn user message)"
	}
	if content == "" {
		content = "(empty message)"
	}
	return strings.Join([]string{
		"[Mid-turn user message]",
		"This message arrived while you were already handling the current user request. It is genuine user input, not tool output.",
		"Treat it as additional context or evidence for the current request unless it clearly cancels, replaces, or redirects that request.",
		"Do not discard the original objective. Your next action or final response should account for the accumulated request across the full turn.",
		"",
		"Message:",
		content,
		"[/Mid-turn user message]",
	}, "\n")
}

func subTurnResultPromptMessage(content string) providers.Message {
	return promptMessageWithMetadata(
		providers.Message{Role: "user", Content: fmt.Sprintf("[SubTurn Result] %s", content)},
		PromptLayerTurn,
		PromptSlotSubTurn,
		PromptSourceSubTurnResult,
	)
}

func interruptPromptMessage(content string) providers.Message {
	return promptMessageWithMetadata(
		providers.Message{Role: "user", Content: content},
		PromptLayerTurn,
		PromptSlotInterrupt,
		PromptSourceInterrupt,
	)
}
