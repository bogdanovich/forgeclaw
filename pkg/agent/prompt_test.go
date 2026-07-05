package agent

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sipeed/picoclaw/pkg/providers"
)

func TestPromptRegistry_RejectsRegisteredSourceWrongPlacement(t *testing.T) {
	registry := NewPromptRegistry()
	if err := registry.RegisterSource(PromptSourceDescriptor{
		ID:      "test:source",
		Owner:   "test",
		Allowed: []PromptPlacement{{Layer: PromptLayerCapability, Slot: PromptSlotTooling}},
	}); err != nil {
		t.Fatalf("RegisterSource() error = %v", err)
	}

	err := registry.ValidatePart(PromptPart{
		ID:      "wrong.placement",
		Layer:   PromptLayerContext,
		Slot:    PromptSlotRuntime,
		Source:  PromptSource{ID: "test:source"},
		Content: "runtime text",
	})
	if err == nil {
		t.Fatal("ValidatePart() error = nil, want placement error")
	}
}

func TestPromptRegistry_AllowsUnregisteredSourceInCompatibilityMode(t *testing.T) {
	registry := NewPromptRegistry()

	err := registry.ValidatePart(PromptPart{
		ID:      "unregistered.part",
		Layer:   PromptLayerCapability,
		Slot:    PromptSlotMCP,
		Source:  PromptSource{ID: "mcp:dynamic-server"},
		Content: "dynamic MCP prompt",
	})
	if err != nil {
		t.Fatalf("ValidatePart() error = %v, want nil for unregistered source", err)
	}
}

func TestRenderPromptPartsLegacy_UsesLayerAndSlotOrder(t *testing.T) {
	parts := []PromptPart{
		{
			ID:      "context.runtime",
			Layer:   PromptLayerContext,
			Slot:    PromptSlotRuntime,
			Source:  PromptSource{ID: PromptSourceRuntime},
			Content: "runtime",
		},
		{
			ID:      "kernel.identity",
			Layer:   PromptLayerKernel,
			Slot:    PromptSlotIdentity,
			Source:  PromptSource{ID: PromptSourceKernel},
			Content: "kernel",
		},
		{
			ID:      "capability.skill",
			Layer:   PromptLayerCapability,
			Slot:    PromptSlotActiveSkill,
			Source:  PromptSource{ID: "skill:test"},
			Content: "skill",
		},
		{
			ID:      "instruction.workspace",
			Layer:   PromptLayerInstruction,
			Slot:    PromptSlotWorkspace,
			Source:  PromptSource{ID: PromptSourceWorkspace},
			Content: "workspace",
		},
	}

	got := renderPromptPartsLegacy(parts)
	want := strings.Join([]string{"kernel", "workspace", "skill", "runtime"}, "\n\n---\n\n")
	if got != want {
		t.Fatalf("renderPromptPartsLegacy() = %q, want %q", got, want)
	}
}

func TestBuildMessagesFromPrompt_IncludesSystemPromptOverlay(t *testing.T) {
	t.Setenv("PICOCLAW_BUILTIN_SKILLS", t.TempDir())
	cb := NewContextBuilder(t.TempDir())

	messages := cb.BuildMessagesFromPrompt(PromptBuildRequest{
		CurrentMessage: "do child task",
		Overlays: promptOverlaysForOptions(processOptions{
			SystemPromptOverride: "Use child-only system instructions.",
		}),
	})

	if len(messages) < 2 {
		t.Fatalf("messages len = %d, want at least 2", len(messages))
	}
	if messages[0].Role != "system" {
		t.Fatalf("messages[0].Role = %q, want system", messages[0].Role)
	}
	if !strings.Contains(messages[0].Content, "Use child-only system instructions.") {
		t.Fatalf("system prompt missing overlay: %q", messages[0].Content)
	}
	if messages[1].Role != "user" || messages[1].Content != "do child task" {
		t.Fatalf("messages[1] = %#v, want user task", messages[1])
	}
}

func TestBuildMessagesFromPrompt_MediaOnlyCurrentTurnGetsStandaloneMarker(t *testing.T) {
	cb := NewContextBuilder(t.TempDir())

	messages := cb.BuildMessagesFromPrompt(PromptBuildRequest{
		CurrentMessage: "[media only]",
		Media:          []string{"media://image-1"},
	})

	if len(messages) == 0 {
		t.Fatal("expected messages")
	}
	last := messages[len(messages)-1]
	if last.Role != "user" {
		t.Fatalf("last role = %q, want user", last.Role)
	}
	if !strings.Contains(last.Content, "[New user message with attached media only]") {
		t.Fatalf("last content = %q, want standalone media marker", last.Content)
	}
	if !strings.Contains(last.Content, "Do not assume it continues the previous request") {
		t.Fatalf("last content = %q, want anti-carryover guidance", last.Content)
	}
	if len(last.Media) != 1 || last.Media[0] != "media://image-1" {
		t.Fatalf("last media = %#v, want media://image-1", last.Media)
	}
}

func TestBuildMessagesFromPrompt_MediaOnlyReplyTurnPreservesReplySemantics(t *testing.T) {
	cb := NewContextBuilder(t.TempDir())

	messages := cb.BuildMessagesFromPrompt(PromptBuildRequest{
		CurrentMessage:   "[media only]",
		Media:            []string{"media://image-1"},
		ReplyToMessageID: "123",
	})

	last := messages[len(messages)-1]
	if !strings.Contains(last.Content, "This message was sent as a reply to an earlier chat message") {
		t.Fatalf("last content = %q, want reply-aware media marker", last.Content)
	}
	if strings.Contains(last.Content, "Do not assume it continues the previous request") {
		t.Fatalf("last content = %q, should not suppress reply continuity", last.Content)
	}
}

func TestBuildMessagesFromPrompt_MediaOnlyRecentUserFollowupUsesAdjacentContext(t *testing.T) {
	cb := NewContextBuilder(t.TempDir())
	ts := time.Now().Add(-time.Minute)

	messages := cb.BuildMessagesFromPrompt(PromptBuildRequest{
		History: []providers.Message{
			{Role: "user", Content: "Here is what I ate", CreatedAt: &ts},
		},
		CurrentMessage: "[media only]",
		Media:          []string{"media://image-1"},
	})

	last := messages[len(messages)-1]
	if !strings.Contains(last.Content, "arrived shortly after the user's previous message") {
		t.Fatalf("last content = %q, want adjacent follow-up marker", last.Content)
	}
}

func TestBuildMessagesFromPrompt_MediaOnlyDoesNotAttachAfterAssistantReply(t *testing.T) {
	cb := NewContextBuilder(t.TempDir())
	userTS := time.Now().Add(-time.Minute)
	assistantTS := time.Now().Add(-30 * time.Second)

	messages := cb.BuildMessagesFromPrompt(PromptBuildRequest{
		History: []providers.Message{
			{Role: "user", Content: "Here is what I ate", CreatedAt: &userTS},
			{Role: "assistant", Content: "Saved.", CreatedAt: &assistantTS},
		},
		CurrentMessage: "[media only]",
		Media:          []string{"media://image-1"},
	})

	last := messages[len(messages)-1]
	if !strings.Contains(last.Content, "Do not assume it continues the previous request") {
		t.Fatalf("last content = %q, want standalone marker after assistant reply", last.Content)
	}
}

func TestSteeringPromptMessage_PreservesCanonicalContent(t *testing.T) {
	msg := steeringPromptMessage(providers.Message{
		Role:    "user",
		Content: "use this photo too",
		Media:   []string{"media://photo"},
	})

	if msg.Role != "user" {
		t.Fatalf("Role = %q, want user", msg.Role)
	}
	if len(msg.Media) != 1 || msg.Media[0] != "media://photo" {
		t.Fatalf("Media = %#v, want original media", msg.Media)
	}
	if msg.Content != "use this photo too" {
		t.Fatalf("Content = %q, want raw user content", msg.Content)
	}
	if msg.PromptLayer != string(PromptLayerTurn) ||
		msg.PromptSlot != string(PromptSlotSteering) ||
		msg.PromptSource != string(PromptSourceSteering) {
		t.Fatalf("prompt metadata = (%q, %q, %q), want turn/steering/source",
			msg.PromptLayer, msg.PromptSlot, msg.PromptSource)
	}
}

func TestProviderPromptMessageForTurn_WrapsSteeringContract(t *testing.T) {
	raw := steeringPromptMessage(providers.Message{
		Role:    "user",
		Content: "use this photo too",
		Media:   []string{"media://photo"},
	})
	msg := providerPromptMessageForTurn(raw)

	if raw.Content != "use this photo too" {
		t.Fatalf("raw Content = %q, want unchanged user content", raw.Content)
	}
	for _, want := range []string{
		"[Mid-turn user message]",
		"additional context or evidence for the current request",
		"Do not discard the original objective",
		"use this photo too",
		"[/Mid-turn user message]",
	} {
		if !strings.Contains(msg.Content, want) {
			t.Fatalf("Content missing %q:\n%s", want, msg.Content)
		}
	}
}

func TestProviderPromptMessageForTurn_ExplainsMediaOnlySteering(t *testing.T) {
	msg := providerPromptMessageForTurn(steeringPromptMessage(providers.Message{
		Role:  "user",
		Media: []string{"media://photo"},
	}))

	if !strings.Contains(msg.Content, "attached media is part of this mid-turn user message") {
		t.Fatalf("Content missing media-only explanation:\n%s", msg.Content)
	}
}

func TestBuildMessagesFromPrompt_IncludesWorkspaceTmpPath(t *testing.T) {
	t.Setenv("PICOCLAW_BUILTIN_SKILLS", t.TempDir())
	workspace := t.TempDir()
	cb := NewContextBuilder(workspace)

	messages := cb.BuildMessagesFromPrompt(PromptBuildRequest{
		CurrentMessage: "hello",
	})
	system := messages[0].Content
	want := filepath.Join(workspace, "tmp")
	if !strings.Contains(system, "- Temporary files: "+want) {
		t.Fatalf("system prompt missing workspace tmp path %q:\n%s", want, system)
	}
}

func TestBuildMessagesFromPrompt_IncludesTaskBoardGuidanceWhenToolsAvailable(t *testing.T) {
	t.Setenv("PICOCLAW_BUILTIN_SKILLS", t.TempDir())
	cb := NewContextBuilder(t.TempDir())

	messages := cb.BuildMessagesFromPrompt(PromptBuildRequest{
		CurrentMessage: "do composite work",
	})
	system := messages[0].Content
	for _, want := range []string{
		"**Task boards**",
		"one stable board_id",
		"task_board",
		"delegate/spawn",
		"stable step_id",
		"task_board results",
		"task_status with board_id",
	} {
		if !strings.Contains(system, want) {
			t.Fatalf("system prompt missing task-board guidance %q:\n%s", want, system)
		}
	}
}

func TestBuildMessagesFromPrompt_OmitsTaskBoardGuidanceWhenToolsUnavailable(t *testing.T) {
	t.Setenv("PICOCLAW_BUILTIN_SKILLS", t.TempDir())
	cb := NewContextBuilder(t.TempDir())

	messages := cb.BuildMessagesFromPrompt(PromptBuildRequest{
		CurrentMessage:      "hello",
		SuppressToolUseRule: true,
	})
	system := messages[0].Content
	if strings.Contains(system, "**Task boards**") {
		t.Fatalf("system prompt should omit task-board guidance when tools are unavailable:\n%s", system)
	}
}

func TestBuildMessagesFromPrompt_AttachesInternalPromptMetadata(t *testing.T) {
	t.Setenv("PICOCLAW_BUILTIN_SKILLS", t.TempDir())
	cb := NewContextBuilder(t.TempDir())

	messages := cb.BuildMessagesFromPrompt(PromptBuildRequest{
		CurrentMessage: "hello",
		Summary:        "prior context",
	})
	if len(messages) != 2 {
		t.Fatalf("messages len = %d, want 2", len(messages))
	}

	system := messages[0]
	if len(system.SystemParts) < 3 {
		t.Fatalf("system parts len = %d, want at least 3", len(system.SystemParts))
	}
	if system.SystemParts[0].PromptLayer != string(PromptLayerKernel) ||
		system.SystemParts[0].PromptSlot != string(PromptSlotIdentity) ||
		system.SystemParts[0].PromptSource != string(PromptSourceKernel) {
		t.Fatalf("static system metadata = %#v, want kernel identity", system.SystemParts[0])
	}

	var hasRuntime, hasSummary bool
	for _, part := range system.SystemParts {
		switch part.PromptSource {
		case string(PromptSourceRuntime):
			hasRuntime = true
			if part.CacheControl != nil {
				t.Fatalf("runtime cache control = %#v, want nil", part.CacheControl)
			}
		case string(PromptSourceSummary):
			hasSummary = true
			if part.CacheControl != nil {
				t.Fatalf("summary cache control = %#v, want nil", part.CacheControl)
			}
		}
	}
	if !hasRuntime {
		t.Fatal("system parts missing runtime prompt metadata")
	}
	if !hasSummary {
		t.Fatal("system parts missing summary prompt metadata")
	}

	user := messages[1]
	if user.PromptLayer != string(PromptLayerTurn) ||
		user.PromptSlot != string(PromptSlotMessage) ||
		user.PromptSource != string(PromptSourceUserMessage) {
		t.Fatalf("user message metadata = %#v, want turn message", user)
	}

	data, err := json.Marshal(messages)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	if strings.Contains(string(data), "PromptSource") ||
		strings.Contains(string(data), "PromptLayer") ||
		strings.Contains(string(data), "PromptSlot") {
		t.Fatalf("internal prompt metadata leaked into JSON: %s", data)
	}
}

func TestContextBuilder_CollectsToolDiscoveryContributor(t *testing.T) {
	t.Setenv("PICOCLAW_BUILTIN_SKILLS", t.TempDir())
	cb := NewContextBuilder(t.TempDir()).WithToolDiscovery(true, false)

	messages := cb.BuildMessagesFromPrompt(PromptBuildRequest{CurrentMessage: "hello"})
	system := messages[0]
	if !strings.Contains(system.Content, "tool_search_tool_bm25") {
		t.Fatalf("system prompt missing tool discovery rule: %q", system.Content)
	}

	var found bool
	for _, part := range system.SystemParts {
		if part.PromptSource == string(PromptSourceToolDiscovery) {
			found = true
			if part.PromptLayer != string(PromptLayerCapability) || part.PromptSlot != string(PromptSlotTooling) {
				t.Fatalf("tool discovery metadata = %#v, want capability/tooling", part)
			}
			if part.CacheControl == nil || part.CacheControl.Type != "ephemeral" {
				t.Fatalf("tool discovery cache control = %#v, want ephemeral", part.CacheControl)
			}
		}
	}
	if !found {
		t.Fatal("system parts missing tool discovery prompt metadata")
	}
}

func TestContextBuilder_SuppressesToolDiscoveryContributorWhenToolsUnavailable(t *testing.T) {
	t.Setenv("PICOCLAW_BUILTIN_SKILLS", t.TempDir())
	cb := NewContextBuilder(t.TempDir()).WithToolDiscovery(true, false)

	messages := cb.BuildMessagesFromPrompt(PromptBuildRequest{
		CurrentMessage:      "hello",
		SuppressToolUseRule: true,
	})
	system := messages[0]
	if strings.Contains(system.Content, "tool_search_tool_bm25") {
		t.Fatalf("system prompt includes tool discovery despite tools being unavailable: %q", system.Content)
	}
	for _, part := range system.SystemParts {
		if part.PromptSource == string(PromptSourceToolDiscovery) {
			t.Fatalf("system parts include tool discovery despite tools being unavailable: %#v", part)
		}
	}
}

func TestContextBuilder_OmitsToolDiscoveryContributorWhenDisabled(t *testing.T) {
	t.Setenv("PICOCLAW_BUILTIN_SKILLS", t.TempDir())
	cb := NewContextBuilder(t.TempDir())
	if err := cb.RegisterPromptContributor(toolDiscoveryPromptContributor{
		useBM25:  false,
		useRegex: false,
	}); err != nil {
		t.Fatalf("RegisterPromptContributor() error = %v", err)
	}

	messages := cb.BuildMessagesFromPrompt(PromptBuildRequest{CurrentMessage: "hello"})
	system := messages[0]
	if strings.Contains(system.Content, "tool_search_tool_bm25") ||
		strings.Contains(system.Content, "tool_search_tool_regex") {
		t.Fatalf("system prompt includes tool discovery despite contributor being disabled: %q", system.Content)
	}
}

func TestContextBuilder_SuppressesToolReferencesWhenToolsUnavailable(t *testing.T) {
	workspace := t.TempDir()
	t.Setenv("PICOCLAW_BUILTIN_SKILLS", t.TempDir())
	writeTurnProfileSkill(
		t,
		workspace,
		"research",
		"---\ndescription: research skill\n---\n# research\n\nResearch carefully.",
	)
	cb := NewContextBuilder(workspace)

	messages := cb.BuildMessagesFromPrompt(PromptBuildRequest{
		CurrentMessage:      "hello",
		SuppressToolUseRule: true,
	})
	system := messages[0]
	if strings.Contains(system.Content, "When using tools") ||
		strings.Contains(system.Content, "read_file tool") ||
		strings.Contains(system.Content, "update "+workspace+"/memory/MEMORY.md") {
		t.Fatalf("system prompt includes tool references despite tools being unavailable: %q", system.Content)
	}
	if !strings.Contains(system.Content, "<name>research</name>") {
		t.Fatalf("system prompt should keep non-tool skill catalog context, got: %q", system.Content)
	}
}

func TestContextBuilder_CustomToolAllowListSuppressesReadFileSkillInstruction(t *testing.T) {
	workspace := t.TempDir()
	t.Setenv("PICOCLAW_BUILTIN_SKILLS", t.TempDir())
	writeTurnProfileSkill(
		t,
		workspace,
		"research",
		"---\ndescription: research skill\n---\n# research\n\nResearch carefully.",
	)
	cb := NewContextBuilder(workspace)

	messages := cb.BuildMessagesFromPrompt(PromptBuildRequest{
		CurrentMessage: "hello",
		AllowedTools:   []string{"web_search"},
	})
	system := messages[0]
	if strings.Contains(system.Content, "read_file tool") {
		t.Fatalf("system prompt includes read_file skill instruction without read_file permission: %q", system.Content)
	}
	if !strings.Contains(system.Content, "<name>research</name>") {
		t.Fatalf("system prompt should keep skill catalog context, got: %q", system.Content)
	}
}

func TestContextBuilder_CollectsMCPServerContributor(t *testing.T) {
	t.Setenv("PICOCLAW_BUILTIN_SKILLS", t.TempDir())
	cb := NewContextBuilder(t.TempDir())
	err := cb.RegisterPromptContributor(mcpServerPromptContributor{
		serverName:   "GitHub Server",
		visibleCount: 0,
		hiddenCount:  3,
	})
	if err != nil {
		t.Fatalf("RegisterPromptContributor() error = %v", err)
	}

	messages := cb.BuildMessagesFromPrompt(PromptBuildRequest{CurrentMessage: "hello"})
	system := messages[0]
	if !strings.Contains(system.Content, "MCP server `GitHub Server` is connected") {
		t.Fatalf("system prompt missing MCP contributor content: %q", system.Content)
	}

	var found bool
	for _, part := range system.SystemParts {
		if part.PromptSource == "mcp:github_server" {
			found = true
			if part.PromptLayer != string(PromptLayerCapability) || part.PromptSlot != string(PromptSlotMCP) {
				t.Fatalf("mcp metadata = %#v, want capability/mcp", part)
			}
			if part.CacheControl == nil || part.CacheControl.Type != "ephemeral" {
				t.Fatalf("mcp cache control = %#v, want ephemeral", part.CacheControl)
			}
		}
	}
	if !found {
		t.Fatal("system parts missing MCP prompt metadata")
	}
}

func TestContextBuilder_SuppressesMCPServerContributorWhenToolsUnavailable(t *testing.T) {
	t.Setenv("PICOCLAW_BUILTIN_SKILLS", t.TempDir())
	cb := NewContextBuilder(t.TempDir())
	err := cb.RegisterPromptContributor(mcpServerPromptContributor{
		serverName:   "GitHub Server",
		visibleCount: 3,
		hiddenCount:  0,
	})
	if err != nil {
		t.Fatalf("RegisterPromptContributor() error = %v", err)
	}

	messages := cb.BuildMessagesFromPrompt(PromptBuildRequest{
		CurrentMessage:      "hello",
		SuppressToolUseRule: true,
	})
	system := messages[0]
	if strings.Contains(system.Content, "MCP server `GitHub Server` is connected") ||
		strings.Contains(system.Content, "available as native tools") {
		t.Fatalf("system prompt includes MCP tooling despite tools being unavailable: %q", system.Content)
	}
	for _, part := range system.SystemParts {
		if part.PromptSource == "mcp:github_server" {
			t.Fatalf("system parts include MCP tooling despite tools being unavailable: %#v", part)
		}
	}
}

func TestContextBuilder_SuppressesAgentDiscoveryContributorWhenToolsUnavailable(t *testing.T) {
	t.Setenv("PICOCLAW_BUILTIN_SKILLS", t.TempDir())
	cb := NewContextBuilder(t.TempDir()).WithAgentDiscovery(
		"main",
		func(agentID string) []AgentDescriptor {
			return []AgentDescriptor{{
				ID:          "helper",
				Name:        "Helper",
				Description: "Helps with tasks",
			}}
		},
	)

	messages := cb.BuildMessagesFromPrompt(PromptBuildRequest{
		CurrentMessage:      "hello",
		SuppressToolUseRule: true,
	})
	system := messages[0]
	if strings.Contains(system.Content, "Agent Discovery") ||
		strings.Contains(system.Content, "calling spawn") {
		t.Fatalf("system prompt includes agent discovery despite tools being unavailable: %q", system.Content)
	}
	for _, part := range system.SystemParts {
		if part.PromptSource == string(PromptSourceAgentDiscovery) {
			t.Fatalf("system parts include agent discovery despite tools being unavailable: %#v", part)
		}
	}
}

func TestContextBuilder_CustomToolAllowListSuppressesUnallowedToolContributors(t *testing.T) {
	t.Setenv("PICOCLAW_BUILTIN_SKILLS", t.TempDir())
	cb := NewContextBuilder(t.TempDir()).
		WithToolDiscovery(true, true).
		WithAgentDiscovery(
			"main",
			func(agentID string) []AgentDescriptor {
				return []AgentDescriptor{{
					ID:          "helper",
					Name:        "Helper",
					Description: "Helps with tasks",
				}}
			},
		)
	err := cb.RegisterPromptContributor(mcpServerPromptContributor{
		serverName:   "GitHub Server",
		visibleCount: 3,
		hiddenCount:  0,
	})
	if err != nil {
		t.Fatalf("RegisterPromptContributor() error = %v", err)
	}

	messages := cb.BuildMessagesFromPrompt(PromptBuildRequest{
		CurrentMessage: "hello",
		AllowedTools:   []string{"echo_text"},
	})
	system := messages[0]
	blockedSnippets := []string{
		"tool_search_tool_bm25",
		"tool_search_tool_regex",
		"MCP server `GitHub Server` is connected",
		"Agent Discovery",
		"calling spawn",
	}
	for _, snippet := range blockedSnippets {
		if strings.Contains(system.Content, snippet) {
			t.Fatalf("system prompt includes unallowed tool contributor %q: %q", snippet, system.Content)
		}
	}
	for _, part := range system.SystemParts {
		switch part.PromptSource {
		case string(PromptSourceToolDiscovery), string(PromptSourceAgentDiscovery), "mcp:github_server":
			t.Fatalf("system parts include unallowed tool contributor: %#v", part)
		}
	}
}

type testPromptContributor struct {
	desc PromptSourceDescriptor
	part PromptPart
}

func (c testPromptContributor) PromptSource() PromptSourceDescriptor {
	return c.desc
}

func (c testPromptContributor) ContributePrompt(_ context.Context, _ PromptBuildRequest) ([]PromptPart, error) {
	return []PromptPart{c.part}, nil
}

func TestContextBuilder_CollectsRegisteredPromptContributors(t *testing.T) {
	t.Setenv("PICOCLAW_BUILTIN_SKILLS", t.TempDir())
	cb := NewContextBuilder(t.TempDir())

	sourceID := PromptSourceID("test:contributor")
	err := cb.RegisterPromptContributor(testPromptContributor{
		desc: PromptSourceDescriptor{
			ID:      sourceID,
			Owner:   "test",
			Allowed: []PromptPlacement{{Layer: PromptLayerCapability, Slot: PromptSlotMCP}},
		},
		part: PromptPart{
			ID:      "capability.mcp.test",
			Layer:   PromptLayerCapability,
			Slot:    PromptSlotMCP,
			Source:  PromptSource{ID: sourceID, Name: "test"},
			Content: "registered contributor prompt",
		},
	})
	if err != nil {
		t.Fatalf("RegisterPromptContributor() error = %v", err)
	}

	messages := cb.BuildMessagesFromPrompt(PromptBuildRequest{CurrentMessage: "hello"})
	if !strings.Contains(messages[0].Content, "registered contributor prompt") {
		t.Fatalf("system prompt missing contributor content: %q", messages[0].Content)
	}
}
