package evalscenario

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/evaltrace"
	"github.com/sipeed/picoclaw/pkg/testharness/llmscenario"
)

func TestScenarioConfigDisablesEveryProductionToolFamily(t *testing.T) {
	cfg := scenarioConfig(t.TempDir(), Scenario{
		Model: "fixture-model", ContextWindow: 65_536, MaxTokens: 2048, MaxToolTurns: 4,
	})
	if cfg.Agents.Defaults.ContextWindow != 65_536 ||
		cfg.Agents.Defaults.MaxTokens != 2048 ||
		cfg.Agents.Defaults.MaxToolIterations != 4 {
		t.Fatalf("scenario limits were not preserved: %#v", cfg.Agents.Defaults)
	}
	toolNames := []string{
		"web", "cron", "exec", "skills", "media_cleanup", "append_file", "apply_patch", "find_skills",
		"i2c", "image_generate", "install_skill", "list_dir", "load_image", "message", "read_file",
		"serial", "search_files", "spawn", "spawn_status", "spi", "subagent", "update_plan", "web_fetch",
		"write_file", "send_file", "send_tts", "mcp",
	}
	for _, name := range toolNames {
		if cfg.Tools.IsToolEnabled(name) {
			t.Fatalf("production tool family %q is enabled", name)
		}
	}
	if len(cfg.Tools.MCP.Servers) != 0 {
		t.Fatalf("MCP servers = %#v", cfg.Tools.MCP.Servers)
	}
}

func TestRunUsesRealAgentPathAndProducesReplayableTrace(t *testing.T) {
	scenario := Scenario{
		ID: "real-agent-tool", Source: "pkg/evalscenario/scenario_test.go",
		Prompt: "look up the fixture value",
		ProviderSteps: []ProviderStep{
			{
				Content: "checking",
				ToolCalls: []ToolCall{
					{
						ID:        "call-1",
						Name:      "fixture_lookup",
						Arguments: map[string]any{"key": "answer"},
					},
				},
			},
			{Content: "The fixture answer is 42."},
		},
		Tools: []StubTool{{Name: "fixture_lookup", Result: "42"}},
	}

	observation, err := Run(context.Background(), scenario)
	if err != nil {
		t.Fatal(err)
	}
	if observation.Response != "The fixture answer is 42." || observation.ProviderCalls != 2 {
		t.Fatalf("observation = %#v", observation)
	}
	if observation.ToolCalls["fixture_lookup"] != 1 {
		t.Fatalf("tool calls = %#v", observation.ToolCalls)
	}
	invocations := observation.ToolInvocations["fixture_lookup"]
	if len(invocations) != 1 || invocations[0].Arguments["key"] != "answer" {
		t.Fatalf("tool invocations = %#v", invocations)
	}
	if err := evaltrace.Validate(observation.Trace); err != nil {
		t.Fatalf("trace validation: %v", err)
	}
	if !hasRecord(observation.Trace, evaltrace.RecordDeliveryOutcome) ||
		!hasRecord(observation.Trace, evaltrace.RecordToolResult) {
		t.Fatalf("trace records = %#v", observation.Trace.Records)
	}
	if len(observation.Replay.Projection.Diagnostics) != 0 {
		t.Fatalf("replay diagnostics = %#v", observation.Replay.Projection.Diagnostics)
	}
}

func TestRunWithProviderInjectsIsolatedInstructionsAndCandidateSkill(t *testing.T) {
	globalHome := t.TempDir()
	builtinRoot := t.TempDir()
	t.Setenv(config.EnvHome, globalHome)
	t.Setenv(config.EnvBuiltinSkills, builtinRoot)
	writeInstalledSkill(t, filepath.Join(globalHome, "skills"), "candidate-rule", "GLOBAL COLLISION MUST NOT LEAK")
	writeInstalledSkill(t, filepath.Join(globalHome, "skills"), "unrelated-global", "UNRELATED GLOBAL MUST NOT LEAK")
	writeInstalledSkill(t, builtinRoot, "unrelated-builtin", "UNRELATED BUILTIN MUST NOT LEAK")

	provider := llmscenario.NewScriptedProvider(
		"live-trial-model",
		llmscenario.ProviderStep{
			Name:     "inspect baseline and candidate context",
			Response: llmscenario.TextResponse("candidate behavior selected"),
			Assert: func(call llmscenario.ProviderCall) error {
				if len(call.Tools) != 1 ||
					call.Tools[0].Function.Name != "held_out_lookup" ||
					call.Tools[0].Function.Description != "look up isolated held-out state" {
					return &missingPromptTextError{Text: "realistic held-out tool contract"}
				}
				var prompt strings.Builder
				for _, message := range call.Messages {
					prompt.WriteString(message.Content)
					for _, part := range message.SystemParts {
						prompt.WriteString(part.Text)
					}
				}
				for _, required := range []string{
					"baseline behavior contract",
					"candidate behavior contract",
					"# Active Skills",
				} {
					if !strings.Contains(prompt.String(), required) {
						return &missingPromptTextError{Text: required}
					}
				}
				for _, forbidden := range []string{
					"GLOBAL COLLISION MUST NOT LEAK",
					"UNRELATED GLOBAL MUST NOT LEAK",
					"UNRELATED BUILTIN MUST NOT LEAK",
				} {
					if strings.Contains(prompt.String(), forbidden) {
						return &unexpectedPromptTextError{Text: forbidden}
					}
				}
				return nil
			},
		},
	)
	scenario := Scenario{
		ID:           "live-provider-context",
		Source:       "pkg/evalscenario/scenario_test.go",
		Prompt:       "choose the correct behavior",
		Model:        "live-trial-model",
		Instructions: "baseline behavior contract",
		Skills: []Skill{{
			Name: "candidate-rule",
			Content: "---\nname: candidate-rule\ndescription: held-out candidate\n---\n" +
				"candidate behavior contract\n",
		}},
		ActiveSkills: []string{"candidate-rule"},
		Tools: []StubTool{{
			Name:        "held_out_lookup",
			Description: "look up isolated held-out state",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"id": map[string]any{"type": "integer"},
				},
			},
			Result: `{"id":42}`,
		}},
	}

	observation, err := RunWithProvider(context.Background(), scenario, provider)
	if err != nil {
		t.Fatal(err)
	}
	if observation.Response != "candidate behavior selected" || observation.ProviderCalls != 1 {
		t.Fatalf("observation = %#v", observation)
	}
	if err := provider.AssertExhausted(); err != nil {
		t.Fatal(err)
	}
}

type missingPromptTextError struct{ Text string }

func (e *missingPromptTextError) Error() string { return "provider prompt is missing " + e.Text }

type unexpectedPromptTextError struct{ Text string }

func (e *unexpectedPromptTextError) Error() string { return "provider prompt contains " + e.Text }

func writeInstalledSkill(t *testing.T, root, name, marker string) {
	t.Helper()
	dir := filepath.Join(root, name)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	content := "---\nname: " + name + "\ndescription: " + marker + "\n---\n" + marker + "\n"
	if err := os.WriteFile(filepath.Join(dir, "SKILL.md"), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestRunIsCanonicalAcrossRepeatedFixtures(t *testing.T) {
	scenario := Scenario{
		ID: "repeatable-answer", Source: "pkg/evalscenario/scenario_test.go",
		Prompt:        "answer deterministically",
		ProviderSteps: []ProviderStep{{Content: "stable answer"}},
	}
	first, err := Run(context.Background(), scenario)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Run(context.Background(), scenario)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first.Replay.Canonical, second.Replay.Canonical) {
		t.Fatalf(
			"canonical replay changed:\n%s\n%s",
			first.Replay.Canonical,
			second.Replay.Canonical,
		)
	}
}

func TestRunDeniesUnregisteredToolWithoutExecutingIt(t *testing.T) {
	scenario := Scenario{
		ID: "unknown-tool-denied", Source: "pkg/evalscenario/scenario_test.go",
		Prompt: "try an unavailable tool",
		ProviderSteps: []ProviderStep{
			{
				Content: "trying",
				ToolCalls: []ToolCall{
					{
						ID:        "call-shell",
						Name:      "shell",
						Arguments: map[string]any{"command": "touch forbidden"},
					},
				},
			},
			{Content: "The requested tool is unavailable."},
		},
	}
	observation, err := Run(context.Background(), scenario)
	if err != nil {
		t.Fatal(err)
	}
	if observation.Response != "The requested tool is unavailable." ||
		len(observation.ToolCalls) != 0 {
		t.Fatalf("observation = %#v", observation)
	}
	if !hasErrorToolResult(observation.Trace) {
		t.Fatalf(
			"unknown tool was not recorded as a denied error result: %#v",
			observation.Trace.Records,
		)
	}
}

func hasRecord(trace evaltrace.Trace, kind evaltrace.RecordKind) bool {
	for _, record := range trace.Records {
		if record.Kind == kind {
			return true
		}
	}
	return false
}

func hasErrorToolResult(trace evaltrace.Trace) bool {
	for _, record := range trace.Records {
		if record.Kind != evaltrace.RecordToolResult {
			continue
		}
		var payload evaltrace.ToolPayload
		if err := json.Unmarshal(record.Data, &payload); err == nil && payload.IsError {
			return true
		}
	}
	return false
}
