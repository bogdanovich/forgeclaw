package evalscenario

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/sipeed/picoclaw/pkg/evaltrace"
)

func TestScenarioConfigDisablesEveryProductionToolFamily(t *testing.T) {
	cfg := scenarioConfig(t.TempDir(), Scenario{Model: "fixture-model"})
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
