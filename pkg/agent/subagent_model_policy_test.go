package agent

import (
	"testing"

	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/state"
)

func TestMergeSubagentsConfig_AgentOverridesDefaults(t *testing.T) {
	defaults := &config.SubagentsConfig{
		AllowAgents:              []string{"browser"},
		Model:                    &config.AgentModelConfig{Primary: "deepseek", Fallbacks: []string{"glm"}},
		SessionModelOverrideMode: subagentSessionModelOverrideFallbackOnly,
	}
	override := &config.SubagentsConfig{
		AllowAgents:              []string{"media"},
		Model:                    &config.AgentModelConfig{Primary: "gemini-flash-lite", Fallbacks: []string{"kimi"}},
		SessionModelOverrideMode: subagentSessionModelOverrideIgnore,
	}
	got := mergeSubagentsConfig(defaults, override)
	if got == nil {
		t.Fatal("mergeSubagentsConfig() = nil")
	}
	if len(got.AllowAgents) != 1 || got.AllowAgents[0] != "media" {
		t.Fatalf("AllowAgents = %#v, want media override", got.AllowAgents)
	}
	if got.Model == nil || got.Model.Primary != "gemini-flash-lite" {
		t.Fatalf("Model = %#v, want override primary", got.Model)
	}
	if got.SessionModelOverrideMode != subagentSessionModelOverrideIgnore {
		t.Fatalf("SessionModelOverrideMode = %q, want ignore", got.SessionModelOverrideMode)
	}
}

func TestResolveSubagentModelPlan_Ignore(t *testing.T) {
	target := &AgentInstance{
		Model:     "gpt-5.4",
		Fallbacks: []string{"deepseek", "glm"},
		Subagents: &config.SubagentsConfig{
			SessionModelOverrideMode: subagentSessionModelOverrideIgnore,
		},
	}
	got := resolveSubagentModelPlan(target, "gemini-flash-lite")
	if got.Primary != "gpt-5.4" {
		t.Fatalf("Primary = %q, want gpt-5.4", got.Primary)
	}
	if len(got.Fallbacks) != 2 || got.Fallbacks[0] != "deepseek" || got.Fallbacks[1] != "glm" {
		t.Fatalf("Fallbacks = %#v, want unchanged", got.Fallbacks)
	}
}

func TestResolveSubagentModelPlan_Inherit(t *testing.T) {
	target := &AgentInstance{
		Model:     "gpt-5.4",
		Fallbacks: []string{"deepseek", "glm"},
		Subagents: &config.SubagentsConfig{
			SessionModelOverrideMode: subagentSessionModelOverrideInherit,
		},
	}
	got := resolveSubagentModelPlan(target, "gemini-flash-lite")
	if got.Primary != "gemini-flash-lite" {
		t.Fatalf("Primary = %q, want gemini-flash-lite", got.Primary)
	}
	if len(got.Fallbacks) != 2 || got.Fallbacks[0] != "deepseek" || got.Fallbacks[1] != "glm" {
		t.Fatalf("Fallbacks = %#v, want base fallbacks", got.Fallbacks)
	}
}

func TestResolveSubagentModelPlan_FallbackOnly(t *testing.T) {
	target := &AgentInstance{
		Model:     "gpt-5.4",
		Fallbacks: []string{"deepseek", "glm"},
		Subagents: &config.SubagentsConfig{
			SessionModelOverrideMode: subagentSessionModelOverrideFallbackOnly,
		},
	}
	got := resolveSubagentModelPlan(target, "gemini-flash-lite")
	if got.Primary != "gpt-5.4" {
		t.Fatalf("Primary = %q, want gpt-5.4", got.Primary)
	}
	if len(got.Fallbacks) != 3 || got.Fallbacks[0] != "gemini-flash-lite" {
		t.Fatalf("Fallbacks = %#v, want override prepended", got.Fallbacks)
	}
}

func TestResolveSubagentModelPlan_UsesConfiguredSubagentModel(t *testing.T) {
	target := &AgentInstance{
		Model:     "gpt-5.4",
		Fallbacks: []string{"deepseek", "glm"},
		Subagents: &config.SubagentsConfig{
			Model: &config.AgentModelConfig{
				Primary:   "kimi",
				Fallbacks: []string{"glm"},
			},
			SessionModelOverrideMode: subagentSessionModelOverrideIgnore,
		},
	}
	got := resolveSubagentModelPlan(target, "gemini-flash-lite")
	if got.Primary != "kimi" {
		t.Fatalf("Primary = %q, want kimi", got.Primary)
	}
	if len(got.Fallbacks) != 1 || got.Fallbacks[0] != "glm" {
		t.Fatalf("Fallbacks = %#v, want subagents.model fallbacks", got.Fallbacks)
	}
}

func TestInheritedSubagentOverride_ReadsParentBinding(t *testing.T) {
	parent := &turnState{
		model: effectiveModelBinding{
			Override: state.SessionModelOverride{Model: "gemini-flash-lite"},
		},
	}
	if got := inheritedSubagentOverride(parent); got != "gemini-flash-lite" {
		t.Fatalf("inheritedSubagentOverride() = %q, want gemini-flash-lite", got)
	}
}

func TestBuildSubagentChildBinding_ReusesTargetRuntimeWhenPlanMatches(t *testing.T) {
	var al *AgentLoop
	target := &AgentInstance{
		ID:        "main",
		Model:     "test-model",
		Fallbacks: []string{"deepseek"},
	}
	parent := &turnState{
		opts: processOptions{
			Dispatch: DispatchRequest{
				RouteSessionKey: "route-parent",
			},
		},
		model: effectiveModelBinding{
			Override: state.SessionModelOverride{Model: "gemini-flash-lite"},
		},
	}

	got, err := al.buildSubagentChildBinding(parent, target)
	if err != nil {
		t.Fatalf("buildSubagentChildBinding() error = %v", err)
	}
	if got.WorkspaceAgent != target {
		t.Fatalf("WorkspaceAgent = %#v, want original target agent", got.WorkspaceAgent)
	}
	if got.Execution.Model != "" {
		t.Fatalf("Execution.Model = %q, want reused runtime binding", got.Execution.Model)
	}
	if got.RouteSessionKey != "route-parent" {
		t.Fatalf("RouteSessionKey = %q, want route-parent", got.RouteSessionKey)
	}
	if got.Override.Model != "gemini-flash-lite" {
		t.Fatalf("Override.Model = %q, want gemini-flash-lite", got.Override.Model)
	}
}
