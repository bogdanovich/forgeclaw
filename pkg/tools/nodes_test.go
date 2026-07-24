package tools

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/nodes"
	"github.com/sipeed/picoclaw/pkg/tools/loopguard"
)

type fakeNodeDiscoverySource struct {
	byRef         map[string]nodes.Snapshot
	registrations map[nodes.ID]nodes.Registration
	err           error
}

func (source *fakeNodeDiscoverySource) Resolve(ref string) (nodes.Snapshot, bool, error) {
	if source.err != nil {
		return nodes.Snapshot{}, false, source.err
	}
	snapshot, ok := source.byRef[ref]
	return snapshot, ok, nil
}

func (source *fakeNodeDiscoverySource) Registration(
	id nodes.ID,
) (nodes.Registration, bool, error) {
	if source.err != nil {
		return nodes.Registration{}, false, source.err
	}
	registration, ok := source.registrations[id]
	return registration, ok, nil
}

func TestNodeDiscoveryToolListUsesEffectiveAgentPolicy(t *testing.T) {
	cfg := nodeDiscoveryTestConfig()
	source := &fakeNodeDiscoverySource{
		byRef: map[string]nodes.Snapshot{
			"builder-node": {
				ID:              "node-secret-builder",
				State:           nodes.StateConnected,
				DisplayName:     "Builder",
				Platform:        "linux",
				Architecture:    "amd64",
				SoftwareVersion: "1.2.3",
			},
		},
		registrations: map[nodes.ID]nodes.Registration{
			"node-secret-builder": {
				AllowedCommands: []string{"system.info.v1"},
			},
		},
	}
	tool := NewNodeDiscoveryTool(cfg, source)

	mainResult := tool.Execute(
		WithToolSessionContext(context.Background(), "main", "session", nil),
		map[string]any{"action": "list"},
	)
	mainPayload := decodeNodeResult(t, mainResult)
	mainTargets := mainPayload["targets"].([]any)
	if len(mainTargets) != 2 {
		t.Fatalf("main target count = %d, want 2: %s", len(mainTargets), mainResult.ForLLM)
	}
	if got := mainTargets[0].(map[string]any)["target"]; got != "build" {
		t.Fatalf("first main target = %v, want build", got)
	}
	if got := mainTargets[0].(map[string]any)["default"]; got != true {
		t.Fatalf("build default = %v, want true", got)
	}
	if got := mainTargets[0].(map[string]any)["available"]; got != true {
		t.Fatalf("build available = %v, want true", got)
	}
	if strings.Contains(mainResult.ForLLM, "node-secret-builder") ||
		strings.Contains(mainResult.ForLLM, "builder-node") {
		t.Fatalf("list leaked raw node identity or reference: %s", mainResult.ForLLM)
	}

	opsResult := tool.Execute(
		WithToolSessionContext(context.Background(), "OPS", "session", nil),
		map[string]any{"action": "list"},
	)
	opsPayload := decodeNodeResult(t, opsResult)
	opsTargets := opsPayload["targets"].([]any)
	if len(opsTargets) != 1 || opsTargets[0].(map[string]any)["target"] != "vpn" {
		t.Fatalf("ops targets = %#v, want only vpn", opsTargets)
	}
}

func TestNodeDiscoveryToolDescribeRedactsIdentityAndUnapprovedCapabilities(t *testing.T) {
	cfg := nodeDiscoveryTestConfig()
	snapshot := nodes.Snapshot{
		ID:              "private-node-id",
		Aliases:         []nodes.Alias{"private-node-alias"},
		State:           nodes.StateConnected,
		DisplayName:     "Build host",
		ProtocolVersion: 1,
		Platform:        "linux",
		Architecture:    "arm64",
		SoftwareVersion: "2.0.0",
		LastSeenAt:      12345,
		Catalog: nodes.CapabilityCatalog{Commands: []nodes.CommandDescriptor{
			{Name: "system.info.v1", Risk: nodes.RiskRead, SupportsProgress: true},
			{Name: "system.service.restart.v1", Risk: nodes.RiskPrivileged, SupportsCancel: true},
		}},
	}
	source := &fakeNodeDiscoverySource{
		byRef: map[string]nodes.Snapshot{"builder-node": snapshot},
		registrations: map[nodes.ID]nodes.Registration{
			snapshot.ID: {
				PublicKey:       []byte("top-secret-key"),
				AllowedCommands: []string{"system.info.v1"},
			},
		},
	}
	tool := NewNodeDiscoveryTool(cfg, source)
	ctx := WithToolSessionContext(context.Background(), "main", "session", nil)

	result := tool.Execute(ctx, map[string]any{"action": "describe", "target": "build"})
	if result.IsError {
		t.Fatalf("describe failed: %s", result.ForLLM)
	}
	payload := decodeNodeResult(t, result)
	commands := payload["commands"].([]any)
	if len(commands) != 1 || commands[0].(map[string]any)["name"] != "system.info.v1" {
		t.Fatalf("visible commands = %#v, want approved command only", commands)
	}
	for _, secret := range []string{
		"private-node-id",
		"private-node-alias",
		"builder-node",
		"top-secret-key",
		"system.service.restart.v1",
		"input_schema",
		"output_schema",
	} {
		if strings.Contains(result.ForLLM, secret) {
			t.Fatalf("describe leaked %q: %s", secret, result.ForLLM)
		}
	}
}

func TestNodeDiscoveryToolRejectsInvisibleTarget(t *testing.T) {
	tool := NewNodeDiscoveryTool(nodeDiscoveryTestConfig(), &fakeNodeDiscoverySource{})
	ctx := WithToolSessionContext(context.Background(), "ops", "session", nil)
	result := tool.Execute(ctx, map[string]any{"action": "describe", "target": "build"})
	if !result.IsError || !strings.Contains(result.ForLLM, "not visible") {
		t.Fatalf("result = %#v, want invisible-target error", result)
	}
}

func TestNodeDiscoveryToolWithoutPolicyExposesNoTargets(t *testing.T) {
	cfg := &config.Config{
		Execution: config.ExecutionConfig{
			Targets: map[string]config.ExecutionTarget{
				"build": {Type: "node", Node: "builder-node"},
			},
		},
	}
	tool := NewNodeDiscoveryTool(cfg, &fakeNodeDiscoverySource{})
	result := tool.Execute(context.Background(), map[string]any{"action": "list"})
	payload := decodeNodeResult(t, result)
	if got := payload["count"]; got != float64(0) {
		t.Fatalf("count = %v, want 0", got)
	}
}

func TestNodeDiscoveryToolReturnsRegistryErrors(t *testing.T) {
	tool := NewNodeDiscoveryTool(
		nodeDiscoveryTestConfig(),
		&fakeNodeDiscoverySource{err: errors.New("registry unavailable")},
	)
	result := tool.Execute(context.Background(), map[string]any{"action": "list"})
	if !result.IsError || !strings.Contains(result.ForLLM, "node registry lookup failed") ||
		strings.Contains(result.ForLLM, "registry unavailable") {
		t.Fatalf("result = %#v, want registry error", result)
	}
}

func TestNodeDiscoveryToolRuntimeClassification(t *testing.T) {
	tool := NewNodeDiscoveryTool(nil, nil)
	if got := tool.ToolLoopSemantics(); got != loopguard.SemanticsReadOnlyIdempotent {
		t.Fatalf("loop semantics = %q", got)
	}
	if got := tool.ToolSteeringSafety(nil); got != SteeringSafetyReadOnly {
		t.Fatalf("steering safety = %q", got)
	}
}

func nodeDiscoveryTestConfig() *config.Config {
	return &config.Config{
		Execution: config.ExecutionConfig{
			Targets: map[string]config.ExecutionTarget{
				"build": {Type: "node", Node: "builder-node"},
				"cold":  {Type: "node", Node: "offline-node"},
				"vpn":   {Type: "node", Node: "vpn-node"},
			},
		},
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				TargetPolicy: &config.TargetPolicy{
					DefaultTarget:  "build",
					AllowedTargets: []string{"cold", "build"},
				},
			},
			List: []config.AgentConfig{
				{
					ID: "ops",
					TargetPolicy: &config.TargetPolicy{
						AllowedTargets: []string{"vpn"},
					},
				},
			},
		},
	}
}

func decodeNodeResult(t *testing.T, result *ToolResult) map[string]any {
	t.Helper()
	if result.IsError {
		t.Fatalf("tool result error: %s", result.ForLLM)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(result.ForLLM), &payload); err != nil {
		t.Fatalf("decode result %q: %v", result.ForLLM, err)
	}
	return payload
}
