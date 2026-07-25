package tools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/nodes"
	"github.com/sipeed/picoclaw/pkg/routing"
	"github.com/sipeed/picoclaw/pkg/tools/loopguard"
)

type NodeDiscoverySource interface {
	Lookup(string) (NodeDiscoveryRecord, bool, error)
}

type NodeDiscoveryRecord struct {
	Snapshot     nodes.Snapshot
	Registration *nodes.Registration
	Connected    bool
}

type NodeDiscoveryTool struct {
	access *nodeTargetAccess
}

type nodeTargetAccess struct {
	source        NodeDiscoverySource
	targets       map[string]config.ExecutionTarget
	defaultPolicy *config.TargetPolicy
	agentPolicies map[string]*config.TargetPolicy
}

type nodeListEntry struct {
	Target             string      `json:"target"`
	Default            bool        `json:"default,omitempty"`
	State              nodes.State `json:"state,omitempty"`
	Available          bool        `json:"available"`
	DisplayName        string      `json:"display_name,omitempty"`
	RequiresReapproval bool        `json:"requires_reapproval,omitempty"`
	CommandCount       int         `json:"command_count,omitempty"`
}

type nodeCommandSummary struct {
	Name             string     `json:"name"`
	Risk             nodes.Risk `json:"risk"`
	SupportsProgress bool       `json:"supports_progress,omitempty"`
	SupportsCancel   bool       `json:"supports_cancel,omitempty"`
}

type nodeDescription struct {
	nodeListEntry
	ProtocolVersion int                  `json:"protocol_version,omitempty"`
	LastSeenAt      int64                `json:"last_seen_at,omitempty"`
	Commands        []nodeCommandSummary `json:"commands"`
}

func NewNodeDiscoveryTool(cfg *config.Config, source NodeDiscoverySource) *NodeDiscoveryTool {
	return &NodeDiscoveryTool{access: newNodeTargetAccess(cfg, source)}
}

func newNodeTargetAccess(cfg *config.Config, source NodeDiscoverySource) *nodeTargetAccess {
	access := &nodeTargetAccess{
		source:        source,
		targets:       make(map[string]config.ExecutionTarget),
		agentPolicies: make(map[string]*config.TargetPolicy),
	}
	if cfg == nil {
		return access
	}
	for name, target := range cfg.Execution.Targets {
		access.targets[name] = target
	}
	access.defaultPolicy = cloneTargetPolicy(cfg.Agents.Defaults.TargetPolicy)
	for i := range cfg.Agents.List {
		agentCfg := &cfg.Agents.List[i]
		if agentCfg.TargetPolicy != nil {
			access.agentPolicies[routing.NormalizeAgentID(agentCfg.ID)] = cloneTargetPolicy(agentCfg.TargetPolicy)
		}
	}
	return access
}

func (*NodeDiscoveryTool) Name() string { return "nodes" }

func (*NodeDiscoveryTool) Description() string {
	return "List execution targets visible to this agent or describe one visible target. " +
		"Only operator-configured target names are accepted; connection details and raw node IDs are never exposed."
}

func (*NodeDiscoveryTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"action": map[string]any{
				"type":        "string",
				"enum":        []string{"list", "describe"},
				"description": "Read-only discovery action.",
			},
			"target": map[string]any{
				"type":        "string",
				"description": "Operator-configured target name. Required for describe.",
			},
		},
		"required":             []string{"action"},
		"additionalProperties": false,
	}
}

func (tool *NodeDiscoveryTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	action, _ := args["action"].(string)
	switch strings.ToLower(strings.TrimSpace(action)) {
	case "list":
		return tool.list(ctx)
	case "describe":
		target, _ := args["target"].(string)
		return tool.describe(ctx, strings.TrimSpace(target))
	default:
		return ErrorResult("action must be list or describe")
	}
}

func (*NodeDiscoveryTool) ToolLoopSemantics() loopguard.Semantics {
	return loopguard.SemanticsReadOnlyIdempotent
}

func (*NodeDiscoveryTool) ToolSteeringSafety(map[string]any) SteeringSafety {
	return SteeringSafetyReadOnly
}

func (tool *NodeDiscoveryTool) list(ctx context.Context) *ToolResult {
	names, defaultTarget := tool.access.visibleTargets(ToolAgentID(ctx))
	entries := make([]nodeListEntry, 0, len(names))
	for _, name := range names {
		entry, err := tool.access.listEntry(name, defaultTarget)
		if err != nil {
			return ErrorResult(fmt.Sprintf("list node target %q: %v", name, err))
		}
		entries = append(entries, entry)
	}
	return nodeJSONResult(map[string]any{
		"targets": entries,
		"count":   len(entries),
	})
}

func (tool *NodeDiscoveryTool) describe(ctx context.Context, target string) *ToolResult {
	if target == "" {
		return ErrorResult("target is required for describe")
	}
	names, defaultTarget := tool.access.visibleTargets(ToolAgentID(ctx))
	if !containsSorted(names, target) {
		return ErrorResult(fmt.Sprintf("target %q is not visible to this agent", target))
	}
	entry, snapshot, registration, err := tool.access.resolve(target, defaultTarget)
	if err != nil {
		return ErrorResult(fmt.Sprintf("describe node target %q: %v", target, err))
	}
	description := nodeDescription{
		nodeListEntry: entry,
		Commands:      make([]nodeCommandSummary, 0),
	}
	if snapshot == nil {
		return nodeJSONResult(description)
	}
	description.ProtocolVersion = snapshot.ProtocolVersion
	description.LastSeenAt = snapshot.LastSeenAt
	description.Commands = visibleNodeCommands(snapshot.Catalog, registration)
	return nodeJSONResult(description)
}

func (access *nodeTargetAccess) listEntry(target, defaultTarget string) (nodeListEntry, error) {
	entry, _, _, err := access.resolve(target, defaultTarget)
	return entry, err
}

func (access *nodeTargetAccess) resolve(
	target string,
	defaultTarget string,
) (nodeListEntry, *nodes.Snapshot, *nodes.Registration, error) {
	entry := nodeListEntry{Target: target, Default: target == defaultTarget}
	binding, exists := access.targets[target]
	if !exists || access.source == nil {
		return entry, nil, nil, nil
	}
	record, found, err := access.source.Lookup(binding.Node)
	if err != nil {
		return entry, nil, nil, errors.New("node registry lookup failed")
	}
	if !found {
		return entry, nil, nil, nil
	}
	snapshot := record.Snapshot
	registration := record.Registration
	entry.State = snapshot.State
	entry.Available = snapshot.State == nodes.StateConnected && record.Connected
	entry.DisplayName = snapshot.DisplayName
	if registration != nil {
		currentCatalogHash := catalogHash(snapshot.Catalog)
		if registration.RevokedAt == 0 &&
			snapshot.State != nodes.StateRevoked &&
			registration.ApprovedAt > 0 &&
			(registration.ApprovedCatalogHash == "" ||
				currentCatalogHash == "" ||
				registration.ApprovedCatalogHash != currentCatalogHash) {
			entry.RequiresReapproval = true
			return entry, &snapshot, registration, nil
		}
		entry.CommandCount = len(visibleNodeCommands(snapshot.Catalog, registration))
		return entry, &snapshot, registration, nil
	}
	return entry, &snapshot, nil, nil
}

func (access *nodeTargetAccess) visibleTargets(agentID string) ([]string, string) {
	policy := access.defaultPolicy
	if agentPolicy, exists := access.agentPolicies[routing.NormalizeAgentID(agentID)]; exists {
		policy = agentPolicy
	}
	if policy == nil {
		return []string{}, ""
	}
	names := append([]string(nil), policy.AllowedTargets...)
	sort.Strings(names)
	return names, policy.DefaultTarget
}

func visibleNodeCommands(
	catalog nodes.CapabilityCatalog,
	registration *nodes.Registration,
) []nodeCommandSummary {
	if registration == nil || len(registration.AllowedCommands) == 0 {
		return []nodeCommandSummary{}
	}
	if registration.ApprovedAt <= 0 ||
		registration.ApprovedCatalogHash == "" ||
		registration.ApprovedCatalogHash != catalogHash(catalog) {
		return []nodeCommandSummary{}
	}
	allowed := make(map[string]struct{}, len(registration.AllowedCommands))
	for _, name := range registration.AllowedCommands {
		allowed[name] = struct{}{}
	}
	commands := make([]nodeCommandSummary, 0, len(allowed))
	for _, descriptor := range catalog.Commands {
		if _, ok := allowed[descriptor.Name]; !ok {
			continue
		}
		commands = append(commands, nodeCommandSummary{
			Name:             descriptor.Name,
			Risk:             descriptor.Risk,
			SupportsProgress: descriptor.SupportsProgress,
			SupportsCancel:   descriptor.SupportsCancel,
		})
	}
	sort.Slice(commands, func(i, j int) bool { return commands[i].Name < commands[j].Name })
	return commands
}

func catalogHash(catalog nodes.CapabilityCatalog) string {
	hash, err := catalog.Hash()
	if err != nil {
		return ""
	}
	return hash
}

func cloneTargetPolicy(policy *config.TargetPolicy) *config.TargetPolicy {
	if policy == nil {
		return nil
	}
	return &config.TargetPolicy{
		DefaultTarget:  policy.DefaultTarget,
		AllowedTargets: append([]string(nil), policy.AllowedTargets...),
	}
}

func containsSorted(values []string, value string) bool {
	index := sort.SearchStrings(values, value)
	return index < len(values) && values[index] == value
}

func nodeJSONResult(value any) *ToolResult {
	data, err := json.Marshal(value)
	if err != nil {
		return ErrorResult(fmt.Sprintf("encode node discovery result: %v", err))
	}
	return NewToolResult(string(data))
}
