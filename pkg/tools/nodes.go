package tools

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/bogdanovich/mintclaw/pkg/config"
	"github.com/bogdanovich/mintclaw/pkg/nodes"
	"github.com/bogdanovich/mintclaw/pkg/routing"
	"github.com/bogdanovich/mintclaw/pkg/tools/loopguard"
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
	source                NodeDiscoverySource
	targets               map[string]config.ExecutionTarget
	defaultPolicy         *config.TargetPolicy
	agentPolicies         map[string]*config.TargetPolicy
	approvalBypassTargets map[string]struct{}
}

type nodeListEntry struct {
	Target             string      `json:"target"`
	Default            bool        `json:"default,omitempty"`
	State              nodes.State `json:"state,omitempty"`
	Availability       string      `json:"availability"`
	Available          bool        `json:"available"`
	RequiresReapproval bool        `json:"requires_reapproval,omitempty"`
	CommandCount       int         `json:"command_count,omitempty"`
	liveConnected      bool
}

type nodeCommandSummary struct {
	Name             string     `json:"name"`
	Risk             nodes.Risk `json:"risk"`
	Availability     string     `json:"availability"`
	SupportsProgress bool       `json:"supports_progress,omitempty"`
	SupportsCancel   bool       `json:"supports_cancel,omitempty"`
	Approval         string     `json:"approval"`
}

type nodeDescription struct {
	nodeListEntry
	Commands []nodeCommandSummary `json:"commands"`
}

type nodeCommandResult struct {
	Kind            string `json:"kind"`
	SchemaAvailable bool   `json:"schema_available"`
}

type nodeCommandExecution struct {
	TimeoutSecondsMax int    `json:"timeout_seconds_max"`
	OutputBytesMax    int    `json:"output_bytes_max"`
	SupportsProgress  bool   `json:"supports_progress"`
	SupportsCancel    bool   `json:"supports_cancel"`
	Approval          string `json:"approval"`
}

type nodeCommandContract struct {
	Name         string                        `json:"name"`
	Risk         nodes.Risk                    `json:"risk"`
	Availability string                        `json:"availability"`
	InputSchema  json.RawMessage               `json:"input_schema"`
	Result       nodeCommandResult             `json:"result"`
	Execution    nodeCommandExecution          `json:"execution"`
	Constraints  nodes.CommandModelConstraints `json:"constraints"`
	Guidance     []string                      `json:"guidance"`
	Examples     []json.RawMessage             `json:"examples"`
	File         *nodeFileCommandContract      `json:"file,omitempty"`
	Service      *nodeServiceCommandContract   `json:"service,omitempty"`
}

type nodeFileCommandContract struct {
	ReadableRoots  []string                  `json:"readable_roots,omitempty"`
	WritableRoots  []string                  `json:"writable_roots,omitempty"`
	AllowCreate    bool                      `json:"allow_create,omitempty"`
	AllowOverwrite bool                      `json:"allow_overwrite,omitempty"`
	MaxFileBytes   int64                     `json:"max_file_bytes"`
	Digest         string                    `json:"digest"`
	Approval       nodes.FileProfileApproval `json:"approval"`
}

type nodeServiceCommandContract struct {
	Manager        string                    `json:"manager"`
	Services       []nodes.ServiceDescriptor `json:"services"`
	LogLimits      nodes.ServiceLogLimits    `json:"log_limits"`
	ActionApproval string                    `json:"action_approval"`
}

type nodeCommandDescription struct {
	nodeListEntry
	Command           nodeCommandContract `json:"command"`
	DiscoveryRevision string              `json:"discovery_revision"`
}

func NewNodeDiscoveryTool(cfg *config.Config, source NodeDiscoverySource) *NodeDiscoveryTool {
	return &NodeDiscoveryTool{access: newNodeTargetAccess(cfg, source)}
}

func newNodeTargetAccess(cfg *config.Config, source NodeDiscoverySource) *nodeTargetAccess {
	access := &nodeTargetAccess{
		source:                source,
		targets:               make(map[string]config.ExecutionTarget),
		agentPolicies:         make(map[string]*config.TargetPolicy),
		approvalBypassTargets: make(map[string]struct{}),
	}
	if cfg == nil {
		return access
	}
	for name, target := range cfg.Execution.Targets {
		access.targets[name] = target
	}
	for _, target := range cfg.Tools.Approval.BypassNodeTargets {
		access.approvalBypassTargets[target] = struct{}{}
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
			"command": map[string]any{
				"type": "string",
				"description": "Approved command name. When set, describe returns one bounded model contract " +
					"and its freshness revision.",
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
		command, _ := args["command"].(string)
		return tool.describe(ctx, strings.TrimSpace(target), strings.TrimSpace(command))
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

func (tool *NodeDiscoveryTool) describe(
	ctx context.Context,
	target string,
	command string,
) *ToolResult {
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
		if command != "" {
			return ErrorResult("command is unavailable on this target")
		}
		return nodeJSONResult(description)
	}
	binding := tool.access.targets[target]
	description.Commands = visibleNodeCommands(
		snapshot.Catalog,
		registration,
		entry.Availability,
		binding.FileProfile,
		binding.ServiceProfile,
		tool.access.bypassesApproval(target),
	)
	if command == "" {
		return nodeJSONResult(description)
	}
	descriptor, ok := visibleNodeCommand(snapshot.Catalog, registration, command)
	if !ok || entry.RequiresReapproval {
		return ErrorResult("command is unavailable on this target")
	}
	descriptor, ok = projectDescriptorForTarget(
		descriptor,
		binding.FileProfile,
		binding.ServiceProfile,
	)
	if !ok {
		return ErrorResult("command is unavailable on this target")
	}
	contractDescriptor := projectServiceApprovalForTarget(
		descriptor,
		tool.access.bypassesApproval(target),
	)
	if !commandProjectionFits(contractDescriptor) {
		return ErrorResult("command discovery is incomplete because its safe projection exceeds limits")
	}
	contract := projectedNodeCommandContract(contractDescriptor, entry.Availability)
	revision, revisionErr := tool.access.discoveryRevision(
		ToolAgentID(ctx),
		target,
		command,
		*snapshot,
		*registration,
		descriptor,
		entry.liveConnected,
	)
	if revisionErr != nil {
		return ErrorResult("command discovery is temporarily unavailable")
	}
	return nodeJSONResult(nodeCommandDescription{
		nodeListEntry:     entry,
		Command:           contract,
		DiscoveryRevision: revision,
	})
}

func (access *nodeTargetAccess) listEntry(target, defaultTarget string) (nodeListEntry, error) {
	entry, _, _, err := access.resolve(target, defaultTarget)
	return entry, err
}

func (access *nodeTargetAccess) resolve(
	target string,
	defaultTarget string,
) (nodeListEntry, *nodes.Snapshot, *nodes.Registration, error) {
	entry := nodeListEntry{
		Target:       target,
		Default:      target == defaultTarget,
		Availability: string(nodes.ModelUnavailable),
	}
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
	connected := snapshot.State == nodes.StateConnected && record.Connected
	entry.liveConnected = connected
	if registration != nil {
		currentCatalogHash := catalogHash(snapshot.Catalog)
		if registration.RevokedAt == 0 &&
			snapshot.State != nodes.StateRevoked &&
			registration.ApprovedAt > 0 &&
			(registration.ApprovedCatalogHash == "" ||
				currentCatalogHash == "" ||
				registration.ApprovedCatalogHash != currentCatalogHash) {
			entry.RequiresReapproval = true
			entry.Availability = "requires_reapproval"
			return entry, &snapshot, registration, nil
		}
		targetAvailability := string(nodes.ModelUnavailable)
		if connected {
			targetAvailability = string(nodes.ModelAvailable)
		}
		commands := visibleNodeCommands(
			snapshot.Catalog,
			registration,
			targetAvailability,
			binding.FileProfile,
			binding.ServiceProfile,
			access.bypassesApproval(target),
		)
		entry.CommandCount = len(commands)
		if connected {
			entry.Availability = aggregateTargetAvailability(commands)
			entry.Available = entry.Availability == string(nodes.ModelAvailable)
		}
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
	targetAvailability string,
	fileProfile string,
	serviceProfile string,
	approvalBypass bool,
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
		projected, available := projectDescriptorForTarget(descriptor, fileProfile, serviceProfile)
		if !available {
			continue
		}
		projected = projectServiceApprovalForTarget(projected, approvalBypass)
		commands = append(commands, nodeCommandSummary{
			Name:             projected.Name,
			Risk:             projected.Risk,
			Availability:     commandAvailability(projected, targetAvailability),
			SupportsProgress: projected.SupportsProgress,
			SupportsCancel:   projected.SupportsCancel,
			Approval:         descriptorApprovalMode(projected),
		})
	}
	sort.Slice(commands, func(i, j int) bool { return commands[i].Name < commands[j].Name })
	return commands
}

func projectServiceApprovalForTarget(
	descriptor nodes.CommandDescriptor,
	approvalBypass bool,
) nodes.CommandDescriptor {
	if !approvalBypass || descriptor.Name != "service.action.v1" ||
		len(descriptor.ServiceProfiles) != 1 || descriptor.ModelContract == nil {
		return descriptor
	}
	descriptor.ServiceProfiles = nodes.CloneServiceProfileDescriptors(descriptor.ServiceProfiles)
	descriptor.ServiceProfiles[0].ActionApproval = "operator_bypass_configured"
	contract := *descriptor.ModelContract
	contract.ApprovalMode = ""
	descriptor.ModelContract = &contract
	return descriptor
}

func projectDescriptorForTarget(
	descriptor nodes.CommandDescriptor,
	fileProfile string,
	serviceProfile string,
) (nodes.CommandDescriptor, bool) {
	projected, available := projectFileDescriptorForTarget(descriptor, fileProfile)
	if !available {
		return nodes.CommandDescriptor{}, false
	}
	return projectServiceDescriptorForTarget(projected, serviceProfile)
}

func projectFileDescriptorForTarget(
	descriptor nodes.CommandDescriptor,
	fileProfile string,
) (nodes.CommandDescriptor, bool) {
	if len(descriptor.FileProfiles) == 0 {
		return descriptor, true
	}
	if fileProfile == "" {
		return nodes.CommandDescriptor{}, false
	}
	for _, profile := range descriptor.FileProfiles {
		if profile.Alias != fileProfile {
			continue
		}
		descriptor.FileProfiles = []nodes.FileProfileDescriptor{profile}
		if descriptor.ModelContract == nil {
			return nodes.CommandDescriptor{}, false
		}
		contract := *descriptor.ModelContract
		contract.Availability = nodes.ModelAvailable
		switch descriptor.Name {
		case "file.info.v1":
			if profile.Approval.Metadata == "required" {
				contract.ApprovalMode = "each_command"
			}
		case "file.download.v1":
			if profile.Approval.Read == "required" {
				contract.ApprovalMode = "each_command"
			}
		case "file.upload.v1":
			if profile.Approval.Write == "required" {
				contract.ApprovalMode = "each_command"
			}
		default:
			return nodes.CommandDescriptor{}, false
		}
		descriptor.ModelContract = &contract
		return descriptor, true
	}
	return nodes.CommandDescriptor{}, false
}

func projectServiceDescriptorForTarget(
	descriptor nodes.CommandDescriptor,
	serviceProfile string,
) (nodes.CommandDescriptor, bool) {
	return nodes.ProjectServiceDescriptorForProfile(descriptor, serviceProfile)
}

func visibleNodeCommand(
	catalog nodes.CapabilityCatalog,
	registration *nodes.Registration,
	name string,
) (nodes.CommandDescriptor, bool) {
	if registration == nil {
		return nodes.CommandDescriptor{}, false
	}
	for _, descriptor := range catalog.Commands {
		if descriptor.Name != name {
			continue
		}
		for _, allowed := range registration.AllowedCommands {
			if allowed == name &&
				registration.ApprovedAt > 0 &&
				registration.ApprovedCatalogHash != "" &&
				registration.ApprovedCatalogHash == catalogHash(catalog) {
				return descriptor, true
			}
		}
	}
	return nodes.CommandDescriptor{}, false
}

func commandAvailability(descriptor nodes.CommandDescriptor, targetAvailability string) string {
	if targetAvailability == string(nodes.ModelUnavailable) {
		return string(nodes.ModelUnavailable)
	}
	if descriptor.ModelContract == nil {
		return string(nodes.ModelPartiallyDescribed)
	}
	if !commandProjectionFits(descriptor) {
		return string(nodes.ModelPartiallyDescribed)
	}
	return string(descriptor.ModelContract.Availability)
}

func aggregateTargetAvailability(commands []nodeCommandSummary) string {
	availability := string(nodes.ModelUnavailable)
	for _, command := range commands {
		if command.Availability == string(nodes.ModelAvailable) {
			return command.Availability
		}
		if command.Availability == string(nodes.ModelPartiallyDescribed) {
			availability = command.Availability
		}
	}
	return availability
}

func projectedNodeCommandContract(
	descriptor nodes.CommandDescriptor,
	targetAvailability string,
) nodeCommandContract {
	model := descriptorModelContract(descriptor)
	availability := commandAvailability(descriptor, targetAvailability)
	return makeNodeCommandContract(descriptor, model, availability)
}

func descriptorModelContract(descriptor nodes.CommandDescriptor) nodes.CommandModelContract {
	model := nodes.CommandModelContract{
		Availability:      nodes.ModelPartiallyDescribed,
		TimeoutSecondsMax: nodes.MaxInvocationTimeout,
		OutputBytesMax:    nodes.MaxInvocationOutput,
		ResultKind:        "json",
		Guidance:          []string{},
		Examples:          []json.RawMessage{},
	}
	if descriptor.ModelContract != nil {
		model = *descriptor.ModelContract
	}
	return model
}

func makeNodeCommandContract(
	descriptor nodes.CommandDescriptor,
	model nodes.CommandModelContract,
	availability string,
) nodeCommandContract {
	inputSchema := append(json.RawMessage(nil), descriptor.InputSchema...)
	if descriptor.Name == "system.exec.v1" {
		if projected, err := nodes.SystemExecModelInputSchema(model); err == nil {
			inputSchema = projected
		} else {
			inputSchema = json.RawMessage("false")
		}
	} else if descriptor.Name == "shell.exec.v1" {
		if projected, err := nodes.ShellExecModelInputSchema(model); err == nil {
			inputSchema = projected
		} else {
			inputSchema = json.RawMessage("false")
		}
	} else if len(descriptor.FileProfiles) == 1 {
		inputSchema = projectedFileToolInputSchema(descriptor.Name)
	} else if len(descriptor.ServiceProfiles) == 1 {
		inputSchema = nodes.ServiceCommandInputSchema(descriptor.Name, descriptor.ServiceProfiles)
	}
	contract := nodeCommandContract{
		Name:         descriptor.Name,
		Risk:         descriptor.Risk,
		Availability: availability,
		InputSchema:  inputSchema,
		Result: nodeCommandResult{
			Kind:            model.ResultKind,
			SchemaAvailable: len(descriptor.OutputSchema) > 0,
		},
		Execution: nodeCommandExecution{
			TimeoutSecondsMax: model.TimeoutSecondsMax,
			OutputBytesMax:    model.OutputBytesMax,
			SupportsProgress:  descriptor.SupportsProgress,
			SupportsCancel:    descriptor.SupportsCancel,
			Approval:          projectedApprovalMode(model),
		},
		Constraints: model.Constraints,
		Guidance:    append([]string(nil), model.Guidance...),
		Examples:    append([]json.RawMessage(nil), model.Examples...),
	}
	if len(descriptor.FileProfiles) == 1 {
		profile := descriptor.FileProfiles[0]
		contract.Constraints.ProfileAliases = nil
		contract.File = &nodeFileCommandContract{
			ReadableRoots:  append([]string(nil), profile.ReadableRoots...),
			WritableRoots:  append([]string(nil), profile.WritableRoots...),
			AllowCreate:    profile.AllowCreate,
			AllowOverwrite: profile.AllowOverwrite,
			MaxFileBytes:   profile.MaxFileBytes,
			Digest:         "sha256",
			Approval:       profile.Approval,
		}
	}
	if len(descriptor.ServiceProfiles) == 1 {
		profile := descriptor.ServiceProfiles[0]
		contract.Constraints.ProfileAliases = nil
		contract.Service = &nodeServiceCommandContract{
			Manager:        profile.Manager,
			Services:       cloneServiceDescriptions(profile.Services),
			LogLimits:      profile.LogLimits,
			ActionApproval: profile.ActionApproval,
		}
		if descriptor.Name == "service.action.v1" &&
			profile.ActionApproval == "operator_bypass_configured" {
			contract.Execution.Approval = profile.ActionApproval
		}
	}
	return contract
}

func cloneServiceDescriptions(services []nodes.ServiceDescriptor) []nodes.ServiceDescriptor {
	result := make([]nodes.ServiceDescriptor, len(services))
	for index, service := range services {
		result[index] = service
		result[index].Actions = append([]nodes.ServiceAction(nil), service.Actions...)
	}
	return result
}

func projectedFileToolInputSchema(command string) json.RawMessage {
	switch command {
	case "file.info.v1":
		return json.RawMessage(
			`{"additionalProperties":false,"properties":{` +
				`"target":{"maxLength":64,"minLength":1,"type":"string"},` +
				`"path":{"maxLength":4096,"minLength":1,"type":"string"},` +
				`"discovery_revision":{"maxLength":128,"minLength":1,"type":"string"}},` +
				`"required":["target","path","discovery_revision"],"type":"object"}`,
		)
	case "file.upload.v1":
		return json.RawMessage(
			`{"additionalProperties":false,"properties":{` +
				`"target":{"maxLength":64,"minLength":1,"type":"string"},` +
				`"artifact_ref":{"maxLength":256,"minLength":1,"type":"string"},` +
				`"destination":{"maxLength":4096,"minLength":1,"type":"string"},` +
				`"publication":{"enum":["create","replace"],"type":"string"},` +
				`"discovery_revision":{"maxLength":128,"minLength":1,"type":"string"}},` +
				`"required":["target","artifact_ref","destination","publication","discovery_revision"],` +
				`"type":"object"}`,
		)
	case "file.download.v1":
		return json.RawMessage(
			`{"additionalProperties":false,"properties":{` +
				`"target":{"maxLength":64,"minLength":1,"type":"string"},` +
				`"source":{"maxLength":4096,"minLength":1,"type":"string"},` +
				`"deliver":{"type":"boolean"},` +
				`"discovery_revision":{"maxLength":128,"minLength":1,"type":"string"}},` +
				`"required":["target","source","deliver","discovery_revision"],"type":"object"}`,
		)
	default:
		return json.RawMessage("false")
	}
}

func descriptorApprovalMode(descriptor nodes.CommandDescriptor) string {
	if descriptor.Name == "service.action.v1" && len(descriptor.ServiceProfiles) == 1 &&
		descriptor.ServiceProfiles[0].ActionApproval == "operator_bypass_configured" {
		return descriptor.ServiceProfiles[0].ActionApproval
	}
	if descriptor.ModelContract != nil && descriptor.ModelContract.ApprovalMode != "" {
		return descriptor.ModelContract.ApprovalMode
	}
	return "may_be_required"
}

func projectedApprovalMode(model nodes.CommandModelContract) string {
	if model.ApprovalMode != "" {
		return model.ApprovalMode
	}
	return "may_be_required"
}

func commandProjectionFits(descriptor nodes.CommandDescriptor) bool {
	model := descriptorModelContract(descriptor)
	contract := makeNodeCommandContract(descriptor, model, string(model.Availability))
	data, err := json.Marshal(contract)
	return err == nil && len(data) <= nodes.MaxModelContractBytes
}

type discoveryRevisionInput struct {
	AgentTargets         []string    `json:"agent_targets"`
	DefaultTarget        string      `json:"default_target"`
	Target               string      `json:"target"`
	TargetType           string      `json:"target_type"`
	TargetExecutor       string      `json:"target_executor"`
	TargetFileProfile    string      `json:"target_file_profile,omitempty"`
	TargetServiceProfile string      `json:"target_service_profile,omitempty"`
	TargetApprovalBypass bool        `json:"target_approval_bypass,omitempty"`
	TargetBindingDigest  string      `json:"target_binding_digest"`
	NodeIdentityDigest   string      `json:"node_identity_digest"`
	Command              string      `json:"command"`
	DescriptorDigest     string      `json:"descriptor_digest"`
	State                nodes.State `json:"state"`
	Connected            bool        `json:"connected"`
	CatalogDigest        string      `json:"catalog_digest"`
	PolicyRevision       string      `json:"policy_revision"`
	NodeExecutor         string      `json:"node_executor"`
	ApprovedCatalog      string      `json:"approved_catalog"`
	ApprovedCommands     []string    `json:"approved_commands"`
	ApprovedAt           int64       `json:"approved_at"`
	RevokedAt            int64       `json:"revoked_at"`
}

func (access *nodeTargetAccess) discoveryRevision(
	agentID string,
	target string,
	command string,
	snapshot nodes.Snapshot,
	registration nodes.Registration,
	descriptor nodes.CommandDescriptor,
	connected bool,
) (string, error) {
	targets, defaultTarget := access.visibleTargets(agentID)
	binding, ok := access.targets[target]
	if !ok {
		return "", errors.New("target binding is unavailable")
	}
	descriptorDigest, err := descriptor.Hash()
	if err != nil {
		return "", err
	}
	bindingDigest := sha256.Sum256([]byte(binding.Node))
	nodeIdentityDigest := sha256.Sum256([]byte(snapshot.ID))
	approvedCommands := append([]string(nil), registration.AllowedCommands...)
	sort.Strings(approvedCommands)
	input := discoveryRevisionInput{
		AgentTargets:         targets,
		DefaultTarget:        defaultTarget,
		Target:               target,
		TargetType:           binding.Type,
		TargetExecutor:       binding.Executor,
		TargetFileProfile:    binding.FileProfile,
		TargetServiceProfile: binding.ServiceProfile,
		TargetApprovalBypass: access.bypassesApproval(target),
		TargetBindingDigest:  base64.RawURLEncoding.EncodeToString(bindingDigest[:]),
		NodeIdentityDigest:   base64.RawURLEncoding.EncodeToString(nodeIdentityDigest[:]),
		Command:              command,
		DescriptorDigest:     descriptorDigest,
		State:                snapshot.State,
		Connected:            connected,
		CatalogDigest:        snapshot.CatalogHash,
		PolicyRevision:       snapshot.PolicyRevision,
		NodeExecutor:         snapshot.Executor,
		ApprovedCatalog:      registration.ApprovedCatalogHash,
		ApprovedCommands:     approvedCommands,
		ApprovedAt:           registration.ApprovedAt,
		RevokedAt:            registration.RevokedAt,
	}
	data, err := json.Marshal(input)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(data)
	return "dr_v1_" + base64.RawURLEncoding.EncodeToString(digest[:]), nil
}

func (access *nodeTargetAccess) bypassesApproval(target string) bool {
	_, bypass := access.approvalBypassTargets[target]
	return bypass
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
