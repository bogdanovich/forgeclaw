package tools

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/nodes"
	"github.com/sipeed/picoclaw/pkg/tools/loopguard"
)

const (
	defaultNodeInvocationTimeout = 30
	defaultNodeInvocationOutput  = 64 * 1024
)

type NodeInvocationSource interface {
	NodeDiscoverySource
	PrepareInvocation(
		target string,
		toolCallID string,
		plan nodes.ExecutionPlan,
		descriptor nodes.CommandDescriptor,
	) (nodes.GatewayInvocationRecord, error)
	LookupInvocationByToolCall(
		principal nodes.GatewayInvocationPrincipal,
		toolCallID string,
	) (nodes.GatewayInvocationRecord, bool, error)
	LookupInvocation(
		principal nodes.GatewayInvocationPrincipal,
		invocationID string,
	) (nodes.GatewayInvocationRecord, bool, error)
	DispatchInvocation(
		ctx context.Context,
		owner nodes.GatewayInvocationOwner,
		invocationID string,
		expectedPlanHash string,
	) (result json.RawMessage, dispatched bool, err error)
	QueryInvocation(
		ctx context.Context,
		principal nodes.GatewayInvocationPrincipal,
		target string,
		nodeID nodes.ID,
		invocationID string,
	) (nodes.InvocationRecord, error)
}

type NodeInvokeTool struct {
	runtime *nodeInvocationToolRuntime
}

type NodeStatusTool struct {
	runtime *nodeInvocationToolRuntime
}

type nodeInvocationToolRuntime struct {
	access *nodeTargetAccess
	source NodeInvocationSource
}

type resolvedNodeTarget struct {
	name         string
	binding      config.ExecutionTarget
	snapshot     nodes.Snapshot
	registration *nodes.Registration
	available    bool
}

type nodeInvokeResult struct {
	InvocationID   string                       `json:"invocation_id"`
	Target         string                       `json:"target"`
	Command        string                       `json:"command"`
	Risk           nodes.Risk                   `json:"risk"`
	GatewayState   nodes.GatewayInvocationState `json:"gateway_state"`
	State          string                       `json:"state"`
	Result         json.RawMessage              `json:"result,omitempty"`
	ErrorCode      string                       `json:"error_code,omitempty"`
	RecoveryAction string                       `json:"recovery_action,omitempty"`
}

type nodeStatusResult struct {
	InvocationID   string                        `json:"invocation_id"`
	Target         string                        `json:"target"`
	Command        string                        `json:"command"`
	Risk           nodes.Risk                    `json:"risk"`
	GatewayState   nodes.GatewayInvocationState  `json:"gateway_state"`
	State          string                        `json:"state"`
	NodeAvailable  bool                          `json:"node_available"`
	AcceptedAt     int64                         `json:"accepted_at,omitempty"`
	UpdatedAt      int64                         `json:"updated_at,omitempty"`
	CompletedAt    int64                         `json:"completed_at,omitempty"`
	Result         json.RawMessage               `json:"result,omitempty"`
	Failure        *nodes.InvocationFailure      `json:"failure,omitempty"`
	Cancellation   *nodes.InvocationCancellation `json:"cancellation,omitempty"`
	ErrorCode      string                        `json:"error_code,omitempty"`
	RecoveryAction string                        `json:"recovery_action,omitempty"`
}

func NewNodeInvokeTool(cfg *config.Config, source NodeInvocationSource) *NodeInvokeTool {
	return &NodeInvokeTool{runtime: newNodeInvocationToolRuntime(cfg, source)}
}

func NewNodeStatusTool(cfg *config.Config, source NodeInvocationSource) *NodeStatusTool {
	return &NodeStatusTool{runtime: newNodeInvocationToolRuntime(cfg, source)}
}

func newNodeInvocationToolRuntime(
	cfg *config.Config,
	source NodeInvocationSource,
) *nodeInvocationToolRuntime {
	return &nodeInvocationToolRuntime{
		access: newNodeTargetAccess(cfg, source),
		source: source,
	}
}

func (*NodeInvokeTool) Name() string { return "nodes_invoke" }

func (*NodeInvokeTool) Description() string {
	return "Invoke one approved typed command on an operator-configured node target. " +
		"Use nodes describe first to discover visible commands. Never invent target or command names."
}

func (*NodeInvokeTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"target": map[string]any{
				"type":        "string",
				"description": "Visible target name. Omit only when the agent has a default target.",
			},
			"command": map[string]any{
				"type":        "string",
				"description": "Approved versioned command name from nodes describe.",
			},
			"input": map[string]any{
				"type":        "object",
				"description": "Typed command input matching the advertised command schema.",
			},
			"timeout_seconds": map[string]any{
				"type":        "integer",
				"description": "Optional execution timeout from 1 to 3600 seconds.",
			},
			"output_limit_bytes": map[string]any{
				"type":        "integer",
				"description": "Optional bounded result size from 1 to 524288 bytes.",
			},
		},
		"required":             []string{"command", "input"},
		"additionalProperties": false,
	}
}

func (tool *NodeInvokeTool) ApprovalArguments(
	ctx context.Context,
	args map[string]any,
) (map[string]any, error) {
	record, err := tool.runtime.prepare(ctx, args)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"target":          record.Target,
		"invocation_id":   record.Plan.InvocationID,
		"node_id":         string(record.Plan.NodeID),
		"command":         record.Plan.Command,
		"risk":            record.Plan.Risk,
		"plan_hash":       record.ExpectedPlanHash,
		"policy_revision": record.Plan.PolicyRevision,
		"expires_at":      record.Plan.ExpiresAt,
	}, nil
}

func (tool *NodeInvokeTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	record, err := tool.runtime.prepare(ctx, args)
	if err != nil {
		return nodeInvocationError("PREPARE_DENIED", err.Error(), nil)
	}
	owner := nodes.GatewayInvocationOwner{
		Target:     record.Target,
		AgentID:    record.Plan.AgentID,
		SessionID:  record.Plan.SessionID,
		ActorID:    record.Plan.ActorID,
		ToolCallID: record.ToolCallID,
	}
	result, dispatched, err := tool.runtime.source.DispatchInvocation(
		ctx,
		owner,
		record.Plan.InvocationID,
		record.ExpectedPlanHash,
	)
	if err != nil {
		if errors.Is(err, nodes.ErrGatewayInvocationDispatched) || dispatched {
			view := nodeInvokeResult{
				InvocationID:   record.Plan.InvocationID,
				Target:         record.Target,
				Command:        record.Plan.Command,
				Risk:           record.Plan.Risk,
				GatewayState:   nodes.GatewayInvocationDispatched,
				State:          string(nodes.InvocationUnknown),
				ErrorCode:      "DISPATCH_UNCERTAIN",
				RecoveryAction: "Call nodes_status with this invocation_id; do not replay the command.",
			}
			return nodeInvocationError(
				"DISPATCH_UNCERTAIN",
				"the invocation outcome is uncertain",
				&view,
			)
		}
		view := nodeInvokeResult{
			InvocationID: record.Plan.InvocationID,
			Target:       record.Target,
			Command:      record.Plan.Command,
			Risk:         record.Plan.Risk,
			GatewayState: nodes.GatewayInvocationPrepared,
			State:        "not_dispatched",
			ErrorCode:    "DISPATCH_DENIED",
		}
		return nodeInvocationError(
			"DISPATCH_DENIED",
			"the gateway rejected dispatch before contacting the node",
			&view,
		)
	}
	return nodeJSONResult(nodeInvokeResult{
		InvocationID: record.Plan.InvocationID,
		Target:       record.Target,
		Command:      record.Plan.Command,
		Risk:         record.Plan.Risk,
		GatewayState: nodes.GatewayInvocationDispatched,
		State:        string(nodes.InvocationSucceeded),
		Result:       result,
	})
}

func (*NodeInvokeTool) ToolLoopSemantics() loopguard.Semantics {
	return loopguard.SemanticsMutating
}

func (*NodeInvokeTool) ToolSteeringSafety(map[string]any) SteeringSafety {
	return SteeringSafetyCancellable
}

func (*NodeStatusTool) Name() string { return "nodes_status" }

func (*NodeStatusTool) Description() string {
	return "Inspect one invocation owned by this agent, conversation, and actor. " +
		"This recovery query never retries or replays the command."
}

func (*NodeStatusTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"invocation_id": map[string]any{
				"type":        "string",
				"description": "Invocation ID returned by nodes_invoke.",
			},
		},
		"required":             []string{"invocation_id"},
		"additionalProperties": false,
	}
}

func (tool *NodeStatusTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	record, principal, snapshot, available, err := tool.runtime.visibleInvocation(ctx, args)
	if err != nil {
		return nodeInvocationError("INVOCATION_UNAVAILABLE", err.Error(), nil)
	}
	view := gatewayStatusResult(record, available)
	if record.State == nodes.GatewayInvocationPrepared {
		view.State = string(nodes.GatewayInvocationPrepared)
		return nodeJSONResult(view)
	}
	if !available {
		view.State = string(nodes.InvocationUnknown)
		view.ErrorCode = "NODE_UNAVAILABLE"
		view.RecoveryAction = "Retry nodes_status after the target reconnects."
		return nodeJSONResult(view)
	}
	remote, err := tool.runtime.source.QueryInvocation(
		ctx,
		principal,
		record.Target,
		snapshot.ID,
		record.Plan.InvocationID,
	)
	if err != nil {
		view.State = string(nodes.InvocationUnknown)
		view.ErrorCode = "STATUS_UNAVAILABLE"
		view.RecoveryAction = "Retry nodes_status; do not replay the original command."
		return nodeJSONResult(view)
	}
	return nodeJSONResult(remoteStatusResult(record, remote, true))
}

func (*NodeStatusTool) ToolLoopSemantics() loopguard.Semantics {
	return loopguard.SemanticsReadOnlyIdempotent
}

func (*NodeStatusTool) ToolSteeringSafety(map[string]any) SteeringSafety {
	return SteeringSafetyReadOnly
}

func (runtime *nodeInvocationToolRuntime) prepare(
	ctx context.Context,
	args map[string]any,
) (nodes.GatewayInvocationRecord, error) {
	if runtime == nil || runtime.source == nil || runtime.access == nil {
		return nodes.GatewayInvocationRecord{}, errors.New("node invocation runtime is unavailable")
	}
	agentID := strings.TrimSpace(ToolAgentID(ctx))
	resolved, err := runtime.resolveTarget(agentID, stringArgument(args, "target"), true)
	if err != nil {
		return nodes.GatewayInvocationRecord{}, err
	}
	command := strings.TrimSpace(stringArgument(args, "command"))
	if command == "" {
		return nodes.GatewayInvocationRecord{}, errors.New("command is required")
	}
	descriptor, err := resolved.registration.ApprovedCommand(command)
	if err != nil {
		return nodes.GatewayInvocationRecord{}, errors.New("command is not currently approved")
	}
	profile := nodes.ExecutionProfile{
		Executor:       resolved.snapshot.Executor,
		PolicyRevision: resolved.snapshot.PolicyRevision,
	}
	if profileErr := profile.Validate(); profileErr != nil {
		return nodes.GatewayInvocationRecord{}, errors.New(
			"target has no authenticated execution profile",
		)
	}
	if resolved.binding.Executor != "" && resolved.binding.Executor != profile.Executor {
		return nodes.GatewayInvocationRecord{}, errors.New(
			"target executor does not match the authenticated node",
		)
	}
	input, ok := args["input"].(map[string]any)
	if !ok {
		return nodes.GatewayInvocationRecord{}, errors.New("input must be an object")
	}
	inputJSON, err := json.Marshal(input)
	if err != nil {
		return nodes.GatewayInvocationRecord{}, errors.New("encode command input")
	}
	timeout, err := boundedNodeInteger(
		args,
		"timeout_seconds",
		defaultNodeInvocationTimeout,
		nodes.MaxInvocationTimeout,
	)
	if err != nil {
		return nodes.GatewayInvocationRecord{}, err
	}
	outputLimit, err := boundedNodeInteger(
		args,
		"output_limit_bytes",
		defaultNodeInvocationOutput,
		nodes.MaxInvocationOutput,
	)
	if err != nil {
		return nodes.GatewayInvocationRecord{}, err
	}
	principal, executionCallID, err := nodeInvocationIdentity(ctx)
	if err != nil {
		return nodes.GatewayInvocationRecord{}, err
	}
	invocationID := stableNodeInvocationID(
		"inv",
		principal.AgentID,
		principal.SessionID,
		principal.ActorID,
		executionCallID,
	)
	storedToolCallID := stableNodeInvocationID("call", executionCallID)
	request := nodes.InvocationRequest{
		InvocationID:     invocationID,
		IdempotencyKey:   stableNodeInvocationID("idem", invocationID),
		NodeID:           resolved.snapshot.ID,
		CatalogHash:      resolved.snapshot.CatalogHash,
		Command:          command,
		Input:            inputJSON,
		AgentID:          principal.AgentID,
		SessionID:        principal.SessionID,
		ActorID:          principal.ActorID,
		TimeoutSeconds:   timeout,
		OutputLimitBytes: outputLimit,
	}
	retained, found, err := runtime.source.LookupInvocationByToolCall(
		principal,
		storedToolCallID,
	)
	if err != nil {
		return nodes.GatewayInvocationRecord{}, errors.New("invocation registry is unavailable")
	}
	if found {
		if retainedErr := validateRetainedNodeInvocation(
			retained,
			resolved.name,
			request,
			descriptor,
			profile,
		); retainedErr != nil {
			return nodes.GatewayInvocationRecord{}, retainedErr
		}
		return retained, nil
	}
	if ToolApprovalContinuation(ctx) {
		return nodes.GatewayInvocationRecord{}, errors.New(
			"retained invocation authority expired before approval resumed",
		)
	}
	plan, err := nodes.PrepareExecutionPlan(
		request,
		descriptor,
		profile.Executor,
		profile.PolicyRevision,
		time.Now(),
		nodes.MaxExecutionPlanTTL,
	)
	if err != nil {
		return nodes.GatewayInvocationRecord{}, errors.New(
			"command input violates target policy",
		)
	}
	return runtime.source.PrepareInvocation(
		resolved.name,
		storedToolCallID,
		plan,
		descriptor,
	)
}

func validateRetainedNodeInvocation(
	retained nodes.GatewayInvocationRecord,
	target string,
	request nodes.InvocationRequest,
	descriptor nodes.CommandDescriptor,
	profile nodes.ExecutionProfile,
) error {
	ttlSeconds := retained.Plan.ExpiresAt - retained.Plan.PreparedAt
	if ttlSeconds <= 0 {
		return errors.New("retained invocation has invalid authority")
	}
	candidate, err := nodes.PrepareExecutionPlan(
		request,
		descriptor,
		profile.Executor,
		profile.PolicyRevision,
		time.Unix(retained.Plan.PreparedAt, 0),
		time.Duration(ttlSeconds)*time.Second,
	)
	if err != nil ||
		retained.Target != target ||
		candidate.PlanHash != retained.ExpectedPlanHash {
		return errors.New("tool call conflicts with retained invocation authority")
	}
	return nil
}

func (runtime *nodeInvocationToolRuntime) visibleInvocation(
	ctx context.Context,
	args map[string]any,
) (
	nodes.GatewayInvocationRecord,
	nodes.GatewayInvocationPrincipal,
	nodes.Snapshot,
	bool,
	error,
) {
	if runtime == nil || runtime.source == nil || runtime.access == nil {
		return nodes.GatewayInvocationRecord{}, nodes.GatewayInvocationPrincipal{},
			nodes.Snapshot{}, false, errors.New("node invocation runtime is unavailable")
	}
	invocationID := strings.TrimSpace(stringArgument(args, "invocation_id"))
	if invocationID == "" {
		return nodes.GatewayInvocationRecord{}, nodes.GatewayInvocationPrincipal{},
			nodes.Snapshot{}, false, errors.New("invocation_id is required")
	}
	principal, err := nodeInvocationIdentityWithoutCall(ctx)
	if err != nil {
		return nodes.GatewayInvocationRecord{}, nodes.GatewayInvocationPrincipal{},
			nodes.Snapshot{}, false, err
	}
	record, found, err := runtime.source.LookupInvocation(principal, invocationID)
	if err != nil {
		return nodes.GatewayInvocationRecord{}, nodes.GatewayInvocationPrincipal{},
			nodes.Snapshot{}, false, errors.New("invocation registry is unavailable")
	}
	if !found {
		return nodes.GatewayInvocationRecord{}, nodes.GatewayInvocationPrincipal{},
			nodes.Snapshot{}, false, errors.New("invocation was not found in this scope")
	}
	resolved, err := runtime.resolveTarget(ToolAgentID(ctx), record.Target, false)
	if err != nil || resolved.snapshot.ID != record.Plan.NodeID {
		return nodes.GatewayInvocationRecord{}, nodes.GatewayInvocationPrincipal{},
			nodes.Snapshot{}, false, errors.New("invocation target is no longer visible")
	}
	return record, principal, resolved.snapshot, resolved.available, nil
}

func (runtime *nodeInvocationToolRuntime) resolveTarget(
	agentID string,
	requested string,
	requireAvailable bool,
) (resolvedNodeTarget, error) {
	names, defaultTarget := runtime.access.visibleTargets(agentID)
	target := strings.TrimSpace(requested)
	if target == "" {
		target = defaultTarget
	}
	if target == "" || !containsSorted(names, target) {
		return resolvedNodeTarget{}, errors.New("target is not visible to this agent")
	}
	entry, snapshot, registration, err := runtime.access.resolve(target, defaultTarget)
	if err != nil {
		return resolvedNodeTarget{}, errors.New("node registry lookup failed")
	}
	if snapshot == nil || registration == nil {
		return resolvedNodeTarget{}, errors.New("target is not paired and approved")
	}
	if requireAvailable && !entry.Available {
		return resolvedNodeTarget{}, errors.New("target is not currently connected")
	}
	return resolvedNodeTarget{
		name:         target,
		binding:      runtime.access.targets[target],
		snapshot:     *snapshot,
		registration: registration,
		available:    entry.Available,
	}, nil
}

func nodeInvocationIdentity(
	ctx context.Context,
) (nodes.GatewayInvocationPrincipal, string, error) {
	principal, err := nodeInvocationIdentityWithoutCall(ctx)
	if err != nil {
		return nodes.GatewayInvocationPrincipal{}, "", err
	}
	toolCallID := strings.TrimSpace(ToolCallID(ctx))
	executionID := strings.TrimSpace(ToolExecutionID(ctx))
	workspace := strings.TrimSpace(ToolWorkspace(ctx))
	if toolCallID == "" || executionID == "" || workspace == "" {
		return nodes.GatewayInvocationPrincipal{}, "", errors.New(
			"node invocation requires workspace, execution, and provider tool-call identity",
		)
	}
	return principal, stableNodeInvocationID(
		"execution",
		workspace,
		executionID,
		toolCallID,
	), nil
}

func nodeInvocationIdentityWithoutCall(
	ctx context.Context,
) (nodes.GatewayInvocationPrincipal, error) {
	agentID := strings.TrimSpace(ToolAgentID(ctx))
	sessionID := strings.TrimSpace(ToolRouteSessionKey(ctx))
	if sessionID == "" {
		sessionID = strings.TrimSpace(ToolSessionKey(ctx))
	}
	actorID := strings.TrimSpace(ToolActorID(ctx))
	if actorID == "" {
		actorID = strings.TrimSpace(ToolSenderID(ctx))
	}
	if actorID == "" {
		actorID = agentID
	}
	if agentID == "" || sessionID == "" || actorID == "" {
		return nodes.GatewayInvocationPrincipal{}, errors.New(
			"node invocation requires agent, session, and actor identity",
		)
	}
	return nodes.GatewayInvocationPrincipal{
		AgentID:   stableNodeInvocationID("agent", agentID),
		SessionID: stableNodeInvocationID("session", sessionID),
		ActorID:   stableNodeInvocationID("actor", actorID),
	}, nil
}

func stableNodeInvocationID(prefix string, values ...string) string {
	hash := sha256.New()
	for _, value := range values {
		_, _ = fmt.Fprintf(hash, "%d:", len(value))
		_, _ = hash.Write([]byte(value))
	}
	return prefix + "_" + hex.EncodeToString(hash.Sum(nil))
}

func boundedNodeInteger(
	args map[string]any,
	name string,
	fallback int,
	maximum int,
) (int, error) {
	raw, exists := args[name]
	if !exists {
		return fallback, nil
	}
	var value int
	switch typed := raw.(type) {
	case int:
		value = typed
	case int64:
		if typed > int64(maximum) {
			return 0, fmt.Errorf("%s is outside bounds", name)
		}
		value = int(typed)
	case float64:
		if typed < 1 || typed > float64(maximum) || typed != float64(int(typed)) {
			return 0, fmt.Errorf("%s must be an integer", name)
		}
		value = int(typed)
	default:
		return 0, fmt.Errorf("%s must be an integer", name)
	}
	if value <= 0 || value > maximum {
		return 0, fmt.Errorf("%s is outside bounds", name)
	}
	return value, nil
}

func stringArgument(args map[string]any, name string) string {
	value, _ := args[name].(string)
	return value
}

func nodeInvocationError(code string, message string, view *nodeInvokeResult) *ToolResult {
	payload := map[string]any{"error": message, "error_code": code}
	if view != nil {
		payload["invocation"] = view
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return ErrorResult("node invocation failed")
	}
	return ErrorResult(string(data))
}

func gatewayStatusResult(
	record nodes.GatewayInvocationRecord,
	available bool,
) nodeStatusResult {
	return nodeStatusResult{
		InvocationID:  record.Plan.InvocationID,
		Target:        record.Target,
		Command:       record.Plan.Command,
		Risk:          record.Plan.Risk,
		GatewayState:  record.State,
		NodeAvailable: available,
	}
}

func remoteStatusResult(
	gateway nodes.GatewayInvocationRecord,
	remote nodes.InvocationRecord,
	available bool,
) nodeStatusResult {
	view := gatewayStatusResult(gateway, available)
	view.State = string(remote.State)
	view.AcceptedAt = remote.AcceptedAt
	view.UpdatedAt = remote.UpdatedAt
	view.CompletedAt = remote.CompletedAt
	view.Result = remote.Result
	view.Failure = remote.Failure
	view.Cancellation = remote.Cancellation
	return view
}
