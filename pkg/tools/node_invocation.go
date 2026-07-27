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

	"github.com/bogdanovich/mintclaw/pkg/config"
	runtimeevents "github.com/bogdanovich/mintclaw/pkg/events"
	"github.com/bogdanovich/mintclaw/pkg/nodes"
	"github.com/bogdanovich/mintclaw/pkg/tools/loopguard"
)

const (
	defaultNodeInvocationTimeout = 30
	defaultNodeInvocationOutput  = 64 * 1024
)

// ErrNodeDiscoveryStale marks a failed atomic preparation revalidation.
var ErrNodeDiscoveryStale = errors.New("node discovery revision is stale")

var (
	errDiscoveryStale       = ErrNodeDiscoveryStale
	errNodeTargetNotPaired  = errors.New("target is not paired and approved")
	errNodeTargetNotVisible = errors.New("target is not visible to this agent")
)

const (
	nodeDenialTargetUnavailable   = "TARGET_UNAVAILABLE"
	nodeDenialCommandUnavailable  = "COMMAND_UNAVAILABLE"
	nodeDenialReapprovalRequired  = "REAPPROVAL_REQUIRED"
	nodeDenialDiscoveryIncomplete = "DISCOVERY_INCOMPLETE"
	nodeDenialDiscoveryStale      = "DISCOVERY_STALE"
	nodeDenialSchemaInvalid       = "SCHEMA_INVALID"
	nodeDenialConstraintViolation = "CONSTRAINT_VIOLATION"

	nodeConstraintInputSchema   = "input_schema"
	nodeConstraintExecutable    = "executable_alias"
	nodeConstraintWorkingScope  = "working_scope"
	nodeConstraintEnvironment   = "environment_name"
	nodeConstraintTimeout       = "timeout"
	nodeConstraintOutputLimit   = "output_limit"
	nodeConstraintCommandPolicy = "command_policy"

	nodeActionRefreshDiscovery = "refresh_discovery"
	nodeActionCorrectInput     = "correct_input"
	nodeActionAskOperator      = "ask_operator"
)

type nodeSafeDenialError struct {
	Code       string
	Constraint string
	Action     string
	cause      error
}

func (denial *nodeSafeDenialError) Error() string {
	return "node invocation denied"
}

func (denial *nodeSafeDenialError) Unwrap() error {
	return denial.cause
}

func denyNodeInvocation(
	code string,
	constraint string,
	action string,
	cause error,
) error {
	return &nodeSafeDenialError{
		Code: code, Constraint: constraint, Action: action, cause: cause,
	}
}

func denyStaleNodeDiscovery() error {
	return denyNodeInvocation(
		nodeDenialDiscoveryStale,
		nodeConstraintCommandPolicy,
		nodeActionRefreshDiscovery,
		errDiscoveryStale,
	)
}

type NodeInvocationSource interface {
	NodeDiscoverySource
	PrepareInvocation(
		nodeRef string,
		target string,
		toolCallID string,
		principal nodes.GatewayInvocationPrincipal,
		plan nodes.ExecutionPlan,
		descriptor nodes.CommandDescriptor,
		allowCreate bool,
		validate func(NodeDiscoveryRecord) error,
	) (nodes.GatewayInvocationRecord, bool, error)
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
	access        *nodeTargetAccess
	source        NodeInvocationSource
	runtimeEvents runtimeevents.Bus
}

type resolvedNodeTarget struct {
	name               string
	binding            config.ExecutionTarget
	snapshot           nodes.Snapshot
	registration       *nodes.Registration
	available          bool
	requiresReapproval bool
}

type nodeDenialResult struct {
	Status     string `json:"status"`
	Code       string `json:"code"`
	Constraint string `json:"constraint"`
	Action     string `json:"action"`
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

const (
	NodeInvocationObservationPrepared   = "prepared"
	NodeInvocationObservationDispatched = "dispatched"
	NodeInvocationObservationCompleted  = "completed"
	NodeInvocationObservationStatus     = "status"
	NodeInvocationObservationUncertain  = "uncertain"
)

// NodeInvocationEventPayload is a redacted, passive invocation snapshot
// published to the runtime event bus. Concurrent observations are not a
// transaction log and may arrive out of order. Command input, output, node
// identity, and plan authority are intentionally excluded.
type NodeInvocationEventPayload struct {
	Observation  string                       `json:"observation"`
	InvocationID string                       `json:"invocation_id"`
	Target       string                       `json:"target"`
	Command      string                       `json:"command"`
	Risk         nodes.Risk                   `json:"risk"`
	GatewayState nodes.GatewayInvocationState `json:"gateway_state"`
	State        string                       `json:"state"`
	ErrorCode    string                       `json:"error_code,omitempty"`
}

func NewNodeInvokeTool(cfg *config.Config, source NodeInvocationSource) *NodeInvokeTool {
	return &NodeInvokeTool{runtime: newNodeInvocationToolRuntime(cfg, source)}
}

func NewNodeStatusTool(cfg *config.Config, source NodeInvocationSource) *NodeStatusTool {
	return &NodeStatusTool{runtime: newNodeInvocationToolRuntime(cfg, source)}
}

// SetEventPublisher injects the runtime event bus used for node invocation audit events.
func (tool *NodeInvokeTool) SetEventPublisher(eventBus runtimeevents.Bus) {
	if tool != nil && tool.runtime != nil {
		tool.runtime.runtimeEvents = eventBus
	}
}

// SetEventPublisher injects the runtime event bus used for node status audit events.
func (tool *NodeStatusTool) SetEventPublisher(eventBus runtimeevents.Bus) {
	if tool != nil && tool.runtime != nil {
		tool.runtime.runtimeEvents = eventBus
	}
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
			"discovery_revision": map[string]any{
				"type":        "string",
				"description": "Opaque revision returned by command-specific nodes describe.",
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
		"required":             []string{"command", "input", "discovery_revision"},
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
		var denial *nodeSafeDenialError
		if errors.As(err, &denial) {
			return nodeDenialToolResult(nodeDenialResult{
				Status:     "denied",
				Code:       denial.Code,
				Constraint: denial.Constraint,
				Action:     denial.Action,
			})
		}
		if errors.Is(err, errDiscoveryStale) {
			return nodeDenialToolResult(nodeDenialResult{
				Status:     "denied",
				Code:       nodeDenialDiscoveryStale,
				Constraint: nodeConstraintCommandPolicy,
				Action:     nodeActionRefreshDiscovery,
			})
		}
		return nodeDenialToolResult(nodeDenialResult{
			Status:     "denied",
			Code:       nodeDenialCommandUnavailable,
			Constraint: nodeConstraintCommandPolicy,
			Action:     nodeActionRefreshDiscovery,
		})
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
			if dispatched {
				tool.runtime.publishInvocationEvent(
					ctx,
					NodeInvocationObservationDispatched,
					"nodes_invoke",
					record,
					string(nodes.GatewayInvocationDispatched),
					"",
				)
			}
			tool.runtime.publishInvocationEvent(
				ctx,
				NodeInvocationObservationUncertain,
				"nodes_invoke",
				record,
				string(nodes.InvocationUnknown),
				"DISPATCH_UNCERTAIN",
			)
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
	tool.runtime.publishInvocationEvent(
		ctx,
		NodeInvocationObservationDispatched,
		"nodes_invoke",
		record,
		string(nodes.GatewayInvocationDispatched),
		"",
	)
	tool.runtime.publishInvocationEvent(
		ctx,
		NodeInvocationObservationCompleted,
		"nodes_invoke",
		record,
		string(nodes.InvocationSucceeded),
		"",
	)
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
		tool.runtime.publishInvocationEvent(
			ctx,
			NodeInvocationObservationUncertain,
			"nodes_status",
			record,
			view.State,
			view.ErrorCode,
		)
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
		tool.runtime.publishInvocationEvent(
			ctx,
			NodeInvocationObservationUncertain,
			"nodes_status",
			record,
			view.State,
			view.ErrorCode,
		)
		return nodeJSONResult(view)
	}
	if remote.State.Terminal() {
		errorCode := ""
		if remote.Failure != nil {
			errorCode = remote.Failure.Code
		}
		tool.runtime.publishInvocationEvent(
			ctx,
			NodeInvocationObservationStatus,
			"nodes_status",
			record,
			string(remote.State),
			errorCode,
		)
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
		return nodes.GatewayInvocationRecord{}, denyNodeInvocation(
			nodeDenialTargetUnavailable,
			nodeConstraintCommandPolicy,
			nodeActionRefreshDiscovery,
			nil,
		)
	}
	agentID := strings.TrimSpace(ToolAgentID(ctx))
	resolved, err := runtime.resolveTarget(agentID, stringArgument(args, "target"), false)
	if err != nil {
		if strings.TrimSpace(stringArgument(args, "discovery_revision")) != "" &&
			(errors.Is(err, errNodeTargetNotVisible) || errors.Is(err, errNodeTargetNotPaired)) {
			return nodes.GatewayInvocationRecord{}, denyStaleNodeDiscovery()
		}
		return nodes.GatewayInvocationRecord{}, denyNodeInvocation(
			nodeDenialTargetUnavailable,
			nodeConstraintCommandPolicy,
			nodeActionRefreshDiscovery,
			err,
		)
	}
	command := strings.TrimSpace(stringArgument(args, "command"))
	if command == "" {
		return nodes.GatewayInvocationRecord{}, denyNodeInvocation(
			nodeDenialCommandUnavailable,
			nodeConstraintCommandPolicy,
			nodeActionRefreshDiscovery,
			nil,
		)
	}
	descriptor, advertised := nodeCatalogDescriptor(resolved.snapshot.Catalog, command)
	if !advertised {
		return nodes.GatewayInvocationRecord{}, denyNodeInvocation(
			nodeDenialCommandUnavailable,
			nodeConstraintCommandPolicy,
			nodeActionRefreshDiscovery,
			nil,
		)
	}
	if resolved.requiresReapproval {
		return nodes.GatewayInvocationRecord{}, denyNodeInvocation(
			nodeDenialReapprovalRequired,
			nodeConstraintCommandPolicy,
			nodeActionAskOperator,
			nil,
		)
	}
	currentRevision, err := runtime.access.discoveryRevision(
		agentID,
		resolved.name,
		command,
		resolved.snapshot,
		*resolved.registration,
		descriptor,
		resolved.available,
	)
	if err != nil {
		return nodes.GatewayInvocationRecord{}, denyNodeInvocation(
			nodeDenialDiscoveryIncomplete,
			nodeConstraintInputSchema,
			nodeActionRefreshDiscovery,
			err,
		)
	}
	if strings.TrimSpace(stringArgument(args, "discovery_revision")) != currentRevision {
		return nodes.GatewayInvocationRecord{}, denyStaleNodeDiscovery()
	}
	if !resolved.available {
		return nodes.GatewayInvocationRecord{}, denyNodeInvocation(
			nodeDenialTargetUnavailable,
			nodeConstraintCommandPolicy,
			nodeActionRefreshDiscovery,
			nil,
		)
	}
	if descriptor.ModelContract == nil ||
		descriptor.ModelContract.Availability == nodes.ModelPartiallyDescribed {
		return nodes.GatewayInvocationRecord{}, denyNodeInvocation(
			nodeDenialDiscoveryIncomplete,
			nodeConstraintInputSchema,
			nodeActionRefreshDiscovery,
			nil,
		)
	}
	if descriptor.ModelContract.Availability == nodes.ModelUnavailable {
		return nodes.GatewayInvocationRecord{}, denyNodeInvocation(
			nodeDenialCommandUnavailable,
			nodeConstraintCommandPolicy,
			nodeActionAskOperator,
			nil,
		)
	}
	descriptor, err = resolved.registration.ApprovedCommand(command)
	if err != nil {
		return nodes.GatewayInvocationRecord{}, denyNodeInvocation(
			nodeDenialReapprovalRequired,
			nodeConstraintCommandPolicy,
			nodeActionRefreshDiscovery,
			err,
		)
	}
	profile := nodes.ExecutionProfile{
		Executor:       resolved.snapshot.Executor,
		PolicyRevision: resolved.snapshot.PolicyRevision,
	}
	if profileErr := profile.Validate(); profileErr != nil {
		return nodes.GatewayInvocationRecord{}, denyNodeInvocation(
			nodeDenialCommandUnavailable,
			nodeConstraintCommandPolicy,
			nodeActionAskOperator,
			profileErr,
		)
	}
	if resolved.binding.Executor != "" && resolved.binding.Executor != profile.Executor {
		return nodes.GatewayInvocationRecord{}, denyNodeInvocation(
			nodeDenialCommandUnavailable,
			nodeConstraintCommandPolicy,
			nodeActionAskOperator,
			nil,
		)
	}
	input, ok := args["input"].(map[string]any)
	if !ok {
		return nodes.GatewayInvocationRecord{}, denyNodeInvocation(
			nodeDenialSchemaInvalid,
			nodeConstraintInputSchema,
			nodeActionCorrectInput,
			nil,
		)
	}
	if constraintErr := validateNodeModelConstraints(descriptor, input); constraintErr != nil {
		return nodes.GatewayInvocationRecord{}, constraintErr
	}
	inputJSON, err := json.Marshal(input)
	if err != nil {
		return nodes.GatewayInvocationRecord{}, denyNodeInvocation(
			nodeDenialSchemaInvalid,
			nodeConstraintInputSchema,
			nodeActionCorrectInput,
			err,
		)
	}
	timeoutMaximum := nodes.MaxInvocationTimeout
	outputMaximum := nodes.MaxInvocationOutput
	if descriptor.ModelContract != nil {
		timeoutMaximum = descriptor.ModelContract.TimeoutSecondsMax
		outputMaximum = descriptor.ModelContract.OutputBytesMax
	}
	timeout, err := boundedNodeInteger(
		args,
		"timeout_seconds",
		min(defaultNodeInvocationTimeout, timeoutMaximum),
		timeoutMaximum,
	)
	if err != nil {
		return nodes.GatewayInvocationRecord{}, denyNodeInvocation(
			nodeDenialConstraintViolation,
			nodeConstraintTimeout,
			nodeActionCorrectInput,
			err,
		)
	}
	outputLimit, err := boundedNodeInteger(
		args,
		"output_limit_bytes",
		min(defaultNodeInvocationOutput, outputMaximum),
		outputMaximum,
	)
	if err != nil {
		return nodes.GatewayInvocationRecord{}, denyNodeInvocation(
			nodeDenialConstraintViolation,
			nodeConstraintOutputLimit,
			nodeActionCorrectInput,
			err,
		)
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
	plan, err := nodes.PrepareExecutionPlan(
		request,
		descriptor,
		profile.Executor,
		profile.PolicyRevision,
		time.Now(),
		nodes.MaxExecutionPlanTTL,
	)
	if err != nil {
		return nodes.GatewayInvocationRecord{}, denyNodeInvocation(
			nodeDenialSchemaInvalid,
			nodeConstraintInputSchema,
			nodeActionCorrectInput,
			err,
		)
	}
	requestedRevision := strings.TrimSpace(stringArgument(args, "discovery_revision"))
	record, created, err := runtime.source.PrepareInvocation(
		resolved.binding.Node,
		resolved.name,
		storedToolCallID,
		principal,
		plan,
		descriptor,
		!ToolApprovalContinuation(ctx),
		func(current NodeDiscoveryRecord) error {
			return runtime.validatePreparationAuthority(
				agentID,
				resolved.name,
				command,
				requestedRevision,
				current,
			)
		},
	)
	if errors.Is(err, nodes.ErrGatewayInvocationNotFound) && ToolApprovalContinuation(ctx) {
		return nodes.GatewayInvocationRecord{}, denyStaleNodeDiscovery()
	}
	if errors.Is(err, errDiscoveryStale) {
		return nodes.GatewayInvocationRecord{}, denyStaleNodeDiscovery()
	}
	if err != nil {
		return nodes.GatewayInvocationRecord{}, denyNodeInvocation(
			nodeDenialTargetUnavailable,
			nodeConstraintCommandPolicy,
			nodeActionRefreshDiscovery,
			err,
		)
	}
	if !created {
		if retainedErr := validateRetainedNodeInvocation(
			record,
			resolved.name,
			request,
			descriptor,
			profile,
		); retainedErr != nil {
			return nodes.GatewayInvocationRecord{}, denyStaleNodeDiscovery()
		}
		return record, nil
	}
	if err == nil && created {
		runtime.publishInvocationEvent(
			ctx,
			NodeInvocationObservationPrepared,
			"nodes_invoke",
			record,
			string(nodes.GatewayInvocationPrepared),
			"",
		)
	}
	return record, err
}

func (runtime *nodeInvocationToolRuntime) validatePreparationAuthority(
	agentID string,
	target string,
	command string,
	requestedRevision string,
	current NodeDiscoveryRecord,
) error {
	if current.Registration == nil || current.Snapshot.ID == "" {
		return errDiscoveryStale
	}
	descriptor, advertised := nodeCatalogDescriptor(current.Snapshot.Catalog, command)
	if !advertised {
		return errDiscoveryStale
	}
	revision, err := runtime.access.discoveryRevision(
		agentID,
		target,
		command,
		current.Snapshot,
		*current.Registration,
		descriptor,
		current.Connected,
	)
	if err != nil || revision != requestedRevision {
		return errDiscoveryStale
	}
	if !current.Connected ||
		(descriptor.ModelContract != nil &&
			descriptor.ModelContract.Availability == nodes.ModelUnavailable) {
		return errDiscoveryStale
	}
	if _, err := current.Registration.ApprovedCommand(command); err != nil {
		return errDiscoveryStale
	}
	return nil
}

func nodeCatalogDescriptor(
	catalog nodes.CapabilityCatalog,
	command string,
) (nodes.CommandDescriptor, bool) {
	for _, descriptor := range catalog.Commands {
		if descriptor.Name == command {
			return descriptor, true
		}
	}
	return nodes.CommandDescriptor{}, false
}

func (runtime *nodeInvocationToolRuntime) publishInvocationEvent(
	ctx context.Context,
	observation string,
	sourceName string,
	record nodes.GatewayInvocationRecord,
	state string,
	errorCode string,
) {
	if runtime == nil || runtime.runtimeEvents == nil {
		return
	}
	sessionKey := strings.TrimSpace(ToolRouteSessionKey(ctx))
	if sessionKey == "" {
		sessionKey = strings.TrimSpace(ToolSessionKey(ctx))
	}
	gatewayState := record.State
	if observation != NodeInvocationObservationPrepared {
		gatewayState = nodes.GatewayInvocationDispatched
	}
	payload := NodeInvocationEventPayload{
		Observation:  observation,
		InvocationID: record.Plan.InvocationID,
		Target:       record.Target,
		Command:      record.Plan.Command,
		Risk:         record.Plan.Risk,
		GatewayState: gatewayState,
		State:        state,
		ErrorCode:    errorCode,
	}
	severity := runtimeevents.SeverityInfo
	if observation == NodeInvocationObservationUncertain {
		severity = runtimeevents.SeverityWarn
	}
	attrs := map[string]any{
		"observation":   payload.Observation,
		"invocation_id": payload.InvocationID,
		"target":        payload.Target,
		"command":       payload.Command,
		"risk":          payload.Risk,
		"gateway_state": payload.GatewayState,
		"state":         payload.State,
	}
	if payload.ErrorCode != "" {
		attrs["error_code"] = payload.ErrorCode
	}
	runtime.runtimeEvents.PublishNonBlocking(runtimeevents.Event{
		Kind:   runtimeevents.KindNodeInvocationObserved,
		Source: runtimeevents.Source{Component: "nodes", Name: sourceName},
		Scope: runtimeevents.Scope{
			TraceScope: runtimeevents.NewTraceScope(
				ToolWorkspace(ctx),
				ToolExecutionID(ctx),
			),
			AgentID:    ToolAgentID(ctx),
			SessionKey: sessionKey,
			Channel:    ToolChannel(ctx),
			ChatID:     ToolChatID(ctx),
			TopicID:    ToolTopicID(ctx),
			SenderID:   ToolSenderID(ctx),
			MessageID:  ToolMessageID(ctx),
		},
		Correlation: runtimeevents.Correlation{RequestID: ToolCallID(ctx)},
		Severity:    severity,
		Payload:     payload,
		Attrs:       attrs,
	})
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
		return resolvedNodeTarget{}, errNodeTargetNotVisible
	}
	entry, snapshot, registration, err := runtime.access.resolve(target, defaultTarget)
	if err != nil {
		return resolvedNodeTarget{}, errors.New("node registry lookup failed")
	}
	if snapshot == nil || registration == nil {
		return resolvedNodeTarget{}, errNodeTargetNotPaired
	}
	if requireAvailable && !entry.liveConnected {
		return resolvedNodeTarget{}, errors.New("target is not currently connected")
	}
	return resolvedNodeTarget{
		name:               target,
		binding:            runtime.access.targets[target],
		snapshot:           *snapshot,
		registration:       registration,
		available:          entry.liveConnected,
		requiresReapproval: entry.RequiresReapproval,
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

func validateNodeModelConstraints(
	descriptor nodes.CommandDescriptor,
	input map[string]any,
) error {
	if descriptor.Name != "system.exec.v1" || descriptor.ModelContract == nil {
		return nil
	}
	constraints := descriptor.ModelContract.Constraints
	argv, ok := input["argv"].([]any)
	if !ok || len(argv) == 0 {
		return denyNodeInvocation(
			nodeDenialSchemaInvalid,
			nodeConstraintInputSchema,
			nodeActionCorrectInput,
			nil,
		)
	}
	executable, ok := argv[0].(string)
	if !ok || strings.TrimSpace(executable) == "" {
		return denyNodeInvocation(
			nodeDenialSchemaInvalid,
			nodeConstraintInputSchema,
			nodeActionCorrectInput,
			nil,
		)
	}
	if !containsSorted(constraints.ExecutableAliases, executable) {
		return denyNodeInvocation(
			nodeDenialConstraintViolation,
			nodeConstraintExecutable,
			nodeActionCorrectInput,
			nil,
		)
	}
	if raw, exists := input["cwd"]; exists {
		workingScope, valid := raw.(string)
		if !valid {
			return denyNodeInvocation(
				nodeDenialSchemaInvalid,
				nodeConstraintInputSchema,
				nodeActionCorrectInput,
				nil,
			)
		}
		if !containsSorted(constraints.WorkingScopes, workingScope) {
			return denyNodeInvocation(
				nodeDenialConstraintViolation,
				nodeConstraintWorkingScope,
				nodeActionCorrectInput,
				nil,
			)
		}
	}
	if raw, exists := input["env"]; exists {
		environment, valid := raw.(map[string]any)
		if !valid {
			return denyNodeInvocation(
				nodeDenialSchemaInvalid,
				nodeConstraintInputSchema,
				nodeActionCorrectInput,
				nil,
			)
		}
		for name := range environment {
			if !containsSorted(constraints.EnvironmentNames, name) {
				return denyNodeInvocation(
					nodeDenialConstraintViolation,
					nodeConstraintEnvironment,
					nodeActionCorrectInput,
					nil,
				)
			}
		}
	}
	if raw, exists := input["timeout_seconds"]; exists {
		timeout, valid := nodeInteger(raw)
		if !valid {
			return denyNodeInvocation(
				nodeDenialSchemaInvalid,
				nodeConstraintInputSchema,
				nodeActionCorrectInput,
				nil,
			)
		}
		if timeout <= 0 || timeout > descriptor.ModelContract.TimeoutSecondsMax {
			return denyNodeInvocation(
				nodeDenialConstraintViolation,
				nodeConstraintTimeout,
				nodeActionCorrectInput,
				nil,
			)
		}
	}
	return nil
}

func nodeInteger(raw any) (int, bool) {
	switch typed := raw.(type) {
	case int:
		return typed, true
	case int64:
		if int64(int(typed)) != typed {
			return 0, false
		}
		return int(typed), true
	case float64:
		if typed != float64(int(typed)) {
			return 0, false
		}
		return int(typed), true
	default:
		return 0, false
	}
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

func nodeDenialToolResult(denial nodeDenialResult) *ToolResult {
	data, err := json.Marshal(denial)
	if err != nil {
		return ErrorResult("node invocation denied")
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
