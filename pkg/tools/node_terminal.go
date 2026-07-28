package tools

import (
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/config"
	runtimeevents "github.com/bogdanovich/mintclaw/pkg/events"
	"github.com/bogdanovich/mintclaw/pkg/nodes"
	"github.com/bogdanovich/mintclaw/pkg/tools/loopguard"
)

const (
	defaultNodeTerminalColumns = 80
	defaultNodeTerminalRows    = 24
)

type NodeTerminalSource interface {
	NodeDiscoverySource
	PrepareTerminal(
		nodeID nodes.ID,
		nodeRef string,
		openID string,
		idempotencyKey string,
		owner nodes.TerminalOwner,
		workingScope string,
		columns int,
		rows int,
		allowCreate bool,
	) (nodes.GatewayTerminalRecord, bool, error)
	OpenTerminal(
		ctx context.Context,
		owner nodes.TerminalOwner,
		openID string,
		expectedPlanHash string,
	) (nodes.TerminalMetadata, bool, error)
	TerminalStatus(
		ctx context.Context,
		owner nodes.TerminalOwner,
		terminalID string,
	) (nodes.TerminalMetadata, error)
	SignalTerminal(
		ctx context.Context,
		owner nodes.TerminalOwner,
		terminalID string,
		signal string,
	) (nodes.TerminalMetadata, error)
	CloseTerminal(
		ctx context.Context,
		owner nodes.TerminalOwner,
		terminalID string,
	) (nodes.TerminalMetadata, error)
	BindTerminalOperator(
		owner nodes.TerminalOwner,
		terminalID string,
		operatorSessionID string,
	) error
}

type NodeTerminalTool struct {
	access        *nodeTargetAccess
	source        NodeTerminalSource
	runtimeEvents runtimeevents.Bus
}

type nodeTerminalPreparation struct {
	record            nodes.GatewayTerminalRecord
	operatorSessionID string
}

type nodeTerminalDiscovery struct {
	Target            string   `json:"target"`
	Available         bool     `json:"available"`
	Profiles          []string `json:"profiles"`
	WorkingScopes     []string `json:"working_scopes"`
	Approval          string   `json:"approval"`
	DiscoveryRevision string   `json:"discovery_revision"`
	ColumnsMin        int      `json:"columns_min"`
	ColumnsMax        int      `json:"columns_max"`
	RowsMin           int      `json:"rows_min"`
	RowsMax           int      `json:"rows_max"`
}

type nodeTerminalResult struct {
	TerminalID   string `json:"terminal_id"`
	Target       string `json:"target"`
	Profile      string `json:"profile"`
	State        string `json:"state"`
	Reason       string `json:"reason,omitempty"`
	ExitCode     int    `json:"exit_code,omitempty"`
	Signal       string `json:"signal,omitempty"`
	StartedAt    int64  `json:"started_at,omitempty"`
	CompletedAt  int64  `json:"completed_at,omitempty"`
	AttachBefore int64  `json:"attach_before,omitempty"`
	ErrorCode    string `json:"error_code,omitempty"`
}

const (
	NodeTerminalObservationPrepared = "prepared"
	NodeTerminalObservationOpened   = "opened"
	NodeTerminalObservationStatus   = "status"
	NodeTerminalObservationSignal   = "signal"
	NodeTerminalObservationClosed   = "closed"
	NodeTerminalObservationUnknown  = "unknown"
)

// NodeTerminalEventPayload is a passive redacted lifecycle observation.
// It contains no terminal bytes, owner identity, paths, environment, broker
// details, credentials, or authority digests.
type NodeTerminalEventPayload struct {
	Observation string `json:"observation"`
	TerminalID  string `json:"terminal_id,omitempty"`
	Target      string `json:"target"`
	Profile     string `json:"profile"`
	State       string `json:"state"`
	ErrorCode   string `json:"error_code,omitempty"`
}

func NewNodeTerminalTool(cfg *config.Config, source NodeTerminalSource) *NodeTerminalTool {
	return &NodeTerminalTool{
		access: newNodeTargetAccess(cfg, source),
		source: source,
	}
}

func (tool *NodeTerminalTool) SetEventPublisher(eventBus runtimeevents.Bus) {
	if tool != nil {
		tool.runtimeEvents = eventBus
	}
}

func (*NodeTerminalTool) Name() string { return "nodes_terminal" }

func (*NodeTerminalTool) Description() string {
	return "Discover and control one bounded attached terminal on an operator-configured node. " +
		"Terminal input, output, and resize are available only to the authenticated operator client."
}

func (*NodeTerminalTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"action": map[string]any{
				"type": "string",
				"enum": []string{"discover", "open", "status", "signal", "close"},
			},
			"target": map[string]any{
				"type":        "string",
				"description": "Visible operator-configured target name.",
			},
			"profile": map[string]any{
				"type":        "string",
				"description": "Operator-owned profile alias returned by terminal discovery.",
			},
			"working_scope": map[string]any{
				"type":        "string",
				"description": "Model-safe working scope alias returned by terminal discovery.",
			},
			"discovery_revision": map[string]any{
				"type":        "string",
				"description": "Opaque revision returned by terminal discovery.",
			},
			"columns": map[string]any{
				"type":    "integer",
				"minimum": 20,
				"maximum": 400,
			},
			"rows": map[string]any{
				"type":    "integer",
				"minimum": 5,
				"maximum": 200,
			},
			"terminal_id": map[string]any{
				"type":        "string",
				"description": "Terminal ID returned by open.",
			},
			"signal": map[string]any{
				"type": "string",
				"enum": []string{"INT", "TERM", "HUP"},
			},
		},
		"required":             []string{"action", "target"},
		"additionalProperties": false,
	}
}

func (tool *NodeTerminalTool) ApprovalArguments(
	ctx context.Context,
	args map[string]any,
) (map[string]any, error) {
	action := normalizedTerminalAction(args)
	if action != "open" {
		return map[string]any{
			"action":      action,
			"target":      strings.TrimSpace(stringArgument(args, "target")),
			"profile":     strings.TrimSpace(stringArgument(args, "profile")),
			"terminal_id": strings.TrimSpace(stringArgument(args, "terminal_id")),
			"signal":      strings.TrimSpace(stringArgument(args, "signal")),
		}, nil
	}
	prepared, err := tool.prepareOpen(ctx, args)
	if err != nil {
		return nil, err
	}
	plan := prepared.record.Plan
	return map[string]any{
		"action":        "open",
		"target":        plan.Owner.Target,
		"profile":       plan.Owner.Profile,
		"working_scope": plan.WorkingScope,
		"columns":       plan.Columns,
		"rows":          plan.Rows,
		"open_id":       plan.OpenID,
		"approval":      plan.ApprovalMode,
		"expires_at":    plan.ExpiresAt,
	}, nil
}

func (tool *NodeTerminalTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	switch normalizedTerminalAction(args) {
	case "discover":
		return tool.discover(ctx, args)
	case "open":
		return tool.open(ctx, args)
	case "status":
		return tool.status(ctx, args)
	case "signal":
		return tool.signal(ctx, args)
	case "close":
		return tool.close(ctx, args)
	default:
		return ErrorResult("terminal action must be discover, open, status, signal, or close")
	}
}

func (*NodeTerminalTool) ToolLoopSemantics() loopguard.Semantics {
	return loopguard.SemanticsMutating
}

func (*NodeTerminalTool) ToolSteeringSafety(map[string]any) SteeringSafety {
	return SteeringSafetyCancellable
}

func (tool *NodeTerminalTool) discover(ctx context.Context, args map[string]any) *ToolResult {
	if _, err := terminalOperatorSessionID(ctx); err != nil {
		return nodeTerminalDenial()
	}
	resolved, descriptor, revision, err := tool.resolveTerminalAuthority(ctx, args, false)
	if err != nil {
		return nodeTerminalDenial()
	}
	contract := descriptor.ModelContract
	return nodeJSONResult(nodeTerminalDiscovery{
		Target:            resolved.name,
		Available:         resolved.available,
		Profiles:          append([]string(nil), contract.Constraints.ProfileAliases...),
		WorkingScopes:     append([]string(nil), contract.Constraints.WorkingScopes...),
		Approval:          "session_start",
		DiscoveryRevision: revision,
		ColumnsMin:        20,
		ColumnsMax:        400,
		RowsMin:           5,
		RowsMax:           200,
	})
}

func (tool *NodeTerminalTool) open(ctx context.Context, args map[string]any) *ToolResult {
	prepared, err := tool.prepareOpen(ctx, args)
	if err != nil {
		return nodeTerminalDenial()
	}
	if !ToolApprovalContinuation(ctx) {
		return nodeDenialToolResult(nodeDenialResult{
			Status:     "denied",
			Code:       nodeDenialApprovalRequired,
			Constraint: nodeConstraintApproval,
			Action:     nodeActionAskOperator,
		})
	}
	record := prepared.record
	metadata, dispatched, err := tool.source.OpenTerminal(
		ctx,
		record.Plan.Owner,
		record.Plan.OpenID,
		record.ExpectedPlanHash,
	)
	if err != nil {
		observation := NodeTerminalObservationUnknown
		state := string(nodes.GatewayTerminalPrepared)
		errorCode := "TERMINAL_OPEN_DENIED"
		if dispatched {
			state = string(nodes.GatewayTerminalUnknown)
			errorCode = "TERMINAL_OPEN_UNKNOWN"
		}
		tool.publishEvent(ctx, observation, record.Plan.Owner, metadata.TerminalID, state, errorCode)
		return nodeJSONResult(nodeTerminalResult{
			TerminalID: metadata.TerminalID,
			Target:     record.Plan.Owner.Target,
			Profile:    record.Plan.Owner.Profile,
			State:      state,
			ErrorCode:  errorCode,
		})
	}
	if err := tool.source.BindTerminalOperator(
		record.Plan.Owner,
		metadata.TerminalID,
		prepared.operatorSessionID,
	); err != nil {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.WithoutCancel(ctx),
			5*time.Second,
		)
		defer cleanupCancel()
		closed, closeErr := tool.source.CloseTerminal(
			cleanupCtx,
			record.Plan.Owner,
			metadata.TerminalID,
		)
		tool.publishEvent(
			ctx,
			NodeTerminalObservationUnknown,
			record.Plan.Owner,
			metadata.TerminalID,
			closed.State,
			"OPERATOR_ATTACH_UNAVAILABLE",
		)
		if closeErr != nil {
			return nodeJSONResult(nodeTerminalResult{
				TerminalID: metadata.TerminalID,
				Target:     record.Plan.Owner.Target,
				Profile:    record.Plan.Owner.Profile,
				State:      string(nodes.GatewayTerminalUnknown),
				ErrorCode:  "OPERATOR_ATTACH_UNAVAILABLE",
			})
		}
		return nodeTerminalMetadataResult(record.Plan.Owner, closed)
	}
	tool.publishEvent(
		ctx,
		NodeTerminalObservationOpened,
		record.Plan.Owner,
		metadata.TerminalID,
		metadata.State,
		"",
	)
	result := nodeTerminalMetadataView(record.Plan.Owner, metadata)
	result.AttachBefore = metadata.StartedAt + 30
	return nodeJSONResult(result)
}

func (tool *NodeTerminalTool) status(ctx context.Context, args map[string]any) *ToolResult {
	owner, terminalID, err := terminalControlIdentity(ctx, args)
	if err != nil || tool == nil || tool.source == nil {
		return nodeTerminalDenial()
	}
	metadata, err := tool.source.TerminalStatus(ctx, owner, terminalID)
	if err != nil {
		return nodeTerminalDenial()
	}
	tool.publishEvent(ctx, NodeTerminalObservationStatus, owner, terminalID, metadata.State, "")
	return nodeTerminalMetadataResult(owner, metadata)
}

func (tool *NodeTerminalTool) signal(ctx context.Context, args map[string]any) *ToolResult {
	owner, terminalID, err := terminalControlIdentity(ctx, args)
	signal := strings.TrimSpace(stringArgument(args, "signal"))
	if err != nil || !slices.Contains([]string{"INT", "TERM", "HUP"}, signal) ||
		tool == nil ||
		tool.source == nil {
		return nodeTerminalDenial()
	}
	metadata, err := tool.source.SignalTerminal(ctx, owner, terminalID, signal)
	if err != nil {
		return nodeTerminalDenial()
	}
	tool.publishEvent(ctx, NodeTerminalObservationSignal, owner, terminalID, metadata.State, "")
	return nodeTerminalMetadataResult(owner, metadata)
}

func (tool *NodeTerminalTool) close(ctx context.Context, args map[string]any) *ToolResult {
	owner, terminalID, err := terminalControlIdentity(ctx, args)
	if err != nil || tool == nil || tool.source == nil {
		return nodeTerminalDenial()
	}
	metadata, err := tool.source.CloseTerminal(ctx, owner, terminalID)
	if err != nil {
		return nodeTerminalDenial()
	}
	tool.publishEvent(ctx, NodeTerminalObservationClosed, owner, terminalID, metadata.State, "")
	return nodeTerminalMetadataResult(owner, metadata)
}

func (tool *NodeTerminalTool) prepareOpen(
	ctx context.Context,
	args map[string]any,
) (nodeTerminalPreparation, error) {
	resolved, descriptor, revision, err := tool.resolveTerminalAuthority(ctx, args, true)
	if err != nil {
		return nodeTerminalPreparation{}, err
	}
	if strings.TrimSpace(stringArgument(args, "discovery_revision")) != revision {
		return nodeTerminalPreparation{}, denyStaleNodeDiscovery()
	}
	profile := strings.TrimSpace(stringArgument(args, "profile"))
	workingScope := strings.TrimSpace(stringArgument(args, "working_scope"))
	contract := descriptor.ModelContract
	if !slices.Contains(contract.Constraints.ProfileAliases, profile) ||
		!slices.Contains(contract.Constraints.WorkingScopes, workingScope) {
		return nodeTerminalPreparation{}, nodes.ErrCommandDenied
	}
	columns, err := boundedNodeInteger(args, "columns", defaultNodeTerminalColumns, 400)
	if err != nil || columns < 20 {
		return nodeTerminalPreparation{}, nodes.ErrInvalidTerminal
	}
	rows, err := boundedNodeInteger(args, "rows", defaultNodeTerminalRows, 200)
	if err != nil || rows < 5 {
		return nodeTerminalPreparation{}, nodes.ErrInvalidTerminal
	}
	owner, operatorSessionID, err := terminalOpenIdentity(ctx, resolved.name, profile)
	if err != nil {
		return nodeTerminalPreparation{}, err
	}
	workspace := strings.TrimSpace(ToolWorkspace(ctx))
	executionID := strings.TrimSpace(ToolExecutionID(ctx))
	toolCallID := strings.TrimSpace(ToolCallID(ctx))
	if workspace == "" || executionID == "" || toolCallID == "" {
		return nodeTerminalPreparation{}, errors.New("terminal open identity is incomplete")
	}
	executionCallID := stableNodeInvocationID("terminal_execution", workspace, executionID, toolCallID)
	openID := stableNodeInvocationID(
		"terminal",
		owner.AgentID,
		owner.SessionID,
		owner.ActorID,
		executionCallID,
	)
	record, created, err := tool.source.PrepareTerminal(
		resolved.snapshot.ID,
		resolved.binding.Node,
		openID,
		stableNodeInvocationID("terminal_idem", openID),
		owner,
		workingScope,
		columns,
		rows,
		!ToolApprovalContinuation(ctx),
	)
	if errors.Is(err, nodes.ErrGatewayTerminalNotFound) && ToolApprovalContinuation(ctx) {
		return nodeTerminalPreparation{}, denyStaleNodeDiscovery()
	}
	if err != nil {
		return nodeTerminalPreparation{}, err
	}
	if created {
		tool.publishEvent(
			ctx,
			NodeTerminalObservationPrepared,
			owner,
			"",
			string(nodes.GatewayTerminalPrepared),
			"",
		)
	}
	return nodeTerminalPreparation{record: record, operatorSessionID: operatorSessionID}, nil
}

func (tool *NodeTerminalTool) resolveTerminalAuthority(
	ctx context.Context,
	args map[string]any,
	requireAvailable bool,
) (resolvedNodeTarget, nodes.CommandDescriptor, string, error) {
	if tool == nil || tool.source == nil || tool.access == nil {
		return resolvedNodeTarget{}, nodes.CommandDescriptor{}, "", errors.New("terminal runtime is unavailable")
	}
	runtime := &nodeInvocationToolRuntime{access: tool.access}
	resolved, err := runtime.resolveTarget(
		ToolAgentID(ctx),
		strings.TrimSpace(stringArgument(args, "target")),
		requireAvailable,
	)
	if err != nil || resolved.requiresReapproval {
		return resolvedNodeTarget{}, nodes.CommandDescriptor{}, "", nodes.ErrCommandDenied
	}
	descriptor, found := nodeCatalogDescriptor(resolved.snapshot.Catalog, "shell.exec.v1")
	if !found ||
		descriptor.ModelContract == nil ||
		descriptor.ModelContract.Availability != nodes.ModelAvailable ||
		descriptor.ModelContract.ApprovalMode != "each_command" ||
		len(descriptor.ModelContract.Constraints.ProfileAliases) == 0 ||
		len(descriptor.ModelContract.Constraints.WorkingScopes) == 0 {
		return resolvedNodeTarget{}, nodes.CommandDescriptor{}, "", nodes.ErrCommandDenied
	}
	if _, approvalErr := resolved.registration.ApprovedCommand("shell.exec.v1"); approvalErr != nil {
		return resolvedNodeTarget{}, nodes.CommandDescriptor{}, "", nodes.ErrCommandDenied
	}
	revision, err := tool.access.discoveryRevision(
		ToolAgentID(ctx),
		resolved.name,
		"shell.exec.v1",
		resolved.snapshot,
		*resolved.registration,
		descriptor,
		resolved.available,
	)
	if err != nil {
		return resolvedNodeTarget{}, nodes.CommandDescriptor{}, "", err
	}
	return resolved, descriptor, revision, nil
}

func terminalOpenIdentity(
	ctx context.Context,
	target string,
	profile string,
) (nodes.TerminalOwner, string, error) {
	principal, err := nodeInvocationIdentityWithoutCall(ctx)
	if err != nil {
		return nodes.TerminalOwner{}, "", err
	}
	operatorSessionID, err := terminalOperatorSessionID(ctx)
	if err != nil {
		return nodes.TerminalOwner{}, "", err
	}
	return nodes.TerminalOwner{
		ActorID: principal.ActorID,
		AgentID: principal.AgentID,
		RouteID: stableNodeInvocationID(
			"route",
			ToolChannel(ctx),
			ToolChatID(ctx),
			ToolRouteSessionKey(ctx),
		),
		SessionID:   principal.SessionID,
		WorkspaceID: principal.WorkspaceID,
		Target:      target,
		Profile:     profile,
	}, operatorSessionID, nil
}

func terminalOperatorSessionID(ctx context.Context) (string, error) {
	channel := strings.TrimSpace(ToolChannel(ctx))
	chatID := strings.TrimSpace(ToolChatID(ctx))
	routeSessionID := strings.TrimSpace(ToolRouteSessionKey(ctx))
	const prefix = "mintclaw:"
	if channel != config.ChannelMintClaw ||
		!strings.HasPrefix(chatID, prefix) ||
		len(chatID) <= len(prefix) ||
		routeSessionID == "" {
		return "", errors.New(
			"attached terminals require an authenticated MintClaw operator session",
		)
	}
	operatorSessionID := strings.TrimSpace(strings.TrimPrefix(chatID, prefix))
	if operatorSessionID == "" {
		return "", errors.New("operator session identity is empty")
	}
	return operatorSessionID, nil
}

func terminalControlIdentity(
	ctx context.Context,
	args map[string]any,
) (nodes.TerminalOwner, string, error) {
	target := strings.TrimSpace(stringArgument(args, "target"))
	profile := strings.TrimSpace(stringArgument(args, "profile"))
	terminalID := strings.TrimSpace(stringArgument(args, "terminal_id"))
	owner, _, err := terminalOpenIdentity(ctx, target, profile)
	if err != nil {
		return nodes.TerminalOwner{}, "", err
	}
	if err := (nodes.TerminalSessionRequest{
		TerminalID: terminalID,
		Owner:      owner,
	}).Validate(); err != nil {
		return nodes.TerminalOwner{}, "", err
	}
	return owner, terminalID, nil
}

func normalizedTerminalAction(args map[string]any) string {
	return strings.ToLower(strings.TrimSpace(stringArgument(args, "action")))
}

func nodeTerminalMetadataResult(
	owner nodes.TerminalOwner,
	metadata nodes.TerminalMetadata,
) *ToolResult {
	return nodeJSONResult(nodeTerminalMetadataView(owner, metadata))
}

func nodeTerminalMetadataView(
	owner nodes.TerminalOwner,
	metadata nodes.TerminalMetadata,
) nodeTerminalResult {
	return nodeTerminalResult{
		TerminalID:  metadata.TerminalID,
		Target:      owner.Target,
		Profile:     owner.Profile,
		State:       metadata.State,
		Reason:      safeTerminalReason(metadata.Reason),
		ExitCode:    metadata.ExitCode,
		Signal:      safeTerminalSignal(metadata.Signal),
		StartedAt:   metadata.StartedAt,
		CompletedAt: metadata.CompletedAt,
	}
}

func safeTerminalReason(reason string) string {
	switch reason {
	case "exit",
		"close",
		"idle_timeout",
		"lifetime_timeout",
		"disconnect",
		"output_overflow",
		"attach_timeout",
		"input_outcome_unknown",
		"invalid_sequence",
		"gateway_restarted",
		"gateway_shutdown":
		return reason
	default:
		return ""
	}
}

func safeTerminalSignal(signal string) string {
	switch signal {
	case "SIGHUP",
		"SIGINT",
		"SIGQUIT",
		"SIGKILL",
		"SIGTERM",
		"SIGPIPE",
		"hangup",
		"interrupt",
		"quit",
		"killed",
		"terminated",
		"broken pipe":
		return signal
	default:
		return ""
	}
}

func nodeTerminalDenial() *ToolResult {
	return nodeJSONResult(nodeTerminalResult{
		State:     "denied",
		ErrorCode: "TERMINAL_DENIED",
	})
}

func (tool *NodeTerminalTool) publishEvent(
	ctx context.Context,
	observation string,
	owner nodes.TerminalOwner,
	terminalID string,
	state string,
	errorCode string,
) {
	if tool == nil || tool.runtimeEvents == nil {
		return
	}
	sessionKey := strings.TrimSpace(ToolRouteSessionKey(ctx))
	if sessionKey == "" {
		sessionKey = strings.TrimSpace(ToolSessionKey(ctx))
	}
	payload := NodeTerminalEventPayload{
		Observation: observation,
		TerminalID:  terminalID,
		Target:      owner.Target,
		Profile:     owner.Profile,
		State:       state,
		ErrorCode:   errorCode,
	}
	attributes := map[string]any{
		"observation": payload.Observation,
		"terminal_id": payload.TerminalID,
		"target":      payload.Target,
		"profile":     payload.Profile,
		"state":       payload.State,
	}
	if errorCode != "" {
		attributes["error_code"] = errorCode
	}
	severity := runtimeevents.SeverityInfo
	if observation == NodeTerminalObservationUnknown {
		severity = runtimeevents.SeverityWarn
	}
	tool.runtimeEvents.PublishNonBlocking(runtimeevents.Event{
		Kind:   runtimeevents.KindNodeTerminalObserved,
		Source: runtimeevents.Source{Component: "nodes", Name: "nodes_terminal"},
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
		Attrs:       attributes,
	})
}
