package tools

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/browser"
	"github.com/bogdanovich/mintclaw/pkg/config"
	"github.com/bogdanovich/mintclaw/pkg/interactions"
	"github.com/bogdanovich/mintclaw/pkg/routing"
	"github.com/bogdanovich/mintclaw/pkg/tools/loopguard"
)

// BrowserToolSource is the narrow gateway-owned boundary used by first-party
// browser tools. Implementations keep the runtime alive for the full method
// call so configuration reload cannot hand a tool a stale broker pointer.
type BrowserToolSource interface {
	Available() bool
	ProfileAvailability(context.Context, string, string) (browser.ProfileAvailability, error)
	Open(context.Context, browser.OpenRequest) (browser.Session, error)
	Status(context.Context, browser.Owner, string) (browser.Session, error)
	Close(context.Context, browser.Owner, string) (browser.Session, error)
	Observe(context.Context, browser.Owner, string, string) (browser.Observation, error)
	PrepareAction(context.Context, browser.PrepareActionRequest) (browser.Preparation, error)
	ExecuteAction(context.Context, browser.Owner, string, *browser.ApprovalBinding) (browser.Invocation, error)
}

type browserToolRuntime struct {
	config        config.BrowserToolsConfig
	source        BrowserToolSource
	allowedAgents map[string]struct{}
}

type (
	BrowserTargetsTool struct{ runtime *browserToolRuntime }
	BrowserSessionTool struct{ runtime *browserToolRuntime }
	BrowserObserveTool struct{ runtime *browserToolRuntime }
	BrowserActTool     struct{ runtime *browserToolRuntime }
)

func NewBrowserTargetsTool(cfg *config.Config, source BrowserToolSource) *BrowserTargetsTool {
	return &BrowserTargetsTool{runtime: newBrowserToolRuntime(cfg, source)}
}

func NewBrowserSessionTool(cfg *config.Config, source BrowserToolSource) *BrowserSessionTool {
	return &BrowserSessionTool{runtime: newBrowserToolRuntime(cfg, source)}
}

func NewBrowserObserveTool(cfg *config.Config, source BrowserToolSource) *BrowserObserveTool {
	return &BrowserObserveTool{runtime: newBrowserToolRuntime(cfg, source)}
}

func NewBrowserActTool(cfg *config.Config, source BrowserToolSource) *BrowserActTool {
	return &BrowserActTool{runtime: newBrowserToolRuntime(cfg, source)}
}

func newBrowserToolRuntime(cfg *config.Config, source BrowserToolSource) *browserToolRuntime {
	runtime := &browserToolRuntime{source: source, allowedAgents: make(map[string]struct{})}
	if cfg == nil {
		return runtime
	}
	runtime.config = cfg.Tools.Browser
	for _, agentID := range runtime.config.Agents {
		runtime.allowedAgents[routing.NormalizeAgentID(agentID)] = struct{}{}
	}
	return runtime
}

func (runtime *browserToolRuntime) enabledForAgent(agentID string) bool {
	if runtime == nil || !runtime.config.Enabled || runtime.source == nil {
		return false
	}
	_, ok := runtime.allowedAgents[routing.NormalizeAgentID(agentID)]
	return ok
}

func (tool *BrowserTargetsTool) ToolEnabledForAgent(agentID string) bool {
	return tool != nil && tool.runtime.enabledForAgent(agentID)
}

func (tool *BrowserSessionTool) ToolEnabledForAgent(agentID string) bool {
	return tool != nil && tool.runtime.enabledForAgent(agentID)
}

func (tool *BrowserObserveTool) ToolEnabledForAgent(agentID string) bool {
	return tool != nil && tool.runtime.enabledForAgent(agentID)
}

func (tool *BrowserActTool) ToolEnabledForAgent(agentID string) bool {
	return tool != nil && tool.runtime.enabledForAgent(agentID)
}

func (*BrowserTargetsTool) Name() string { return "browser_targets" }
func (*BrowserTargetsTool) Description() string {
	return "List browser targets and managed profiles granted to this agent without starting a browser."
}

func (*BrowserTargetsTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object", "properties": map[string]any{}, "additionalProperties": false,
	}
}

func (*BrowserTargetsTool) ToolLoopSemantics() loopguard.Semantics {
	return loopguard.SemanticsReadOnlyIdempotent
}

type browserTargetResult struct {
	Targets []browserTargetView `json:"targets"`
}

type browserTargetView struct {
	Target   string               `json:"target"`
	Status   string               `json:"status"`
	Reason   string               `json:"reason,omitempty"`
	Profiles []browserProfileView `json:"profiles"`
	Actions  []browser.ActionKind `json:"actions"`
	Limits   browserLimitsView    `json:"limits"`
}

type browserProfileView struct {
	Profile string `json:"profile"`
	Status  string `json:"status"`
	Reason  string `json:"reason,omitempty"`
	DryRun  bool   `json:"dry_run"`
}

type browserLimitsView struct {
	Sessions      int `json:"sessions"`
	Tabs          int `json:"tabs"`
	SnapshotBytes int `json:"snapshot_bytes"`
}

func (tool *BrowserTargetsTool) Execute(ctx context.Context, _ map[string]any) *ToolResult {
	if !tool.runtime.enabledForAgent(ToolAgentID(ctx)) {
		return browserErrorResult(
			"not_granted",
			"Browser access is not granted to this agent.",
			"use_an_authorized_agent",
		)
	}
	runtimeAvailable := tool.runtime.source.Available()
	limits := tool.runtime.config.Limits.Effective()
	targetNames := make([]string, 0, len(tool.runtime.config.Targets))
	for name, target := range tool.runtime.config.Targets {
		if target.Enabled {
			targetNames = append(targetNames, name)
		}
	}
	sort.Strings(targetNames)
	views := make([]browserTargetView, 0, len(targetNames))
	for _, name := range targetNames {
		target := tool.runtime.config.Targets[name]
		profileNames := make([]string, 0, len(target.Profiles))
		for profileName, profile := range target.Profiles {
			if profile.Enabled {
				profileNames = append(profileNames, profileName)
			}
		}
		sort.Strings(profileNames)
		profiles := make([]browserProfileView, 0, len(profileNames))
		for _, profileName := range profileNames {
			profile := target.Profiles[profileName]
			status, reason := "unavailable", "driver_unavailable"
			if runtimeAvailable {
				availability, err := tool.runtime.source.ProfileAvailability(ctx, name, profileName)
				if err == nil {
					status, reason = availability.Status, availability.Reason
				} else {
					reason = "recovery_required"
				}
			}
			profiles = append(profiles, browserProfileView{
				Profile: profileName, Status: status, Reason: reason, DryRun: profile.DryRun,
			})
		}
		targetStatus, targetReason := "ready", ""
		for _, profile := range profiles {
			if profile.Status != "ready" {
				targetStatus, targetReason = profile.Status, profile.Reason
				break
			}
		}
		views = append(views, browserTargetView{
			Target: name, Status: targetStatus, Reason: targetReason, Profiles: profiles,
			Actions: []browser.ActionKind{
				browser.ActionNavigate, browser.ActionClick, browser.ActionFill,
				browser.ActionSelect, browser.ActionPress, browser.ActionScroll, browser.ActionDialog,
			},
			Limits: browserLimitsView{
				Sessions: limits.Sessions, Tabs: limits.Tabs, SnapshotBytes: limits.SnapshotBytes,
			},
		})
	}
	return tool.runtime.result(browserTargetResult{Targets: views})
}

func (*BrowserSessionTool) Name() string { return "browser_session" }
func (*BrowserSessionTool) Description() string {
	return "Open, inspect, or close one broker-owned browser session."
}

func (*BrowserSessionTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"operation":          map[string]any{"type": "string", "enum": []string{"open", "status", "close"}},
			"target":             map[string]any{"type": "string"},
			"profile":            map[string]any{"type": "string"},
			"browser_session_id": map[string]any{"type": "string"},
		},
		"required": []string{"operation"}, "additionalProperties": false,
	}
}

func (*BrowserSessionTool) ToolLoopSemantics() loopguard.Semantics {
	return loopguard.SemanticsMutating
}

type browserSessionView struct {
	BrowserSessionID     string               `json:"browser_session_id"`
	State                browser.SessionState `json:"state"`
	Target               string               `json:"target"`
	Profile              string               `json:"profile"`
	DryRun               bool                 `json:"dry_run"`
	ControllerGeneration uint64               `json:"controller_generation"`
	ExpiresAt            int64                `json:"expires_at"`
	Tabs                 []browserTabView     `json:"tabs"`
	Reason               string               `json:"reason,omitempty"`
}

type browserTabView struct {
	TabID              string `json:"tab_id"`
	SnapshotID         string `json:"snapshot_id,omitempty"`
	SnapshotGeneration uint64 `json:"snapshot_generation,omitempty"`
}

func browserSessionResult(session browser.Session) browserSessionView {
	return browserSessionView{
		BrowserSessionID: session.ID, State: session.State, Target: session.Target,
		Profile: session.Profile, DryRun: session.DryRun,
		ControllerGeneration: session.ControllerGeneration, ExpiresAt: session.ExpiresAt,
		Tabs: []browserTabView{{
			TabID: session.TabID, SnapshotID: session.SnapshotID,
			SnapshotGeneration: session.SnapshotGeneration,
		}},
		Reason: session.SafeFailure,
	}
}

func (tool *BrowserSessionTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	if !tool.runtime.enabledForAgent(ToolAgentID(ctx)) {
		return browserErrorResult(
			"not_granted",
			"Browser access is not granted to this agent.",
			"use_an_authorized_agent",
		)
	}
	owner, err := browserOwnerFromContext(ctx)
	if err != nil {
		return browserToolError(err)
	}
	operation, _ := args["operation"].(string)
	var session browser.Session
	switch operation {
	case "open":
		target, targetOK := args["target"].(string)
		profile, profileOK := args["profile"].(string)
		if !targetOK || !profileOK || len(args) != 3 {
			return browserErrorResult(
				"invalid_request",
				"Open requires exactly target and profile.",
				"correct_arguments",
			)
		}
		session, err = tool.runtime.source.Open(ctx, browser.OpenRequest{
			Owner: owner, Target: target, Profile: profile,
		})
	case "status", "close":
		sessionID, ok := args["browser_session_id"].(string)
		if !ok || len(args) != 2 {
			return browserErrorResult(
				"invalid_request",
				"Status and close require exactly browser_session_id.",
				"correct_arguments",
			)
		}
		if operation == "status" {
			session, err = tool.runtime.source.Status(ctx, owner, sessionID)
		} else {
			session, err = tool.runtime.source.Close(ctx, owner, sessionID)
		}
	default:
		return browserErrorResult("invalid_request", "Unknown browser session operation.", "correct_arguments")
	}
	if err != nil {
		return browserToolError(err)
	}
	return tool.runtime.result(browserSessionResult(session))
}

func (*BrowserObserveTool) Name() string { return "browser_observe" }
func (*BrowserObserveTool) Description() string {
	return "Observe the current page as a bounded accessibility snapshot with scoped element references."
}

func (*BrowserObserveTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"browser_session_id": map[string]any{"type": "string"},
			"tab_id":             map[string]any{"type": "string"},
		},
		"required": []string{"browser_session_id"}, "additionalProperties": false,
	}
}

func (*BrowserObserveTool) ToolLoopSemantics() loopguard.Semantics {
	// Observe is page-read-only, but it advances snapshot authority and session
	// activity, so it is not runtime-idempotent.
	return loopguard.SemanticsMutating
}

type browserObservationView struct {
	BrowserSessionID   string                     `json:"browser_session_id"`
	TabID              string                     `json:"tab_id"`
	SnapshotID         string                     `json:"snapshot_id"`
	SnapshotGeneration uint64                     `json:"snapshot_generation"`
	URL                string                     `json:"url"`
	Origin             string                     `json:"origin"`
	Title              string                     `json:"title,omitempty"`
	Snapshot           string                     `json:"snapshot"`
	Tabs               []browserTabView           `json:"tabs"`
	PendingDialog      *browser.DialogObservation `json:"pending_dialog,omitempty"`
	Truncated          bool                       `json:"truncated"`
	Limits             browserObservationLimits   `json:"limits"`
}

type browserObservationLimits struct {
	SnapshotBytes int `json:"snapshot_bytes"`
	SnapshotRefs  int `json:"snapshot_refs"`
}

func (runtime *browserToolRuntime) observationResult(observation browser.Observation) browserObservationView {
	limits := runtime.config.Limits.Effective()
	return browserObservationView{
		BrowserSessionID: observation.SessionID, TabID: observation.TabID,
		SnapshotID: observation.SnapshotID, SnapshotGeneration: observation.SnapshotGeneration,
		URL: observation.URL, Origin: observation.Origin, Title: observation.Title,
		Snapshot: observation.Snapshot, PendingDialog: observation.PendingDialog,
		Tabs: []browserTabView{{
			TabID: observation.TabID, SnapshotID: observation.SnapshotID,
			SnapshotGeneration: observation.SnapshotGeneration,
		}},
		Truncated: observation.Truncated,
		Limits: browserObservationLimits{
			SnapshotBytes: limits.SnapshotBytes, SnapshotRefs: limits.SnapshotRefs,
		},
	}
}

func (tool *BrowserObserveTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	if !tool.runtime.enabledForAgent(ToolAgentID(ctx)) {
		return browserErrorResult(
			"not_granted",
			"Browser access is not granted to this agent.",
			"use_an_authorized_agent",
		)
	}
	owner, err := browserOwnerFromContext(ctx)
	if err != nil {
		return browserToolError(err)
	}
	sessionID, ok := args["browser_session_id"].(string)
	if !ok {
		return browserErrorResult("invalid_request", "browser_session_id is required.", "correct_arguments")
	}
	tabID, _ := args["tab_id"].(string)
	if tabID == "" {
		session, statusErr := tool.runtime.source.Status(ctx, owner, sessionID)
		if statusErr != nil {
			return browserToolError(statusErr)
		}
		tabID = session.TabID
	}
	observation, err := tool.runtime.source.Observe(ctx, owner, sessionID, tabID)
	if err != nil {
		return browserToolError(err)
	}
	return tool.runtime.result(tool.runtime.observationResult(observation))
}

func (*BrowserActTool) Name() string { return "browser_act" }
func (*BrowserActTool) Description() string {
	return "Prepare and execute exactly one fresh-reference browser action; risky effects suspend for durable human approval."
}

func (tool *BrowserActTool) Parameters() map[string]any {
	limits := config.BrowserLimitsConfig{}.Effective()
	if tool != nil && tool.runtime != nil {
		limits = tool.runtime.config.Limits.Effective()
	}
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"browser_session_id":  map[string]any{"type": "string"},
			"tab_id":              map[string]any{"type": "string"},
			"snapshot_id":         map[string]any{"type": "string"},
			"snapshot_generation": map[string]any{"type": "integer"},
			"action": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"kind": map[string]any{"type": "string", "enum": []string{
						"navigate", "click", "fill", "select", "press", "scroll", "dialog",
					}},
					"url": map[string]any{"type": "string"}, "ref": map[string]any{"type": "string"},
					"value":     map[string]any{"type": "string", "maxLength": limits.TextInputBytes},
					"key":       map[string]any{"type": "string"},
					"direction": map[string]any{"type": "string", "enum": []string{"up", "down"}},
					"amount":    map[string]any{"type": "integer"},
					"decision":  map[string]any{"type": "string", "enum": []string{"accept", "dismiss"}},
				},
				"required": []string{"kind"}, "additionalProperties": false,
			},
		},
		"required": []string{
			"browser_session_id", "tab_id", "snapshot_id", "snapshot_generation", "action",
		},
		"additionalProperties": false,
	}
}
func (*BrowserActTool) ToolLoopSemantics() loopguard.Semantics { return loopguard.SemanticsMutating }

func (tool *BrowserActTool) ApprovalArguments(ctx context.Context, args map[string]any) (map[string]any, error) {
	if !tool.runtime.enabledForAgent(ToolAgentID(ctx)) {
		return nil, &browserSafeDenialError{cause: browser.ErrDenied}
	}
	preparation, err := tool.prepare(ctx, args)
	if err != nil {
		return nil, &browserSafeDenialError{cause: err}
	}
	return map[string]any{
		"prepared_action_id": preparation.Approval.PreparedActionID,
		"action_hash":        preparation.Approval.ActionHash,
		"policy_revision":    preparation.Approval.PolicyRevision,
		"expires_at":         preparation.Approval.ExpiresAt,
		"preview":            browserApprovalSummary(preparation),
	}, nil
}

type browserActionResult struct {
	InvocationID string                  `json:"invocation_id"`
	Effect       browser.Effect          `json:"effect"`
	State        browser.InvocationState `json:"state"`
	Reason       string                  `json:"reason,omitempty"`
	Observation  *browserObservationView `json:"observation,omitempty"`
}

func (tool *BrowserActTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	if !tool.runtime.enabledForAgent(ToolAgentID(ctx)) {
		return browserErrorResult(
			"not_granted",
			"Browser access is not granted to this agent.",
			"use_an_authorized_agent",
		)
	}
	preparation, err := tool.prepare(ctx, args)
	if err != nil {
		return browserToolError(err)
	}
	if preparation.RequiresApproval &&
		!ToolApprovalContinuation(ctx) && !ToolApprovalBypass(ctx) {
		return &ToolResult{Silent: true, Suspension: &interactions.SuspensionRequest{
			Kind:          interactions.KindApproval,
			PromptSummary: browserApprovalSummary(preparation),
			Timeout:       time.Duration(tool.runtime.config.Limits.Effective().PreparedSeconds) * time.Second,
		}}
	}
	var approval *browser.ApprovalBinding
	if preparation.RequiresApproval {
		binding := preparation.Approval
		approval = &binding
	}
	owner, err := browserOwnerFromContext(ctx)
	if err != nil {
		return browserToolError(err)
	}
	invocation, err := tool.runtime.source.ExecuteAction(
		ctx, owner, preparation.Action.ID, approval,
	)
	if err != nil {
		if errors.Is(err, browser.ErrSnapshotInvalidation) || invocation.AcceptedAt != 0 {
			return browserPostActionStateError(
				invocation,
				errors.Is(err, browser.ErrSnapshotInvalidation),
			)
		}
		return browserToolError(err)
	}
	result := browserActionResult{
		InvocationID: invocation.ID, Effect: invocation.Effect,
		State: invocation.State, Reason: invocation.SafeFailure,
	}
	if invocation.State == browser.InvocationSucceeded {
		observation, observeErr := tool.runtime.source.Observe(
			ctx, owner, invocation.SessionID, preparation.Action.TabID,
		)
		if observeErr == nil {
			view := tool.runtime.observationResult(observation)
			result.Observation = &view
		}
	}
	return tool.runtime.result(result)
}

func browserPostActionStateError(invocation browser.Invocation, quarantined bool) *ToolResult {
	action, reason := "do_not_retry_check_session", "state_persistence_failed"
	if quarantined {
		action, reason = "do_not_retry_reopen_session", "session_quarantined"
	}
	encoded, _ := json.Marshal(map[string]any{
		"status":        "failed",
		"code":          "post_action_state_unavailable",
		"message":       "The browser action reached a terminal state, but fresh snapshot authority could not be persisted.",
		"action":        action,
		"invocation_id": invocation.ID,
		"effect":        invocation.Effect,
		"state":         invocation.State,
		"reason":        reason,
	})
	return ErrorResult(string(encoded))
}

func (tool *BrowserActTool) prepare(ctx context.Context, args map[string]any) (browser.Preparation, error) {
	owner, err := browserOwnerFromContext(ctx)
	if err != nil {
		return browser.Preparation{}, err
	}
	requestID, err := browserRequestID(ctx)
	if err != nil {
		return browser.Preparation{}, err
	}
	action, err := browserActionFromArgs(args["action"])
	if err != nil {
		return browser.Preparation{}, err
	}
	sessionID, sessionOK := args["browser_session_id"].(string)
	tabID, tabOK := args["tab_id"].(string)
	snapshotID, snapshotOK := args["snapshot_id"].(string)
	generation, generationOK := browserInteger(args["snapshot_generation"])
	if !sessionOK || !tabOK || !snapshotOK || !generationOK || generation < 1 {
		return browser.Preparation{}, browser.ErrInvalid
	}
	return tool.runtime.source.PrepareAction(ctx, browser.PrepareActionRequest{
		Owner: owner, RequestID: requestID, SessionID: sessionID, TabID: tabID,
		SnapshotID: snapshotID, SnapshotGeneration: uint64(generation), Action: action,
	})
}

func browserActionFromArgs(raw any) (browser.Action, error) {
	args, ok := raw.(map[string]any)
	if !ok {
		return browser.Action{}, browser.ErrInvalid
	}
	action := browser.Action{}
	kind, ok := args["kind"].(string)
	if !ok {
		return browser.Action{}, browser.ErrInvalid
	}
	action.Kind = browser.ActionKind(kind)
	action.URL, _ = args["url"].(string)
	action.Ref, _ = args["ref"].(string)
	action.Value, _ = args["value"].(string)
	if action.Kind == browser.ActionDialog {
		_, action.PromptProvided = args["value"]
	}
	action.Key, _ = args["key"].(string)
	action.Direction, _ = args["direction"].(string)
	action.Decision, _ = args["decision"].(string)
	amount, amountOK := browserInteger(args["amount"])
	if _, present := args["amount"]; present && !amountOK {
		return browser.Action{}, browser.ErrInvalid
	}
	action.Amount = amount
	return action, nil
}

func browserInteger(value any) (int, bool) {
	switch typed := value.(type) {
	case int:
		return typed, true
	case int64:
		return int(typed), int64(int(typed)) == typed
	case float64:
		integer := int(typed)
		return integer, typed == float64(integer)
	default:
		return 0, false
	}
}

func browserApprovalSummary(preparation browser.Preparation) string {
	action := preparation.Action
	origin := action.CurrentOrigin
	if action.DestinationOrigin != "" {
		origin = action.DestinationOrigin
	}
	target := ""
	if action.ElementRole != "" {
		target = " for " + action.ElementRole
		if action.ElementName != "" {
			target += fmt.Sprintf(" %q", action.ElementName)
		}
	}
	return fmt.Sprintf(
		"Allow browser %s action%s with %s effect on %s?",
		action.Action.Kind,
		target,
		action.Effect,
		origin,
	)
}

func browserOwnerFromContext(ctx context.Context) (browser.Owner, error) {
	actorID := strings.TrimSpace(ToolActorID(ctx))
	if actorID == "" {
		actorID = strings.TrimSpace(ToolSenderID(ctx))
	}
	agentID := strings.TrimSpace(ToolAgentID(ctx))
	sessionKey := strings.TrimSpace(ToolRouteSessionKey(ctx))
	if sessionKey == "" {
		sessionKey = strings.TrimSpace(ToolSessionKey(ctx))
	}
	executionID := strings.TrimSpace(ToolExecutionID(ctx))
	if actorID == "" || agentID == "" || sessionKey == "" || executionID == "" {
		return browser.Owner{}, errors.New("browser tool context is incomplete")
	}
	return browser.Owner{
		ActorID:     browserContextID("actor", actorID),
		AgentID:     browser.OpaqueAgentID(routing.NormalizeAgentID(agentID)),
		SessionKey:  browserContextID("session", sessionKey),
		ExecutionID: browserContextID("execution", executionID),
	}, nil
}

func browserRequestID(ctx context.Context) (string, error) {
	callID := strings.TrimSpace(ToolCallID(ctx))
	executionID := strings.TrimSpace(ToolExecutionID(ctx))
	if callID == "" || executionID == "" {
		return "", errors.New("browser tool call identity is incomplete")
	}
	return browserContextID("request", executionID+"\x00"+callID), nil
}

func browserContextID(prefix, value string) string {
	digest := sha256.Sum256([]byte(prefix + "\x00" + value))
	return prefix + "_" + hex.EncodeToString(digest[:16])
}

func (runtime *browserToolRuntime) result(value any) *ToolResult {
	encoded, err := json.Marshal(value)
	if err != nil {
		return browserErrorResult("result_unavailable", "Browser result could not be encoded.", "retry")
	}
	limit := runtime.config.Limits.Effective().ToolResultBytes
	if len(encoded) > limit {
		return browserErrorResult("result_too_large", "Browser result exceeded the configured limit.", "observe_again")
	}
	return NewToolResult(string(encoded))
}

type browserErrorView struct {
	Status  string `json:"status"`
	Code    string `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
}

func browserErrorResult(code, message, action string) *ToolResult {
	encoded, _ := json.Marshal(browserErrorView{
		Status: "denied", Code: code, Message: message, Action: action,
	})
	return ErrorResult(string(encoded))
}

func browserToolError(err error) *ToolResult {
	switch {
	case errors.Is(err, browser.ErrBusy):
		return browserErrorResult("profile_busy", "The browser profile is already in use.", "close_or_wait")
	case errors.Is(err, browser.ErrNotFound):
		return browserErrorResult("not_found", "The browser session or action was not found.", "open_session")
	case errors.Is(err, browser.ErrStale):
		return browserErrorResult("stale_snapshot", "Browser authority is stale.", "observe_again")
	case errors.Is(err, browser.ErrDenied):
		return browserErrorResult("policy_denied", "Browser policy denied the operation.", "choose_allowed_action")
	case errors.Is(err, browser.ErrApprovalRequired):
		return browserErrorResult("approval_required", "The browser action requires human approval.", "ask_operator")
	case errors.Is(err, browser.ErrInvalid):
		return browserErrorResult("invalid_request", "The browser request is invalid.", "correct_arguments")
	case errors.Is(err, browser.ErrConflict):
		return browserErrorResult("state_conflict", "Browser state changed concurrently.", "observe_again")
	case errors.Is(err, browser.ErrDriverIncompatible):
		return browserErrorResult("driver_incompatible", "The browser driver is incompatible.", "contact_operator")
	case errors.Is(err, browser.ErrWorkerUnavailable), errors.Is(err, browser.ErrDriverRejected):
		return browserErrorResult("driver_unavailable", "The browser driver is unavailable.", "retry_or_reopen")
	default:
		return browserErrorResult("runtime_unavailable", "Browser automation is unavailable.", "retry")
	}
}

type browserSafeDenialError struct{ cause error }

func (err *browserSafeDenialError) Error() string { return "browser approval preparation denied" }
func (err *browserSafeDenialError) Unwrap() error { return err.cause }
func (err *browserSafeDenialError) SafeApprovalDenialResult() *ToolResult {
	return browserToolError(err.cause)
}
