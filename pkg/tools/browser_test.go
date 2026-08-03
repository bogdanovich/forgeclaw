package tools

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/bogdanovich/mintclaw/pkg/browser"
	"github.com/bogdanovich/mintclaw/pkg/bus"
	"github.com/bogdanovich/mintclaw/pkg/config"
)

type fakeBrowserToolSource struct {
	available  bool
	open       browser.Session
	status     browser.Session
	observe    browser.Observation
	screenshot browser.ScreenshotArtifact
	prepare    browser.Preparation
	execute    browser.Invocation
	err        error
	executeErr error

	openRequest       browser.OpenRequest
	statusOwner       browser.Owner
	statusSessionID   string
	prepareRequest    browser.PrepareActionRequest
	screenshotRequest browser.ScreenshotRequest
	executeOwner      browser.Owner
	executePrepared   string
	executeApproval   *browser.ApprovalBinding
	prepareCalls      int
	executeCalls      int
	profileStatus     browser.ProfileAvailability
}

func (source *fakeBrowserToolSource) Available() bool { return source.available }

func (source *fakeBrowserToolSource) ProfileAvailability(
	_ context.Context,
	_ string,
	_ string,
) (browser.ProfileAvailability, error) {
	if source.err != nil {
		return browser.ProfileAvailability{}, source.err
	}
	if source.profileStatus.Status == "" {
		return browser.ProfileAvailability{Status: "ready"}, nil
	}
	return source.profileStatus, nil
}

func (source *fakeBrowserToolSource) Open(
	_ context.Context,
	request browser.OpenRequest,
) (browser.Session, error) {
	source.openRequest = request
	result := source.open
	result.Owner = request.Owner
	return result, source.err
}

func (source *fakeBrowserToolSource) Status(
	_ context.Context,
	owner browser.Owner,
	sessionID string,
) (browser.Session, error) {
	source.statusOwner = owner
	source.statusSessionID = sessionID
	return source.status, source.err
}

func (source *fakeBrowserToolSource) Close(
	_ context.Context,
	owner browser.Owner,
	sessionID string,
) (browser.Session, error) {
	source.statusOwner = owner
	source.statusSessionID = sessionID
	return source.status, source.err
}

func (source *fakeBrowserToolSource) Observe(
	_ context.Context,
	owner browser.Owner,
	sessionID string,
	tabID string,
) (browser.Observation, error) {
	source.statusOwner = owner
	source.statusSessionID = sessionID + ":" + tabID
	return source.observe, source.err
}

func (source *fakeBrowserToolSource) CaptureScreenshot(
	_ context.Context,
	request browser.ScreenshotRequest,
) (browser.ScreenshotArtifact, error) {
	source.screenshotRequest = request
	source.statusOwner = request.Owner
	source.statusSessionID = request.SessionID + ":" + request.TabID + ":screenshot"
	return source.screenshot, source.err
}

func (source *fakeBrowserToolSource) PrepareAction(
	_ context.Context,
	request browser.PrepareActionRequest,
) (browser.Preparation, error) {
	source.prepareCalls++
	source.prepareRequest = request
	return source.prepare, source.err
}

func (source *fakeBrowserToolSource) ExecuteAction(
	_ context.Context,
	owner browser.Owner,
	preparedID string,
	approval *browser.ApprovalBinding,
) (browser.Invocation, error) {
	source.executeCalls++
	source.executeOwner = owner
	source.executePrepared = preparedID
	if approval != nil {
		copy := *approval
		source.executeApproval = &copy
	}
	if source.executeErr != nil {
		return source.execute, source.executeErr
	}
	return source.execute, source.err
}

func browserToolTestConfig() *config.Config {
	cfg := config.DefaultConfig()
	cfg.Tools.Browser = config.BrowserToolsConfig{
		Enabled: true,
		Agents:  []string{"browser"},
		Targets: map[string]config.BrowserTargetConfig{
			"gateway": {
				Enabled: true, Driver: config.BrowserDriverPlaywrightMCP, DriverServer: "playwright",
				Profiles: map[string]config.BrowserProfileConfig{
					"managed": {
						Enabled: true, Mode: config.BrowserProfileManaged, DryRun: true,
						AllowedOrigins: []string{"https://example.com"},
					},
				},
			},
		},
	}
	return cfg
}

func browserToolTestContext() context.Context {
	ctx := WithToolInboundMetadata(context.Background(), bus.InboundContext{
		SenderID: "telegram-user-42", ActorID: "person:42",
	})
	ctx = WithToolSessionContext(ctx, "browser", "history-session", nil)
	ctx = WithToolRouteSessionKey(ctx, "telegram:primary:chat:42")
	ctx = WithToolCallID(ctx, "provider-call/1")
	return WithToolExecutionIdentity(ctx, "/workspace/private", "execution/1")
}

func decodeBrowserToolResult(t *testing.T, result *ToolResult, target any) {
	t.Helper()
	if result == nil || result.IsError {
		t.Fatalf("result = %#v, want success", result)
	}
	if err := json.Unmarshal([]byte(result.ContentForLLM()), target); err != nil {
		t.Fatalf("decode result: %v; content = %q", err, result.ContentForLLM())
	}
}

func TestBrowserTargetsIsScopedAndSideEffectFree(t *testing.T) {
	source := &fakeBrowserToolSource{available: true}
	tool := NewBrowserTargetsTool(browserToolTestConfig(), source)
	if !tool.ToolEnabledForAgent("browser") || tool.ToolEnabledForAgent("main") {
		t.Fatal("browser target tool agent scope is incorrect")
	}
	var result browserTargetResult
	decodeBrowserToolResult(t, tool.Execute(browserToolTestContext(), nil), &result)
	if len(result.Targets) != 1 || result.Targets[0].Target != "gateway" ||
		result.Targets[0].Status != "ready" || len(result.Targets[0].Profiles) != 1 ||
		result.Targets[0].Profiles[0].NetworkMode != config.BrowserNetworkExactOrigins ||
		!result.Targets[0].Profiles[0].DryRun || !result.Targets[0].Features.Screenshot ||
		result.Targets[0].Limits.ScreenshotBytes != config.BrowserMaxScreenshotBytes ||
		source.openRequest.Target != "" {
		t.Fatalf("browser targets = %#v", result)
	}

	other := WithToolSessionContext(browserToolTestContext(), "main", "history-session", nil)
	denied := tool.Execute(other, nil)
	if denied == nil || !denied.IsError || !strings.Contains(denied.ContentForLLM(), `"code":"not_granted"`) {
		t.Fatalf("ungranted result = %#v", denied)
	}
}

func TestBrowserTargetsReportsExplicitAnyHTTPMode(t *testing.T) {
	cfg := browserToolTestConfig()
	target := cfg.Tools.Browser.Targets[config.BrowserDefaultTarget]
	profile := target.Profiles[config.BrowserDefaultProfile]
	profile.NetworkMode = config.BrowserNetworkAnyHTTP
	profile.AllowedOrigins = nil
	target.Profiles[config.BrowserDefaultProfile] = profile
	cfg.Tools.Browser.Targets[config.BrowserDefaultTarget] = target

	var result browserTargetResult
	decodeBrowserToolResult(
		t,
		NewBrowserTargetsTool(cfg, &fakeBrowserToolSource{available: true}).Execute(
			browserToolTestContext(), nil,
		),
		&result,
	)
	if len(result.Targets) != 1 || len(result.Targets[0].Profiles) != 1 ||
		result.Targets[0].Profiles[0].NetworkMode != config.BrowserNetworkAnyHTTP {
		t.Fatalf("browser targets = %#v", result)
	}
}

func TestBrowserTargetsReportsBrokerProfileAvailability(t *testing.T) {
	source := &fakeBrowserToolSource{
		available: true,
		profileStatus: browser.ProfileAvailability{
			Status: "busy", Reason: "profile_busy",
		},
	}
	var result browserTargetResult
	decodeBrowserToolResult(
		t,
		NewBrowserTargetsTool(browserToolTestConfig(), source).Execute(browserToolTestContext(), nil),
		&result,
	)
	if len(result.Targets) != 1 || result.Targets[0].Status != "busy" ||
		result.Targets[0].Reason != "profile_busy" || result.Targets[0].Profiles[0].Status != "busy" {
		t.Fatalf("busy targets = %#v", result)
	}
}

func TestBrowserSessionUsesOpaqueContextOwnerAndExactOperations(t *testing.T) {
	source := &fakeBrowserToolSource{available: true, open: browser.Session{
		ID: "browser_session_1", State: browser.SessionReady, Target: "gateway", Profile: "managed",
		DryRun: true, ControllerGeneration: 1, TabID: "tab_primary", ExpiresAt: 100,
	}}
	tool := NewBrowserSessionTool(browserToolTestConfig(), source)
	var result browserSessionView
	decodeBrowserToolResult(t, tool.Execute(browserToolTestContext(), map[string]any{
		"operation": "open", "target": "gateway", "profile": "managed",
	}), &result)
	if result.BrowserSessionID != "browser_session_1" || source.openRequest.Target != "gateway" ||
		source.openRequest.Profile != "managed" {
		t.Fatalf("session result = %#v; request = %#v", result, source.openRequest)
	}
	owner := source.openRequest.Owner
	if owner.Validate() != nil || owner.ActorID == "person:42" ||
		!strings.HasPrefix(owner.ActorID, "actor_") || !strings.HasPrefix(owner.ExecutionID, "execution_") {
		t.Fatalf("opaque owner = %#v", owner)
	}
	invalid := tool.Execute(browserToolTestContext(), map[string]any{
		"operation": "open", "target": "gateway", "profile": "managed", "browser_session_id": "extra",
	})
	if invalid == nil || !invalid.IsError || source.openRequest.Target != "gateway" {
		t.Fatalf("invalid open result = %#v", invalid)
	}
}

func TestBrowserObserveResolvesDefaultTabAndReturnsBoundedProjection(t *testing.T) {
	source := &fakeBrowserToolSource{
		available: true,
		status:    browser.Session{ID: "browser_session_1", State: browser.SessionReady, TabID: "tab_primary"},
		observe: browser.Observation{
			SessionID: "browser_session_1", TabID: "tab_primary", SnapshotID: "snapshot_1",
			SnapshotGeneration: 3, URL: "https://example.com/listing", Origin: "https://example.com",
			Title: "Listing", Snapshot: "- button Publish [ref=element_1]",
		},
	}
	tool := NewBrowserObserveTool(browserToolTestConfig(), source)
	var result browserObservationView
	decodeBrowserToolResult(t, tool.Execute(browserToolTestContext(), map[string]any{
		"browser_session_id": "browser_session_1",
	}), &result)
	if result.SnapshotID != "snapshot_1" || result.SnapshotGeneration != 3 || result.Truncated ||
		source.statusSessionID != "browser_session_1:tab_primary" {
		t.Fatalf("observation = %#v; call = %q", result, source.statusSessionID)
	}
}

func TestBrowserObserveDeliversEscapedTruncatedSnapshotWithinToolLimit(t *testing.T) {
	cfg := browserToolTestConfig()
	cfg.Tools.Browser.Limits.ToolResultBytes = config.BrowserToolResultEnvelopeBytes + 512
	snapshot := `- text "` + strings.Repeat(`quoted\\path"`, 12)
	source := &fakeBrowserToolSource{
		available: true,
		status:    browser.Session{ID: "browser_session_1", State: browser.SessionReady, TabID: "tab_primary"},
		observe: browser.Observation{
			SessionID: "browser_session_1", TabID: "tab_primary", SnapshotID: "snapshot_1",
			SnapshotGeneration: 3, URL: "https://example.com/listing", Origin: "https://example.com",
			Title: "Listing", Snapshot: snapshot, Truncated: true,
		},
	}
	result := NewBrowserObserveTool(cfg, source).Execute(browserToolTestContext(), map[string]any{
		"browser_session_id": "browser_session_1",
	})
	var observation browserObservationView
	decodeBrowserToolResult(t, result, &observation)
	if !observation.Truncated || observation.Snapshot != snapshot ||
		len(result.ContentForLLM()) > cfg.Tools.Browser.Limits.ToolResultBytes {
		t.Fatalf("escaped observation = %#v; encoded bytes = %d", observation, len(result.ContentForLLM()))
	}
}

func TestBrowserObserveCapturesAndDeliversOpaqueScreenshotArtifact(t *testing.T) {
	source := &fakeBrowserToolSource{
		available: true,
		status:    browser.Session{ID: "browser_session_1", State: browser.SessionReady, TabID: "tab_primary"},
		observe: browser.Observation{
			SessionID: "browser_session_1", TabID: "tab_primary", SnapshotID: "snapshot_1",
			SnapshotGeneration: 3, URL: "https://example.com/listing", Origin: "https://example.com",
			Title: "Listing", Snapshot: "- heading Listing",
		},
		screenshot: browser.ScreenshotArtifact{
			Ref: "transfer-artifact://opaque", Kind: "screenshot", ContentType: "image/png",
			Filename: "browser-screenshot.png", Size: 1024, SHA256: strings.Repeat("a", 64),
			ExpiresAt: 200, SessionID: "browser_session_1", TabID: "tab_primary",
			SnapshotID: "snapshot_1", SnapshotGeneration: 3,
			DeliveryState: "claimed", MediaRef: "media://opaque",
		},
	}
	result := NewBrowserObserveTool(browserToolTestConfig(), source).Execute(
		browserToolTestContext(),
		map[string]any{"browser_session_id": "browser_session_1", "screenshot": true},
	)
	var observation browserObservationView
	if result == nil || result.IsError {
		t.Fatalf("screenshot result = %#v", result)
	}
	if err := json.Unmarshal([]byte(result.ForLLM), &observation); err != nil {
		t.Fatalf("decode screenshot result: %v; content = %q", err, result.ForLLM)
	}
	if observation.Artifact == nil || observation.Artifact.Ref != "transfer-artifact://opaque" ||
		observation.Artifact.SnapshotID != "snapshot_1" ||
		observation.Artifact.MediaRef != "" || len(result.Media) != 1 || result.Media[0] != "media://opaque" ||
		source.screenshotRequest.SnapshotID != "snapshot_1" || source.screenshotRequest.RequestID == "" ||
		strings.Contains(result.ForLLM, "media://opaque") ||
		strings.Contains(result.ForLLM, "iVBOR") {
		t.Fatalf(
			"screenshot observation = %#v; result = %#v; request = %#v",
			observation,
			result,
			source.screenshotRequest,
		)
	}

	source.screenshot.DeliveryState = "already_claimed"
	duplicate := NewBrowserObserveTool(browserToolTestConfig(), source).Execute(
		browserToolTestContext(),
		map[string]any{"browser_session_id": "browser_session_1", "screenshot": true},
	)
	if duplicate.IsError || len(duplicate.Media) != 0 {
		t.Fatalf("duplicate screenshot result = %#v", duplicate)
	}
}

func TestBrowserActSuspendsAndResumesWithPreparedAuthority(t *testing.T) {
	binding := browser.ApprovalBinding{
		PreparedActionID: "prepared_1", ActionHash: strings.Repeat("a", 64),
		PolicyRevision: "policy_1", ExpiresAt: 200,
	}
	preparation := browser.Preparation{
		Action: browser.PreparedAction{
			ID: "prepared_1", TabID: "tab_primary", CurrentOrigin: "https://example.com",
			Action: browser.Action{Kind: browser.ActionClick, Ref: "element_1"},
			Effect: browser.EffectExternalCommit,
		},
		Approval: binding, RequiresApproval: true,
	}
	source := &fakeBrowserToolSource{
		available: true, prepare: preparation,
		execute: browser.Invocation{
			ID: "invocation_1", SessionID: "browser_session_1",
			Effect: browser.EffectExternalCommit, State: browser.InvocationSucceeded,
		},
		observe: browser.Observation{
			SessionID: "browser_session_1", TabID: "tab_primary", SnapshotID: "snapshot_2",
			SnapshotGeneration: 4, URL: "https://example.com/done", Origin: "https://example.com",
			Snapshot: "- status Published", Truncated: true,
		},
	}
	tool := NewBrowserActTool(browserToolTestConfig(), source)
	args := map[string]any{
		"browser_session_id": "browser_session_1", "tab_id": "tab_primary",
		"snapshot_id": "snapshot_1", "snapshot_generation": 3,
		"action": map[string]any{"kind": "click", "ref": "element_1"},
	}
	approval, err := tool.ApprovalArguments(browserToolTestContext(), args)
	if err != nil || approval["prepared_action_id"] != "prepared_1" || approval["action_hash"] != binding.ActionHash ||
		approval["preview"] != "Allow browser click action with external_commit effect on https://example.com?" {
		t.Fatalf("approval = %#v, error = %v", approval, err)
	}
	suspended := tool.Execute(browserToolTestContext(), args)
	if suspended == nil || suspended.Suspension == nil || source.executeCalls != 0 ||
		!strings.Contains(suspended.Suspension.PromptSummary, "external_commit") {
		t.Fatalf("suspended result = %#v; execute calls = %d", suspended, source.executeCalls)
	}
	resumeCtx := WithToolApprovalContinuation(browserToolTestContext(), true)
	var result browserActionResult
	decodeBrowserToolResult(t, tool.Execute(resumeCtx, args), &result)
	if result.InvocationID != "invocation_1" || result.Observation == nil ||
		!result.Observation.Truncated ||
		source.executePrepared != "prepared_1" || source.executeApproval == nil ||
		*source.executeApproval != binding || source.prepareRequest.RequestID == "" ||
		source.prepareRequest.Owner != source.executeOwner {
		t.Fatalf("action result = %#v; source = %#v", result, source)
	}
}

func TestBrowserActApprovalPreparationFailsWithSafeDenial(t *testing.T) {
	source := &fakeBrowserToolSource{available: true, err: errors.New("PRIVATE driver failure")}
	tool := NewBrowserActTool(browserToolTestConfig(), source)
	_, err := tool.ApprovalArguments(browserToolTestContext(), map[string]any{
		"browser_session_id": "browser_session_1", "tab_id": "tab_primary",
		"snapshot_id": "snapshot_1", "snapshot_generation": 1,
		"action": map[string]any{"kind": "scroll", "direction": "down", "amount": 1},
	})
	result, safe := SafeApprovalDenialResult(err)
	if !safe || result == nil || !result.IsError || strings.Contains(result.ContentForLLM(), "PRIVATE") {
		t.Fatalf("safe denial = %#v, safe = %t, error = %v", result, safe, err)
	}
}

func TestBrowserActSurfacesTerminalPostActionStateFailure(t *testing.T) {
	source := &fakeBrowserToolSource{
		available: true,
		prepare: browser.Preparation{Action: browser.PreparedAction{
			ID: "prepared_1", TabID: "tab_primary", CurrentOrigin: "https://example.com",
			Action: browser.Action{Kind: browser.ActionScroll, Direction: "down", Amount: 1},
			Effect: browser.EffectRead,
		}},
		execute: browser.Invocation{
			ID: "invocation_1", SessionID: "browser_session_1",
			Effect: browser.EffectRead, State: browser.InvocationSucceeded,
		},
		executeErr: browser.ErrSnapshotInvalidation,
	}
	result := NewBrowserActTool(browserToolTestConfig(), source).Execute(
		browserToolTestContext(),
		map[string]any{
			"browser_session_id": "browser_session_1", "tab_id": "tab_primary",
			"snapshot_id": "snapshot_1", "snapshot_generation": 1,
			"action": map[string]any{"kind": "scroll", "direction": "down", "amount": 1},
		},
	)
	if result == nil || !result.IsError ||
		!strings.Contains(result.ContentForLLM(), `"code":"post_action_state_unavailable"`) ||
		!strings.Contains(result.ContentForLLM(), `"state":"succeeded"`) ||
		!strings.Contains(result.ContentForLLM(), `"action":"do_not_retry_reopen_session"`) {
		t.Fatalf("post-action state result = %#v", result)
	}
}

func TestBrowserActPreservesDryRunPolicyDenial(t *testing.T) {
	source := &fakeBrowserToolSource{
		available: true,
		prepare: browser.Preparation{Action: browser.PreparedAction{
			ID: "prepared_1", TabID: "tab_primary", CurrentOrigin: "https://example.com",
			Action: browser.Action{Kind: browser.ActionClick, Ref: "element_1"},
			Effect: browser.EffectExternalCommit,
		}},
		execute: browser.Invocation{
			ID: "invocation_1", SessionID: "browser_session_1",
			Effect: browser.EffectExternalCommit, State: browser.InvocationCanceled,
			SafeFailure: "dry_run_denied",
		},
		executeErr: browser.ErrDenied,
	}
	result := NewBrowserActTool(browserToolTestConfig(), source).Execute(
		WithToolApprovalContinuation(browserToolTestContext(), true),
		map[string]any{
			"browser_session_id": "browser_session_1", "tab_id": "tab_primary",
			"snapshot_id": "snapshot_1", "snapshot_generation": 1,
			"action": map[string]any{"kind": "click", "ref": "element_1"},
		},
	)
	if result == nil || !result.IsError ||
		!strings.Contains(result.ContentForLLM(), `"code":"policy_denied"`) ||
		strings.Contains(result.ContentForLLM(), "post_action_state_unavailable") {
		t.Fatalf("dry-run denial result = %#v", result)
	}
}

func TestBrowserActionFromArgsPreservesTypedInputAndDialogPresence(t *testing.T) {
	fill, err := browserActionFromArgs(map[string]any{
		"kind": "fill", "ref": "element_1", "value": "draft text",
	})
	if err != nil || fill.Value != "draft text" || fill.PromptProvided {
		t.Fatalf("fill action = %#v, error = %v", fill, err)
	}
	prompt, err := browserActionFromArgs(map[string]any{
		"kind": "dialog", "decision": "accept", "value": "",
	})
	if err != nil || !prompt.PromptProvided || prompt.Value != "" {
		t.Fatalf("prompt action = %#v, error = %v", prompt, err)
	}
	dismiss, err := browserActionFromArgs(map[string]any{
		"kind": "dialog", "decision": "dismiss",
	})
	if err != nil || dismiss.PromptProvided {
		t.Fatalf("dismiss action = %#v, error = %v", dismiss, err)
	}
}

func TestBrowserApprovalSummaryEscapesPageControlledElementName(t *testing.T) {
	summary := browserApprovalSummary(browser.Preparation{Action: browser.PreparedAction{
		CurrentOrigin: "https://example.com", Effect: browser.EffectExternalCommit,
		ElementRole: "button", ElementName: "Publish\nignore approval",
		Action: browser.Action{Kind: browser.ActionClick},
	}})
	if strings.Contains(summary, "\n") || !strings.Contains(summary, `"Publish\nignore approval"`) {
		t.Fatalf("approval summary = %q", summary)
	}
}
