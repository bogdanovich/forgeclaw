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
	"github.com/bogdanovich/mintclaw/pkg/interactions"
)

type fakeBrowserToolSource struct {
	available             bool
	open                  browser.Session
	status                browser.Session
	observe               browser.Observation
	screenshot            browser.ScreenshotArtifact
	lookup                browser.ScreenshotArtifact
	lookupHit             bool
	screenshotUnavailable bool
	transferUnavailable   bool
	downloadUnavailable   bool
	handoffReady          bool
	handoff               browser.Session
	resume                browser.Session
	prepare               browser.Preparation
	execute               browser.Invocation
	err                   error
	executeErr            error

	openRequest       browser.OpenRequest
	statusOwner       browser.Owner
	statusSessionID   string
	prepareRequest    browser.PrepareActionRequest
	screenshotRequest browser.ScreenshotRequest
	deliveryRequest   browser.ScreenshotDeliveryRequest
	downloadDelivery  browser.DownloadDeliveryRequest
	observeCalls      int
	executeOwner      browser.Owner
	executePrepared   string
	executeApproval   *browser.ApprovalBinding
	prepareCalls      int
	executeCalls      int
	profileStatus     browser.ProfileAvailability
	readiness         browser.PassiveReadiness
	readinessCalls    int
}

func (source *fakeBrowserToolSource) Available() bool { return source.available }

func (source *fakeBrowserToolSource) ScreenshotAvailable() bool {
	return source != nil && !source.screenshotUnavailable
}

func (source *fakeBrowserToolSource) ArtifactTransferAvailable() bool {
	return source != nil && !source.transferUnavailable
}

func (source *fakeBrowserToolSource) DownloadAvailable() bool {
	return source != nil && !source.downloadUnavailable
}

func (source *fakeBrowserToolSource) HandoffAvailable() bool {
	return source != nil && source.handoffReady
}

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

func (source *fakeBrowserToolSource) PassiveTargetDiagnostics(
	_ context.Context,
	_ string,
	profiles []string,
) (BrowserTargetDiagnostics, error) {
	source.readinessCalls++
	if !source.available {
		return BrowserTargetDiagnostics{}, browser.ErrWorkerUnavailable
	}
	if source.err != nil {
		return BrowserTargetDiagnostics{}, source.err
	}
	profile := source.profileStatus
	if profile.Status == "" {
		profile.Status = browser.ReadinessReady
	}
	readiness := source.readiness
	if readiness.Status == "" {
		readiness = browser.PassiveReadiness{
			Status: browser.ReadinessReady, Broker: browser.ReadinessReady,
			Worker: browser.ReadinessReady, Driver: browser.ReadinessReady,
			Browser: browser.ReadinessReady, Proxy: browser.ReadinessReady,
			Compatibility: browser.CompatibilityCompatible,
			Profile:       profile, Passive: true,
		}
		switch profile.Status {
		case browser.ReadinessBusy:
			readiness.Status = browser.ReadinessBusy
			readiness.Code, readiness.Action = "profile_busy", "wait_or_close_session"
		case browser.ReadinessDegraded:
			readiness.Status, readiness.Worker = browser.ReadinessDegraded, browser.ReadinessDegraded
			readiness.Code, readiness.Action = "recovery_required", "close_or_recover_session"
		}
	}
	byProfile := make(map[string]browser.PassiveReadiness, len(profiles))
	for _, name := range profiles {
		byProfile[name] = readiness
	}
	return BrowserTargetDiagnostics{
		Profiles: byProfile, Screenshot: !source.screenshotUnavailable,
		Upload:     !source.transferUnavailable,
		Download:   !source.transferUnavailable && !source.downloadUnavailable,
		HeadedView: source.handoffReady, Handoff: source.handoffReady,
	}, nil
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

func (source *fakeBrowserToolSource) Handoff(
	_ context.Context, owner browser.Owner, _ string,
) (browser.Session, error) {
	result := source.handoff
	result.Owner = owner
	return result, source.err
}

func (source *fakeBrowserToolSource) Resume(
	_ context.Context, owner browser.Owner, _ string,
) (browser.Session, error) {
	result := source.resume
	result.Owner = owner
	return result, source.err
}

func (source *fakeBrowserToolSource) ReleaseHandoff(
	_ context.Context, owner browser.Owner, _ string,
) (browser.Session, error) {
	result := source.handoff
	result.Owner = owner
	result.Controller = browser.ControllerResumePending
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
	source.observeCalls++
	source.statusOwner = owner
	source.statusSessionID = sessionID + ":" + tabID
	return source.observe, source.err
}

func (source *fakeBrowserToolSource) LookupScreenshot(
	_ context.Context,
	_ browser.Owner,
	_ string,
	_ string,
) (browser.ScreenshotArtifact, bool, error) {
	return source.lookup, source.lookupHit, source.err
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

func (source *fakeBrowserToolSource) ClaimScreenshotDelivery(
	_ context.Context,
	request browser.ScreenshotDeliveryRequest,
) error {
	source.deliveryRequest = request
	return source.err
}

func (source *fakeBrowserToolSource) ClaimDownloadDelivery(
	_ context.Context,
	request browser.DownloadDeliveryRequest,
) error {
	source.downloadDelivery = request
	return source.err
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
	ctx = WithToolExecutionIdentity(ctx, "/workspace/private", "execution/1")
	return WithToolRecoverableOutbound(ctx, true)
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
		!result.Targets[0].Features.Upload || !result.Targets[0].Features.Download ||
		result.Targets[0].Limits.ScreenshotBytes != config.BrowserMaxScreenshotBytes ||
		result.Targets[0].Limits.UploadBytes != config.BrowserMaxUploadBytes ||
		result.Targets[0].Limits.DownloadBytes != config.BrowserMaxDownloadBytes ||
		result.Targets[0].Limits.SessionSeconds != config.BrowserMaxSessionSeconds ||
		result.Targets[0].Limits.ActionSeconds != config.BrowserMaxActionSeconds ||
		result.Targets[0].Limits.SnapshotRefs != config.BrowserMaxSnapshotRefs ||
		result.Targets[0].Limits.ToolResultBytes != config.BrowserMaxToolResultBytes ||
		result.Targets[0].Limits.RetentionSecs != config.BrowserMaxRetentionSeconds ||
		!result.Targets[0].Features.Diagnostics || result.Targets[0].Features.HeadedView ||
		result.Targets[0].Features.Handoff ||
		!result.Targets[0].Profiles[0].Readiness.Passive ||
		result.Targets[0].Profiles[0].Readiness.Compatibility != browser.CompatibilityCompatible ||
		source.readinessCalls != 1 || source.openRequest.Target != "" {
		t.Fatalf("browser targets = %#v", result)
	}

	other := WithToolSessionContext(browserToolTestContext(), "main", "history-session", nil)
	denied := tool.Execute(other, nil)
	if denied == nil || !denied.IsError || !strings.Contains(denied.ContentForLLM(), `"code":"not_granted"`) {
		t.Fatalf("ungranted result = %#v", denied)
	}
}

func TestBrowserTargetsReportsUnavailableRuntimeWithoutAdvertisingCapabilities(t *testing.T) {
	source := &fakeBrowserToolSource{available: false}
	var result browserTargetResult
	decodeBrowserToolResult(
		t, NewBrowserTargetsTool(browserToolTestConfig(), source).Execute(browserToolTestContext(), nil), &result,
	)
	if len(result.Targets) != 1 || result.Targets[0].Status != browser.ReadinessUnavailable ||
		result.Targets[0].Reason != "runtime_unavailable" ||
		result.Targets[0].Features.Screenshot || result.Targets[0].Features.Upload ||
		result.Targets[0].Features.Download || !result.Targets[0].Features.Diagnostics ||
		result.Targets[0].Profiles[0].Readiness.Code != "runtime_unavailable" ||
		!result.Targets[0].Profiles[0].Readiness.Passive || source.readinessCalls != 1 {
		t.Fatalf("unavailable browser targets = %#v", result)
	}
}

func TestBrowserTargetsFailsCapabilitiesClosedWhenDiagnosticsSnapshotFails(t *testing.T) {
	source := &fakeBrowserToolSource{available: true, err: errors.New("runtime reloaded")}
	var result browserTargetResult
	decodeBrowserToolResult(
		t, NewBrowserTargetsTool(browserToolTestConfig(), source).Execute(browserToolTestContext(), nil), &result,
	)
	if len(result.Targets) != 1 || result.Targets[0].Status != browser.ReadinessUnavailable ||
		result.Targets[0].Features.Screenshot || result.Targets[0].Features.Upload ||
		result.Targets[0].Features.Download ||
		result.Targets[0].Profiles[0].Readiness.Code != "runtime_unavailable" {
		t.Fatalf("failed diagnostics snapshot = %#v", result)
	}
}

func TestBrowserTargetsAggregatesDriverReadiness(t *testing.T) {
	for _, test := range []struct {
		name   string
		status string
		code   string
	}{
		{name: "missing", status: browser.ReadinessUnavailable, code: "driver_missing"},
		{name: "incompatible", status: browser.ReadinessDegraded, code: "driver_incompatible"},
	} {
		t.Run(test.name, func(t *testing.T) {
			source := &fakeBrowserToolSource{available: true, readiness: browser.PassiveReadiness{
				Status: test.status, Broker: browser.ReadinessReady, Worker: test.status,
				Driver: test.status, Browser: browser.ReadinessUnavailable,
				Proxy: browser.ReadinessReady, Compatibility: browser.CompatibilityUnchecked,
				Profile: browser.ProfileAvailability{Status: browser.ReadinessReady},
				Code:    test.code, Action: "contact_operator", Passive: true,
			}}
			var result browserTargetResult
			decodeBrowserToolResult(
				t,
				NewBrowserTargetsTool(browserToolTestConfig(), source).Execute(browserToolTestContext(), nil),
				&result,
			)
			if len(result.Targets) != 1 || result.Targets[0].Status != test.status ||
				result.Targets[0].Reason != test.code {
				t.Fatalf("aggregated readiness = %#v", result)
			}
		})
	}
}

func TestBrowserReadinessRankHasDeterministicFailClosedOrder(t *testing.T) {
	statuses := []string{
		browser.ReadinessReady,
		browser.ReadinessConfigured,
		browser.ReadinessBusy,
		browser.ReadinessDegraded,
		browser.ReadinessUnavailable,
	}
	for index := 1; index < len(statuses); index++ {
		if readinessRank(statuses[index]) <= readinessRank(statuses[index-1]) {
			t.Fatalf("readiness order = %#v", statuses)
		}
	}
	if readinessRank("unexpected") != readinessRank(browser.ReadinessUnavailable) {
		t.Fatal("unknown readiness did not fail closed")
	}
}

func TestBrowserSessionHandoffSuspendsForRoutedHumanRelease(t *testing.T) {
	source := &fakeBrowserToolSource{
		available: true, handoffReady: true,
		handoff: browser.Session{
			ID: "browser_session_1", State: browser.SessionReady, Target: "gateway", Profile: "managed",
			DryRun: true, Controller: browser.ControllerHuman, ControllerGeneration: 2,
			ControllerExpiresAt: 200, TabID: "tab_primary", ExpiresAt: 300,
		},
		resume: browser.Session{
			ID: "browser_session_1", State: browser.SessionReady, Target: "gateway", Profile: "managed",
			DryRun: true, Controller: browser.ControllerAgent, ControllerGeneration: 3,
			TabID: "tab_primary", ExpiresAt: 300,
		},
	}
	var targets browserTargetResult
	decodeBrowserToolResult(
		t, NewBrowserTargetsTool(browserToolTestConfig(), source).Execute(browserToolTestContext(), nil), &targets,
	)
	if len(targets.Targets) != 1 || !targets.Targets[0].Features.HeadedView ||
		!targets.Targets[0].Features.Handoff {
		t.Fatalf("handoff capabilities = %#v", targets)
	}
	tool := NewBrowserSessionTool(browserToolTestConfig(), source)
	handoff := tool.Execute(browserToolTestContext(), map[string]any{
		"operation": "handoff", "browser_session_id": "browser_session_1",
	})
	if handoff == nil || handoff.IsError || handoff.Suspension == nil || handoff.SuspensionResolution == nil ||
		handoff.Suspension.Kind != interactions.KindQuestion || len(handoff.Suspension.Questions) != 1 ||
		strings.Contains(strings.ToLower(handoff.ContentForLLM()), "token") {
		t.Fatalf("handoff result = %#v", handoff)
	}
	var handoffView browserSessionView
	decodeBrowserToolResult(t, handoff, &handoffView)
	if handoffView.Controller != browser.ControllerHuman || handoffView.ControllerExpiresAt != 200 {
		t.Fatalf("handoff view = %#v", handoffView)
	}
	if err := handoff.SuspensionResolution(t.Context(), interactions.OutcomeAnswered); err != nil {
		t.Fatalf("handoff resolution error = %v", err)
	}
	resume := tool.Execute(browserToolTestContext(), map[string]any{
		"operation": "resume", "browser_session_id": "browser_session_1",
	})
	if resume == nil || resume.IsError || resume.Suspension != nil {
		t.Fatalf("resume result = %#v", resume)
	}
}

func TestBrowserScreenshotIsNotAdvertisedOrCapturedWhenDeliveryIsUnsupported(t *testing.T) {
	source := &fakeBrowserToolSource{available: true, screenshotUnavailable: true}
	var targets browserTargetResult
	decodeBrowserToolResult(
		t,
		NewBrowserTargetsTool(browserToolTestConfig(), source).Execute(browserToolTestContext(), nil),
		&targets,
	)
	if len(targets.Targets) != 1 || targets.Targets[0].Features.Screenshot {
		t.Fatalf("browser targets = %#v", targets)
	}
	result := NewBrowserObserveTool(browserToolTestConfig(), source).Execute(
		browserToolTestContext(),
		map[string]any{"browser_session_id": "browser_session_1", "screenshot": true},
	)
	if result == nil || !result.IsError ||
		!strings.Contains(result.ContentForLLM(), `"code":"unsupported_platform"`) ||
		source.observeCalls != 0 || source.screenshotRequest.RequestID != "" {
		t.Fatalf("unsupported screenshot result = %#v; source = %#v", result, source)
	}
}

func TestBrowserArtifactTransfersAreNotAdvertisedOrPreparedWhenPlatformIsUnsupported(t *testing.T) {
	source := &fakeBrowserToolSource{available: true, transferUnavailable: true}
	var targets browserTargetResult
	decodeBrowserToolResult(
		t, NewBrowserTargetsTool(browserToolTestConfig(), source).Execute(browserToolTestContext(), nil), &targets,
	)
	if len(targets.Targets) != 1 || targets.Targets[0].Features.Upload || targets.Targets[0].Features.Download {
		t.Fatalf("unsupported transfer features = %#v", targets)
	}
	for _, action := range targets.Targets[0].Actions {
		if action == browser.ActionUpload || action == browser.ActionDownload {
			t.Fatalf("unsupported action advertised: %q", action)
		}
	}
	result := NewBrowserActTool(browserToolTestConfig(), source).Execute(browserToolTestContext(), map[string]any{
		"browser_session_id": "browser_session_1", "tab_id": "tab_primary",
		"snapshot_id": "snapshot_1", "snapshot_generation": 1,
		"action": map[string]any{"kind": "download", "ref": "ref_download"},
	})
	if result == nil || !result.IsError || source.prepareCalls != 0 ||
		!strings.Contains(result.ContentForLLM(), `"code":"driver_incompatible"`) {
		t.Fatalf("unsupported transfer result = %#v; prepare calls = %d", result, source.prepareCalls)
	}
}

func TestBrowserDownloadIsNotAdvertisedOrPreparedWithoutScopedDriver(t *testing.T) {
	source := &fakeBrowserToolSource{available: true, downloadUnavailable: true}
	var targets browserTargetResult
	decodeBrowserToolResult(
		t, NewBrowserTargetsTool(browserToolTestConfig(), source).Execute(browserToolTestContext(), nil), &targets,
	)
	if len(targets.Targets) != 1 || !targets.Targets[0].Features.Upload || targets.Targets[0].Features.Download {
		t.Fatalf("scoped transfer features = %#v", targets)
	}
	upload, download := false, false
	for _, action := range targets.Targets[0].Actions {
		upload = upload || action == browser.ActionUpload
		download = download || action == browser.ActionDownload
	}
	if !upload || download {
		t.Fatalf("scoped transfer actions = %#v", targets.Targets[0].Actions)
	}
	result := NewBrowserActTool(browserToolTestConfig(), source).Execute(browserToolTestContext(), map[string]any{
		"browser_session_id": "browser_session_1", "tab_id": "tab_primary",
		"snapshot_id": "snapshot_1", "snapshot_generation": 1,
		"action": map[string]any{"kind": "download", "ref": "ref_download"},
	})
	if result == nil || !result.IsError || source.prepareCalls != 0 ||
		!strings.Contains(result.ContentForLLM(), `"code":"driver_incompatible"`) {
		t.Fatalf("unavailable download result = %#v; prepare calls = %d", result, source.prepareCalls)
	}
}

func TestBrowserActSchemaDoesNotAdvertiseDeferredDownload(t *testing.T) {
	parameters := NewBrowserActTool(
		browserToolTestConfig(),
		&fakeBrowserToolSource{available: true, downloadUnavailable: true},
	).Parameters()
	properties := parameters["properties"].(map[string]any)
	action := properties["action"].(map[string]any)
	actionProperties := action["properties"].(map[string]any)
	kind := actionProperties["kind"].(map[string]any)
	for _, candidate := range kind["enum"].([]string) {
		if candidate == string(browser.ActionDownload) {
			t.Fatalf("deferred download action advertised in schema: %#v", kind["enum"])
		}
	}
	if _, ok := actionProperties["deliver"]; ok {
		t.Fatalf("download-only deliver field advertised in schema: %#v", actionProperties)
	}
}

func TestBrowserActSchemaAdvertisesAdmittedDownload(t *testing.T) {
	parameters := NewBrowserActTool(browserToolTestConfig(), &fakeBrowserToolSource{available: true}).Parameters()
	properties := parameters["properties"].(map[string]any)
	action := properties["action"].(map[string]any)
	actionProperties := action["properties"].(map[string]any)
	kind := actionProperties["kind"].(map[string]any)
	download := false
	for _, candidate := range kind["enum"].([]string) {
		download = download || candidate == string(browser.ActionDownload)
	}
	if !download {
		t.Fatalf("admitted download action missing from schema: %#v", kind["enum"])
	}
	if _, ok := actionProperties["deliver"]; !ok {
		t.Fatalf("admitted download delivery field missing from schema: %#v", actionProperties)
	}
}

func TestBrowserScreenshotRequiresRecoverableOutboundOwnerBeforeCapture(t *testing.T) {
	source := &fakeBrowserToolSource{available: true}
	ctx := WithToolRecoverableOutbound(browserToolTestContext(), false)
	result := NewBrowserObserveTool(browserToolTestConfig(), source).Execute(
		ctx,
		map[string]any{"browser_session_id": "browser_session_1", "screenshot": true},
	)
	if result == nil || !result.IsError ||
		!strings.Contains(result.ContentForLLM(), `"code":"delivery_unavailable"`) ||
		source.observeCalls != 0 || source.screenshotRequest.RequestID != "" {
		t.Fatalf("unrecoverable screenshot result = %#v; source = %#v", result, source)
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
			DeliveryState: browser.ScreenshotDeliveryPending, MediaRef: "media://opaque",
			Recovery: &browser.ScreenshotRecovery{
				WorkspaceID: "workspace_1", AgentID: "browser", ActorID: "actor_1",
				RouteID: "route_1", SessionID: "browser_session_1", ToolCallID: "request_1",
			},
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
		observation.Artifact.MediaRef != "" || len(result.Media) != 0 || result.Outbound == nil ||
		len(result.Outbound.Media) != 1 || result.Outbound.Media[0].Ref != "media://opaque" ||
		result.Outbound.Recovery == nil || result.Outbound.Recovery.ArtifactRef != "transfer-artifact://opaque" ||
		!result.ImmediateDelivery || result.CommitOutbound == nil ||
		source.screenshotRequest.SnapshotID != "snapshot_1" || source.screenshotRequest.RequestID == "" ||
		strings.Contains(result.ForLLM, "delivery_state") ||
		strings.Contains(result.ForLLM, "media://opaque") ||
		strings.Contains(result.ForLLM, "iVBOR") {
		t.Fatalf(
			"screenshot observation = %#v; result = %#v; request = %#v",
			observation,
			result,
			source.screenshotRequest,
		)
	}
	if err := result.CommitOutbound(browserToolTestContext()); err != nil ||
		source.deliveryRequest.Ref != "transfer-artifact://opaque" ||
		source.deliveryRequest.RequestID != source.screenshotRequest.RequestID ||
		source.deliveryRequest.Recovery == source.screenshot.Recovery ||
		*source.deliveryRequest.Recovery != *source.screenshot.Recovery {
		t.Fatalf("commit outbound = %#v, %v", source.deliveryRequest, err)
	}

	source.lookup = source.screenshot
	source.lookup.DeliveryState = browser.ScreenshotDeliveryAlreadyClaimed
	source.lookupHit = true
	duplicate := NewBrowserObserveTool(browserToolTestConfig(), source).Execute(
		browserToolTestContext(),
		map[string]any{"browser_session_id": "browser_session_1", "screenshot": true},
	)
	var replay browserObservationView
	if duplicate.IsError || duplicate.Outbound == nil || duplicate.CommitOutbound == nil ||
		source.observeCalls != 1 || json.Unmarshal([]byte(duplicate.ForLLM), &replay) != nil ||
		!replay.Replayed || replay.Artifact == nil || replay.Artifact.Ref != observation.Artifact.Ref ||
		replay.Artifact.SnapshotID != observation.Artifact.SnapshotID {
		t.Fatalf("duplicate screenshot result = %#v", duplicate)
	}
	if err := duplicate.CommitOutbound(browserToolTestContext()); err != nil {
		t.Fatalf("recovery commit outbound error = %v", err)
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

func TestBrowserActDeliversRetainedDownloadWithRecovery(t *testing.T) {
	recovery := &browser.ScreenshotRecovery{
		WorkspaceID: "workspace", AgentID: "agent", ActorID: "actor", RouteID: "route",
		SessionID: "browser_session_1", ToolCallID: "request_download",
	}
	prepared := browser.PreparedAction{
		ID: "prepared_download", RequestID: "request_download", TabID: "tab_primary",
		CurrentOrigin: "https://example.com", Action: browser.Action{Kind: browser.ActionDownload, Deliver: true},
		Effect: browser.EffectRead,
	}
	source := &fakeBrowserToolSource{
		available: true,
		prepare:   browser.Preparation{Action: prepared},
		execute: browser.Invocation{
			ID: "invocation_download", SessionID: "browser_session_1", Effect: browser.EffectRead,
			State: browser.InvocationSucceeded,
			Download: &browser.DownloadArtifact{
				Ref: "transfer-artifact://download", Kind: "download", ContentType: "text/plain",
				Filename: "fixture.txt", Size: 7, SHA256: strings.Repeat("a", 64), ExpiresAt: 200,
				SessionID: "browser_session_1", TabID: "tab_primary", Generation: 2, Deliver: true,
				DeliveryState: browser.ScreenshotDeliveryPending, MediaRef: "media://download", Recovery: recovery,
			},
		},
	}
	result := NewBrowserActTool(browserToolTestConfig(), source).Execute(browserToolTestContext(), map[string]any{
		"browser_session_id": "browser_session_1", "tab_id": "tab_primary",
		"snapshot_id": "snapshot_1", "snapshot_generation": 2,
		"action": map[string]any{"kind": "download", "ref": "ref_download", "deliver": true},
	})
	if result == nil || result.IsError || result.Outbound == nil || len(result.Outbound.Media) != 1 ||
		result.Outbound.Media[0].Ref != "media://download" || result.Outbound.Recovery == nil ||
		result.Outbound.Recovery.Kind != bus.OutboundRecoveryBrowserDownload {
		t.Fatalf("download result = %#v", result)
	}
	if err := result.CommitOutbound(browserToolTestContext()); err != nil ||
		source.downloadDelivery.Ref != "transfer-artifact://download" ||
		source.downloadDelivery.RequestID != "request_download" {
		t.Fatalf("download commit = %#v, %v", source.downloadDelivery, err)
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
