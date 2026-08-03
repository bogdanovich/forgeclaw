package gateway

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/agent"
	"github.com/bogdanovich/mintclaw/pkg/browser"
	"github.com/bogdanovich/mintclaw/pkg/bus"
	"github.com/bogdanovich/mintclaw/pkg/config"
	runtimeevents "github.com/bogdanovich/mintclaw/pkg/events"
	"github.com/bogdanovich/mintclaw/pkg/nodes"
	"github.com/bogdanovich/mintclaw/pkg/outbox"
	"github.com/bogdanovich/mintclaw/pkg/providers"
	"github.com/bogdanovich/mintclaw/pkg/tools"
)

func TestReplayGatewayOutboundIntentsPublishesRecoveredMedia(t *testing.T) {
	root := t.TempDir()
	first, err := outbox.OpenCoordinator(root)
	if err != nil {
		t.Fatal(err)
	}
	identity := outbox.Identity{
		SourceID: "spool-browser-recovery", Kind: outbox.KindMedia,
		Channel: "telegram", ChatID: "chat-1", SessionKey: "session-1",
	}
	admission, err := first.AdmitMedia(
		"/agents/browser",
		identity,
		bus.OutboundMediaMessage{
			Channel: "telegram", ChatID: "chat-1", SessionKey: "session-1",
			Parts: []bus.MediaPart{{Type: "image", Ref: "media://browser-screenshot"}},
		},
	)
	if err != nil || !admission.Dispatch {
		t.Fatalf("AdmitMedia() = %+v, %v", admission, err)
	}
	if err = first.Close(); err != nil {
		t.Fatal(err)
	}
	recovered, err := outbox.OpenCoordinator(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = recovered.Close() })
	pending, err := recovered.RecoverPending()
	if err != nil || len(pending) != 1 {
		t.Fatalf("RecoverPending() = %+v, %v", pending, err)
	}
	msgBus := bus.NewMessageBus()
	if err = replayGatewayOutboundIntents(t.Context(), msgBus, recovered, nil, pending); err != nil {
		t.Fatalf("replayGatewayOutboundIntents() error = %v", err)
	}
	select {
	case message := <-msgBus.OutboundMediaChan():
		if message.DeliveryID != admission.Intent.ID || len(message.Parts) != 1 ||
			message.Parts[0].Ref != "media://browser-screenshot" {
			t.Fatalf("recovered media = %+v", message)
		}
	case <-time.After(time.Second):
		t.Fatal("recovered media was not published")
	}
	if err = recovered.BeginAttempt(admission.Intent.ID); err != nil {
		t.Fatalf("BeginAttempt(recovered) error = %v", err)
	}
}

func TestReplayGatewayOutboundFailureLeavesIntentPendingForNextStartup(t *testing.T) {
	root := t.TempDir()
	first, err := outbox.OpenCoordinator(root)
	if err != nil {
		t.Fatal(err)
	}
	identity := outbox.Identity{
		SourceID: "spool-recovery-retry", Kind: outbox.KindMessage,
		Channel: "telegram", ChatID: "chat-1", SessionKey: "session-1",
	}
	admission, err := first.AdmitMessage(
		"/agents/main", identity,
		bus.OutboundMessage{Channel: "telegram", ChatID: "chat-1", Content: "retry me"},
	)
	if err != nil || !admission.Dispatch {
		t.Fatalf("AdmitMessage() = %+v, %v", admission, err)
	}
	if err = first.Close(); err != nil {
		t.Fatal(err)
	}
	recovered, err := outbox.OpenCoordinator(root)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := recovered.RecoverPending()
	if err != nil || len(pending) != 1 {
		t.Fatalf("RecoverPending(first restart) = %+v, %v", pending, err)
	}
	closedBus := bus.NewMessageBus()
	closedBus.Close()
	if err = replayGatewayOutboundIntents(
		t.Context(), closedBus, recovered, nil, pending,
	); err == nil {
		t.Fatal("replayGatewayOutboundIntents() unexpectedly succeeded")
	}
	intent, getErr := recovered.Get(admission.Intent.ID)
	if getErr != nil || intent.Status != outbox.StatusPending {
		t.Fatalf("intent after rejected recovery = %+v, %v", intent, getErr)
	}
	if err = recovered.Close(); err != nil {
		t.Fatal(err)
	}
	next, err := outbox.OpenCoordinator(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = next.Close() })
	pending, err = next.RecoverPending()
	if err != nil || len(pending) != 1 || pending[0].ID != admission.Intent.ID {
		t.Fatalf("RecoverPending(next restart) = %+v, %v", pending, err)
	}
}

func TestRun_StartupFailuresReturnErrorAndEmitStructuredLog(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		prepare    func(t *testing.T, dir string) string
		wantErr    string
		wantLogSub string
	}{
		{
			name: "invalid config returns load error",
			prepare: func(t *testing.T, dir string) string {
				t.Helper()
				cfgPath := filepath.Join(dir, "invalid-config.json")
				if err := os.WriteFile(cfgPath, []byte("{invalid-json"), 0o644); err != nil {
					t.Fatalf("WriteFile(invalid config) error = %v", err)
				}
				return cfgPath
			},
			wantErr:    "error loading config:",
			wantLogSub: "error loading config:",
		},
		{
			name: "invalid config returns pre-check error",
			prepare: func(t *testing.T, dir string) string {
				t.Helper()
				cfg := config.DefaultConfig()
				cfg.Gateway.Port = 0
				cfgPath := filepath.Join(dir, "config.json")
				if err := config.SaveConfig(cfgPath, cfg); err != nil {
					t.Fatalf("SaveConfig() error = %v", err)
				}
				return cfgPath
			},
			wantErr:    "config pre-check failed: invalid gateway port: 0",
			wantLogSub: "config pre-check failed: invalid gateway port: 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			homeDir := t.TempDir()
			configPath := tt.prepare(t, homeDir)

			cmd := exec.Command(os.Args[0], "-test.run=TestGatewayRunStartupFailureHelper")
			cmd.Env = append(os.Environ(),
				"GO_WANT_GATEWAY_RUN_HELPER=1",
				"MINTCLAW_TEST_HOME="+homeDir,
				"MINTCLAW_TEST_CONFIG="+configPath,
			)

			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("helper exited unexpectedly: %v\noutput:\n%s", err, string(output))
			}

			out := string(output)
			if !strings.Contains(out, tt.wantErr) {
				t.Fatalf("helper output missing expected error substring %q:\n%s", tt.wantErr, out)
			}

			logData, readErr := os.ReadFile(filepath.Join(homeDir, logPath, logFile))
			if readErr != nil {
				t.Fatalf("ReadFile(gateway.log) error = %v", readErr)
			}
			logText := string(logData)
			if !strings.Contains(logText, "Gateway startup failed") {
				t.Fatalf("gateway.log missing structured startup failure log:\n%s", logText)
			}
			if !strings.Contains(logText, tt.wantLogSub) {
				t.Fatalf("gateway.log missing expected failure detail %q:\n%s", tt.wantLogSub, logText)
			}
		})
	}
}

func TestGatewayRunStartupFailureHelper(t *testing.T) {
	if os.Getenv("GO_WANT_GATEWAY_RUN_HELPER") != "1" {
		return
	}

	homeDir := os.Getenv("MINTCLAW_TEST_HOME")
	configPath := os.Getenv("MINTCLAW_TEST_CONFIG")

	err := Run(false, homeDir, configPath, false)
	if err == nil {
		fmt.Fprintln(os.Stdout, "expected startup error, got nil")
		os.Exit(2)
	}

	fmt.Fprintln(os.Stdout, err.Error())
	os.Exit(0)
}

func TestSetupSafeRestartToolRegistersGatewayRestart(t *testing.T) {
	useRestartRuntimeGOOS(t, "linux")

	cfg := config.DefaultConfig()
	cfg.Agents.Defaults.Workspace = t.TempDir()
	cfg.Gateway.SafeRestart = config.GatewaySafeRestartConfig{
		Enabled:             true,
		ServiceManager:      "systemd-user",
		Service:             "mintclaw-main.service",
		DrainTimeoutSeconds: 1,
		ForceAfterTimeout:   true,
	}
	msgBus := bus.NewMessageBus()
	al := agent.NewAgentLoop(cfg, msgBus, &startupBlockedProvider{reason: "not used"})

	if err := setupSafeRestartTool(cfg, al, msgBus, knownPreflightOptions()); err != nil {
		t.Fatalf("setupSafeRestartTool() error = %v", err)
	}

	info := al.GetStartupInfo()
	toolsInfo := info["tools"].(map[string]any)
	toolsList := toolsInfo["names"].([]string)
	if !slices.Contains(toolsList, "gateway_restart") {
		t.Fatalf("registered tools = %#v, want gateway_restart", toolsList)
	}
}

func TestSetupSafeRestartToolDisabledDoesNotAffectReload(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Agents.Defaults.Workspace = t.TempDir()
	cfg.Agents.Defaults.ContextManager = "none"
	msgBus := bus.NewMessageBus()
	al := agent.NewAgentLoop(cfg, msgBus, &startupBlockedProvider{reason: "not used"})

	if err := setupSafeRestartTool(cfg, al, msgBus, knownPreflightOptions()); err != nil {
		t.Fatalf("setupSafeRestartTool() error = %v", err)
	}
	if err := al.ReloadProviderAndConfig(
		context.Background(),
		&startupBlockedProvider{reason: "not used"},
		cfg,
	); err != nil {
		t.Fatalf("ReloadProviderAndConfig() error = %v", err)
	}

	info := al.GetStartupInfo()
	toolsInfo := info["tools"].(map[string]any)
	toolsList := toolsInfo["names"].([]string)
	if slices.Contains(toolsList, "gateway_restart") {
		t.Fatalf("registered tools = %#v, gateway_restart should stay disabled", toolsList)
	}
}

func TestSafeRestartToolSurvivesAgentRegistryReload(t *testing.T) {
	useRestartRuntimeGOOS(t, "linux")

	cfg := config.DefaultConfig()
	cfg.Agents.Defaults.Workspace = t.TempDir()
	cfg.Agents.Defaults.ContextManager = "none"
	cfg.Gateway.SafeRestart = config.GatewaySafeRestartConfig{
		Enabled:             true,
		ServiceManager:      "systemd-user",
		Service:             "mintclaw-main.service",
		DrainTimeoutSeconds: 1,
		ForceAfterTimeout:   true,
	}
	msgBus := bus.NewMessageBus()
	al := agent.NewAgentLoop(cfg, msgBus, &startupBlockedProvider{reason: "not used"})
	if err := setupSafeRestartTool(cfg, al, msgBus, knownPreflightOptions()); err != nil {
		t.Fatalf("setupSafeRestartTool() error = %v", err)
	}

	reloadCfg := config.DefaultConfig()
	reloadCfg.Agents.Defaults.Workspace = cfg.Agents.Defaults.Workspace
	reloadCfg.Agents.Defaults.ContextManager = "none"
	reloadCfg.Gateway.SafeRestart = cfg.Gateway.SafeRestart
	err := al.ReloadProviderAndConfig(
		context.Background(),
		&startupBlockedProvider{reason: "not used"},
		reloadCfg,
	)
	if err != nil {
		t.Fatalf("ReloadProviderAndConfig() error = %v", err)
	}

	info := al.GetStartupInfo()
	toolsInfo := info["tools"].(map[string]any)
	toolsList := toolsInfo["names"].([]string)
	if !slices.Contains(toolsList, "gateway_restart") {
		t.Fatalf("registered tools after reload = %#v, want gateway_restart", toolsList)
	}
}

func TestNodeToolsTrackNodeEnablementAcrossReload(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Agents.Defaults.Workspace = t.TempDir()
	cfg.Agents.Defaults.ContextManager = "none"
	cfg.Nodes.Enabled = true
	msgBus := bus.NewMessageBus()
	al := agent.NewAgentLoop(cfg, msgBus, &startupBlockedProvider{reason: "not used"})
	runtime := &nodeAdmissionRuntime{
		registryPath: nodes.RegistryPath(cfg.WorkspacePath()),
		handler:      &fakeNodeAdmissionHandler{},
		generation:   1,
		mounted:      true,
	}
	if err := setupNodeTools(cfg, al, runtime); err != nil {
		t.Fatalf("setupNodeTools() error = %v", err)
	}
	toolsList := al.GetStartupInfo()["tools"].(map[string]any)["names"].([]string)
	for _, name := range []string{"nodes", "nodes_invoke", "nodes_status", "nodes_cancel"} {
		if !slices.Contains(toolsList, name) {
			t.Fatalf("registered tools = %#v, want %s", toolsList, name)
		}
	}

	reloadCfg := config.DefaultConfig()
	reloadCfg.Agents.Defaults.Workspace = cfg.Agents.Defaults.Workspace
	reloadCfg.Agents.Defaults.ContextManager = "none"
	if err := al.ReloadProviderAndConfig(
		context.Background(),
		&startupBlockedProvider{reason: "not used"},
		reloadCfg,
	); err != nil {
		t.Fatalf("ReloadProviderAndConfig() error = %v", err)
	}
	toolsList = al.GetStartupInfo()["tools"].(map[string]any)["names"].([]string)
	for _, name := range []string{"nodes", "nodes_invoke", "nodes_status", "nodes_cancel"} {
		if slices.Contains(toolsList, name) {
			t.Fatalf("registered tools = %#v, %s should be disabled", toolsList, name)
		}
	}
}

func TestBrowserToolsTrackAgentGrantAcrossReload(t *testing.T) {
	cfg := gatewayBrowserConfig(t.TempDir())
	cfg.Agents.Defaults.ContextManager = "none"
	cfg.Tools.Browser.Agents = []string{"main"}
	al := agent.NewAgentLoop(cfg, bus.NewMessageBus(), &startupBlockedProvider{reason: "not used"})
	defer al.Close()
	services := &services{}
	if err := setupBrowserTools(cfg, al, services); err != nil {
		t.Fatalf("setupBrowserTools() error = %v", err)
	}
	toolNames := al.GetStartupInfo()["tools"].(map[string]any)["names"].([]string)
	for _, name := range []string{"browser_targets", "browser_session", "browser_observe", "browser_act"} {
		if !slices.Contains(toolNames, name) {
			t.Fatalf("registered tools = %#v, want %s", toolNames, name)
		}
	}

	reloadCfg := config.DefaultConfig()
	reloadCfg.Agents.Defaults.Workspace = cfg.Agents.Defaults.Workspace
	reloadCfg.Agents.Defaults.ContextManager = "none"
	if err := al.ReloadProviderAndConfig(
		context.Background(),
		&startupBlockedProvider{reason: "not used"},
		reloadCfg,
	); err != nil {
		t.Fatalf("ReloadProviderAndConfig() error = %v", err)
	}
	toolNames = al.GetStartupInfo()["tools"].(map[string]any)["names"].([]string)
	for _, name := range []string{"browser_targets", "browser_session", "browser_observe", "browser_act"} {
		if slices.Contains(toolNames, name) {
			t.Fatalf("registered tools = %#v, %s should be disabled", toolNames, name)
		}
	}
}

func TestConfigReloadRetainsOldRegistryWhenBrowserLeaseCannotDrain(t *testing.T) {
	cfg := gatewayBrowserConfig(t.TempDir())
	cfg.Agents.Defaults.ContextManager = "none"
	cfg.Tools.Browser.Agents = []string{"main"}
	msgBus := bus.NewMessageBus()
	defer msgBus.Close()
	al := agent.NewAgentLoop(cfg, msgBus, &startupBlockedProvider{reason: "not used"})
	defer al.Close()
	runtime := &browserRuntime{}
	runningServices := &services{Browser: runtime}
	if err := setupBrowserTools(cfg, al, runningServices); err != nil {
		t.Fatalf("setupBrowserTools() error = %v", err)
	}

	newCfg := config.DefaultConfig()
	newCfg.Agents.Defaults.Workspace = cfg.Agents.Defaults.Workspace
	newCfg.Agents.Defaults.ContextManager = "none"
	provider := providers.LLMProvider(&startupBlockedProvider{reason: "not used"})
	runningServices.browserMu.RLock()
	err := handleConfigReload(
		context.Background(), al, newCfg, &provider, runningServices, msgBus,
		true, false, 20*time.Millisecond,
	)
	runningServices.browserMu.RUnlock()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("handleConfigReload() error = %v, want deadline exceeded", err)
	}
	if runningServices.Browser != runtime {
		t.Fatal("failed reload replaced the retained browser runtime")
	}
	activeCfg := al.GetConfig()
	if activeCfg == nil || !activeCfg.Tools.Browser.Enabled ||
		!slices.Contains(activeCfg.Tools.Browser.Agents, "main") {
		t.Fatalf("active config after failed reload = %#v", activeCfg)
	}
	toolNames := al.GetStartupInfo()["tools"].(map[string]any)["names"].([]string)
	for _, name := range []string{"browser_targets", "browser_session", "browser_observe", "browser_act"} {
		if !slices.Contains(toolNames, name) {
			t.Fatalf("registered tools after failed reload = %#v, want %s", toolNames, name)
		}
	}
}

func TestConfigReloadRequiresRestartForWorkspaceChange(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Agents.Defaults.Workspace = t.TempDir()
	cfg.Agents.Defaults.ContextManager = "none"
	msgBus := bus.NewMessageBus()
	t.Cleanup(msgBus.Close)
	al := agent.NewAgentLoop(cfg, msgBus, &startupBlockedProvider{reason: "not used"})
	t.Cleanup(al.Close)

	reloadCfg := *cfg
	reloadCfg.Agents.Defaults.Workspace = t.TempDir()
	provider := providers.LLMProvider(&startupBlockedProvider{reason: "not used"})
	err := handleConfigReload(
		context.Background(),
		al,
		&reloadCfg,
		&provider,
		&services{},
		msgBus,
		true,
		false,
		time.Second,
	)
	if err == nil || !strings.Contains(err.Error(), "workspace changes require a gateway restart") {
		t.Fatalf("handleConfigReload() error = %v, want restart requirement", err)
	}
	if al.GetConfig().WorkspacePath() != cfg.WorkspacePath() {
		t.Fatalf("active workspace = %q, want %q", al.GetConfig().WorkspacePath(), cfg.WorkspacePath())
	}
}

func TestBrowserToolLeaseRejectsRevokedGrantAfterSuccessfulReload(t *testing.T) {
	cfg := gatewayBrowserConfig(t.TempDir())
	cfg.Agents.Defaults.ContextManager = "none"
	cfg.Agents.List = []config.AgentConfig{
		{ID: "main", Default: true},
		{ID: "browser"},
	}
	cfg.Tools.Browser.Agents = []string{"main"}
	msgBus := bus.NewMessageBus()
	defer msgBus.Close()
	al := agent.NewAgentLoop(cfg, msgBus, &startupBlockedProvider{reason: "not used"})
	defer al.Close()
	runningServices := &services{}
	if err := setupBrowserTools(cfg, al, runningServices); err != nil {
		t.Fatalf("setupBrowserTools() error = %v", err)
	}
	oldMain, ok := al.GetRegistry().GetAgent("main")
	if !ok {
		t.Fatal("old main agent is unavailable")
	}
	oldTool, ok := oldMain.Tools.Get("browser_session")
	if !ok {
		t.Fatal("old main browser_session tool is unavailable")
	}

	reloadCfg := *cfg
	reloadCfg.Tools.Browser = cfg.Tools.Browser
	reloadCfg.Tools.Browser.Agents = []string{"browser"}
	if err := al.ReloadProviderAndConfig(
		context.Background(),
		&startupBlockedProvider{reason: "not used"},
		&reloadCfg,
	); err != nil {
		t.Fatalf("ReloadProviderAndConfig() error = %v", err)
	}
	policyRevision, err := reloadCfg.Tools.Browser.PolicyRevision()
	if err != nil {
		t.Fatal(err)
	}
	broker, err := browser.NewBroker(
		&reloadCfg,
		browser.NewMemoryStore(),
		&gatewayTestBrowserFactory{worker: &gatewayTestBrowserWorker{}},
	)
	if err != nil {
		t.Fatal(err)
	}
	runningServices.Browser = &browserRuntime{
		broker: broker, policyRevision: policyRevision,
	}
	defer func() { _ = closeBrowserRuntime(context.Background(), runningServices) }()

	oldResult := oldTool.Execute(
		gatewayBrowserToolContext("main"),
		map[string]any{
			"operation": "open", "target": config.BrowserDefaultTarget,
			"profile": config.BrowserDefaultProfile,
		},
	)
	if oldResult == nil || !oldResult.IsError ||
		!strings.Contains(oldResult.ContentForLLM(), `"code":"policy_denied"`) {
		t.Fatalf("stale old-registry tool result = %#v", oldResult)
	}
	newBrowser, ok := al.GetRegistry().GetAgent("browser")
	if !ok {
		t.Fatal("new browser agent is unavailable")
	}
	newTool, ok := newBrowser.Tools.Get("browser_session")
	if !ok {
		t.Fatal("new browser_session tool is unavailable")
	}
	newResult := newTool.Execute(
		gatewayBrowserToolContext("browser"),
		map[string]any{
			"operation": "open", "target": config.BrowserDefaultTarget,
			"profile": config.BrowserDefaultProfile,
		},
	)
	if newResult == nil || newResult.IsError ||
		!strings.Contains(newResult.ContentForLLM(), `"state":"ready"`) {
		t.Fatalf("new-registry tool result = %#v", newResult)
	}
}

func gatewayBrowserToolContext(agentID string) context.Context {
	ctx := tools.WithToolInboundMetadata(context.Background(), bus.InboundContext{
		SenderID: "browser-test-user", ActorID: "browser-test-actor",
	})
	ctx = tools.WithToolSessionContext(ctx, agentID, "browser-test-history", nil)
	ctx = tools.WithToolRouteSessionKey(ctx, "telegram:browser-test")
	ctx = tools.WithToolCallID(ctx, "browser-test-call")
	return tools.WithToolExecutionIdentity(ctx, "/browser-test", "browser-test-execution")
}

func TestNodeFileToolsRequireConfiguredTargetGrant(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Agents.Defaults.Workspace = t.TempDir()
	cfg.Agents.Defaults.ContextManager = "none"
	cfg.Nodes.Enabled = true
	cfg.Execution.Targets = map[string]config.ExecutionTarget{
		"personal": {
			Type:        "node",
			Node:        "personal-node",
			FileProfile: "project",
		},
	}
	cfg.Agents.Defaults.TargetPolicy = &config.TargetPolicy{
		DefaultTarget:  "personal",
		AllowedTargets: []string{"personal"},
	}
	msgBus := bus.NewMessageBus()
	al := agent.NewAgentLoop(cfg, msgBus, &startupBlockedProvider{reason: "not used"})
	runtime := &nodeAdmissionRuntime{
		registryPath: nodes.RegistryPath(cfg.WorkspacePath()),
		handler:      &fakeNodeAdmissionHandler{},
		generation:   1,
		mounted:      true,
	}
	t.Cleanup(func() {
		if runtime.transferSpool != nil {
			_ = runtime.transferSpool.Close()
		}
	})
	if err := setupNodeTools(cfg, al, runtime); err != nil {
		t.Fatal(err)
	}
	toolNames := al.GetStartupInfo()["tools"].(map[string]any)["names"].([]string)
	for _, name := range []string{
		"nodes_file_info",
		"nodes_upload",
		"nodes_download",
	} {
		if !slices.Contains(toolNames, name) {
			t.Fatalf("registered tools = %#v, want %s", toolNames, name)
		}
	}

	withoutGrant := config.DefaultConfig()
	withoutGrant.Agents.Defaults.Workspace = t.TempDir()
	withoutGrant.Agents.Defaults.ContextManager = "none"
	withoutGrant.Nodes.Enabled = true
	withoutGrant.Execution.Targets = map[string]config.ExecutionTarget{
		"personal": {Type: "node", Node: "personal-node"},
	}
	withoutGrant.Agents.Defaults.TargetPolicy = &config.TargetPolicy{
		DefaultTarget:  "personal",
		AllowedTargets: []string{"personal"},
	}
	otherLoop := agent.NewAgentLoop(
		withoutGrant,
		bus.NewMessageBus(),
		&startupBlockedProvider{reason: "not used"},
	)
	otherRuntime := &nodeAdmissionRuntime{
		registryPath: nodes.RegistryPath(withoutGrant.WorkspacePath()),
		handler:      &fakeNodeAdmissionHandler{},
		generation:   1,
		mounted:      true,
	}
	if err := setupNodeTools(withoutGrant, otherLoop, otherRuntime); err != nil {
		t.Fatal(err)
	}
	toolNames = otherLoop.GetStartupInfo()["tools"].(map[string]any)["names"].([]string)
	for _, name := range []string{
		"nodes_file_info",
		"nodes_upload",
		"nodes_download",
	} {
		if slices.Contains(toolNames, name) {
			t.Fatalf("registered tools = %#v, %s requires an explicit file profile", toolNames, name)
		}
	}
}

func TestReplayGatewayInboundSnapshotReplaysCapturedMessages(t *testing.T) {
	msgBus := bus.NewMessageBus()
	spool, err := bus.NewInboundSpool(t.TempDir())
	if err != nil {
		t.Fatalf("NewInboundSpool() error = %v", err)
	}
	msgBus.SetInboundSpool(spool)
	ctx := context.Background()
	original := bus.InboundMessage{
		Channel: "telegram",
		ChatID:  "chat-1",
		Context: bus.InboundContext{
			Channel:  "telegram",
			ChatID:   "chat-1",
			SenderID: "user-1",
		},
		SenderID: "user-1",
		Content:  "pending restart message",
	}
	if _, err = spool.Prepare(ctx, original); err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}

	pending := snapshotGatewayInboundSpool(ctx, msgBus)
	if len(pending) != 1 {
		t.Fatalf("snapshot entries = %d, want 1", len(pending))
	}
	replayGatewayInboundSnapshot(ctx, msgBus, pending)

	select {
	case got := <-msgBus.InboundChan():
		if got.Content != original.Content {
			t.Fatalf("replayed content = %q, want %q", got.Content, original.Content)
		}
	case <-time.After(time.Second):
		t.Fatal("expected replayed inbound message")
	}
}

func TestReplayGatewayInboundSnapshotDoesNotReplayMessagesAddedAfterSnapshot(t *testing.T) {
	msgBus := bus.NewMessageBus()
	spool, err := bus.NewInboundSpool(t.TempDir())
	if err != nil {
		t.Fatalf("NewInboundSpool() error = %v", err)
	}
	msgBus.SetInboundSpool(spool)
	ctx := context.Background()
	first := bus.InboundMessage{
		Channel: "telegram",
		ChatID:  "chat-1",
		Context: bus.InboundContext{
			Channel: "telegram",
			ChatID:  "chat-1",
		},
		Content: "durable before startup",
	}
	if _, err := spool.Prepare(ctx, first); err != nil {
		t.Fatalf("Prepare(first) error = %v", err)
	}
	pending := snapshotGatewayInboundSpool(ctx, msgBus)

	second := first
	second.Content = "arrived after snapshot"
	if _, err := spool.Prepare(ctx, second); err != nil {
		t.Fatalf("Prepare(second) error = %v", err)
	}
	replayGatewayInboundSnapshot(ctx, msgBus, pending)

	select {
	case got := <-msgBus.InboundChan():
		if got.Content != first.Content {
			t.Fatalf("replayed content = %q, want %q", got.Content, first.Content)
		}
	case <-time.After(time.Second):
		t.Fatal("expected snapshot message to be replayed")
	}
	select {
	case got := <-msgBus.InboundChan():
		t.Fatalf("unexpected replay of post-snapshot message: %#v", got)
	default:
	}
}

func TestCollectGatewayStartupStatusHandlesMalformedInfo(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                string
		startupInfo         map[string]any
		wantToolsCount      int
		wantSkillsAvailable int
		wantSkillsTotal     int
		wantLogFields       map[string]any
	}{
		{
			name:          "missing info",
			startupInfo:   map[string]any{},
			wantLogFields: map[string]any{},
		},
		{
			name: "wrong map shapes",
			startupInfo: map[string]any{
				"tools":  "unexpected",
				"skills": []any{"unexpected"},
			},
			wantLogFields: map[string]any{},
		},
		{
			name: "valid startup info",
			startupInfo: map[string]any{
				"tools": map[string]any{
					"count": 3,
				},
				"skills": map[string]any{
					"available": 2,
					"total":     5,
				},
			},
			wantToolsCount:      3,
			wantSkillsAvailable: 2,
			wantSkillsTotal:     5,
			wantLogFields: map[string]any{
				"tools_count":      3,
				"skills_available": 2,
				"skills_total":     5,
			},
		},
		{
			name: "json number startup info",
			startupInfo: map[string]any{
				"tools": map[string]any{
					"count": float64(4),
				},
				"skills": map[string]any{
					"available": float64(1),
					"total":     float64(6),
				},
			},
			wantToolsCount:      4,
			wantSkillsAvailable: 1,
			wantSkillsTotal:     6,
			wantLogFields: map[string]any{
				"tools_count":      4,
				"skills_available": 1,
				"skills_total":     6,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := collectGatewayStartupStatus(tt.startupInfo)
			if got.toolsCount != tt.wantToolsCount {
				t.Fatalf("toolsCount = %d, want %d", got.toolsCount, tt.wantToolsCount)
			}
			if got.skillsAvailable != tt.wantSkillsAvailable {
				t.Fatalf("skillsAvailable = %d, want %d", got.skillsAvailable, tt.wantSkillsAvailable)
			}
			if got.skillsTotal != tt.wantSkillsTotal {
				t.Fatalf("skillsTotal = %d, want %d", got.skillsTotal, tt.wantSkillsTotal)
			}
			if !reflect.DeepEqual(got.logFields, tt.wantLogFields) {
				t.Fatalf("logFields = %#v, want %#v", got.logFields, tt.wantLogFields)
			}
		})
	}
}

func TestPublishGatewayEvent(t *testing.T) {
	eventBus := runtimeevents.NewBus()
	t.Cleanup(func() {
		if err := eventBus.Close(); err != nil {
			t.Fatalf("Close runtime event bus: %v", err)
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	sub, eventsCh, err := eventBus.Channel().OfKind(runtimeevents.KindGatewayStart).SubscribeChan(
		ctx,
		runtimeevents.SubscribeOptions{Name: "gateway-test", Buffer: 4},
	)
	if err != nil {
		t.Fatalf("SubscribeChan() error = %v", err)
	}
	t.Cleanup(func() {
		if err := sub.Close(); err != nil {
			t.Fatalf("Close subscription: %v", err)
		}
	})

	al := agent.NewAgentLoop(
		config.DefaultConfig(),
		bus.NewMessageBus(),
		&startupBlockedProvider{reason: "not used"},
		agent.WithRuntimeEvents(eventBus),
	)
	t.Cleanup(al.Close)

	startedAt := time.Now().Add(-1500 * time.Millisecond)
	publishGatewayEvent(al, runtimeevents.KindGatewayStart, startedAt, nil)

	evt := receiveGatewayRuntimeEvent(t, eventsCh)
	if evt.Kind != runtimeevents.KindGatewayStart ||
		evt.Source.Component != "gateway" ||
		evt.Severity != runtimeevents.SeverityInfo {
		t.Fatalf("gateway event = %+v", evt)
	}
	payload, ok := evt.Payload.(gatewayEventPayload)
	if !ok {
		t.Fatalf("payload type = %T, want gatewayEventPayload", evt.Payload)
	}
	if payload.DurationMS <= 0 {
		t.Fatalf("DurationMS = %d, want positive", payload.DurationMS)
	}
	if evt.Attrs["duration_ms"] == nil {
		t.Fatalf("gateway event attrs missing duration_ms: %#v", evt.Attrs)
	}
}

func TestShutdownGatewayClosesMessageBus(t *testing.T) {
	msgBus := bus.NewMessageBus()
	al := agent.NewAgentLoop(
		config.DefaultConfig(),
		msgBus,
		&startupBlockedProvider{reason: "not used"},
	)
	msgBus.SetEventPublisher(al.RuntimeEventBus())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sub, eventsCh, err := al.RuntimeEventBus().Channel().OfKind(runtimeevents.KindBusCloseCompleted).SubscribeChan(
		ctx,
		runtimeevents.SubscribeOptions{Name: "bus-close-test", Buffer: 4},
	)
	if err != nil {
		t.Fatalf("SubscribeChan() error = %v", err)
	}
	defer func() {
		_ = sub.Close()
	}()

	shutdownGateway(&services{}, al, &startupBlockedProvider{reason: "not used"}, msgBus, true)

	evt := receiveGatewayRuntimeEvent(t, eventsCh)
	if evt.Kind != runtimeevents.KindBusCloseCompleted {
		t.Fatalf("shutdown event kind = %q, want %q", evt.Kind, runtimeevents.KindBusCloseCompleted)
	}
	if err := msgBus.PublishVoiceControl(context.Background(), bus.VoiceControl{}); !errors.Is(err, bus.ErrBusClosed) {
		t.Fatalf("PublishVoiceControl after shutdown error = %v, want %v", err, bus.ErrBusClosed)
	}
}

func receiveGatewayRuntimeEvent(t *testing.T, ch <-chan runtimeevents.Event) runtimeevents.Event {
	t.Helper()

	select {
	case evt := <-ch:
		return evt
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for gateway runtime event")
		return runtimeevents.Event{}
	}
}
