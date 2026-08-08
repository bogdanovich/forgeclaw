package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bogdanovich/mintclaw/pkg/agent"
	"github.com/bogdanovich/mintclaw/pkg/bus"
	"github.com/bogdanovich/mintclaw/pkg/config"
	"github.com/bogdanovich/mintclaw/pkg/providers"
	toolshared "github.com/bogdanovich/mintclaw/pkg/tools/shared"
)

func deployConfig(script string) config.GatewayDeployConfig {
	return config.GatewayDeployConfig{
		Enabled:        true,
		Group:          "local",
		Command:        script,
		DefaultTarget:  "current",
		AllowedTargets: []string{"current", "all"},
		TimeoutSeconds: 1,
	}
}

func writeDeployScript(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "deploy.sh")
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+body), 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestDeployRunnerValidatesTargetAndRecordsSuccess(t *testing.T) {
	script := writeDeployScript(t, "printf '%s:%s' \"$1\" \"$MINTCLAW_DEPLOY_TARGET\"")
	workspace := t.TempDir()
	runner, err := NewDeployRunner(deployConfig(script), workspace, "main.service")
	if err != nil {
		t.Fatal(err)
	}
	origin := RestartOrigin{
		Channel:    "telegram",
		ChatID:     "chat-1",
		TopicID:    "topic-1",
		SessionKey: "session-1",
	}
	out, code, err := runner.Run(context.Background(), "current", origin)
	if err != nil || code != 0 || out != "--target:current" {
		t.Fatalf("Run() = %q, %d, %v", out, code, err)
	}
	if _, _, runErr := runner.Run(context.Background(), "bad", RestartOrigin{}); runErr == nil {
		t.Fatal("expected invalid target error")
	}

	data, err := os.ReadFile(
		filepath.Join(workspace, "state", "gateway-deploy", "deploy-sentinel.json"),
	)
	if err != nil {
		t.Fatal(err)
	}
	var sentinel DeploySentinel
	if err := json.Unmarshal(data, &sentinel); err != nil {
		t.Fatal(err)
	}
	if sentinel.Kind != "deploy" || sentinel.Status != "succeeded" ||
		sentinel.Group != "local" || sentinel.Target != "current" {
		t.Fatalf("sentinel = %#v", sentinel)
	}
	if sentinel.Command != script || sentinel.ExitCode != 0 || sentinel.Origin != origin {
		t.Fatalf("sentinel command/result/origin = %#v", sentinel)
	}
}

func TestNewDeployRunnerRejectsDisabledAndRelativeCommand(t *testing.T) {
	cfg := deployConfig("deploy.sh")
	if _, err := NewDeployRunner(cfg, t.TempDir(), ""); err == nil {
		t.Fatal("expected relative command to be rejected")
	}
	cfg.Enabled = false
	if _, err := NewDeployRunner(cfg, t.TempDir(), ""); err == nil {
		t.Fatal("expected disabled deploy to be rejected")
	}
}

func TestDeploySentinelStoreDoesNotAcknowledgeReplacedDeploy(t *testing.T) {
	store, err := NewDeploySentinelStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	first := DeploySentinel{
		Kind: "deploy", Status: "failed", Group: "local", Target: "current",
		RequestedAt: time.Now().UTC().Add(-time.Minute), UpdatedAt: time.Now().UTC().Add(-time.Minute),
	}
	second := DeploySentinel{
		Kind: "deploy", Status: "running", Group: "local", Target: "current",
		RequestedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}
	if writeErr := store.Write(second); writeErr != nil {
		t.Fatal(writeErr)
	}
	deliveryCalled := false
	delivered, err := store.DeliverContinuationIfCurrent(first, time.Now().UTC(), func(DeploySentinel) error {
		deliveryCalled = true
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if delivered {
		t.Fatal("replaced deploy was acknowledged")
	}
	if deliveryCalled {
		t.Fatal("replaced deploy was delivered")
	}
	current, err := store.Read()
	if err != nil {
		t.Fatal(err)
	}
	if !current.ContinuationSentAt.IsZero() || !current.RequestedAt.Equal(second.RequestedAt) {
		t.Fatalf("current sentinel was modified: %#v", current)
	}
}

func TestGatewayDeployToolPersistsTopicOrigin(t *testing.T) {
	workspace := t.TempDir()
	runner, err := NewDeployRunner(deployConfig(writeDeployScript(t, "true")), workspace, "")
	if err != nil {
		t.Fatal(err)
	}
	ctx := toolshared.WithToolTopicID(
		toolshared.WithToolContext(context.Background(), "telegram", "chat-1"), "topic-1",
	)
	result := (&GatewayDeployTool{runner: runner}).Execute(ctx, map[string]any{})
	if result.Err != nil {
		t.Fatalf("Execute() error = %v", result.Err)
	}
	data, err := os.ReadFile(
		filepath.Join(workspace, "state", "gateway-deploy", "deploy-sentinel.json"),
	)
	if err != nil {
		t.Fatal(err)
	}
	var sentinel DeploySentinel
	if err := json.Unmarshal(data, &sentinel); err != nil {
		t.Fatal(err)
	}
	if sentinel.Origin.Channel != "telegram" || sentinel.Origin.ChatID != "chat-1" ||
		sentinel.Origin.TopicID != "topic-1" {
		t.Fatalf("sentinel origin = %#v", sentinel.Origin)
	}
}

type fakeDeployHandoffLauncher struct {
	called bool
	calls  int
	target string
	origin RestartOrigin
	err    error
}

func (l *fakeDeployHandoffLauncher) Launch(
	_ context.Context,
	_ *DeployRunner,
	target string,
	origin RestartOrigin,
) error {
	l.called = true
	l.calls++
	l.target = target
	l.origin = origin
	return l.err
}

func TestGatewayDeployToolUsesDetachedHandoffForConfiguredTarget(t *testing.T) {
	workspace := t.TempDir()
	cfg := deployConfig(writeDeployScript(t, "true"))
	cfg.HandoffTargets = []string{"current"}
	runner, err := NewDeployRunner(cfg, workspace, "mintclaw-main.service")
	if err != nil {
		t.Fatal(err)
	}
	launcher := &fakeDeployHandoffLauncher{}
	tool := &GatewayDeployTool{runner: runner, launcher: launcher}
	ctx := toolshared.WithToolTopicID(
		toolshared.WithToolContext(context.Background(), "telegram", "chat-1"), "topic-1",
	)
	result := tool.Execute(ctx, nil)
	if result.Err != nil {
		t.Fatalf("Execute() error = %v", result.Err)
	}
	if !launcher.called || launcher.target != "current" {
		t.Fatalf("launcher = %#v", launcher)
	}
	if launcher.calls != 1 {
		t.Fatalf("launcher calls = %d, want 1", launcher.calls)
	}
	if launcher.origin.Channel != "telegram" || launcher.origin.ChatID != "chat-1" ||
		launcher.origin.TopicID != "topic-1" {
		t.Fatalf("launcher origin = %#v", launcher.origin)
	}
	if !strings.Contains(result.ForUser, "detached worker") {
		t.Fatalf("result = %q", result.ForUser)
	}
	assertFinalHandledDeployResult(t, result)
}

func TestGatewayDeployToolSuccessfulNonHandoffIsFinalHandled(t *testing.T) {
	countPath := filepath.Join(t.TempDir(), "deploy-count")
	script := writeDeployScript(t, "printf x >> \"$MINTCLAW_WORKSPACE/deploy-count\"; printf 'deploy complete'")
	runner, err := NewDeployRunner(deployConfig(script), filepath.Dir(countPath), "")
	if err != nil {
		t.Fatal(err)
	}

	result := (&GatewayDeployTool{runner: runner}).Execute(context.Background(), map[string]any{"target": "all"})
	if result.Err != nil {
		t.Fatalf("Execute() error = %v", result.Err)
	}
	if result.ForUser != "deploy complete" {
		t.Fatalf("ForUser = %q, want deploy output", result.ForUser)
	}
	assertFinalHandledDeployResult(t, result)

	count, err := os.ReadFile(countPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(count) != "x" {
		t.Fatalf("deploy marker = %q, want one invocation", count)
	}
}

func TestGatewayDeployToolSuccessfulNonHandoffWithoutOutputReportsSuccess(t *testing.T) {
	runner, err := NewDeployRunner(
		deployConfig(writeDeployScript(t, "true")),
		t.TempDir(),
		"",
	)
	if err != nil {
		t.Fatal(err)
	}

	result := (&GatewayDeployTool{runner: runner}).Execute(context.Background(), map[string]any{"target": "all"})
	if result.Err != nil {
		t.Fatalf("Execute() error = %v", result.Err)
	}
	if result.ForUser != "Gateway deploy for target all completed successfully." {
		t.Fatalf("ForUser = %q", result.ForUser)
	}
	assertFinalHandledDeployResult(t, result)
}

func TestGatewayDeployToolFailuresRemainExplicitAndUnhandled(t *testing.T) {
	t.Run("handoff launch", func(t *testing.T) {
		cfg := deployConfig(writeDeployScript(t, "true"))
		cfg.HandoffTargets = []string{"current"}
		runner, err := NewDeployRunner(cfg, t.TempDir(), "")
		if err != nil {
			t.Fatal(err)
		}
		launchErr := errors.New("launcher unavailable")
		result := (&GatewayDeployTool{
			runner:   runner,
			launcher: &fakeDeployHandoffLauncher{err: launchErr},
		}).Execute(context.Background(), nil)
		assertFailedDeployResult(t, result, launchErr)
	})

	t.Run("deploy", func(t *testing.T) {
		runner, err := NewDeployRunner(
			deployConfig(writeDeployScript(t, "printf failure; exit 7")),
			t.TempDir(),
			"",
		)
		if err != nil {
			t.Fatal(err)
		}
		result := (&GatewayDeployTool{runner: runner}).Execute(context.Background(), map[string]any{"target": "all"})
		if result.Err == nil || !strings.Contains(result.ForLLM, "gateway deploy failed") ||
			!strings.Contains(result.ForLLM, "failure") {
			t.Fatalf("result = %#v", result)
		}
		if result.ResponseHandled || result.DeliveryIntent == toolshared.DeliveryFinalHandled {
			t.Fatalf("failed deploy claimed handled success: %#v", result)
		}
	})
}

func assertFinalHandledDeployResult(t *testing.T, result *toolshared.ToolResult) {
	t.Helper()
	if result.DeliveryIntent != toolshared.DeliveryFinalHandled {
		t.Fatalf("DeliveryIntent = %q, want final_handled", result.DeliveryIntent)
	}
	if !result.ResponseHandled {
		t.Fatal("successful deploy result did not own the turn")
	}
	if result.ImmediateDelivery || result.Silent {
		t.Fatalf("successful deploy retained immediate/silent flags: %#v", result)
	}
}

func assertFailedDeployResult(t *testing.T, result *toolshared.ToolResult, wantErr error) {
	t.Helper()
	if !errors.Is(result.Err, wantErr) || !result.IsError {
		t.Fatalf("result = %#v, want error %v", result, wantErr)
	}
	if result.ResponseHandled || result.DeliveryIntent == toolshared.DeliveryFinalHandled {
		t.Fatalf("failed deploy claimed handled success: %#v", result)
	}
}

type gatewayDeployTurnProvider struct {
	calls  int
	target string
}

func (p *gatewayDeployTurnProvider) Chat(
	context.Context,
	[]providers.Message,
	[]providers.ToolDefinition,
	string,
	map[string]any,
) (*providers.LLMResponse, error) {
	p.calls++
	if p.calls == 1 {
		return &providers.LLMResponse{ToolCalls: []providers.ToolCall{{
			ID:        "gateway-deploy-call",
			Type:      "function",
			Name:      "gateway_deploy",
			Arguments: map[string]any{"target": p.target},
		}}}, nil
	}
	return &providers.LLMResponse{}, nil
}

func (p *gatewayDeployTurnProvider) GetDefaultModel() string { return "gateway-deploy-test" }

func TestGatewayDeployToolSuccessfulResultCompletesAgentTurn(t *testing.T) {
	tests := []struct {
		name              string
		handoffTargets    []string
		wantLauncherCalls int
		wantContent       string
	}{
		{
			name:              "handoff",
			handoffTargets:    []string{"all"},
			wantLauncherCalls: 1,
			wantContent:       "Deploy started in a detached worker",
		},
		{
			name:        "non-handoff",
			wantContent: "deploy complete",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config.DefaultConfig()
			cfg.Agents.Defaults.Workspace = t.TempDir()
			cfg.Agents.Defaults.ModelName = "gateway-deploy-test"
			cfg.Agents.Defaults.MaxTokens = 1024
			cfg.Agents.Defaults.ContextWindow = 32768

			msgBus := bus.NewMessageBus()
			provider := &gatewayDeployTurnProvider{target: "all"}
			loop := agent.NewAgentLoop(cfg, msgBus, provider)
			runnerWorkspace := t.TempDir()
			deployCfg := deployConfig(writeDeployScript(t, "printf 'deploy complete'"))
			deployCfg.HandoffTargets = tc.handoffTargets
			runner, err := NewDeployRunner(deployCfg, runnerWorkspace, "mintclaw-main.service")
			if err != nil {
				t.Fatal(err)
			}
			launcher := &fakeDeployHandoffLauncher{}
			loop.RegisterTool(&GatewayDeployTool{runner: runner, launcher: launcher})

			response, err := loop.ProcessDirectWithChannel(
				context.Background(),
				"deploy all configured profiles",
				"deploy-session",
				"telegram",
				"chat-1",
			)
			if err != nil {
				t.Fatalf("ProcessDirectWithChannel() error = %v", err)
			}
			if response != "" {
				t.Fatalf("response = %q, want handled empty return", response)
			}
			if provider.calls != 1 {
				t.Fatalf("provider calls = %d, want 1", provider.calls)
			}
			if launcher.calls != tc.wantLauncherCalls {
				t.Fatalf("launcher calls = %d, want %d", launcher.calls, tc.wantLauncherCalls)
			}

			select {
			case outbound := <-msgBus.OutboundChan():
				if outbound.Channel != "telegram" || outbound.ChatID != "chat-1" {
					t.Fatalf("outbound route = %s/%s", outbound.Channel, outbound.ChatID)
				}
				if !strings.Contains(outbound.Content, tc.wantContent) {
					t.Fatalf("outbound content = %q, want %q", outbound.Content, tc.wantContent)
				}
				if strings.Contains(outbound.Content, "empty response") {
					t.Fatalf("outbound contained fallback: %q", outbound.Content)
				}
			default:
				t.Fatal("deploy acknowledgement was not delivered")
			}
			select {
			case outbound := <-msgBus.OutboundChan():
				t.Fatalf("unexpected duplicate outbound: %#v", outbound)
			default:
			}
		})
	}
}

func TestDeployHandoffUnitNameIsStableAndScopedToGroup(t *testing.T) {
	first := deployHandoffUnitName("mintclaw-local")
	if first != deployHandoffUnitName("mintclaw-local") {
		t.Fatalf("unit name must be stable")
	}
	if first == deployHandoffUnitName("another-group") {
		t.Fatalf("unit name must include group identity")
	}
}

func TestDeployRunnerFailureTimeoutAndTruncation(t *testing.T) {
	t.Run("failure", func(t *testing.T) {
		r, _ := NewDeployRunner(
			deployConfig(writeDeployScript(t, "echo fail; exit 7")),
			t.TempDir(),
			"",
		)
		_, code, err := r.Run(context.Background(), "", RestartOrigin{})
		if err == nil || code != 7 {
			t.Fatalf("code=%d err=%v", code, err)
		}
	})
	t.Run("timeout", func(t *testing.T) {
		r, _ := NewDeployRunner(deployConfig(writeDeployScript(t, "sleep 2")), t.TempDir(), "")
		_, _, err := r.Run(context.Background(), "", RestartOrigin{})
		if err == nil || !strings.Contains(err.Error(), "timed out") {
			t.Fatalf("err=%v", err)
		}
	})
	t.Run("truncation", func(t *testing.T) {
		script := writeDeployScript(t, "head -c 20000 /dev/zero | tr '\\000' x")
		r, _ := NewDeployRunner(deployConfig(script), t.TempDir(), "")
		out, _, err := r.Run(context.Background(), "", RestartOrigin{})
		if err != nil || !strings.HasPrefix(out, "[output truncated]") {
			t.Fatalf("len=%d err=%v", len(out), err)
		}
	})
}

func TestDeployRunnerRejectsConcurrentDeploy(t *testing.T) {
	workspace := t.TempDir()
	script := writeDeployScript(t, "touch \"$MINTCLAW_WORKSPACE/started\"; sleep 2")
	cfg := deployConfig(script)
	cfg.TimeoutSeconds = 5
	first, err := NewDeployRunner(cfg, workspace, "")
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewDeployRunner(cfg, t.TempDir(), "")
	if err != nil {
		t.Fatal(err)
	}
	firstDone := make(chan error, 1)
	go func() {
		_, _, runErr := first.Run(context.Background(), "", RestartOrigin{})
		firstDone <- runErr
	}()

	started := filepath.Join(workspace, "started")
	deadline := time.Now().Add(time.Second)
	for {
		if _, err := os.Stat(started); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("first deploy did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, _, err := second.Run(context.Background(), "", RestartOrigin{}); !errors.Is(
		err,
		ErrDeployAlreadyRunning,
	) {
		t.Fatalf("second Run() error = %v, want ErrDeployAlreadyRunning", err)
	}
	if err := <-firstDone; err != nil {
		t.Fatalf("first Run() error = %v", err)
	}
}
