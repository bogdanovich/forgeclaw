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

	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/tools"
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
	script := writeDeployScript(t, "printf '%s:%s' \"$1\" \"$FORGECLAW_DEPLOY_TARGET\"")
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

func TestGatewayDeployToolPersistsTopicOrigin(t *testing.T) {
	workspace := t.TempDir()
	runner, err := NewDeployRunner(deployConfig(writeDeployScript(t, "true")), workspace, "")
	if err != nil {
		t.Fatal(err)
	}
	ctx := tools.WithToolTopicID(
		tools.WithToolContext(context.Background(), "telegram", "chat-1"), "topic-1",
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
	target string
	origin RestartOrigin
}

func (l *fakeDeployHandoffLauncher) Launch(
	_ context.Context,
	_ *DeployRunner,
	target string,
	origin RestartOrigin,
) error {
	l.called = true
	l.target = target
	l.origin = origin
	return nil
}

func TestGatewayDeployToolUsesDetachedHandoffForConfiguredTarget(t *testing.T) {
	workspace := t.TempDir()
	cfg := deployConfig(writeDeployScript(t, "true"))
	cfg.HandoffTargets = []string{"current"}
	runner, err := NewDeployRunner(cfg, workspace, "picoclaw-main.service")
	if err != nil {
		t.Fatal(err)
	}
	launcher := &fakeDeployHandoffLauncher{}
	tool := &GatewayDeployTool{runner: runner, launcher: launcher}
	ctx := tools.WithToolTopicID(
		tools.WithToolContext(context.Background(), "telegram", "chat-1"), "topic-1",
	)
	result := tool.Execute(ctx, nil)
	if result.Err != nil {
		t.Fatalf("Execute() error = %v", result.Err)
	}
	if !launcher.called || launcher.target != "current" {
		t.Fatalf("launcher = %#v", launcher)
	}
	if launcher.origin.Channel != "telegram" || launcher.origin.ChatID != "chat-1" ||
		launcher.origin.TopicID != "topic-1" {
		t.Fatalf("launcher origin = %#v", launcher.origin)
	}
	if !strings.Contains(result.ForUser, "detached worker") {
		t.Fatalf("result = %q", result.ForUser)
	}
}

func TestDeployHandoffUnitNameIsStableAndScopedToGroup(t *testing.T) {
	first := deployHandoffUnitName("picoclaw-local")
	if first != deployHandoffUnitName("picoclaw-local") {
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
	script := writeDeployScript(t, "touch \"$FORGECLAW_WORKSPACE/started\"; sleep 2")
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
