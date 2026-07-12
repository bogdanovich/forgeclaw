package gateway

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sipeed/picoclaw/pkg/config"
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
	out, code, err := runner.Run(context.Background(), "current", "session-1")
	if err != nil || code != 0 || out != "--target:current" {
		t.Fatalf("Run() = %q, %d, %v", out, code, err)
	}
	if _, _, runErr := runner.Run(context.Background(), "bad", ""); runErr == nil {
		t.Fatal("expected invalid target error")
	}

	data, err := os.ReadFile(filepath.Join(workspace, "state", "gateway-deploy", "deploy-sentinel.json"))
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
	if sentinel.Command != script || sentinel.ExitCode != 0 || sentinel.Origin.SessionKey != "session-1" {
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

func TestDeployRunnerFailureTimeoutAndTruncation(t *testing.T) {
	t.Run("failure", func(t *testing.T) {
		r, _ := NewDeployRunner(deployConfig(writeDeployScript(t, "echo fail; exit 7")), t.TempDir(), "")
		_, code, err := r.Run(context.Background(), "", "")
		if err == nil || code != 7 {
			t.Fatalf("code=%d err=%v", code, err)
		}
	})
	t.Run("timeout", func(t *testing.T) {
		r, _ := NewDeployRunner(deployConfig(writeDeployScript(t, "sleep 2")), t.TempDir(), "")
		_, _, err := r.Run(context.Background(), "", "")
		if err == nil || !strings.Contains(err.Error(), "timed out") {
			t.Fatalf("err=%v", err)
		}
	})
	t.Run("truncation", func(t *testing.T) {
		script := writeDeployScript(t, "head -c 20000 /dev/zero | tr '\\000' x")
		r, _ := NewDeployRunner(deployConfig(script), t.TempDir(), "")
		out, _, err := r.Run(context.Background(), "", "")
		if err != nil || !strings.HasPrefix(out, "[output truncated]") {
			t.Fatalf("len=%d err=%v", len(out), err)
		}
	})
}
