package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/fileutil"
	"github.com/sipeed/picoclaw/pkg/tools"
)

const deployOutputLimit = 16 * 1024

type DeploySentinel struct {
	Kind        string        `json:"kind"`
	Status      string        `json:"status"`
	Group       string        `json:"group"`
	Target      string        `json:"target"`
	Command     string        `json:"command"`
	OutputTail  string        `json:"output_tail,omitempty"`
	ExitCode    int           `json:"exit_code"`
	Origin      RestartOrigin `json:"origin"`
	RequestedAt time.Time     `json:"requested_at"`
	UpdatedAt   time.Time     `json:"updated_at"`
}
type DeploySentinelStore struct{ path string }

func NewDeploySentinelStore(dir string) (*DeploySentinelStore, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	return &DeploySentinelStore{path: filepath.Join(dir, "deploy-sentinel.json")}, nil
}

func (s *DeploySentinelStore) Write(v DeploySentinel) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return fileutil.WriteFileAtomic(s.path, b, 0o600)
}

type DeployRunner struct {
	cfg                config.GatewayDeployConfig
	workspace, service string
	store              *DeploySentinelStore
}

func NewDeployRunner(cfg config.GatewayDeployConfig, workspace, service string) (*DeployRunner, error) {
	if !cfg.Enabled {
		return nil, errors.New("deploy is disabled")
	}
	if !filepath.IsAbs(strings.TrimSpace(cfg.Command)) {
		return nil, errors.New("deploy command must be an absolute path")
	}
	if strings.TrimSpace(cfg.Group) == "" {
		return nil, errors.New("deploy group is required")
	}
	if len(cfg.AllowedTargets) == 0 {
		return nil, errors.New("deploy allowed_targets is required")
	}
	store, err := NewDeploySentinelStore(filepath.Join(workspace, "state", "gateway-deploy"))
	if err != nil {
		return nil, err
	}
	return &DeployRunner{cfg: cfg, workspace: workspace, service: service, store: store}, nil
}

func (r *DeployRunner) Run(ctx context.Context, target, sessionKey string) (string, int, error) {
	target = strings.TrimSpace(target)
	if target == "" {
		target = strings.TrimSpace(r.cfg.DefaultTarget)
	}
	if !containsTarget(r.cfg.AllowedTargets, target) {
		return "", -1, fmt.Errorf("deploy target %q is not allowed", target)
	}
	now := time.Now().UTC()
	sentinel := DeploySentinel{
		Kind:        "deploy",
		Status:      "running",
		Group:       r.cfg.Group,
		Target:      target,
		Command:     r.cfg.Command,
		ExitCode:    -1,
		Origin:      RestartOrigin{SessionKey: sessionKey},
		RequestedAt: now,
		UpdatedAt:   now,
	}
	if err := r.store.Write(sentinel); err != nil {
		return "", -1, err
	}
	ctx, cancel := context.WithTimeout(ctx, time.Duration(r.cfg.EffectiveTimeoutSeconds())*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, r.cfg.Command, "--target", target)
	cmd.Env = append(cmd.Environ(),
		"FORGECLAW_DEPLOY_GROUP="+r.cfg.Group,
		"FORGECLAW_DEPLOY_TARGET="+target,
		"FORGECLAW_WORKSPACE="+r.workspace,
		"FORGECLAW_SERVICE="+r.service,
		"FORGECLAW_SESSION_KEY="+sessionKey,
	)
	out, err := cmd.CombinedOutput()
	text := truncateDeployOutput(string(out))
	code := 0
	if err != nil {
		code = -1
		if exit, ok := err.(*exec.ExitError); ok {
			code = exit.ExitCode()
		}
	}
	sentinel.OutputTail, sentinel.ExitCode, sentinel.UpdatedAt = text, code, time.Now().UTC()
	sentinel.Status = "succeeded"
	if err != nil || ctx.Err() == context.DeadlineExceeded {
		sentinel.Status = "failed"
	}
	_ = r.store.Write(sentinel)
	if ctx.Err() == context.DeadlineExceeded {
		return text, -1, fmt.Errorf("deploy timed out")
	}
	if err != nil {
		return text, code, err
	}
	return text, 0, nil
}

func containsTarget(targets []string, target string) bool {
	for _, item := range targets {
		if strings.TrimSpace(item) == target {
			return true
		}
	}
	return false
}
func truncateDeployOutput(s string) string {
	if len(s) <= deployOutputLimit {
		return s
	}
	return "[output truncated]\n" + s[len(s)-deployOutputLimit:]
}

type GatewayDeployTool struct{ runner *DeployRunner }

func (t *GatewayDeployTool) Name() string { return "gateway_deploy" }
func (t *GatewayDeployTool) Description() string {
	return "Run the configured deploy script for an allowed target."
}
func (t *GatewayDeployTool) Parameters() map[string]any {
	return map[string]any{"type": "object", "properties": map[string]any{"target": map[string]any{"type": "string"}}}
}
func (t *GatewayDeployTool) Execute(ctx context.Context, args map[string]any) *tools.ToolResult {
	target, _ := args["target"].(string)
	out, _, err := t.runner.Run(ctx, target, tools.ToolSessionKey(ctx))
	if err != nil {
		return tools.ErrorResult(fmt.Sprintf("gateway deploy failed: %v\n%s", err, out)).WithError(err)
	}
	return tools.UserResult(out).WithImmediateDelivery()
}
