package gateway

import (
	"context"
	"crypto/sha256"
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

var ErrDeployAlreadyRunning = errors.New("gateway deploy is already running")

type DeploySentinel struct {
	Kind               string        `json:"kind"`
	Status             string        `json:"status"`
	Group              string        `json:"group"`
	Target             string        `json:"target"`
	Command            string        `json:"command"`
	OutputTail         string        `json:"output_tail,omitempty"`
	ExitCode           int           `json:"exit_code"`
	Handoff            bool          `json:"handoff,omitempty"`
	Origin             RestartOrigin `json:"origin"`
	RequestedAt        time.Time     `json:"requested_at"`
	UpdatedAt          time.Time     `json:"updated_at"`
	ContinuationSentAt time.Time     `json:"continuation_sent_at,omitempty"`
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

func (s *DeploySentinelStore) Read() (DeploySentinel, error) {
	data, err := os.ReadFile(s.path)
	if err != nil {
		return DeploySentinel{}, err
	}
	var sentinel DeploySentinel
	if err := json.Unmarshal(data, &sentinel); err != nil {
		return DeploySentinel{}, fmt.Errorf("decode deploy sentinel: %w", err)
	}
	return sentinel, nil
}

func (s *DeploySentinelStore) MarkContinuationSent(now time.Time) error {
	sentinel, err := s.Read()
	if err != nil {
		return err
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	sentinel.ContinuationSentAt = now.UTC()
	sentinel.UpdatedAt = now.UTC()
	return s.Write(sentinel)
}

func deployGroupLockPath(group string) (string, error) {
	cacheDir, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("resolve deploy lock directory: %w", err)
	}
	dir := filepath.Join(cacheDir, "picoclaw", "deploy-locks")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("create deploy lock directory: %w", err)
	}
	hash := sha256.Sum256([]byte(group))
	return filepath.Join(dir, fmt.Sprintf("%x.lock", hash)), nil
}

type DeployRunner struct {
	cfg                config.GatewayDeployConfig
	workspace, service string
	store              *DeploySentinelStore
	lockPath           string
}

func NewDeployRunner(
	cfg config.GatewayDeployConfig,
	workspace, service string,
) (*DeployRunner, error) {
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
	lockPath, err := deployGroupLockPath(cfg.Group)
	if err != nil {
		return nil, err
	}
	return &DeployRunner{
		cfg:       cfg,
		workspace: workspace,
		service:   service,
		store:     store,
		lockPath:  lockPath,
	}, nil
}

func (r *DeployRunner) Run(
	ctx context.Context,
	target string,
	origin RestartOrigin,
) (string, int, error) {
	return r.run(ctx, target, origin, false)
}

// RunHandoffWorker executes a deploy that was detached from the gateway
// service because the target may restart that service.
func (r *DeployRunner) RunHandoffWorker(
	ctx context.Context,
	target string,
	origin RestartOrigin,
) (string, int, error) {
	return r.run(ctx, target, origin, true)
}

func (r *DeployRunner) run(
	ctx context.Context,
	target string,
	origin RestartOrigin,
	handoff bool,
) (string, int, error) {
	target = strings.TrimSpace(target)
	if target == "" {
		target = strings.TrimSpace(r.cfg.DefaultTarget)
	}
	if !containsTarget(r.cfg.AllowedTargets, target) {
		return "", -1, fmt.Errorf("deploy target %q is not allowed", target)
	}
	lock, err := acquireDeployLock(r.lockPath)
	if err != nil {
		return "", -1, err
	}
	defer lock.Close()
	now := time.Now().UTC()
	sentinel := DeploySentinel{
		Kind:        "deploy",
		Status:      "running",
		Group:       r.cfg.Group,
		Target:      target,
		Command:     r.cfg.Command,
		ExitCode:    -1,
		Handoff:     handoff,
		Origin:      origin,
		RequestedAt: now,
		UpdatedAt:   now,
	}
	if writeErr := r.store.Write(sentinel); writeErr != nil {
		return "", -1, writeErr
	}
	ctx, cancel := context.WithTimeout(
		ctx,
		time.Duration(r.cfg.EffectiveTimeoutSeconds())*time.Second,
	)
	defer cancel()
	cmd := exec.CommandContext(ctx, r.cfg.Command, "--target", target)
	cmd.Env = append(cmd.Environ(),
		"FORGECLAW_DEPLOY_GROUP="+r.cfg.Group,
		"FORGECLAW_DEPLOY_TARGET="+target,
		"FORGECLAW_WORKSPACE="+r.workspace,
		"FORGECLAW_SERVICE="+r.service,
		"FORGECLAW_SESSION_KEY="+origin.SessionKey,
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

type GatewayDeployTool struct {
	runner   *DeployRunner
	launcher DeployHandoffLauncher
}

func (t *GatewayDeployTool) Name() string { return "gateway_deploy" }

func (t *GatewayDeployTool) ToolSteeringSafety(map[string]any) tools.SteeringSafety {
	return tools.SteeringSafetyCancellable
}

func (t *GatewayDeployTool) Description() string {
	return "Run the configured deploy script for an allowed target."
}

func (t *GatewayDeployTool) Parameters() map[string]any {
	return map[string]any{
		"type":       "object",
		"properties": map[string]any{"target": map[string]any{"type": "string"}},
	}
}

func (t *GatewayDeployTool) Execute(ctx context.Context, args map[string]any) *tools.ToolResult {
	target, _ := args["target"].(string)
	origin := RestartOrigin{
		Channel:    tools.ToolChannel(ctx),
		ChatID:     tools.ToolChatID(ctx),
		TopicID:    tools.ToolTopicID(ctx),
		SessionKey: tools.ToolSessionKey(ctx),
	}
	if target = strings.TrimSpace(target); target == "" {
		target = strings.TrimSpace(t.runner.cfg.DefaultTarget)
	}
	if t.runner.cfg.RequiresHandoff(target) && t.launcher != nil {
		if err := t.launcher.Launch(ctx, t.runner, target, origin); err != nil {
			return tools.ErrorResult(fmt.Sprintf("gateway deploy handoff failed: %v", err)).
				WithError(err)
		}
		return tools.UserResult("Deploy started in a detached worker. The final handoff status will be reported after completion.").
			WithImmediateDelivery()
	}
	out, _, err := t.runner.Run(ctx, target, origin)
	if err != nil {
		return tools.ErrorResult(fmt.Sprintf("gateway deploy failed: %v\n%s", err, out)).
			WithError(err)
	}
	return tools.UserResult(out).WithImmediateDelivery()
}
