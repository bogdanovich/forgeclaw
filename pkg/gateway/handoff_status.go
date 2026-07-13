package gateway

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/sipeed/picoclaw/pkg/agent"
	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/logger"
	"github.com/sipeed/picoclaw/pkg/tools"
)

const handoffOutputPreviewLimit = 1024

const deployHandoffPollInterval = time.Second

type gatewayHandoffStatusTool struct {
	restartStore *RestartSentinelStore
	deployStore  *DeploySentinelStore
}

func (t *gatewayHandoffStatusTool) Name() string { return "gateway_handoff_status" }

func (t *gatewayHandoffStatusTool) Description() string {
	return "Show the latest gateway restart and deploy handoff status."
}

func (t *gatewayHandoffStatusTool) Parameters() map[string]any {
	return map[string]any{"type": "object", "properties": map[string]any{}}
}

func (t *gatewayHandoffStatusTool) Execute(_ context.Context, _ map[string]any) *tools.ToolResult {
	return tools.UserResult(formatGatewayHandoffStatus(t.restartStore, t.deployStore))
}

func newGatewayHandoffStatusTool(cfg *config.Config) (tools.Tool, error) {
	if cfg == nil || (!cfg.Gateway.SafeRestart.Enabled && !cfg.Gateway.Deploy.Enabled) {
		return nil, nil
	}
	restartStore, err := NewRestartSentinelStore(restartSentinelDir(cfg))
	if err != nil {
		return nil, err
	}
	deployStore, err := NewDeploySentinelStore(deploySentinelDir(cfg))
	if err != nil {
		return nil, err
	}
	return &gatewayHandoffStatusTool{restartStore: restartStore, deployStore: deployStore}, nil
}

func setupGatewayHandoffStatusTool(cfg *config.Config, agentLoop *agent.AgentLoop) error {
	if cfg == nil || (!cfg.Gateway.SafeRestart.Enabled && !cfg.Gateway.Deploy.Enabled) {
		return nil
	}
	return agentLoop.RegisterRuntimeTool("gateway_handoff_status", newGatewayHandoffStatusTool)
}

func reportGatewayHandoffStatus(ctx context.Context, cfg *config.Config, msgBus *bus.MessageBus) {
	if cfg == nil || msgBus == nil || (!cfg.Gateway.SafeRestart.Enabled && !cfg.Gateway.Deploy.Enabled) {
		return
	}
	restartStore, err := NewRestartSentinelStore(restartSentinelDir(cfg))
	if err != nil {
		logger.WarnCF("gateway", "Failed to open restart sentinel store", map[string]any{"error": err.Error()})
		return
	}
	deployStore, err := NewDeploySentinelStore(deploySentinelDir(cfg))
	if err != nil {
		logger.WarnCF("gateway", "Failed to open deploy sentinel store", map[string]any{"error": err.Error()})
		return
	}
	if cfg.Gateway.SafeRestart.Enabled {
		reportRestartHandoff(ctx, msgBus, restartStore)
	}
	if cfg.Gateway.Deploy.Enabled {
		if reportDeployHandoff(ctx, msgBus, deployStore) {
			go waitForDeployHandoff(ctx, msgBus, deployStore)
		}
	}
}

func reportRestartHandoff(ctx context.Context, msgBus *bus.MessageBus, store *RestartSentinelStore) {
	sentinel, err := store.Read()
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			logger.WarnCF("gateway", "Failed to read restart sentinel", map[string]any{"error": err.Error()})
		}
		return
	}
	if recovered, ok, recoverErr := store.MarkInterruptedRestartComplete(time.Now().UTC()); recoverErr != nil {
		logger.WarnCF("gateway", "Failed to recover restart sentinel", map[string]any{"error": recoverErr.Error()})
	} else if ok {
		sentinel = recovered
	}
	logger.InfoCF("gateway", "Latest restart sentinel", restartLogFields(sentinel))
	if !sentinel.ContinuationSentAt.IsZero() {
		return
	}
	content := formatRestartContinuation(sentinel)
	if err := publishHandoffContinuation(ctx, msgBus, sentinel.Origin, content); err != nil {
		logger.WarnCF("gateway", "Failed to publish restart continuation", map[string]any{"error": err.Error()})
		return
	}
	if err := store.MarkContinuationSent(time.Now().UTC()); err != nil {
		logger.WarnCF("gateway", "Failed to mark restart continuation sent", map[string]any{"error": err.Error()})
	}
}

// reportDeployHandoff returns true while a detached worker is still running.
func reportDeployHandoff(ctx context.Context, msgBus *bus.MessageBus, store *DeploySentinelStore) bool {
	sentinel, err := store.Read()
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			logger.WarnCF("gateway", "Failed to read deploy sentinel", map[string]any{"error": err.Error()})
		}
		return false
	}
	logger.InfoCF("gateway", "Latest deploy sentinel", deployLogFields(sentinel))
	if !sentinel.Handoff {
		return false
	}
	if sentinel.Status == "running" {
		return true
	}
	if !sentinel.ContinuationSentAt.IsZero() {
		return false
	}
	content := formatDeployContinuation(sentinel)
	if err := publishHandoffContinuation(ctx, msgBus, sentinel.Origin, content); err != nil {
		logger.WarnCF("gateway", "Failed to publish deploy continuation", map[string]any{"error": err.Error()})
		return false
	}
	if err := store.MarkContinuationSent(time.Now().UTC()); err != nil {
		logger.WarnCF("gateway", "Failed to mark deploy continuation sent", map[string]any{"error": err.Error()})
	}
	return false
}

func waitForDeployHandoff(ctx context.Context, msgBus *bus.MessageBus, store *DeploySentinelStore) {
	ticker := time.NewTicker(deployHandoffPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !reportDeployHandoff(ctx, msgBus, store) {
				return
			}
		}
	}
}

func publishHandoffContinuation(
	ctx context.Context,
	msgBus *bus.MessageBus,
	origin RestartOrigin,
	content string,
) error {
	if strings.TrimSpace(origin.Channel) == "" || strings.TrimSpace(origin.ChatID) == "" {
		return errors.New("handoff origin does not include channel and chat ID")
	}
	outboundContext := bus.NewOutboundContext(origin.Channel, origin.ChatID, "")
	outboundContext.TopicID = strings.TrimSpace(origin.TopicID)
	return msgBus.PublishOutbound(ctx, bus.OutboundMessage{
		Context:    outboundContext,
		SessionKey: origin.SessionKey,
		Content:    content,
	})
}

func formatGatewayHandoffStatus(restartStore *RestartSentinelStore, deployStore *DeploySentinelStore) string {
	var lines []string
	if restartStore != nil {
		if sentinel, err := restartStore.Read(); err == nil {
			lines = append(lines, formatRestartStatus(sentinel))
		} else if !errors.Is(err, os.ErrNotExist) {
			lines = append(lines, "Restart handoff: unavailable ("+err.Error()+")")
		}
	}
	if deployStore != nil {
		if sentinel, err := deployStore.Read(); err == nil {
			lines = append(lines, formatDeployStatus(sentinel))
		} else if !errors.Is(err, os.ErrNotExist) {
			lines = append(lines, "Deploy handoff: unavailable ("+err.Error()+")")
		}
	}
	if len(lines) == 0 {
		return "No restart or deploy handoff has been recorded."
	}
	return strings.Join(lines, "\n\n")
}

func formatRestartStatus(s RestartSentinel) string {
	return fmt.Sprintf("Restart handoff\nStatus: %s\nService: %s\nRequested: %s\nContinuation sent: %t",
		s.Status, s.RequestedService, s.RequestedAt.Format(time.RFC3339), !s.ContinuationSentAt.IsZero())
}

func formatDeployStatus(s DeploySentinel) string {
	result := fmt.Sprintf(
		"Deploy handoff\nStatus: %s\nGroup/target: %s/%s\nExit code: %d\nRequested: %s\nContinuation sent: %t",
		s.Status, s.Group, s.Target, s.ExitCode, s.RequestedAt.Format(time.RFC3339), !s.ContinuationSentAt.IsZero())
	if output := truncateHandoffOutput(s.OutputTail); output != "" {
		result += "\nOutput tail:\n" + output
	}
	return result
}

func formatRestartContinuation(s RestartSentinel) string {
	return fmt.Sprintf("Gateway is back. Restart handoff for %s is %s.", s.RequestedService, s.Status)
}

func formatDeployContinuation(s DeploySentinel) string {
	return fmt.Sprintf("Gateway is back. Deploy handoff for %s target %s is %s (exit code %d).",
		s.Group, s.Target, s.Status, s.ExitCode)
}

func restartLogFields(s RestartSentinel) map[string]any {
	return map[string]any{
		"status": s.Status, "requested_service": s.RequestedService,
		"requested_at": s.RequestedAt, "updated_at": s.UpdatedAt,
		"continuation_sent": !s.ContinuationSentAt.IsZero(),
	}
}

func deployLogFields(s DeploySentinel) map[string]any {
	return map[string]any{
		"status": s.Status, "group": s.Group, "target": s.Target,
		"exit_code": s.ExitCode, "requested_at": s.RequestedAt, "updated_at": s.UpdatedAt,
		"continuation_sent": !s.ContinuationSentAt.IsZero(),
	}
}

func truncateHandoffOutput(output string) string {
	if len(output) <= handoffOutputPreviewLimit {
		return output
	}
	return "[output truncated]\n" + output[len(output)-handoffOutputPreviewLimit:]
}
