package agent

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/channels"
	"github.com/sipeed/picoclaw/pkg/interactions"
	"github.com/sipeed/picoclaw/pkg/logger"
	"github.com/sipeed/picoclaw/pkg/session"
)

func (al *AgentLoop) scheduleHumanInteractionRecovery(ctx context.Context) {
	if al == nil {
		return
	}
	go al.RecoverHumanInteractions(ctx)
}

// RecoverHumanInteractions retries prompt delivery, claims timeouts, and
// resumes answers whose durable owner disappeared during restart or reload.
func (al *AgentLoop) RecoverHumanInteractions(ctx context.Context) int {
	if al == nil || !al.interactionRecoveryRunning.CompareAndSwap(false, true) {
		return 0
	}
	defer al.interactionRecoveryRunning.Store(false)
	al.loadCatalogedInteractionRegistries()
	recovered := 0
	al.interactionRegistries.Range(func(key, value any) bool {
		if ctx.Err() != nil {
			return false
		}
		workspace, _ := key.(string)
		registry, _ := value.(*interactions.Registry)
		if registry == nil {
			return true
		}
		if claimed, err := registry.ClaimOverdue(time.Now()); err != nil {
			logger.WarnCF("agent", "Failed to claim overdue interactions", map[string]any{
				"workspace": workspace, "error": err.Error(),
			})
		} else if len(claimed) > 0 {
			logger.InfoCF("agent", "Claimed overdue human interactions", map[string]any{
				"workspace": workspace, "count": len(claimed),
			})
		}
		for _, record := range registry.ListNonterminal() {
			if ctx.Err() != nil {
				return false
			}
			if !al.interactionAgentAvailable(workspace, record) {
				if _, err := registry.Fail(
					record.ID,
					record.Revision,
					"agent_unavailable",
					"the originating agent or workspace is no longer configured",
				); err == nil {
					recovered++
				}
				continue
			}
			switch record.Status {
			case interactions.StatusCreated:
				if record.PromptDelivered {
					if _, err := registry.MarkWaiting(record.ID, record.Revision); err == nil {
						recovered++
					}
				} else if record.PromptDeliveryState == interactions.DeliveryStateSending ||
					record.PromptDeliveryState == interactions.DeliveryStateAmbiguous {
					claimed, err := registry.ClaimDeliveryUnknown(record.ID, record.Revision)
					if err == nil && al.recoverClaimedInteraction(ctx, workspace, claimed) {
						recovered++
					}
				} else if al.retryInteractionPrompt(ctx, registry, record) {
					recovered++
				}
			case interactions.StatusResuming:
				if record.FinalDeliveryState == interactions.DeliveryStateSending ||
					record.FinalDeliveryState == interactions.DeliveryStateAmbiguous {
					if _, err := registry.Fail(
						record.ID,
						record.Revision,
						"final_delivery_ambiguous",
						"final response delivery could not be confirmed and was not retried",
					); err == nil {
						recovered++
						_ = al.drainDeferredInteractionIngress(
							ctx,
							record.Route,
							inboundContextForInteraction(record.Route),
						)
					}
				} else if al.recoverClaimedInteraction(ctx, workspace, record) {
					recovered++
				}
			case interactions.StatusClaimed:
				if al.recoverClaimedInteraction(ctx, workspace, record) {
					recovered++
				}
			case interactions.StatusCanceling:
				if al.recoverCancelingInteraction(ctx, workspace, registry, record) {
					recovered++
				}
			}
		}
		_ = registry.Prune(time.Now())
		if registry.Stats().RecordCount == 0 && al.interactionCatalog != nil {
			_ = al.interactionCatalog.Remove(workspace)
		}
		return true
	})
	return recovered
}

func (al *AgentLoop) loadCatalogedInteractionRegistries() {
	if al == nil || al.interactionCatalog == nil {
		return
	}
	workspaces, err := al.interactionCatalog.List()
	if err != nil {
		logger.WarnCF("agent", "Interaction workspace catalog has invalid entries", map[string]any{
			"error": err.Error(),
		})
	}
	for _, workspace := range workspaces {
		_ = al.interactionRegistryForWorkspace(workspace)
	}
}

func (al *AgentLoop) interactionAgentAvailable(
	workspace string,
	record interactions.Record,
) bool {
	if al == nil {
		return false
	}
	registry := al.GetRegistry()
	if registry == nil {
		// Isolated runtimes can reconcile store-only transitions without an
		// agent registry. Production loops always provide one.
		return true
	}
	agent, ok := registry.GetAgent(record.Route.AgentID)
	return ok && agent != nil &&
		strings.TrimSpace(agent.Workspace) == strings.TrimSpace(workspace)
}

func (al *AgentLoop) recoverCancelingInteraction(
	ctx context.Context,
	workspace string,
	registry *interactions.Registry,
	record interactions.Record,
) bool {
	agentRegistry := al.GetRegistry()
	if agentRegistry == nil {
		return false
	}
	agent, ok := agentRegistry.GetAgent(record.Route.AgentID)
	if !ok || agent == nil || strings.TrimSpace(agent.Workspace) != strings.TrimSpace(workspace) {
		return false
	}
	routeSessionKey := record.Route.RouteSessionKey
	if routeSessionKey == "" {
		routeSessionKey = record.Route.SessionKey
	}
	target := &inboundDispatchTarget{
		Agent:         agent,
		RouteClaimKey: runtimeRouteClaimKey(routeSessionKey, ""),
		Allocation: session.Allocation{
			RouteScopeKey: routeSessionKey,
			SessionKey:    record.Route.SessionKey,
		},
		SessionKey: record.Route.SessionKey,
	}
	claim, _, claimed := al.claimRuntimeRouteSession(
		target,
		fmt.Sprintf("pending-interaction-cancel-recovery-%s-%d", record.ShortID, al.turnSeq.Add(1)),
	)
	if !claimed {
		return false
	}
	defer claim.releaseIfOwned()
	if err := al.ensureInteractionCancellationToolResult(
		ctx,
		agent,
		record,
		record.FailureCode,
	); err != nil {
		return false
	}
	if _, err := registry.CompleteCancellation(record.ID, record.Revision); err != nil {
		return false
	}
	_ = al.drainDeferredInteractionIngress(
		ctx,
		record.Route,
		inboundContextForInteraction(record.Route),
	)
	return true
}

func (al *AgentLoop) retryInteractionPrompt(
	ctx context.Context,
	registry *interactions.Registry,
	record interactions.Record,
) bool {
	if al.channelManager == nil {
		_, _ = registry.RecordDeliveryAttempt(
			record.ID,
			record.Revision,
			false,
			"channel manager unavailable",
		)
		return false
	}
	started, err := registry.BeginPromptDelivery(record.ID, record.Revision)
	if err != nil {
		return false
	}
	deliveryErr := al.humanInteractionRuntime().publishPrompt(ctx, started)
	updated, err := registry.CompletePromptDelivery(
		started.ID,
		started.Revision,
		deliveryErr == nil,
		deliveryErr != nil && !channels.DeliveryDefinitelyNotSent(deliveryErr),
		errString(deliveryErr),
	)
	if err != nil || deliveryErr != nil {
		return false
	}
	if _, err := registry.MarkWaiting(updated.ID, updated.Revision); err != nil {
		return false
	}
	return true
}

func (al *AgentLoop) recoverClaimedInteraction(
	ctx context.Context,
	workspace string,
	record interactions.Record,
) bool {
	agentRegistry := al.GetRegistry()
	if agentRegistry == nil {
		return false
	}
	agent, ok := agentRegistry.GetAgent(record.Route.AgentID)
	if !ok || agent == nil || strings.TrimSpace(agent.Workspace) != strings.TrimSpace(workspace) {
		return false
	}
	scope := sessionScopeForRecovery(agent.Sessions, record.Route.SessionKey)
	if scope == nil {
		scope = &session.SessionScope{
			Version:       1,
			AgentID:       record.Route.AgentID,
			Channel:       record.Route.Channel,
			RouteScopeKey: record.Route.RouteSessionKey,
		}
	}
	routeSessionKey := record.Route.RouteSessionKey
	if routeSessionKey == "" {
		routeSessionKey = record.Route.SessionKey
	}
	target := &inboundDispatchTarget{
		Agent:         agent,
		RouteClaimKey: runtimeRouteClaimKey(routeSessionKey, ""),
		Allocation: session.Allocation{
			RouteScopeKey: routeSessionKey,
			SessionKey:    record.Route.SessionKey,
			Scope:         *session.CloneScope(scope),
		},
		SessionKey: record.Route.SessionKey,
	}
	claim, _, claimed := al.claimRuntimeRouteSession(
		target,
		fmt.Sprintf("pending-interaction-recovery-%s-%d", record.ShortID, al.turnSeq.Add(1)),
	)
	if !claimed {
		return false
	}
	defer claim.releaseIfOwned()
	if err := al.resumeClaimedInteraction(
		ctx,
		agent,
		scope,
		inboundContextForInteraction(record.Route),
		record,
	); err != nil {
		logger.WarnCF("agent", "Failed to recover human interaction", map[string]any{
			"interaction_id": record.ID,
			"session_key":    record.Route.SessionKey,
			"error":          err.Error(),
		})
		return false
	}
	if err := al.drainDeferredInteractionIngress(
		ctx,
		record.Route,
		inboundContextForInteraction(record.Route),
	); err != nil {
		logger.WarnCF("agent", "Failed to continue messages after interaction recovery", map[string]any{
			"interaction_id": record.ID,
			"session_key":    record.Route.SessionKey,
			"error":          err.Error(),
		})
	}
	return true
}

func inboundContextForInteraction(route interactions.Route) bus.InboundContext {
	return bus.InboundContext{
		Channel:   route.Channel,
		Account:   route.AccountID,
		ChatID:    route.ChatID,
		ChatType:  route.ChatType,
		TopicID:   route.TopicID,
		SpaceID:   route.SpaceID,
		SpaceType: route.SpaceType,
		SenderID:  route.SenderID,
	}
}
