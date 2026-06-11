package agent

import (
	"fmt"
	"strings"
	"time"

	"github.com/sipeed/picoclaw/pkg/logger"
	"github.com/sipeed/picoclaw/pkg/providers"
	"github.com/sipeed/picoclaw/pkg/state"
)

type modelSelectionDecision struct {
	selectedCandidates []providers.FallbackCandidate
	activeCandidates   []providers.FallbackCandidate
	model              string
	usedLight          bool
}

func autoFallbackTTL(reason providers.FailoverReason) (time.Duration, bool) {
	switch reason {
	case providers.FailoverRateLimit, providers.FailoverOverloaded:
		return 20 * time.Minute, true
	case providers.FailoverBilling:
		return 6 * time.Hour, true
	default:
		return 0, false
	}
}

func candidateMatchesSelection(candidate providers.FallbackCandidate, sel state.AutoModelSelection) bool {
	return providers.ModelKey(candidate.Provider, candidate.Model) ==
		providers.ModelKey(sel.ActiveProvider, sel.ActiveModel)
}

func reorderCandidatesForAutoFallback(
	candidates []providers.FallbackCandidate,
	sel state.AutoModelSelection,
) ([]providers.FallbackCandidate, bool) {
	if len(candidates) < 2 {
		return candidates, false
	}
	matchIdx := -1
	for i, candidate := range candidates {
		if candidateMatchesSelection(candidate, sel) {
			matchIdx = i
			break
		}
	}
	if matchIdx <= 0 {
		return candidates, matchIdx == 0
	}

	reordered := make([]providers.FallbackCandidate, 0, len(candidates))
	reordered = append(reordered, candidates[matchIdx])
	reordered = append(reordered, candidates[:matchIdx]...)
	reordered = append(reordered, candidates[matchIdx+1:]...)
	return reordered, true
}

func normalizeSelection(sel state.AutoModelSelection) state.AutoModelSelection {
	sel.SelectedProvider = providers.NormalizeProvider(sel.SelectedProvider)
	sel.ActiveProvider = providers.NormalizeProvider(sel.ActiveProvider)
	sel.SelectedModel = strings.TrimSpace(sel.SelectedModel)
	sel.ActiveModel = strings.TrimSpace(sel.ActiveModel)
	sel.Reason = strings.TrimSpace(sel.Reason)
	return sel
}

func (al *AgentLoop) getAutoModelSelection(routeSessionKey string) (state.AutoModelSelection, bool) {
	if al == nil || al.state == nil {
		return state.AutoModelSelection{}, false
	}
	sel, ok := al.state.GetAutoModelSelection(routeSessionKey)
	if !ok {
		return state.AutoModelSelection{}, false
	}
	return normalizeSelection(sel), true
}

func (al *AgentLoop) setAutoModelSelection(routeSessionKey string, selection state.AutoModelSelection) error {
	if al == nil || al.state == nil {
		return fmt.Errorf("state manager not initialized")
	}
	return al.state.SetAutoModelSelection(routeSessionKey, normalizeSelection(selection))
}

func (al *AgentLoop) clearAutoModelSelection(routeSessionKey string) error {
	if al == nil || al.state == nil {
		return fmt.Errorf("state manager not initialized")
	}
	return al.state.ClearAutoModelSelection(routeSessionKey)
}

func (al *AgentLoop) selectCandidates(
	agent *AgentInstance,
	userMsg string,
	history []providers.Message,
	routeSessionKey string,
) modelSelectionDecision {
	baseCandidates := agent.Candidates
	baseModel := resolvedCandidateModel(agent.Candidates, agent.Model)
	usedLight := false

	if agent.Router != nil && len(agent.LightCandidates) > 0 {
		_, usedLightCandidate, score := agent.Router.SelectModel(userMsg, history, agent.Model)
		if usedLightCandidate {
			logger.InfoCF("agent", "Model routing: light model selected",
				map[string]any{
					"agent_id":    agent.ID,
					"light_model": agent.Router.LightModel(),
					"score":       score,
					"threshold":   agent.Router.Threshold(),
				})
			baseCandidates = agent.LightCandidates
			baseModel = resolvedCandidateModel(agent.LightCandidates, agent.Router.LightModel())
			usedLight = true
		} else {
			logger.DebugCF("agent", "Model routing: primary model selected",
				map[string]any{
					"agent_id":  agent.ID,
					"score":     score,
					"threshold": agent.Router.Threshold(),
				})
		}
	}

	decision := modelSelectionDecision{
		selectedCandidates: append([]providers.FallbackCandidate(nil), baseCandidates...),
		activeCandidates:   append([]providers.FallbackCandidate(nil), baseCandidates...),
		model:              baseModel,
		usedLight:          usedLight,
	}

	if usedLight || strings.TrimSpace(routeSessionKey) == "" {
		return decision
	}

	sel, ok := al.getAutoModelSelection(routeSessionKey)
	if !ok {
		return decision
	}
	if sel.ExpiresAt.IsZero() || time.Now().After(sel.ExpiresAt) {
		_ = al.clearAutoModelSelection(routeSessionKey)
		return decision
	}

	reordered, matched := reorderCandidatesForAutoFallback(decision.activeCandidates, sel)
	if !matched {
		_ = al.clearAutoModelSelection(routeSessionKey)
		return decision
	}

	decision.activeCandidates = reordered
	decision.model = resolvedCandidateModel(reordered, decision.model)
	return decision
}

func fallbackReasonForSelection(result *providers.FallbackResult) providers.FailoverReason {
	if result == nil || len(result.Attempts) == 0 {
		return ""
	}
	for _, attempt := range result.Attempts {
		if attempt.Skipped || attempt.Reason == "" {
			continue
		}
		return attempt.Reason
	}
	return ""
}

func updateAutoFallbackSelection(
	al *AgentLoop,
	routeSessionKey string,
	selectedCandidates []providers.FallbackCandidate,
	result *providers.FallbackResult,
) {
	if al == nil || strings.TrimSpace(routeSessionKey) == "" || len(selectedCandidates) == 0 || result == nil {
		return
	}

	selected := selectedCandidates[0]
	winnerKey := providers.ModelKey(result.Provider, result.Model)
	selectedKey := providers.ModelKey(selected.Provider, selected.Model)

	if winnerKey == selectedKey {
		_ = al.clearAutoModelSelection(routeSessionKey)
		return
	}

	reason := fallbackReasonForSelection(result)
	ttl, sticky := autoFallbackTTL(reason)
	if !sticky {
		return
	}

	selection := state.AutoModelSelection{
		SelectedProvider: selected.Provider,
		SelectedModel:    selected.Model,
		ActiveProvider:   result.Provider,
		ActiveModel:      result.Model,
		Reason:           string(reason),
		ExpiresAt:        time.Now().Add(ttl),
	}
	_ = al.setAutoModelSelection(routeSessionKey, selection)
}
