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

func autoModelSelectionLogFields(
	routeSessionKey string,
	sel state.AutoModelSelection,
) map[string]any {
	fields := map[string]any{
		"route_session_key": routeSessionKey,
		"selected_provider": sel.SelectedProvider,
		"selected_model":    sel.SelectedModel,
		"active_provider":   sel.ActiveProvider,
		"active_model":      sel.ActiveModel,
		"failover_reason":   sel.Reason,
	}
	if !sel.ExpiresAt.IsZero() {
		fields["expires_at"] = sel.ExpiresAt.Format(time.RFC3339)
		fields["ttl_remaining_secs"] = max(0, int(time.Until(sel.ExpiresAt).Seconds()))
	}
	return fields
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

func selectedModelMatchesSelection(candidate providers.FallbackCandidate, sel state.AutoModelSelection) bool {
	return providers.ModelKey(candidate.Provider, candidate.Model) ==
		providers.ModelKey(sel.SelectedProvider, sel.SelectedModel)
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

func (m *modelExecutionManager) getAutoModelSelection(routeSessionKey string) (state.AutoModelSelection, bool) {
	if m == nil || m.state == nil {
		return state.AutoModelSelection{}, false
	}
	sel, ok := m.state.GetAutoModelSelection(routeSessionKey)
	if !ok {
		return state.AutoModelSelection{}, false
	}
	return normalizeSelection(sel), true
}

func (m *modelExecutionManager) setAutoModelSelection(
	routeSessionKey string,
	selection state.AutoModelSelection,
) error {
	if m == nil || m.state == nil {
		return fmt.Errorf("state manager not initialized")
	}
	selection = normalizeSelection(selection)
	if err := m.state.SetAutoModelSelection(routeSessionKey, selection); err != nil {
		return err
	}
	logger.InfoCF("agent", "Auto fallback pinned", autoModelSelectionLogFields(routeSessionKey, selection))
	return nil
}

func (m *modelExecutionManager) clearAutoModelSelection(routeSessionKey string) error {
	return m.clearAutoModelSelectionWithReason(routeSessionKey, "explicit")
}

func (m *modelExecutionManager) clearAutoModelSelectionWithReason(routeSessionKey, clearReason string) error {
	if m == nil || m.state == nil {
		return fmt.Errorf("state manager not initialized")
	}
	sel, ok := m.getAutoModelSelection(routeSessionKey)
	if err := m.state.ClearAutoModelSelection(routeSessionKey); err != nil {
		return err
	}
	if ok {
		fields := autoModelSelectionLogFields(routeSessionKey, sel)
		fields["clear_reason"] = strings.TrimSpace(clearReason)
		logger.InfoCF("agent", "Auto fallback cleared", fields)
	}
	return nil
}

func (m *modelExecutionManager) selectCandidates(
	execution effectiveExecutionState,
	userMsg string,
	history []providers.Message,
	routeSessionKey string,
) modelSelectionDecision {
	baseCandidates := execution.Candidates
	baseModel := resolvedCandidateModel(execution.Candidates, execution.Model)
	usedLight := false

	if execution.Router != nil && len(execution.LightCandidates) > 0 {
		_, usedLightCandidate, score := execution.Router.SelectModel(userMsg, history, execution.Model)
		if usedLightCandidate {
			logger.InfoCF("agent", "Model routing: light model selected",
				map[string]any{
					"agent_id":    execution.AgentID,
					"light_model": execution.Router.LightModel(),
					"score":       score,
					"threshold":   execution.Router.Threshold(),
				})
			baseCandidates = execution.LightCandidates
			baseModel = resolvedCandidateModel(execution.LightCandidates, execution.Router.LightModel())
			usedLight = true
		} else {
			logger.DebugCF("agent", "Model routing: primary model selected",
				map[string]any{
					"agent_id":  execution.AgentID,
					"score":     score,
					"threshold": execution.Router.Threshold(),
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

	return m.applyStickyAutoFallback(decision, routeSessionKey)
}

func (m *modelExecutionManager) applyStickyAutoFallback(
	decision modelSelectionDecision,
	routeSessionKey string,
) modelSelectionDecision {
	return m.applyStickyAutoFallbackWithMode(decision, routeSessionKey, true)
}

func (m *modelExecutionManager) previewStickyAutoFallback(
	decision modelSelectionDecision,
	routeSessionKey string,
) modelSelectionDecision {
	return m.applyStickyAutoFallbackWithMode(decision, routeSessionKey, false)
}

func (m *modelExecutionManager) applyStickyAutoFallbackWithMode(
	decision modelSelectionDecision,
	routeSessionKey string,
	mutate bool,
) modelSelectionDecision {
	if strings.TrimSpace(routeSessionKey) == "" || len(decision.selectedCandidates) == 0 {
		return decision
	}

	sel, ok := m.getAutoModelSelection(routeSessionKey)
	if !ok {
		return decision
	}
	if sel.ExpiresAt.IsZero() || time.Now().After(sel.ExpiresAt) {
		if mutate {
			_ = m.clearAutoModelSelectionWithReason(routeSessionKey, "expired")
		}
		return decision
	}
	if !selectedModelMatchesSelection(decision.selectedCandidates[0], sel) {
		if mutate {
			_ = m.clearAutoModelSelectionWithReason(routeSessionKey, "selected_model_mismatch")
		}
		return decision
	}

	reordered, matched := reorderCandidatesForAutoFallback(decision.activeCandidates, sel)
	if !matched {
		if mutate {
			_ = m.clearAutoModelSelectionWithReason(routeSessionKey, "active_candidate_missing")
		}
		return decision
	}

	decision.activeCandidates = reordered
	decision.model = resolvedCandidateModel(reordered, decision.model)
	fields := autoModelSelectionLogFields(routeSessionKey, sel)
	fields["active_candidate_count"] = len(reordered)
	logger.InfoCF("agent", "Auto fallback reused", fields)
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

func (m *modelExecutionManager) updateAutoFallbackSelection(
	routeSessionKey string,
	selectedCandidates []providers.FallbackCandidate,
	result *providers.FallbackResult,
	usedLight bool,
) {
	if m == nil ||
		usedLight ||
		strings.TrimSpace(routeSessionKey) == "" ||
		len(selectedCandidates) == 0 ||
		result == nil {
		return
	}

	selected := selectedCandidates[0]
	winnerKey := providers.ModelKey(result.Provider, result.Model)
	selectedKey := providers.ModelKey(selected.Provider, selected.Model)

	if winnerKey == selectedKey {
		_ = m.clearAutoModelSelectionWithReason(routeSessionKey, "primary_recovered")
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
	_ = m.setAutoModelSelection(routeSessionKey, selection)
}

func (al *AgentLoop) updateAutoFallbackSelection(
	routeSessionKey string,
	selectedCandidates []providers.FallbackCandidate,
	result *providers.FallbackResult,
	usedLight bool,
) {
	manager := al.modelExecutionManager()
	if manager == nil {
		return
	}
	manager.updateAutoFallbackSelection(routeSessionKey, selectedCandidates, result, usedLight)
}

func (al *AgentLoop) getAutoModelSelection(routeSessionKey string) (state.AutoModelSelection, bool) {
	manager := al.modelExecutionManager()
	if manager == nil {
		return state.AutoModelSelection{}, false
	}
	return manager.getAutoModelSelection(routeSessionKey)
}

func (al *AgentLoop) setAutoModelSelection(
	routeSessionKey string,
	selection state.AutoModelSelection,
) error {
	manager := al.modelExecutionManager()
	if manager == nil {
		return fmt.Errorf("model execution manager not initialized")
	}
	return manager.setAutoModelSelection(routeSessionKey, selection)
}

func (al *AgentLoop) clearAutoModelSelection(routeSessionKey string) error {
	manager := al.modelExecutionManager()
	if manager == nil {
		return fmt.Errorf("model execution manager not initialized")
	}
	return manager.clearAutoModelSelection(routeSessionKey)
}

func (al *AgentLoop) selectCandidates(
	execution effectiveExecutionState,
	userMsg string,
	history []providers.Message,
	routeSessionKey string,
) modelSelectionDecision {
	manager := al.modelExecutionManager()
	if manager == nil {
		return modelSelectionDecision{}
	}
	return manager.selectCandidates(execution, userMsg, history, routeSessionKey)
}

func (al *AgentLoop) previewStickyAutoFallback(
	decision modelSelectionDecision,
	routeSessionKey string,
) modelSelectionDecision {
	manager := al.modelExecutionManager()
	if manager == nil {
		return decision
	}
	return manager.previewStickyAutoFallback(decision, routeSessionKey)
}
