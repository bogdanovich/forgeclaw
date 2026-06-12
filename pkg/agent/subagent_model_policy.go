package agent

import (
	"strings"

	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/logger"
)

const (
	subagentSessionModelOverrideIgnore       = "ignore"
	subagentSessionModelOverrideInherit      = "inherit"
	subagentSessionModelOverrideFallbackOnly = "fallback_only"
)

type subagentModelPlan struct {
	Primary        string
	Fallbacks      []string
	Mode           string
	ParentOverride string
}

func normalizedModelName(raw string) string {
	return strings.ToLower(strings.TrimSpace(raw))
}

func sameFallbackChain(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if normalizedModelName(left[i]) != normalizedModelName(right[i]) {
			return false
		}
	}
	return true
}

func subagentPlanMatchesAgent(plan subagentModelPlan, targetAgent *AgentInstance) bool {
	if targetAgent == nil {
		return false
	}
	if normalizedModelName(plan.Primary) != normalizedModelName(targetAgent.Model) {
		return false
	}
	return sameFallbackChain(plan.Fallbacks, targetAgent.Fallbacks)
}

func normalizeSubagentSessionModelOverrideMode(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case subagentSessionModelOverrideIgnore:
		return subagentSessionModelOverrideIgnore
	case subagentSessionModelOverrideInherit:
		return subagentSessionModelOverrideInherit
	case subagentSessionModelOverrideFallbackOnly:
		return subagentSessionModelOverrideFallbackOnly
	default:
		return ""
	}
}

func mergeSubagentsConfig(defaults, override *config.SubagentsConfig) *config.SubagentsConfig {
	if defaults == nil && override == nil {
		return nil
	}
	merged := &config.SubagentsConfig{}
	if defaults != nil {
		if defaults.AllowAgents != nil {
			merged.AllowAgents = append([]string(nil), defaults.AllowAgents...)
		}
		if defaults.Model != nil {
			modelCopy := *defaults.Model
			if defaults.Model.Fallbacks != nil {
				modelCopy.Fallbacks = append([]string(nil), defaults.Model.Fallbacks...)
			}
			merged.Model = &modelCopy
		}
		merged.SessionModelOverrideMode = strings.TrimSpace(defaults.SessionModelOverrideMode)
	}
	if override != nil {
		if override.AllowAgents != nil {
			merged.AllowAgents = append([]string(nil), override.AllowAgents...)
		}
		if override.Model != nil {
			modelCopy := *override.Model
			if override.Model.Fallbacks != nil {
				modelCopy.Fallbacks = append([]string(nil), override.Model.Fallbacks...)
			}
			merged.Model = &modelCopy
		}
		if strings.TrimSpace(override.SessionModelOverrideMode) != "" {
			merged.SessionModelOverrideMode = strings.TrimSpace(override.SessionModelOverrideMode)
		}
	}
	if merged.AllowAgents == nil && merged.Model == nil && merged.SessionModelOverrideMode == "" {
		return nil
	}
	return merged
}

func resolveSubagentModelPlan(
	targetAgent *AgentInstance,
	parentOverride string,
) subagentModelPlan {
	plan := subagentModelPlan{
		Primary:        strings.TrimSpace(targetAgent.Model),
		Fallbacks:      append([]string(nil), targetAgent.Fallbacks...),
		Mode:           subagentSessionModelOverrideIgnore,
		ParentOverride: strings.TrimSpace(parentOverride),
	}
	if targetAgent != nil && targetAgent.Subagents != nil {
		if targetAgent.Subagents.Model != nil && strings.TrimSpace(targetAgent.Subagents.Model.Primary) != "" {
			plan.Primary = strings.TrimSpace(targetAgent.Subagents.Model.Primary)
			plan.Fallbacks = append([]string(nil), targetAgent.Subagents.Model.Fallbacks...)
		}
		if mode := normalizeSubagentSessionModelOverrideMode(
			targetAgent.Subagents.SessionModelOverrideMode,
		); mode != "" {
			plan.Mode = mode
		}
	}
	if plan.ParentOverride == "" {
		return plan
	}
	switch plan.Mode {
	case subagentSessionModelOverrideInherit:
		plan.Primary = plan.ParentOverride
	case subagentSessionModelOverrideFallbackOnly:
		plan.Fallbacks = prependUniqueFallbackModel(plan.Fallbacks, plan.ParentOverride, plan.Primary)
	}
	return plan
}

func prependUniqueFallbackModel(fallbacks []string, candidate, primary string) []string {
	candidate = strings.TrimSpace(candidate)
	if candidate == "" {
		return append([]string(nil), fallbacks...)
	}
	if strings.EqualFold(candidate, strings.TrimSpace(primary)) {
		return append([]string(nil), fallbacks...)
	}
	for _, existing := range fallbacks {
		if strings.EqualFold(strings.TrimSpace(existing), candidate) {
			return append([]string(nil), fallbacks...)
		}
	}
	next := make([]string, 0, len(fallbacks)+1)
	next = append(next, candidate)
	next = append(next, fallbacks...)
	return next
}

func inheritedSubagentOverride(parentTS *turnState) string {
	if parentTS == nil {
		return ""
	}
	override := normalizeSessionModelOverride(parentTS.model.Override)
	return strings.TrimSpace(override.Model)
}

func (al *AgentLoop) buildSubagentChildBinding(
	parentTS *turnState,
	targetAgent *AgentInstance,
) (effectiveModelBinding, error) {
	if targetAgent == nil {
		return effectiveModelBinding{}, nil
	}
	overrideModel := inheritedSubagentOverride(parentTS)
	plan := resolveSubagentModelPlan(targetAgent, overrideModel)
	if subagentPlanMatchesAgent(plan, targetAgent) {
		binding := effectiveModelBinding{
			WorkspaceAgent: targetAgent,
		}
		if parentTS != nil {
			binding.RouteSessionKey = strings.TrimSpace(parentTS.opts.Dispatch.RouteSessionKey)
		}
		if overrideModel != "" {
			binding.Override = normalizeSessionModelOverride(parentTS.model.Override)
		}
		return binding, nil
	}
	execution, cleanup, err := al.buildExecutionStateForModel(targetAgent, plan.Primary, plan.Fallbacks)
	if err != nil && overrideModel != "" && plan.Mode != subagentSessionModelOverrideIgnore {
		logger.WarnCF("subturn", "Falling back to target agent model after subagent override resolution failed",
			map[string]any{
				"target_agent_id": targetAgent.ID,
				"override_model":  overrideModel,
				"mode":            plan.Mode,
				"error":           err.Error(),
			})
		execution, cleanup, err = al.buildExecutionStateForModel(targetAgent, targetAgent.Model, targetAgent.Fallbacks)
	}
	if err != nil {
		return effectiveModelBinding{}, err
	}
	binding := effectiveModelBinding{
		WorkspaceAgent: targetAgent,
		Execution:      execution,
		cleanup:        cleanup,
	}
	if parentTS != nil {
		binding.RouteSessionKey = strings.TrimSpace(parentTS.opts.Dispatch.RouteSessionKey)
	}
	if overrideModel != "" {
		binding.Override = normalizeSessionModelOverride(parentTS.model.Override)
	}
	return binding, nil
}
