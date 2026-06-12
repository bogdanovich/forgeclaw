# Subagent Model Policy

PicoClaw distinguishes three separate concerns for child-agent model selection:

1. The target agent's normal model configuration.
2. An optional subagent-specific model policy.
3. An optional propagated parent session model override.

This document defines the intended precedence and behavior for `spawn`,
`subagent`, and `delegate` child runs.

## Goals

- Keep normal agent specialization stable by default.
- Support a session-scoped emergency model switch such as `/model gemini-flash-lite`.
- Allow specific child agents to opt out of inherited overrides.
- Avoid coupling diagnostics or turn setup to hidden runtime magic.

## Config Surface

Two levels can configure child-run behavior:

- `agents.defaults.subagents`
- `agents.list[].subagents`

Supported fields:

- `allow_agents`
- `model`
- `session_model_override_mode`

`session_model_override_mode` accepts:

- `ignore`
- `inherit`
- `fallback_only`

## Resolution Order

Child-run model selection follows this order:

1. Explicit per-call model override.
   Current PicoClaw child tools do not expose this yet; the slot is reserved for
   future extension.
2. Target agent `subagents.model`
3. Global `agents.defaults.subagents.model`
4. Target agent normal `model`

Session model override propagation is then applied according to
`session_model_override_mode`, resolved with:

1. `agents.list[].subagents.session_model_override_mode`
2. `agents.defaults.subagents.session_model_override_mode`
3. implicit default: `ignore`

## Mode Semantics

### `ignore`

The parent session override does not affect the child run.

- Child primary model remains the resolved child base model.
- Child fallback chain remains the resolved child base fallbacks.

Use this for specialized agents that should keep their configured model policy
even when the parent conversation was manually switched to another model.

### `inherit`

The parent session override becomes the child run's effective primary model.

- Child primary model becomes the propagated override model.
- Child fallback chain remains the resolved child base fallbacks.

Use this for "panic switch" behavior where an operator changes the current
session model and expects all delegated work in that session tree to follow it.

### `fallback_only`

The parent session override is inserted at the front of the child fallback
chain without replacing the child's configured primary model.

- Child primary model remains the resolved child base model.
- Parent override becomes the first fallback, unless it duplicates the primary
  or an existing fallback.

Use this when child agents should preserve their specialization, but should
still prefer the parent session override when their own primary model fails.

## Propagation Across Nested Child Runs

When a parent session override is present, child runs carry that override
metadata forward in their effective model binding even if the mode is
`fallback_only`.

This ensures nested child runs can apply their own `session_model_override_mode`
consistently without requiring the override to be re-persisted in per-child
ephemeral sessions.

## Recommended Defaults

Backward-compatible default:

- `ignore`

Operationally useful default for mixed-cost deployments:

- `fallback_only`

Suggested specializations:

- `coding`: `ignore` or `fallback_only`
- `media`: `inherit` or `fallback_only`
- `reviewer`: `ignore`

## Non-Goals

This policy intentionally does not:

- rebuild the full parent runtime prompt for children
- implicitly rewrite every child tool or async runtime
- force all child agents to inherit a parent override unless configured

The design is intentionally declarative: target child agents declare how much
parent session model state they want to inherit.
