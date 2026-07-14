# ForgeClaw Reviewer Context

This file is repo-owned reviewer context for `bogdanovich/forgeclaw`. It is
separate from `.forgeclaw/reviewer-rules.json`:

- `reviewer-rules.json` is for deterministic path/risk/Codex policy.
- `reviewer-context.md` is for architecture notes, invariants, compatibility
  expectations, and review philosophy.

Reviewers should treat this file as higher-priority repo-specific context than
external reviewer workspace notes when the two overlap.

## Project Character

ForgeClaw is a long-lived personal fork of upstream PicoClaw. The fork
prioritizes:

- deployable multi-workspace runtime behavior;
- durable background automation;
- Telegram-first UX correctness;
- safe changes without breaking upstream mergeability more than necessary.

Review changes with that fork reality in mind. A change can be locally clean
and still be a bad ForgeClaw change if it increases operational ambiguity,
workspace coupling, or merge friction without enough payoff.

## High-Risk Architecture Areas

These changes deserve extra skepticism even when tests are green:

- `pkg/agent/**`
  - turn ownership
  - final reply/media/tool delivery
  - async completions
  - subturn/subagent handoff
- `pkg/gateway/**`
  - startup orchestration
  - workspace/service wiring
  - recovery paths
- `pkg/channels/**`
  - Telegram/group/topic routing
  - message suppression rules
  - formatting and tool feedback visibility
- `pkg/seahorse/**`
  - compaction boundaries
  - context durability
  - summarization/replay safety
- `pkg/tasks/**`, `pkg/cron/**`, `pkg/bus/**`
  - durable state
  - queue/replay semantics
  - event/report contracts
- `pkg/tools/**`, `pkg/mcp/**`
  - delivery contract shape
  - tool visibility / allowlist behavior
  - side effects that cross parent/child boundaries
- `pkg/providers/**`, `pkg/auth/**`, `pkg/credential/**`
  - OAuth/session lifecycle
  - provider fallback
  - secret handling
- `web/backend/**`
  - external API compatibility
  - webhook correctness

## Important Invariants

Review against these invariants explicitly:

1. **Structured state should be canonical**
   - Durable reports, task state, queue state, and typed artifacts are more
     trustworthy than terminal prose.
   - Human-readable summaries should be projections of structured state, not the
     only source of truth.

2. **Workspace isolation matters**
   - A change must not accidentally leak data, sessions, or durable state across
     workspaces/profiles.
   - Shared infrastructure is acceptable; shared mutable business state is risky.

3. **Ingress and recovery should favor at-least-once durability over silent loss**
   - Startup/restart behavior must not silently drop inbound work.
   - Replay/recovery paths should be explicit and testable.

4. **Delivery should be deterministic**
   - Tool/media/message delivery semantics should not depend on fragile prompt
     phrasing or ad hoc tool-specific exceptions.
   - If two code paths deliver user-visible output, reviewers should ask whether
     they should share one delivery pattern.

5. **Telegram UX regressions are real regressions**
   - Wrong topic routing, broken formatting, leaked tool feedback, or missed
     suppression rules are not cosmetic issues.

6. **Upstream mergeability still matters**
   - Avoid gratuitous renames or abstractions that make future upstream syncs
     harder unless the local payoff is clear.

## Review Philosophy

- Prefer concrete correctness and operational-risk findings over style.
- Use KISS/DRY/YAGNI/SOLID only as support for a real failure mode.
- Be wary of changes that introduce a second pattern for an already-solved
  runtime concern.
- Reward deterministic helpers and machine-readable contracts.
- Call out missing tests when the change affects durability, delivery, routing,
  or recovery semantics.

## Typical Review Questions

When changes touch runtime plumbing, ask:

- What is the canonical state here?
- Can restart/replay duplicate or lose work?
- Does this create another delivery path instead of reusing the current one?
- Does this change cross workspace/profile boundaries?
- Does this make upstream merges harder than necessary?
- Is the user-visible chat/Telegram behavior still deterministic?
