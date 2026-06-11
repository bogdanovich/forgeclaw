# Memory System

This document describes the recommended target memory architecture for
ForgeClaw, the current gaps, the desired behavior, and the incremental roadmap.

## Goals

ForgeClaw memory should support multiple use cases:

- personal assistant continuity
- coding/project continuity
- research accumulation
- operational preferences
- structured archival workflows

The system should separate:

- session context
- working memory
- durable memory
- user/operator profile memory

The system should also be configurable per agent and per use case.

Not every ForgeClaw instance needs the same memory behavior:

- a repetitive code-review agent may need almost no curated memory
- a personal daily agent benefits from strong user-memory and promotion
- a coding agent may want working memory and some durable project memory, but
  conservative user-memory writes
- a research agent may want larger working memory and delayed promotion

## Current State

Today ForgeClaw has two main memory mechanisms:

1. `seahorse`
   - session-scoped history and compaction
   - SQLite-backed, budget-aware context assembly
   - good at preserving recent conversational continuity
2. workspace memory files
   - `memory/MEMORY.md`
   - recent daily notes injected into prompt context

This is usable, but it is not yet a complete layered memory system.

### What Works

- `seahorse` is a solid context engine for turns, compaction, and retrieval over
  the active session history.
- `MEMORY.md` is simple and easy to reason about.
- Workspace memory is file-based and inspectable.

### What Is Missing

- no separate mutable user/operator profile memory
- no clear working-memory lifecycle
- no automatic promotion pipeline from working memory into durable memory
- no regular curation loop for dedupe / consolidation / pruning
- recent daily notes are injected bluntly rather than recalled selectively

## Recommended Target Architecture

ForgeClaw should use a layered design.

### 1. Session Memory

Owned by `seahorse`.

Use it for:

- turn history
- compaction
- active-session continuity
- bounded assembly under token pressure
- session-level grep / expand tools

Do not use it as durable memory.

### 2. Durable Memory

Canonical file:

- `memory/MEMORY.md`

Use it for:

- durable environment facts
- persistent project conventions
- stable operating constraints
- important decisions that should survive across sessions

This file should stay compact and curated.

### 3. User / Operator Memory

Canonical file:

- `memory/USER_MEMORY.md`

Use it for:

- response-style preferences
- workflow preferences
- approval habits
- things to avoid
- stable patterns about how the user wants the agent to behave

This is distinct from workspace `USER.md`.

- `USER.md` remains a bootstrap instruction file
- `USER_MEMORY.md` is mutable learned memory

### 4. Working Memory

Canonical files:

- `memory/YYYY/MM/YYYY-MM-DD.md`

Optional future variants:

- `memory/YYYY/MM/YYYY-MM-DD-<slug>.md`

Use working memory for:

- daily findings
- raw observations
- intermediate summaries
- unresolved items worth revisiting
- short-term facts that may or may not become durable

Working memory is allowed to be larger and noisier than `MEMORY.md`.

## Configurability Requirements

Memory behavior should be policy-driven rather than globally always-on.

At minimum, the target design should support configuration for:

- working-memory writes
- user-memory writes
- durable-memory promotion
- background curation / review
- recall from recent daily notes

These controls should be available per agent or per routed agent profile, not
only globally.

### Recommended Policy Surface

The exact config schema can evolve, but the behavior should map to something
like:

- `working_memory.mode`
  - `off`
  - `manual`
  - `auto`
- `user_memory.mode`
  - `off`
  - `manual`
  - `auto`
- `promotion.mode`
  - `off`
  - `review_only`
  - `auto_conservative`
- `recall.mode`
  - `off`
  - `recent_only`
  - `targeted`

Optional future controls:

- maximum recent daily notes considered
- whether curation can rewrite or only append/merge
- separate policies for personal vs project workspaces
- per-agent memory-file roots if an installation needs isolation

### Example Profiles

#### Reviewer Agent

Recommended defaults:

- working memory: `off` or `manual`
- user memory: `off`
- promotion: `off`
- recall: `recent_only` or `off`

Reasoning:

- review work is repetitive and low-personalization
- over-curation risks noisy memory with little value
- deterministic fresh context matters more than continuity

#### Daily Personal Agent

Recommended defaults:

- working memory: `auto`
- user memory: `auto`
- promotion: `auto_conservative`
- recall: `targeted`

Reasoning:

- continuity across days matters
- learned preferences are valuable
- durable personal habits should accumulate

#### Coding Agent

Recommended defaults:

- working memory: `auto`
- user memory: `manual` or conservative `auto`
- promotion: `review_only` or `auto_conservative`
- recall: `targeted`

Reasoning:

- project continuity matters
- user preferences matter somewhat
- incorrect durable promotion can create stale engineering assumptions

## Desired Behavior

### Session Behavior

On normal turns:

- `seahorse` assembles session history
- `MEMORY.md` and `USER_MEMORY.md` are loaded as durable context
- recent working-memory notes may be included in a bounded way

### Automatic Daily Memory Updates

ForgeClaw should be able to write working memory automatically when:

- a compaction boundary is near
- a session ends
- a scheduled maintenance job runs
- a background review concludes there is useful short-term context to keep

Daily memory writes should:

- append to the canonical daily note
- prefer facts, outcomes, and short summaries over transcripts
- avoid duplicating large raw tool output
- remain safe to overwrite only by append-or-merge, not destructive rewrite
- be suppressible by policy for agents that do not benefit from daily memory

### Automatic User Memory Updates

ForgeClaw should update `USER_MEMORY.md` automatically only when the signal is
strong and durable, for example:

- the user repeatedly asks for a response style
- the user corrects workflow habits
- the user explicitly says "remember this" or equivalent
- the user sets a long-lived operating preference

It should not write transient task context or one-off moods into
`USER_MEMORY.md`.

For some agents, especially review or triage agents, automatic `USER_MEMORY.md`
writes should be disabled entirely.

### Automatic Curation

ForgeClaw should periodically curate memory automatically:

1. review recent working memory
2. identify durable candidates
3. promote stable candidates into `MEMORY.md` or `USER_MEMORY.md`
4. dedupe or merge overlapping entries
5. leave noisy or uncertain items in working memory only

Automatic curation should prefer:

- append-and-merge
- stable wording
- fewer, denser entries

It should avoid:

- rewriting bootstrap files like `AGENT.md`, `USER.md`, `SOUL.md`
- copying transcripts verbatim into durable memory
- promoting environment failures or transient errors as long-lived truths

Automatic curation should also be optional. For some agents, the correct
configuration is no curation at all.

## Recommended Promotion Rules

Promote to `MEMORY.md` when:

- the fact is operationally durable
- it affects future task execution
- it is likely to remain true across sessions

Promote to `USER_MEMORY.md` when:

- it describes how the user wants the agent to behave
- it reflects stable preferences rather than one task's immediate need

Keep only in daily working memory when:

- the information is recent, uncertain, or task-local
- it may matter later but is not yet clearly durable
- it is useful for recall but should not be in every prompt

## Comparison to Other Designs

### Hermes-Style Curation

Useful ideas:

- separate user-profile memory from general memory
- use background review to decide what is worth saving
- keep durable memory compact

Not sufficient alone for ForgeClaw because:

- it lacks a rich working-memory and promotion lifecycle
- it optimizes more for assistant preference memory than for broader knowledge accumulation

### OpenClaw-Style Working Memory and Promotion

Useful ideas:

- canonical daily notes
- memory flush before compaction
- promotion from working memory to durable memory
- recall/search over working notes

This is the more important model for ForgeClaw's evolution.

## Roadmap

### Phase 1: Memory Shape Cleanup

- add `memory/USER_MEMORY.md`
- normalize daily memory to `memory/YYYY/MM/YYYY-MM-DD.md`
- keep backward-compatible reads for legacy daily-note layouts
- update prompts and docs to reflect the memory layers

### Phase 1.5: Config Surface

- add explicit config for memory behavior by agent/profile
- allow disabling or constraining:
  - working-memory writes
  - user-memory writes
  - promotion
  - curation
  - recall
- set conservative defaults for non-personal agents

### Phase 2: Safer Automatic Writing

- add explicit write helpers for:
  - durable memory
  - user memory
  - daily working memory
- add append-only / merge-friendly policies
- add tests around promotion boundaries
- respect per-agent memory policy when deciding whether to write at all

### Phase 3: Background Review and Promotion

- add a bounded review path that:
  - reviews recent working memory
  - proposes or applies promotions
  - updates `MEMORY.md` / `USER_MEMORY.md`
- make review conservative by default
- keep reviewer/automation-oriented agents opted out by default

### Phase 4: Recall Improvements

- reduce blunt prompt injection of recent daily notes
- add targeted recall/search over working memory
- eventually support semantic recall over memory files if needed

### Phase 5: Full Memory Lifecycle

- explicit pruning / stale-memory cleanup
- configurable promotion thresholds
- optional maintenance cron for memory consolidation

## Phase 1 Scope

Phase 1 is intentionally small:

- improve structure
- avoid breaking `seahorse`
- keep existing file-based memory understandable
- lay groundwork for promotion and recall later
