# Replay And Evaluation

## Status

This document defines the target architecture and delivery plan for deterministic
ForgeClaw replay and evaluation. It is based on the runtime at `origin/main`
after PR #206. Implementation may revise package names or boundaries when code
evidence requires it, but must update this document in the same pull request.

## Problem

ForgeClaw has regression tests for delivery, steering, restart recovery,
compaction, provider fallback, tool loops, and evolution. They do not share a
versioned execution record or evaluation vocabulary. This makes it difficult to
turn a production failure into a reusable fixture, compare behavior across
versions, or show that a runtime mechanism improved an outcome.

The objective is not to reproduce model sampling. It is to capture enough
bounded evidence to replay runtime decisions with deterministic provider and
tool responses, then evaluate observable invariants.

## Goals

- Record a bounded, versioned, redacted execution trace with stable correlation.
- Reuse canonical task, session, event, delivery, and evolution facts rather
  than creating a competing source of truth.
- Replay fixtures without network access, live tools, user delivery, or writes
  to production state.
- Evaluate deterministic reliability properties with machine-readable results.
- Convert real ForgeClaw regressions into sanitized fixtures.
- Permit optional model-assisted grading only for semantic questions that
  deterministic checks cannot answer.

## Non-Goals

- Capturing hidden provider reasoning or reproducing stochastic model output.
- Treating logs or user-facing prose as canonical task state.
- Persisting every runtime event or full conversation by default.
- Replacing existing unit and integration tests.
- Inferring that an ordinary follow-up is a user correction.
- Executing arbitrary trace-provided commands, tools, network requests, or
  deliveries during replay.
- Making CI depend on credentials or external model calls.

## Current Source Inventory

| Fact | Current authority | Durability | Replay use |
| --- | --- | --- | --- |
| Runtime lifecycle | `pkg/events.Event` bus | In-memory only | Capture selected normalized records while subscribed |
| Task lifecycle and delivery decisions | `pkg/tasks.TaskEvent` | `state/task_registry.json` | Import by event ID and sequence; never reconstruct from prose |
| Current task projection | `pkg/tasks.Record` | `state/task_registry.json` | Initial/final snapshot and consistency checks |
| Session messages and tool-call pairing | `session.SessionStore` / memory JSONL | Durable | Explicit bounded snapshot or fixture input |
| Seahorse summaries and reconciliation | Seahorse SQLite plus session revision | Durable | Snapshot IDs, digests, token counts, and selected sanitized content |
| Steering queue | `pkg/agent.steeringQueue` | Transient | Capture enqueue/inject decisions; session history proves committed injection |
| Inbound restart recovery | `pkg/bus.InboundSpool` | Durable until ack | Capture spool ID and state transitions, not a second spool |
| Async delivery idempotency | task completion ID and delivery events | Durable | Canonical duplicate-prevention evidence |
| Channel delivery attempts | `channel.message.*` runtime events | Transient | Capture queued/sent/failed normalized records |
| Provider fallback attempts | `providers.FallbackResult.Attempts` | Transient | Add a typed capture adapter at the fallback boundary |
| Tool calls/results | provider history plus agent runtime | Session durable; runtime detail transient | Capture sanitized call/result identities and scripted fixture values |
| Evolution inputs and outputs | evolution JSONL and draft/profile stores | Durable | Snapshot record/draft/profile identities and objective policy facts |
| User correction | No explicit authority | Unavailable | Explicit annotation only; never infer from message wording |

The runtime event bus is an observation surface, not a durable log. Its payload
types currently include raw user messages, final content, tool arguments,
errors, and arbitrary attributes. A recorder must not serialize an
`events.Event` wholesale.

## Component Boundaries

### `pkg/evaltrace`

Owns the trace schema, validation, canonical ordering, redaction policy,
bounds, atomic file storage, retention, and import/export. It must not import
`pkg/agent`, execute tools, call providers, publish messages, or mutate task and
session stores.

### Capture adapters

Small adapters at authoritative boundaries convert facts into `evaltrace.Record`
values. Adapters own source-specific mapping but not storage policy. The
recorder accepts normalized records, enforces bounds, and writes one finalized
trace.

The initial adapters are expected at:

- typed runtime-event subscription for turn, context, steering, tool-loop, and
  channel delivery lifecycle;
- task registry event append for task and async delivery transitions;
- provider fallback completion for candidate attempts;
- turn completion for outcome and bounded session/context snapshots;
- evolution bridge/store boundaries for learning-record and draft decisions.

Adapters must use `(origin, origin_id)` deduplication. A task delivery change
seen through both a task event and a runtime event is recorded from the task
event only. Runtime events may add a link to the task event but not a second
transition.

### `pkg/replay`

Consumes a validated trace or committed fixture and drives pure state machines
and controlled runtime harnesses. It owns virtual time, deterministic ID
allocation, scripted provider/tool/delivery outcomes, restart boundaries, and
side-effect policy. It cannot construct production provider, channel, MCP,
shell, filesystem-write, or task/session-store implementations.

### `pkg/evaluation`

Consumes replay observations and emits deterministic findings. Evaluators are
named and versioned. They do not modify a replay or call a model. Optional
semantic graders live behind a separate interface and command flag.

### CLI

The stable surface is expected under `picoclaw eval`:

- `inspect TRACE --json`
- `replay TRACE --json`
- `run TRACE [--check NAME] [--json]`
- `fixture validate PATH`
- optional `grade TRACE --model ... --budget ...`

Exact Cobra layout may follow current command conventions. Exit code is zero
only when input is valid and every selected deterministic check passes.

## Trace Contract

The initial envelope is conceptually:

```go
type Trace struct {
    SchemaVersion string
    TraceID       string
    CreatedAt     time.Time
    Source        Source
    Policy        CapturePolicy
    Limits        AppliedLimits
    Metadata      Metadata
    Records       []Record
    Outcome       *Outcome
    Corrections   []Correction
    Truncation    Truncation
}

type Record struct {
    Sequence     uint64
    OffsetNanos int64
    Kind         RecordKind
    Origin       Origin
    Scope        Scope
    Correlation  Correlation
    Digest       string
    Data         json.RawMessage
}
```

All JSON fields use explicit snake-case names. `schema_version` starts at
`forgeclaw.eval_trace.v1`. Trace IDs and fixture IDs are opaque. Runtime IDs,
turn IDs, task IDs, completion IDs, event IDs, request IDs, and session keys
remain separate typed correlation fields; they must not be overloaded into one
generic ID.

Records use a closed kind vocabulary. Initial kinds include:

- turn start/end and final outcome;
- model request, response, retry, and fallback attempt;
- tool call, result, skip, and loop decision;
- steering enqueue/injection and interrupt;
- task transition and delivery decision/attempt/outcome;
- compaction/reconciliation transition and context snapshot;
- restart boundary and inbound spool transition;
- evolution record, draft, review, apply, rollback, and profile snapshot;
- explicit user correction annotation.

Unknown record kinds or major schema versions are rejected. Readers may ignore
new optional fields within a supported schema version. Migration is explicit,
pure, and tested; readers never silently reinterpret old fields.

## Ordering And Identity

- Record sequence is assigned by one recorder after normalization.
- Runtime timestamps are diagnostic. Replay order uses `sequence`, not wall
  clock order.
- `offset_nanos` is relative to trace start and must be non-negative and
  monotonic after canonicalization.
- Concurrent source records are ordered by recorder receipt, then stable source
  priority and origin ID when importing existing durable records.
- `digest` is SHA-256 over canonical kind, scope, correlation, and redacted
  data. It detects fixture drift; it is not a secret-hiding substitute.
- `(origin.kind, origin.id)` is unique when the source provides an ID. Duplicate
  imports are rejected or coalesced only when canonical bytes match.

## Capture Policy And Security

Production recording is disabled by default. Enabling evaluation does not
implicitly enable trace capture.

Three content modes are defined:

1. `metadata_only` (default when capture is enabled): lengths, counts, status,
   safe enums, hashes, and correlation IDs only.
2. `redacted_content`: explicitly allowlisted content fields pass through the
   configured secret filter and structural redactors.
3. `fixture`: test-only sanitized content supplied by fixture authors; rejected
   from production recorder configuration.

Security is allowlist-based. Generic runtime payloads, arbitrary `Attrs`, raw
provider options, headers, credentials, environment values, MCP configuration,
filesystem content, and unrestricted errors are never copied. Error records
use classifier code plus a bounded sanitized summary only when policy permits.

Mandatory structural redaction covers API keys/tokens, authorization and cookie
fields, URL user info/query secrets, environment assignments, configured secure
strings, and media data URLs. Redaction runs before hashing and storage. Tests
must seed representative secrets and prove they do not appear in serialized
traces, diagnostics, validation errors, or digests rendered as input material.

Default bounds when capture is enabled:

- 2 MiB serialized trace;
- 2,000 records;
- 64 KiB per record before redaction and 16 KiB after redaction;
- 256 model/tool records each;
- 128 task/delivery records each;
- 32 context snapshots;
- 8 corrections;
- 24-hour retention and 100 traces per workspace.

Limits are configurable downward or upward within compiled hard ceilings.
When a soft limit is reached, the recorder retains terminal outcome and
truncation metadata, drops lower-priority detail, and never emits malformed
JSON. A hard ceiling finalizes or rejects the trace. Storage uses atomic writes,
owner-only directories/files, no symlink traversal, bounded startup pruning,
and path-safe opaque trace IDs.

## Capture Lifecycle

One trace normally covers one root turn and correlated SubTurns/background task
activity until terminal outcome or a configured duration. Long-lived tasks may
produce linked child traces rather than keeping a recorder open indefinitely.

The recorder subscribes before turn start and finalizes after terminal outcome
plus bounded delivery settlement. It records its own drop/truncation counters.
Subscriber backpressure must not block agent execution; a full recorder queue
drops low-priority records and marks the trace incomplete.

Task registry and session stores remain authoritative. The recorder may read a
bounded snapshot during finalization but never acknowledges spool entries,
changes delivery status, truncates history, or updates evolution state.

### Implemented capture boundary

The capture implementation subscribes to typed runtime events and observes task
events only after the task registry has persisted them. Root turns and
long-lived tasks are stored as separate traces. When a task is first observed,
its existing durable event history is imported before the new event, which
preserves restart-reconciliation evidence without replacing the registry.

Captured runtime evidence includes turn outcomes; model request/response hashes
and fallback attempts; tool call/result hashes and call IDs; steering acceptance
and injection; compaction counts and final context identity; tool-loop
decisions; channel delivery attempts/outcomes while a unique active or
delivery-settling target can be identified; task delivery decisions/outcomes;
and durable evolution record,
pattern, draft, and apply transitions. Evolution event payloads contain policy
codes and provenance IDs, never draft bodies or review prose.

Capture persistence runs on a bounded worker. Event and record limits never
block the agent. Runtime-event subscriber drops mark active turn traces
incomplete; persistence-queue overflow drops the finalized trace and emits a
safe operational warning. Disabling capture during reload discards unfinished
in-memory traces immediately.

Current capture limitations are explicit:

- channel events do not carry a turn ID or session key, so they are associated
  only when channel/chat identifies one active or delivery-settling turn;
  expected delivery has a bounded settlement timeout, and durable task delivery
  remains authoritative for async work;
- context content is represented by filtered hashes, counts, goal presence,
  steering count, and tool-pair validity, not by raw session messages;
- failed evolution apply rollback details remain canonical in evolution state;
  trace transitions expose only successful apply and persisted draft states;
- user correction remains an explicit fixture/CLI annotation and is never
  inferred during production capture.

## Deterministic Replay

Replay has two levels:

### Contract replay

A pure reducer applies normalized records to typed task, delivery, steering,
context, provider, tool-loop, and evolution projections. This verifies ordering,
legal transitions, idempotency, correlation, and terminal invariants without
starting an agent loop.

The implemented `pkg/evalreplay` reducer accepts only traces that pass the
versioned `evaltrace.Validate` contract. It decodes each record into its exact
typed payload, produces a canonical JSON projection, and emits stable
diagnostics for illegal transitions, missing correlations, unresolved tool
calls, duplicate terminal outcomes, and incomplete turns. It does not repair or
reinterpret malformed evidence.

Replay safety primitives include a virtual clock, sequential ID source,
deep-copy isolated session/task/evolution checkpoints, a deny-only external
side-effect policy, and a tool catalog that can invoke only explicitly compiled
safe stubs. These primitives do not import or construct production providers,
channels, gateways, MCP, shell, or filesystem tools.

### Scenario replay

Fixtures may include scripted provider responses, tool results, virtual inbound
messages, and delivery outcomes. Scenario replay uses the existing
`pkg/testharness/llmscenario` concepts behind stricter interfaces and runs the
real orchestration path with:

- virtual clock and deterministic ID source;
- isolated session/task/evolution state confined to a runner-owned temporary
  workspace that is removed after each scenario;
- scripted providers and explicitly registered stub tools;
- recording delivery sink that never starts a channel;
- denied network, MCP, shell, external filesystem mutation tools, subprocess,
  and gateway construction; the agent's own state writes are confined to its
  disposable workspace;
- explicit restart checkpoints that reconstruct only allowed isolated state.

A trace cannot name a production tool and cause it to execute. Fixture tools
must be registered by test code or a compiled safe stub catalog. Unknown tools
produce a deterministic denied result.

The implemented `pkg/evalscenario` adapter drives the real inbound
`AgentLoop.Run` path with scripted model responses and sealed text-only stub
tools. It writes an explicit tool/MCP allowlist before constructing the loop,
uses an isolated bootstrap option that skips shared production tool and state
manager construction, verifies the resulting registry contains exactly the
declared stubs, records the outbound delivery, normalizes the captured evidence
to fixture identity, and feeds it through contract replay. The temporary
workspace is deleted before the runner returns.

Replay output contains observations and diagnostics, never production writes.
The same fixture, binary, options, and evaluator versions must produce identical
canonical JSON.

## Deterministic Evaluators

Each finding contains evaluator name/version, pass/fail/error, severity,
record references, expected invariant, observed fact, and remediation hint.

Initial evaluators:

- `delivery_reliability.v1`: every required delivery decision reaches one
  valid terminal outcome or an explicit retryable failure.
- `duplicate_response.v1`: a completion/fingerprint is not delivered more than
  once to the same target; parent synthesis and direct user delivery follow
  declared mode.
- `steering_correctness.v1`: accepted steering is injected once into its scope,
  remaining provider calls receive paired skipped results, and stale final
  output is not delivered.
- `restart_recovery.v1`: active work becomes reconciled/lost or resumes by
  policy; completed delivery is not repeated; inbound spool ack/release is
  consistent.
- `compaction_retention.v1`: protected fresh tail, tool-call/result pairing,
  steering, and required goal/context facts survive the recorded compaction
  boundary.
- `tool_loop_recovery.v1`: warning/block thresholds and provider pairing match
  configuration, and blocked calls do not execute.
- `provider_failover.v1`: candidates follow policy, non-retriable failures stop,
  cooldown/rate-limit skips are explicit, and selected identity is correct.
- `evolution_safety.v1`: failed/heartbeat/ineligible turns do not promote
  drafts, quarantine/review policy is honored, provenance exists, and apply or
  rollback transitions match lifecycle rules.

All eight deterministic evaluators are implemented in `pkg/evalevaluator`.
Their findings are independent and include stable status, severity, record
references, expected and observed facts, and remediation. Missing evidence is
reported as `not_evaluable`; malformed typed evidence is `error`.

These are correctness checks, not a single quality score. Missing required
evidence returns `error` or `not_evaluable`, never `pass`.

## Historical Fixture Sources

Fixtures must cite a source commit/test and contain only synthetic or sanitized
content. Initial candidates are:

- duplicate steering injection: commit `4de727cd` and current steering tests;
- async delivery idempotency: `c877f564`, `b987e242`, and delivery coordinator
  tests;
- stale delivery/restart reconciliation: `7368ca47` and task registry tests;
- Seahorse restart and duplicate ingestion: reconciliation and context tests;
- repeated fatal MCP loop: `0984d65a` plus current fatal-MCP tests;
- sticky model failover: `7a3de0f2`, `59d88050`, and fallback/turn tests;
- evolution quarantine and heartbeat exclusion: evolution bridge/cold-path
  tests and `88684cdc`.

The fixture manifest records source references, sanitized status, expected
evaluators, and why the failure mattered. A source reference is evidence, not
permission to copy private production content.

The checked-in versioned manifest contains one passing and one failing fixture
for every deterministic evaluator. `picoclaw eval fixtures` validates the
manifest, and the pull-request Go test job runs the manifest fixture matrix.

## User Corrections

ForgeClaw does not currently have a reliable correction signal. A reply,
follow-up, or steering message may add information without correcting anything.
The initial system accepts corrections only through explicit fixture/CLI/API
annotations referencing a prior outcome and optional record IDs. Automatic
classification is deferred to optional semantic analysis and may not alter
deterministic ground truth.

## Optional Model-Assisted Evaluation

Model grading is a later, optional adapter for compaction faithfulness, final
answer usefulness, and evolution draft quality. It requires an explicit flag,
model/provider, rubric version, request/token/cost budget, timeout, and maximum
samples. Input is the redacted trace projection, never the raw trace.

Results record model identity, provider, rubric hash, sampling parameters,
usage, latency, and every sample score. Multiple samples report variance.
Model failure produces `not_evaluable`; it never changes deterministic findings
or ordinary CI status.

### Current decision

Model-assisted grading is deferred. The current roadmap questions are delivery,
transition, correlation, recovery, and policy invariants, all of which are
deterministically evaluable. No specific semantic rubric, acceptable model,
cost budget, or variance threshold has been approved. Adding a grader now would
increase nondeterminism and data exposure without answering a concrete question.
This decision can be revisited only with an explicit semantic target and budget;
deterministic findings remain authoritative.

## Delivery Plan

1. **Foundation**: this architecture, `pkg/evaltrace` contracts, validation,
   canonicalization, redaction, limits, atomic store, and tests.
2. **Capture**: recorder lifecycle and source adapters for runtime events, task
   events, delivery, provider fallback, context snapshots, and evolution.
3. **Replay**: pure reducer, side-effect policy, virtual clock/IDs, controlled
   fakes, restart checkpoints, and scenario harness integration.
4. **Evaluation product**: deterministic evaluators, sourced fixtures, CLI,
   JSON output, schema/fixture validation, CI, and operator/developer guides.
5. **Semantic grading**: only if deterministic coverage is complete and a
   concrete semantic question justifies the cost and risk.

Dependent PRs start from merged `origin/main`. A PR must not enable unsafe or
unconsumed production capture. Contract-only packages and test utilities are
acceptable foundation outputs because they are independently validated.

## Completion Criteria

- Every required trace category is captured or explicitly documented as
  unavailable with a source-level reason.
- Serialized traces pass secret, bounds, schema, ordering, and migration tests.
- Replay demonstrates that live side effects are structurally unavailable.
- Every named deterministic evaluator has passing and failing sourced fixtures.
- CLI output is stable and machine-readable; deterministic fixtures run in CI.
- Operator and fixture-author documentation covers security and limitations.
- All dependent pull requests are merged and the final audit is against merged
  `origin/main`, not only topic branches.
