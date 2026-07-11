# Async Task Delivery

PicoClaw background work now uses an explicit task/completion/delivery shape:

1. A tool or child runtime records a durable task in the task registry.
2. When the async result completes, the runtime builds a typed `AsyncCompletionInput`.
3. The delivery coordinator applies the requested delivery mode: `user_only`, `parent_only`, or `user_and_parent`.
4. User delivery goes through normal outbound text/media delivery.
5. Parent synthesis calls `processAsyncCompletion` directly. It must not publish a synthetic `system` inbound message.
6. The task registry records delivery status, completion id, delivery timestamp, and delivery error if one occurs.

## Current Ownership Boundaries

The runtime now has three distinct delivery paths, and each has a clear owner:

1. Sync tool delivery during the turn
   - Owner: the sync tool loop in `pipeline_execute.go`
   - Scope: normal tool execution and hook-respond tool results
   - Source of truth:
     - `ToolResult.ResponseHandled`
     - explicit delivery outcome (`none`, `direct`, `queued`)
   - Current invariant:
     - `direct` delivery may terminate the turn as fully handled
     - queued media/text may still require a follow-up LLM turn depending on the tool path

2. Final-turn delivery after the loop
   - Owner: `agent_outbound.go`
   - Scope: final answer text and final completion media after the turn result is known
   - Source of truth:
     - final `turnResult`
     - same delivery helpers used for media/text dispatch
   - Current invariant:
     - final media prefers the normal tool-style delivery path
     - if media delivery does not land, the runtime falls back to final text

3. Async completion delivery after child/background work
   - Owner: `delivery_coordinator.go`
   - Scope: spawn/delegate/async tool completions
   - Source of truth:
     - `AsyncCompletionInput`
     - registry delivery status
     - delivery mode: `user_only`, `parent_only`, `user_and_parent`
   - Current invariant:
     - duplicate user/media/parent delivery is suppressed durably
     - parent synthesis never re-enters through synthetic `system` inbound messages

This is not yet a single fully unified delivery coordinator for every runtime
path. The current state is intentionally incremental:

- async completion policy is centrally coordinated
- sync tool and final-turn delivery now share more helper logic and explicit
  delivery outcomes
- legacy parallel policy branches are being removed step by step instead of via
  one large rewrite

## Deliverables

`ToolResult` separates three output channels:

- `ForLLM`: context for the model.
- `ForUser`: text that may be sent directly to the user.
- `Deliverable`: the actual produced result/artifacts.

`Deliverable` is the ownership payload for durable task state. It should describe
what was produced, for example a downloaded media ref, a generated file path, or
extracted text. It must not depend on the wording of the final chat response.

Durable `DeliverablePayload` also carries an optional versioned
`DeliverableReport`. When a producer only provides the legacy deliverable
projection, the task registry derives a minimal report with schema version,
stable content hash, report id, summary, fact claim, metadata, and provenance.
New producers may provide a richer report directly. New consumers that need a
machine contract should prefer `deliverable.report`; `text`, `artifacts`, and
`metadata` remain the compatibility projection.

Tool results can now carry the same report shape on `DeliverableResult`.
Delegate and spawn persist explicit tool reports into task registry records.
When no report is supplied, the registry still derives the minimal projection.

Legacy child-run `Completion` remains supported and is mirrored into
`Deliverable` when possible.

New status/API consumers should treat `Deliverable` as the source of truth for
produced text and artifacts. `Completion` is a legacy child-run handoff payload
and should not be extended with new artifact semantics.

Current contract summary:

- `deliverable`
  - durable ownership payload
  - source of truth for produced text/artifacts in registry/status/board views
- `completion`
  - compatibility adapter for older child-run handoff paths
  - may still be persisted/read, but should not gain new semantics
- final chat wording
  - a projection for users
  - must not be parsed by runtimes as task state

Migration status:

- Done: hide `Completion` from user-facing status/board output when `Deliverable` is present.
- Done: new delegate/spawn registry writes store `Deliverable` as the durable payload and keep `Completion` only when no deliverable is available.
- Done: task registry projects legacy deliverables into `DeliverableReport`
  automatically when producers do not supply one.
- Done: delegate/spawn task registry mapping preserves explicit
  `DeliverableResult.Report` payloads from tool producers.

Migration TODO:

- Keep reading legacy `Completion` only as an adapter for old records.
- Teach important producers to supply richer `DeliverableReport` payloads with
  claims, negative evidence, field deltas, and provenance directly.
- Remove `Completion` from public API/storage after all producers and persisted
  records have migrated.

## Typed Task Events

The task registry has two layers:

- `Record`: the current-state projection for status tools, board views, and
  existing integrations.
- `TaskEvent`: the append-only canonical event stream for lifecycle and
  delivery transitions.

This follows the same principle as durable deliverables: structured state is
canonical; chat, terminal text, and UI strings are projections. Producers should
not require another agent to parse prose in order to decide whether a task
started, completed, failed, delivered, or needs recovery.

`TaskEvent` currently records:

- schema version
- task, board, parent, and step identity
- runtime and producer
- event type
- task status and delivery status
- per-task sequence number
- emitted timestamp
- fingerprint
- small structured payload

The initial event types are:

- `task.upserted`
- `task.status_changed`
- `task.delivery_changed`
- `task.delivery_decision`
- `task.progress`
- `task.updated`
- `task.reconciled`

`task.delivery_decision` is emitted by the async delivery coordinator before it
attempts user delivery or parent synthesis. It records the completion id,
source tool, delivery mode, whether user and/or parent delivery will run, and
the result size hints. The later `task.delivery_changed` event records the
durable delivery outcome. Keeping both events makes failed deliveries and
restart recovery auditable without parsing chat text.

Cron-triggered tasks also emit `task.delivery_decision` when the runtime starts
the cron execution. The cron task record's `delivery_mode` distinguishes the
execution shape:

- `deliver_text`: publish the scheduled text directly without an agent turn.
- `agent_turn`: run the scheduled message through the agent.
- `command`: execute the scheduled command path.

This makes reminders auditable from task status alone: an operator can see why
the cron run fired, whether it was direct text or an agent turn, and the later
delivery outcome without reading service logs or inferring behavior from chat
wording.

The event stream is persisted in the same `state/task_registry.json` snapshot
as `tasks`. `Record` remains the compatibility API and is still what most tools
read. New consumers that care about auditability, idempotency, or recovery
should prefer events and treat records as a projection.

Current source-of-truth rule:

- audit/debug/recovery
  - prefer `TaskEvent`
- task status, board views, tool/UI compatibility
  - prefer normalized `Record`
- user-facing prose
  - never treat chat text as canonical lifecycle state

Migration TODO:

- Emit explicit delivery events for additional coordinator/reconciliation
  phases when a consumer needs finer-grained observability.
- Introduce a versioned `DeliverableReport` shape for rich outputs with claims,
  artifacts, field deltas, and provenance.
- Render Telegram/GitHub/web summaries from structured reports instead of
  freeform child-agent prose.

## Status Tools

Use `task_status` for durable task history across spawn, delegate, cron executions, and future background runtimes. It is the source of truth for completed tasks and restart-persistent state.

Use `task_status {"task_id":"...","include_events":true}` to inspect a task's
full typed event stream. The output includes the current record projection,
completion id, delivery timestamp/error, and event lines with runtime, producer,
source, status, delivery status, payload kind, delivery mode, completion id,
fingerprint, and payload. Use `task_status {"include_events":true}` without a
specific `task_id` to show recent events for each visible task in the list.

Active delegate/spawn runs periodically heartbeat the task registry by updating
`last_event_at` while their child turn is still running. `freshness=stalled`
therefore means the active run has not reported liveness recently, not merely
that it started a long time ago.

`spawn_status` is kept as a compatibility/debug view for tasks started specifically by the `spawn` tool. It is backed by the same durable registry but intentionally remains spawn-only.

Status tool rule of thumb:

- use `task_status` for the durable cross-runtime view
- use `spawn_status` only for spawn-specific debugging or backward compatibility

## Legacy System Messages

Older async completion paths used synthetic inbound messages with `channel=system` and `kind=async_completion`. That path is now an adapter only, so queued or stored legacy messages can still be processed.

New producers must not enqueue async completions through `PublishInbound(system)`. They should use `AsyncCompletionInput` and the delivery coordinator instead.

Current legacy boundary:

- reading legacy synthetic async completion messages is still supported
- producing new synthetic async completion messages is not allowed
- extending legacy `completion` payloads with new semantics is not allowed

## Runtime Smoke Checklist

- Run a simple media task that only sends a video.
- Run a composite media task that sends a video and returns text for parent synthesis.
- Run or trigger a scheduled cron task and confirm it appears as `runtime=cron`.
- Check `task_status` after completion.
- Restart the service.
- Check `task_status` again and confirm completed tasks are still visible.
- Confirm no completed task replays user-visible text or media after restart.
