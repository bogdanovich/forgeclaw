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

- Add a status/debug surface for task events once there is a concrete consumer.
- Emit explicit delivery events for additional coordinator/reconciliation
  phases when a consumer needs finer-grained observability.
- Introduce a versioned `DeliverableReport` shape for rich outputs with claims,
  artifacts, field deltas, and provenance.
- Render Telegram/GitHub/web summaries from structured reports instead of
  freeform child-agent prose.

## Task Boards

The task registry also carries lightweight task-board metadata:

- `board_id`: workflow/group id for all related steps.
- `parent_task_id`: parent/root task when a step belongs to a larger workflow.
- `step_id` / `step_title`: stable step identity and readable title.
- `owner`: agent/runtime responsible for the step.
- `depends_on` / `blocked_by`: dependency and blocker ids.

This is intentionally built on the existing durable registry rather than a
separate planner store. The registry remains the low-level run ledger, while the
board fields let agents inspect a composite workflow as one operational plan.

Task boards may also have an optional `task_packet` on the board-root record.
The packet is the typed workflow contract: objective, scope, acceptance
criteria, verification plan, resources, constraints, reporting, and recovery
policy. It is generic by default and can carry domain-specific blocks such as
`coding`, `media`, `research`, or `nutrition`. Code-specific fields like repo,
worktree, branch policy, commit policy, and tests belong under `coding`, not at
the top level.

Use `task_packet` for serious/composite workflows where the success contract
matters. Do not add it to simple one-step tasks just to satisfy ceremony.

`delegate` and `spawn` expose these board fields as optional parameters. For a
composite workflow, the orchestrating agent should choose one `board_id` and
create it with `task_board`, add planned child steps, then pass the shared
`board_id` to each `delegate`/`spawn` child run with a stable `step_id`,
readable `step_title`, and `depends_on` when ordering matters.

Synchronous `delegate` steps also accept `timeout_seconds`. Use it for child
steps that can block the parent workflow, especially when a later step depends
on their result.

## Status Tools

Use `task_status` for durable task history across spawn, delegate, cron executions, and future background runtimes. It is the source of truth for completed tasks and restart-persistent state.

Use `task_board {"action":"list","board_id":"..."}` to inspect the planned and
executed records for one workflow. Use `task_board {"action":"results",...}` to
read durable deliverables produced by completed child runs. `task_status
{"board_id":"..."}` remains the lower-level status view over the same records.

`task_board results` returns raw result-bearing records for compatibility and a
`step_results` view for orchestration. `step_results` groups records by
`step_id`, hides placeholder board steps, exposes current latest-run output at
top level, and includes latest run/failure metadata so the parent can decide
whether to continue, retry, or report a failure. If the latest run failed,
top-level `deliverable`/`has_result` remain empty/false; any older successful
output is exposed only under explicit `latest_successful_*` fields.

`task_board ready` is a read-only dependency resolver. It groups visible board
steps into `ready_steps`, `waiting_steps`, `active_steps`, `done_steps`, and
`blocked_steps`. A planned step is ready when every `depends_on` step has
succeeded. Missing or not-yet-finished dependencies are waiting; failed/lost
dependencies or explicit `blocked_by` markers are blocked. A succeeded step
with missing or failed dependencies is also reported as blocked/inconsistent so
schedulers do not treat an invalid DAG as satisfied. This is the bridge toward
future board execution, but it does not execute anything.

`task_board next` is a dry-run executor plan built on the same readiness model.
It returns runnable steps with suggested `delegate_args`, `spawn_args`, or
manual `task_board.update` args, but still does not execute tools. This gives an
LLM/orchestrator a deterministic next action without hiding execution policy,
concurrency, retry, or delivery-mode choices inside the board primitive.
Planned steps can provide execution hints such as `execution_tool`,
`delivery_mode`, and `timeout_seconds`; when present, `next` uses those hints
instead of relying only on owner/manual heuristics.

`task_board_execute_next` is the first intentionally conservative execution
primitive. It is disabled by default and only executes one ready
delegate-backed step from the `task_board next` plan. It does not auto-run
`spawn` steps, because spawn has async callback and delivery ownership
semantics, and it does not execute manual/local steps. For those cases, use
`task_board next` to get the plan and then call the appropriate tool explicitly.

`task_board_execute_all` applies the same conservative policy repeatedly. It is
also disabled by default and executes ready delegate-backed steps until the
board has no more delegate-backed ready work, reaches `max_steps`, hits a
delegate error, or stops on active/waiting/blocked/non-delegate work. This is
the generic "continue this approved board workflow" primitive: the model can
create a board, get one user approval, then call `task_board_execute_all`
instead of asking for another "continue" between independent delegate steps.
It still does not run `spawn` or manual/local steps automatically.

Current task-board role:

- durable workflow grouping and inspection
- explicit step planning and dependency visibility
- optional typed workflow contract via `task_packet`
- conservative execution bridge for delegate-backed steps

Task boards are intentionally not a fully autonomous planner. They provide
durable workflow structure first, then opt-in execution primitives on top. That
keeps orchestration state explicit and reviewable while still letting approved
delegate-backed workflows continue without user nudges between every step.

`task_board list` also returns an effective board view derived from the raw
records:

- `overall_status`: computed workflow state.
- `effective_counts`: counts by effective step status.
- `effective_steps`: one computed row per `step_id`.
- `freshness`: `healthy`, `stalled`, `finished`, `lost`, or `unknown`.
- `latest_run_task_id`: latest non-placeholder run for the step, when present.

The effective view does not mutate registry state. It lets agents and UIs tell
whether a workflow is actually progressing, stalled, finished, or only planned
without having to infer that from raw task records.

Active delegate/spawn runs periodically heartbeat the task registry by updating
`last_event_at` while their child turn is still running. `freshness=stalled`
therefore means the active run has not reported liveness recently, not merely
that it started a long time ago.

`spawn_status` is kept as a compatibility/debug view for tasks started specifically by the `spawn` tool. It is backed by the same durable registry but intentionally remains spawn-only.

Status tool rule of thumb:

- use `task_status` for the durable cross-runtime view
- use `task_board` for workflow/step inspection and results
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
