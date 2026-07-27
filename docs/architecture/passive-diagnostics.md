# Passive Diagnostics

## Status

This document defines the replacement architecture for MintClaw tracing and
supersedes the replay and evaluation program. The inventory is based on
`origin/main` at `a87183a9`.

The durable human-interaction runtime is already shipped. Questions, answers,
approvals, task continuation, and delivery remain authoritative runtime
concerns. Diagnostic capture observes that runtime but cannot participate in
its correctness.

The cleanup has removed authoritative task trace state and projection,
simplified the writer to best-effort submission, and removed the eval CLI,
replay, evaluator, and scenario packages. The retained implementation is
`pkg/diagnosticcapture`, `pkg/diagnostictrace`,
`pkg/agent/trace_capture.go`, and `pkg/agent/trace_turn_projector.go`, configured
under `diagnostics.trace_capture`.

## Decision

MintClaw has three layers:

1. **Authoritative runtime.** Durable tasks, interactions, approvals,
   continuation, and delivery own their state and exactly-once guarantees.
2. **Passive diagnostics.** Optional bounded traces record a readable model,
   tool, state, retry, error, delivery, and timing timeline. Capture is
   asynchronous and best effort. Metadata-only and redacted-content modes
   remain supported.
3. **Focused regression tests.** Once an incident is understood, its relevant
   invariant is tested directly against the runtime. A case-specific fixture is
   acceptable only when an ordinary test cannot reproduce the incident.

The governing rule is:

> Losing a diagnostic trace may make an incident harder to investigate, but it
> must never change, delay, duplicate, reject, retain, or prevent the agent task
> itself.

Diagnostic capture must never mutate authoritative workflow state, protect a
source record from pruning or reuse, acknowledge persistence to a registry,
block task progress or shutdown, or become a prerequisite for runtime
functionality.

## Non-Goals

This cleanup will not:

- preserve compatibility with removed trace schemas, commands, files, or
  configuration;
- build a universal event replay system;
- build deterministic or model-based evaluator infrastructure;
- introduce a replacement coordinator, acknowledgement protocol, or cross-store
  transaction;
- make diagnostic completeness a deployment or runtime correctness condition;
- constrain future runtime changes to historical diagnostic event shapes.

## Baseline

The dedicated production implementation is approximately 6,509 lines before
counting trace state embedded in `pkg/tasks/registry.go`.

| Area | Production lines | Current role |
| --- | ---: | --- |
| `pkg/evalcapture` | 1,481 | builder, durable coordinator, trace writer |
| `pkg/evaltrace` | 790 | trace schema and storage |
| `pkg/evalreplay` | 873 | universal replay |
| `pkg/evalevaluator` | 633 | deterministic evaluators |
| `pkg/evalscenario` | 543 | scripted scenario replay |
| Former evaluator CLI | 175 | Removed evaluator command and wiring |
| `pkg/agent/trace_capture.go` | 218 | capture manager |
| `pkg/agent/trace_turn_projector.go` | 818 | passive turn projection |
| `pkg/agent/trace_task_projector.go` | 978 | durable task projection |

The evaluator packages contain another approximately 2,714 test lines. Task
registry trace behavior is additionally covered by task projector and agent
capture tests.

The baseline dependency direction was:

```text
authoritative task registry
  -> trace capture markers and retained event journal
  -> task trace projector
  -> durable projection coordinator
  -> tracked writer receipt
  -> registry compare-and-set confirmation

mintclaw eval
  -> evalevaluator
  -> evalreplay
  -> evaltrace

evalscenario
  -> agent runtime
  -> evalreplay
  -> evaltrace
```

The target dependency direction is one way:

```text
runtime events -> passive diagnostic builder -> bounded best-effort writer
                                                   -> readable JSON trace
```

Nothing in the runtime may import or wait on a downstream diagnostic consumer.

## Runtime Coupling To Remove

The following baseline production paths let tracing influence authoritative
behavior and have been removed:

- `pkg/tasks/registry.go` persists `TraceCapturePending`,
  `TraceCaptureEvents`, and `TraceCaptureDropped`.
- The task registry exposes `SetTraceCapturePending`,
  `SetTraceCaptureProtection`, and `ConfirmTraceCapturePersisted`.
- A pending trace marker can reject task-generation reuse with
  `ErrTraceCapturePending`.
- Pending capture protects task records from pruning.
- `pkg/agent/trace_task_projector.go` treats task registry revisions and trace
  confirmations as a durable projection protocol.
- `pkg/evalcapture/coordinator.go` recovers projections, waits for admission,
  consumes writer receipts, and acknowledges persistence to source registries.
- The capture manager's shutdown path waits for coordinator admission and
  writer draining.
- Tracked writer submission and receipt APIs exist to support source
  acknowledgement rather than passive diagnostics.

The merged interaction registry does not contain equivalent trace markers.
None will be added.

## Inventory

### Keep

| Surface | Decision |
| --- | --- |
| Durable task and interaction registries | Keep authoritative and independent of diagnostics. |
| Runtime domain events | Keep when they have runtime, logging, or diagnostic value independent of replay. |
| Turn projection | Keep as a passive source of readable timelines; simplify its dependencies and names. |
| Bounded trace records and storage | Keep the minimum schema needed for direct inspection. |
| Metadata-only mode | Keep for low-content operational capture. |
| Redacted-content mode | Keep as the rich diagnostic mode; retain model/tool/error detail after secret filtering. |
| Credential filtering and explicit-secret redaction | Keep and test directly. |
| Retention and storage bounds | Keep; local deployment uses 168 hours. |
| JSON output | Keep as the direct Codex and human debugging interface. |

Rich content is projected only when `redacted_content` is explicitly enabled.
Runtime event producers may attach bounded, already-redacted diagnostic
previews; metadata-only events leave those fields empty. The passive projector
applies the same credential redaction again when it converts existing raw
turn, tool, retry, delivery, or error fields. Preview generation cannot change
whether an event is emitted or whether the underlying operation succeeds.

### Simplify

| Surface | Target |
| --- | --- |
| Former `pkg/evalcapture` builder, now `pkg/diagnosticcapture` | Retain only passive bounded construction and filtering. |
| Former `pkg/evalcapture` writer, now `pkg/diagnosticcapture` | One nonblocking diagnostic priority, bounded queue/retry/storage, observable drops, and nonblocking stop. No receipts or admission waits. |
| `pkg/agent/trace_capture.go` | Observe runtime and submit best effort. Never wait for persistence. |
| `pkg/agent/trace_turn_projector.go` | Produce a useful model/tool/error timeline without replay-specific records. |
| Former `pkg/evaltrace`, now `pkg/diagnostictrace` | Retain only diagnostic schema, validation needed for safe reading, storage, filtering, and retention. Remove evaluator-only vocabulary. |
| Configuration | Rename `evaluation.trace_capture` and evaluation storage terminology to diagnostics. No compatibility alias is required. |
| Operator documentation and skills | Teach direct trace discovery, rendering, and root-cause analysis without a dedicated evaluator CLI. |

### Remove

| Surface | Reason |
| --- | --- |
| `pkg/agent/trace_task_projector.go` | Implements lossless derived task projection. |
| `pkg/evalcapture/coordinator.go` | Implements registry-to-trace acknowledgement and recovery. |
| Tracked writer receipts and admission waits | Exist for lossless projection. |
| Task registry trace fields, journals, mutation APIs, errors, and pruning protection | Diagnostics cannot alter authoritative state or lifetime. |
| Former evaluator command package and CLI registration | The evaluator product is not used. |
| `pkg/evalreplay` | Duplicates runtime state machines and constrains their evolution. |
| `pkg/evalevaluator` | Mechanical invariants belong in direct runtime tests. |
| `pkg/evalscenario` | Remove portions that exist only for universal replay/evaluation. Preserve independently useful runtime test helpers only when demonstrated. |
| Evaluator fixtures, reports, CI expectations, and schema fields | No remaining consumer. |
| Replay/evaluator architecture documents and user references | Superseded by this design. Historical PR discussion remains in Git history. |

## Surface Map

This table is the implementation checklist. A surface is complete only when its
target state is represented on merged `main`.

| Current surface | Target state |
| --- | --- |
| Former `pkg/config.Config.Evaluation` | Replaced by `Config.Diagnostics`. |
| Former `pkg/config/evaluation.go` and `evaluation_test.go` | Replaced by diagnostic capture config containing only limits, mode, path, and retention. |
| `pkg/agent/trace_capture.go` | Simplify to passive manager lifecycle and nonblocking submission. |
| `pkg/agent/trace_turn_projector.go` | Keep passive turn/runtime-event projection and rich redacted timeline. |
| `pkg/agent/trace_task_projector.go` and tests | Delete. Do not replace with another task projector. |
| `pkg/tasks/registry.go` trace fields, validation, journaling, reuse, pruning, and acknowledgement branches | Delete. |
| `pkg/tasks/observer.go` trace-journal cloning | Delete with the registry fields. |
| Former `pkg/evalcapture/builder.go` | Kept as `pkg/diagnosticcapture/builder.go` with bounded record assembly and no evaluator-only limits. |
| Former `pkg/evalcapture/writer.go` | Kept as `pkg/diagnosticcapture/writer.go` with bounded best-effort persistence and no tracked receipt/admission APIs. |
| `pkg/evalcapture/coordinator.go` and tests | Delete. |
| Former `pkg/evaltrace/{canonical,limits,payloads,redact,store,types,validate}.go` | Kept under `pkg/diagnostictrace` with only readable diagnostic contracts, redaction, bounds, and storage. |
| `pkg/evalreplay` | Delete. |
| `pkg/evalevaluator` and `testdata/historical_failures.json` | Delete. |
| `pkg/evalscenario` | Delete unless a helper is proven useful to direct runtime tests and can move without replay/fixture dependencies. |
| `pkg/agent/memory_replay_test.go` use of `evalreplay.VirtualClock` | Replace with a local/shared test clock independent of replay. |
| Former evaluator command package | Delete. |
| CLI entry-point evaluator import and command registration | Delete. |
| Former `state/evaluation/traces` | Replaced by `state/diagnostics/traces`. Local deployment is migrated atomically; old data is not read. |
| `docs/architecture/replay-evaluation.md` | Delete after its implementation inventory is removed. |
| `docs/architecture/replay-evaluation-audit.md` | Delete. |
| `docs/architecture/interaction-replay-redesign.md` | Delete; PR #339 remains closed. |
| `docs/guides/replay-evaluation.md` and `replay-evaluation-overview.md` | Replace with direct diagnostic-trace documentation. |
| `docs/guides/configuration.md` evaluation examples | Replace with diagnostics configuration. |
| Architecture and guide indexes | Link only the retained passive diagnostic documentation. |
| Installed Codex and MintClaw trace-debug skills | Replace evaluator invocation with direct trace inspection. |

### Configuration Map

The former root was `evaluation.trace_capture`, with matching
`MINTCLAW_EVALUATION_TRACE_CAPTURE_*` environment variables. The retained
surface is `diagnostics.trace_capture` and
`MINTCLAW_DIAGNOSTICS_TRACE_CAPTURE_*`; the old names are not accepted.

| Former or retained option | Target |
| --- | --- |
| `enabled` | Keep. Capture remains disabled by default in product defaults and explicitly enabled per local profile. |
| `content_mode` | Keep `metadata_only` and rich `redacted_content`; remove `fixture`. |
| `state_dir` | Keep under a diagnostic name and diagnostic default path. |
| `max_trace_bytes` | Keep as a hard bound. |
| `max_records` | Keep as a hard bound. |
| `max_record_bytes` | Keep as a hard bound. |
| `max_corrections` | Remove; user-correction limits exist for evaluator fixtures rather than direct diagnostics. |
| `retention_hours` | Keep; configure local profiles to 168. |
| `max_traces` | Keep as a storage bound. |

The corresponding Go constants, JSON tags, environment names, defaulting,
validation, configuration documentation, and tests move together. No alias,
migration decoder, or deprecated-field acceptance is required.

### Task Registry Map

Delete all of the following together so no intermediate build can leave a
trace-dependent runtime branch:

- record fields `TraceCapturePending`, `TraceCaptureEvents`, and
  `TraceCaptureDropped`;
- `ErrTraceCapturePending`;
- `SetTraceCapturePending`, `SetTraceCaptureProtection`,
  `ConfirmTraceCapturePersisted`, and `IsTraceCaptureTerminal`;
- trace-journal validation, cloning, bounding, restoration, and persistence;
- create/reuse rejection based on pending capture;
- terminal retention and pruning protection based on pending capture;
- projector-only observer snapshots and tests for these contracts.

Existing persisted fields are ignored by normal JSON decoding after removal.
No cleanup migration or compatibility behavior will be added.

### Writer And Coordinator Map

Delete coordinator concepts including durable sources/candidates,
revision/confirmation contracts, recovery scans, admission errors, source
registration, persistence receipts, and acknowledgement callbacks.

From the writer, remove tracked submission IDs, receipt channels or lookups,
class-based admission needed by durable projection, wait-for-admission, and
drain-as-runtime-gate behavior. Retain only a bounded nonblocking submission
path, bounded retry while the process is running, storage/retention enforcement,
and counters or logs that make dropped diagnostics observable.

## Cleanup Sequence

Every phase must leave `main` buildable and deployable. Use as many focused PRs
as necessary; there is no fixed PR limit.

### 1. Detach Authoritative Tasks

Remove the task projector and coordinator from capture manager construction.
Make any existing trace markers inert in the same build so old local records
cannot affect task reuse, pruning, progress, or shutdown. Then remove the task
registry trace fields, event journals, acknowledgement APIs, errors, and tests.

Exit condition: task lifecycle has no read, write, wait, prune, or reuse branch
whose result depends on tracing.

### 2. Reduce Capture To Passive Writing

Remove tracked submissions, receipts, registry confirmations, recovery, and
drain-dependent shutdown. Retain bounded nonblocking submission, bounded
storage, filtering, retention, and observable incomplete/drop accounting.

Exit condition: queue saturation, writer failure, missing output, and shutdown
cannot fail or delay an agent task.

### 3. Remove Replay And Evaluation

Delete the former evaluator CLI, `pkg/evalreplay`, `pkg/evalevaluator`,
evaluator fixtures/reports, and evaluator-specific CI. Delete scenario replay
code without an independent runtime-testing use.

Exit condition: CLI help has no `eval`; production packages do not import
replay/evaluator packages; no evaluator fixture or report contract remains.

### 4. Prune And Rename Diagnostics

Remove dead record kinds, schema fields, package dependencies, configuration,
paths, tests, and documentation. Rename retained evaluation-oriented surfaces
to diagnostic terms without compatibility adapters.

Exit condition: retained code describes passive diagnostics only and no stale
evaluation terminology implies an automated quality verdict.

### 5. Document, Install, And Deploy

Update MintClaw and installed Codex skills to inspect JSON traces directly.
Document trace modes, redaction, limits, retention, discovery, and a practical
root-cause workflow. Configure intended profiles for rich redacted capture with
168-hour retention, restart them, and verify healthy operation and trace output.

Exit condition: Codex and MintClaw can inspect a generated trace, cite relevant
entries, identify a likely root cause, and propose a focused regression test
without invoking an evaluator.

## Validation

Each implementation PR runs targeted tests first, then relevant race tests,
changed-code lint, full `go test ./...`, and Windows compile checks when shared
packages are touched.

The completed program must prove:

- durable human-input, approval, task, continuation, and delivery tests remain
  green;
- task and interaction schemas contain no trace acknowledgement,
  capture-pending, journal, or pruning-protection state;
- no production workflow imports diagnostics or waits for trace persistence;
- writer failure, saturation, missing output, and shutdown do not alter runtime
  outcomes;
- the former evaluator CLI, replay, evaluators, and evaluator fixtures are
  absent;
- metadata-only and rich redacted traces remain bounded;
- credentials, authentication material, private keys, tokens, and explicit
  secrets are filtered;
- incomplete or missing traces are reported as diagnostic loss, never runtime
  failure;
- production lines and dependency edges are measurably below this baseline.

Useful checks include repository-wide searches for removed package imports,
registry trace fields and APIs, CLI registration, evaluator fixture names, and
stale documentation. Generate and inspect at least one rich trace before
deployment completion.

## Convergence Rules

An implementation PR must remove or simplify an inventory item and own one
coherent boundary. Large deletions are acceptable. Production additions require
a specific immediately-following deletion they enable.

Pause for a product decision only if cleanup requires a new subsystem, a
replacement replay or acknowledgement design, backward compatibility,
cumulative production growth, an unrelated production fix, conflicting target
architecture feedback, or no safe buildable intermediate state.

Do not create prerequisite PRs for evaluator edge cases. Record unrelated
findings separately. The program is complete only when every inventory item is
removed or explicitly retained with current evidence and no required cleanup,
deployment, or PR monitoring remains.
