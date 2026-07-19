# Durable Human Interaction

## Status

Proposed architecture for model-requested user input and human approval of
sensitive tool calls. This document defines the contracts that implementation
PRs must preserve.

## Problem

ForgeClaw can receive steering while a turn is running, but it cannot suspend a
turn, release runtime resources, survive a restart, and later resume the exact
tool call after an authorized person responds. Approval hooks are synchronous,
and background tasks have no `waiting_for_input` state.

This prevents agents from safely handling workflows such as:

- asking the user to choose between materially different implementations;
- collecting missing deployment parameters without abandoning a task;
- pausing a background coding task for a product decision;
- requiring human approval before a sensitive shell or network operation;
- surviving a process restart while any of those questions are outstanding.

The feature must not keep an agent goroutine, provider request, session claim,
or tool execution open while it waits. Waiting may last hours and is durable
workflow state, not an active model turn.

## Design Goals

1. Provide a built-in `request_user_input` tool with structured questions.
2. Use one durable interaction subsystem for model questions and approvals.
3. Resume the original tool call with a correctly paired tool result.
4. Correlate answers to the canonical session, route, sender, and request.
5. Make answer acceptance and restart recovery idempotent.
6. Represent waiting background tasks honestly as `waiting_for_input`.
7. Expose lifecycle events and bounded evaluation traces for debugging.
8. Keep policy authority outside model-controlled tool arguments.
9. Work across text channels without requiring channel-specific forms.

## Non-Goals

- Holding an LLM or tool request open while a person responds.
- Treating arbitrary steering messages as answers.
- Building a general workflow engine or distributed transaction system.
- Granting durable policy exceptions based on model-generated text.
- Supporting multiple simultaneous unresolved interactions in one canonical
  session in the first version.
- Automatically choosing an answer when a request times out.

## Prior Art and Deliberate Differences

OpenClaw's ask-user tool provides a useful structured question schema, explicit
timeouts, and a gateway boundary. Its current pending-question map is in process
memory and restricts blocking questions to the main session. ForgeClaw adopts
the structured UX and bounded inputs, but replaces the in-memory wait with a
durable suspend/resume protocol that also works for background tasks.

Hermes cleanly separates clarification and approval callbacks from ordinary
tool execution. Its approval queues are also process-local. ForgeClaw adopts
the separation between clarification and authorization, while storing both as
typed interactions with restart reconciliation.

ForgeClaw already has stronger primitives that this design reuses: canonical
routed session keys, sender and topic context, durable task records, completion
IDs, runtime events, and replay traces. Interaction state augments those
subsystems instead of creating a second routing or task model.

## Core Model

An interaction is a durable request for one authorized human response.

```go
type InteractionKind string

const (
    InteractionQuestion InteractionKind = "question"
    InteractionApproval InteractionKind = "approval"
)

type InteractionStatus string

const (
    InteractionCreated   InteractionStatus = "created"
    InteractionWaiting   InteractionStatus = "waiting"
    InteractionClaimed   InteractionStatus = "answer_claimed"
    InteractionResuming  InteractionStatus = "resuming"
    InteractionCanceling InteractionStatus = "canceling"
    InteractionResolved  InteractionStatus = "resolved"
    InteractionCancelled InteractionStatus = "cancelled"
    InteractionFailed    InteractionStatus = "failed"
)

type InteractionOutcome string

const (
    OutcomeAnswered InteractionOutcome = "answered"
    OutcomeTimedOut InteractionOutcome = "timed_out"
    OutcomeAllowed  InteractionOutcome = "allowed"
    OutcomeDenied   InteractionOutcome = "denied"
    OutcomeDeliveryUnknown InteractionOutcome = "delivery_unknown"
)
```

Each record contains:

- a random opaque interaction ID and short display ID;
- kind, status, terminal outcome, creation/update/expiry timestamps, and state
  revision;
- agent, canonical session, route session, channel, chat, topic, and account;
- authorized sender identity captured from trusted inbound context;
- originating turn ID, tool call ID, and tool name;
- optional durable task ID;
- bounded structured questions or a policy-generated approval prompt;
- delivery state and attempt metadata;
- the accepted answer, answer message identity, and sanitized display form;
- resume attempt metadata and terminal error details.

The raw arguments of a tool awaiting approval are not copied into this record.
Approval records contain a policy-produced bounded summary and a keyed hash of
canonical arguments. Full arguments remain in canonical session history and,
when enabled, protected full traces under their existing retention policy.

## State Machine

```text
created -> waiting -> answer_claimed -> resuming -> resolved
    |         |              |             |
    |         |              |             +-> failed
    |         |              +-> retry recovery (status unchanged)
    |         +-> answer_claimed (timeout outcome)
    +-> canceling -> cancelled
    +-> failed
```

Rules:

- Transitions use compare-and-swap semantics on ID, status, and revision.
- Only `waiting` accepts an answer.
- Only one nonterminal interaction may exist per canonical session.
- A duplicate inbound delivery with the same message identity is a no-op.
- A second answer after `answer_claimed` receives an explanatory response and
  cannot overwrite the first answer.
- A recoverable commit or resume failure records an attempt without reopening
  the request; the accepted answer remains immutable while recovery retries.
- Terminal records are retained for a bounded audit period, then pruned.
- Lifecycle status and resolution outcome are separate. Timeout never silently
  selects an option or becomes terminal before resumption: it atomically claims
  the request with `OutcomeTimedOut`, appends an explicit timeout tool result,
  and resumes the model so it can explain or choose another safe path.

## Durable Storage

Use a dedicated interaction store under the configured data directory, following
the task registry's atomic append/checkpoint and bounded event-log patterns.
Do not place interaction records in general configuration or session metadata.

Required store operations are intentionally narrow:

```go
type InteractionStore interface {
    Create(context.Context, CreateInteraction) (Interaction, error)
    Get(context.Context, string) (Interaction, error)
    FindWaiting(context.Context, AnswerRoute) ([]Interaction, error)
    Transition(context.Context, TransitionInteraction) (Interaction, error)
    ListNonterminal(context.Context) ([]Interaction, error)
    Prune(context.Context, time.Time) error
}
```

The first implementation may use a JSON checkpoint plus append-only event log,
provided writes are atomic, revisions are monotonic, corruption fails closed,
and retention is bounded. An interface keeps a future database backend possible
without leaking persistence details into tools or channel adapters.

## Suspension Contract

The pipeline needs an explicit suspended outcome. Suspension is neither success,
failure, abort, nor finalization.

```go
const ToolControlSuspend ToolControl = ...

type turnResult struct {
    // existing fields
    suspendedInteractionID string
}
```

When `request_user_input` or approval suspends execution:

1. The assistant tool-call message is already durably persisted by the LLM
   phase. Creation of the interaction verifies that persistence succeeded.
2. The interaction is persisted before its outbound prompt is published.
3. Before calling a channel, delivery durably transitions to `sending`. A
   confirmed acknowledgement transitions to `delivered` and then `waiting`.
   Failures known to occur before any channel attempt become `not_sent` and are
   retryable. A crash, partial delivery, or uncertain channel error after
   `sending` is `ambiguous` and is never retried automatically because most
   channel APIs do not provide an idempotency key.
4. The tool loop returns `ToolControlSuspend` without adding a fabricated tool
   result for the suspended call.
5. Remaining tool calls in the same model response are recorded as deferred and
   are not executed. The model must reissue them after resumption if needed.
6. The turn runner exits with a suspended status, skips normal final rendering,
   and releases the active route/session claim and provider resources.
7. A suspended turn emits no default or duplicate user response.

Suspension must be represented in turn status and metrics so it is not counted
as an error or ordinary completion.

## Answer Correlation and Authorization

Inbound interception happens after canonical routing is resolved and before
busy-session steering, command handling, or starting a normal turn.

An inbound message can answer a request only when:

- its canonical and route session keys match the stored request;
- channel, account, chat, and topic match where present;
- sender ID matches the trusted sender captured when the request was created;
- the request is still `waiting` and not expired; and
- correlation is unambiguous.

Text channels support two correlation forms:

- a reply to the delivered prompt when reply metadata is available;
- `/answer <short-id> <answer>` for explicit correlation.

When exactly one request is waiting for that authorized route and sender, a
plain next message may be accepted as its answer. In group conversations,
sender matching is mandatory. A message routed to a different canonical session
continues through normal inbound handling. An ambiguous or unauthorized message
for the suspended canonical session is durably deferred for normal continuation
after the interaction closes; it is not appended between the incomplete
assistant tool call and its eventual tool result. It never reveals protected
request details.

The manager validates option labels but always permits bounded free-form text
for clarification questions. Approval interactions accept only the fixed
policy-owned choices `allow_once` and `deny` in the first version.

## Atomic Answer Commit and Resumption

Canonical provider history requires every assistant tool call to be followed by
its matching tool result. The existing sanitizer correctly drops incomplete
tool-call turns, so resumption must commit the answer before assembling context.

Answer processing is:

1. Compare-and-swap `waiting -> answer_claimed`, storing inbound message identity
   and the bounded answer.
2. Inspect canonical history for the originating tool call and matching result.
3. If the result is absent, append exactly one `role=tool` message using the
   original tool call ID. Use an error-aware session writer and ingest it into
   the context runtime only after the canonical write outcome is known.
4. Transition `answer_claimed -> resuming`.
5. Claim the session and invoke a dedicated `ResumeInteraction` path with no
   synthetic user or steering message. Context now contains a valid paired tool
   turn and the next LLM call can continue naturally.
6. On normal completion, transition to `resolved`. On a recoverable process or
   provider failure, retain enough state for reconciliation to retry.

The tool-result payload is structured JSON containing request ID, question IDs,
answers, and resolution reason. It must not contain channel envelope data.

History reconciliation makes crash windows idempotent:

- `answer_claimed` plus no tool result: append it and resume;
- `answer_claimed` plus matching result: advance to `resuming` and resume;
- `resuming` plus no active turn: retry resumption;
- a completed continuation plus a nonterminal interaction: detect the matching
  result and later assistant response, then mark resolved;
- conflicting history or mismatched hashes: fail closed and emit diagnostics.

## `request_user_input` Tool

The built-in tool is available to normal stateful turns and background tasks,
but not stateless direct turns, ephemeral subturns without durable sessions, or
contexts whose channel cannot deliver a response.

Schema limits:

- one to three questions;
- stable unique question IDs;
- headers up to 12 characters;
- bounded question and description lengths;
- zero or two to three options per question;
- optional timeout from 60 seconds to 24 hours;
- default timeout supplied by trusted configuration, initially one hour.

The tool itself only validates model input and asks the interaction manager to
suspend. It does not own maps, timers, persistence, inbound routing, or channel
delivery. Tool output on resumption is generated by the manager.

## Human Approval

Approval is a second producer of the same interaction protocol, not a second
waiting subsystem.

The current synchronous hook result remains supported for immediate allow or
deny decisions. A trusted approval policy may additionally return
`require_human`, with a bounded policy-generated explanation. The model cannot
request approval to elevate its own authority and cannot choose the approval
recipient.

On `allow_once`, the resumed pipeline verifies all of the following before tool
execution:

- interaction ID, tool call ID, tool name, canonical argument hash, and session
  match the pending call;
- the approval is unexpired and has not been consumed;
- current policy still permits human override for that classification.

The approval is consumed exactly once. `deny` appends a normal denied tool result
and resumes the model. Persistent allowlists and approve-for-session behavior are
out of scope until policy and audit evidence justify them.

## Background Task Integration

Add `waiting_for_input` as a nonterminal durable task status and an optional
interaction ID on task records.

- A root or delegated task that suspends transitions from `running` to
  `waiting_for_input`.
- It does not publish completion, consume a completion ID, or start a duplicate
  retry while waiting.
- Answer claim transitions the task back to `running` before continuation.
- Timeout, cancellation, and terminal resume failures propagate through normal
  task state and delivery semantics.
- Task status output identifies that human input is required and includes only
  the safe short request ID and bounded prompt summary.
- Restart reconciliation preserves waiting tasks instead of marking them lost.

## Events, Traces, and Privacy

Add typed lifecycle events for created, delivery attempted, waiting, answer
accepted/rejected, resume started, resolved, timed out, cancelled, and failed.
Events include interaction kind, IDs, route/session hashes, task ID, status,
latency, and failure codes. They exclude raw answers, full questions, secrets,
and tool arguments.

Evaluation traces may capture bounded question and answer content according to
the existing metadata/full capture mode and redaction policy. Correlation fields
must link interaction ID, turn ID, tool call ID, task ID, inbound message, and
delivery attempt so replay evaluators can detect:

- duplicate prompts or accepted answers;
- unauthorized answer acceptance;
- missing or duplicate tool results;
- restart recovery failures;
- task-state mismatches;
- final responses emitted while suspended.

The interaction record, accepted answer, and canonical tool result have
exactly-once state transitions. Channel publication cannot be exactly once when
the remote API has no idempotency protocol. The runtime therefore records
`sending` before the external call and records `delivered` only after an
acknowledgement. Recovery treats a surviving `sending` state or a partial-send
error as `ambiguous`: it does not resend that prompt or final response. Prompt
ambiguity resumes the suspended tool with a `delivery_unknown` outcome; final
ambiguity terminalizes the interaction with a visible failure code. The stable
interaction delivery key is correlation metadata, not an idempotency claim.

Cancellation uses the same durable ordering. `/stop` first transitions the
record to `canceling`, then writes the paired canceled tool result, and finally
terminalizes it. Recovery completes any surviving `canceling` record, so a
crash cannot leave waiting state in conflict with canceled canonical history.

## Configuration

The question tool is enabled by default when durable sessions and outbound
delivery are available. Human approval remains opt-in because it changes tool
execution policy.

Configuration owns operational limits, not interaction authority:

```json
{
  "tools": {
    "request_user_input": {
      "enabled": true,
      "default_timeout_seconds": 3600,
      "max_timeout_seconds": 86400,
      "retention_hours": 168
    }
  }
}
```

Invalid bounds fail configuration validation. Disabling the tool prevents new
requests but does not delete existing requests; reconciliation can still resolve,
timeout, or cancel them.

## Recovery and Operations

At startup, after sessions, tasks, channels, and the event sink are available:

1. load and validate nonterminal interactions;
2. claim overdue requests with a timeout outcome;
3. retry only prompts whose delivery is known to be `not_sent`, and reconcile
   ambiguous delivery without resending;
4. reconcile `answer_claimed` and `resuming` records against canonical history;
5. restore task `waiting_for_input` projections;
6. resume eligible interactions with bounded concurrency;
7. prune terminal records beyond retention.

Shutdown does not cancel waiting interactions. Explicit task cancellation does.
Deploy/restart tooling must report nonterminal interaction counts and perform a
post-restart reconciliation check.

## Implementation Sequence

1. **Architecture:** land this contract before runtime changes.
2. **Manager:** add durable types, store, state transitions, event vocabulary,
   timeout/pruning, and restart reconciliation tests.
3. **Question tool:** add schema, pipeline suspension, outbound rendering,
   inbound correlation, atomic answer commit, and dedicated resumption.
4. **Tasks:** add `waiting_for_input`, status projection, cancellation, and
   restart behavior for spawn/delegate/cron paths.
5. **Approval:** extend trusted policy results with `require_human` and consume
   one-time approval through the shared manager.
6. **Evaluation and operations:** add trace correlation, deterministic replay
   checks, configuration documentation, deployment checks, and end-to-end tests.

Each runtime PR must be independently deployable, keep incomplete later stages
disabled or unreachable, and include migrations or compatibility handling only
for persisted data introduced by earlier PRs in this sequence.

## Acceptance Criteria

- A foreground question suspends without a final/default response and resumes
  the exact tool call when its authorized user answers.
- A background task visibly waits, survives restart, resumes once, and delivers
  one completion.
- Duplicate inbound deliveries and concurrent answers produce one tool result
  and one continuation.
- Unauthorized senders cannot answer or inspect a request.
- A restart at every state-machine boundary converges without lost questions,
  duplicate prompts, or duplicate tool execution.
- Timeout and cancellation resume or terminate through explicit audited states.
- Human approval cannot be forged through model arguments or reused.
- Replay evaluators identify malformed pairing, duplicate delivery, invalid
  state transitions, and missing task projection.
- Targeted race tests and broader shared-package tests pass before activation.
