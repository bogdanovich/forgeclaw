# Interaction Replay Redesign

## Status

This document replaces the delivery approach attempted in draft PR #272. That
branch remains an architectural spike and a source of regression cases; its
unpublished APIs and interaction trace format have no compatibility guarantee.

The durable human-interaction runtime merged before the spike remains the
production authority. This redesign extends its observability and evaluation
without making trace capture part of the interaction state machine.

Stages 1 through 7 are merged. Stage 8 extracts durable trace persistence from
the agent capture manager; later stages remain separate pull requests.

## Why The Spike Is Not The Implementation

The spike established that dedicated interaction traces, deterministic replay,
startup reconciliation, and evidence-aware diagnostics are useful. Review also
showed that implementing them together left several contracts implicit:

- active turn identity did not initially include workspace ownership;
- registry snapshot and observer ordering were not one atomic contract;
- capture correlation, recovery, retry, retention, and persistence accumulated
  in one agent-owned manager;
- replay duplicated interaction transition rules with a second state machine;
- incomplete evidence was interpreted from diagnostic-code allowlists.

Those are boundary problems. Further conditionals in the spike would not fix
them. The replacement is a sequence of independently useful foundations whose
contracts are testable before interaction evaluation depends on them.

## Design Rules

1. Identity is explicit. Trace-producing events never infer ownership from a
   session, channel, chat, timing, or process-wide ID uniqueness.
2. Durable state remains authoritative. Capture observes committed facts and
   cannot acknowledge, repair, or advance an interaction.
3. Ordering is assigned at the authority boundary. Startup reconciliation and
   live observation share one ordered stream contract.
4. Persistence is a service, not projector logic. Queueing, retry, retention,
   atomic writes, and loss accounting have one owner.
5. Production and replay share a declarative protocol contract, not registry
   implementation or mutable storage.
6. Missing history is typed evidence. Evaluators do not infer completeness from
   diagnostic names.
7. Trace capture must never block agent execution, but every dropped critical
   fact must make incompleteness visible.

## 1. Mandatory Trace Scope

Introduce a runtime-only identity value:

```go
type TraceScope struct {
    Workspace string
    TurnID    string
}
```

Both fields are required and normalized at turn creation. `Workspace` is the
canonical workspace root used internally for routing and storage; persisted
trace metadata stores only the existing safe workspace identity or hash.
Runtime event and hook scopes embed this value object rather than maintaining
independent workspace and turn fields.

Every turn-owned runtime event carries `TraceScope`. Every turn-owned outbound
text or media message carries the same scope through the bus and channel
delivery boundary. A physical outbound message produced from several steering
turns carries one workspace and a deduplicated ordered list of turn IDs. Mixing
turns from different workspaces in one outbound is invalid.

Correlation and settlement are separate. `TraceScopes` state which turns an
outbound belongs to; `TraceSettlement` states that a terminal channel outcome
may settle those turns. The channel manager publishes one terminal outcome for
one logical outbound, even when transport splits it into chunks. A preliminary
direct or media attempt that has a fallback uses success-only outcome
publication: success and ambiguous failure are terminal, while a failure proven
not sent is left to the fallback. A settlement marker without a complete scope
is cleared during normalization.

Async admission has no caller available to perform that fallback. Once a bus
outbound is accepted by the channel delivery owner, worker cancellation,
shutdown, or rejection is therefore a terminal failed outcome even when no
remote send began. The failure settles capture with explicit loss evidence; a
capture timeout is not a delivery-recovery mechanism.

Internal channels have no external delivery owner. Their ordinary traffic
remains outside this protocol, but an internal outbound carrying
`TraceSettlement` is rejected with a terminal failure instead of being silently
dropped. Media adapters may omit remote message IDs on success, so the manager
does not infer failure from an empty ID list; adapters must instead return an
error for every requested part they did not send and preserve IDs from any
parts sent before that error.

Capture indexes active turns by `(workspace, turn_id)`. There is no fallback by
session, channel, chat, route, or unique turn ID. An event without a complete
scope is operationally observable but cannot mutate a turn trace. This is an
intentional fail-closed break from the current heuristic channel correlation.

A settling terminal delivery may arrive before `agent.turn.end`, for example
when a response-handling tool delivers final media. Capture retains that fact
on the exact active scope and persists only after the turn outcome arrives. A
single aggregated delivery event may settle several scoped steering turns, but
never turns from different workspaces.

Acceptance tests must create identical turn and session IDs in two workspaces,
exercise concurrent runtime and delivery events, and prove that no evidence or
settlement crosses the workspace boundary.

### Workspace-Scoped Runtime Sessions

Exact trace ownership depends on the runtime authorities using the same
workspace boundary before a turn exists. Root-turn claims, route claims,
steering queues, continuation dequeue, pending stop markers, and pending skill
selection therefore use a comparable `(workspace, session_key)` value rather
than a process-wide session string. Subturn lookup uses a distinct
`(workspace, turn_id)` key so
the active-turn map never mixes session and turn identifiers in one string
namespace.

Every routed ingress and continuation supplies the workspace resolved from its
agent. Public steering and continuation entry points require workspace and
session explicitly. They do not select an arbitrary active turn or fall back
to an unowned process-wide queue. Session-only inspection helpers fail closed
when the same session is active in more than one workspace.

Acceptance tests must claim, steer, continue, stop, and release identical
session and route IDs concurrently in separate workspaces without blocking or
consuming the other workspace's state.

### Exact Runtime Session Ownership

Session keys do not identify an agent or workspace. Outbound delivery and
context management must consume the owner already selected by routing or turn
setup; they must not scan session stores to rediscover an owner from a bare
session key. Context requests carry the exact `AgentInstance`, while outbound
publication carries workspace, agent ID, and session key and resolves them as
one fail-closed scope. Message-tool suppression examines only that owner.

The same ownership applies to legacy compaction, Seahorse reconciliation,
history clearing, background reconciliation, and interaction-answer ingest.
Session-only runtime inspection remains diagnostic and returns no result when
ambiguous. Acceptance tests use identical session keys in different workspaces
and prove that outbound suppression and canonical context reads cannot cross
owners.

## 2. Ordered Registry Observation

The interaction registry exposes one atomic subscribe-and-snapshot operation:

```go
type Event struct {
    CommitSequence uint64
    // domain transition fields
}

type ObservationSnapshot struct {
    Through uint64
    Records []Record
    Events  []Event
}

func (r *Registry) SubscribeSnapshot(Observer) (ObservationSnapshot, func())
```

While holding the registry lock, subscription installation and snapshot
creation share one boundary. The snapshot contains all committed observations
through `Through`; the subscriber receives only later sequences. Observers are
never called while the registry lock is held. A single ordered dispatcher
preserves commit order and permits callbacks to call registry read methods.

Every mutating operation persists the record and event before publishing its
observation. The persisted global commit sequence is distinct from each
interaction's local lifecycle sequence. Persistence failure publishes nothing.
Reload reconstructs the next commit sequence from durable data. Tests cover a commit at the subscription
boundary, callback re-entry, unsubscribe races, persistence failure, and
restart ordering.

This contract is general registry infrastructure. It does not mention traces.

## 3. Durable Trace Writer

Extract persistence from `pkg/agent` into a package such as
`pkg/evalcapture`. Its input is an immutable finalized trace plus storage
policy; it does not know about turns, tasks, interactions, channels, or agent
configuration.

The writer owns:

- bounded nonblocking admission;
- separate accounting for ordinary and critical submissions;
- retry of transient atomic-store failures;
- graceful drain and a bounded final retry;
- retention and count pruning per workspace;
- owner-only, path-safe atomic files;
- metrics and typed operational events for every rejected, evicted, retried,
  permanently failed, or truncated submission.

There is no silent critical drop. If memory is exhausted by critical work, the
writer rejects the new submission with a typed reason that the caller records
as incomplete, and emits a bounded operational event. It does not create an
apparently complete malformed trace.

The writer does not reconstruct domain state after restart. Durable projectors
rebuild missing finalized traces from their authoritative registries.

## 4. Source-Specific Projectors

Replace the generic agent capture manager with small source owners:

- a turn projector subscribes to scoped runtime events and owns active turn
  buffers and bounded delivery settlement;
- a task projector consumes committed task observations and task snapshots;
- an interaction projector consumes the ordered interaction snapshot/stream
  and performs startup reconciliation;
- a coordinator owns configuration reload, projector lifetime, and the shared
  writer, but no domain correlation maps.

The interaction projector has one deterministic builder:

```go
BuildInteractionTrace(record Record, events []Event) (evaltrace.Trace, Evidence)
```

Both live terminalization and startup reconciliation call this function. A
terminal registry record is not considered captured until its trace has been
accepted by the writer. Reconciliation scans retained terminal records and
rebuilds a missing, incomplete, or stale trace from durable history. If history
was pruned, the builder emits an explicitly incomplete trace rather than
inventing transitions.

Projectors may add typed links between traces. They do not copy one domain's
state machine into another projector or append interaction transitions directly
to a turn buffer through session heuristics.

## 5. Shared Interaction Protocol

Extract a pure, storage-free protocol package from the interaction registry.
It owns the closed vocabulary and declarative legality of:

- statuses and terminal statuses;
- event types;
- allowed `(event, from, to)` transitions;
- required kind, outcome, code, delivery state, and revision constraints;
- whether a violation is conclusive from one event or requires prior history.

The production registry validates a proposed committed event against this
contract before persistence. Replay folds captured events through the same
contract. Replay must not call registry mutation methods, load registry files,
or reuse registry locks and persistence.

Protocol validation returns typed evidence:

```go
type EvidenceRequirement string

const (
    EvidenceConclusive              EvidenceRequirement = "conclusive"
    EvidenceRequiresCompleteHistory EvidenceRequirement = "requires_complete_history"
)
```

Each diagnostic carries this classification at creation. Evaluators can report
a conclusive violation from a truncated trace while returning `not_evaluable`
for findings that require missing history. No later diagnostic-code allowlist
is permitted.

A table-driven contract matrix is the protocol specification. Every allowed
and rejected transition appears once in that matrix and is exercised through
both the registry adapter and replay adapter.

## 6. Interaction Replay And Evaluation

After the foundations merge, extend the versioned trace schema with metadata-
only interaction transitions. Interaction records never contain question text,
answers, approval summaries, sender identity, routes, tool arguments, or
secrets, even when general capture uses full content.

The deterministic evaluator checks:

- legal ordered lifecycle transitions;
- exactly one successful prompt delivery before waiting;
- authorized answer claim evidence;
- approval expiry and at-most-once consumption;
- final delivery and terminal outcome consistency;
- no transition after terminal state;
- trace identity, final revision, and terminal sequence consistency;
- explicit handling of incomplete retained history.

The evaluator demonstrates runtime behavior; it does not claim that a trace is
the state authority. Historical fixtures are sanitized and versioned. Scenario
tests use isolated workspaces and never execute trace-provided tools.

## Delivery Plan

Each item is a separate focused PR based on the merged predecessor:

1. **Identity value:** define the canonical mandatory `TraceScope` value.
2. **Passive transport:** carry validated scope lists through the bus and
   channel event payloads without changing capture behavior.
3. **Delivery protocol:** separate correlation from settlement, publish one
   terminal outcome per logical outbound, and support success-only preliminary
   attempts before fallback.
4. **Runtime session identity:** key root-turn and route claims, steering,
   continuation, pending stop state, and pending skill selection by workspace
   plus session; keep subturn identity in a distinct key namespace.
5. **Exact runtime ownership:** pass the routed owner through outbound and
   context-manager boundaries; remove session-only owner rediscovery.
6. **Exact adoption:** propagate scopes from runtime producers and outbound
   aggregation, remove heuristic turn-delivery correlation, and add workspace-
   collision tests.
7. **Ordered observation:** add the registry subscribe-and-snapshot contract
   with ordering, re-entry, persistence-failure, and restart tests.
8. **Durable writer:** extract queueing, retry, retention, atomic persistence,
   and explicit loss accounting from the agent capture manager.
9. **Projectors:** split turn/task capture and add the dedicated interaction
   projector with deterministic live/startup construction.
10. **Protocol contract:** extract declarative interaction transitions and use
   them from both registry validation and replay reduction.
11. **Evaluation:** add the interaction trace schema, evaluator, fixtures, CLI
   reporting, operator docs, and only the spike tests relevant to these final
   boundaries.

No PR should recreate the full spike as an intermediate state. A PR must leave
main useful, internally consistent, and fully tested on its own. Dependent PRs
wait for merge unless explicitly published as a short-lived stack.

## Spike Migration

Keep from PR #272 as test evidence or behavior requirements:

- atomic subscribe/snapshot ordering and restart reconciliation;
- terminal interaction backfill and retention boundaries;
- critical persistence retry and explicit truncation accounting;
- exact workspace collision cases;
- approval mutation, expiry, consumption, and crash-window fixtures;
- explicit diagnostic evidence classification.

Do not port directly:

- the expanded all-domain `traceCaptureManager`;
- session/channel fallback correlation;
- replay's manually duplicated interaction transition switch;
- diagnostic-code allowlists for incomplete traces;
- unpublished compatibility adapters or trace migrations.

## Completion Criteria

The redesign is complete only when:

- every trace-affecting turn event has mandatory workspace-scoped identity;
- registry startup and live observation form one gap-free ordered stream;
- trace persistence failures and critical drops are observable and tested;
- interaction live capture and startup reconciliation use the same projector;
- production and replay pass one lifecycle contract matrix;
- deterministic fixtures reproduce the spike's confirmed historical failures;
- focused race tests, full Go tests, lint, and CI pass for every PR;
- documentation describes merged behavior rather than planned behavior.
