# AgentLoop Runtime Host

`pkg/agent.AgentLoop` is the runtime host for an agent workspace. It owns shared
services and public APIs, but it should not contain turn-step execution logic.
One logical agent turn still starts at `runAgentLoop`; detailed turn execution is
delegated to `Pipeline`.

## Responsibility Split

| Layer | Owns | Should Not Own |
| --- | --- | --- |
| `AgentLoop` | runtime lifecycle, service wiring, registry/config access, public APIs, active-turn registry, channel/bus integration | LLM/tool iteration mechanics |
| `inboundTurnCoordinator` | inbound scheduling, same-session serialization, busy-session steering enqueue, worker goroutine lifecycle, ack/release decisions | route normalization or LLM/tool execution |
| `runtimeSessionClaim` | atomic session placeholder claim/release semantics shared by inbound workers and recovery | turn execution, routing, delivery |
| `inboundMessageTurn` | normalized inbound route/session/model/dispatch envelope for one message | command handling or pipeline execution |
| `turnRuntimeHost` | narrow host callbacks needed by in-turn execution: runtime events, abort, steering ack/release, sensitive-data filtering, final reply rendering | LLM/tool iteration mechanics, active-turn registration, or pipeline phase policy |
| `Pipeline` | context assembly, LLM calls, tool loops, steering injection during a turn, finalization | inbound bus scheduling or session claiming |

## Inbound Flow

1. `AgentLoop.Run` initializes runtime services and reads inbound/observed bus
   channels.
2. Inbound messages are delegated to `inboundTurnCoordinator`.
3. The coordinator resolves the message's steering target. Non-routable/system
   messages stay synchronous.
4. For routable messages, the coordinator claims the session through
   `runtimeSessionClaim`.
5. If the session is already active, the message becomes queued steering for
   that session, except `/stop`, which is handled immediately.
6. If the claim succeeds, a worker goroutine acquires the worker semaphore,
   handles pending stop state, runs `runTurnWithSteering`, and then acks or
   releases the inbound spool entry.
7. `runTurnWithSteering` calls `processMessage`, then drains queued steering
   continuations for the same session before publishing the final response.

This keeps same-session messages serialized while allowing different sessions
to run concurrently up to the configured worker limit.

## Turn Launch Boundary

`processMessage` is the last host-side step before turn execution. It prepares
and logs the inbound message, handles system messages, builds an
`inboundMessageTurn`, processes commands, applies pending skill overrides, and
then calls:

```go
al.runAgentLoop(ctx, turn.Agent, opts)
```

`runAgentLoop` remains the boundary for one logical agent turn. `AgentLoop`
then wraps the turn with runtime lifecycle concerns in `runTurn` and delegates
turn progression to:

```go
pipeline.runTurnLoop(ctx, turnCtx, ts, host)
```

Code below that boundary should be considered turn execution and belongs in
`Pipeline` or pipeline-owned helpers.

Inside `runTurn`, host-owned callbacks are exposed to the turn loop through
`turnRuntimeHost`. This keeps PR-sized refactors honest: `AgentLoop` still owns
runtime lifecycle and observability, while the loop can move toward
pipeline-owned code without carrying a full `AgentLoop` dependency with it.

## Session Claiming

`runtimeSessionClaim` is the shared session ownership primitive. A claim stores
a setup placeholder in `AgentLoop.activeTurnStates` with `LoadOrStore`, so two
messages for the same session cannot both launch workers during setup.

Claims release only the exact placeholder they installed. This is intentional:
if a real `turnState` or a newer placeholder replaces the original value, stale
cleanup must not delete it.

Current claim users:

- inbound workers, including context-cancel cleanup while waiting for the worker
  semaphore
- pending-stop cleanup before draining queued steering
- unanswered-session recovery

## Recovery

Unanswered-session recovery scans durable session history for sessions ending in
an unanswered user message. When recovery launches a turn, it now claims the
session through the same `runtimeSessionClaim` primitive used by inbound
workers.

Recovery still builds its own `processOptions` because the unanswered user
message is already durable history and must not be appended again as a fresh
user message.

## Intentional Coupling

Some coupling remains by design:

- `AgentLoop` still owns `activeTurnStates` because `/stop`, `/subagents`,
  hard abort, recovery, and runtime observability all need the same live turn
  registry.
- `processMessage` still handles commands before `runAgentLoop` because commands
  need routed session/model context but should not always start an LLM turn.
- `runTurnWithSteering` still publishes the final joined response after
  continuation drains. This preserves current delivery behavior while keeping
  in-turn steering injection inside `Pipeline`.
- `inboundMessageTurn` still lives in `pkg/agent` rather than a new package
  because it depends on agent registry, routing, session allocation, and model
  binding internals. Moving it would create an artificial package boundary.

Future refactors should preserve this rule: host/runtime code may decide when a
turn starts and how it is observed or delivered, but the turn's internal LLM and
tool progression belongs to `Pipeline`.
