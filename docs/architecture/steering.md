# Steering

Steering allows injecting messages into an already-running agent loop at safe
boundaries without waiting for the entire cycle to complete.

## How it works

When the model requests a sequence of tool calls, steering checks the queue
before the first dispatch and after each tool completes. Once steering is
pending, every remaining call is evaluated independently:

1. Read-only and explicitly non-cancellable calls finish.
2. Cancellable and unclassified calls are skipped before dispatch.
3. Every call receives exactly one source-ordered tool result, including a
   synthetic result for skipped calls.
4. Steering is injected before the next model request.

```
User ──► Steer("change approach")
                │
Agent Loop      ▼
  ├─ tool[0] ✔  (executed)
  ├─ [polling] → steering found!
  ├─ tool[1] ✔  (read-only, finished)
  ├─ tool[2] ✘  (cancellable, skipped)
  └─ new LLM turn with steering message
```

## Scoped queues

Steering is now isolated per resolved session scope, not stored in a single
global queue.

- The active turn writes and reads from its own scope key (usually the routed session key such as `agent:<agent_id>:...`)
- `Steer()` still works outside an active turn through a legacy fallback queue
- `Continue()` first dequeues messages for the requested session scope, then falls back to the legacy queue for backwards compatibility

This prevents a message arriving from another chat, DM peer, or routed agent
session from being injected into the wrong conversation.

## Configuration

In `config.json`, under `agents.defaults`:

```json
{
  "agents": {
    "defaults": {
      "steering_mode": "one-at-a-time"
    }
  }
}
```

### Modes

| Value | Behavior |
|-------|----------|
| `"one-at-a-time"` | **(default)** Dequeues only one message per polling cycle. If there are 3 messages in the queue, they are processed one at a time across 3 successive iterations. |
| `"all"` | Drains the entire queue in a single poll. All pending messages are injected into the context together. |

The environment variable `PICOCLAW_AGENTS_DEFAULTS_STEERING_MODE` can be used as an alternative.

## Go API

### Steer — Send a steering message

```go
err := agentLoop.Steer(providers.Message{
    Role:    "user",
    Content: "change direction, focus on X instead",
})
if err != nil {
    // Queue is full (MaxQueueSize=10) or not initialized
}
```

The message is enqueued in a thread-safe manner. Returns an error if the queue is full or not initialized. It will be picked up at the next polling point (after the current tool finishes).

### SteeringMode / SetSteeringMode

```go
// Read the current mode
mode := agentLoop.SteeringMode() // SteeringOneAtATime | SteeringAll

// Change it at runtime
agentLoop.SetSteeringMode(agent.SteeringAll)
```

### Continue — Resume an idle agent

When the agent is idle (it has finished processing and its last message was from the assistant), `Continue` checks if there are steering messages in the queue and uses them to start a new cycle:

```go
response, err := agentLoop.Continue(ctx, sessionKey, channel, chatID)
if err != nil {
    // Error (e.g. "no default agent available")
}
if response == "" {
    // No steering messages in queue, the agent stays idle
}
```

`Continue` internally uses `SkipInitialSteeringPoll: true` to avoid double-dequeuing the same messages (since it already extracted them and passes them directly as input).

`Continue` also resolves the target agent from the provided session key, so
agent-scoped sessions continue on the correct agent instead of always using
the default one.

## Polling points in the loop

Steering is checked at the following points in the agent cycle:

1. **At loop start** — before the first LLM call, to catch messages enqueued during setup
2. **Before the first tool dispatch** — catches steering that arrived while the model was responding
3. **After every tool completes** — including the first and last; remaining calls are classified individually
4. **After a direct LLM response** — if a new steering message arrived while the model was generating a non-tool response, the loop continues instead of returning a stale answer
5. **Right before the turn is finalized** — if steering arrived at the very end of the turn, the agent immediately starts a continuation turn instead of leaving the message orphaned in the queue

## Tool cancellation safety

Tools optionally implement `ToolSteeringSafety(args)`. The declared value controls
only calls that have not been dispatched:

| Classification | Pending-call decision |
|---|---|
| `read_only` | Finish; observing state is safe and may still help the next model turn |
| `non_cancellable` | Finish; the tool contract says skipping at this boundary is unsafe |
| `cancellable` | Skip before execution |
| `unknown` or missing | Skip before execution (fail closed) |

Once a tool has been dispatched, ForgeClaw lets it finish. Steering does not
cancel its context because cancellation after an external commit could leave
the runtime and external system in disagreement. Tool authors must classify
the externally visible operation, not merely its Go implementation. Call
arguments support mixed-operation tools: `exec` treats `list`, `poll`, and
`read` as read-only, while commands and session writes, keys, or kills are
cancellable.

All built-in tool types declare a policy. Remote MCP annotations are not used
as authorization to continue because MCP defines them as untrusted hints;
dynamic MCP calls deliberately remain `unknown` unless a trusted wrapper
implements a stronger local policy.

### Preventing unwanted side effects

Tools can have **irreversible side effects**. If the user says "no, wait" while the agent is mid-batch, executing the remaining tools means those side effects happen anyway:

| Tool batch | Steering message | With skip | Without skip |
|---|---|---|---|
| `[web_search, send_email]` | "don't send it" | Email **not** sent | Email sent, damage done |
| `[query_db, write_file, spawn_agent]` | "use another database" | Only the query runs | File written + subagent spawned, all wasted |
| `[search₁, search₂, search₃, write_file]` | user changes topic entirely | 1 search | 3 searches + file write, all irrelevant |

### Avoiding wasted time

Tools that take seconds (web fetches, API calls, database queries) would all run to completion before the agent sees the user's correction. In a batch of 3 tools each taking 3-4 seconds, that's 10+ seconds of work that will be discarded.

With skipping, the agent reacts as soon as the current tool finishes — typically within a few seconds instead of waiting for the entire batch.

### The LLM gets full context

Skipped tools receive an explicit synthetic result describing the cause and
the required reconciliation behavior, so the model knows which actions were
not performed. A queued message defers unsafe pending calls; it does not imply
that every earlier operation was canceled. At the next model boundary, the
model must reissue operations that are still requested, update operations
affected by a correction, and omit only operations the user canceled or
replaced.

### Trade-off: sequential execution

Skipping requires tools to run **sequentially** (the previous implementation ran them in parallel). This introduces latency when the LLM requests multiple independent tools in a single turn. In practice, most batches contain 1-2 tools, so the impact is minimal compared to the benefit of being able to stop unwanted actions.

## Skipped tool result format

When steering skips a call, it receives a `tool` result:

```
Content: "Deferred without execution because a newer user message arrived. Reconcile this operation after reading the newer message: reissue it if it is still requested, update it if the user corrected it, and omit it only if the user canceled or replaced it."
```

The structured `agent.tool.steering_decision` event separately records the
classification, decision, cause, and tool-call correlation. The result is
saved to the session and sent to the model, so it knows the action did not run.

## Full flow example

```
1. User: "search for info on X, write a file, and send me a message"

2. LLM responds with 3 tool calls: [web_search, write_file, message]

3. web_search is executed → result saved

4. [polling] → User called Steer("no, search for Y instead")

5. A pending read-only search finishes, while `write_file` and `message` are
   skipped with synthetic results.

6. Message "search for Y instead" injected into context

7. LLM receives the full updated context and responds accordingly
```

## Automatic bus drain

When the agent loop (`Run()`) starts, it reads inbound messages from a shared message bus. The routing logic determines how each message is handled:

1. **No active turn for the message's session** — the message is dispatched to a **worker goroutine** that processes the full turn (LLM calls, tool execution, steering drain)
2. **An active turn already exists for the same session** — the message is enqueued directly into that session's **steering queue** via `enqueueSteeringMessage`. No background drain goroutine is needed
3. **Non-routable message** (e.g. `system`) — processed synchronously in the main loop

This design enables **parallel processing of messages from different sessions** while keeping same-session messages strictly sequential. Key implications:

- Messages from different users/channels are processed **concurrently** (up to `max_parallel_turns`)
- Messages from the same session are **serialized** — subsequent messages go to the steering queue
- Users don't need to do anything special — their messages are automatically captured as steering when the agent is busy for their session
- Audio messages are transcribed within the worker that processes the turn, so the agent receives text
- `system` inbound messages are processed immediately and do not trigger steering

## Steering with media

Steering messages can include `Media` refs, just like normal inbound user
messages.

- The original `media://` refs are preserved in session history via `AddFullMessage`
- Before the next provider call, steering messages go through the normal media resolution pipeline
- Image refs are converted to data URLs for multimodal providers; non-image refs are resolved the same way as standard inbound media

This applies both to in-turn steering and to idle-session continuation through
`Continue()`.

## Notes

- Steering **does not interrupt** a tool that is currently executing. It waits for the current tool to finish, then classifies pending calls.
- Tool execution is currently sequential, so there is only one already-dispatched call at a time.
- Steering queues remain in memory. Accepted messages are persisted when they
  are injected, but a process restart before injection can still lose queued
  steering; cancellation decisions therefore do not claim restart durability.
- With `one-at-a-time` mode, if multiple messages are enqueued rapidly, they will be processed one per iteration. This gives the model the opportunity to react to each message individually.
- With `all` mode, all pending messages are combined into a single injection. Useful when you want the agent to receive all the context at once.
- The steering queue has a maximum capacity of 10 messages (`MaxQueueSize`). `Steer()` returns an error when the queue is full. In the bus drain path, the error is logged as a warning and the message is effectively dropped.
- Manual `Steer()` calls made outside an active turn still go to the legacy fallback queue, so older integrations keep working.
