# Durable Human Interaction

ForgeClaw can pause an agent turn or durable background task, ask the authorized
user for input, release runtime resources while waiting, and resume the exact
tool call after the answer arrives. Pending interactions survive process
restarts.

## Default Behavior

`request_user_input` is enabled by default:

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

The default wait is one hour. A model may request a timeout from 60 seconds up
to the configured maximum of 24 hours. Terminal interaction records are retained
for seven days by default and then become eligible for pruning.

Set `enabled` to `false` to prevent new model-requested questions. Existing
records remain available for recovery, timeout, cancellation, and retention
cleanup.

## Asking and Answering

The model can ask one to three bounded questions. Each question may accept free
text or offer two to three choices. ForgeClaw sends the prompt to the same routed
conversation and accepts an answer only from the recorded channel, account,
chat, topic, session, and sender.

The model owns every user-facing question, header, option label, and option
description. It writes them in the language and general style of the
conversation and includes enough context for the prompt to be answerable
without runtime-added prose. ForgeClaw renders that content directly. It does
not detect a locale, translate text, or make another model call after
suspension.

For one question, the prompt contains the model-authored content followed by a
compact command fallback:

```text
Which environment should be used?

• development — Local development.
• staging — Pre-production validation.
• production — The live environment.

`/answer 16131195 …`
```

A normal message reply remains sufficient. For several questions, ForgeClaw
adds question numbers and stable question IDs, then appends a keyed answer
template:

```text
1. `region`
Which region should be used?

2. `mode`
Which deployment mode should be used?

`/answer 16131195`
`region: …`
`mode: …`
```

The short interaction ID, question IDs, option layout, and `/answer` syntax are
runtime-owned machine structure. Replies may contain the keyed lines directly
or prefix them with the shown `/answer <short-id>` command.

Commands such as `/new`, `/reset`, and `/clear` cancel the pending interaction
before changing the session. `/stop` durably cancels it and completes the
suspended tool call with a cancellation result.

## Background Tasks

Spawned and delegated durable tasks move to `waiting_for_input` while a question
or approval is pending. `task_status` and `spawn_status` expose the safe short
interaction ID and bounded summary. Waiting does not publish task completion or
consume a completion ID.

After an authorized answer, the task returns to `running`, resumes once, and
uses the normal completion and delivery path. Restart reconciliation preserves
waiting tasks instead of marking them lost.

## Human Approval

Human approval is opt-in. A trusted tool approval hook can return:

```json
{
  "require_human": true,
  "action_summary": "Delete the production cache namespace",
  "timeout_seconds": 3600
}
```

`action_summary` is trusted presentation data and must be action-specific,
bounded, and free of secrets. ForgeClaw renders the exact runtime-owned tool
name and trusted summary without model-authored approval presentation or
generic tool arguments:

```text
filesystem_delete
Delete the production cache namespace

`/answer a1b2c3d4 allow_once`
`/answer a1b2c3d4 deny`
```

Direct `allow_once` and `deny` replies also work. The runtime binds approval to
the tool call and canonical argument hash, checks expiry, revalidates current
policy, and consumes the grant before execution. The model cannot create its
own approval authority or select the approving user.

## Restart and Delivery Semantics

Interaction state is stored at:

```text
<workspace>/state/interaction_registry.json
```

Workspace discovery records used during restart recovery are stored under:

```text
<picoclaw-home>/state/interaction_workspaces/
```

On startup ForgeClaw loads pending records, expires overdue requests, restores
task state, and resumes already-claimed answers. Duplicate or concurrent answers
produce one accepted claim and one continuation.

Remote chat APIs generally do not provide exactly-once publication. ForgeClaw
therefore does not resend a prompt or final response after an ambiguous send
window, which avoids duplicate user-visible delivery at the cost of reporting a
delivery-unknown failure.

## Debugging

Startup logs include `Loaded human interaction registry` with workspace,
record, nonterminal, retention, and load-error fields. Runtime lifecycle events
cover creation, delivery, waiting, answer claim, resumption, terminal outcome,
and cancellation without exposing raw answers or tool arguments.

Optional trace capture is debugging evidence only. It is not required for
interaction correctness, may be incomplete, and must never block interaction
progress, task reuse, pruning, delivery, or shutdown.

## Limits

- Only one unresolved interaction is allowed per canonical session.
- The runtime does not keep an LLM request or goroutine open while waiting.
- Timeout never chooses an answer automatically.
- Persistent approval allowlists and approve-for-session grants are unsupported.
- A channel must provide trusted sender and route metadata plus outbound
  delivery for durable interaction support.
