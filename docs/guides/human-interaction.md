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

For one question, reply normally or use:

```text
/answer 13ccbf94 eu
```

For several questions, reply with one line per question:

```text
deployment_mode: rolling
maintenance_window: 02:00 UTC
```

To correlate a multi-question reply explicitly, put the first answer on the
next line after the short interaction ID:

```text
/answer 13ccbf94
test_region: eu
test_mode: balanced
```

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
bounded, and free of secrets. ForgeClaw adds the runtime-owned tool name and
never generically renders tool arguments.

Reply `allow_once` to authorize that exact tool call once, or `deny`. The runtime
binds approval to the tool call and canonical argument hash, checks expiry,
revalidates current policy, and consumes the grant before execution. The model
cannot create its own approval authority or select the approving user.

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
