# Stateless direct turns

Direct agent invocations are stateful by default. They resolve the configured
route, assemble that route's conversation history, and persist the completed
turn for future invocations.

Use `picoclaw agent --stateless` for self-contained automation jobs whose
durable state lives outside conversational memory:

```bash
picoclaw agent --stateless --session review-pr-251 \
  -m "Process the queued review job and wait for writeback."
```

A stateless direct turn still receives the workspace and system instructions,
current input, configured tools, and every tool call and result produced during
that turn. It does not load prior messages or summaries and does not persist its
user, assistant, reasoning, or tool messages after completion. Routing, model
selection, hooks, runtime events, token metrics, and external durable stores are
unchanged.

This mode is appropriate for queue workers when each payload is complete and
the queue or task registry is the recovery source of truth. Human chat channels
should remain stateful unless their product semantics explicitly require
independent turns.

## Migration

After changing an automation worker to pass `--stateless`, old conversation
files are no longer read or extended. They may be retained for audit or removed
once the operator has identified the exact automation session. Do not delete
task registries, queue databases, evaluation traces, or human-channel sessions
as part of that cleanup.
