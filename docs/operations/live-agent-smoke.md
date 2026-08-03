# Live Gateway Agent Smoke Test

`mintclaw agent live` sends one bounded request through the authenticated
MintClaw WebSocket channel of an already running gateway. It uses the gateway's
existing `AgentLoop`, configured tools, connected nodes, approval policy,
durable invocation records, and diagnostic tracing. It does not create the
separate local runtime used by the traditional `mintclaw agent` command.

## Requirements

- run the command on the gateway host;
- select the gateway profile with its `config.json`;
- enable the `mintclaw` channel and configure its token; and
- keep the configured gateway probe address on loopback.

The bearer token is read from configuration and is never printed. The client
creates an isolated protocol session by default. `--session` may be supplied
when a deliberately stable session is needed.

## Transport and invocation model

`mintclaw agent live` is a one-shot CLI client for the same authenticated
MintClaw WebSocket transport that interactive clients use. It is not a second
transport and it does not start another agent runtime. The difference is the
client lifecycle:

- an interactive client normally keeps one WebSocket connection open and
  exchanges multiple messages;
- `agent live` opens a connection, sends one request, waits for its correlated
  terminal outcome, and exits; and
- each later `agent live` command opens a new connection, even when it resumes
  the same durable agent session.

This bounded lifecycle makes the command suitable for scripts, smoke tests,
and operator automation while preserving the gateway's normal routing,
approval, tool, and trace behavior.

### `agent --stateless` compared with `agent live`

These commands solve different problems:

| Command | Agent runtime | Conversation state | Live gateway-owned resources |
| --- | --- | --- | --- |
| `mintclaw agent --stateless -m '...'` | Creates a separate local `AgentLoop` in the CLI process | Does not load or save conversation history | Cannot use connected node sessions and other runtime state owned by the already running gateway |
| `mintclaw agent live --message '...'` | Sends the request to the already running gateway's `AgentLoop` | Uses a new isolated session by default, or resumes the durable session selected by `--session` | Uses the gateway's live tools, node registry, approval state, delivery ownership, and diagnostic tracing |

`--stateless` controls conversation persistence for a direct local turn; it
does not mean "send an isolated request to the live gateway." Conversely,
`agent live` controls where the turn runs, and currently has no `--stateless`
flag. Omit `--session` when a fresh live conversation is sufficient, but note
that the resulting turn is still processed and recorded by the live gateway's
normal durable runtime.

## Continue an existing session

Without `--session`, every invocation generates a new `live-smoke-<UUID>`
protocol session and therefore starts with an isolated conversation context.
Pass the same explicit session ID to sequential invocations when the next
message should be appended to the same durable agent history:

```sh
mintclaw agent live \
  --config /home/server/.mintclaw/main/config.json \
  --session vpn-maintenance-2026-08 \
  --message 'Check the current state of the vpn node.' \
  --timeout 3m \
  --json

mintclaw agent live \
  --config /home/server/.mintclaw/main/config.json \
  --session vpn-maintenance-2026-08 \
  --message 'Now report free disk space on the same node.' \
  --timeout 3m \
  --json
```

The two commands use separate WebSocket connections, but the stable session
ID maps them to the same routed conversation, so the second turn can use the
first turn's context. A session ID is a routing identifier, not an
authentication credential; authentication still comes from the configured
channel token.

Use the generated default session for independent smoke tests. Use a stable,
purpose-specific ID for a deliberate multi-turn operator workflow, and submit
turns sequentially when their order matters rather than running concurrent
commands against the same session.

## Basic live-agent check

```sh
mintclaw agent live \
  --config /home/server/.mintclaw/main/config.json \
  --message 'Reply with exactly MINTCLAW_LIVE_OK.' \
  --timeout 2m \
  --json
```

A successful result has `outcome: "success"`, the response marker, and the
correlated actor, agent, session, request, workspace, and turn identities.

## Live node invocation check

Use an explicit target, command, profile, and working scope in the prompt so
the normal node grants and approval policy remain authoritative:

```sh
mintclaw agent live \
  --config /home/server/.mintclaw/main/config.json \
  --message 'On vpn use shell.exec.v1 with profile root and working scope root to run id && hostname. Do not use ordinary system exec. Return UID and hostname.' \
  --timeout 3m \
  --json
```

The command sends the request once. A timeout or disconnect never causes an
automatic replay. If the target requires operator approval, the result is
`approval_required` and contains the bounded approval prompt; the command does
not approve it. JSON also includes `interaction_id` and
`interaction_short_id`, which identify the durable approval record.

Stable outcomes are:

- `success` — a correlated final response arrived;
- `approval_required` — normal policy suspended the turn for an operator;
- `timeout` — the local wait deadline elapsed;
- `unavailable` — the configured channel or gateway is unavailable;
- `authentication_failed` — gateway authentication rejected the client;
- `canceled` — the local caller canceled its wait;
- `disconnected` — the WebSocket closed before a terminal result;
- `protocol_error` — the gateway rejected the request frame;
- `output_limit` — the response exceeded the one-MiB client bound; and
- `internal_error` — local validation or configuration failed.

Input is limited to 64 KiB. JSON output never includes the channel token or raw
configuration. After an uncertain timeout or disconnect, inspect the
correlated turn trace and durable node invocation status instead of repeating a
possibly mutating request.

## Rollback

The command does not change gateway configuration or node policy. To roll back
the feature, restore the previous MintClaw binary from the normal timestamped
deployment backup and restart only the gateway service. Existing node
registrations, approval records, and companion processes need no migration.
