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
