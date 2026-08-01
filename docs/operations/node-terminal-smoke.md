# Node terminal client and lifecycle smoke test

MintClaw provides two operator commands for the authenticated attached-PTY
transport:

- `mintclaw nodes terminal open` is a real interactive terminal client;
- `mintclaw nodes terminal smoke` is a bounded, non-interactive deployment
  check with stable JSON output for automation.

Both commands use the deterministic authenticated terminal-open endpoint. An
LLM is not involved in opening, attaching, or closing the PTY.

## Prerequisites

The gateway configuration must have:

- `nodes.enabled: true` and `nodes.terminal_enabled: true`;
- an enabled MintClaw channel with its token stored through the normal secure
  configuration path;
- a paired, connected target visible to the `main` agent;
- approved `shell.exec.v1` authority whose model contract exposes the selected
  profile and working scope; and
- a companion authority broker configured for that profile.

Run these commands on the gateway host. The CLI refuses a non-loopback gateway
address so the MintClaw channel token never crosses a plaintext network hop.
Raw PTY input and output remain on the authenticated operator WebSocket and are
not copied into diagnostic traces or passive lifecycle metadata.

## Interactive terminal

```bash
mintclaw nodes terminal open \
  --config /path/to/config.json \
  --target vpn-smoke \
  --profile owner-test \
  --working-scope workspace
```

The client prints `Opening`, `opened; attaching`, and `Attached` status before
placing the local terminal in raw mode. Once attached, it behaves like a normal
remote terminal:

- ordinary bytes, Enter, arrow/function keys, Ctrl+C, and Ctrl+D are forwarded
  to the remote PTY without line buffering;
- the initial local size and subsequent `SIGWINCH` resizes are propagated;
- remote bytes are written to local stdout in cursor order;
- press `Ctrl+]` to request a confirmed remote close and disconnect locally.

The local TTY is restored after normal exit, remote close, local escape,
signal, transport failure, or protocol error. A successful local return means
the companion confirmed remote process-tree termination. An unconfirmed
disconnect returns a non-zero status and an explicit error.

## Automated smoke

```bash
mintclaw nodes terminal smoke \
  --config /path/to/config.json \
  --target vpn-smoke \
  --profile owner-test \
  --working-scope workspace
```

Human-readable mode writes progress to stderr as it opens, attaches, checks
resize/input/output, and confirms termination. For automation, stdout remains
one stable JSON object and progress is suppressed:

```bash
mintclaw nodes terminal smoke \
  --config /path/to/config.json \
  --target vpn-smoke \
  --profile owner-test \
  --working-scope workspace \
  --timeout 30s \
  --json
```

Example success:

```json
{
  "target": "vpn-smoke",
  "profile": "owner-test",
  "terminal_id": "terminal_<opaque>",
  "uid": 1001,
  "rows": 31,
  "columns": 100,
  "marker": "MINTCLAW_PTY_OK",
  "state": "closed",
  "close_reason": "close"
}
```

The smoke does not create, modify, or delete files. It resizes the PTY,
disables echo for its fixture, reads the process UID and PTY size, checks the
`MINTCLAW_PTY_OK` marker, requests an ordered close, and waits for confirmed
process-tree termination. The marker is assembled remotely, so echoed input
cannot produce a false positive.

Common deterministic open errors are:

- `UNAUTHORIZED` or `ORIGIN_DENIED`: operator authentication is invalid;
- `TERMINAL_DENIED`: target, pairing/catalog, profile, working scope, or
  durable command authority does not permit the request;
- `TARGET_UNAVAILABLE`: the target is not currently connected;
- `TERMINAL_UNAVAILABLE`: the configured terminal runtime is not mounted.
