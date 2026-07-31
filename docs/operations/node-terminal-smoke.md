# Node terminal lifecycle smoke test

Use `mintclaw nodes terminal smoke` to verify the complete attached PTY path on
a running gateway:

```text
authenticated MintClaw session
  -> nodes_terminal discover/open
  -> operator WebSocket attach
  -> resize
  -> ordered input and bounded output
  -> confirmed close
```

The command is intended for deployment verification and manual operator
testing. It is not a general interactive terminal client.

## Prerequisites

The active gateway configuration must have:

- `nodes.enabled: true`;
- `nodes.terminal_enabled: true`;
- an enabled MintClaw channel with its token stored through the normal secure
  configuration path;
- a visible paired target whose approved `shell.exec.v1` contract exposes the
  selected owner profile and working scope; and
- an approval policy that can complete terminal-open authorization during the
  command's bounded wait.

The companion and authority broker must already be configured for that profile.
Fresh installations and product profiles remain terminal-disabled by default.

Run the smoke command on the gateway host. It refuses non-loopback gateway
addresses so the channel token cannot cross a plaintext WebSocket, connects
only to the gateway address in the selected MintClaw configuration, and never
prints the token:

```bash
mintclaw nodes terminal smoke \
  --config /path/to/config.json \
  --target vpn-smoke \
  --profile owner-test \
  --working-scope workspace
```

For automation, request stable JSON and use the process exit status:

```bash
mintclaw nodes terminal smoke \
  --config /path/to/config.json \
  --target vpn-smoke \
  --profile owner-test \
  --working-scope workspace \
  --json
```

A successful result contains:

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

The exact UID depends on the selected out-of-band profile.

## What the smoke changes

The smoke does not create, modify, or delete files on the target node. The
gateway still writes its normal session state, redacted diagnostic trace, and
bounded lifecycle audit metadata. After attachment the smoke:

1. resizes the PTY to the requested bounded dimensions;
2. disables terminal echo for the fixture;
3. obtains the process UID and terminal size;
4. prints the deterministic `MINTCLAW_PTY_OK` marker; and
5. requests an ordered close and waits for confirmed process-tree termination.

The literal success marker is not present in the input frame, so an echoed
command cannot create a false positive. Raw PTY bytes remain on the authenticated
operator WebSocket and are not copied into diagnostic traces or passive audit
events.

## Failure interpretation

- An error mentioning `terminal_enabled` means the capability is still
  disabled in the selected gateway configuration.
- A MintClaw channel or token error means authenticated operator transport is
  unavailable.
- A timeout before terminal attachment commonly means the agent could not
  complete `nodes_terminal` discovery/open, the target was offline, or runtime
  approval was still required.
- An attachment error means the operator route was not mounted, the terminal
  missed its attach deadline, or the session identity did not match.
- A size or marker error means PTY input/output or resize did not complete as
  requested.
- A close error means process-tree termination was not confirmed and must be
  investigated before retrying privileged tests.

After a failed deployed smoke, inspect the newest redacted diagnostic trace and
bounded gateway, companion, and broker journals. Do not paste raw terminal
frames or secure configuration into incident reports.

## Ordinary terminal clients

SSH, Telnet, and generic WebSocket clients cannot attach directly. The
companion opens no inbound terminal port, and the operator transport requires
MintClaw authentication, exact session ownership, ordered control sequences,
base64 byte frames, resize semantics, and confirmed close handling.

A future `mintclaw nodes terminal` interactive client can adapt the local
console's raw mode, input, resize, and signals to this protocol. A browser
terminal similarly requires the authenticated Launcher proxy and a terminal
emulator boundary. Those interactive clients are separate from this bounded
smoke command.
