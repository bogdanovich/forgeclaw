# Node Companion

`picoclaw-node` is the slim first-party process that connects a Linux or macOS
machine to a ForgeClaw gateway. It does not include models, agents, channels,
sessions, MCP hosting, or workspace memory.

The current implementation performs outbound admission only: it creates a
durable device identity, authenticates it with a signed challenge over WSS, and
keeps retrying while the gateway records the node as `pending_pairing`. Operator
approval and command execution are added in later node milestones; no executable
command surface is exposed by this stage.

## Build

```bash
make build-node
```

The resulting binary is `build/picoclaw-node`.

## Configure

Create `~/.picoclaw-node/config.json`:

```json
{
  "gateway_url": "wss://forgeclaw.example.com/nodes/v1/ws",
  "state_dir": "~/.picoclaw-node",
  "tls": {
    "ca_file": "/etc/ssl/private/forgeclaw-ca.pem"
  },
  "reconnect": {
    "min_delay_seconds": 1,
    "max_delay_seconds": 30,
    "pending_delay_seconds": 30
  }
}
```

Normal public certificates use the operating-system trust store and do not need
`ca_file`. A private CA can be supplied as shown. An exact out-of-band
certificate pin can be used instead:

```json
{
  "tls": {
    "certificate_sha256": "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
  }
}
```

There is no `insecure_skip_verify` option. Plain `ws://` is accepted only for a
loopback endpoint when `allow_loopback_plaintext` is explicitly true.

## Run

```bash
picoclaw-node run --config ~/.picoclaw-node/config.json
```

The first successful handshake creates
`<state_dir>/identity.json` with owner-only permissions. Back up that file as a
secret: replacing it creates a different node identity.

## Multiple Workspaces

The MVP uses one gateway binding per process. Run named service instances from
the same binary with distinct config and state directories:

```text
~/.picoclaw-node/main/config.json
~/.picoclaw-node/main/state/
~/.picoclaw-node/nutrition/config.json
~/.picoclaw-node/nutrition/state/
```

Each instance is paired and authorized independently. Do not point multiple
instances at the same state directory. A future multi-gateway supervisor may
share a capability runtime with explicit resource scheduling, but gateway trust,
policy, tokens, and invocation ledgers will remain isolated per binding.

## Pairing Administration

After an unknown companion connects, inspect and approve its durable identity
from the gateway host:

```bash
picoclaw nodes list --state pending_pairing
picoclaw nodes describe node_<fingerprint>
picoclaw nodes approve node_<fingerprint> \
  --alias vpn-box \
  --display-name "VPN box" \
  --allow-command node.info.v1
```

Approval grants no commands unless each advertised command is named explicitly
with `--allow-command`. Deny an untrusted pending identity or revoke a paired
one with a recorded reason:

```bash
picoclaw nodes deny node_<fingerprint> --reason "unknown device"
picoclaw nodes revoke vpn-box --reason "device retired"
```

All read and mutation commands accept `--json`. The CLI prints only a public-key
fingerprint, never the stored raw public key.
