# Node Companion

`picoclaw-node` is the slim first-party process that connects a Linux or macOS
machine to a ForgeClaw gateway. It does not include models, agents, channels,
sessions, MCP hosting, or workspace memory.

The companion creates a durable device identity, authenticates it with a signed
challenge over WSS, and keeps retrying while the gateway records an unknown
node as `pending_pairing`. After explicit operator approval, the gateway can
invoke only the commands allowed by both gateway policy and the node-local
policy. The current command surface includes `node.info.v1`,
`system.which.v1`, and optional synchronous `system.exec.v1`.

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

## Systemd Services On Linux

Install a named systemd user service after creating its configuration:

```bash
picoclaw-node install \
  --instance main \
  --config ~/.picoclaw-node/main/config.json
```

System installation requires an absolute configuration path and an explicit
unprivileged account:

```bash
sudo picoclaw-node install \
  --system \
  --instance vpn \
  --config /etc/forgeclaw/vpn-node.json \
  --service-user forgeclaw-node
```

Installation is create-only. It refuses an existing managed or administrator
unit rather than replacing it. The installer serializes work per service,
publishes the unit without replacement, starts it, and waits for a stable
`active` state. A failed install removes only the exact unit created by that
transaction. Reinstall, upgrade, and uninstall are separate lifecycle actions.
The per-service lock coordinates ForgeClaw lifecycle commands; administrators
must not edit or reload the same unit concurrently with a lifecycle transaction.

Remove a managed user service:

```bash
picoclaw-node uninstall --instance main
```

System-service removal is explicit:

```bash
sudo picoclaw-node uninstall --system --instance vpn
```

Uninstall is idempotent when both the managed unit and its systemd registration
are absent. It refuses administrator units, units resolved from another path,
drop-ins, unexpected enablement links, and unsupported service states. The
command disables and stops the verified service before removing it. If removal
cannot be committed, it restores the exact unit and its prior persistent
enablement and active state when safe.

Inspect a named systemd user service:

```bash
picoclaw-node status --instance main
```

System-service status is explicit:

```bash
sudo picoclaw-node status --system --instance vpn
```

Use `--json` for stable machine-readable output from install, uninstall, and
status. Status is read-only and
fail-closed: it refuses symlinked or unowned unit files, units resolved from a
different systemd search path, units modified by drop-ins, and stale systemd
state awaiting `daemon-reload`. `run` remains available on every supported
platform.

## Launchd Services On macOS

Install a named per-user LaunchAgent:

```bash
picoclaw-node install \
  --instance main \
  --config ~/.picoclaw-node/main/config.json
```

This writes
`~/Library/LaunchAgents/com.forgeclaw.picoclaw-node.main.plist`, bootstraps it
into the current user's launchd domain, and waits for a stable running state.
Installation is create-only and refuses an existing or foreign plist or an
already loaded job.

Inspect or remove that instance with:

```bash
picoclaw-node status --instance main
picoclaw-node uninstall --instance main
```

A system LaunchDaemon requires root, an absolute configuration path, and an
explicit unprivileged service account:

```bash
sudo picoclaw-node install \
  --system \
  --instance vpn \
  --config /etc/forgeclaw/vpn-node.json \
  --service-user forgeclaw-node
sudo picoclaw-node status --system --instance vpn
sudo picoclaw-node uninstall --system --instance vpn
```

The LaunchAgent and LaunchDaemon lifecycle is transactional and fail-closed.
Status and removal verify the managed plist identity and the exact launchd
domain and plist path. Uninstall first unloads the verified job, quarantines
the exact plist, and restores the previous plist and loaded state when removal
cannot be committed safely. As on Linux, `--json` provides stable
machine-readable output.

## Lifecycle Compatibility

| Platform | User service | System service | Install | Status | Uninstall |
| --- | --- | --- | --- | --- | --- |
| Linux | systemd user unit | systemd system unit | Supported | Supported | Supported |
| macOS | LaunchAgent | LaunchDaemon | Supported | Supported | Supported |
| Other | None | None | Not supported | Not supported | Not supported |

Lifecycle tests exercise both managers' rendering, identity checks,
create-only publication, state inspection, rollback, and removal behavior.
Darwin command tests are cross-compiled on non-macOS development hosts; final
release qualification should still run the lifecycle suite on the listed
native operating systems.

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
policy, identity, and invocation ledgers will remain isolated per binding.

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
with `--allow-command`. If the authenticated catalog changes, execution is
suspended until `nodes approve` is run again with the complete aliases,
display name, and allowed-command set to retain. Deny an untrusted pending
identity or revoke a paired one with a recorded reason:

```bash
picoclaw nodes deny node_<fingerprint> --reason "unknown device"
picoclaw nodes revoke vpn-box --reason "device retired"
```

All read and mutation commands accept `--json`. The CLI prints only a public-key
fingerprint, never the stored raw public key.
