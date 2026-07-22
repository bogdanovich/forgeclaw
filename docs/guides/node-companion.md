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

## Install On Linux

Install a named systemd user service after validating the node configuration:

```bash
picoclaw-node install \
  --instance main \
  --config ~/.picoclaw-node/main/config.json
picoclaw-node status --instance main
```

The installer writes
`~/.config/systemd/user/picoclaw-node-main.service`, reloads the user manager,
and enables and starts the service. It uses direct `systemctl` arguments and
records absolute executable and configuration paths in the unit. Installation
is create-only and refuses to replace an existing unit; upgrades and
reinstallation are intentionally separate lifecycle operations.

System-wide installation is explicit and normally requires root:

```bash
sudo picoclaw-node install \
  --system \
  --instance vpn \
  --service-user forgeclaw-node \
  --config /etc/forgeclaw/node-vpn.json
sudo picoclaw-node status --system --instance vpn
```

Use `--json` with any lifecycle command for stable machine-readable status.
System installation deliberately requires an existing unprivileged account;
the generated unit never runs the companion as root. It also requires an
explicit absolute `--config` path so an invocation through `sudo` cannot bind
the service account to the invoking account's home directory. For a user
service that must remain active after logout, enable systemd lingering for that
account with the operator-controlled `loginctl enable-linger <user>` command.
macOS LaunchAgent lifecycle support follows separately; `run` remains available
on every supported platform.

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
