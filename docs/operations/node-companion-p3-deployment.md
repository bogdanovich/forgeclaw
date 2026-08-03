# Node Companion P3 service-administration deployment

This runbook enables one bounded Linux systemd service profile after the P3
implementation is merged. It does not enable service authority by default and
does not authorize shell, arbitrary units, flags, environment, D-Bus, or a
second service manager.

## Authority layout

Use four independent grants:

1. the companion advertises only `service.status.v1`, `service.logs.v1`, and
   `service.action.v1` allowed by its local policy;
2. the paired catalog explicitly approves those commands;
3. the gateway target binds one operator-owned `service_profile`, and the
   effective agent explicitly allows that target;
4. a root-owned local helper maps model-safe aliases to exact systemd units
   and actions.

Missing or changed authority at any layer fails closed. Do not put the target
in `tools.approval.bypass_node_targets` unless unattended operation is an
explicit operator decision. A model argument cannot request bypass.

## Example companion config

The companion remains an unprivileged user. The service-helper client merely
names a local root-owned Unix socket:

```json
{
  "policy": {
    "revision": "service-node-v1",
    "allowed_commands": [
      "service.status.v1",
      "service.logs.v1",
      "service.action.v1"
    ],
    "maximum_risk": "privileged",
    "max_timeout_seconds": 30,
    "max_output_bytes": 65536
  },
  "service_helper": {
    "enabled": true,
    "socket_path": "/run/mintclaw-service/helper.sock"
  }
}
```

## Example root-helper config

Keep this file root-owned and not writable by the companion or its groups.
Use the exact UID, GID, systemd cgroup, and executable paths from the deployed
host. The helper accepts exactly one enabled profile.

```json
{
  "socket_path": "/run/mintclaw-service/helper.sock",
  "allowed_uid": 1000,
  "allowed_gid": 1000,
  "companion_cgroup": "/system.slice/mintclaw-node.service",
  "systemctl_path": "/usr/bin/systemctl",
  "journalctl_path": "/usr/bin/journalctl",
  "node_service_policies": {
    "server-services": {
      "enabled": true,
      "revision": "server-services-v1",
      "manager": "systemd-system",
      "services": {
        "application": {
          "unit": "example-application.service",
          "description": "Example application",
          "status": true,
          "logs": true,
          "actions": ["restart"],
          "expected_active_state": "active"
        }
      },
      "log_limits": {
        "entries_max": 50,
        "bytes_max": 16384,
        "age_seconds_max": 3600
      }
    }
  }
}
```

The helper socket must be root-owned, group-readable only by the companion
group, and local to the host. Restart the helper whenever the companion
process identity is deliberately replaced so its pinned cgroup/PID identity
is re-established from a fresh snapshot.

## Gateway target and agent grant

Bind the profile out of band; the model never selects it:

```json
{
  "execution": {
    "targets": {
      "service-node": {
        "type": "node",
        "node": "service-node",
        "executor": "local",
        "service_profile": "server-services"
      }
    }
  },
  "agents": {
    "list": [
      {
        "id": "main",
        "target_policy": {
          "allowed_targets": ["service-node"]
        }
      }
    ]
  }
}
```

Approve only the three intended service commands in node pairing. A broadened
catalog, changed profile revision, changed alias/action mapping, or stale
discovery revision requires fresh authority and cannot reuse a prepared plan.

## Canary and validation

Use a reversible non-essential long-running `.service`; never use networking,
SSH, Tailscale, the gateway, the companion, or the helper itself.

1. Back up gateway, companion, helper, configs, and systemd units.
2. Validate configs before restart and confirm the companion is unprivileged.
3. Start helper and companion, approve pairing, and confirm the target is
   connected.
4. Through the real model-facing path, run `nodes describe` before each fresh
   `service.status.v1`, `service.logs.v1`, and `service.action.v1` invocation.
5. Approve one exact canary restart as the routed human. Verify a new
   activation timestamp and model-visible post-action state.
6. Restart the gateway and prove the activation timestamp did not change.
7. Scan gateway journals and passive diagnostic traces for raw unit names,
   helper paths, service output, credentials, and unrestricted command input.
8. Verify wrong alias/action/actor, stale revision, disconnect, cancellation,
   unknown outcome, and no-replay behavior with deterministic repository tests.

Runtime events may retain bounded lifecycle metadata such as target, command,
service alias, action, risk, state, and opaque correlation. They must not
retain raw unit names, manager output, service log messages, helper paths, or
`nodes_invoke` argument/result previews.

## Rollback

Disable authority before replacing artifacts:

1. remove the exact target from the effective agent's `allowed_targets`;
2. remove the target's `service_profile` binding and restart the gateway;
3. verify `nodes list` no longer exposes the target to that agent;
4. stop the companion and helper;
5. restore the recorded binaries, configs, and units atomically;
6. start helper before the companion completes its initial snapshot, then
   start the gateway and verify health, pairing, journals, and no replay.

Re-enable only after the restored profile, catalog, target, and helper identity
are all current. Rollback never retries a completed or uncertain action.
