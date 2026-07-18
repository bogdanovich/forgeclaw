# Safe Restart And Deploy

ForgeClaw safe restart and deploy support is a control-plane contract between
core runtime orchestration and operator-owned infrastructure scripts.

The runtime goal is to make restart and deploy requests explicit, bounded, and
durable enough to avoid dropping accepted inbound messages. The runtime does not
own Go hot-code reload, git operations, build staging, binary swaps, health
checks, or service-manager-specific deployment policy.

## Terminology

- **Safe restart** restarts the currently configured gateway service without
  necessarily changing the executable binary.
- **Deploy** hands off to an operator-configured script that may update the
  shared binary and restart one or more services.
- **Shared binary group** is a named set of workspace services that execute the
  same active binary path.
- **Target** is the validated deploy scope passed to the deploy script, such as
  `current`, `all`, or a configured profile name.
- **Sentinel** is a durable handoff record written before exit or before a
  deploy command runs, then inspected on startup and through status surfaces.

## Why Restart Is Required

ForgeClaw and PicoClaw are Go binaries. Once the operating system starts a Go
process, the process executes the mapped binary image it already opened. Replacing
the file on disk does not change code that is already running.

For that reason, updating ForgeClaw code requires starting a new process. The
expected mechanism is a supervisor such as systemd, launchd, a container
orchestrator, or another operator-owned process manager. Core may request a
configured service restart; it must not try to replace its own executable in
place or implement Go hot-code reload.

## Safe Restart Flow

Safe restart is designed for commands like "restart this gateway when safe".
The runtime entry point is the `gateway_restart` tool when
`gateway.safe_restart.enabled` is true. The tool accepts an optional reason; the
service manager and service unit are always read from config, never from
model-supplied arguments.

1. Validate that safe restart is enabled in config.
2. Run a bounded preflight that reports known active work:
   - active turns or sessions
   - pending normalized inbound spool entries
   - active cron or background jobs when available
   - outbound/channel drain state when available
3. If active work exists, defer until idle or until the configured drain timeout.
4. Write a durable restart sentinel that records:
   - kind: `restart`
   - status
   - configured service
   - originating workspace/session
   - timestamp and reason
   - preflight summary
5. Request a restart through a narrow platform abstraction, for example
   `systemctl --user restart <configured-unit>`.
6. On startup, read the sentinel and expose continuation/status where possible.

Safe restart should reduce risk by waiting for active work to drain. It is not a
checkpointing system for an in-flight LLM call or tool execution.

## Platform Support Matrix

| Platform | `service_manager` | Service identifier | Status |
| --- | --- | --- | --- |
| Linux | `systemd-user` | Simple `.service` unit, such as `picoclaw-main.service` | Supported. Core uses `systemctl --user restart --no-block`. A signalled caller is an indeterminate handoff, confirmed by the replacement gateway. |
| macOS | `launchd` | Explicit launchctl target, such as `gui/501/com.example.picoclaw` | Supported backend. Core uses `launchctl kickstart -k`; the operator owns bootstrap, plist installation, and target domain selection. |
| Windows | `windows-scm` | Windows service name | Explicitly unsupported for self-restart. SCM has no atomic self-restart primitive; core rejects this configuration until an operator-provided external supervisor helper is implemented. |

Core validates the manager against the running operating system before any
restart work is scheduled. It never silently routes a non-Linux configuration
to `systemctl`.

## Deploy Flow

Deploy is a separate operation from safe restart. A deploy may rebuild a binary,
swap active binary paths, restart services, run health checks, and roll back on
failure. Those details are intentionally outside core.

Core responsibilities:

1. Validate that deploy is enabled in config.
2. Validate the requested target against configured allowed targets.
3. Write a durable deploy sentinel before invoking the command.
4. Invoke only the configured command with a fixed target argument.
5. Set useful environment variables for the script.
6. Enforce a bounded timeout.
7. Capture bounded stdout/stderr for operator-visible status.
8. Treat command exit status, and optional structured JSON if later specified,
   as the only source of truth.

Core also takes a non-blocking, kernel-managed advisory lock scoped to the
configured deploy group before writing its singleton deploy sentinel. This
prevents two workspace services in the same shared-binary group from launching
overlapping deploy scripts. The lock is released automatically if the gateway
process exits; the operator script must still use its own `flock` because it is
the final authority over local build and service operations.

Operator script responsibilities:

1. Acquire a deploy lock, for example with `flock`.
2. Fetch or pull source according to local policy.
3. Build into a staged path.
4. Verify the staged binary.
5. Atomically swap the active binary path.
6. Restart the selected service or services through the local supervisor.
7. Run health checks.
8. Roll back on failure.
9. Print clear operator-readable output.

Core must not accept arbitrary shell fragments from the model or chat command.

## Suggested Config Shape

```json
{
  "gateway": {
    "safe_restart": {
      "enabled": true,
      "service_manager": "systemd-user",
      "service": "picoclaw-main.service",
      "drain_timeout_seconds": 300,
      "force_after_timeout": true
    },
    "deploy": {
      "enabled": true,
      "group": "picoclaw-local",
      "command": "/home/server/.picoclaw/shared/deploy/picoclaw/deploy.sh",
      "default_target": "current",
      "allowed_targets": ["current", "all", "main", "nutrition", "spouse", "reviewer"],
      "handoff_targets": ["current", "all"],
      "timeout_seconds": 600
    }
  }
}
```

For a macOS LaunchAgent, use the configured launchctl target rather than a
plist path:

```json
{
  "gateway": {
    "safe_restart": {
      "enabled": true,
      "service_manager": "launchd",
      "service": "gui/501/com.example.picoclaw"
    }
  }
}
```

This shape is illustrative. Runtime implementation should preserve existing
config compatibility and validation conventions.

## Deploy Command Contract

Core invokes exactly the configured command path and passes one validated fixed
argument:

```sh
deploy.sh --target current
deploy.sh --target all
deploy.sh --target main
```

Core sets these environment variables when values are known:

```sh
FORGECLAW_DEPLOY_GROUP=picoclaw-local
FORGECLAW_DEPLOY_TARGET=current
FORGECLAW_WORKSPACE=/path/to/workspace
FORGECLAW_SERVICE=picoclaw-main.service
FORGECLAW_SESSION_KEY=<originating-session>
```

The command path should be absolute or validated according to existing config
policy. The target must come from `allowed_targets`; model-supplied text must
never become additional shell arguments.

`handoff_targets` identifies targets that can restart the gateway which invoked
the deploy. On Linux with `safe_restart.service_manager: "systemd-user"`, those
targets run in a transient systemd worker rather than in the gateway service
cgroup. The worker survives the restart and records its terminal deploy status
for the restarted gateway to deliver. Other supervisors keep the ordinary
synchronous deploy path, so the configuration remains portable.

## Shared Binary Groups

Multiple workspace services may execute the same binary path. A deploy group
makes that relationship explicit while still leaving local process management to
the operator script.

Target semantics:

- `current` means the service/profile associated with the current workspace.
- `all` means every service in the configured shared binary group.
- a named target such as `main` or `reviewer` means that configured
  service/profile only.

Important caveat: replacing a shared binary file does not update already running
processes. If one profile is restarted after a binary swap, only that profile
starts the new executable. Other running services keep executing the old inode
until they are restarted. Operators should use `all` when they intend every
profile in the group to run the new binary immediately.

## Durability Boundaries

Safe restart and deploy must preserve current durable inbound behavior.

Protected today:

- Normalized inbound messages after `MessageBus.PublishInbound` writes them to
  the durable ingress spool.
- Replayed pending or processing ingress entries on gateway startup.
- Messages accepted into session history, relying on existing unanswered-session
  recovery behavior after restart.

Not fully protected today:

- Raw platform updates before channel normalization and `PublishInbound`.
- Active LLM or tool execution in the middle of a turn.
- In-memory outbound delivery queues.
- In-memory steering state after a continuation has crossed into deeper turn
  execution.

Safe restart reduces exposure by deferring until idle and by recording durable
handoff status. It does not add a raw transport spool, a durable outbound queue,
or full execution checkpointing.

## Status And Continuation

Sentinels provide the bridge across process exit:

- Before restart or deploy, core records the requested operation and origin.
- During or after deploy, core records status, exit code, and bounded output tail.
- Core also records the originating channel, chat, and topic/thread when they
  are known, so a later process can address a continuation without asking the
  model to infer a destination. A restart or deploy initiated in a Telegram
  forum topic must continue in that same topic rather than the parent chat.
- After channels are running, a gateway-lifetime reconciler inspects deploy
  sentinels and sends one terminal continuation to the saved origin. This
  reports failures while the original gateway remains alive; after a successful
  self-replacing deploy, the replacement gateway performs the same
  reconciliation on startup.
- The read-only `gateway_handoff_status` tool exposes the latest restart/deploy
  handoff, including deploy exit code and a bounded output preview.
- Core records `continuation_sent_at` after queueing a continuation. This keeps
  each handoff idempotent across live polling and process startup, but does not
  create a durable outbound queue.

Handoff notification must be idempotent. Repeated polling and process starts
must not spam the same session with duplicate continuation messages.

## Non-Goals

- No in-process Go hot-code reload.
- No arbitrary shell execution from model-supplied arguments.
- No core-owned git pull, build, binary swap, health check, or rollback logic.
- No durable outbound queue in this series.
- No transport-level Telegram raw update spool in this series.
- No broad channel lifecycle refactor unless required by the safe restart control
  plane.

## Implementation Sequence

The intended implementation path is intentionally small and reviewable:

1. Document the architecture and boundaries.
2. Add safe restart config, preflight, and durable restart sentinel support.
3. Wire a controlled restart command/action through a service-manager
   abstraction.
4. Add deploy config, target validation, script execution, timeouts, bounded
   output capture, and deploy sentinels.
5. Expose startup continuation and operator status idempotently.
6. Add optional operator-owned example scripts and deployment docs.
