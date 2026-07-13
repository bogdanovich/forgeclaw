# PicoClaw Doctor

`picoclaw doctor` audits a ForgeClaw deployment for unsafe configuration and
unresolved operational state. It is designed for interactive troubleshooting,
deployment checks, and CI automation.

Doctor is read-only. It does not start processes, call the network, apply
remediation, migrate configuration, write backups, reconcile tasks, or mutate
workspace state. You can run it while the gateway is active.

## Quick Start

Run Doctor on the active deployment:

```sh
picoclaw doctor
```

The command prints every finding with:

- a stable check ID, such as `channels.open_allow_from`;
- severity and status;
- the reason the configuration or state is risky;
- a suggested remediation;
- redacted evidence identifying the relevant configuration or state location.

Doctor does not print credential values, task text, chat IDs, task errors, or
other persisted payloads.

After changing configuration or resolving operational state, run the same
command again. A normal remediation workflow is:

```sh
picoclaw doctor
# Review and fix findings.
picoclaw doctor
```

## Usage

```sh
picoclaw doctor
picoclaw doctor --json
picoclaw doctor --strict
picoclaw doctor --config /path/to/config.json
picoclaw doctor --stale-task-age 45m --pending-delivery-age 20m
```

`--config` selects a deployment explicitly. Without it, Doctor uses the active
PicoClaw configuration path in the same way as other CLI commands.

## Reading Results

Severities have the following meaning:

| Severity | Meaning | Default exit behavior |
| --- | --- | --- |
| `error` | Doctor could not reliably audit part of the deployment, for example because state is malformed. | Exit `2`. |
| `fail` | A high-confidence unsafe or broken condition was found. | Exit `2`. |
| `warning` | A risky or operationally suspicious condition needs review. | Exit `0`, or `2` with `--strict`. |
| `info` | A relevant trust or configuration characteristic is present. | Exit `0`. |

A finding is not automatically proof of compromise. For example, a public
gateway bind may be intentional behind an authenticated reverse proxy, and a
remote MCP endpoint may be acceptable on a trusted authenticated network.
Doctor reports the locally observable risk and documents where runtime or
network context still requires operator judgment.

## Exit Semantics

- `0`: no `error` or `fail` findings; warnings are allowed by default.
- `1`: command, parsing, or config loading error.
- `2`: one or more `error`/`fail` findings, or any warning when `--strict` is set.

Exit `2` is an audit result, not a CLI crash. Human-readable findings are still
written to stdout. Exit `1` means Doctor itself could not complete normally.

Use strict mode for deployment gates where warnings must be resolved or
explicitly accepted:

```sh
picoclaw doctor --strict
```

## JSON Schema

Use JSON output for scripts and CI:

```sh
picoclaw doctor --json > doctor-report.json
jq '.summary' doctor-report.json
```

In shell scripts, capture the audit exit code separately because a valid report
can intentionally exit with `2`:

```sh
set +e
picoclaw doctor --json > doctor-report.json
doctor_status=$?
set -e

jq empty doctor-report.json
if [ "$doctor_status" -ne 0 ]; then
  jq '.findings[] | select(.severity == "error" or .severity == "fail")' doctor-report.json
  exit "$doctor_status"
fi
```

JSON stdout contains only the report, without the interactive PicoClaw banner.
The output uses `schema_version: "doctor.v1"`.
Top-level fields are `schema_version`, `generated_by`, `config_path`, `summary`,
and `findings`. Each finding has `id`, `severity`, `status`, `title`,
`rationale`, `remediation`, and optional redacted `evidence`.

Evidence never includes secret values. Credential checks report only document
paths and a presence summary.

Operational checks inspect each unique configured agent workspace. They read
persisted JSON directly and never instantiate state stores that prune, save,
lock, reconcile, or create directories. Missing state files are normal. The
default thresholds are 30 minutes for active tasks, 15 minutes for pending
terminal deliveries, 24 hours for recent failures, and 10 minutes for gateway
handoffs; use the corresponding duration flags to tune them.

## Operational Thresholds

Thresholds accept Go duration values such as `30m`, `2h`, or `48h`:

| Flag | Default | Controls |
| --- | --- | --- |
| `--stale-task-age` | `30m` | How long a queued/running task may go without activity. |
| `--pending-delivery-age` | `15m` | How long a terminal task may remain pending delivery. |
| `--recent-failure-age` | `24h` | How far back task, delivery, and handoff failures are reported. |
| `--handoff-age` | `10m` | How long restart/deploy reconciliation or continuation may remain unresolved. |

Example for a deployment with intentionally long background jobs:

```sh
picoclaw doctor \
  --stale-task-age 2h \
  --pending-delivery-age 30m \
  --recent-failure-age 48h
```

Raising a threshold suppresses age-based findings; it does not modify or
reconcile the underlying task or delivery.

## What Doctor Does Not Do

Doctor does not:

- inspect firewall, NAT, reverse-proxy, or remote server policy;
- connect to providers, channels, MCP servers, or skill registries;
- verify that configured credentials are valid;
- probe whether platform isolation is available at runtime;
- retry tasks, resend deliveries, or complete restart reconciliation;
- replace host monitoring, secret scanning, or incident response.

These boundaries keep the command deterministic, offline, and safe to run on a
live personal deployment.

## Checks

| ID | Severity | Rationale | Remediation | Limitations |
| --- | --- | --- | --- | --- |
| `gateway.public_exposure` | fail | Wildcard or public gateway binds can expose local control surfaces. | Bind to loopback or put the gateway behind authenticated infrastructure. | Static bind analysis only; does not inspect firewall/NAT. |
| `channels.open_allow_from` | warning | Empty or wildcard `allow_from` allows all sender identities accepted by the channel. | Configure explicit sender/chat/account allowlists. | Channel-specific identity semantics vary. |
| `channels.permissive_group_trigger` | warning | Group channels without mention, prefix, or topic constraints can activate unexpectedly. | Require mention-only triggers or narrow prefixes/topics. | Does not know whether a channel is currently in group chats. |
| `tools.exec_remote_write` | fail | Remote exec with write-capable permissions can start mutating host processes. | Disable remote exec or set `permission_mode: read_only`. | Does not execute or classify runtime commands. |
| `tools.filesystem_write_scope` | fail | Write tools with broad workspace/write scopes can mutate host files. | Keep workspace restriction enabled and write roots narrow. | Path broadness is conservative and local-config based. |
| `tools.install_skill_enabled` | warning | Skill installation mutates local skill directories and may introduce instructions/scripts. | Disable `install_skill` unless explicitly needed. | Does not inspect registry contents. |
| `isolation.disabled_or_ineffective` | warning | Disabled isolation or writable exposed paths weakens subprocess containment. | Enable isolation and prefer read-only exposed paths. | Platform support is not probed. |
| `mcp.remote_transport` | warning | Remote MCP transports expand trust beyond local stdio. | Prefer stdio or trusted loopback endpoints. | Does not connect to MCP servers. |
| `mcp.insecure_transport` | fail | HTTP MCP can expose prompts, tool data, and credentials in transit. | Use HTTPS or stdio. | Local HTTP is still reported as insecure transport. |
| `mcp.overexposed_transport` | warning | Non-loopback MCP endpoints rely on external network/server controls. | Use loopback/private authenticated endpoints or stdio. | Does not inspect DNS, firewall, or auth policy. |
| `credentials.plaintext_presence` | fail | Plaintext credentials in config/security documents can leak via backups or commits. | Use encrypted security storage or file/env references; rotate if exposed. | Reports presence only; never emits values. |
| `skills.external_registry` | warning | External registries influence skill discovery/install inputs. | Enable only trusted registries and review installed skills. | Does not fetch registry metadata. |
| `skills.workspace_global_shadowing` | info | Workspace skills may shadow or supplement global skills. | Keep trusted skill sources separated from untrusted workspaces. | Reports locally knowable workspace differences only. |
| `skills.automatic_mutability` | info | Skill discovery can feed later installation workflows. | Keep `install_skill` disabled unless delegated installs are intentional. | Discovery itself is not treated as mutation. |
| `evolution.auto_apply` | warning | Evolution apply or automatic modes can create/apply local changes. | Use observe/manual modes unless automatic mutation is intended. | Does not inspect pending evolution drafts. |
| `models.fallback_missing` | fail | Missing model fallback references break deterministic failover. | Add the referenced model or remove the fallback. | Static model-list references only. |
| `models.fallback_duplicate` | warning | Duplicate fallbacks reduce failover clarity. | Remove duplicate fallback names. | Does not judge provider equivalence. |
| `models.fallback_cycle` | fail | Cyclic fallback chains can prevent predictable failover. | Remove an edge in the cycle. | Reports configured graph cycles only. |
| `agents.fallback_missing` | fail | Missing agent/subagent model references can prevent startup or delegation. | Add the model or update the reference. | Static agent model references only. |
| `tokens.context_inconsistent` | fail/warning | Inconsistent token/window/summarization settings can produce invalid requests or ineffective compaction. | Keep `max_tokens` below context and summarization thresholds sane. | Uses configured defaults, not provider runtime metadata. |
| `state.unreadable` | error | Malformed or unreadable persisted state prevents a trustworthy operational audit. | Repair permissions/JSON or restore trusted state. | Contents and parse details are omitted to avoid leaking task data. |
| `tasks.stale_active` | fail | Old queued/running tasks may have lost their runtime owner. | Inspect and reconcile or restart affected tasks. | Uses persisted heartbeat/start/create timestamps and a configurable threshold. |
| `tasks.recent_failure` | warning | Recent failed, timed-out, or lost tasks may need attention. | Inspect task status and retry where appropriate. | Historical failures outside the configured window are ignored. |
| `deliveries.pending_terminal` | warning | Terminal tasks with old pending delivery may never reach recipients. | Inspect and settle or retry delivery. | Reports aggregate counts only. |
| `deliveries.recent_failure` | warning | Failed or parent-missing deliveries indicate lost results. | Check channel/parent health and retry safely. | Historical failures outside the configured window are ignored. |
| `restart.reconciliation_pending` | fail | Old pending/running restart or deploy sentinels indicate incomplete reconciliation. | Inspect handoff status/logs before retrying. | Reads the default gateway workspace only. |
| `restart.continuation_pending` | warning | A terminal handoff lacks continuation acknowledgement. | Inspect channel delivery and acknowledge/retry safely. | Cannot prove whether an out-of-band notification arrived. |
| `restart.recent_failure` | warning | A restart/deploy handoff failed recently. | Inspect handoff status and logs. | Historical failures outside the configured window are ignored. |
