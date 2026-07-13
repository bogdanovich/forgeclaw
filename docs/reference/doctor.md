# PicoClaw Doctor

`picoclaw doctor` runs a read-only static safety audit of the active configuration.
It does not start processes, call the network, apply remediation, migrate config,
write backups, or mutate workspace/state.

## Usage

```sh
picoclaw doctor
picoclaw doctor --json
picoclaw doctor --strict
picoclaw doctor --config /path/to/config.json
picoclaw doctor --stale-task-age 45m --pending-delivery-age 20m
```

## Exit Semantics

- `0`: no `error` or `fail` findings; warnings are allowed by default.
- `1`: command, parsing, or config loading error.
- `2`: one or more `error`/`fail` findings, or any warning when `--strict` is set.

## JSON Schema

The JSON output is stable for PR1 and uses `schema_version: "doctor.v1"`.
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
