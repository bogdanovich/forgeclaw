# Node Companion P3 deployment evidence

Date: 2026-08-03

Status: complete

This record closes the Node Companion P3 typed service-administration
milestone. It combines merged-code evidence, Linux CI, deterministic lifecycle
tests, and a bounded live model-to-systemd canary. It does not authorize P4,
macOS launchd administration, broad approval bypass, or arbitrary system
management.

## Merged revisions

| PR | Merge commit | Scope |
| --- | --- | --- |
| #480 | `c6e6a296` | P3 admission contract and mandatory stop condition |
| #483 | `d7155ade` | Typed service domain, policy, schemas, discovery, and target profile |
| #492 | `02ef7eca` | Bounded Linux systemd status and historical logs |
| #498 | `80e88dd5` | Root-owned local service helper and verified actions |
| #504 | `74998a25` | Generic approval/WSS/companion vertical slice and lifecycle events |
| #509 | `b52ee76b` | Fail-closed `nodes_invoke` diagnostic content redaction |

Every implementation head passed GitHub Tests, Integration Tests, Linter,
Security Check, and Browser Windows before merge. PR #504 completed four
substantive review/fix cycles. Its architecture checkpoint found about ten
percent production growth, no repeated unresolved lifecycle invariant, and no
new store, transport, protocol version, helper family, or execution path.

The deployment redaction canary then found one raw service-log result inside a
model-request diagnostic preview. The correction was deliberately smaller
than a new abstraction: all generic `nodes_invoke` arguments and results are
now content-redacted, while `node.invocation.observed` retains bounded passive
lifecycle metadata. PR #509 and this final evidence PR are the only checkpoint
adjustment to the admitted delivery sequence.

## Built and deployed artifacts

| Artifact | Source revision | SHA-256 |
| --- | --- | --- |
| Linux amd64 gateway and CLI | `b52ee76b` | `e132c9c76102b9b1ea69d6b2e40a4e45248cc54311bdafee5b4d98011e5384b7` |
| Linux amd64 P3 companion | `74998a25` | `8295e65f50c66ddf08c42760e66b16a70e887858bd5c4cd1f2ef7e76c1b41c79` |
| Linux amd64 service helper | `74998a25` | `a824687b275f1ce871f2d9f6c995d1e28c937e20ad7466230171030f6d559bdc` |

The companion and helper source revision is an ancestor of the deployed
gateway and contains the complete P3 runtime. The later gateway revisions do
not change their protocol contract. The gateway was fast-forwarded from clean
`origin/main`, rebuilt, installed atomically, and restarted only for the main
workspace. The reviewer process remained PID `3237375` throughout the P3
closeout deployment.

Health and readiness returned `ok` and `ready`; the main ten-minute error
journal contained zero entries. The P3 companion, service helper, and canary
units were active, and target `p3-canary` was connected after restart.

## Requirement matrix

| Requirement | Merged PR/package | Authoritative tests | Deployed status |
| --- | --- | --- | --- |
| Disabled bounded authority | #483; `pkg/config`, `pkg/nodes/companion`, `pkg/tools` | policy/schema/default/unsupported-platform tests | One named Linux target/profile enabled; no broad bypass |
| Ownership and discovery intersection | #483, #504; `pkg/nodes`, `pkg/tools`, `pkg/gateway` | actor, agent, route, session, execution identity, profile, catalog, and stale-revision tests | Main agent alone grants `p3-canary`; fresh discovery required |
| Safe status and bounded logs | #492; `pkg/nodes/companion` | exact argv, fixed projection, malformed output, record/byte/age ceiling, cancellation, and subprocess tests | Live status active/running; bounded log result completed |
| Exact action and approval | #498, #504; helper and invocation packages | alias/action, retained plan, expiry, actor, bypass, duplicate, and approval continuation tests | One routed `allow_once` approved one exact restart |
| Root helper boundary | #498; `cmd/mintclaw-node-service-helper`, `pkg/nodes/companion` | UID/GID, socket, cgroup/PID, profile/revision, executable identity, wrong peer, and protocol tests | Companion UID/GID 1000; helper UID/GID 0; local socket mode 0660 |
| Post-action truth | #498, #504; service manager/helper/runtime | activation monotonicity, manager acceptance, verification loss, and unknown-outcome tests | Activation timestamp changed once and verified active/running |
| Cancellation, recovery, no replay | #498, #504; helper, ledger, WSS, gateway | acceptance/cancel/disconnect/restart/duplicate/status race and E2E tests | Gateway restart left activation timestamp unchanged |
| Platform and observability truth | #492, #504, #509; companion, events, agent diagnostics | Linux real-process, Darwin unsupported, lifecycle event, and trace redaction tests | Linux only; journal and post-fix trace scans contain no service output |
| Real operator vertical slice | #504; gateway/WSS/companion/helper/systemd | mock-model real-process E2E plus production canary | Model discovered, read, requested approval, restarted, and received verified state |
| Merge, deployment, rollback | all listed PRs; this runbook/evidence | exact-head CI, focused validation, health, canary, redaction, and rollback checks | Merged main deployed, backed up, healthy, and rollback rehearsed |

## Live canary evidence

The reversible canary is a non-essential long-running service. The root helper
profile exposes only model-safe alias `canary`, bounded status/log reads, and
action `restart`. The unit mapping, helper paths, executables, cgroup, and
manager details are never projected as authority. Authorized log messages are
untrusted content and may themselves mention a unit name.

- The live main agent discovered `service.status.v1`, obtained routed human
  approval, and received `active`/`running` through gateway, WSS, unprivileged
  companion, root helper, and systemd.
- A separately approved `service.logs.v1` request returned one bounded record;
  a post-fix canary returned two records with `truncated=false`.
- The model discovered `service.action.v1`, requested exactly one restart,
  waited for routed `allow_once`, and received `completed` with verified
  `active` state. The systemd activation identity changed exactly once.
- Restarting the gateway did not change the canary activation timestamp, so no
  completed mutation was replayed.
- Removing only the target binding and main-agent grant made
  `p3_canary_visible=false`. Restoring the saved configuration reconnected the
  same target; unrelated profiles and services were not changed.
- Repository tests provide deterministic wrong actor/route/action, stale
  authority, disconnect, cancellation, response-loss, unknown, duplicate, and
  helper-peer denial evidence that is unsafe or impractical to manufacture on
  the live service.

## Redaction, retention, and cleanup

Gateway `info` journals retained only bounded lifecycle metadata and contained
zero hits for the raw unit, helper socket/config/executable paths, or raw
service-log message. Debug logging remained disabled.

The pre-fix scan found one diagnostic trace containing the bounded canary log
result in `latest_message`. After PR #509 deployment, the same live operation
produced a `redacted_content` trace whose `nodes_invoke` tool record contained
only `tool`, `executed`, and `status`; it had no arguments or result preview.
The exact pre-fix canary trace was removed from the active bounded trace store.
A full rescan then found zero trace files containing the raw unit or canary log
message. No other trace was removed.

## Backups and rollback

Retained backups are:

- initial P3 gateway deployment:
  `/home/server/mintclaw-p3-deploy-backup-20260803T064446Z`;
- canary config and unit state:
  `/home/server/mintclaw-p3-canary-backup-20260803T065100Z`;
- pre-redaction-fix gateway, CLI, config, unit, and source revision:
  `/home/server/mintclaw-p3-redaction-backup-20260803T072924Z`.

Rollback was rehearsed by removing the exact P3 target and main-agent grant,
restarting the gateway, and proving the target invisible. The saved
configuration was then restored atomically; gateway, helper, companion, and
canary returned healthy and the target reconnected. The separate artifact
backup supports binary rollback after authority is disabled.

## Enabled authority and residual limits

The active `p3-canary` target is granted only to the main agent. It binds one
`p3-canary-services` profile with one `canary` alias, bounded status/log reads,
and restart. Human approval is required; the target is not in the approval
bypass list. The companion remains unprivileged and only the local helper has
UID 0.

P3 does not provide macOS launchd or Windows service management, arbitrary
units, flags, shell, environment, D-Bus, live log following, unbounded history,
daemon reload, package operations, dependency orchestration, fleet management,
or another privileged helper family.

All ten P3 gates now have authoritative evidence. Under the admission
contract's mandatory completion condition, P3 is complete and implementation
stops here. P4 and every later milestone remain unadmitted.
