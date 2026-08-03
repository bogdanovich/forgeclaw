# Node Companion P3 Typed Service Administration Admission

## Status And Decision

Status: complete; merged and deployed evidence is recorded in
[`node-companion-p3-deployment-evidence.md`](../operations/node-companion-p3-deployment-evidence.md)

P3 adds one bounded Linux systemd vertical slice through the existing node
transport, target policy, discovery, durable invocation, approval, recovery,
and audit architecture. It does not create another gateway execution path or
turn service administration into shell execution.

The initial commands are:

- `service.status.v1` for one configured service alias;
- `service.logs.v1` for a bounded historical log window; and
- `service.action.v1` for one explicitly allowlisted action.

The model continues to use `nodes describe`, `nodes_invoke`, `nodes_status`,
and `nodes_cancel`. P3 adds no model-facing service tool, gateway store,
transport, task type, replay engine, protocol version, or generic durable-tool
abstraction.

Linux systemd is the only admitted service manager. macOS launchd scope is
explicitly deferred: a Darwin companion advertises no P3 service command. P3
does not establish a privileged macOS helper, unified-log policy, system-domain
launchd mutation contract, or installer lifecycle for those authorities.

## Concrete Operator Use Case

An operator installs an unprivileged companion on a Linux server and creates
one out-of-band service profile. The profile exposes stable model-safe aliases,
for example `vpn` and `mintclaw`, that map to exact root-owned systemd service
units.

An authorized agent can:

1. discover the service commands and the exact aliases and actions available
   on its configured target;
2. inspect a bounded status projection for `vpn`;
3. retrieve at most the configured number and byte size of recent `vpn` log
   entries;
4. request an allowlisted action such as `restart`;
5. wait for durable human approval unless an operator configured an exact
   target-scoped bypass;
6. receive a verified post-action status or an explicit unknown outcome; and
7. recover the same durable result through `nodes_status` without replaying
   the action.

The first deployed mutation uses a dedicated reversible canary service. P3
does not require restarting networking, SSH, Tailscale, the active companion,
the active gateway, or another control-plane dependency to prove the slice.

## Security Truth And Threat Model

Service administration is privileged remote execution with a smaller input
language. A typed command reduces ambiguity and blast radius; it does not make
an allowed restart harmless.

Protected assets include:

- services not named by the selected profile;
- actions not allowed for the selected alias;
- raw systemd unit names, templates, drop-ins, environment, credentials, and
  manager connection details;
- journal content outside the selected unit and bounded window;
- root authority held by the local service helper;
- actor, route, session, agent, target, approval, and execution identity;
- truthful action acceptance, completion, cancellation, and recovery state;
  and
- unrelated owner-shell, terminal, file-helper, and system-exec authority.

P3 assumes the model, prompt content, service logs, remote peer, and
unprivileged companion may be malicious or compromised. It also assumes
disconnects, process crashes, response loss, stale discovery, concurrent
requests, and service-manager delays can occur at every lifecycle boundary.

The design must prevent:

- caller-selected unit names, manager scopes, helper paths, executables,
  command-line flags, environment, users, or approval modes;
- shell text, argv, globbing, template expansion, manager passthrough, and
  arbitrary D-Bus requests;
- one alias selecting a different unit after preparation or approval;
- log queries escaping to another unit or unbounded history;
- automatic retry of an accepted mutation after an uncertain outcome;
- treating current service state alone as proof that a requested action ran;
- a model, service log line, or ordinary chat response approving its own
  mutation; and
- passive events, traces, or journals retaining service log content.

Compromise of a companion with an enabled system-service profile can exercise
the typed authority in that profile. The helper boundary keeps all unrelated
companion functionality outside UID 0; it is not a sandbox for a deliberately
allowed service action.

## Sources Of Truth

The existing sources of truth remain authoritative:

1. authenticated channel sender and routed conversation ownership;
2. workspace, agent, session, turn, provider tool-call, and execution identity;
3. the agent's configured target grant and target service-profile binding;
4. durable node pairing, approved command catalog, and current live session;
5. the exact prepared node execution plan and its existing durable approval;
6. companion local command policy and current service-profile projection;
7. root-owned helper config, peer identity, profile revision, and unit/action
   mapping; and
8. the systemd manager's observed job and unit state.

Discovery, prompts, model arguments, log text, current unit state, passive
events, and helper response text are not authority sources.

## Authority And Profile Model

Authority is the intersection of all sources above. Missing or changed
authority fails before manager acceptance.

The gateway target binds at most one service-profile alias. The model selects a
target and service alias; it never selects a profile. A target without a
service-profile grant exposes no service command even if the paired node
advertises one.

Delegated agents and background tasks receive no implicit service authority.
They require their own agent target grant, routed ownership, and current
discovery revision through the existing invocation path.

The architecture-level node policy shape is:

```yaml
nodes:
  targets:
    personal-vpn:
      service_profile: server-services

node_service_policies:
  server-services:
    enabled: false
    revision: server-services-v1
    manager: systemd-system
    services:
      vpn:
        unit: wg-quick@wg0.service
        description: Managed VPN service
        status: true
        logs: true
        actions: [restart]
    log_limits:
      entries_max: 200
      bytes_max: 65536
      age_seconds_max: 86400
```

The companion receives a model-safe projection and a local helper socket. The
root-owned helper independently retains the real alias-to-unit mapping and
limits. Exact implementation field placement may differ, but these semantics
are fixed.

Validation requires:

- absent profiles, missing target bindings, and `enabled: false` grant
  nothing;
- aliases and revisions are bounded identifiers with duplicate and
  case-collision rejection;
- exactly one manager value, `systemd-system`, is accepted in P3;
- each alias maps to one exact `.service` unit; wildcards, partial names,
  caller-supplied instances, paths, and other unit types are rejected;
- units used for mutation are ordinary long-running service units with
  configured post-action expectations; oneshot, transient, generated,
  socket, timer, mount, target, slice, scope, and device units are excluded;
- read and action grants are independent;
- actions are a subset of `start`, `stop`, `restart`, `reload`, `enable`, and
  `disable`;
- `enable` and `disable` never imply `--now`; the running-state change, when
  wanted, is a separate allowlisted action and approval;
- log ceilings may narrow hard limits but cannot exceed them; and
- any authority-bearing profile, helper snapshot, unit mapping, action set, or
  limit change alters the authenticated descriptor/catalog identity and makes
  stale discovery and prepared work fail closed.

The model cannot provide or override profile, unit, manager scope, helper,
executable, environment, OS identity, approval policy, verification rule, or
limit.

## Model-Visible Contract

P3 extends the authenticated command descriptor with one bounded service
profile projection, following the existing file-profile pattern. The
projection participates in the descriptor and catalog hashes.

For every visible service alias discovery may expose:

- alias and bounded operator description;
- whether status and logs are available;
- exact admitted action names;
- effective log-entry, byte, and age ceilings;
- manager kind `systemd` without its connection details;
- action approval as `required` or `operator_bypass_configured`; and
- command timeout, output, cancellation, and result-schema metadata.

Discovery excludes:

- raw unit names, helper paths, executables, D-Bus endpoints, UIDs, GIDs,
  cgroups, credentials, environment, and root policy;
- service names outside the selected target profile;
- unit files, drop-ins, environment files, dependency graphs, neighboring
  units, and journal fields not in the result contract;
- catalog, descriptor, policy, plan, and helper-authority hashes; and
- unrestricted examples or manager-specific flags.

The command-specific effective schema enumerates only aliases available for
that command. The action schema represents only valid alias/action pairs; a
union that falsely implies every action is valid for every alias is not
acceptable. Discovery remains advisory and preparation rechecks the complete
authority intersection.

## Typed Commands

### `service.status.v1`

Input:

```json
{
  "service": "vpn"
}
```

The bounded result contains:

- service alias;
- normalized load state;
- normalized active state;
- normalized substate from a fixed safe vocabulary;
- enabled state when systemd can determine it;
- observed timestamp;
- a bounded manager result code when unavailable; and
- no raw unit name, command line, environment, path, dependency list, or
  unrestricted manager output.

Status is observational. It cannot acknowledge or complete a separate action
and is not proof that a requested restart occurred.

### `service.logs.v1`

Input:

```json
{
  "service": "vpn",
  "entries": 100,
  "since_seconds": 3600
}
```

Both numeric fields are optional bounded requests and are clamped by the
profile and hard limits. The result contains a bounded chronological array of
records with timestamp, normalized severity when known, and UTF-8 message.

Initial hard limits are 500 records, 256 KiB total result bytes, 16 KiB per
record, and seven days of history. Configuration may only narrow them. Invalid
UTF-8 is replaced safely; control characters are bounded; oversized records
and totals report explicit truncation. There is no follow mode, cursor, live
stream, export, binary field, structured-field selector, grep, regex, or
caller-supplied journal predicate.

Authorized service logs are user data and may contain secrets emitted by the
managed service. They may appear only in the authorized model result. MintClaw
does not copy their messages into its own logs, passive events, diagnostic
traces, approval text, or deployment evidence.

### `service.action.v1`

Input:

```json
{
  "service": "vpn",
  "action": "restart"
}
```

The command risk is `privileged`. Existing durable approval binds the exact
target, node, service alias, action, descriptor/catalog identity, policy
revision, helper projection, actor, agent, route, session, tool call,
execution identity, plan hash, and expiry. A target-scoped operator bypass may
skip the prompt only when existing policy explicitly admits that target and
actor context; a model argument cannot request bypass.

The helper maps the alias and action to fixed systemd operations. It accepts no
shell, argv, unit, flag, mode, user, environment, timeout extension, or
verification override from the request.

## Linux Systemd Semantics

The root helper may use systemd's manager API or root-owned absolute
`systemctl` and `journalctl` executables. If executables are used, the helper
constructs a fixed argument vector, supplies a minimal fixed environment,
disables pagers and colors, bounds stdout/stderr, and never invokes a shell.
Executable identity is validated from root-owned configuration.

Status reads only a fixed property allowlist. Log reads bind the exact unit in
helper policy and fixed journal fields; caller input cannot become a predicate
or command option.

For mutation, the acceptance boundary is the first point at which systemd may
have accepted the job. Before that boundary a denial or local failure is
definitively not executed. After that boundary, loss of job/result proof is
uncertain and must never cause automatic replay.

Post-action verification is mandatory:

- `start` verifies the configured active expectation;
- `stop` verifies the configured inactive expectation;
- `restart` verifies both the active expectation and a manager observation
  proving a new activation relative to the pre-action snapshot;
- `reload` requires manager proof that the exact reload job completed and a
  fresh status snapshot;
- `enable` and `disable` verify the resulting enablement state without
  changing running state implicitly.

A command exit code or current active state alone is insufficient proof of a
restart. If the configured unit cannot supply the required verification
signal, that action is rejected at config validation or returns explicit
unknown after acceptance.

No action performs daemon reload, reset-failed, kill, signal, mask, unmask,
revert, edit, link, preset, isolate, dependency traversal, package management,
or arbitrary job mode.

## Privileged Service Helper

P3 adds one Linux-only `mintclaw-node-service-helper` process. It is separate
from both `mintclaw-node-file-helper` and `mintclaw-node-broker`:

- the file helper continues to reject service actions;
- the owner-shell broker continues to accept only its P1 shell and terminal
  protocol; and
- the service helper accepts only P3 snapshot, status, logs, action, and
  cancel messages.

The helper is installed, owned, configured, and started by root. It listens
only on a root-owned local IPC endpoint and validates the exact unprivileged
companion peer identity and lifecycle boundary using the already proven Linux
peer-credential and cgroup pattern.

The helper's root-owned config binds:

- one enabled service profile and revision;
- exact alias-to-unit mappings;
- allowed reads and actions;
- verification expectations and log ceilings;
- manager/executable identity;
- companion UID, GID, cgroup, and IPC endpoint; and
- a bounded safe snapshot used for authenticated catalog projection.

Every request binds request identity, command, profile alias and revision,
service alias, action or log bounds, helper snapshot identity, and expiry. The
helper revalidates current config and peer authority before manager access.

P3 may reuse small private framing, strict-decoding, config-protection, and
peer-validation helpers where that reduces duplicated security code. It must
not introduce a public generic privileged-operation protocol, arbitrary action
registry, shared root daemon, plugin API, or compatibility layer.

The helper does not need a second durable invocation ledger. The existing
companion ledger owns invocation identity and terminal recovery. Once the
helper crosses the manager acceptance boundary, response loss becomes
explicit unknown and neither companion nor gateway retries it automatically.

## Approval, Cancellation, Recovery, And Races

Status and logs use command risk `read`. Actions use `privileged`. Hooks may
require stricter approval but cannot reduce the descriptor risk or broaden
node/helper policy.

Cancellation semantics are:

- before helper or manager acceptance, cancellation prevents the operation;
- a status or log read may be interrupted and returns canceled;
- after mutation acceptance, cancellation is advisory and never triggers an
  inverse action;
- if systemd supplies definitive job cancellation proof, the result may be
  canceled; otherwise it is completed, failed, or unknown from observed
  evidence; and
- repeated cancellation and status queries remain actor-scoped and
  idempotent through existing node invocation APIs.

The existing prepared-to-dispatched gateway boundary is unchanged. A
disconnected node with a dispatched service invocation reports recovered
terminal state or explicit unknown, never failed-safe-to-retry. Completed and
uncertain actions have no automatic replay path across reconnect, restart,
approval continuation, status recovery, or duplicate delivery.

Deterministic tests cover races among approval, expiry, policy change,
cancellation, manager acceptance, job completion, verification, response
loss, companion restart, helper restart, node disconnect, gateway restart,
status recovery, and duplicate requests.

## Redaction, Audit, And Retention

P3 uses the existing typed runtime-event and diagnostic-trace infrastructure.
It creates no audit database or event subsystem.

Passive observations may contain:

- target and model-safe service alias;
- command and allowlisted action;
- prepared, dispatched, accepted, verifying, completed, failed, canceled, or
  uncertain lifecycle state;
- bounded status codes, durations, requested log counts, returned counts, and
  truncation flags; and
- existing opaque invocation, turn, and trace correlation.

They must not contain service log messages, raw unit names, command output,
manager flags, helper paths, environment, credentials, approval answers,
systemd connection details, or unrestricted status properties.

Approval text may display target alias, service alias, action, blast-radius
description, and post-action expectation. It never includes log content,
raw unit name, helper details, or hidden policy.

No new log-content store is added. Service log content lives only in the
bounded authorized tool result and normal routed conversation retention.
Invocation metadata uses existing bounded ledger retention.

## Configuration, Defaults, And Migration

All P3 fields are optional and absent by default. A fresh install, existing
node config, Darwin node, missing helper, disabled profile, missing target
binding, missing pairing command approval, or stale helper snapshot advertises
no usable service authority.

Configuration loading uses strict unknown-field rejection and hard size
limits. The helper config must be root-owned and not writable by the companion
or its groups. Invalid or partially configured authority fails startup rather
than silently narrowing into a misleading catalog.

P3 uses the current protocol contract directly. There are no deployed P3
clients requiring compatibility, so implementation adds no v2 schema,
migration shim, legacy service command, dual helper path, or fallback to
`system.exec.v1`.

## Real-Process Evidence

The real-process vertical slice uses a deterministic/mock LLM and actual
gateway, WSS node connection, unprivileged companion, local root helper
boundary, and Linux systemd canary unit:

```text
model request
  -> nodes describe
  -> agent target/service-profile policy
  -> durable human approval for mutation
  -> existing gateway invocation coordinator
  -> WSS dispatch
  -> companion policy
  -> authenticated local service helper
  -> systemd canary unit
  -> verified result or explicit unknown
  -> nodes_status/model-visible result
```

The fixture emits unique non-secret bounded log markers. Evidence records only
counts, hashes when useful, state transitions, and redacted identifiers, not
raw log text or credentials.

The deployment canary proves status, bounded logs, one approved restart, one
denied action, stale revision, wrong actor/route, disconnect after acceptance,
restart recovery without replay, helper-peer denial, post-action verification,
redaction, and rollback. It never uses an essential connectivity or
control-plane service for the mutation drill.

## Dependency-Ordered Delivery

Implementation proceeded in focused non-stacked PRs from latest `origin/main`:

1. **Typed service domain and discovery.** Add bounded service policy and
   descriptor projection, schemas, default-deny config, exact target-profile
   intersection, and a Linux service-manager interface. Do not advertise a
   command before an enforcement source exists.
2. **Read-only Linux systemd runtime.** Implement bounded status and historical
   logs through the system manager using only the companion account's existing
   OS read permissions, with strict alias resolution, output bounds,
   cancellation, redaction, and real-process coverage. No mutation authority
   and no privilege escalation.
3. **Privileged Linux helper and actions.** Add the separate root service
   helper, exact system-manager operations, approval-bound actions,
   post-action verification, unknown/no-replay semantics, and helper boundary
   tests. Do not change file-helper or owner-shell protocols.
4. **Model-to-systemd vertical slice.** Complete existing generic invocation,
   status/cancel, target-profile projection, real-process WSS E2E, runtime
   events, and deterministic lifecycle/race fixtures. Add no dedicated model
   tool unless concrete evidence proves the generic surface insufficient and
   the operator approves that scope change.
5. **Operations and deployment evidence.** Add config/runbook documentation,
   requirement matrix, deny-by-default rollout, reversible canary unit,
   backups, rollback rehearsal, redaction scan, merged-main validation, and
   the final evidence record.

The production redaction canary found one `nodes_invoke` result preview that
retained service-log content. The required fail-closed correction merged as a
small security PR before this closeout record. It added no store, protocol,
helper, execution path, or other architecture. The checkpoint and final proof
are recorded in the deployment evidence above.

Do not add a prerequisite PR outside this sequence without an explicit
architecture checkpoint and operator approval. Prefer deleting or narrowing
scope over adding another abstraction.

## Exact Definition Of Done

P3 is complete only when every gate below has authoritative evidence.

### Gate 1: disabled and bounded authority

- fresh, existing, Darwin, delegated, and disabled configurations expose no
  service authority by default;
- the model cannot select or broaden profile, manager, unit, helper, OS user,
  executable, flags, environment, approval mode, verification, or limits; and
- profile, helper, catalog, pairing, target, and approval changes invalidate
  stale discovery and prepared work before acceptance.

### Gate 2: ownership and discovery intersection

- workspace, agent, actor, route, session, turn/tool call, execution identity,
  target, profile, command, service, and action are bound;
- another value in each dimension is denied independently;
- discovery exposes only the effective target-profile/helper intersection and
  valid alias/action pairs; and
- delegation receives no implicit service authority.

### Gate 3: status and log safety

- status returns only the fixed bounded projection;
- logs are restricted to the exact configured unit, time window, entry count,
  record size, and total bytes;
- malformed values, predicates, flags, raw units, neighboring units, binary
  fields, and unbounded output fail closed; and
- service messages reach only the authorized model result, never passive
  logs, events, traces, approval text, or evidence.

### Gate 4: exact action and approval binding

- only configured alias/action pairs can reach systemd;
- approval binds the exact retained plan and cannot be changed, expired,
  revoked, replayed, or self-authored;
- target-scoped bypass works only through operator configuration and cannot be
  requested by model input; and
- enable/disable do not imply running-state changes.

### Gate 5: helper and systemd boundary

- the companion remains unprivileged and the service helper is root-owned,
  local-only, peer-authenticated, cgroup-bound, profile-bound, and strict;
- file helper and owner-shell broker continue to reject service messages;
- shell, argv, arbitrary units/flags/environment, general D-Bus, and unsupported
  systemd operations cannot cross the helper boundary; and
- missing, altered, stale, wrong-peer, wrong-profile, or oversized requests
  fail closed before manager access.

### Gate 6: post-action truth

- start, stop, restart, reload, enable, and disable are exposed only where the
  configured unit can meet their exact verification rule;
- successful mutation includes fresh post-action verification;
- restart proves a new activation rather than merely observing `active`; and
- loss of proof after manager acceptance produces explicit unknown, never a
  false success or safe-to-retry failure.

### Gate 7: cancellation, restart, and no replay

- pre-acceptance cancellation prevents the operation;
- post-acceptance cancellation never performs an inverse action;
- deterministic races cover acceptance, cancel, completion, verification,
  disconnect, helper/node/gateway restart, duplicate request, and recovery;
- `nodes_status` recovers the durable result or explicit unknown; and
- no completed or uncertain mutation has an automatic replay path.

### Gate 8: platform and observability truth

- Linux systemd behavior is covered by unit, integration, race, and
  real-process tests;
- Darwin and every unsupported manager advertise no P3 service command;
- typed events and traces contain lifecycle metadata without service output or
  hidden authority; and
- no new audit database, service-log store, or retention subsystem exists.

### Gate 9: real operator vertical slice

- a model discovers a configured alias, reads status and bounded logs, requests
  an approved action, and receives verified post-action state through the real
  gateway/WSS/companion/helper/systemd path;
- wrong actor/route/action and stale authority are denied;
- one deterministic disconnect yields recovered terminal state or unknown
  without replay; and
- deployment uses a reversible non-essential canary service.

### Gate 10: merge, deployment, and rollback

- focused tests, relevant race tests, formatting/lint, CI broad tests, and
  real-process E2E pass without weakening gates;
- every in-scope PR is reviewed, green, authorized, and confirmed merged;
- docs and configuration match implemented behavior and limitations;
- latest merged `main` builds the recorded gateway, companion, and helper;
- canary-first deployment records checksums, backups, health, pairing,
  persistence, journals, no replay, and enabled authority; and
- disabling the exact profile and restoring artifacts/config/unit state is
  rehearsed successfully.

## Mandatory Completion And Stop Condition

When Gates 1 through 10 all have authoritative evidence, every in-scope PR is
confirmed merged, and deployment verification is recorded, P3 is complete.

At that exact point the implementing agent must:

1. mark the P3 goal complete;
2. stop implementation, review polling, deployment changes, refactoring, and
   roadmap expansion;
3. return one concise report listing PRs, merge commits, validation,
   deployment evidence, rollback, enabled authority, and residual limits; and
4. defer every additional idea without opening another PR.

Completion does not authorize P4, macOS launchd administration, service-log
streaming, arbitrary manager APIs, package management, daemon reload,
dependency orchestration, another helper family, or optional cleanup. No “one
more prerequisite” may continue under the completed P3 goal.

## Non-Goals And Stop Conditions

P3 explicitly excludes:

- macOS launchd and Windows service administration;
- arbitrary unit names, templates, instances, globs, manager scopes, flags,
  properties, D-Bus methods, journal predicates, shell, or argv;
- live log following, log export, search language, cursor persistence, or a
  service-log store;
- daemon reload, reset-failed, kill, signals, mask/unmask, preset, isolate,
  dependency operations, unit editing, drop-ins, and environment editing;
- package installation/update, host reboot/shutdown, scheduler, containers,
  process management, monitoring, alerts, and fleet orchestration;
- restarting the active connectivity or control-plane service as canary proof;
- a shared generic privileged-operation daemon or plugin protocol; and
- refactors not required by the P3 vertical slice.

Stop the affected PR and perform an architecture checkpoint if implementation
requires a new gateway state machine/store/transport, a generic privileged
action registry, a file-helper or owner-shell protocol expansion, automatic
mutation replay, service content in passive diagnostics, a second service
manager, or more than the admitted PR sequence. A product decision that changes
these boundaries requires explicit operator approval before work continues.
