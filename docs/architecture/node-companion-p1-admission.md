# Node Companion P1 Owner-Control Admission

## Status And Decision

P1 implementation is admitted as a dependency-ordered sequence under this
contract. Production enablement is not admitted yet.

The admitted implementation may add:

- an actor-scoped `nodes_cancel` adapter over the existing cancellation path;
- a separate non-interactive `shell.exec.v1` capability;
- operator-owned shell profiles that are absent and disabled by default;
- a minimal local authority broker for Linux profiles that select another OS
  identity, including UID 0;
- a live interactive terminal protocol with bounded open, input, resize,
  signal, status, output, and close operations; and
- focused owner-control model and operator interfaces.

Production enablement remains blocked until an operator separately configures
a trusted approval hook, approves the exact owner route, target, and profile,
and completes the deployment gates in this document. This admission does not
enable a profile, create root authority, change a running companion, or treat
the current production deployment as approval for owner mode.

Any implementation finding that requires file transfer, service
administration, detached terminals, a second generic invocation store, a
second capability registry, or a general workflow engine stops the affected
PR and returns to roadmap admission.

## Concrete Owner Use Case

The initial consumer is one operator's existing personal Linux VPN server,
already running an outbound paired MintClaw companion. The repository uses
model-safe names for the future authority:

- actor: `owner`;
- agent: `main`;
- authenticated route: the owner's direct MintClaw route;
- target: `personal-vpn`;
- profile: `owner-root`.

Those names describe bindings, not credentials or deployment topology. The
real target binding and actor authentication remain out-of-band configuration.

The owner needs to:

1. run ordinary administrative shell snippets with pipelines, redirects,
   variables, conditionals, and shell exit status;
2. open a terminal for interactive programs;
3. stop a non-interactive invocation without replaying either the invocation
   or its cancellation; and
4. use UID 0 only through the exact configured profile.

The following remain denied even if they know the target or profile alias:

- another actor, agent, route, routed session, or target;
- a delegated subagent or background task without an explicit independent
  grant;
- a model that invents a profile, shell path, UID, environment override,
  approval mode, broker address, or transport field;
- any fresh installation or configuration without owner mode explicitly
  enabled; and
- the same owner route after the target grant, profile grant, pairing,
  approval, catalog, or node policy is revoked or changed.

Actor, agent, route, session, workspace, target, and profile are all part of
the authority intersection. Equality in one dimension never substitutes for a
missing grant in another.

## Security Truth And Threat Model

An owner shell is equivalent in impact to SSH access as its configured OS
user. A UID 0 shell can read credentials, replace MintClaw binaries and
configuration, change audit sources, open network connections, install
persistence, and destroy data.

Shell parsing, command scanning, prompt instructions, approval wording, and
redaction do not sandbox arbitrary shell text. They improve usability and
operator control but do not narrow the authority of the selected OS identity.

### Protected assets

P1 must protect:

- pairing keys, channel credentials, model credentials, and approval secrets;
- node policy, owner-profile configuration, and authority-broker
  configuration;
- other agents, routes, targets, profiles, and terminal sessions;
- invocation and cancellation durability;
- audit integrity and the absence of raw shell or terminal content in passive
  evidence; and
- delegated/product-mode defaults.

### Adversaries and failures

The design assumes:

- a model or prompt may be malicious or confused;
- an untrusted chat participant may claim to be the owner;
- terminal output may contain prompt injection, control sequences, secrets,
  or arbitrary binary bytes;
- the network may disconnect after dispatch or after an input write;
- completion, cancellation, timeout, process exit, and disconnect may race;
- the unprivileged companion may be compromised;
- the authority broker is a high-value local attack surface; and
- the owner may deliberately choose a profile whose blast radius is the
  complete host.

The design does not claim to protect the host from a correctly authorized UID
0 shell. It protects the decision to enter that authority and keeps it from
spreading to other actors or default profiles.

### Trust boundaries

Authority continues to be the intersection of:

1. authenticated actor and route ownership;
2. the selected agent and routed session;
3. the agent's target and owner-profile grants;
4. durable pairing and approved capability catalog;
5. current authenticated node policy and profile revision;
6. trusted approval policy;
7. the exact prepared invocation or terminal-open plan; and
8. final node-local and OS enforcement.

The model-visible contract is advisory. The gateway and node recompute the
intersection before dispatch. Possessing a target name, profile alias,
discovery revision, invocation ID, or terminal ID grants nothing by itself.

### Sources of truth

| Decision | Authoritative source |
| --- | --- |
| Actor and route ownership | Existing authenticated inbound identity and routed session |
| Agent access to target and profile alias | Effective gateway agent target policy |
| Pairing and approved capability surface | Existing durable node registration and catalog approval |
| Unprivileged profile authority | Companion service account plus node-local companion policy |
| Root profile authority | Root-owned authority-broker policy |
| Model-visible profile contract | Authenticated safe projection of the current effective node/broker profile |
| Human approval | Existing trusted approval hook and durable interaction record |
| Non-interactive dispatch and outcome | Existing gateway invocation store and companion ledger |
| Terminal live ownership | P1 terminal coordinator plus broker-owned PTY/process tree |
| Terminal terminal metadata | Bounded P1-specific terminal metadata store, never a generic workflow store |

The gateway grant can only narrow a node or broker profile. Companion
configuration cannot create or broaden a root profile, and broker policy
cannot grant a profile to an agent.

## Authority Process Decision

P1 chooses a minimal local authority broker for Linux profiles that select an
OS user different from the companion service account. The complete companion
does not run as root.

The broker is selected because it keeps WebSocket, catalog, model-contract,
session-routing, and general companion parsers outside the root process. It
does not make root shell authority narrow: compromise of a companion that is
already granted `owner-root` can request arbitrary root shell behavior through
the broker.

The broker boundary must have all of these properties:

- installed, owned, and configured by root outside companion-writable paths;
- reachable only over a local root-owned IPC endpoint;
- peer credentials checked against the configured companion service account;
- a small versioned request surface for profile execution and terminal
  lifecycle, not arbitrary helper methods;
- profile aliases resolved only from root-owned broker configuration;
- no caller-supplied UID, GID, shell path, environment policy, root flag,
  executable path, approval mode, or IPC destination;
- requests bound to the node-authenticated profile revision and a prepared
  execution or terminal-open identity;
- bounded request and response frames;
- a broker-owned cgroup or equivalent execution domain that arbitrary shell
  descendants cannot leave and whose empty state proves termination;
- no network listener, remote enrollment, general file API, service-manager
  API, updater, scheduler, or credential store; and
- broker logs subject to the same metadata-only redaction contract.

All P1 shell and terminal profiles, including profiles that ultimately run as
the companion's own unprivileged account, use the broker-owned execution
domain. A same-UID shell can create a new session or process group, so direct
companion execution cannot prove arbitrary descendant termination. The broker
may select the unprivileged companion identity for a profile, but the
root-owned containment domain and profile policy remain outside that identity's
write authority.

A root-run companion remains an explicit rejected alternative for P1. It would
reduce one IPC boundary, but it would place the full remote protocol and
companion feature surface inside UID 0. Reconsidering that choice requires a
new admission decision, not a review-time implementation shortcut.

## Operator-Owned Profile Schema

Owner profiles exist only in out-of-band node-local configuration. There is no
generated default profile and no gateway API or model tool that creates or
broadens one. A profile using `authority_broker` is defined in root-owned
broker configuration; companion configuration may reference its alias and
safe projection but cannot supply or override its OS authority. A profile
using the companion service account is defined in node-local companion policy.

The architecture-level shape below represents the root-owned broker policy
plus the companion's matching safe profile reference:

```json
{
  "owner_shell": {
    "enabled": false,
    "revision": "owner-shell-v1",
    "profiles": {
      "owner-root": {
        "executor": "authority_broker",
        "shell": {
          "path": "/bin/bash",
          "login": false
        },
        "identity": {
          "uid": 0,
          "gid": 0,
          "supplementary_groups": []
        },
        "working_roots": ["/"],
        "initial_directory": "/",
        "fixed_environment": {
          "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
        },
        "permitted_environment_names": [],
        "network": "inherit",
        "approval": {
          "shell_exec": "each_command",
          "terminal_open": "session_start"
        },
        "limits": {
          "command_bytes": 65536,
          "timeout_seconds": 900,
          "output_bytes": 131072,
          "concurrent_commands": 2,
          "concurrent_terminals": 1,
          "terminal_idle_seconds": 900,
          "terminal_lifetime_seconds": 28800,
          "terminal_buffer_bytes": 1048576
        }
      }
    }
  }
}
```

This is a shape contract, not a production configuration recommendation. The
implementation PR fixes exact Go types, validation, and migration behavior
without changing these authority semantics.

For a broker profile, the broker returns a bounded safe projection and
revision to the companion over local authenticated IPC. The companion binds
that projection into its catalog identity proof. The broker still revalidates
the profile and revision on every execution or terminal open. A companion
cannot turn a mismatched or missing broker profile into usable root authority.

### Validation and defaults

- `owner_shell.enabled` defaults to `false`.
- Missing `owner_shell`, an empty profile map, or an unknown profile grants no
  shell or terminal capability.
- Profile and revision aliases use bounded identifiers and reject duplicate or
  case-colliding names.
- Shell paths, identities, working roots, fixed environment values, and broker
  selection are node-local and never copied into model discovery.
- `uid: 0` is accepted only with `executor: authority_broker`.
- Environment values are fixed out of band. A model may supply values only
  for names explicitly listed by the profile; the initial root profile lists
  none.
- `network: inherit` states the truth that P1 adds no network sandbox. Future
  network isolation requires its own admitted executor.
- Limits may be narrowed per profile but cannot exceed the hard ceilings in
  this document.
- An authority-bearing profile change updates the broker revision and safe
  projection, then updates the authenticated catalog/model contract,
  invalidates stale discovery, and requires the existing catalog reapproval
  behavior.

### Gateway grants

The gateway target policy may grant a bounded set of owner-profile aliases to
an agent. A profile is usable only when both the gateway grant and current
node-local profile exist. Gateway configuration cannot synthesize a profile or
broaden its OS authority.

Delegation does not inherit owner profiles. A subagent needs an explicit
independent actor/agent/route/target/profile grant; the initial P1
configuration grants none.

## Model-Visible Contract

P0 discovery is extended, not replaced. An available shell or terminal
contract may reveal:

- the approved capability name;
- the operator-owned profile alias and model-safe label;
- `risk: privileged`;
- shell dialect or feature flags such as pipelines and redirects;
- visible working-scope aliases;
- permitted environment names, never values;
- effective timeout, output, concurrency, idle, and lifetime ceilings;
- approval mode as policy metadata; and
- cancellation or terminal-control support.

It must not reveal:

- UID, GID, group membership, root flags, shell paths, broker paths, or IPC
  endpoints;
- raw working paths unless the operator deliberately uses one as the visible
  alias;
- fixed environment values;
- credentials, pairing identity, policy documents, catalog hashes, plan
  hashes, or broker authentication material; or
- another actor's targets, profiles, invocations, or terminal sessions.

The P0 discovery revision includes the effective profile grant, authenticated
profile revision, descriptor/model contract, executor selection, and approval
mode. A relevant change returns `DISCOVERY_STALE` before approval or dispatch.

## Non-Interactive `shell.exec.v1`

`shell.exec.v1` is a new privileged typed command over the existing durable
invocation path. `system.exec.v1` remains direct argv execution and never gains
implicit `sh -c` behavior.

The model-safe input is:

```json
{
  "profile": "owner-root",
  "script": "pipeline or shell snippet",
  "cwd": "owner-workspace",
  "env": {},
  "timeout_seconds": 300
}
```

Only `profile`, `script`, visible working-scope alias, permitted environment
names, and bounded timeout are model inputs. Shell path, login mode, OS
identity, fixed environment, network policy, executor, and approval mode come
from the profile.

The command:

1. resolves the profile through the current agent target grant and
   authenticated node policy;
2. binds the exact script digest and effective profile revision into the
   existing execution plan;
3. completes the configured trusted approval flow;
4. dispatches once through the existing gateway coordinator;
5. executes the script as one shell program under the selected profile;
6. records bounded stdout, stderr, exit status, signal, truncation, and timing
   in the existing companion ledger; and
7. returns or recovers the durable outcome through `nodes_status`.

The authorized agent session necessarily contains the model-authored script
and any permitted environment values as tool arguments. Bounded stdout and
stderr may be returned to that session under the existing tool-result policy.
P1 does not copy those fields into passive events, diagnostic traces, ordinary
logs, or the operational evidence record.

### Hard limits

Initial P1 hard ceilings are:

- script: 64 KiB UTF-8;
- supplied environment: 64 names, 8 KiB total, subject to the profile
  allowlist;
- timeout: 3,600 seconds;
- captured stdout plus stderr: 128 KiB, leaving room for worst-case JSON
  escaping inside the existing 1 MiB invocation frame;
- concurrent non-interactive commands per profile: 8; and
- existing invocation and catalog frame limits remain unchanged.

Profiles default lower. Limit overflow fails closed before dispatch or
terminates capture with an explicit truncation marker; it never silently
creates a larger authority.

### Commit and replay boundary

The existing dispatch boundary remains the commit boundary. A request is
replayable only before dispatch. After dispatch, a disconnect yields durable
status recovery or an explicit unknown outcome. Neither a successful,
failed, timed-out, canceled, nor uncertain shell mutation is automatically
replayed.

## `nodes_cancel`

`nodes_cancel` is a model-facing adapter over the existing gateway
coordinator, invocation store, companion cancellation request, and companion
ledger. It creates no new invocation or workflow store.

Input contains only the `invocation_id` returned by `nodes_invoke`. The target,
command, profile, actor, agent, route, routed session, workspace, and original
tool-call ownership come from the retained invocation record.

Cancellation is authorized only when all original ownership dimensions still
match and current policy still permits the actor to observe that invocation.
Knowing or guessing an invocation ID is insufficient.

The bounded result vocabulary is:

- `cancel_requested`: the request was durably accepted but termination is not
  yet proven;
- `canceled`: the companion proved process-tree termination;
- `already_terminal`: the original succeeded, failed, timed out, or was
  already canceled, and its terminal state is unchanged;
- `unknown`: transport or restart prevents proof; and
- `denied`: ownership or current authority does not permit cancellation.

The adapter is idempotent. Duplicate requests reuse the same durable
cancellation timestamp and do not send multiple logical effects. Completion
wins if it became durable before cancellation. `canceled` is emitted only
after the companion or broker proves the owned process tree is gone.

After an ambiguous disconnect or restart, `nodes_status` remains the recovery
surface. The gateway does not replay the original invocation or cancellation.
An implementation that cannot prove process-tree termination returns
`cancel_requested` or `unknown`, never a fabricated `canceled`.

Cancellation narrows an already granted effect and does not itself require a
second human approval in the initial P1 policy. Only the same original owner
scope can request it.

## Interactive Terminal Protocol

An interactive terminal is a live session, not a synchronous
`system.exec.v1` or `shell.exec.v1` invocation. The first release is attached
only: no detach, reconnect, terminal sharing, background ownership transfer,
or replayed input.

### Operations

The versioned session protocol defines:

- `terminal.open`: prepare and approve a profile-bound terminal, then return
  an opaque session ID only after the broker owns the process and PTY;
- `terminal.input`: write one ordered bounded byte frame;
- `terminal.resize`: apply bounded rows and columns;
- `terminal.signal`: request one enumerated signal;
- `terminal.output`: deliver ordered bounded output frames with cursors;
- `terminal.status`: report live or terminal metadata;
- `terminal.close`: request process-tree termination and PTY closure; and
- terminal lifecycle events for opened, output-available, exit,
  cancel-requested, closed, and unknown.

The first P1 model surface may present focused `nodes_terminal` actions for
discovery, open, status, enumerated signal, and close. Raw input, output, and
resize are carried only by an attached authenticated operator terminal client,
outside the agent message bus and model session history. Non-interactive model
automation uses `shell.exec.v1`.

The operator client and any model actions remain adapters over the same
session operations. They do not become generic transport or broker tools.

### Session authority

Every operation is bound to the actor, agent, authenticated route, routed
session, workspace, target, profile, terminal ID, and terminal-open plan.
Terminal IDs are random correlation values, not bearer authority. Another
route or session cannot observe, write, resize, signal, or close the terminal.

The open plan binds current target grant, catalog approval, authenticated
profile revision, effective limits, initial working-scope alias, terminal size,
and approval mode. A stale plan fails before broker allocation.

### Framing and ordering

- Transport byte fields use bounded base64 framing because PTY output is
  arbitrary bytes. Base64 framing is not admitted as file transfer.
- Each input and control request has a session-local monotonic sequence and
  idempotency key.
- The terminal owner accepts each sequence at most once and rejects gaps or
  stale sequence numbers.
- Acknowledgement means the frame was accepted for the live PTY write; it does
  not prove application processing.
- An unacknowledged input frame after disconnect is not resent.
- Output frames carry monotonic cursors. Clients may acknowledge delivery for
  backpressure, but the first release cannot reconnect and resume a terminal.
- Terminal output is treated as untrusted binary content. UI renderers must
  use a terminal emulator boundary and must not copy control sequences into
  ordinary logs or chat formatting.
- Terminal input and output are never converted into ordinary agent messages
  or persisted tool results.

### Backpressure and limits

Initial hard ceilings are:

- input or output frame: 32 KiB decoded bytes;
- unread output buffer: 1 MiB per terminal;
- dimensions: 20–400 columns and 5–200 rows;
- concurrent terminals: 8 per node and 2 per profile;
- authenticated operator attach deadline: 30 seconds;
- idle lifetime: 3,600 seconds;
- maximum lifetime: 28,800 seconds; and
- terminal metadata retention: 30 days.

Profiles default to one terminal, 900 seconds idle, eight hours maximum, and a
1 MiB buffer. When the output buffer is full, the broker first applies OS
backpressure. It must not drop or reorder bytes while claiming a complete
stream. If bounded operation cannot continue, it terminates the process tree
and reports an explicit overflow outcome.

### Signals, exit, timeout, and disconnect

The first release allows only `INT`, `TERM`, `HUP`, `WINCH` through resize, and
`KILL` through close escalation. The model cannot supply arbitrary signal
numbers.

Signal acceptance is not termination proof. Terminal state becomes closed or
canceled only after process-tree termination is observed. A natural exit wins
over a later close and preserves its exit status.

Idle timeout, maximum lifetime, explicit close, gateway loss, companion loss,
or owner-route disconnect triggers process-tree termination. The first release
does not leave a detached terminal running. If termination cannot be proven,
the durable terminal metadata records `unknown`.

If no operator client with the same authenticated owner route attaches before
the 30-second deadline, the terminal owner terminates the process tree and
records the open as expired.

Process-tree ownership must use a platform primitive that can be tested:
process groups or a cgroup on Linux, with the broker retaining the authority
needed to enumerate and terminate descendants. Timing-only assumptions or
killing only the shell leader are insufficient.

## Approval Policy

Approval mode is operator-owned profile configuration. A model cannot select
or relax it.

Supported architecture values are:

- `each_command` for every non-interactive shell invocation;
- `session_start` once for a terminal-open plan, after which input, resize,
  signal, status, and close remain bound to that approved terminal; and
- `none` only for a deliberately configured owner profile.

The initial production root profile is required to use:

- `shell_exec: each_command`; and
- `terminal_open: session_start`.

The current deployment has no trusted approval hook, so P1 production
enablement is blocked. A later deployment PR must configure and validate a
trusted hook out of band before enabling the profile. The hook must identify
the authenticated approving owner; model-authored text, a chat claim, or a
direct untrusted reply cannot become approval authority.

The trusted per-command approval presentation transiently shows the exact
script to the authenticated owner from the bound plan; otherwise
`each_command` would not be an informed approval. The script is never supplied
by model-authored approval prose. The terminal-start presentation shows the
profile alias, privileged blast radius, initial scope, and effective limits.

Durable approval records contain a trusted summary, profile alias, risk,
effective limits, and plan correlation. They exclude raw script, environment
values, terminal input/output, shell path, UID, broker endpoint, and
credentials.

## Redaction, Audit, And Retention

Passive `shell.invocation.observed`, `node.invocation.observed`, and
`terminal.session.observed` events may contain:

- bounded actor/agent/session correlations;
- target and profile aliases already visible to that actor;
- capability, risk, lifecycle state, denial code, timing, byte counts,
  truncation, and opaque revision digest; and
- cancellation requested/confirmed and process-tree termination proof state.

They exclude:

- raw script, command digest, terminal input/output, transcript, stdout,
  stderr, and environment names or values;
- raw node identity, public key, endpoint, broker path, IPC credentials, shell
  path, UID, GID, fixed environment, and hidden working paths;
- full policy, catalog, descriptor, plan, or authority hashes; and
- approval secrets or model-authored approval presentation.

Ordinary logs follow the same exclusion. Diagnostic traces may correlate
model tool calls with lifecycle events but must not store raw shell or terminal
content. Operational evidence uses only redacted correlations and outcome
metadata.

Terminal transcripts are not retained in P1. Live terminal bytes remain
outside model session persistence. Adding encrypted opt-in transcripts,
model-visible terminal bytes, search, export, or replay requires a separate
retention and access control admission.

Terminal metadata and passive events have a configurable retention capped at
30 days. Existing invocation retention remains authoritative for
non-interactive shell and cancellation.

## Real-Process Evidence

Tests use real gateway, WebSocket admission, companion, executor or broker
test process, shell, and PTY boundaries. Scripted fixtures may choose model
tool calls only after model-visible discovery; they cannot inject hidden shell
paths, UIDs, broker fields, profile internals, or revisions.

The required vertical evidence is:

1. an authorized outcome-only model prompt discovers one owner profile;
2. a trusted approval interaction is completed;
3. `shell.exec.v1` proves pipelines, redirects, variables, conditionals,
   stderr, exit status, timeout, truncation, and no replay;
4. a UID-reporting broker fixture proves the selected root profile executes as
   UID 0 without recording the raw command or output;
5. another actor, agent, route, target, and profile each fail before approval
   or dispatch;
6. a long-running shell invocation traverses
   `nodes_cancel` → `cancel_requested` → proven `canceled` and recovers through
   `nodes_status`;
7. deterministic barriers cover completion-versus-cancel and
   disconnect-versus-cancel races without sleep-only assertions;
8. a scripted model opens a PTY and an authenticated operator test client
   covers input ordering, output cursors, resize, enumerated signal, exit,
   idle timeout, maximum lifetime, backpressure overflow, disconnect
   termination, descendant process cleanup, and absence from model session
   history;
9. stale target, catalog, profile, approval, and node-policy revisions fail
   before approval or dispatch; and
10. passive event and trace assertions prove the redaction contract.

Production proof is permitted only after all implementation PRs merge and the
trusted approval prerequisite is satisfied. It uses one explicitly enabled
personal-server profile, a reversible non-destructive shell command, one
terminal lifecycle, one cancellation, a denied actor, and a stale-profile
drill. Evidence records no raw shell or terminal content.

## Dependency-Ordered Delivery

Each item is a separate merge unit based on the latest merged `main`. Do not
start a dependent PR until its prerequisite is merged unless a temporary
stacked PR is required only to preserve useful progress.

1. **Admission contract:** this architecture-only PR.
2. **Non-interactive shell:** add the disabled profile schema and
   `shell.exec.v1` over the existing invocation path for the companion's OS
   user, including P0 discovery, cancel-capable process-tree ownership, and
   real-process shell semantics.
3. **Cancellation adapter:** expose actor-scoped `nodes_cancel` over the
   existing coordinator/store/ledger and the cancel-capable shell command;
   prove idempotency, races, restart recovery, and no replay.
4. **Authority broker:** add the minimal Linux local broker and consume it
   from `shell.exec.v1` for an explicit UID 0 test profile; do not add another
   remote protocol or general helper API.
5. **Terminal core:** add broker-backed live terminal sessions, framing,
   ordering, backpressure, process-tree containment, limits, and lifecycle
   tests without model UX.
6. **Owner UX and complete E2E:** add focused model/operator adapters and prove
   authorized and denied real-process flows, approval, cancellation, PTY, and
   redaction.
7. **Production enablement:** only after a separate operator decision,
   configure the trusted approval hook and exact profile, deploy canary-first,
   record evidence, and decide whether P2 is admissible.

No PR may quietly absorb a later item merely because its types are convenient.
A prerequisite abstraction must have the current item's concrete consumer and
tests in the same merge unit.

## Exact Completion Gates

P1 is complete only when every gate has authoritative evidence.

### Gate 1: disabled and bounded authority

- fresh installs and delegated/product profiles expose no shell, terminal,
  owner profile, or broker authority;
- profiles are out-of-band, operator-owned, schema-validated, revision-bound,
  and absent by default;
- the model cannot select shell path, UID, environment policy, executor,
  network policy, approval mode, or broker endpoint; and
- changing any authority-bearing grant or profile field invalidates stale
  discovery before approval or dispatch.

### Gate 2: ownership intersection

- only the configured actor, agent, route, routed session, workspace, target,
  and profile intersection can invoke or control owner mode;
- knowing aliases or opaque IDs does not grant access;
- delegation and another actor, agent, route, session, target, or profile are
  denied independently; and
- revocation prevents new work and does not transfer an existing terminal.

### Gate 3: shell correctness and durability

- ordinary shell syntax, stdout/stderr, exit, signal, timeout, and truncation
  behavior is deterministic;
- the existing invocation path owns dispatch and durable status;
- terminal and shell effects are never smuggled through `system.exec.v1`; and
- completed, failed, timed-out, canceled, and uncertain mutations have no
  automatic replay path.

### Gate 4: cancellation truth

- `nodes_cancel` is actor-scoped, idempotent, and uses existing durable state;
- deterministic tests cover cancel-versus-completion and disconnect/restart
  races;
- `canceled` requires proven process-tree termination; and
- `nodes_status` recovers terminal or unknown state without replay.

### Gate 5: terminal lifecycle

- open, input, output, resize, signal, status, close, exit, timeout,
  backpressure, and disconnect semantics are bounded and deterministic;
- input is ordered and accepted at most once, and ambiguous input is not
  resent;
- output and terminal controls cannot corrupt ordinary logs or UI; and
- close, timeout, and disconnect terminate the complete process tree or report
  unknown.

### Gate 6: root boundary

- the root broker has no remote listener or general helper surface;
- root-owned policy and peer credentials bind the companion account and
  profile;
- a configured profile proves UID 0 while the complete companion remains
  unprivileged; and
- unconfigured or altered broker/profile requests fail closed.

### Gate 7: approval and redaction

- approval is configured out of band and cannot be relaxed by a model;
- the initial production root profile uses per-command shell approval and
  terminal-start approval;
- passive events, traces, logs, approval records, and operational evidence
  contain no raw shell, terminal, environment, credential, hidden path, or
  authority material; and
- no terminal transcript is retained.

### Gate 8: merged and deployed evidence

- every dependent PR has required CI and review complete and is merged;
- gateway, companion, and broker are built from recorded reviewed revisions;
- real-process evidence covers authorized and denied actors, shell, root,
  cancellation, terminal lifecycle, stale authority, and redaction;
- production remains deny-by-default except one explicit owner profile;
- canary and final health, error journals, persistence, duplicate-effect
  checks, backups, rollback, and legacy audits pass; and
- operational evidence records residual limitations before P2 is admitted or
  deferred.

## Admission Checklist

| Requirement | Decision and evidence required by P1 |
| --- | --- |
| Concrete deployed use case | One operator's existing personal Linux VPN companion |
| Authenticated actors and profiles | `owner` + `main` + direct owner route + `personal-vpn` + `owner-root`; all other intersections denied |
| Minimum typed surface | `nodes_cancel`, `shell.exec.v1`, and bounded terminal session operations |
| Node-local and OS authority | Authenticated profile revision plus companion account or minimal local broker; UID 0 only through the broker |
| Commit, retry, cancellation, timeout, unknown | Existing invocation dispatch boundary; no replay; proven cancellation; attached PTY terminates or reports unknown |
| Data and concurrency limits | 64 KiB scripts, 128 KiB command output, 32 KiB terminal frames, 1 MiB terminal buffers, bounded commands/terminals/lifetimes |
| Retention and output | Existing invocation retention, terminal metadata at most 30 days, no transcripts |
| Approval and redaction | Out-of-band `each_command`/`session_start`; no raw shell or terminal content in passive evidence |
| Real-process test | Gateway → approval → companion/broker → shell/cancel/PTY plus denied actor and stale-profile drills |
| Explicit exclusions | File/service APIs, detach/reconnect, transcript retention, general broker APIs, root companion, automatic approval |
| Concrete consumer per foundation | Cancellation consumes existing invocation state and cancel-capable shell; broker is introduced with root shell; PTY core owns real terminal sessions |

The admission checklist is satisfied by this contract. Implementation is
admitted in the ordered sequence above. Production enablement remains deferred
until the trusted approval prerequisite and a separate operator decision are
recorded.

## Non-Goals And Stop Conditions

P1 does not implement or authorize:

- file upload/download, filesystem APIs, service management, companion update,
  package management, browser/MCP routing, or remote workspace routing;
- hiding shell, terminal, or file behavior inside `system.exec.v1`;
- automatic root profiles, profile discovery from the host, model-created
  profiles, or chat-claimed owner identity;
- a root-run complete companion;
- shell-text allowlists or scanners presented as a sandbox;
- detached, shared, reconnectable, or replayable terminals;
- transcript retention, export, search, or replay;
- terminal byte framing used as a file-transfer protocol;
- a second generic capability registry, invocation store, approval store,
  scheduler, workflow engine, or remote broker protocol;
- automatic approval, approval inferred from pairing, or approval modes
  selected by the model;
- arbitrary signals, kernel namespaces, containers, network sandboxes, or new
  executors;
- macOS or Windows root/PTY parity in the initial Linux vertical slice; or
- production deployment before the trusted approval hook and exact
  owner-profile decision exist.

Stop and return to admission if implementation requires any item above, if
process-tree termination cannot be proven, if terminal input must be replayed
after ambiguity, if raw content must enter passive evidence, or if the broker
grows beyond profile-bound shell and terminal lifecycle.

Green unit tests alone do not complete P1. A root shell that bypasses actor or
route binding does not complete P1. A PTY that can leave an unowned process
after disconnect does not complete P1. A production profile without trusted
out-of-band approval does not complete P1.
