# Node Companion Post-MVP Roadmap

## Status

Active post-MVP roadmap after the node companion MVP defined in
[`node-companion.md`](node-companion.md) is merged, validated from `main`, and
deployed. This roadmap does not expand that MVP or authorize implementation of
every item below.

P0, P1, P2, and P3 are complete. P2 deployment evidence is recorded in
[`node-companion-p2-deployment-evidence.md`](../operations/node-companion-p2-deployment-evidence.md).
P3 completion evidence is recorded in
[`node-companion-p3-deployment-evidence.md`](../operations/node-companion-p3-deployment-evidence.md).
P4 and later milestones remain unadmitted.

The local interactive client slice of the Future P1 follow-up is complete and
deployed, with evidence in
[`node-terminal-client-deployment-evidence.md`](../operations/node-terminal-client-deployment-evidence.md).
Browser terminal UI remains separate future work. Agent-operated PTY control
is not planned; agents use structured `shell.exec.v1` instead. Exact
target-scoped approval bypass is also complete and deployed under
[`node-target-approval-deployment-evidence.md`](../operations/node-target-approval-deployment-evidence.md);
it is shared approval infrastructure, not completion evidence for P3.

The roadmap is ordered by operator value and security dependencies rather than
calendar dates. Each milestone requires a fresh scope decision based on
evidence from the deployed preceding milestone.

## Starting Point

The roadmap assumes the MVP already provides:

- an outbound paired WSS companion with a slim dependency boundary;
- operator-owned target aliases and agent target policy;
- typed `node.info.v1`, `system.which.v1`, and `system.exec.v1` commands;
- node-local policy enforcement;
- durable invocation identity, no-blind-retry semantics, recovery, and explicit
  unknown outcomes;
- model-facing discovery, invocation, and status;
- durable human approval for sensitive execution;
- redacted audit events and a real-process model-to-companion test;
- Linux systemd and macOS LaunchAgent lifecycle support.

Missing MVP requirements are completed in the MVP program, not moved into this
roadmap.

## Roadmap Rules

Every post-MVP capability follows these rules:

1. **Authority is configured, not claimed.** A model argument or chat message
   cannot grant administrator, filesystem, service, network, or device access.
   Authority comes from authenticated actor routing, agent policy, target
   binding, gateway policy, node-local policy, and operating-system authority.
2. **The node remains the final enforcement boundary.** Gateway approval may
   narrow authority but cannot broaden the node's configured policy.
3. **Capabilities stay typed.** File transfer, service administration,
   updates, browser control, and hardware access do not become shell strings.
   An admitted owner shell is its own explicit capability, not the hidden
   implementation of every other capability.
4. **Placement and isolation remain separate.** Selecting a node does not imply
   a root account, container, sandbox, or unrestricted filesystem.
5. **Uncertain mutations are not replayed.** A disconnect after a commit or
   acceptance boundary produces recovery or an explicit unknown outcome.
6. **Observability is passive.** Audit and diagnostics never acknowledge,
   advance, retry, or retain authoritative workflow state.
7. **Each milestone proves one vertical slice.** Do not build general
   foundations without an admitted consumer and an end-to-end test.
8. **Review findings do not silently expand the roadmap.** A finding that
   requires another subsystem triggers a checkpoint and a new scope decision.
9. **Safe defaults do not prohibit explicit administration.** Default profiles
   are narrow, while an operator may deliberately configure broader authority,
   including filesystem root `/`, for a specifically authenticated actor and
   target.
10. **Usable authority is discoverable.** Before invoking a capability, the
    model receives a bounded, model-safe description of the effective
    operations and constraints available to it. Policy denial remains final
    enforcement, not the normal mechanism for discovering basic usage.

## Operating Modes

The companion supports two deliberate operating modes over the same pairing,
target, invocation, recovery, and audit foundations:

- **Owner-control mode** is for an operator's own server. An out-of-band
  configuration may authorize a specific actor, agent, and target to use an
  arbitrary shell as a configured OS user, including UID 0, and to open an
  interactive PTY. This is intentionally equivalent in authority to giving
  that identity SSH access as the configured user. It is disabled by default
  and cannot be enabled or broadened by a model argument or chat claim.
- **Delegated/product mode** is for shared, customer, production, or
  least-privilege deployments. It uses typed commands, unprivileged service
  accounts, narrow privileged helpers, allowlists, and human approval where
  appropriate. Its normal general-purpose execution surface is a constrained
  `system.exec.v1`: node policy bounds executable identities, arguments,
  working roots, environment, timeout, output, and OS user.

Owner-control mode does not replace typed capabilities. Typed operations remain
easier to validate, approve, retry, audit, and expose safely to constrained
agents. Conversely, typed capabilities must not make personal server ownership
unnecessarily awkward. Policy profiles select the intended trust model without
weakening the delegated default.

A product profile may expose `shell.exec.v1` when shell syntax is genuinely
needed, but shell-text allowlists or scanners do not make it constrained.
Its acceptable blast radius must instead be enforced by the configured OS user,
filesystem permissions, container or sandbox, resource and network policy, and
target binding. A product can therefore offer either restricted direct
execution or an isolated shell without inheriting an owner's root profile.

## Priority Overview

| Priority | Milestone | Operator outcome | Depends on |
| --- | --- | --- | --- |
| P0 | Model-visible capability contracts | Let an agent plan valid node calls from bounded discovery instead of guessing hidden policy | Deployed execution MVP |
| P1 | Owner-controlled shell and terminal | Operate a personal server through ordinary shell commands and an interactive PTY, including explicit root profiles | P0 and deployed execution MVP |
| P2 | File transfer and administrator filesystem access | Send files to a node, retrieve files and images, and manage explicitly authorized paths | P0, owner profiles, and deployed execution MVP |
| P3 | Typed service administration | Inspect logs/status and perform allowlisted service actions without broad shell authority | Privileged helper boundary proven by P2 |
| P4 | Fleet operations and companion updates | Diagnose, version, update, and roll back companion instances safely | Stable node lifecycle and artifacts |
| P5 | Additional executors and long-running work | Run contained builds/jobs without confusing placement with isolation | Stable invocation and artifact contracts |
| P6 | Bootstrap and alternative transports | Enroll hosts through SSH and support bounded static SSH targets | Stable target-driver contract |
| P7 | Interactive application capabilities | Add browser, MCP, camera, location, and other typed capabilities | Per-capability threat models |
| P8 | Remote workspace routing | Route an explicitly selected set of workspace-aware tools through one node execution context | P2, P5, and remote-capable P7 tools |
| P9 | Remaining platforms and compatibility adapters | Add Windows, iOS, constrained-device companions, and explicitly versioned external adapters | Stable internal contracts |
| Future P1 follow-up | Browser terminal client | Use the existing attached PTY from a browser without exposing a node port through NAT | Deployed P1 terminal core and trusted owner approval |
| Future operations follow-up | Authenticated live-agent and invocation smoke | Exercise the running gateway agent and durable node invocation path without Telegram or a second disconnected AgentLoop | Stable gateway operator authentication and deployed node execution |

Priorities express ordering, not a commitment to implement every milestone.

The local CLI portion of the Future P1 follow-up is complete. The browser
portion remains pending and is not implied by the CLI deployment.

## P0: Model-Visible Capability Contracts

P0 is complete under the fixed scope and completion gates in
[`node-companion-p0-contracts.md`](node-companion-p0-contracts.md). P1 is
complete under
[`node-companion-p1-admission.md`](node-companion-p1-admission.md). P2 is
complete under
[`node-companion-p2-admission.md`](node-companion-p2-admission.md), with its
merged and deployed proof recorded in
[`node-companion-p2-deployment-evidence.md`](../operations/node-companion-p2-deployment-evidence.md).
P3 is complete under
[`node-companion-p3-admission.md`](node-companion-p3-admission.md), with its
merged and deployed proof recorded in
[`node-companion-p3-deployment-evidence.md`](../operations/node-companion-p3-deployment-evidence.md).
P4 and later work remain unadmitted.

### Current limitation

The MVP enforces node authority correctly but exposes too little of that
authority for reliable model planning. `nodes describe` reports an approved
command name, risk, progress support, and cancellation support. For a command
such as `system.exec.v1`, it does not tell the model which executable
identities, working scopes, environment names, timeout ceiling, or output
ceiling are usable. The generic `nodes_invoke.input` object also does not carry
the selected command's typed input schema.

Consequently, a model can know that `system.exec.v1` exists without knowing how
to construct a call that the node will accept. A user-supplied prompt can fill
in the hidden details, and node-local policy safely denies guesses, but policy
denial is a poor capability-discovery interface.

### Operator outcome

An authorized agent can inspect a target and determine, before invocation:

- which approved typed commands are currently usable;
- the bounded input shape for each command;
- model-safe effective constraints needed to construct a valid request;
- whether human approval may be required;
- timeout, output, progress, and cancellation behavior;
- one or more bounded examples when the operator explicitly provides them.

The agent must not infer that a typed command grants unrestricted shell,
filesystem, environment, service, network, or device access.

### Model-visible contract

Discovery should expose the effective intersection of target grant, approved
catalog, and node-local policy, not the broad union of independently configured
authority. A command entry may include:

- its approved versioned name, risk, and bounded input schema;
- stable executable or action aliases rather than host paths where practical;
- operator-defined working-scope aliases and descriptions;
- permitted environment-name aliases or an explicit `environment: none`;
- effective timeout and output ceilings;
- progress, cancellation, and expected result metadata;
- operator-authored usage guidance and schema-valid examples.

Raw node IDs, credentials, transport endpoints, public keys, policy or catalog
hashes, unrestricted filesystem paths, environment values, and hidden policy
rules remain excluded. An operator may explicitly mark a path or service name
as model-visible when it is necessary input, but discovery must not expose
host details merely because enforcement knows them.

The response must distinguish:

- `available`: the model has enough current information to invoke;
- `partially_described`: the command is authorized but some constraint cannot
  yet be represented safely;
- `requires_reapproval`: catalog or policy authority changed;
- `unavailable`: the target or command cannot currently accept work.

`partially_described` must not be presented as unrestricted. It should explain
that an operator must provide missing model-safe guidance or that an attempted
call may still be denied.

### Authority and freshness

Discovery is advisory and never becomes an authority source. Invocation still
revalidates target policy, pairing approval, catalog authority, node-local
policy, actor ownership, and the exact prepared plan.

The model-visible contract must be derived from authoritative current state or
bound to an opaque revision that is rechecked during preparation. Cached
descriptions fail closed after relevant policy or catalog changes. Operator
examples and labels cannot broaden executable, path, environment, timeout, or
OS authority.

Structured denial remains useful for races and stale plans. It should identify
the violated model-visible constraint when safe, without leaking hidden policy
or encouraging trial-and-error enumeration.

### Suggested delivery sequence

1. Define the bounded discovery schema, redaction rules, effective-policy
   projection, freshness semantics, and explicit exclusions.
2. Expose approved command input schemas and generic execution ceilings without
   adding a new authority, store, protocol version, or invocation path.
3. Add operator-owned aliases, descriptions, and examples for constraints that
   should not expose raw host details.
4. Add structured, model-safe policy-denial results aligned with the discovery
   vocabulary.
5. Prove one real model flow where the prompt names only the desired outcome:
   the model discovers an executable and working scope, constructs a valid
   `nodes_invoke`, and observes the result through `nodes_status`.

The implementation should extend the existing `nodes` discovery surface. It
must not create a second capability registry or mirror the complete node policy
inside the gateway.

### Completion evidence

P0 is complete only when:

- a model can invoke a constrained `system.exec.v1` without the user supplying
  hidden executable, working-directory, environment, timeout, or schema facts;
- the model cannot discover or invoke a target or command outside its agent
  target policy and pairing grant;
- changing catalog or relevant policy authority invalidates stale discovery
  and fails closed before dispatch;
- discovery and denial expose no credential, environment value, raw transport
  detail, hidden path, or unrestricted policy document;
- an operator-authored label or example cannot broaden node-local authority;
- compact discovery remains bounded for large catalogs;
- a real-process end-to-end test proves discovery, invocation, durable result,
  and denial after a constraint change.

## P1: Owner-Controlled Shell And Interactive Terminal

P1 implementation is admitted with exact scope, authority decisions, delivery
order, stop conditions, and completion gates in
[`node-companion-p1-admission.md`](node-companion-p1-admission.md). This
admission does not enable production owner mode. Production remains
deny-by-default until the trusted approval and deployment gates in that
contract are satisfied.

### Operator outcome

After installing a companion on a personal VPS, an explicitly authorized owner
can:

- run an ordinary shell command or pasted shell snippet as a configured OS
  user, including root;
- use familiar pipelines, redirects, expansions, conditionals, and scripts;
- open an interactive PTY, send input and signals, resize it, and close it;
- choose through operator configuration whether authorization is required for
  every command, once per terminal session, or not at runtime.

This milestone is about full ownership of a deliberately selected node. It
does not claim that arbitrary root shell access is safe for untrusted agents.

### Separate execution surfaces

The existing `system.exec.v1` remains direct argv execution without shell
parsing. It is the preferred primitive for typed and constrained automation.
Shell behavior must not be smuggled through it with an implicit `sh -c`.

Non-interactive shell execution uses a separate typed command such as
`shell.exec.v1`. Its input is a command or bounded script evaluated by an
operator-selected shell profile, so pipes, redirects, globbing, variables, and
the syntax commonly shown in operating guides behave normally.

Interactive terminal access uses a session protocol rather than synchronous
`system.exec.v1` semantics. Its minimum operations are open, input, resize,
signal, status, and close. Terminal sessions bind to stable session IDs and the
same authenticated actor, agent, routed session, target, and policy profile
that opened them.

### Owner profiles and authority

An operator-defined shell profile selects:

- the shell executable and login or non-login behavior;
- OS user and group, including an explicit UID 0 profile;
- fixed and permitted environment, `PATH`, working roots, and initial
  directory;
- timeout, idle lifetime, maximum lifetime, output and concurrency limits;
- executor and network policy;
- approval mode such as each command, session start, or none.

The exact schema belongs in the milestone architecture PR. The model may select
only an alias already granted by target policy. It cannot supply a shell path,
UID, root flag, environment-policy override, approval mode, or helper endpoint.
Broad authority and approval-free operation are valid only when deliberately
written into operator-owned configuration.

### Security truth

Parsing or scanning arbitrary shell text is not a security boundary. A root
shell can read credentials, replace binaries and policy, alter audit sources,
open network connections, and permanently change the host. Owner-control mode
is therefore equivalent in impact to remote root SSH.

The meaningful boundaries are authenticated pairing, actor and route binding,
target policy, the selected out-of-band profile, node-local enforcement, and
OS authority. The default profile remains disabled. Delegated agents never
inherit owner authority merely because an owner used it elsewhere.

The companion should remain unprivileged where practical, with a separately
authenticated session broker providing the configured OS authority. However,
an arbitrary root-shell broker is itself broad root authority, not a narrow
helper. The architecture PR must compare that design with an explicitly
root-run companion profile and choose based on attack surface, isolation, and
operational simplicity rather than presenting either as harmless.

### Terminal and durability semantics

PTY transport must define terminal-byte framing, backpressure, resize,
signals, process-tree containment, idle and maximum duration, output limits,
and disconnect behavior. It must also prevent terminal control sequences from
corrupting operator UI or ordinary logs.

A non-interactive invocation follows existing durable execution identity and
unknown-outcome rules. It is never replayed after the dispatch boundary.
Interactive input is ordered within one live session and is not replayed after
an ambiguous disconnect. The first release may terminate a terminal on
disconnect; detached and reconnectable sessions require an explicit later
contract rather than accidental persistence.

Audit records contain identity, profile, lifecycle, timing, and bounded result
metadata. Raw commands, terminal input, output, environment, and transcripts
are excluded by default. Any transcript retention is a separate encrypted,
opt-in policy with explicit retention and access controls.

### Model-facing invocation cancellation

`nodes_cancel` was intentionally deferred from the MVP because the existing
cancellation API could not be exposed as a thin model-facing adapter without
additional lifecycle guarantees. Before exposing it, the contract must define:

- authority scoped to the same actor, agent, routed session, target, workspace,
  and execution identity as the invocation;
- idempotent duplicate cancellation requests;
- deterministic cancel-versus-completion races;
- `cancel_requested` as distinct from confirmed `canceled`;
- confirmed cancellation only after the companion proves process-tree
  termination;
- an explicit unknown outcome after disconnect or restart when termination
  cannot be proven;
- status recovery through `nodes_status`;
- no replay of either the original invocation or the cancellation side effect.

This follow-up reuses the existing coordinator, invocation store, companion
ledger, and cancellation API. If implementation requires another generic
lifecycle or durable-execution subsystem, stop and perform an architecture
checkpoint instead of silently expanding the milestone.

Interactive PTY close and signal operations are live session controls. They do
not replace durable `nodes_cancel` semantics for non-interactive invocations,
and accepting a cancellation request never by itself proves that execution
stopped.

### Suggested delivery sequence

1. Land an owner-mode threat model, shell and PTY contracts, profile schema,
   approval choices, redaction, lifecycle, and explicit non-goals.
2. Add non-interactive `shell.exec.v1` over the existing invocation path with
   cancel-capable process-tree ownership, no hidden replay, and a real-process
   test.
3. Define and expose the bounded `nodes_cancel` adapter over the existing
   cancellation path and cancel-capable shell consumer, with authority, race,
   restart, and recovery tests.
4. Add authenticated terminal session streaming with input ordering,
   backpressure, resize, signal, disconnect, and process-containment tests.
5. Add and deploy the selected Linux root authority profile or broker, while
   preserving disabled defaults and unprivileged delegated profiles.
6. Expose focused model and operator UX and prove the complete flow on a real
   VPS for both an authorized owner and a denied actor.

### Completion evidence

P1 is complete only when:

- a configured owner profile can prove UID 0, while an unconfigured actor,
  agent, route, target, or profile is denied;
- normal shell snippets with pipelines, redirects, variables, and failure
  status behave consistently;
- PTY input, resize, signals, exit, timeout, and disconnect behavior are
  deterministic and tested without timing-only assertions;
- neither a completed nor an uncertain shell mutation has an automatic replay
  path;
- `nodes_cancel` is actor-scoped, idempotent, race-safe, recoverable through
  `nodes_status`, and reports confirmed cancellation only after proven
  process-tree termination;
- audit events expose lifecycle metadata without raw command, environment,
  terminal content, or credentials;
- owner approval policy is configured out of band and cannot be relaxed by the
  model;
- delegated/product profiles and fresh installations remain deny-by-default.

## Deferred P1 Follow-Up: Browser Terminal Client

This is a future usability milestone, not a prerequisite for P2 and not part
of the completed P1 production-code gate. The P1 backend already owns the PTY,
process tree, ordered control stream, output cursor, limits, and authenticated
operator attachment. The local interactive CLI is complete; Launcher does not
yet provide a browser terminal emulator.

### Operator outcome

An authorized owner can reach a paired companion behind NAT without exposing
SSH or another inbound node port and can:

- run the deployed local `mintclaw nodes terminal` client; or
- open an attached terminal in MintClaw Launcher and use it through a browser
  terminal emulator once that future client is admitted.

The browser and CLI connect to the gateway, while the companion continues to
initiate the outbound node connection. Neither client connects directly to
the node or receives node connection details.

### Client surface

The Launcher client should use a terminal-emulator boundary such as xterm.js,
reuse the authenticated MintClaw session identity, and proxy the terminal
WebSocket without exposing the raw gateway token to browser JavaScript. It
handles binary output, input, resize, signals, attach deadline, connection
state, and explicit close. Closing or losing the attached client preserves the
existing fail-closed process-tree termination behavior.

The deployed CLI accepts explicit target, profile, and scope flags, enters
local raw terminal mode only after approval, forwards resize and signals, and
always restores the local console on exit or error.

### Security and scope boundaries

- Session start still requires the trusted operator-owned approval mode.
- Actor, authenticated operator session, workspace, target, profile, and
  terminal identity remain bound on every action.
- A Telegram chat message is not approval; a separately authenticated,
  plan-bound approval callback may be.
- Another browser, CLI, or operator session cannot attach, observe, take over,
  or close a session without a separately admitted handoff contract.
- Browser control sequences are rendered only inside the terminal emulator;
  passive logs never receive the raw byte stream.
- Attached-only behavior remains the default. Detach, reconnect, transcript
  retention, terminal sharing, file transfer, port forwarding, and SSH-agent
  forwarding remain separate future decisions.

### Suggested delivery sequence

1. Admit the browser authentication, retention, routing, and approval contract.
2. Add a Launcher terminal WebSocket proxy and browser terminal emulator with
   authenticated session reuse.
3. Validate browser and existing CLI flows against a real companion behind
   NAT, then deploy with terminal profiles still disabled by default.

### Completion criteria

This follow-up is complete only when:

- browser and CLI clients each complete input, output, resize, signal, exit,
  disconnect, and local cleanup against the same real PTY implementation;
- neither client needs an inbound node port, node address, SSH key, or direct
  broker access;
- another actor, operator session, target, or profile is denied before
  attachment or terminal content exposure;
- input remains ordered and at most once, output cursors and limits are
  enforced, and disconnect leaves no process tree behind;
- terminal content may enter only the authorized live client defined by the
  revised contract and never appears in passive logs, traces,
  approvals, or operational evidence;
- fresh installs and deployed profiles remain disabled by default; and
- merged-main validation, canary rollback, health checks, and redacted
  operational evidence are recorded.

## P2: File Transfer And Administrator Filesystem Access

P2 is complete under the exact scope, authority decisions, delivery order,
completion gates, and mandatory stop conditions in
[`node-companion-p2-admission.md`](node-companion-p2-admission.md). The
authoritative merged-code, canary, deployment, redaction, retention, restart,
rollback, and residual-limit evidence is recorded in
[`node-companion-p2-deployment-evidence.md`](../operations/node-companion-p2-deployment-evidence.md).
Fresh and delegated profiles remain deny-by-default; file authority exists
only through explicit operator configuration.

### Operator outcome

An authorized operator can ask MintClaw to:

- upload a local gateway file or retained artifact to a node path;
- download a node file into a gateway artifact;
- deliver a downloaded image or file through a supported channel;
- inspect bounded file metadata before transfer;
- deliberately use an administrator filesystem profile when configured.

This is a first-party capability. `system.exec.v1` is not used as an implicit
file-transfer channel.

### Initial model-facing surface

The first surface should remain small:

- `nodes_file_info`: inspect one path and its transferable type, size, mode,
  owner, modification time, and digest when policy permits;
- `nodes_upload`: transfer one gateway file or artifact to one node path;
- `nodes_download`: transfer one node file into a gateway artifact and
  optionally deliver it through the active channel.

The model supplies a target alias and paths or artifact references. It cannot
supply a hostname, credential, helper socket, transport endpoint, policy
profile, OS user, or administrator flag.

### Transfer contract

The first transfer contract supports regular files only and requires:

- bounded total size and chunk size;
- declared byte count and SHA-256;
- a temporary destination followed by atomic publication;
- explicit create-versus-overwrite behavior;
- bounded timeout, cancellation, cleanup, and concurrency;
- no partial destination publication after failure;
- no automatic replay after an ambiguous publication boundary;
- owner-only gateway spool permissions and bounded retention;
- an opaque artifact reference instead of placing binary data in model text;
- streaming that does not place file bytes in ordinary JSON envelopes.

The initial version does not include directory recursion, filesystem
synchronization, delta transfer, arbitrary archive extraction, resumable
cross-restart uploads, device files, sockets, FIFOs, or implicit decompression.

### Filesystem policy

Gateway and node policy use operator-defined profiles. A narrow profile may
grant only selected roots:

```yaml
node_file_policies:
  project:
    readable_roots: ["/srv/project"]
    writable_roots: ["/srv/project"]
    allow_create: true
    allow_overwrite: false
    max_file_bytes: 67108864
```

An explicit administrator profile may grant `/`:

```yaml
node_file_policies:
  server_admin:
    readable_roots: ["/"]
    writable_roots: ["/"]
    allow_create: true
    allow_overwrite: true
    max_file_bytes: 1073741824
    approval:
      read: none
      write: required
```

The exact configuration schema is decided in the milestone architecture PR.
The security requirements are fixed:

- the selected profile is operator configuration, never a tool argument;
- target, agent, routed session, and authenticated actor are bound to the
  transfer authority;
- approval, when required, binds node identity, canonical path, action,
  overwrite mode, digest, size, requested metadata, policy revision, and
  expiry;
- path validation rejects traversal and uses descriptor-relative or equivalent
  race-resistant filesystem operations;
- symlink following is off by default and, if allowed, is an explicit policy;
- special files and pseudo-filesystems are denied by default even under `/`;
- audit events contain bounded path/action/digest metadata but no file content.

### Privileged access

The companion continues to run unprivileged. Root-owned files use a narrow
privileged helper rather than running the whole companion as root.

The helper accepts typed file operations such as metadata inspection, bounded
read, create, and atomic replace. It never accepts shell text, argv, arbitrary
environment variables, or an unrestricted file descriptor request. It
validates peer credentials, the signed or authenticated transfer authority,
policy revision, path, operation, expiry, digest, size, and publication mode.

Linux is the first administrator-helper target. macOS privileged filesystem
support requires a separate platform decision and is not implied by the Linux
helper.

### Suggested delivery sequence

1. Land the file-transfer threat model, typed schemas, policy profiles, limits,
   approval binding, failure semantics, and explicit non-goals.
2. Add the bounded gateway artifact spool and authenticated transfer framing
   without a model tool or privileged access.
3. Add unprivileged node upload/download for configured roots with atomic
   publication and recovery tests.
4. Add `nodes_file_info`, `nodes_upload`, and `nodes_download`, including
   channel delivery for downloaded files and images.
5. Add the Linux privileged file helper and explicit administrator profiles.
6. Add one real-process binary round trip plus config replacement and image
   delivery tests, then deploy behind deny-by-default configuration.

Each PR must leave transfer authority unreachable until both gateway and
node-local enforcement exist.

### Completion evidence

P2 is complete only when:

- a text config and a binary image round-trip with matching digests;
- overwrite and no-overwrite publication behave atomically;
- unauthorized actor, target, path, symlink, size, and special-file attempts
  fail closed;
- disconnects before and after publication boundaries have explicit outcomes
  and never cause blind replay;
- an administrator profile can read and replace an approved root-owned Linux
  file through the helper;
- logs, events, model results, and channel delivery contain no unintended file
  bytes or credentials;
- the deployed default remains deny-all until an operator selects a profile.

## P3: Typed Service Administration

P3 is complete under the exact scope, platform decision, authority model,
delivery order, completion gates, and mandatory stop conditions in
[`node-companion-p3-admission.md`](node-companion-p3-admission.md). Linux
systemd is the deployed vertical slice. Its proof is recorded in
[`node-companion-p3-deployment-evidence.md`](../operations/node-companion-p3-deployment-evidence.md).
macOS launchd and every later roadmap milestone remain unadmitted.

Add typed commands for:

- `service.status.v1`;
- `service.logs.v1`;
- `service.action.v1` for start, stop, restart, reload, and narrowly defined
  enablement operations.

Service names and actions are node-local allowlists. Read-only status and
bounded logs do not imply mutation authority. Mutating actions bind approval to
the exact node, service, action, policy revision, and expiry.

The privileged helper may reuse its authenticated request envelope and peer
validation from P2, but service handlers remain separate from file handlers.
The helper never accepts a shell command or arbitrary system-manager flags.

The deployed operator slice proves bounded reads, exact approved restart,
post-action verification, recovery without replay, helper isolation,
fail-closed diagnostics, and reversible rollback. No P4 work is implied.

## P4: Fleet Operations And Companion Updates

Improve operations only after multiple deployed companions justify it:

- richer `doctor` checks for TLS posture, policy breadth, stale nodes, version
  compatibility, state permissions, and helper configuration;
- inventory and bounded health summaries for larger fleets;
- signed companion release artifacts;
- explicit staged update, health verification, and rollback;
- configuration validation and dry-run before restart;
- safe key rotation and re-pairing procedures;
- documented backup and disaster-recovery behavior.

An update channel is separate from command execution and file transfer.
Downloaded binaries require release-signature verification and cannot be
authorized solely by a model-generated URL or digest.

### Future operations follow-up: authenticated live-agent and invocation smoke

Status: the authenticated live-agent slice is implemented by
`mintclaw agent live`; see the
[operations guide](../operations/live-agent-smoke.md). It can exercise durable
node invocation through the live agent without Telegram. A future direct
low-level invocation fixture remains separate work if provider-independent
testing is still needed.

Add a deterministic operator-facing smoke path for the already running gateway.
The existing `mintclaw agent` command creates a separate `AgentLoop`, so it
cannot see node sessions owned by the gateway process and is not a valid test
of `nodes_invoke` or `nodes_status` in production.

The future smoke must:

- reuse the gateway's authenticated operator identity and live node session
  instead of starting a second agent runtime;
- support a stateless live-agent prompt and a lower-level durable invocation
  fixture so model selection and transport/persistence can be tested
  independently;
- accept only configured target, command, profile, and working-scope aliases;
- preserve normal target grants, node-local policy, approval, durable plan,
  idempotency, status recovery, and no-blind-replay semantics;
- print bounded machine-readable progress, invocation identity, terminal state,
  exit status, and a fixed marker without command output or credentials;
- use a fixed read-only fixture by default and require an explicit operator
  choice before any mutating fixture;
- time out, cancel local waiting, and leave an uncertain remote invocation for
  `nodes_status` recovery rather than replaying it; and
- produce a correlated redacted diagnostic trace suitable for deployment
  verification.

This follow-up is complete only when CI covers authentication denial, target
and profile denial, successful execution, definitive rejection, disconnect
after dispatch, status recovery, duplicate-request idempotency, and output
redaction; and a deployed command can verify the `vpn` companion without a
Telegram message or an inbound node port.

## P5: Additional Executors And Long-Running Work

Add isolation and durable work as independent capabilities:

- Docker executor with pinned images, resource limits, explicit mounts, and
  denied network by default;
- bubblewrap or another supported local sandbox where platform evidence
  justifies it;
- bounded background jobs with durable status and artifact output;
- streamed output with explicit backpressure and retention;
- process-tree containment before advertising reliable cancellation.

Target selection continues to answer *where* work runs; executor selection
answers *how* it is isolated. A node target never silently changes a command
from local execution to Docker or vice versa.

## P6: Bootstrap And Alternative Transports

### SSH bootstrap

Provide an explicit operator command that:

- verifies the SSH host key;
- copies a signed or locally built slim companion;
- installs an unprivileged service;
- transfers short-lived enrollment material and pinned gateway TLS identity;
- verifies the outbound paired connection;
- removes bootstrap secrets after enrollment.

SSH credentials remain operator-owned and are never exposed to the model.

### Static SSH target

Consider a separate static SSH target driver only for hosts that cannot run a
companion. It reuses named-target policy, canonical plans, approval, bounded
results, and audit, while honestly reporting weaker guarantees: no live
catalog, durable remote ledger, or reconnect recovery unless a narrow remote
helper is present.

## P7: Interactive Application Capabilities

Admit capabilities independently, each with its own policy and threat model:

- browser navigation, snapshot, screenshot, and download commands;
- node-hosted MCP tool catalogs with bounded descriptor approval;
- camera, microphone, location, notification, and sensor commands;
- clipboard and desktop-control capabilities;
- application-specific adapters that do not expose a general shell.

Interactive sessions must not reuse synchronous `system.exec.v1` semantics.
Media output uses the artifact contract established by P2.

## P8: Remote Workspace Routing

After shell, filesystem, artifact, and selected application capabilities have
proven their individual contracts, consider a remote workspace abstraction.
It gives a turn or agent an explicit execution context such as `build-node`
and routes only compatible tools through that target:

```text
agent turn
    |
    v
workspace context: target=build-node
    |
    +-- exec/read/write/patch/search -> node workspace capabilities
    +-- browser                    -> node browser capability, when admitted
    +-- MCP tools                  -> node MCP catalog, when admitted
    +-- memory/channels/approvals  -> gateway services
```

This is a routing layer over existing target, capability, policy, invocation,
artifact, and audit contracts. It is not a second transport and does not move
the AgentLoop, model session, memory, channel delivery, or approval authority
onto the node.

Each tool must declare whether it supports a remote execution context and how
its paths, artifacts, cancellation, progress, and results cross that boundary.
Unsupported tools remain gateway-local or fail explicitly; they must never
silently run on the wrong machine. Operator policy defines selectable workspace
targets and allowed tool groups. A model may select only from that bounded
surface and cannot supply connection details or broaden the node catalog.

Initial admission should require one cohesive vertical slice, preferably
`exec`, `read`, `write`, `patch`, and `search` against a shared node workspace.
Browser or MCP routing is added only after its independent P7 capability and
threat model exist. Avoid a generic proxy that forwards arbitrary current or
future tool calls without per-tool compatibility and policy declarations.

## P9: Remaining Platforms And Compatibility

Potential later targets include:

- Windows companion service, process containment, filesystem helper, and
  service-manager adapters;
- iOS companions with platform-native permission prompts;
- constrained appliance or camera companions with reduced catalogs;
- an OpenClaw protocol adapter pinned to an explicitly supported version;
- native gRPC, MQTT, or other transports when deployment evidence justifies
  them.

Android now has a dedicated
[`android-companion-roadmap.md`](android-companion-roadmap.md). Its operator
chat track may start after its own admission because it does not depend on P7
device capabilities or completion of this generic platform milestone. Its
device-node track reuses this architecture and admits each Android capability
under the P7 rules.

Compatibility remains an adapter. Core policy, target, invocation, and
capability packages do not import external wire types or branch on external
client identities.

## Milestone Admission Checklist

Before implementation of any milestone:

- identify one concrete deployed operator use case;
- name the authenticated actors and target profiles that need it;
- document the minimum typed command surface;
- document node-local enforcement and required OS authority;
- define commit, retry, cancellation, timeout, and unknown-outcome boundaries;
- set data, concurrency, retention, and output limits;
- define approval and redaction behavior;
- identify a real-process end-to-end test;
- list features explicitly excluded from the milestone;
- reject any general foundation without a consumer in the same milestone.

After implementation:

- validate merged `main`, not only feature branches;
- deploy with deny-by-default configuration;
- verify logs, persistence, health, and absence of duplicate effects;
- record operational evidence and residual limitations;
- decide whether evidence supports admitting the next milestone.

## Roadmap Non-Goals

- turning the companion into another agent, gateway, or workflow scheduler;
- giving the model authority because a user message claims administrator
  status;
- using `system.exec.v1` argv, environment variables, shell text, or base64
  JSON as file transfer;
- enabling owner or root profiles by default;
- allowing a model, tool argument, or chat claim to create or broaden an owner
  profile;
- presenting shell-text scanning as a sandbox for arbitrary commands;
- exposing owner-control profiles to delegated or product agents;
- running the complete companion as root by default when a better-isolated
  broker or narrow helper suffices;
- automatic synchronization of gateway and node filesystems;
- treating a remote workspace as migration of the whole agent runtime;
- implicitly routing an unsupported tool to either the gateway or a node;
- forwarding every registered tool through a generic node proxy;
- implementing all platform and compatibility work before a demonstrated use
  case;
- treating this roadmap as a release schedule or as authorization to enable
  owner mode or broad automatic approval on any deployed profile.
