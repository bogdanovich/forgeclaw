# Node Companion Post-MVP Roadmap

## Status

Planned work after the node companion MVP defined in
[`node-companion.md`](node-companion.md) is merged, validated from `main`, and
deployed. This roadmap does not expand that MVP or authorize implementation of
every item below.

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
| P0 | Owner-controlled shell and terminal | Operate a personal server through ordinary shell commands and an interactive PTY, including explicit root profiles | Deployed execution MVP |
| P1 | File transfer and administrator filesystem access | Send files to a node, retrieve files and images, and manage explicitly authorized paths | Owner profiles and deployed execution MVP |
| P2 | Typed service administration | Inspect logs/status and perform allowlisted service actions without broad shell authority | Privileged helper boundary proven by P1 |
| P3 | Fleet operations and companion updates | Diagnose, version, update, and roll back companion instances safely | Stable node lifecycle and artifacts |
| P4 | Additional executors and long-running work | Run contained builds/jobs without confusing placement with isolation | Stable invocation and artifact contracts |
| P5 | Bootstrap and alternative transports | Enroll hosts through SSH and support bounded static SSH targets | Stable target-driver contract |
| P6 | Interactive application capabilities | Add browser, MCP, camera, location, and other typed capabilities | Per-capability threat models |
| P7 | Platforms and compatibility adapters | Add Windows/mobile companions and explicitly versioned external adapters | Stable internal contracts |

Priorities express ordering, not a commitment to implement every milestone.

## P0: Owner-Controlled Shell And Interactive Terminal

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
2. Define and expose the bounded `nodes_cancel` adapter over the existing
   cancellation path, with authority, race, restart, and recovery tests.
3. Add non-interactive `shell.exec.v1` over the existing invocation path with
   no hidden replay and a real-process test.
4. Add authenticated terminal session streaming with input ordering,
   backpressure, resize, signal, disconnect, and process-containment tests.
5. Add and deploy the selected Linux root authority profile or broker, while
   preserving disabled defaults and unprivileged delegated profiles.
6. Expose focused model and operator UX and prove the complete flow on a real
   VPS for both an authorized owner and a denied actor.

### Completion evidence

P0 is complete only when:

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

## P1: File Transfer And Administrator Filesystem Access

### Operator outcome

An authorized operator can ask ForgeClaw to:

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

P1 is complete only when:

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

## P2: Typed Service Administration

Add typed commands for:

- `service.status.v1`;
- `service.logs.v1`;
- `service.action.v1` for start, stop, restart, reload, and narrowly defined
  enablement operations.

Service names and actions are node-local allowlists. Read-only status and
bounded logs do not imply mutation authority. Mutating actions bind approval to
the exact node, service, action, policy revision, and expiry.

The privileged helper may reuse its authenticated request envelope and peer
validation from P1, but service handlers remain separate from file handlers.
The helper never accepts a shell command or arbitrary system-manager flags.

Completion requires Linux systemd coverage, explicit macOS launchd scope, real
post-action verification, bounded logs, cancellation/unknown semantics, and
one deployed operator use case.

## P3: Fleet Operations And Companion Updates

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

## P4: Additional Executors And Long-Running Work

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

## P5: Bootstrap And Alternative Transports

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

## P6: Interactive Application Capabilities

Admit capabilities independently, each with its own policy and threat model:

- browser navigation, snapshot, screenshot, and download commands;
- node-hosted MCP tool catalogs with bounded descriptor approval;
- camera, microphone, location, notification, and sensor commands;
- clipboard and desktop-control capabilities;
- application-specific adapters that do not expose a general shell.

Interactive sessions must not reuse synchronous `system.exec.v1` semantics.
Media output uses the artifact contract established by P1.

## P7: Platforms And Compatibility

Potential later targets include:

- Windows companion service, process containment, filesystem helper, and
  service-manager adapters;
- iOS and Android companions with platform-native permission prompts;
- constrained appliance or camera companions with reduced catalogs;
- an OpenClaw protocol adapter pinned to an explicitly supported version;
- native gRPC, MQTT, or other transports when deployment evidence justifies
  them.

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
- implementing all platform and compatibility work before a demonstrated use
  case;
- treating this roadmap as a release schedule or as authorization to enable
  owner mode or broad automatic approval on any deployed profile.
