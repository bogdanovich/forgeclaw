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

## Priority Overview

| Priority | Milestone | Operator outcome | Depends on |
| --- | --- | --- | --- |
| P0 | File transfer and administrator filesystem access | Send files to a node, retrieve files and images, and manage explicitly authorized paths | Deployed execution MVP |
| P1 | Typed service administration | Inspect logs/status and perform allowlisted service actions without a root shell | Privileged helper boundary proven by P0 |
| P2 | Fleet operations and companion updates | Diagnose, version, update, and roll back companion instances safely | Stable node lifecycle and artifacts |
| P3 | Additional executors and long-running work | Run contained builds/jobs without confusing placement with isolation | Stable invocation and artifact contracts |
| P4 | Bootstrap and alternative transports | Enroll hosts through SSH and support bounded static SSH targets | Stable target-driver contract |
| P5 | Interactive and application capabilities | Add PTY, browser, MCP, camera, location, and other typed capabilities | Per-capability threat models |
| P6 | Platforms and compatibility adapters | Add Windows/mobile companions and explicitly versioned external adapters | Stable internal contracts |

Priorities express ordering, not a commitment to implement every milestone.

## P0: File Transfer And Administrator Filesystem Access

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

P0 is complete only when:

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

## P1: Typed Service Administration

Add typed commands for:

- `service.status.v1`;
- `service.logs.v1`;
- `service.action.v1` for start, stop, restart, reload, and narrowly defined
  enablement operations.

Service names and actions are node-local allowlists. Read-only status and
bounded logs do not imply mutation authority. Mutating actions bind approval to
the exact node, service, action, policy revision, and expiry.

The privileged helper may reuse its authenticated request envelope and peer
validation from P0, but service handlers remain separate from file handlers.
The helper never accepts a shell command or arbitrary system-manager flags.

Completion requires Linux systemd coverage, explicit macOS launchd scope, real
post-action verification, bounded logs, cancellation/unknown semantics, and
one deployed operator use case.

## P2: Fleet Operations And Companion Updates

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

## P3: Additional Executors And Long-Running Work

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

## P4: Bootstrap And Alternative Transports

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

## P5: Interactive And Application Capabilities

Admit capabilities independently, each with its own policy and threat model:

- streamed PTY sessions with explicit interactive authorization;
- browser navigation, snapshot, screenshot, and download commands;
- node-hosted MCP tool catalogs with bounded descriptor approval;
- camera, microphone, location, notification, and sensor commands;
- clipboard and desktop-control capabilities;
- application-specific adapters that do not expose a general shell.

Interactive sessions must not reuse synchronous `system.exec.v1` semantics.
Media output uses the artifact contract established by P0.

## P6: Platforms And Compatibility

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
- running the complete companion as root when a narrow helper suffices;
- a general remote root shell;
- automatic synchronization of gateway and node filesystems;
- implementing all platform and compatibility work before a demonstrated use
  case;
- treating this roadmap as a release schedule or standing authorization for
  broad automatic approval.
