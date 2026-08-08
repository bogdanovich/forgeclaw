# Node Companion P4 Safe Single-Node Update Admission

## Status And Decision

Status: admitted for implementation after this contract merges

P4 admits one remotely requested, lifecycle-managed companion update at a
time on Linux and macOS. It is a single-node operational capability, not fleet
management.

The admitted platforms and service managers are:

- Linux `amd64` and `arm64`, under a managed systemd user or system service;
- macOS `amd64` and `arm64`, under a managed LaunchAgent or LaunchDaemon; and
- only instances installed with MintClaw lifecycle metadata and the stable
  update coordinator introduced by P4.

The model continues to use `nodes describe`, `nodes_invoke`, and
`nodes_status`. P4 adds the typed `node.update.v1` capability and extends the
existing `node.info.v1` result with bounded update facts when needed. It adds
no model-facing updater tool, gateway transport, gateway invocation store,
generic job system, alternate protocol version, or file-transfer shortcut.

P4 explicitly does not admit fleet inventory, group targeting, rolling or
parallel updates, fleet health aggregation, automatic rollout, key rotation,
bootstrap, or arbitrary package management. Existing `nodes` discovery is
enough to select one configured target for the admitted use case.

## Evidence For Admission

The repository already has the necessary remote authority path:

- authenticated outbound WSS pairing and target aliases;
- descriptor-bound discovery and node-local command policy;
- durable prepared plans, approval, invocation identity, status recovery, and
  no-blind-replay semantics;
- a companion invocation ledger and redacted runtime events;
- `node.info.v1`, which reports node ID, platform, architecture, and version;
- create-only lifecycle management for Linux systemd and macOS launchd; and
- transactional lifecycle ownership checks and readiness observation.

The missing pieces are specific and cannot be delegated to the existing
gateway updater:

- the current GoReleaser configuration does not publish `mintclaw-node`;
- release checksums alone do not establish an authenticated release identity;
- the existing updater accepts release selectors intended for an operator CLI
  and does not define a model-safe release allowlist;
- replacing the running executable does not prove that the successor starts,
  reconnects, or is compatible; and
- the running companion cannot reliably roll itself back after a successor
  fails before startup.

Those facts justify one narrow local update coordinator. They do not justify
a second remote execution architecture.

## Concrete Operator Use Case

An operator installs a managed companion on a personal Linux server or Mac and
configures one update profile, for example `stable`. The profile pins the
trusted MintClaw release source and allows only a bounded version progression.

An authorized agent can:

1. discover `node.info.v1` and `node.update.v1` on one configured target;
2. inspect the installed version, platform, architecture, and update state;
3. request the configured release alias, never a URL or digest;
4. wait for durable human approval unless the exact target and actor have an
   explicit operator-configured bypass;
5. stage and verify the exact platform artifact locally;
6. activate it through the managed service lifecycle;
7. observe either a healthy authenticated successor, a proven rollback, or an
   explicit unknown outcome; and
8. use `nodes_status` and a fresh `node.info.v1` after reconnect without
   replaying the update.

The first deployment proof updates reversible Linux and macOS canary
instances. It must not use the production gateway, reviewer, SSH path,
Tailscale path, or another control-plane dependency as its first mutation.

## Security Truth And Threat Model

Companion update is remote code installation. A signed artifact and typed
command narrow authority; they do not make a malicious trusted release safe.

Protected assets include:

- executable, config, identity, credential, ledger, helper, and service files
  outside the exact managed instance;
- release signing roots, channel configuration, and rollback material;
- platform, architecture, service scope, executable path, and service owner;
- actor, route, session, agent, target, approval, tool-call, invocation, and
  execution identity;
- truthful staging, activation, restart, reconnect, rollback, and recovery
  state; and
- unrelated shell, file, service, terminal, browser, and helper authority.

P4 assumes the model, prompt content, release metadata endpoint, network,
unprivileged companion, and downloaded bytes may be malicious. It also assumes
disconnects, crashes, power loss, response loss, concurrent requests, stale
discovery, full disks, and service-manager delays at every boundary.

The design must prevent:

- caller-selected URLs, digests, signing keys, repositories, tags, versions,
  platforms, architectures, executable paths, services, users, or restart
  commands;
- treating a checksum supplied beside an artifact as release authenticity;
- archive traversal, symlink or hardlink replacement, cross-filesystem
  non-atomic publication, and writes outside the managed update store;
- updating an unowned lifecycle entry or a different binary after approval;
- concurrent activation, stale-policy activation, and downgrade unless a
  separately configured recovery rule allows the exact version;
- automatic replay after staging or activation may have begun;
- reporting success merely because the service manager accepted a restart;
- a model or ordinary chat response approving its own update; and
- logs, events, approval prompts, or traces retaining credentials, release
  tokens, raw manifests, binary bytes, or unrestricted process output.

Compromise of a companion can already exercise its configured OS identity.
P4 must not turn that identity into root or another user. A system-scoped
instance is admitted only through the exact narrow update authority installed
for that managed instance; it does not expose a general privileged service
manager or filesystem writer.

## Sources Of Truth And Authority

The authority intersection is:

1. authenticated channel sender and routed conversation ownership;
2. workspace, agent, session, turn, provider tool call, and execution identity;
3. the agent's configured target grant and update-profile binding;
4. durable node pairing, current live session, and fresh descriptor catalog;
5. the exact prepared execution plan and existing durable approval;
6. companion local policy and update-profile revision;
7. the locally installed managed-service and coordinator identity;
8. an operator-pinned release signing root and signed manifest;
9. the manifest's exact release, platform, architecture, size, and digest; and
10. local coordinator evidence for activation, health, and rollback.

Discovery, model text, chat attachments, redirects, file-transfer artifacts,
current version alone, release page text, passive events, and a digest supplied
by the same untrusted request are not authority sources.

Missing, expired, ambiguous, or changed authority fails closed before
activation. A descriptor, profile, release catalog, signing root, managed
executable, service identity, or coordinator change invalidates stale
discovery and prepared work.

## Release And Profile Contract

The architecture-level profile shape is:

```yaml
nodes:
  targets:
    personal-mac:
      update_profile: stable-node

node_update_policies:
  stable-node:
    enabled: false
    revision: stable-node-v1
    channel: stable
    source: mintclaw-production
    allow_downgrade: false
    approval: required
```

Exact field placement may differ, but these semantics are fixed:

- absent profiles, missing target bindings, and `enabled: false` grant
  nothing;
- source and channel are bounded operator-defined aliases, not network input;
- the model may select only an enumerated release alias exposed by fresh
  discovery; `latest` is resolved and pinned during preparation, not again
  after approval;
- stable profiles reject prereleases and nightly builds; a nightly profile is
  a separate explicit operator choice;
- downgrades are denied by default and cannot be enabled by model input;
- only `linux` and `darwin` artifacts matching the authenticated node's actual
  `amd64` or `arm64` platform tuple are admitted;
- size, digest, release identity, minimum launcher version, companion protocol
  compatibility, and required config schema are signed manifest facts;
- P4 updates only the companion payload. Helper, broker, launcher, config,
  identity, and service-definition migrations are refused and deferred; and
- the effective profile projection participates in descriptor and catalog
  hashes without exposing signing material or network credentials.

The release pipeline must publish a separately identifiable slim
`mintclaw-node` artifact and authenticated manifest for every admitted tuple.
The local verifier must authenticate an operator-pinned signing identity or
equivalent repository provenance before trusting manifest content, then verify
the artifact digest and size. TLS and a GitHub asset checksum are defense in
depth, not the signing trust root.

The implementation PR that selects the signing mechanism must document key
ownership, rotation, revocation, CI permissions, verification behavior, and an
offline fixture. If the repository cannot produce and verify authenticated
provenance without granting the model or companion release-publishing
credentials, implementation stops at the architecture checkpoint.

## Model-Visible Contract

### Existing `node.info.v1`

The existing fields remain compatible: node ID, platform, architecture, and
version. P4 may add only bounded facts needed to operate updates, such as:

- managed-update availability;
- active release identity;
- last update state from a fixed vocabulary;
- previous release identity when locally retained; and
- whether recovery requires operator action.

It must not expose local paths, service labels, signing keys, URLs, credentials,
raw manifests, launcher internals, or unrestricted failure text.

### New `node.update.v1`

Illustrative input after effective-schema projection:

```json
{
  "release": "stable-2026-08-07"
}
```

The effective input schema enumerates only releases admitted by the target's
current profile and authenticated manifest catalog. The model cannot supply a
URL, tag, digest, platform, architecture, source, channel, service, path,
restart mode, approval mode, timeout extension, downgrade flag, or rollback
override.

The command risk is `privileged`. Existing durable approval binds the exact
target, node, release identity, manifest digest, artifact digest, current
version, platform, architecture, profile revision, descriptor/catalog identity,
managed instance, actor, agent, route, session, tool call, execution identity,
plan hash, and expiry.

The bounded result reports only:

- requested and previous release identity;
- fixed lifecycle state;
- whether activation was attempted;
- whether the expected successor was verified;
- whether rollback was attempted and proven;
- installed version when proven; and
- a fixed safe error code and recovery action when not proven.

The existing `nodes_status` remains the invocation recovery API. P4 adds no
`nodes_update_status` tool. A fresh `node.info.v1` is the independent post-
reconnect observation; it cannot retroactively prove that an ambiguous update
invocation completed.

## Durable Lifecycle And No-Replay Semantics

One local durable update record is allowed because restart crosses companion
process lifetimes. It is instance-local, bounded, private, and owned by the
stable coordinator. It is not a second gateway invocation store or a generic
job framework.

The fixed lifecycle is:

```text
prepared -> downloading -> verified -> staged -> activating
          -> healthy
          -> rolling_back -> rolled_back
          -> unknown | operator_action_required
```

Gateway invocation semantics remain `prepared -> dispatched`. Before the node
durably accepts the exact update identity, denial is definitive and a new
operator request may prepare new work. After durable acceptance, any lost
response is uncertain and neither the gateway nor model may automatically
dispatch again.

The coordinator deduplicates the exact execution and release identity. A
duplicate returns the same durable observation; different arguments under the
same identity fail as a conflict. Only one nonterminal update may exist for an
instance. Concurrent, stale, expired, canceled, or policy-changed requests
fail closed.

Cancellation is supported only before activation begins. Once the activation
record is committed, cancellation reports unsupported or too late and must not
race a replacement or rollback. `nodes_cancel` remains the only model-facing
cancellation path.

Download and verification are safe to resume by exact signed artifact digest.
Activation is never replayed automatically. Restart, response loss, or an
unknown gateway result causes status recovery, not another activation.

## Stable Coordinator And Platform Semantics

A stable local coordinator is required because the payload being replaced
cannot prove its own failed startup. The coordinator must remain materially
smaller than the companion and expose only the update transaction. It may be a
dedicated launcher or an equivalently narrow lifecycle component, but it must:

- be installed and referenced by MintClaw-owned lifecycle metadata;
- accept only authenticated local records created for that exact instance;
- retain one verified previous payload and one staged candidate;
- launch no caller-selected executable or arguments;
- use a fixed minimal environment and the existing config path;
- keep companion credentials out of its logs and update records;
- atomically select the candidate on the same filesystem and fsync durable
  state;
- distinguish candidate startup from authenticated gateway readiness; and
- restore the verified previous payload when candidate health cannot be
  proven within bounded attempts.

The coordinator itself is not remotely self-updated in P4. A release requiring
a coordinator, helper, service definition, config, identity, or state-schema
migration is incompatible and refused. Coordinator upgrades remain an
operator lifecycle action until a later admission proves safe two-component
rotation.

### Linux

Linux activation supports MintClaw-owned systemd user and system units. The
system unit may run the companion as a configured unprivileged service user.
Any root authority is limited to the exact preinstalled coordinator and
managed instance; no request can select a unit, user, binary path, or manager
operation.

The implementation must pin parent directories and ownership, reject symlinks
and unexpected hardlinks, create files with safe modes, bound disk use, use
same-filesystem atomic publication, and fsync file and directory state. A
systemd restart or process exit is only activation acceptance, not success.

### macOS

macOS activation supports MintClaw-owned LaunchAgents and LaunchDaemons. The
same exact-instance and no-privilege-expansion rules apply. The implementation
must validate Mach-O platform and architecture and verify the admitted release
signature/provenance before publication. Where Apple code-signing or
notarization is configured for production artifacts, native validation is an
additional mandatory preflight; it never replaces the MintClaw release trust
root.

Launchd job acceptance is not success. A successor must become stable and
reconnect as the same authenticated node with the expected version and
catalog. Failure or timeout triggers the same bounded local rollback.

## Health, Rollback, And Outcome Truth

Candidate health requires all of:

- coordinator-observed stable process startup;
- successful loading of the existing config, identity, ledger, and policy;
- authenticated outbound connection as the same node ID;
- expected version, platform, architecture, and compatible catalog; and
- a bounded stable observation period without restart-loop evidence.

Only then may the coordinator commit `healthy` and retire obsolete staging
data. Process existence, a service-manager state, an open socket, or version
text alone is insufficient.

If health fails, the coordinator restores the exact verified previous payload,
restarts it, and applies the same health criteria. `rolled_back` is reported
only when the previous payload is proven healthy. If neither candidate nor
previous payload can be proven, the state is `operator_action_required` or
`unknown`; it is never `failed, safe to retry`.

Rollback material is bounded to one prior payload, private to the instance,
verified before use, and retained until candidate health commits. Cleanup must
not remove the only recoverable payload. Power-loss tests cover every durable
publication boundary.

## Approval, Events, And Redaction

`node.update.v1` requires durable human approval by default. A bypass is valid
only when configured out of band for the exact target and actor context using
the existing approval policy. A model argument, ordinary text answer, trace,
or node response cannot grant approval.

Approval text may include target alias, current and requested release,
platform, architecture, channel, downgrade fact, and a fixed risk summary. It
must exclude URLs, credentials, local paths, signing material, raw manifest
content, and command output.

Existing runtime events record bounded transitions such as prepared, accepted,
verified, staged, activation attempted, successor observed, healthy, rollback
attempted, rolled back, unknown, and operator action required. They include
correlation and fixed reason codes, never binary bytes, credentials, manifest
payloads, local connection details, or unrestricted stderr.

## Configuration, Compatibility, And Migration

All update policy defaults are deny-all. Existing installations advertise no
update command until an operator installs the stable coordinator and enables a
profile. Merely upgrading the gateway or companion does not enable updates.

Adoption is explicit:

1. back up config, identity, and node state;
2. install the coordinator and updated MintClaw-owned lifecycle definition;
3. validate ownership, permissions, release trust roots, and rollback store;
4. verify the unchanged companion reconnects through the coordinator;
5. enable one target-scoped update profile; and
6. run a reversible canary before production use.

There are no deployed update-protocol clients requiring a version shim. P4
maintains one current contract. It adds no legacy path, migration framework,
or compatibility mode. An artifact requiring unsupported state or config
migration is rejected before activation.

## Explicit Non-Goals

P4 does not include:

- fleet list/status beyond existing target discovery;
- group, staged-fleet, rolling, parallel, scheduled, or percentage rollout;
- automatic discovery and installation of every new release;
- model-supplied URLs, tags, digests, keys, repositories, or channels;
- gateway, web UI, mobile, helper, broker, launcher, config, or identity update;
- OS package-manager, Homebrew, apt, rpm, deb, Docker, or SSH orchestration;
- arbitrary service restart, shell, terminal, or file-write authority;
- bootstrap, pairing, key rotation, re-pairing, backup, or disaster recovery;
- Windows, BSD, containers, or architectures other than admitted tuples;
- delta patches, binary transfer through chat, or user-provided artifacts;
- unattended broad approval or global `approve_all`; or
- a generic updater, durable job, supervisor, or fleet abstraction.

## Focused PR Sequence

After this docs-only admission PR merges, implementation proceeds from fresh
`origin/main` in this order:

1. **Trusted node releases and policy contract.** Publish slim Linux and macOS
   node artifacts plus authenticated manifests; add offline verification,
   update-profile validation, and effective descriptor projection. No remote
   activation is reachable.
2. **Stable coordinator and local transaction.** Add the narrow coordinator,
   bounded durable record, exact-instance staging, activation, health proof,
   rollback, crash recovery, and lifecycle adoption for systemd and launchd.
   Validate locally; do not expose `node.update.v1` yet.
3. **One model-facing vertical slice.** Add `node.update.v1` through existing
   discovery, target policy, approval, gateway invocation, WSS, companion
   ledger, `nodes_status`, and events. Preserve no-blind-replay and expose no
   new model tool.
4. **Real-platform proof and operations docs.** Add deterministic Linux and
   macOS real-process canaries, update/rollback/power-loss evidence, accurate
   architecture and operator docs, merged-main validation, and controlled
   deployment behind deny-by-default profiles.

Boundaries may be narrowed when evidence supports a smaller reviewable PR, but
implementation may not add a prerequisite program, fleet layer, generic
durability layer, or additional platform without a new admission decision.

After four substantive review/fix cycles in a PR, or after the same lifecycle
invariant is challenged three times, stop patching and perform an architecture
checkpoint. Also checkpoint if production scope reaches twice the PR's
original baseline. Prefer deleting or deferring scope to adding abstractions.

## Validation Matrix

Focused tests must cover:

- malformed, oversized, duplicate, case-colliding, disabled, and missing
  profile/release configuration;
- unsigned, wrong-signer, revoked, expired, malformed, oversized, and
  mismatched platform/architecture/version artifacts and manifests;
- redirects, partial downloads, digest mismatch, archive traversal, symlink,
  hardlink, permissions, full disk, and concurrent staging;
- wrong actor, target, agent, route, session, tool call, execution identity,
  approval, policy revision, catalog, managed instance, and signing root;
- changed arguments after approval and stale discovery/release resolution;
- duplicate and concurrent requests before and after durable acceptance;
- crash or power loss at every download, verification, publication,
  activation, health-commit, rollback, and cleanup boundary;
- disconnect before dispatch, after acceptance, during restart, after healthy
  reconnect, and during rollback, with no automatic replay;
- cancellation before activation and rejection after activation;
- successor startup failure, restart loop, wrong version, wrong node identity,
  incompatible catalog, timeout, successful rollback, failed rollback, and
  explicit unknown/operator-action outcomes;
- Linux systemd user/system and macOS LaunchAgent/LaunchDaemon ownership and
  lifecycle behavior;
- exact result schemas and redaction of manifests, URLs, credentials, paths,
  binary bytes, and unrestricted output; and
- unchanged behavior for non-update commands and update-disabled targets.

Each implementation PR runs focused race tests, lint with repository build
tags, and the relevant repository tests. CI remains the broad-suite authority
when a full local suite is prohibitively slow. Timing-only assertions are not
accepted.

The final proof uses real Linux and macOS companion processes and deterministic
release fixtures. At least one canary must prove successful update and one must
prove automatic rollback. A disconnect/unknown case must demonstrate status
recovery without a second activation.

## Definition Of Done And Mandatory Stop

P4 is complete only when all of the following are true:

- signed or equivalently authenticated slim `mintclaw-node` artifacts exist
  for Linux and macOS on `amd64` and `arm64`;
- a configured model can discover and request exactly one admitted release on
  exactly one target through `node.update.v1`;
- durable approval binds and resumes the exact prepared update;
- neither model nor request can choose network source, digest, platform,
  architecture, path, service, signing root, downgrade, or restart behavior;
- the stable coordinator survives payload replacement and durably proves
  healthy activation, verified rollback, or explicit unknown/operator action;
- duplicate, stale, concurrent, disconnected, and restarted execution cannot
  activate the candidate through automatic replay;
- `nodes_status` recovers the same invocation truth and fresh `node.info.v1`
  reports the independently observed installed version;
- Linux systemd and macOS launchd real-process canaries prove success and
  rollback under deny-by-default configuration;
- logs, events, approval prompts, traces, and docs expose no protected update
  material;
- all focused PRs are merged, merged-main validation passes, architecture and
  operations docs match behavior, and deployment evidence is recorded; and
- existing non-update node, gateway, approval, and lifecycle behavior remains
  healthy.

**Mandatory stop:** once every item above is evidenced, mark the P4 goal
complete and stop. Do not begin fleet status, batch updates, key rotation,
bootstrap, launcher self-update, additional platforms, package managers, or
any other P5+ or deferred work under the P4 goal. Any such work requires a new
operator decision and separate admission contract.
