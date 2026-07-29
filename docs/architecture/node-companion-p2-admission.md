# Node Companion P2 File Transfer Admission

## Status And Decision

P2 implementation is admitted as a dependency-ordered sequence under this
contract. Production administrator-file access is not admitted until the
implementation, trusted approval, and deployment gates below are satisfied.

The admitted implementation may add:

- a bounded gateway transfer spool with opaque artifact references;
- authenticated chunked transfer framing over the existing node connection;
- regular-file metadata, upload, and download operations;
- model-facing `nodes_file_info`, `nodes_upload`, and `nodes_download` tools;
- delivery of an authorized downloaded file or image through the originating
  routed channel;
- operator-owned file-policy profiles that are absent and deny-all by default;
  and
- a minimal Linux privileged file helper for explicitly configured
  administrator profiles.

This admission does not enable a profile, grant `/`, expose a root-owned file,
change a running companion, or allow a model or chat message to claim
administrator authority.

Any implementation finding that requires directory synchronization, recursive
trees, archive extraction, arbitrary file-descriptor passing, a second generic
workflow engine, a second authority store, another node transport, service
administration, or a privileged helper broader than typed regular-file
operations stops the affected PR and returns to roadmap admission.

## Concrete Operator Use Cases

P2 admits two bounded use cases over an already paired Linux companion.

### Unprivileged project transfer

An authenticated owner asks the configured `main` agent to:

1. upload one gateway file or retained media artifact into a configured
   project root;
2. inspect bounded metadata for one regular file;
3. download one regular file into the gateway spool; and
4. optionally deliver the downloaded file or image once to the same authorized
   channel, chat, and topic.

The agent does not need a hostname, node ID, transport endpoint, credential,
raw node policy, or host filesystem enumeration.

### Explicit administrator transfer

The same owner may deliberately configure an administrator file profile for a
personal Linux server. That profile may authorize the filesystem root `/`, but
only through a root-owned local helper and an exact actor, agent, route, target,
and profile grant.

The admitted proof reads and atomically replaces one designated root-owned
regular config fixture. It does not grant directory recursion, special-file
access, arbitrary descriptor access, service control, or general root RPC.

The following remain denied even if they know a target, path, artifact
reference, transfer ID, or profile alias:

- another actor, agent, authenticated route, routed session, workspace, or
  target;
- a delegated subagent or background task without an explicit independent
  grant;
- a model that invents a profile, administrator flag, helper address, OS user,
  transport field, credential, or policy override;
- a stale or revoked target, pairing, catalog, policy, profile, approval, or
  artifact authority; and
- any fresh installation or configuration without an explicitly enabled file
  profile.

## Security Truth And Threat Model

File transfer is a typed first-party capability. It must not be hidden inside
`system.exec.v1`, `shell.exec.v1`, terminal input, base64 in model JSON, or an
implicit synchronization mechanism.

An administrator profile that grants `/` can disclose credentials and replace
security-sensitive files. Approval wording, model instructions, path
normalization, and redacted logging do not sandbox that authority. The
meaningful boundaries are authenticated routing, target and profile grants,
current node policy, exact transfer authority, trusted approval, race-resistant
filesystem resolution, the privileged helper, and operating-system
permissions.

### Protected assets

P2 must protect:

- pairing keys, channel credentials, model credentials, approval secrets, and
  helper authentication;
- gateway spool contents and references;
- node and helper file-policy configuration;
- files outside the exact effective profile;
- another actor's artifacts, transfers, paths, metadata, and routed delivery;
- publication integrity, digest integrity, and no-blind-replay guarantees;
- logs, passive events, traces, approvals, and deployment evidence from file
  content leakage; and
- deny-by-default product and delegated profiles.

### Adversaries and failures

The design assumes:

- a model, prompt, file name, metadata field, or file content may be malicious;
- a chat participant may falsely claim to be the owner;
- paths may contain traversal, case collisions, Unicode ambiguity, symlinks,
  mount crossings, or rename races;
- a source file may change while it is hashed or streamed;
- a destination may change between validation and publication;
- the network may disconnect before or after transfer acceptance or atomic
  publication;
- duplicate, concurrent, canceled, expired, and restarted transfers may race;
- downloaded content may contain prompt injection, secrets, active media, or
  channel-hostile names;
- the unprivileged companion may be compromised; and
- an explicitly authorized administrator may deliberately replace a
  security-sensitive regular file.

The design does not claim to protect a host from a correctly authorized
administrator replacement. It protects the decision, scope, bytes, and
publication boundary from being broadened or replayed.

## Sources Of Truth

P2 creates no new general authority source.

| Decision | Authoritative source |
| --- | --- |
| Actor, route, session, and workspace ownership | Existing authenticated inbound identity and routed session |
| Agent access to target and file profile | Effective gateway agent target policy |
| Device and capability approval | Existing durable node registration and approved catalog |
| Current file capability and safe projection | Authenticated companion catalog and node-local file policy |
| Unprivileged path authority | Companion OS account plus node-local profile |
| Administrator path authority | Root-owned privileged-helper profile |
| Prepared, accepted, committed, and terminal state | P2 transfer record bound to existing node execution identity |
| Human approval | Existing trusted approval hook and durable interaction record |
| Gateway source or downloaded result bytes | Bounded gateway transfer spool |
| Node publication result | Companion ledger and filesystem publication proof |
| Channel delivery | Existing routed delivery coordinator and media store |

Possessing a discovery revision, transfer ID, artifact reference, or path is
not authority. Every prepare, stream, commit, status, cancel, and delivery
operation revalidates the relevant ownership intersection.

## Authority And Profile Model

Authority is the intersection of:

1. authenticated actor and route ownership;
2. routed session, workspace, and selected agent;
3. the agent's configured target grant;
4. the target's configured file-profile grant;
5. durable pairing and approved capability catalog;
6. current authenticated node or helper profile revision;
7. trusted approval policy when applicable;
8. the exact prepared transfer plan; and
9. node-local, helper, and OS enforcement.

The initial P2 target grant binds at most one file-profile alias per agent and
target. The model selects a target and supplies a path or artifact reference;
it does not select the profile. Supporting multiple selectable profiles is a
later contract because a selectable administrator profile would become a new
authority-bearing model input.

Delegation does not inherit file profiles. A subagent or durable task needs an
explicit independent grant and ownership binding.

### Operator-owned profile shape

The architecture-level shape is:

```yaml
nodes:
  targets:
    personal-vpn:
      file_profile: server-admin

node_file_policies:
  server-admin:
    enabled: false
    revision: server-admin-v1
    readable_roots:
      - /
    writable_roots:
      - /
    allow_create: true
    allow_overwrite: true
    follow_symlinks: false
    cross_mounts: false
    max_file_bytes: 1073741824
    approval:
      metadata: none
      read: required
      write: required
```

This is a shape contract, not a production recommendation. Exact Go types and
configuration placement are fixed in the implementation PR without changing
these authority semantics.

Validation rules are:

- missing profile, missing grant, empty roots, or `enabled: false` grants
  nothing;
- aliases and revisions are bounded and reject duplicate or case-colliding
  names;
- roots are absolute, cleaned, and resolved from root-owned or
  companion-owned node-local configuration;
- `/` is valid only when deliberately configured and never synthesized;
- `follow_symlinks` is `false` in P2 and configuration attempting `true` is
  rejected rather than partially implemented;
- pseudo-filesystems and special files remain denied even under `/`;
- limits may narrow hard ceilings but cannot exceed them;
- an authority-bearing profile change updates the authenticated safe
  projection, invalidates stale discovery, and requires existing catalog
  reapproval behavior; and
- the model cannot supply an OS identity, helper selection, root flag, roots,
  mount policy, approval mode, limits, or profile revision.

## Model-Visible Contract

P0 discovery is extended rather than replaced. A file-capable target may
advertise:

- `file.info.v1`, `file.upload.v1`, and `file.download.v1`;
- regular-files-only scope;
- model-safe root or destination aliases when the operator configures them;
- create and overwrite availability;
- effective maximum file and metadata sizes;
- effective timeout and concurrency ceilings;
- digest algorithm `sha256`;
- approval modes as policy metadata; and
- availability plus an opaque discovery revision.

Discovery excludes:

- node identity, public keys, endpoints, credentials, helper paths, IPC
  details, UID, GID, and raw policy;
- hidden filesystem roots or directory contents;
- source or destination file bytes;
- another actor's paths, artifacts, transfers, or profiles;
- catalog, plan, policy, or authority hashes; and
- unbounded examples or host enumeration.

An administrator profile may deliberately expose `/` as a usable path scope,
but only because the operator configured that exact model-visible scope.
Discovery remains advisory. Preparation rechecks target, pairing, catalog,
profile revision, path scope, artifact ownership, and approval policy.

## Initial Tool Surface

The first model-facing tools are separate typed operations.

### `nodes_file_info`

Input:

```json
{
  "target": "personal-vpn",
  "path": "/etc/example.conf",
  "discovery_revision": "dr_v1_opaque"
}
```

The bounded result may contain:

- canonical model-visible path;
- type `regular_file`;
- byte size;
- permission bits;
- owner and group identifiers only when profile policy marks them visible;
- modification timestamp with bounded precision;
- SHA-256 when requested and within hashing limits;
- current policy revision digest; and
- explicit unavailable, denied, stale, changed-during-read, or not-found
  status.

It never lists a directory or follows a symlink.

### `nodes_upload`

Input:

```json
{
  "target": "personal-vpn",
  "artifact_ref": "transfer-artifact://opaque",
  "destination": "/etc/example.conf",
  "publication": "replace",
  "discovery_revision": "dr_v1_opaque"
}
```

`publication` is exactly `create` or `replace`. The prepared plan binds the
source artifact identity, size, SHA-256, destination, publication mode,
effective profile revision, actor and route ownership, and expiry.

### `nodes_download`

Input:

```json
{
  "target": "personal-vpn",
  "source": "/var/tmp/example.png",
  "deliver": true,
  "discovery_revision": "dr_v1_opaque"
}
```

The result is an opaque gateway artifact reference with bounded metadata.
When `deliver` is true, the existing delivery coordinator sends the resulting
file or image once to the originating authorized channel, chat, and topic.
The binary is not embedded in ordinary model text.

The initial surface does not expose a generic `nodes_file` action multiplexer,
directory listing, glob, recursive option, archive option, resume token, remote
URL, hostname, credential, profile selector, or administrator flag.

## Gateway Transfer Spool

P2 adds a transfer-specific spool only because upload and download require a
bounded byte owner across streaming, restart reconciliation, and routed
delivery. It is not a general artifact framework.

Each record binds:

- random opaque artifact and transfer IDs;
- workspace, agent, actor, route, routed session, and originating tool call;
- direction, target alias, and effective profile revision;
- original safe filename and content type when known;
- declared and observed byte counts;
- SHA-256;
- lifecycle state, timestamps, and expiry;
- publication or download-commit evidence; and
- final media reference or delivery correlation when requested.

Spool files are owner-only, created without following links, written to unique
temporary files, synced before durable state advances, and atomically renamed
inside one configured spool filesystem. The index and bytes reconcile after a
restart without treating orphan bytes as authorized artifacts.

Opaque references are scoped capabilities, not bearer tokens. Resolution
requires all retained ownership dimensions to match. A path supplied by a
model is never accepted as a gateway source artifact.

Initial hard ceilings are:

- file size: 1 GiB;
- chunk payload: 256 KiB;
- safe filename: 255 UTF-8 bytes after validation;
- content type: 255 bytes;
- concurrent transfers: 8 per gateway and 2 per target/profile;
- transfer lifetime: 1 hour;
- completed spool retention: 24 hours by default, 7 days maximum; and
- metadata record: 16 KiB excluding content.

Profiles and gateway configuration default lower. Capacity exhaustion fails
before transfer acceptance. Cleanup never removes bytes referenced by a live
transfer or pending routed delivery.

Existing persistent `MediaStore` may own the final routed media reference, but
it does not become the authoritative transfer state machine. Integration must
define one ownership handoff and release each spool artifact exactly once.

## Transfer Framing

File bytes use authenticated bounded binary frames over the existing paired
WSS session. They do not use JSON strings or model-visible base64.

The versioned frame vocabulary is limited to:

- prepare metadata;
- accept or deny;
- ordered data chunk;
- chunk acknowledgement;
- commit request;
- committed result;
- cancel;
- status; and
- bounded failure.

Every frame binds the node session, transfer ID, direction, total size,
SHA-256, policy revision, and monotonic chunk sequence. Chunks are accepted at
most once and gaps are rejected. An acknowledgement proves only durable
acceptance at the defined boundary; it does not imply final publication or
routed delivery.

The connection uses bounded backpressure. It never buffers an unbounded file
in memory. Invalid length, sequence, transfer identity, digest, or lifecycle
state closes or rejects the transfer without advancing authority.

Cross-restart resumable streaming is excluded from P2. A transfer interrupted
before its commit boundary may be canceled and restarted as a new transfer
only through a new authorized model operation. A transfer at or beyond commit
is recovered by status and never automatically replayed.

## Upload Lifecycle And Publication

Upload lifecycle states are:

```text
prepared -> accepted -> streaming -> staged -> commit_requested
         -> published | failed | canceled | expired | unknown
```

State advancement is monotonic and idempotent.

1. The gateway resolves and pins the source spool artifact, computes or
   verifies size and SHA-256, resolves current authority, and persists an exact
   prepared plan.
2. Trusted approval, when required, binds that retained plan. Continuation
   cannot replace the source, destination, mode, bytes, digest, target,
   profile, actor, route, or expiry.
3. The node safely resolves the configured parent root and policy before
   accepting bytes.
4. Chunks stream into a unique temporary regular file in the destination
   directory or another same-filesystem policy-owned staging directory.
5. The node verifies exact size and SHA-256 and applies permitted mode metadata
   before marking the transfer `staged`.
6. `create` atomically publishes only when the final name does not exist.
   `replace` atomically replaces only a permitted existing regular file.
7. The node syncs required file and parent-directory metadata before recording
   `published`.
8. The companion ledger returns the durable publication result for duplicate
   commit or status requests.

Publication is the mutation commit boundary. A disconnect before publication
may prove no destination mutation and clean the staging file. A disconnect
during or after publication is `unknown` until the companion ledger proves
`published` or a terminal pre-publication outcome. It must never cause
automatic retransmission or republication.

No failure exposes a partial destination under the final name. Cleanup may
remove only the exact owned staging inode and never a caller-supplied path.

## Download Lifecycle And Gateway Commit

Download lifecycle states are:

```text
prepared -> accepted -> streaming -> received -> committed
         -> delivered | failed | canceled | expired | unknown
```

1. Preparation binds the canonical source, current metadata identity,
   authority, target, profile, ownership, expiry, and optional delivery route.
2. The node opens the source through the safe resolver, proves it is a regular
   file, captures identity metadata, and streams from the already opened
   descriptor.
3. If identity or size changes inconsistently during the read, the transfer
   fails as `source_changed`; it is not represented as a stable snapshot.
4. The gateway streams into a unique spool temporary file, verifies byte count
   and SHA-256, syncs it, and atomically commits it under an opaque artifact
   reference.
5. Spool commit is the download commit boundary. Duplicate frames or status
   recovery return the same artifact identity.
6. Optional channel delivery consumes that exact artifact through the existing
   routed delivery coordinator and records its idempotent delivery
   correlation.

A disconnect before gateway commit may discard the temporary spool file. A
disconnect during or after commit recovers the exact artifact or reports
`unknown`; it never starts another download automatically. Delivery failure
does not redownload the node file. Delivery retry, when supported by the
existing coordinator, reuses the committed artifact and the same delivery
identity.

## Filesystem Resolution And Race Safety

Lexical path cleaning is not an authorization boundary. All node and helper
operations use descriptor-relative or equivalent race-resistant resolution
from a pre-opened configured root.

The implementation must:

- reject NUL, empty, malformed, traversal, and overlong paths;
- reject symlinks in every component and at the final component;
- reject non-regular source and destination files;
- reject sockets, FIFOs, devices, procfs, sysfs, and other configured
  pseudo-filesystems;
- keep resolution beneath the configured root even across concurrent rename;
- enforce `cross_mounts: false` when configured;
- bind an opened source descriptor rather than reopening a validated path;
- bind destination parent and final basename before staging and publication;
- perform create-without-replace and replace-existing semantics atomically;
- revalidate the destination type and policy at commit; and
- return bounded denial codes without revealing neighboring paths.

Linux privileged operations use `openat2` constraints where supported and a
tested descriptor-walk fallback only if it preserves the same invariants.
Unprivileged Linux and macOS support must prove equivalent no-follow and
beneath-root behavior for their admitted operations. Privileged macOS access
is not admitted.

Hard-link behavior is explicit: P2 may read a regular file with multiple links
when policy permits, but replacement publishes a new inode at the authorized
name and never claims to update other links. Upload staging files are created
with one link and owner-only permissions.

## Linux Privileged File Helper

The complete companion remains unprivileged. Administrator profiles use a
minimal root-owned local helper.

The helper:

- is installed, owned, and configured by root outside companion-writable
  paths;
- listens only on a root-owned local IPC endpoint;
- validates peer credentials against the configured companion account;
- resolves profile aliases only from root-owned policy;
- accepts bounded versioned requests for metadata, open-read, create-stage,
  write-chunk, commit-create, commit-replace, cancel, and status;
- binds every request to a transfer identity, current profile revision,
  canonical relative path, operation, size, SHA-256, publication mode, and
  expiry;
- retains the minimum state required to make commit and duplicate status
  truthful;
- returns a bounded safe profile projection for authenticated catalog
  binding; and
- follows the same metadata-only logging contract.

It accepts no shell text, argv, environment, arbitrary UID/GID, caller-selected
root, arbitrary file descriptor, service action, package action, scheduler,
network listener, remote enrollment, credential store, or updater.

Compromise of a companion granted an administrator file profile can exercise
the typed authority of that profile. The helper boundary keeps unrelated
remote protocol and companion functionality outside UID 0; it does not make a
`/` profile harmless.

## Approval Policy

Approval mode is operator-owned profile configuration. A model cannot select
or relax it.

Architecture values are:

- `none`;
- `read_required`; and
- `write_required`.

The initial production administrator profile requires approval for upload
create/replace and download of root-owned content. Metadata-only inspection may
be configured without approval when its visible fields and path scope are
already intentionally granted.

Approval binds:

- actor, agent, authenticated route, routed session, workspace, and tool call;
- target and current paired node;
- effective file-profile revision;
- operation and direction;
- canonical source or destination path;
- exact source artifact identity for uploads;
- publication mode;
- declared size and SHA-256;
- requested visible metadata;
- optional routed delivery destination;
- plan identity and expiry.

The approval presentation shows the authenticated owner a safe exact path,
operation, size, digest, overwrite consequence, target, and profile blast
radius. It does not accept these values from approval prose.

A normal chat response, model-generated text, pairing approval, or possession
of an interaction ID cannot become trusted approval. Continuation reuses the
same persisted plan. Changed input, ownership, profile, policy, catalog,
artifact, path, digest, size, mode, route, or expiry fails before transfer
acceptance.

Durable approval records exclude file bytes, credentials, hidden policy,
helper authentication, neighboring paths, and directory contents.

## Cancellation, Recovery, And Exactly-Once Effects

Cancellation uses the existing actor-scoped invocation ownership pattern and
the P2 transfer record. It does not add a generic cancellation subsystem.

Bounded outcomes are:

- `cancel_requested`;
- `canceled` before commit;
- `already_committed`;
- `already_terminal`;
- `unknown`; and
- `denied`.

`canceled` proves that no final upload publication or gateway download commit
occurred and that the exact staging file is no longer live. Cancellation after
commit never rolls back or deletes the published file or committed artifact.

Duplicate prepare, chunk, commit, cancel, status, and delivery requests are
idempotent under the same ownership and transfer identity. Conflicting bytes,
digest, path, mode, or ownership under an existing identity fail closed.

Restart reconciliation classifies:

- prepared but unaccepted work as safely terminal or restartable only through
  a new authorized operation;
- accepted or streaming work as canceled when both sides can prove
  pre-commit cleanup;
- commit-requested work as recoverable or `unknown`;
- published uploads as the exact durable publication result;
- committed downloads as the exact durable artifact reference; and
- requested channel delivery through its existing idempotent delivery record.

No completed or uncertain upload, download, cancellation, or delivery creates
an automatic transfer replay path.

## Redaction, Content, And Retention

Passive file-transfer events may contain:

- bounded actor, agent, session, target, profile, transfer, and delivery
  correlations;
- operation, lifecycle state, safe denial code, byte count, duration,
  truncation, digest algorithm, and a shortened non-reversible digest
  correlation;
- whether approval, overwrite, privileged helper, and routed delivery applied;
  and
- commit or uncertainty classification.

They exclude:

- file bytes, previews, extracted text, image data, and terminal content;
- full SHA-256 when it would become a reusable content identifier outside the
  authorized result;
- credentials, environment values, pairing identity, endpoints, helper paths,
  IPC material, UID/GID, and hidden roots;
- directory listings, neighboring paths, unrestricted policy, and raw
  approval text; and
- complete catalog, plan, artifact, policy, or authority records.

Ordinary logs, traces, approval records, and operational evidence follow the
same exclusion. A diagnostic trace may correlate lifecycle state but never
retain transferred content.

The authorized model result may receive bounded metadata and an opaque artifact
reference. The authorized routed channel may receive the requested file bytes.
No other model session, actor, route, workspace, or channel can resolve the
reference or content.

Content scanning, archive inspection, antivirus, DLP, MIME trust, image safety,
and prompt-injection classification are not P2 security boundaries. Filenames
and content types are sanitized for presentation, and channels treat content
as untrusted bytes.

Completed spool retention defaults to 24 hours and is capped at 7 days.
Metadata retention uses the existing bounded invocation policy and is capped
at 30 days. Cleanup is deterministic, does not follow links, and cannot remove
active transfer, approval, status-recovery, or delivery authority.

## Configuration, Migration, And Defaults

Fresh installations expose no file capabilities.

- Gateway target policy has no file-profile grant by default.
- Companion file policies are absent and disabled by default.
- Administrator-helper policy and socket are absent by default.
- Installing the helper binary does not install, enable, or start root
  authority.
- Existing node records and protocol-v1 clients without file descriptors
  continue without migration and advertise no file capability.
- Adding or changing a file capability changes authenticated catalog authority
  and requires normal reapproval.
- Spool creation uses a new private directory with explicit quota and
  retention. Failure to create or validate it disables file tools rather than
  falling back to a broad temporary directory.
- Invalid or partially migrated configuration fails closed and never silently
  chooses `/`, overwrite, approval-free access, or a helper.

No compatibility shim is required for an unshipped file-transfer contract.
During P2 development, update the one current schema directly rather than
introducing v2 messages or dual transfer paths.

## Real-Process Evidence

Tests use a real gateway, authenticated WSS admission, companion process,
filesystem boundaries, and helper test process where privilege is required.
Scripted model fixtures choose tools only through model-visible discovery and
cannot inject hidden profile, helper, node, or transport facts.

Required evidence is:

1. a text config uploads to an unprivileged root and downloads with matching
   size and SHA-256;
2. a binary image round-trips and is delivered exactly once to the original
   routed channel, chat, and topic;
3. `create` refuses an existing path and `replace` atomically replaces one
   permitted regular file without exposing partial bytes;
4. unauthorized actor, agent, route, routed session, workspace, target,
   profile, path, traversal, symlink, mount, size, digest, overwrite, expiry,
   special file, artifact, duplicate, and concurrent request each fail closed;
5. trusted approval continuation reuses the exact prepared transfer, while
   changed or stale authority fails before acceptance;
6. deterministic barriers cover cancel-versus-commit,
   disconnect-versus-publication, restart-versus-cleanup, duplicate commit,
   and delivery retry without sleep-only assertions;
7. status recovery returns one published upload, one committed artifact, or an
   explicit unknown outcome without replay;
8. a root-owned helper fixture proves metadata, read, create, and atomic
   replacement under one administrator profile while the complete companion
   remains unprivileged;
9. helper peer, profile, revision, path, digest, size, mode, expiry, and request
   bounds fail independently;
10. passive events, logs, traces, approval records, and deployment evidence
    satisfy the redaction contract; and
11. fresh and delegated profiles advertise and permit no file capability.

Production proof is permitted only after all implementation PRs merge and the
trusted approval prerequisite is satisfied. It uses one explicitly enabled
personal-server profile, reversible non-sensitive fixtures, one unprivileged
round trip, one root-owned config fixture replacement with backup, one image
delivery, one denied actor, one stale-profile drill, and one disconnect or
recovery drill. It records no transferred content.

## Dependency-Ordered Delivery

Each item is a separate merge unit based on the latest merged `main`.
Dependent work does not begin until its predecessor merges.

1. **Admission contract:** this architecture-only PR.
2. **Spool and framing:** add the bounded gateway transfer spool, protocol
   descriptors, authenticated binary framing, quotas, cleanup, and restart
   reconciliation without model tools or privileged access.
3. **Unprivileged transfer:** add companion regular-file metadata, upload, and
   download for configured unprivileged roots with safe resolution, atomic
   publication, ledger recovery, cancellation, and real-process tests.
4. **Model tools and delivery:** add `nodes_file_info`, `nodes_upload`, and
   `nodes_download`, P0 discovery projection, exact approval continuation,
   actor-scoped status/cancel behavior, artifact handoff, and one-time routed
   file/image delivery.
5. **Administrator helper:** add the minimal Linux local helper and consume it
   for one explicit administrator profile; do not add service, shell, updater,
   or general file-descriptor APIs.
6. **Complete E2E and deployment:** prove the authorized and denied vertical
   slices, redaction, commit races, merged-main behavior, deny-by-default
   deployment, backups, rollback, health, journals, persistence, and
   duplicate-effect absence.

No PR may quietly absorb a later item merely because its types are convenient.
A prerequisite abstraction must have the current item's concrete consumer and
tests in the same merge unit.

## Exact Definition Of Done

P2 is complete only when every gate below has authoritative evidence.

### Gate 1: disabled and bounded authority

- fresh installs and delegated/product profiles expose no file tool, file
  profile, spool reference, or helper authority;
- profiles are out-of-band, operator-owned, schema-validated, revision-bound,
  and absent or disabled by default;
- the model cannot select profile, roots, helper, OS user, administrator flag,
  approval mode, transport, or limits; and
- authority changes invalidate stale discovery and prepared work before
  acceptance.

### Gate 2: ownership intersection

- actor, agent, authenticated route, routed session, workspace, target,
  profile, artifact, path, operation, and tool-call ownership are bound;
- another value in each dimension is denied independently;
- opaque IDs and paths are not bearer authority; and
- delegation receives no implicit file access.

### Gate 3: regular-file and path safety

- only bounded regular files are admitted;
- traversal, symlinks, special files, pseudo-filesystems, forbidden mount
  crossings, malformed paths, and rename races fail closed;
- source reads use one safely opened descriptor; and
- destination staging and publication remain beneath the configured root.

### Gate 4: transfer integrity and publication

- text and binary round trips match declared size and SHA-256;
- chunks are ordered, bounded, at most once, and backpressured;
- create and replace semantics are explicit and atomic;
- partial content is never published as a final destination or committed
  artifact; and
- conflicting duplicate identities fail closed.

### Gate 5: approval and freshness

- approval binds the exact retained transfer plan, bytes, digest, path,
  publication mode, profile revision, ownership, route, and expiry;
- continuation cannot replace or broaden any bound field;
- stale, changed, expired, revoked, or self-authored approval fails before
  acceptance; and
- no model or ordinary chat response can self-approve.

### Gate 6: cancellation, restart, and no replay

- cancellation and status are actor-scoped and idempotent;
- deterministic tests cover cancel, commit, completion, disconnect, cleanup,
  restart, and delivery races;
- committed and unknown outcomes are truthful; and
- no completed or uncertain upload, download, cancellation, or delivery has an
  automatic replay path.

### Gate 7: model tools and routed delivery

- discovery teaches an authorized model enough to call all three tools without
  hidden host facts;
- `nodes_file_info` exposes only permitted bounded metadata;
- upload and download return stable status without duplicate execution; and
- a downloaded file or image is delivered exactly once to the original
  authorized channel, chat, and topic without a duplicate completion reply.

### Gate 8: Linux administrator boundary

- the full companion remains unprivileged;
- the helper is root-owned, local-only, peer-authenticated, profile-bound, and
  limited to admitted typed operations;
- one explicit administrator profile reads and atomically replaces an approved
  root-owned Linux regular file; and
- missing, altered, unconfigured, or unauthorized helper/profile requests
  fail closed.

### Gate 9: redaction and retention

- file bytes and secrets are absent from logs, events, traces, approvals, and
  operational evidence;
- artifact resolution and routed content are visible only to the authorized
  result or destination;
- spool and metadata retention are bounded and cleanup is race-safe; and
- filenames, content types, and errors cannot leak neighboring paths or hidden
  policy.

### Gate 10: validation, merge, and deployment

- focused tests, relevant race tests, repository formatting and lint, full
  tagged Go tests, frontend validation when touched, and real-process E2E pass;
- every in-scope PR receives required CI and review and is confirmed merged;
- architecture and operations docs match final behavior and list residual
  limitations;
- latest merged `main` is validated;
- gateway, companion, and helper artifacts are built from recorded reviewed
  revisions;
- production deploys canary-first with backups and tested rollback;
- health, readiness, pairing, persistence, journals, quota/cleanup, and
  duplicate-effect checks pass; and
- production remains deny-by-default except an exact explicitly approved
  operator profile.

## Mandatory Completion And Stop Condition

When Gates 1 through 10 all have authoritative evidence, every in-scope PR is
confirmed merged, and deployment verification is recorded, P2 is complete.

At that exact point the implementing agent must:

1. mark the P2 goal complete;
2. stop implementation, review polling, deployment changes, and roadmap
   expansion;
3. return one concise report listing PRs, merge commits, validation, deployment
   evidence, rollback, enabled authority, and residual limitations; and
4. defer every additional idea without opening another PR.

Completion does not authorize P3, terminal clients, directory sync, optional
refactors, performance work, more platforms, broader helpers, or follow-up
cleanup. No “one more prerequisite” or optional improvement may continue under
the completed P2 goal.

## Non-Goals And Stop Conditions

P2 does not implement or authorize:

- directory listing, recursion, filesystem synchronization, delta transfer,
  watch mode, mounts, or remote workspaces;
- archive creation or extraction, compression negotiation, content-defined
  chunking, deduplication, or cross-restart resume;
- symlink following, devices, sockets, FIFOs, procfs, sysfs, or arbitrary file
  descriptors;
- file transfer through exec, shell, terminal, model JSON, clipboard, MCP, or
  channel text;
- a general artifact platform, object store, workflow engine, scheduler,
  delivery subsystem, authority store, or transport;
- service administration, package management, companion update, SSH,
  bootstrap, port forwarding, or terminal-client work;
- a root-run complete companion or a helper accepting shell, argv,
  environment, services, updater operations, arbitrary roots, or caller-chosen
  identity;
- automatic profiles, automatic approval, chat-claimed ownership, or a model
  broadening authority;
- privileged macOS or Windows filesystem support; or
- production enablement before trusted approval and exact operator profile
  decisions are recorded.

Stop the affected PR and return to admission if implementation requires an
item above, if a partial destination or artifact must be exposed, if a
post-commit mutation would be replayed, if path safety depends only on lexical
checks, if raw content must enter passive evidence, or if the privileged helper
grows beyond typed regular-file operations.

Use the autonomous-PR architecture checkpoint before continuing when review
causes four substantive fix cycles, doubles production scope, crosses two
unplanned subsystems, or challenges the same authority, commit, path, or replay
invariant on three successive heads.

Green unit tests alone do not complete P2. A transfer that bypasses actor or
route binding does not complete P2. A root profile without trusted approval
does not complete P2. A successful byte copy without truthful commit and
recovery semantics does not complete P2.
