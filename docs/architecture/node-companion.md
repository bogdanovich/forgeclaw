# Node Companion Architecture

Status: proposed

This document defines a first-party ForgeClaw node architecture for running
bounded capabilities on remote machines. The initial companions target Linux
and macOS. Windows, mobile devices, cameras, and compatibility adapters are
future extensions of the same model, not requirements for the first release.

The design follows the distributed node shape used by OpenClaw rather than the
single selected terminal-backend shape used by Hermes. It does not adopt the
OpenClaw wire protocol. Core node concepts stay transport-neutral so a future
adapter can translate an external protocol without changing agent, policy, or
execution code.

## Decision Summary

- A node is a lightweight capability host that makes an outbound connection to
  the ForgeClaw gateway.
- Nodes are discovered dynamically after identity pairing and advertise a
  versioned command catalog.
- Machine placement, command execution, and authorization are separate
  concerns.
- Protocol version 1 uses JSON request, response, and event envelopes over a
  long-lived WebSocket. Production and every non-loopback connection require
  TLS (`wss://`); plaintext is limited to explicit loopback development and
  tests.
- The model can reference only paired node identities or operator-defined
  aliases allowed for its agent. It cannot provide an arbitrary hostname or
  credential.
- The node is the final enforcement boundary for actions on its machine.
- Generic execution uses an argv-based command contract. Privileged service
  management uses typed commands and an optional narrow privileged helper,
  never an unrestricted root shell.
- Accepted mutating invocations are not automatically replayed after an
  uncertain disconnect or crash.
- The companion contains no LLM runtime, provider credentials, session history,
  or channel integrations.

## Goals

1. Let an authorized agent list and inspect connected machines.
2. Run bounded commands on an explicitly selected Linux or macOS node.
3. Inspect and manage allowlisted services such as a VPN daemon.
4. Work when a node is behind NAT by using an outbound gateway connection.
5. Keep credentials and machine-local authority on the node when possible.
6. Support gateway, node, and container execution without conflating them.
7. Preserve clear policy, approval, audit, cancellation, and crash semantics.
8. Keep the first-party companion small enough to run on low-resource hosts.
9. Leave a stable adapter boundary for possible OpenClaw node compatibility.
10. Allow non-shell capabilities such as camera, location, or browser control
    to be added later without redesigning the protocol.

## Non-Goals

- Full OpenClaw protocol or mobile-application compatibility in version 1.
- An arbitrary remote root shell.
- Transparent synchronization of an entire ForgeClaw workspace to every node.
- Moving the LLM, channel gateway, memory, or session store onto companion
  nodes.
- General distributed scheduling, clustering, or workload migration.
- Durable background jobs in the first execution milestone.
- Windows support in the MVP.
- Treating command scanners, prompts, or approvals as an OS sandbox.

## Use Cases

### VPN health and recovery

An operator pairs a node named `vpn-box`, allows the main agent to use it, and
configures `wg-quick@wg0` as a manageable service. The agent can list nodes,
read service status and recent bounded logs, request a restart, and report the
verified post-restart state. A restart may require human approval even when
status inspection does not.

### Remote development machine

A macOS or Linux laptop runs the companion under the user's account. The agent
can run allowlisted development commands in configured working roots. A node
disconnect removes it from the set of runnable targets without removing its
durable identity or policy.

### Sandboxed build host

A connected Linux node can route an invocation into a Docker executor local to
that node. The gateway selects the machine; the node selects the configured
execution environment. Container isolation is therefore orthogonal to remote
placement.

## Terminology

- **Gateway**: the trusted ForgeClaw control plane that owns agents, sessions,
  policy orchestration, and connected-node state.
- **Node**: a paired machine or device that advertises and executes bounded
  capabilities.
- **Companion**: the lightweight first-party process running on a node.
- **Target**: an operator-defined execution destination such as the gateway, a
  paired node, or a future static SSH endpoint.
- **Executor**: the node-local mechanism that runs a command, initially local
  and later optionally Docker.
- **Capability**: a high-level family such as `system`, `service`, `browser`,
  or `camera`.
- **Command**: a versioned typed operation within a capability, such as
  `system.exec.v1` or `service.status.v1`.
- **Invocation**: one uniquely identified request to execute a command.
- **Execution plan**: the canonical, policy-reviewed representation that is
  bound to an invocation before execution.

## Architectural Principles

### Placement is not isolation

Selecting `vpn-box` answers where work runs. Selecting `local`, `bubblewrap`,
or `docker` answers how it runs on that machine. These choices must remain
separate:

```text
agent turn
    |
    v
target policy: gateway | node:vpn-box | node:build-box
    |
    v
node command policy
    |
    v
executor: local | bubblewrap | docker
```

ForgeClaw's existing `pkg/isolation` remains the local subprocess isolation
implementation. A node executor may reuse the same package where its platform
supports it.

### Capabilities are typed

The protocol transports named commands with schemas, not unstructured RPC
method invention. A shell-like command is one capability among many. Mobile
and hardware nodes therefore do not need to pretend to support a terminal.

### Policy follows authority

The gateway may narrow access, request approval, and bind agents to nodes. The
node still validates every invocation against its local policy. Gateway
authorization cannot broaden node-local authority.

### Identity is not presentation

A stable cryptographic device ID is the identity. Display names and aliases
are operator-facing metadata and are never sufficient for authorization.

### Uncertain execution fails closed

After a node acknowledges a mutating invocation, a lost connection does not
mean the gateway can safely retry it. The gateway reports an uncertain outcome
unless the node's durable invocation ledger can prove a terminal result.

## Component Boundaries

The core domain must not depend on WebSocket frame structs or OpenClaw types.
A target driver sits above node-specific transport so future static SSH
execution can reuse plans and policy without pretending to be a connected
node. Illustrative interfaces are:

```go
type TargetDriver interface {
    Kind() string
    Describe(ctx context.Context, target TargetRef) (TargetSnapshot, error)
    Dispatch(ctx context.Context, plan ExecutionPlan) (InvocationResult, error)
    Cancel(ctx context.Context, target TargetRef, invocationID string) error
}

type NodeRegistry interface {
    List(filter NodeFilter) []NodeSnapshot
    Resolve(ref string) (NodeSnapshot, error)
    SetConnected(id NodeID, session NodeSession) error
    SetDisconnected(id NodeID, reason string)
}

type NodeInvoker interface {
    Prepare(ctx context.Context, req InvocationRequest) (ExecutionPlan, error)
    Invoke(ctx context.Context, plan ExecutionPlan) (InvocationResult, error)
    Cancel(ctx context.Context, nodeID NodeID, invocationID string) error
}

type NodeTransport interface {
    SendRequest(ctx context.Context, nodeID NodeID, req TransportRequest) (TransportResponse, error)
    SendEvent(ctx context.Context, nodeID NodeID, event TransportEvent) error
}

type CapabilityHandler interface {
    Descriptor() CommandDescriptor
    Prepare(ctx context.Context, input json.RawMessage) (PreparedCommand, error)
    Execute(ctx context.Context, prepared PreparedCommand, sink EventSink) CommandResult
}
```

These interfaces are directional boundaries, not a required one-interface-per-
file layout. Implementations should remain cohesive and package splits should
wait until the boundaries prove stable.

The expected ownership is:

| Component | Owns |
| --- | --- |
| Agent tools | User-facing node listing, target selection, and invocation requests |
| Target service | Named target resolution and dispatch through an allowed driver |
| Target driver | Placement-specific dispatch for gateway, paired node, or future SSH |
| Gateway node service | Registry, pairing, sessions, routing, policy intersection, audit |
| Transport adapter | Framing, version negotiation, liveness, request correlation |
| Companion runtime | Node identity, local policy, handlers, invocation ledger |
| Capability handler | Input validation, canonical preparation, execution semantics |
| Executor | OS process lifecycle or container execution |

## Transport Choice

### JSON over WebSocket

Protocol version 1 uses JSON text frames over WebSocket.

Reasons:

- nodes initiate outbound connections through common proxies, NAT, and
  firewalls;
- the connection is naturally bidirectional for invocation, progress,
  cancellation, and capability updates;
- Go, Swift, Kotlin, JavaScript, and embedded platforms have mature clients;
- JSON is inspectable and suitable for low-volume control-plane traffic;
- schemas can generate clients without coupling the core to a code generator;
- a future adapter can map the envelopes to another protocol.

gRPC over WebSocket is not selected. It is not a standard gRPC transport, and
gRPC-Web does not provide the same general bidirectional streaming semantics as
native gRPC. Native gRPC over HTTP/2 is strong for controlled server-to-server
systems, but introduces proxy, mobile, and generated-client complexity without
a meaningful throughput benefit here.

The transport interface permits a future native gRPC or OpenClaw adapter. Wire
encoding is therefore a deployment concern, not the node domain model.

### TLS and endpoint trust

The MVP does not send node traffic over an open network in plaintext.
Production companions accept only `wss://` endpoints. The gateway can terminate
TLS itself or sit behind an operator-managed TLS reverse proxy. Companion trust
uses one of:

- a certificate chain rooted in the operating-system trust store;
- an explicitly configured private CA;
- a certificate or SPKI fingerprint transferred out of band during enrollment.

There is no production `insecure_skip_verify` fallback. A self-signed endpoint
must be pinned explicitly. An SSH bootstrap flow may install the expected
gateway fingerprint alongside the companion configuration.

Application-level Ed25519 identity and challenge signatures remain necessary:
TLS authenticates the endpoint and encrypts transport, while node signatures
bind a stable device identity to pairing. Mutual TLS is not required for the
MVP, but can be added as another transport-auth adapter without replacing node
identity.

Plain `ws://` is accepted only on loopback under an explicit development or
test setting. Binding a plaintext listener to a non-loopback address fails
closed. Integration tests cover both the rejection and a real WSS handshake.

### Frame envelopes

Every frame has a bounded size and one of three shapes:

```json
{"type":"request","id":"req_...","method":"node.invoke","params":{}}
{"type":"response","id":"req_...","ok":true,"result":{}}
{"type":"event","event":"node.invoke.progress","payload":{}}
```

Errors use stable machine-readable codes plus mutable human-readable messages:

```json
{
  "type": "response",
  "id": "req_...",
  "ok": false,
  "error": {
    "code": "POLICY_DENIED",
    "message": "service restart is not allowed"
  }
}
```

Side-effecting requests carry an idempotency key. Request IDs correlate one
transport exchange; idempotency keys identify one logical side effect.

### Versioning

The handshake sends `min_protocol` and `max_protocol`. The gateway chooses one
overlapping version or rejects the connection. Commands are independently
versioned in their IDs, so adding `service.status.v2` does not require changing
the transport protocol.

Version 1 compatibility rules should be published with machine-readable JSON
schemas and conformance fixtures. Additive optional fields are allowed. Removing
fields, changing meaning, or broadening authority requires a new protocol or
command version.

Large binary payloads are not placed in ordinary JSON frames. A later artifact
extension can use bounded chunks or gateway-issued upload URLs. Capability
metadata advertises whether that extension is available.

### Output and artifacts

JSON framing does not restrict rich text. Standard encoders preserve UTF-8 and
escape quotes, backslashes, newlines, and control characters without custom
string rewriting. Callers provide ordinary strings and must never construct
protocol JSON through concatenation.

Command output is streamed as ordered bounded events rather than accumulated in
one response:

```json
{
  "type": "event",
  "event": "node.invoke.output",
  "payload": {
    "invocation_id": "inv_...",
    "stream": "stdout",
    "sequence": 4,
    "content_type": "text/plain; charset=utf-8",
    "text": "service is active\n"
  }
}
```

The final result can contain typed content references:

```json
{
  "content": [
    {"type":"text","content_type":"text/markdown","text":"## Result\nHealthy"},
    {"type":"json","value":{"active":true}},
    {"type":"artifact","artifact_id":"art_...","content_type":"image/png","size":12345,"sha256":"..."}
  ]
}
```

Transport fidelity and presentation safety are separate. The gateway retains a
bounded raw result where policy permits, while terminal UIs, logs, channels,
and model-facing projections escape control sequences, redact secrets, and
truncate according to their own contracts. Sanitization must not silently
change the bytes whose digest or artifact identity is reported.

Arbitrary binary data uses a negotiated artifact extension:

1. a JSON `artifact.begin` request declares ID, size, media type, and digest;
2. bounded WebSocket binary frames transfer chunks with artifact ID and
   sequence metadata;
3. `artifact.commit` verifies byte count and SHA-256 before publication.

Gateway-issued upload URLs may be offered as an optimization for large objects,
but are not the only transfer path. Small binary values may use base64 only
under a strict negotiated limit. Unknown encodings fail closed instead of being
decoded heuristically.

## Identity, Pairing, and Connection

### Node identity

On first start, the companion creates an Ed25519 keypair in its private state
directory. The node ID is derived from the public key. Private keys never leave
the node.

The gateway sends a short-lived nonce. The companion signs a transcript that
includes:

- the nonce;
- protocol range;
- node ID;
- client version;
- requested role;
- capability catalog hash.

The gateway verifies the signature and either authenticates an existing paired
node or creates a pending pairing request. Pairing approval records the public
key, operator-assigned aliases, agent bindings, and allowed command families.

WSS is mandatory for non-loopback connections. Development plaintext requires
an explicit opt-in and must not issue reusable credentials. Node key signatures
authenticate the device; TLS authenticates and protects the transport.

### Pairing lifecycle

1. Companion connects and receives a challenge.
2. Companion sends signed identity and capability claims.
3. Unknown identity becomes `pending_pairing` and cannot execute work.
4. An authorized operator approves or denies the node.
5. The companion reconnects and signs a fresh challenge with the approved key.
6. The gateway verifies that the exact key remains approved and non-revoked,
   then establishes the connected session.
7. The MVP binds command-surface approval to the authenticated catalog hash.
   Any catalog change suspends command execution until renewed approval; a
   future descriptor-level approval model may allow verified narrowing without
   reapproving unrelated commands.

The MVP does not add a gateway bearer token beside the device key. A token
stored with that same key would not be an independent authentication factor,
but would add credential delivery and crash-recovery state. Revocation is the
durable registry decision checked on each reconnect and heartbeat. A future
deployment may add mTLS or a separately protected operator credential when it
provides an independent trust boundary.

Pairing is not per-command approval. It establishes device identity and the
maximum command surface that later policy may use.

Gateway admission is opt-in through `nodes.enabled` and is mounted at
`/nodes/v1/ws`. Plain `ws://` admission remains disabled unless
`nodes.allow_loopback_plaintext` is explicitly enabled, and that exception is
accepted only from a loopback peer. Unknown signed identities are persisted as
bounded `pending_pairing` records; this admission path does not approve a node
or expose any executable command surface.

### Liveness

The companion maintains one connection with heartbeat and bounded exponential
backoff plus jitter. The gateway records connected time, last seen time,
software version, platform, and disconnect reason.

Display state distinguishes at least:

- `pending_pairing`;
- `connected`;
- `disconnected`;
- `revoked`;
- `incompatible`;
- `degraded`.

## Capability Catalog

Each node advertises descriptors such as:

```json
{
  "name": "service.status.v1",
  "input_schema": {"type":"object"},
  "output_schema": {"type":"object"},
  "risk": "read",
  "supports_progress": false,
  "supports_cancel": false
}
```

The capability family is derived from the first segment of the command name;
it is not repeated as independently writable descriptor data. For example,
`service.status.v1` belongs to the `service` capability.

Descriptors are claims. The gateway intersects them with paired command
approval, agent policy, and operator configuration. A node cannot gain agent
visibility merely by advertising a new command.

The same catalog supports capabilities that are not process execution. A
future node-hosted MCP runtime can publish validated tool descriptors whose
invocations map to a versioned command such as `mcp.tools.call.v1`. A browser
adapter can publish typed commands such as `browser.navigate.v1`,
`browser.snapshot.v1`, and `browser.screenshot.v1`; screenshots use the
artifact contract rather than embedding bytes in tool JSON. Mobile or appliance
companions can advertise camera, location, or sensor commands without exposing
`system.exec.v1` at all.

Dynamic descriptor updates are bounded and authenticated. Adding or broadening
a command remains subject to gateway command-surface approval and node-local
policy. Disconnecting a node removes its dynamic commands from new agent tool
snapshots.

MVP commands:

| Command | Purpose | Default risk |
| --- | --- | --- |
| `node.info.v1` | OS, architecture, version, uptime, executor inventory | read |
| `system.which.v1` | Resolve one executable under node policy | read |
| `system.exec.v1` | Execute canonical argv in an allowed working root | write |
| `service.status.v1` | Read status for an allowlisted service | read |
| `service.logs.v1` | Read bounded recent logs for an allowlisted service | read |
| `service.action.v1` | Start, stop, restart, or reload an allowlisted service | privileged |

`system.exec.v1` accepts argv rather than a shell command string by default:

```json
{
  "argv": ["git", "status", "--short"],
  "cwd": "/srv/project",
  "timeout_seconds": 60,
  "env": {}
}
```

An explicit shell command can be a later command family with stricter policy.
It must not be smuggled through an argv field such as `sh -c` when local policy
disallows shell execution.

## Invocation Lifecycle

The gateway prepares a canonical execution plan before approval or dispatch.
The plan binds:

- invocation ID and idempotency key;
- node ID, approved catalog hash, and command version;
- normalized command input;
- agent, session, and requesting actor identity;
- executor selection;
- working directory and environment names;
- timeout and output bounds;
- current policy revision;
- plan hash.

The node recomputes and validates relevant fields. It does not trust a human-
readable summary as executable authority.

The plan hash is a canonical binding digest, not an origin signature. The
gateway keeps the expected digest in the approval or invocation record outside
the mutable plan and compares it again before dispatch. Pinned WSS authenticates
the gateway transport. If a future external broker or offline plan format
crosses a separate trust boundary, that format requires a distinct signed
approval envelope; re-labeling this digest as a signature would not provide
that protection.

States are:

```text
created -> prepared -> accepted -> running -> succeeded
                  |          |          |-> failed
                  |          |          |-> cancelled
                  |          |-> denied
                  |-> expired
```

`accepted` is the no-blind-retry boundary for mutating commands. The node stores
a small bounded invocation ledger before acknowledging acceptance. Duplicate
requests with the same idempotency key return the recorded state or terminal
result. A key reused with a different plan hash is rejected.

After reconnect, the gateway may query an invocation by ID. It may not submit a
new mutation merely because the previous result is unavailable. If the node
cannot prove whether execution occurred, the outcome is `unknown`, requiring
operator investigation.

Cancellation is best effort. It never rewrites a completed result, and timeout
does not imply that a remote child was successfully killed. Result metadata
reports whether termination was confirmed.

## Execution and Service Management

### Lightweight companion

The companion runtime lives in shared Go packages but is distributed through a
dedicated `cmd/picoclaw-node` build target. Go therefore links only the node
runtime and its dependencies, producing a smaller binary than the gateway. Its
dependency graph does not import the agent, model-provider, channel, MCP-host,
or workspace-memory runtimes:

```text
picoclaw-node run
picoclaw-node install
picoclaw-node status
picoclaw-node uninstall
```

The full `picoclaw` CLI owns gateway-side node administration commands, while
the remote machine requires only `picoclaw-node`. Shared protocol, identity,
policy, and capability packages prevent implementation duplication. A CI import
boundary test protects the slim binary from accidentally acquiring gateway
dependencies.

It should contain only:

- gateway transport and reconnect logic;
- device identity and pairing state;
- local policy and capability handlers;
- bounded invocation/result ledger;
- process and service adapters;
- structured logs and health data.

It does not load models, agents, sessions, channels, MCP servers, or workspace
memory. Linux installs as a systemd user service by default. macOS installs as
a LaunchAgent. System-level installation is explicit.

The MVP uses one gateway binding per companion process. One machine may run
multiple named service instances from the same `picoclaw-node` binary, each
with a separate configuration, state directory, device key, gateway binding,
policy, and invocation ledger. This is the default way for independently
deployed ForgeClaw workspaces to use the same host without sharing authority.

A future multi-gateway supervisor may host several connection bindings over one
shared capability runtime. The runtime is therefore instance-scoped rather than
global, and future policy and ledger keys must include a stable gateway binding
identity as well as the invocation ID. Shared stateful resources such as a
browser profile require node-local scheduling and explicit cross-binding policy;
they must not be shared merely because two gateway URLs appear in configuration.
This extension changes connection supervision, not the node protocol or command
handler contracts.

### Privilege separation

The companion runs as an unprivileged account. Read-only inspection should not
require elevation.

Privileged service actions use one of these, in preference order:

1. an optional ForgeClaw privileged helper with a narrow typed Unix-socket API;
2. an operator-supplied tightly scoped polkit or sudoers rule for exact service
   actions;
3. no privileged capability.

The helper never accepts arbitrary shell text. It validates peer credentials,
service unit names, allowed actions, request expiry, and a node-local policy
revision. For the MVP, the helper can be a separate milestone after unprivileged
status and log inspection.

Linux service support targets systemd. macOS service support targets launchd.
Both map to the common typed command schema while returning platform-specific
details in an optional namespaced field.

### Docker executor

Docker is a node-local executor, not a transport. It is post-MVP unless needed
for the first deployment. Secure defaults include:

- non-root user;
- no Docker socket mount;
- dropped Linux capabilities;
- `no-new-privileges`;
- CPU, memory, PID, and output limits;
- no network unless explicitly enabled;
- explicit read-only or read-write mounts;
- pinned image reference where practical;
- bounded lifecycle and cleanup policy.

## Policy Model

Authorization is the intersection of independent layers:

```text
gateway global policy
AND agent-to-node binding
AND paired command surface
AND current human approval policy
AND node-local command policy
AND OS account or privileged-helper authority
```

No layer can broaden a stricter layer.

The model cannot submit arbitrary connection details. Operator configuration
defines named execution targets independently of their transport:

```json
{
  "execution": {
    "targets": {
      "vpn": {"type": "node", "node": "vpn-box"},
      "build": {
        "type": "node",
        "node": "linux-builder",
        "executor": "docker"
      }
    }
  },
  "agents": {
    "list": [
      {
        "id": "main",
        "target_policy": {
          "default_target": "build",
          "allowed_targets": ["build", "vpn"]
        }
      }
    ]
  }
}
```

Node-local configuration controls working roots, executable paths, service
units, action classes, timeout ceilings, environment allowlists, executors, and
whether interactive or shell execution is available.

### Future SSH support

SSH is not part of the node MVP, but it has two useful future roles.

First, an explicit operator command may use SSH to bootstrap a companion on a
new machine. It verifies the host key, copies the slim `picoclaw-node` binary,
installs an unprivileged service, and supplies a short-lived enrollment token
plus the pinned gateway TLS identity. Normal operation then switches to the
node's outbound WSS connection. SSH credentials remain in an operator-owned
secret reference or SSH agent and are never exposed to the model or reused as
node identity.

Second, a static SSH target driver may support hosts where installing a
companion is impossible. It reuses named-target authorization, canonical
execution plans, bounded results and artifacts, approval, audit, cancellation,
and unknown-outcome semantics. The model selects only an allowed target alias;
it cannot provide a hostname, user, port, host key, or credential. Those values
are operator configuration with strict host-key verification.

A direct SSH target intentionally has fewer guarantees than a companion. It
has no live capability advertisement, durable remote invocation ledger, or
reconnect-based result recovery unless a small remote helper is installed.
These differences are reported by the target driver rather than hidden behind
false node presence. This keeps SSH and paired nodes as different transports
over shared execution contracts instead of creating parallel policy and result
stacks.

Sensitive actions integrate with ForgeClaw's durable human-interaction system.
Approval is bound to the canonical plan hash, node identity, policy revision,
and expiry. Approval consumption happens before dispatch. An uncertain result
does not restore the approval.

## Agent Tool Surface

The initial model-facing API should be small:

- `nodes list`: connected and configured nodes visible to the current agent;
- `nodes describe`: capabilities, policy-visible aliases, platform, and health;
- `nodes invoke`: invoke one visible typed command on one allowed target;
- `nodes status`: inspect one invocation;
- `nodes cancel`: request cancellation when supported.

`exec` may later accept a named target and delegate internally to
`system.exec.v1`. It must not duplicate node policy or transport logic. Typed
service commands should remain visible as typed operations rather than being
translated into opaque shell strings by the model.

Node descriptions in model context must be bounded. A large fleet should be
queried through tools rather than injected into every prompt.

## Observability and Audit

Runtime events include:

- node pairing requested, approved, denied, and revoked;
- node connected, disconnected, incompatible, and capability changed;
- invocation prepared, approval requested, dispatched, accepted, progressed,
  completed, cancelled, expired, and uncertain;
- node-local policy denial and privileged-helper denial.

Events include node ID, target alias, command ID, invocation ID, plan hash,
policy revision, duration, status, and bounded error code. They exclude raw
commands, environment values, credentials, file contents, and private keys.

Metrics cover connected nodes, reconnects, invocation latency, failures,
denials, uncertain outcomes, and output truncation. `doctor` should eventually
check TLS posture, stale nodes, overly broad node policy, privileged service
configuration, and companion version compatibility.

## OpenClaw Compatibility Seam

Future compatibility is an adapter, not a core dependency:

```text
OpenClaw-compatible WebSocket frames
                |
                v
OpenClaw transport adapter
                |
                v
ForgeClaw NodeRegistry / NodeInvoker / policy
```

The internal model intentionally preserves concepts that map cleanly to an
OpenClaw-style protocol:

| Internal concept | Possible external mapping |
| --- | --- |
| node identity and pairing | device identity and node role |
| capability and command descriptors | caps, commands, permissions |
| node registry | node list and describe |
| invocation request/result/event | node invoke request/result/progress |
| protocol range | min/max protocol negotiation |
| gateway and node policy intersection | gateway command policy plus node approvals |

Compatibility is not promised by similar names. An adapter must own external
handshake fields, tokens, scopes, version negotiation, method names, snapshots,
and error translation. Conformance tests should run against a pinned external
protocol version. Core code must never branch on an OpenClaw client ID or import
external wire structs.

This boundary also allows future native gRPC, MQTT, or constrained-device
transports without changing the node domain.

## MVP Scope

The MVP is complete when:

- one Linux or macOS companion can establish an outbound WSS connection;
- first connection requires explicit device pairing;
- the gateway lists connected and disconnected paired nodes;
- agents can be bound to a bounded set of named targets;
- `node.info.v1`, `system.which.v1`, and synchronous `system.exec.v1` work;
- working roots, executable policy, timeout, output, and environment are
  enforced again on the node;
- accepted invocation IDs are stored in a bounded ledger and duplicate
  mutations are not blindly replayed;
- disconnect, timeout, cancellation, and unknown-outcome semantics are tested;
- audit events contain no raw secrets or command output;
- systemd and LaunchAgent installation paths are documented and tested;
- the companion build is checked to remain independent of LLM, channel, and
  workspace-memory packages;
- an end-to-end test uses a real companion process and loopback WSS gateway.

Typed service status and logs are strongly preferred in the MVP. Privileged
service actions may follow immediately after the basic transport and policy
path if the helper boundary is not yet ready.

## Suggested PR Sequence

1. Add domain types, registry contracts, and this protocol's JSON schemas with
   no network listener or agent tool.
2. Add gateway connection admission, challenge authentication, and pending
   pairing persistence.
3. Add the lightweight `picoclaw-node run` companion with reconnect and
   capability advertisement.
4. Add operator node list, describe, approve, revoke, and agent target binding.
5. Add synchronous `system.exec.v1`, canonical plans, node-local policy, and
   bounded output.
6. Add invocation ledger, idempotency, cancellation, recovery queries, and
   uncertain-outcome tests.
7. Integrate durable human approval and runtime audit events.
8. Add Linux systemd and macOS launchd status/log handlers.
9. Add the narrow privileged service helper and allowlisted service actions.
10. Add installer, doctor checks, deployment documentation, and an end-to-end
    Linux/macOS compatibility matrix.

Each PR should leave the runtime in a valid state and avoid exposing a command
surface before its policy and node-side enforcement exist.

## Later Work

- SSH bootstrap for installing a companion and a direct static SSH target
  driver for hosts that cannot run one.
- Docker execution on a connected node.
- Bounded artifact upload and download.
- Durable background jobs and streamed PTY sessions.
- Browser proxy and node-hosted MCP capability descriptors.
- Windows service and process adapters.
- iOS and Android companions with camera, location, notification, and voice
  capabilities.
- Constrained camera or appliance nodes with a reduced command catalog.
- Experimental OpenClaw protocol adapter pinned to an explicitly supported
  protocol version.

## Risks and Mitigations

| Risk | Mitigation |
| --- | --- |
| Agent gains broad server control | Dedicated user, named targets, node-local policy, typed privileged actions |
| Gateway compromise controls nodes | Node-side final enforcement, revocable pairing, narrow command surface |
| Prompt injection targets production | Agent-to-node ACL, human approval, no arbitrary target parameters |
| Duplicate mutation after disconnect | Acceptance ledger, idempotency binding, unknown outcome instead of retry |
| Companion becomes another full agent | Keep LLM, channels, memory, and MCP out of the companion |
| Protocol becomes tied to one client | Transport-neutral domain and versioned adapter boundary |
| Container is mistaken for whole-host safety | Explicit executor boundary and no Docker socket mount |
| Capability catalog grows without bound | Pairing approval for broadened commands and bounded model projection |
| SSH becomes a parallel execution stack | Shared target driver, plan, policy, result, artifact, and audit contracts |

## Open Questions Before Implementation

- Whether pairing state belongs in the existing durable interaction registry or
  a dedicated device registry with interaction records only for human prompts.
- Whether MVP execution should permit an explicitly configured shell command
  family or only argv-based execution.
- Whether service status/log commands are required for MVP completion or the
  immediately following milestone.
- Whether companion updates are manual in version 1 or use a separately signed
  update channel later.
- What bounded retention window is appropriate for the node invocation ledger.

These questions do not change the major boundaries and can be resolved in the
first implementation PRs with focused threat-model tests.
