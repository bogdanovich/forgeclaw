# Node Companion P0 Model-Visible Capability Contracts

## Status

P0 is admitted as one bounded post-MVP milestone. It extends the existing
`nodes` discovery surface so an authorized model can construct a valid call to
an already approved typed command without guessing hidden node policy.

This document fixes P0 scope and completion criteria. Review findings may
clarify or narrow these contracts. A finding that requires shell/PTY, file
transfer, another capability registry, another authority store, a new
invocation path, or a general policy framework stops the affected PR and
returns to roadmap admission.

## Admission Evidence

The deployed MVP proves the prerequisite lifecycle:

- merged core and companion binaries share one revision;
- the gateway has an operator-defined `vpn-smoke` target;
- the `main` agent target policy grants only that target;
- one companion is connected under an operator-owned node alias;
- pairing approval grants only `system.exec.v1`;
- the companion reports policy revision `vpn-smoke-v1`;
- invocation, approval, durable status, no-blind-replay, and passive
  diagnostics are already covered by the MVP real-process vertical test.

The current gap is also concrete:

- model-facing `nodes describe` returns command name, risk, progress, and
  cancellation flags;
- its tests explicitly exclude `input_schema` and `output_schema`;
- it does not project the companion's executable, working-root, environment,
  timeout, or output constraints;
- `nodes_invoke.input` remains a generic object and carries no opaque
  discovery revision;
- therefore a prompt that names only an outcome cannot construct an accepted
  `system.exec.v1` request without operator-supplied host facts.

The direct `mintclaw agent` CLI is not deployment evidence for node tools.
Those tools are gateway-owned because they require the live admission and
invocation runtime. P0 deployment proof must exercise the Gateway/AgentLoop
path.

## Admitted Use Case

An authenticated owner asks the configured `main` agent to perform one
constrained direct-argv operation on target `vpn-smoke` without naming an
executable path, working directory, environment policy, timeout ceiling, JSON
schema, node ID, or transport detail.

The agent:

1. lists its configured targets;
2. describes the target and the approved `system.exec.v1` command;
3. receives a bounded input schema, model-safe effective constraints, and an
   opaque discovery revision;
4. constructs `nodes_invoke` with that revision;
5. completes any existing human approval flow;
6. observes the durable result through the invocation response or
   `nodes_status`;
7. fails closed before dispatch after relevant target, catalog, or node-policy
   authority changes.

No other actor, agent, route, target, or command becomes visible or usable.

## Existing Sources Of Truth

P0 creates no authority source.

| Decision | Existing authoritative source | P0 projection |
| --- | --- | --- |
| Target visible to an agent | effective agent `target_policy` and `execution.targets` | target name and default flag |
| Device and catalog approved | durable registration, allowed commands, approved catalog hash | approved command summaries only |
| Current live catalog | authenticated connected snapshot | approved descriptors only |
| Generic node ceiling | companion `LocalCommandPolicy` | effective timeout, output, and risk bounds |
| Command-specific bounds | companion handler policy, initially `SystemExecPolicy` | explicitly model-visible aliases and limits |
| Human approval | existing tool/interaction policy | `may_be_required`, never authority |
| Invocation authority | existing preparation, retained plan, registry, and node authorization | opaque revision rechecked before preparation |

Discovery is advisory. Invocation revalidates every authoritative source.
Possessing or inventing discovery output never grants authority.

## Discovery Surface

The existing `nodes` tool keeps `list` and `describe`. `describe` accepts an
optional approved command name:

```json
{
  "action": "describe",
  "target": "vpn-smoke",
  "command": "system.exec.v1"
}
```

Target-only describe returns bounded command summaries. Command-specific
describe returns one full contract. This prevents a large approved catalog
from placing every schema and example in one model result.

### Target description

The model-safe target response is:

```json
{
  "target": "vpn-smoke",
  "default": true,
  "state": "connected",
  "availability": "available",
  "command_count": 1,
  "commands": [
    {
      "name": "system.exec.v1",
      "risk": "write",
      "availability": "available",
      "supports_progress": false,
      "supports_cancel": false,
      "approval": "may_be_required"
    }
  ]
}
```

`available` remains as a compatibility boolean during P0, derived from
`availability == "available"` at target level. New logic uses the explicit
availability vocabulary.

Target-only describe never includes:

- input or output schemas;
- command examples or guidance;
- raw node identity or aliases;
- credentials, keys, transport endpoints, or connection parameters;
- catalog, policy, descriptor, plan, or public-key hashes;
- raw platform claims, executable paths, working paths, or environment
  values.

### Command contract

A command-specific response has this shape:

```json
{
  "target": "vpn-smoke",
  "state": "connected",
  "availability": "available",
  "command": {
    "name": "system.exec.v1",
    "risk": "write",
    "availability": "available",
    "input_schema": {},
    "result": {
      "kind": "json",
      "schema_available": true
    },
    "execution": {
      "timeout_seconds_max": 30,
      "output_bytes_max": 65536,
      "supports_progress": false,
      "supports_cancel": false,
      "approval": "may_be_required"
    },
    "constraints": {},
    "guidance": [],
    "examples": []
  },
  "discovery_revision": "dr_v1_<opaque>"
}
```

The exact Go types may split summaries, contracts, and command-specific
constraints, but the JSON field names and semantics above are stable for P0.
Unknown fields are not emitted.

`input_schema` is the approved descriptor's bounded JSON Schema after applying
the model-visible node projection. `result` describes only how to interpret
the result. P0 does not place output bytes or a full output schema in ordinary
target summaries. A command contract may state that a bounded JSON result
schema exists without teaching the model to depend on host output details.

### Availability vocabulary

Every visible target and command uses one of:

- `available`: current discovery contains enough model-safe information to
  construct an invocation;
- `partially_described`: authority exists, but at least one necessary
  constraint cannot be represented or has not been marked model-visible;
- `requires_reapproval`: authenticated catalog or its authority-bearing
  model contract differs from durable approval;
- `unavailable`: the visible target or approved command cannot currently
  accept new work.

`partially_described` is never presented as unrestricted. It includes bounded
guidance to ask the operator for a configured alias or refresh discovery. It
does not reveal the missing value or encourage enumeration.

Invisible targets remain omitted from list and rejected by name. Revoked
targets are not described as reapprovable. Disconnected persisted state is
`unavailable`, even when the last durable snapshot said connected.

## Effective Contract Projection

The command contract is the intersection of:

1. the target currently granted to the requesting agent;
2. the authenticated connected catalog;
3. durable pairing-approved commands and catalog identity;
4. generic node-local command risk, timeout, and output ceilings;
5. command-specific node-local policy;
6. operator-selected model-visible aliases, guidance, and examples;
7. gateway hard limits.

The intersection never becomes a union. Missing data narrows status to
`partially_described`, `requires_reapproval`, or `unavailable`.

### Descriptor binding

P0 extends the existing command descriptor with an optional bounded
model-contract projection. The projection participates in canonical catalog
and descriptor hashes. It is therefore authenticated by the existing identity
proof, bound to durable catalog approval, copied through the existing
snapshot, and rechecked by the existing execution-plan descriptor hash.

This is an additive protocol-v1 descriptor field, not a new protocol version
or authority path. A companion without a model contract remains valid and its
approved command is `partially_described`. Changing an authority-bearing
projection changes the catalog hash and requires normal catalog reapproval.

The projection contains only enforcement-derived limits and explicitly
operator-authored model-safe metadata. The companion validates it against the
same normalized policy used by command execution before advertising it.

### Generic ceilings

For every approved command, the companion projects:

- effective maximum risk;
- maximum plan timeout;
- maximum output bytes;
- progress and cancellation support from the descriptor;
- expected JSON result availability.

The gateway takes the minimum of companion and hard protocol ceilings.
Configured gateway narrowing may reduce these values but never increase them.

### `system.exec.v1` aliases

Raw normalized executable and filesystem paths remain hidden by default.
`SystemExecPolicy` gains an optional model-discovery section with:

- executable alias to one already allowed normalized executable;
- working-scope alias to one already allowed normalized root;
- explicitly visible environment names;
- bounded operator guidance;
- schema-valid examples expressed only with visible aliases.

Aliases are identifiers, not new executables or roots. Normalization rejects
an alias whose destination is not already present in the enforcement policy.
Duplicate or case-colliding aliases fail config validation.

The existing input keys remain:

```json
{
  "argv": ["diagnostic", "--version"],
  "cwd": "workspace",
  "timeout_seconds": 10,
  "env": {}
}
```

At node preparation, `argv[0]` and `cwd` aliases resolve through the normalized
policy. Existing raw values remain accepted only when node policy already
allows them, but they are not model-visible unless explicitly configured as
visible aliases. Environment values are never advertised.

The effective schema:

- enumerates visible `argv[0]` aliases;
- enumerates visible working-scope aliases;
- restricts environment property names to explicitly visible allowed names;
- applies the effective timeout ceiling;
- preserves existing argv count, argument length, environment value, and
  object-shape bounds.

If no executable or working-scope alias is configured, `system.exec.v1` is
`partially_described`; discovery does not emit normalized host paths.

### Guidance and examples

Operator metadata is non-authoritative:

- guidance is a bounded array of plain statements, not prompt-role content;
- examples contain command input only, never `nodes_invoke` target,
  authority, approval, or revision fields;
- every example is validated against the effective schema and command-specific
  policy at config load;
- labels and examples cannot name hidden aliases or broaden limits;
- examples are sorted deterministically and capped before catalog admission.

Initial P0 limits:

- 128 command summaries per target, matching the catalog limit;
- one full command contract per describe call;
- 32 KiB maximum projected command contract;
- 2 KiB total guidance text;
- four examples;
- 8 KiB canonical JSON per example;
- 64 executable aliases, 32 working-scope aliases, and 64 environment names.

Exceeding a configuration limit fails config validation. An authenticated
legacy descriptor that cannot fit the projection budget becomes
`partially_described`; output is never silently truncated into a misleading
contract.

## Freshness

Command-specific describe returns an opaque `discovery_revision`:

```text
dr_v1_<base64url sha256>
```

It is a non-secret correlation value, not an authority token. It is derived
from canonical current data already used during preparation:

- requesting agent's effective target grant and target binding;
- target name and executor selection;
- approved command name and descriptor identity;
- connected catalog approval state;
- node policy revision;
- effective model contract and generic ceilings.

It excludes raw node ID, public key, transport endpoint, credentials,
environment values, and unprojected policy data.

`nodes_invoke` requires the revision for node commands after P0. Before
creating or reusing a prepared plan, the gateway recomputes it under the
current actor, agent, routed session, target, registration, connection, and
policy context. A mismatch returns `DISCOVERY_STALE` and performs no dispatch,
approval creation, or replacement-plan minting.

Approval continuation retains the already prepared execution plan. It does
not refresh a stale discovery revision or create new authority. Current
node-local authorization remains final and independently rechecks policy
revision, descriptor hash, catalog hash, limits, and command-specific policy.

No discovery cache is authoritative. Implementations may recompute on every
describe and preparation. If a cache is later added, any relevant config,
registration, connection, catalog, or policy change invalidates it.

## Model-Safe Denials

P0 aligns pre-dispatch denial with discovery vocabulary:

```json
{
  "status": "denied",
  "code": "CONSTRAINT_VIOLATION",
  "constraint": "executable_alias",
  "action": "refresh_discovery"
}
```

Allowed codes:

- `TARGET_UNAVAILABLE`;
- `COMMAND_UNAVAILABLE`;
- `REAPPROVAL_REQUIRED`;
- `DISCOVERY_INCOMPLETE`;
- `DISCOVERY_STALE`;
- `SCHEMA_INVALID`;
- `CONSTRAINT_VIOLATION`;
- `APPROVAL_REQUIRED`, only through the existing durable interaction flow.

Allowed constraint labels:

- `input_schema`;
- `executable_alias`;
- `working_scope`;
- `environment_name`;
- `timeout`;
- `output_limit`;
- `command_policy`.

Denials do not echo rejected input, executable/path candidates, hidden
allowlists, environment values, node identity, policy/catalog hashes, or
transport state. Repeated denial is not a discovery mechanism.

Post-dispatch uncertainty and durable command failure retain the existing
`nodes_status` and no-replay semantics. P0 does not collapse an uncertain
outcome into a policy denial.

## Redaction And Trust

Model-visible discovery may contain only:

- operator target and command names already visible to the agent;
- approved risk and support flags;
- bounded JSON input schema;
- effective numeric ceilings;
- explicit aliases, environment names, guidance, and examples;
- availability and opaque discovery revision.

It must not contain:

- raw node IDs, node aliases, public keys, credentials, or certificates;
- gateway/node addresses, transport endpoints, catalog/policy/plan hashes, or
  unrestricted policy documents;
- normalized host paths unless the operator deliberately uses the path itself
  as a visible alias;
- environment values;
- unapproved commands or targets;
- raw node-authored display, platform, architecture, version, denial, or
  guidance strings.

Only config-authored labels and validated descriptor/schema data cross into
the model contract. Node-authored free text is untrusted and excluded.

Passive diagnostic events record availability, denial code, constraint label,
and opaque revision digest only where needed for correlation. They exclude
schemas, aliases, examples, command input/output, paths, environment, node
identity, and hidden policy.

## Delivery Sequence

P0 is delivered through dependent merge units:

1. **Contract PR:** this admission, schema, authority, redaction, freshness,
   denial, limits, non-goals, and completion gates.
2. **Core projection PR:** descriptor/model-contract types, effective generic
   ceilings, command-specific describe, opaque revision, and fail-closed
   preparation.
3. **Operator metadata PR:** `system.exec.v1` aliases, guidance, examples,
   config validation, alias resolution, and structured safe denials.
4. **Evidence PR:** model outcome-only discovery/invoke/status flow, stale
   catalog/policy denial, bounded large-catalog behavior, and deployment
   instructions.

Each PR starts from merged `main`. A later PR does not begin until its
dependency is merged.

## Exact Completion Gates

P0 is complete only when all gates have authoritative evidence.

### Gate 1: bounded usable contract

- command-specific describe returns the approved input schema, effective
  ceilings, availability, and opaque revision;
- large catalogs remain bounded and deterministic;
- `system.exec.v1` becomes `available` only with at least one valid executable
  and working-scope alias;
- a schema-valid operator example cannot fail solely because it names hidden
  policy.

### Gate 2: authority intersection

- another agent, invisible target, unapproved command, disconnected node, and
  revoked node remain unavailable;
- aliases map only to already enforced executables, roots, and environment
  names;
- labels, guidance, examples, or discovery revision cannot broaden authority;
- fresh installs remain deny-by-default.

### Gate 3: freshness fail-closed

- target grant, target binding, approved catalog, descriptor/model contract,
  or relevant node policy change invalidates the old revision;
- stale invocation fails before approval or dispatch;
- approval resume cannot substitute a new revision or plan;
- node authorization still independently rejects stale policy and descriptor
  identity.

### Gate 4: redaction

- discovery, denials, events, logs, and approval prompts expose no raw node
  identity, key, endpoint, hidden path, environment value, hidden command,
  policy document, or authority hash;
- adversarial node-authored strings never become model guidance;
- contract-size overflow fails closed without partial secret-bearing output.

### Gate 5: real-process model flow

A real companion and Gateway/AgentLoop test starts from a prompt containing
only the desired outcome. The scripted model must:

1. receive and call `nodes`;
2. discover one available target and command;
3. obtain a schema-valid alias-based contract;
4. call `nodes_invoke` with the returned revision;
5. traverse the existing human approval path;
6. observe the durable command result;
7. use `nodes_status` for a retained result;
8. receive `DISCOVERY_STALE` with zero duplicate effect after a relevant
   constraint change.

The test cannot inject executable paths, working directories, environment
policy, timeout ceilings, or schemas into the prompt or scripted tool call
fixture before discovery.

### Gate 6: merged and deployed evidence

- every dependent PR is merged with required CI and review complete;
- core and companion are built from the same merged `main`;
- deployment keeps node authority deny-by-default except the existing
  explicitly configured smoke profile;
- the deployed gateway reports healthy services and no new error-level
  journals;
- one gateway-owned model flow uses discovered aliases and produces a valid
  passive diagnostic trace;
- changing one non-destructive smoke constraint makes the retained old
  revision fail before dispatch, then restoring configuration recovers normal
  discovery;
- operational evidence records SHAs, test/run identifiers, trace schema and
  ID, denial code, health result, rollback location, and residual limitations;
- evidence is reviewed before P1 is either admitted or explicitly deferred.

## Non-Goals And Stop Conditions

P0 does not implement:

- `shell.exec.v1`, shell text, owner/root profiles, PTY sessions, or
  model-facing cancellation;
- file transfer, artifacts, service administration, companion updates,
  executors, SSH, browser/MCP routing, or remote workspaces;
- new credentials, pairing flows, approval stores, capability registries,
  invocation stores, transports, protocol versions, or workflow engines;
- raw policy mirroring in the gateway;
- automatic catalog reapproval or automatic broadening after policy change;
- discovery by repeated policy denial;
- model-selected paths, executables, users, authority profiles, transport
  endpoints, or approval modes.

Stop and return to roadmap admission if implementation requires any item above,
if the effective contract cannot be derived from current authoritative state,
or if a review fix expands production code beyond node domain, companion
policy/runtime, node tools, gateway preparation, config validation, and their
direct tests.

Passing unit tests alone does not complete P0. Green CI without the
real-process and deployed gates does not complete P0. A working invocation
that still depends on hidden facts in the prompt does not complete P0.
