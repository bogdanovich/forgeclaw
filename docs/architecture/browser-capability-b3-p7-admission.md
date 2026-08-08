# Browser Capability B3 And Node P7 Admission

## Status And Decision

Browser milestone B3, **Companion-Hosted Browser**, and the browser slice of
node milestone P7, **Interactive Application Capabilities**, are admitted as
the focused sequence in this document.

The first vertical slice is fixed to:

- the already paired Darwin companion with operator alias `ab-local-test`;
- the existing `browser` specialist and its authenticated owner route;
- one model-visible browser target alias, `companion`;
- one browser profile alias, `managed`, with `dry_run=true`;
- the existing Playwright MCP driver running only on the companion;
- open, status, observe, navigate, screenshot, download, and close; and
- the existing production WebSocket node transport and P2 artifact protocol.

The deployed companion is a macOS `darwin/amd64` node. It currently advertises
only node information and bounded file-transfer commands. Google Chrome, Node,
and the Playwright package are present on that host. B3 must add browser
authority explicitly; their presence is not authority and does not make the
capability discoverable.

The model continues to call `browser_targets`, `browser_session`,
`browser_observe`, and `browser_act`. The typed P7 commands are an internal
gateway-to-companion contract. They are not added to the browser specialist's
generic `nodes_invoke` surface.

## Admitted Operator Workflow

An authenticated owner delegates one browser task to the configured browser
specialist. The specialist:

1. discovers that target `companion` and profile `managed` are ready;
2. opens a browser session on that exact target;
3. observes `about:blank`;
4. navigates once to an origin permitted by the effective gateway and
   companion policy;
5. observes the resulting page and may retain one bounded PNG screenshot;
6. may perform one typed, approved download into the gateway P2 spool; and
7. explicitly closes the session.

The session is physically hosted by `ab-local-test`. A disconnect never moves
it to the gateway target. A target selection is immutable after open.

The initial deployment canary uses an operator-controlled HTTP fixture and
the already admitted `any_http` network mode. This is an explicit personal
deployment policy, not a product default. A fresh installation has no
companion browser target, profile, command approval, or browser policy and
therefore remains deny-all.

## Explicit Exclusions

B3 does not admit:

- raw CDP, raw Playwright MCP, or generic MCP forwarding over the node link;
- a model-supplied node ID, endpoint, profile directory, executable, proxy,
  browser argument, environment variable, credential, cookie, or header;
- implicit target selection, gateway fallback, migration, or P8 remote
  workspace routing;
- arbitrary JavaScript evaluation, extension control, clipboard access,
  desktop input, or general computer control;
- attached-user profiles, cookie export, credential management, or browser
  identity migration;
- cloud browser providers, multiple companions, multiple profiles, or driver
  selection by the model;
- external commits such as publish, send, delete, purchase, payment, or
  booking; or
- a second artifact store, transfer protocol, approval system, or invocation
  ledger.

Human handoff remains gateway-local in this slice. Companion handoff requires
a later admission that binds operator presence and a revocable view channel.

## Authority Intersection

An operation is authorized only by the intersection of:

1. the authenticated actor, routed session, workspace, and selected agent;
2. the browser specialist's configured browser-tool grant;
3. the gateway browser target alias and profile policy;
4. the configured mapping from `companion` to the paired `ab-local-test`
   target;
5. current node connection, pairing, approved catalog hash, and approved
   browser commands;
6. the fresh companion execution policy and browser-profile revision;
7. the companion OS account, browser executable identity, profile lease, and
   local network policy;
8. the gateway browser session, tab, snapshot generation, prepared action,
   effect classification, and approval binding; and
9. the companion invocation ledger and, for binary output, the P2 transfer
   record and digest.

Every layer may narrow authority. No layer may broaden an absent grant from
another layer. Possessing a target alias, browser session ID, snapshot ref,
node invocation ID, or artifact ref is not authority.

## Configuration Contract

### Gateway target

The browser target gains a placement discriminator and an operator-owned node
mapping. Conceptually:

```json
{
  "tools": {
    "browser": {
      "targets": {
        "companion": {
          "enabled": true,
          "placement": "node",
          "node_target": "ab-local-test",
          "profiles": {
            "managed": {
              "enabled": true,
              "mode": "managed",
              "network_mode": "any_http",
              "dry_run": true
            }
          }
        }
      }
    }
  }
}
```

`node_target` is operator configuration. Discovery returns the safe browser
alias `companion`, readiness, features, limits, and policy mode; it does not
return the node alias, node ID, gateway URL, driver endpoint, or host paths.

An enabled gateway-local target still requires its local driver template. An
enabled node target instead requires nodes to be enabled, a valid configured
node alias, and a browser profile. Mixed local-driver and node-placement
fields are invalid. Unknown placement values and incomplete mappings fail
configuration validation.

### Companion browser profile

The companion gains an optional list of browser profiles. The selected
profile fixes:

- revision and model-safe alias;
- allowed agent IDs and actor policy;
- driver kind and executable identity;
- private profile directory and exclusive lock path;
- network mode and optional exact origins;
- `dry_run=true` and allowed action kinds;
- headed or headless mode;
- session, idle, action, snapshot, screenshot, upload, download, and retention
  limits; and
- explicit denial of raw evaluation, CDP export, extensions, credentials,
  attached-user state, human handoff, and computer input.

Profile paths, executable paths, process arguments, environment, and lock
paths are never catalog fields, command results, diagnostics, traces, or model
messages. The companion validates the executable identity and acquires the
profile lease before driver launch.

## Typed Node Capability Surface

The companion advertises these commands only when at least one valid browser
profile exists. Schemas use `additionalProperties=false`, bounded strings and
arrays, and the existing node output ceiling.

### `browser.session.open.v1`

Risk: `write`, because it starts a process and creates durable session state.

Input contains an opaque gateway browser session ID, the configured profile
alias and revision, the gateway browser policy revision, `dry_run`, and
bounded effective limits. Target identity, owner identity, catalog hash,
timeout, and idempotency are inherited from the signed node execution plan.

Output contains only session state, safe feature flags, one opaque tab ID,
controller state, and bounded expiries. It contains no process ID, endpoint,
path, or driver payload.

### `browser.session.status.v1`

Risk: `read`.

Input contains only the browser session ID and expected profile revision.
Output reports `opening`, `ready`, `closing`, `closed`, `lost`, or `unknown`,
plus safe reason and recovery codes. Status does not start, resume, or migrate
a session.

### `browser.observe.v1`

Risk: `read` at the node-command layer, while the gateway browser broker keeps
its existing mutating loop semantics because an observation advances snapshot
authority.

Input contains session ID, tab ID, the next gateway snapshot generation, and
whether a screenshot is requested. Output contains a bounded URL, origin,
title, accessibility snapshot, scoped element descriptors, truncation state,
and safe page metadata. Screenshot bytes use P2 and the result contains only
the exact retained-artifact descriptor.

### `browser.act.v1`

Risk: `write`.

Input contains the browser session, tab, snapshot generation, one stable
action invocation ID, one typed action, the gateway effect classification,
the prepared-action hash, policy revisions, and approval digest when required.
The first implementation enables only `navigate` and the separately approved
typed `download` action. It does not accept a generic tool name, method, JSON-
RPC payload, JavaScript source, coordinate, or nested action sequence.

Output contains accepted and terminal state, a bounded safe reason, and the
next observation or exact P2 artifact descriptor. One call performs one
action.

### `browser.session.close.v1`

Risk: `write`.

Input contains the session ID and expected profile revision. Close terminates
driver ownership, releases the profile lease, cleans staging data, and writes
terminal session state. Repeated close returns the same terminal result and
does not start a new worker.

## Ownership And Runtime Boundaries

The gateway browser broker remains the sole model-facing authority. It owns:

- target/profile authorization and safe discovery;
- authenticated owner binding;
- browser session aliases and orchestration state;
- tab/snapshot generation and stale-ref rejection;
- action preparation, effect classification, approval, and dry-run policy;
- gateway-side accepted/terminal invocation state;
- artifact ownership, routed delivery, and model-safe errors; and
- the prohibition on fallback or placement changes.

The companion browser host owns:

- node-local profile policy and revision;
- the browser process, driver transport, profile lock, context, and tabs;
- enforcement proxy and local network checks;
- local accepted/terminal invocation records;
- output and resource limits;
- staging bytes before P2 commit; and
- process, lease, and staging cleanup.

The driver is a replaceable implementation behind the existing typed browser
worker seam. Playwright MCP stdio and any CDP connection remain inside the
companion process boundary.

## Dispatch, Acceptance, And Recovery

The gateway derives one stable node idempotency key from the browser owner,
browser session, command kind, and browser request or action ID. It does not
create a new key when a transport response is lost.

The required state machine is:

```text
gateway prepared
    -> node dispatched
    -> node accepted
    -> node succeeded | node failed | node unknown
```

- Disconnect before authenticated node acceptance may redispatch the same
  plan and idempotency key.
- Disconnect after acceptance must query the existing node invocation ledger.
- A stored terminal result may be returned only when current target, catalog,
  owner, policy, profile, and descriptor authority still match.
- An accepted action without a provable terminal result becomes `unknown`.
  It is never blindly replayed.
- A lost browser process makes its session `lost`; status or a later close may
  reconcile cleanup but may not recreate the session.
- A node reconnect never changes the browser target or profile.
- Gateway and companion restart tests must cover acceptance boundaries for
  open, navigate, and close.

Browser-session state is distinct from the generic node invocation record.
The existing node ledger proves command acceptance and terminal output; the
browser-session ledger proves process/profile ownership and cleanup. Neither
duplicates the other.

## Artifacts And Durable Outbound Recovery

Screenshot and download bytes use deployed P2 framing and the gateway transfer
spool. Bytes never appear in WebSocket JSON, node-command JSON, trace content,
or model results.

The artifact is bound to workspace, agent, actor, route, node target, browser
session, tab, snapshot generation, action invocation, size, digest, media
type, filename, expiry, and policy revisions. Companion staging is not a
model-visible artifact. The gateway exposes a reference only after the P2
record is committed and its digest matches.

Durable outbound recovery must not make gateway startup depend on expired or
missing browser artifacts. When an old delivery intent references an artifact
that is provably absent or expired, reconciliation records a bounded terminal
delivery failure, preserves audit metadata, and continues startup. It must not
invent bytes, resend a different artifact, delete unrelated intents, or loop
the gateway process. Ambiguous remote delivery remains ambiguous rather than
being downgraded to a definite failure.

## Network And Action Policy

Both gateway and companion evaluate the same normalized browser network modes:

- `exact_origins` permits only configured HTTP(S) origins and admitted
  redirect destinations;
- `public_web` permits resolved public HTTP(S) destinations; and
- `any_http` permits HTTP(S), including loopback and private networks, only
  when explicitly configured on both sides.

The effective authority is the narrower result. Redirects, popups, downloads,
and subsequent requests remain subject to the companion enforcing proxy.
DNS rebinding and address-scope changes fail closed under modes that exclude
the resolved destination.

The initial action set is navigation plus typed download. Fill, select, press,
upload, dialog decisions, external commits, and unknown effects remain denied
until focused follow-up slices preserve the same contract. A dry-run profile
never permits an external commit. The existing exact approved-download
exception remains the only admitted unknown-effect operation.

## Limits And Cleanup

The first deployment allows one companion browser session, one tab, one
in-flight node browser command per session, and one profile writer. Existing
browser hard maxima remain ceilings; the companion may impose lower limits.

Every terminal or expired session must:

1. stop action dispatch;
2. terminate the driver and owned browser process;
3. release the profile lease;
4. abandon incomplete P2 staging records;
5. retain only bounded session, invocation, and artifact audit metadata; and
6. report cleanup failure without advertising the profile as ready.

Companion shutdown attempts bounded cleanup. On restart, a stale lease is not
silently stolen. The runtime first proves that no owned process remains or
marks the profile unavailable for operator recovery.

## Diagnostics And Redaction

`browser_targets` may report `ready`, `degraded`, `unavailable`,
`disconnected`, `policy_mismatch`, `catalog_approval_required`,
`driver_incompatible`, `profile_locked`, or `cleanup_required`, with safe
operator guidance.

Diagnostics and traces may contain aliases, opaque IDs, state, feature flags,
limits, revisions or their digests, byte counts, content types, hashes,
timestamps, and bounded safe reason codes. They must redact:

- node ID and transport endpoint;
- browser executable, profile, lock, staging, and artifact paths;
- process arguments, environment, proxy addresses, and raw MCP/CDP traffic;
- credentials, cookies, headers, page form values, and downloaded bytes; and
- complete URLs containing user information, queries, or fragments.

Passive readiness must not connect a driver, open a browser, acquire a lease,
or renew session state.

## Delivery Sequence

Each code slice is a separate focused PR from the latest merged `main`:

1. make missing or expired browser-artifact outbound recovery terminal without
   preventing gateway startup;
2. add node placement configuration, browser capability descriptors, and
   companion profile validation while keeping the new target disabled;
3. add the companion browser host and reuse the B1 Playwright worker adapter;
4. route open, status, observe, navigate, and close through the internal node
   runtime with ledger reconciliation;
5. route screenshot and download bytes through P2;
6. add production-WSS disconnect, restart, stale-authority, and no-fallback
   tests; and
7. deploy deny-by-default, explicitly enable `ab-local-test` and `managed`,
   approve the fresh catalog, and run the admitted canary.

No dependent slice starts before its prerequisite merges. A slice stops and
returns to admission if it requires raw CDP/MCP forwarding, a new transport or
artifact store, implicit placement, or a generic interactive-computer API.

## Required Validation

Automated validation must cover:

- configuration rejection for mixed placement, missing node mapping, unknown
  profile, non-dry-run profile, and disabled node runtime;
- descriptor/schema bounds and deny-by-default catalog visibility;
- unauthorized agent, actor, target, catalog, command, profile, policy
  revision, session, tab, snapshot, prepared action, and approval;
- executable mismatch, profile contention, network denial, output overflow,
  timeout, cancellation, and cleanup failure;
- disconnect before acceptance, after acceptance, and after terminal commit;
- no replay of an accepted navigate or download;
- no gateway fallback after target loss;
- gateway and companion restart recovery;
- matching screenshot/download size and SHA-256 over P2;
- no inline binary data, raw endpoint, host path, credential, cookie, or raw
  driver content in model output, events, node JSON, or traces; and
- missing retained artifacts becoming bounded terminal outbound failures
  without blocking gateway startup.

The real-process canary must run from merged `main` through the normal browser
specialist and production WSS route:

```text
targets -> open(companion/managed) -> observe(about:blank)
        -> navigate(operator fixture) -> observe(screenshot)
        -> approved download -> close
```

Evidence records the gateway and companion build SHAs, target/profile aliases,
catalog and policy revisions, opaque session/invocation IDs, state transitions,
artifact sizes and digests, trace IDs, cleanup counts, and bounded journals.
It does not record secrets or raw paths.

## Completion Gate

B3 and this browser slice of P7 are complete only when:

- a real browser on `ab-local-test` completes the admitted model-visible flow;
- the browser specialist uses only first-party browser tools;
- gateway and companion enforce the same owner, target, profile, network,
  action, approval, and limit intersection;
- raw CDP/MCP, paths, cookies, credentials, and binary bytes remain hidden;
- acceptance-boundary disconnect tests prove explicit terminal or unknown
  outcomes without blind replay;
- target loss never creates a gateway-local replacement session;
- screenshot and download artifacts reach P2 with matching digests;
- stale lease, ref, approval, catalog, and policy revisions fail closed;
- zero browser processes, sessions, invocations, staging records, interactions,
  and tasks remain active after the canary; and
- the companion remains a capability host, not an agent or workspace
  scheduler.

## Mandatory Stop Conditions

Stop the affected implementation PR if:

- browser behavior requires generic `mcp.tools.call.v1`, raw CDP, arbitrary
  JSON-RPC, or JavaScript forwarding;
- any accepted action can be replayed after an uncertain disconnect;
- browser placement becomes implicit or falls back to another target;
- binary media requires another transport or appears in ordinary JSON;
- node-local policy cannot narrow gateway policy;
- profile ownership cannot be proven across process or companion restart;
- gateway startup can still be looped by one provably missing retained browser
  artifact; or
- the change begins to implement attached identity, computer control, P8
  workspace routing, or another excluded capability.
