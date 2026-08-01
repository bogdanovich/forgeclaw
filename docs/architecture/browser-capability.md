# Reliable Browser Capability Architecture

## Status

Architecture investigation and target design recorded on 2026-08-01.

This document describes the observed MintClaw deployment, compares relevant
browser-automation systems, and defines a recommended product boundary. It does
not admit implementation, enable new browser authority, or change the deployed
browser configuration. Ordered implementation work is described in
[`browser-capability-roadmap.md`](browser-capability-roadmap.md).

## Decision Summary

MintClaw should preserve the dedicated browser specialist as the normal
planner for long browser workflows, while moving browser authority and
execution semantics into a first-party typed browser capability.

The product boundary should be:

- model-visible `browser.*` tools implemented and governed by MintClaw;
- a MintClaw browser broker that owns target selection, policy, approvals,
  sessions, profile leases, invocation recovery, artifacts, and audit;
- a browser worker running beside the selected browser on the gateway, a
  companion node, or an optional cloud provider;
- replaceable worker drivers such as Playwright, Playwright MCP,
  `agent-browser`, Chrome DevTools MCP, or a cloud browser API;
- a dedicated browser specialist that uses the first-party tool surface but
  does not itself become an authority boundary.

MCP remains useful as a driver protocol. It must not be the permanent
model-visible authority, session, or remote-routing contract for browser
automation.

## Investigation Snapshot

### Observed deployed capability

The inspected main deployment already has working browser automation:

- the main agent delegates serious browser workflows to a dedicated `browser`
  agent;
- the specialist has its own workspace, instructions, job checkpoints, and
  learned patterns;
- Playwright MCP is launched through `npx` with
  `@playwright/mcp@0.0.78`;
- the worker uses installed Chrome, a persistent browser profile, and a
  separate output directory;
- the browser agent is limited to one parallel turn;
- retained jobs and artifacts show real Craigslist, Facebook Marketplace, and
  Zillow workflows.

The absence of a globally installed `agent-browser` executable or Docker does
not mean browser automation is unavailable. The active path uses Playwright
MCP through `npx` and is independent of the older heavy-image skill.

### Assessment of the earlier capability-gap report

The earlier report correctly identified required product features:

- structured actions and results;
- explicit availability diagnostics;
- persistent named sessions;
- screenshot and media integration;
- credential and profile policy;
- lifecycle cleanup.

Its availability conclusion and proposed boundary are now incomplete. The
deployment has a functioning browser specialist, while wrapping an external
CLI alone would still leave MintClaw without authoritative session ownership,
remote placement, approval binding, and mutation-recovery semantics.

### Current implementation risks

The current system is useful but relies on agent and skill conventions for
several runtime invariants:

- a browser job, running browser session, persistent profile, and MCP process
  are not distinct MintClaw entities;
- one-agent-turn concurrency does not provide an exclusive persistent-profile
  lease across processes or nodes;
- checkpoints describe workflow progress but do not prove whether an external
  website mutation occurred;
- browser approvals and dry-run behavior are primarily prompt-level rules;
- MCP server allowlists exist, but browser authority is not represented as a
  dedicated capability with per-action policy;
- large MCP outputs have local artifact handling, but retention cleanup remains
  an explicit TODO in
  [`pkg/tools/integration/mcp_tool.go`](../../pkg/tools/integration/mcp_tool.go);
- the generic MCP manager reconnects after selected session-loss errors and
  repeats the same `CallTool` request in
  [`pkg/mcp/manager.go`](../../pkg/mcp/manager.go).

The last behavior is unsafe for browser mutations. Repeating a snapshot is
usually harmless; repeating `click`, `submit`, `publish`, `send`, or `buy` may
produce a duplicate effect or an unknowable result.

## Goals

- Support long, authenticated, multi-page browser workflows.
- Keep the existing browser-specialist delegation model.
- Present one stable model-facing contract for gateway, companion, and cloud
  browsers.
- Support persistent login profiles without exposing credentials, cookies,
  profile paths, or CDP endpoints to the model.
- Make mutation, approval, recovery, and uncertain-outcome behavior explicit.
- Use accessibility and DOM state first, with screenshots and vision as a
  supported fallback.
- Support uploads, downloads, screenshots, traces, and optional video through
  bounded artifact references.
- Provide safe human takeover for login, MFA, CAPTCHA, payment, and visual
  confirmation.
- Allow additional drivers and providers without changing agent authority.
- Align companion execution with the existing node identity, target, policy,
  invocation, artifact, and audit foundations.

## Non-Goals

- Building a new browser engine.
- Replacing Playwright as the initial browser implementation.
- Moving the AgentLoop, model session, memory, or channel delivery to a
  companion node.
- Forwarding every MCP or MintClaw tool through a generic remote proxy.
- Claiming exactly-once effects on third-party websites.
- Automatically bypassing CAPTCHA, MFA, anti-bot controls, site policy, or
  terms of service.
- Treating a user browser profile as safe for unattended delegated agents.
- Combining browser control and arbitrary desktop control into one implicit
  permission.
- Letting page content, tool annotations, or model arguments grant authority.

## Comparative Findings

| System | Relevant strengths | MintClaw conclusion |
| --- | --- | --- |
| Playwright MCP | Accessibility snapshots, persistent state, browser introspection, extension and CDP attachment, isolated or persistent profiles | Keep as the initial driver, but place session and authority semantics above MCP |
| `agent-browser` | CLI plus daemon, named sessions, persistent profiles, local and cloud engines, domain and action policy, live viewport | Viable optional driver; do not make a shell skill the product contract |
| Hermes | Broad typed browser tools, provider lifecycle, task isolation, cleanup, dialog and console supervision, vision and computer fallback | Reuse the capability breadth and lifecycle lessons without adopting a process-local browser monolith |
| OpenClaw | Managed profiles, local/host/node placement, diagnostics, tab cleanup, SSRF controls, attached-browser support, frame-bound computer actions | Reuse the placement and stale-frame patterns; avoid a generic route proxy and base64 artifact transport |
| Stagehand | `observe`, `act`, typed `extract`, action preview, caching, and repeatable recipes | Optional recipe/driver optimization after the core contract; do not add a second autonomous planner by default |
| Browserbase | Persistent contexts, managed cloud sessions, live view, and recordings | Optional cloud target and human-handoff backend, not a required core dependency |
| WebDriver BiDi | Emerging bidirectional browser-control standard | Future driver option; do not expose transport-specific primitives in the MintClaw contract |

Playwright MCP explicitly positions MCP as suitable for persistent state and
rich introspection in long autonomous workflows. Its persistent process remains
a transport and driver concern rather than a complete application-level
session contract.

The current `agent-browser` project is substantially more capable than the
older MintClaw heavy-image wrapper suggests. Its session, profile, policy, and
live-view features make it worth supporting as a driver adapter, but MintClaw
still needs to bind those operations to its own actors, targets, approvals, and
durable invocation state.

Hermes demonstrates the practical value of structured navigation, snapshots,
clicks, typing, images, console, CDP, dialogs, vision, provider lifecycle, and
task cleanup. OpenClaw provides the most relevant current reference for
local-versus-node browser placement and for binding coordinate actions to a
fresh screenshot frame.

Stagehand's `observe -> validate -> act` flow and caching are useful for
repeated site-specific workflows. They are optimizations above deterministic
browser primitives, not substitutes for the MintClaw authority and recovery
contract.

## Architectural Principles

### The product contract is not the driver contract

Agents call stable MintClaw browser tools. The broker may translate those calls
to Playwright library calls, Playwright MCP, `agent-browser`, CDP, or a cloud
provider. Changing a driver does not change agent authority or tool semantics.

### The browser specialist plans but does not grant authority

The specialist is the preferred execution context for long workflows because
it contains browser observations, page prompt injection, and token use. Runtime
policy still decides which targets, profiles, domains, actions, artifacts, and
commits are allowed.

### A transport session is not a browser session

An MCP stdio process, WebSocket connection, CDP endpoint, Playwright browser
context, persistent profile, and browser job have different lifetimes. The
broker must model them separately instead of inferring browser continuity from
transport continuity.

### Placement does not imply identity or authority

Selecting `gateway`, `node:macbook`, or `cloud:browserbase` answers where the
browser runs. A separately configured profile alias answers which browser
identity is used. Neither model argument may create or broaden authority.

### Persistent profiles have one writer

A persistent profile is exclusively leased to one mutating browser session.
Multiple read-only observations may be possible inside that session, but two
workers must not concurrently open or mutate the same user-data directory.

### Page content is untrusted

Text, images, downloads, tooltips, scripts, and instructions obtained from a
page are untrusted input. They cannot change target, profile, domain, approval,
credential, filesystem, or tool policy.

### Uncertain mutations fail closed

After a browser mutation reaches its acceptance boundary, a transport failure
must produce recovery from the invocation ledger or an explicit `unknown`
outcome. The broker must not blindly replay the action.

### Human control is an explicit state transition

Human takeover pauses agent mutation authority, changes the session
controller, and invalidates old snapshots and frames. Resuming automation
requires a fresh observation.

## Component Boundaries

```text
main agent
    |
    | delegates long browser workflows
    v
browser specialist
    |
    | first-party typed browser tools
    v
browser broker
    |-- actor, agent, target, and profile binding
    |-- policy, approval, dry-run, and domain enforcement
    |-- session leases and invocation ledger
    |-- artifact, audit, and recovery integration
    |
    +--> gateway browser worker
    +--> companion browser worker
    +--> optional cloud browser worker
              |
              +--> Playwright or Playwright MCP
              +--> agent-browser
              +--> Chrome DevTools MCP
              +--> provider API
```

### Browser specialist

The specialist:

- plans and performs browser workflows;
- consumes page observations and maintains job-level checkpoints;
- has only browser tools and explicitly required task-scoped inputs;
- does not receive shell, unrestricted filesystem, broad memory, unrelated
  MCP servers, or raw credential access by default;
- returns `completed`, `awaiting_approval`, `blocked`, or `failed` with a
  durable job checkpoint.

The main agent remains responsible for user communication, explicit approval
collection, and cross-system orchestration.

### Browser broker

The gateway broker is authoritative for:

- resolving model-visible target and profile aliases;
- intersecting actor, agent, target, profile, domain, and action policy;
- allocating session IDs and profile leases;
- preparing and committing sensitive actions;
- assigning stable invocation IDs;
- storing durable accepted and terminal invocation state;
- selecting the local, companion, or cloud worker;
- issuing and resolving artifact references;
- exposing bounded diagnostics and audit events.

The broker does not parse arbitrary shell commands or forward arbitrary MCP
tools.

### Browser worker

A worker runs beside the browser and owns:

- browser process and context creation;
- local profile locking;
- tabs, dialogs, downloads, uploads, and driver state;
- accessibility/DOM snapshots and screenshots;
- action execution and post-action observation;
- a bounded node-local accepted-invocation ledger;
- cleanup, TTL, heartbeat, and orphan reaping;
- optional headed or live-view presentation.

A companion worker receives typed browser commands over the existing paired
node transport. Raw CDP endpoints, credentials, and profile paths do not cross
the gateway-node connection.

### Driver adapter

The adapter translates the worker contract to a concrete browser system. It
does not decide actor authorization, approvals, target routing, or artifact
retention.

## Runtime Entities

| Entity | Purpose | Required properties |
| --- | --- | --- |
| Profile | Persistent browser identity and policy | Opaque alias, fixed target scope, credential policy, domain policy, exclusivity, storage backend |
| Session | Running browser context | Opaque ID, target, driver, profile lease, controller, owner, TTL, heartbeat, state |
| Job | User workflow and resumable plan | Job ID, owning agent/session, requested outcome, checkpoint, pending approval, terminal state |
| Tab | One page within a session | Opaque tab ID, URL origin, active state, snapshot generation |
| Snapshot | Model-safe observation | Snapshot ID, generation, tab ID, accessibility/DOM refs, optional frame ID |
| Action | One typed browser operation | Kind, normalized arguments, target element, risk, prepared hash |
| Invocation | Durable execution attempt | Stable ID, action hash, acceptance state, terminal result or `unknown` |
| Artifact | Bounded binary or large output | Opaque reference, media type, size, digest, origin, retention and redaction policy |

Browser job checkpoints may refer to sessions and tabs, but a checkpoint does
not keep an expired session alive and cannot prove an unrecorded website
mutation.

## Model-Facing Tool Surface

The initial surface should remain small:

- `browser_targets`: list model-visible targets and bounded effective
  capabilities;
- `browser_session`: `open`, `status`, `handoff`, `resume`, and `close`;
- `browser_observe`: accessibility snapshot, screenshot, tabs, dialogs,
  bounded console errors, and current URL;
- `browser_act`: execute exactly one typed action;
- `browser_extract`: optional schema-bound extraction after the core surface
  is proven.

`browser_act` may initially support:

- `navigate`;
- `click`;
- `fill` and `type`;
- `select`;
- `press`;
- `scroll`;
- `upload`;
- `download`;
- `dialog`.

Raw JavaScript evaluation, arbitrary CDP commands, browser extension
installation, profile import/export, and unrestricted network interception are
not part of the default surface.

Capability discovery describes only the effective intersection of current
target, profile, agent, and node-local policy. It reports safe limits and
availability without exposing host paths, transport endpoints, credentials,
cookies, or complete hidden policy.

## Snapshot and Action Binding

Each observation returns:

- `browser_session_id`;
- `tab_id`;
- `snapshot_id` and monotonically changing `snapshot_generation`;
- scoped element references;
- an optional screenshot `frame_id`.

An element action binds to the session, tab, and snapshot generation. Refs are
invalidated by navigation, tab replacement, human takeover, worker restart, or
a material page-state change.

A coordinate action additionally binds to the exact frame ID, viewport, and
target. It fails closed when the frame is stale. Every input call contains one
action and returns a fresh observation or an explicit reason why freshness
cannot be established.

## Session and Profile Lifecycle

Session opening resolves:

- actor and agent identity;
- target alias;
- profile alias;
- driver and worker availability;
- headed, live-view, upload, download, and tracing features;
- domain and action policy;
- lease duration and idle timeout.

The worker acquires the profile lock before starting the browser. Session
heartbeats renew the broker lease. Close, expiry, agent termination, target
disconnect, and operator revocation all trigger bounded cleanup.

A reconnect may recover an existing worker session only when both sides can
prove the same durable session and invocation state. Otherwise the session is
reported lost; the broker does not silently create a fresh browser and pretend
continuity.

## Local, Companion, and Cloud Placement

### Gateway worker

The first worker should run on the gateway and preserve the current managed
Playwright profile use case. It establishes the first-party contract without
adding a new transport dependency.

### Companion worker

The companion advertises versioned commands such as:

- `browser.session.open.v1`;
- `browser.session.status.v1`;
- `browser.observe.v1`;
- `browser.act.v1`;
- `browser.session.close.v1`.

The node is the final policy boundary. Browser commands reuse node pairing,
target grants, catalog approval, invocation identity, recovery, and audit.
Screenshots and files use the P2 artifact contract instead of base64 in
ordinary JSON.

The selected target is fixed for the lifetime of a session. Automatic fallback
may be offered only before session creation and only from a bounded operator
policy. An authenticated active session never migrates silently between
gateway, companion, and cloud.

### Cloud worker

A provider such as Browserbase may supply a managed browser, persistent
context, live view, and recording. The broker still owns MintClaw authority and
maps provider identifiers to opaque internal state. Provider credentials and
control URLs remain outside model context.

## Mutation and Recovery Semantics

Browser operations are classified by effect:

- read: observe, screenshot, inspect tabs, read bounded console state;
- navigation: open or change a URL without a known external commit;
- local edit: fill a field, select an option, or manipulate unsent page state;
- external commit: submit, publish, send, delete, purchase, book, pay, or
  confirm;
- privileged: raw evaluation, CDP, credential/profile mutation, extension or
  browser-setting changes.

Classification is runtime-derived from the typed action, resolved element,
form semantics, site policy, and configured overrides. The model or MCP server
cannot lower the risk by annotation.

Each mutating call uses a stable invocation ID. The worker records acceptance
before execution and records the terminal result afterward. After a disconnect:

- an unaccepted invocation may be safely attempted;
- an accepted invocation with a stored terminal result returns that result;
- an accepted invocation without a provable terminal state returns `unknown`;
- the agent re-observes and reconciles `unknown` rather than replaying it.

MintClaw cannot promise exactly-once behavior from an external website. It can
promise no blind replay after its own acceptance boundary.

## Approval and Dry-Run Model

Sensitive actions use preparation and commit:

1. The worker resolves the exact page, element, form, destination origin, and
   intended effect.
2. The broker returns a bounded preview containing normalized action details
   and a pre-action screenshot reference.
3. Approval binds actor, agent, target, profile, session, tab, snapshot
   generation, action hash, destination origin, policy revision, and expiry.
4. Commit revalidates every binding and refuses changed page state.

External commits require approval unless a narrowly configured actor, profile,
site, and action policy explicitly authorizes them. Unknown submit semantics
default to approval.

A dry-run profile can navigate, observe, and fill local page state, but runtime
policy denies external commit regardless of model instructions or skill text.

## Credentials and Browser Profiles

Initial profile classes:

- managed: a dedicated MintClaw profile intended for automation;
- attached-user: an existing signed-in browser explicitly attached by the
  operator;
- cloud: a provider-managed context;
- ephemeral: an isolated context destroyed at session close.

Managed and cloud profiles may support unattended operation within configured
domain and action policy. Attached-user profiles are high-trust and should
normally require explicit operator presence or a bounded arming window.

Credential filling is performed by the broker, worker, browser, or operating
system using an opaque credential alias bound to allowed origins. Secret
values, cookies, storage-state files, profile directories, CDP URLs, and
provider keys do not enter model-visible arguments, observations, checkpoints,
or audit.

## Human Takeover

Human takeover is required for workflows such as:

- initial login or account recovery;
- MFA and hardware-key prompts;
- CAPTCHA and anti-bot challenges;
- payment or purchase confirmation;
- visual review of an externally visible listing;
- unexpected site state that cannot be safely resolved by the agent.

`handoff` pauses agent actions and transfers the exclusive session controller
to a human through a local headed browser or short-lived authenticated live
view. `resume` revokes the human control token, rotates controller state, and
requires a new snapshot before the next action.

Takeover is not a hidden approval mechanism. A human may inspect or manipulate
the browser, while a later agent commit still follows current runtime policy.

## Security Boundaries

- The browser specialist receives browser tools and task-scoped data only.
- Page content and downloads are treated as untrusted external input.
- Cross-origin navigation is restricted by job/profile policy, with an
  explicit safe way to approve a required new origin.
- Private-network, loopback, cloud-metadata, and sensitive internal endpoints
  are denied by default unless the selected profile deliberately permits them.
- Raw evaluation and CDP are disabled by default and require a separate
  privileged policy.
- Worker subprocess environments are credential-scrubbed except for exact
  driver requirements.
- Upload sources and download destinations use artifact references and bounded
  file policy rather than arbitrary host paths.
- Browser storage and artifact directories use owner-only permissions,
  bounded retention, and cleanup.
- Screenshots, traces, recordings, console output, and downloads receive
  redaction and retention policy because they may contain personal or secret
  data.
- Site automation must not implicitly circumvent CAPTCHA, abuse controls,
  published site rules, or legal restrictions.

## Artifacts and Observability

Screenshots, uploads, downloads, traces, HAR files, and optional recordings use
the P2 artifact transport and media store. Binary content is never placed in
ordinary model or node-command JSON.

Each artifact records bounded provenance:

- browser session and tab;
- target and worker;
- media type, byte size, and digest;
- creation time and expiry;
- redaction status;
- whether it is model-visible, user-deliverable, diagnostic-only, or
  sensitive.

`browser_targets` or a dedicated doctor view should report:

- worker and driver readiness;
- compatible browser version;
- profile lock or corruption state;
- headed and live-view support;
- snapshot, screenshot, upload, download, tracing, and dialog capabilities;
- effective concurrency and size limits;
- safe unavailable or degraded reasons.

Diagnostics must not expose executable paths, raw profile paths, credentials,
CDP/provider endpoints, cookies, or hidden policy.

## Integration with the Node Companion Roadmap

The existing
[`node-companion-roadmap.md`](node-companion-roadmap.md) already reserves P7
for independently threat-modeled interactive application capabilities.
Browser work is one P7 capability and should use the typed-command direction
already described in [`node-companion.md`](node-companion.md).

Dependencies are:

- node P0 for bounded, fresh model-visible capability discovery;
- node P2 for artifact streaming and retained uploads/downloads;
- the existing pairing, target, policy, invocation, recovery, and audit
  foundations;
- node P7 admission for the companion-hosted browser worker;
- node P8 only for later workspace-level routing.

A companion browser vertical slice does not require moving the workspace or
AgentLoop to the node. P8 may later bind a turn or browser specialist to an
explicit compatible target, but it remains a routing layer over the browser
capability.

## Validation Strategy

Required evaluation should include:

- real-process gateway Playwright execution;
- a real companion behind the production WSS path;
- persistent-profile exclusivity and concurrent-session rejection;
- stale element-ref and screenshot-frame rejection;
- disconnect before action acceptance;
- disconnect after acceptance but before result delivery;
- recovery of a stored terminal result and explicit `unknown` otherwise;
- no duplicate submit after reconnect;
- approval invalidation after page, target, profile, policy, or action change;
- runtime dry-run denial despite contrary page or model instructions;
- prompt-injection and credential-exfiltration fixtures;
- cross-origin and private-network policy tests;
- upload and download artifact round trips with matching digests;
- session expiry, restart cleanup, and orphan reaping;
- human handoff and resume with snapshot invalidation;
- site-shaped test fixtures for listing, messaging, booking, and purchasing
  flows without production mutations in CI.

Production-site smoke tests remain explicit operator actions and stop before
external commit unless separately approved.

## Alternatives Considered

### Keep only the current Playwright MCP specialist

This is the lowest-effort option but leaves profile locking, action approval,
remote placement, replay, and recovery as conventions. It does not satisfy the
companion use case safely.

### Standardize directly on `agent-browser`

This gains sessions, profiles, policy, and a live viewport quickly, but couples
MintClaw authority to an external CLI/daemon contract and still lacks native
node invocation and approval semantics.

### Forward generic MCP tools through companions

This appears flexible but makes target policy, risk, artifacts, replay, and
compatibility depend on arbitrary future tool schemas. It conflicts with the
typed-capability and no-generic-proxy direction in the node architecture.

### Adopt the OpenClaw browser proxy protocol

An adapter may be useful later, but importing a generic route proxy and base64
file behavior would create a second transport and weaken MintClaw's typed
command and artifact boundaries.

### Build a new browser engine

There is no need. Playwright and other drivers already solve browser control.
MintClaw's missing work is the product control plane around them.

## Primary References

- [Playwright MCP](https://github.com/microsoft/playwright-mcp/blob/main/README.md)
- [Playwright browser contexts](https://playwright.dev/docs/browser-contexts)
- [Playwright authentication state](https://playwright.dev/docs/auth)
- [Playwright BrowserType and persistent contexts](https://playwright.dev/docs/api/class-browsertype)
- [Chrome DevTools MCP configuration](https://developer.chrome.com/docs/devtools/agents/get-started/configuration)
- [`agent-browser`](https://github.com/vercel-labs/agent-browser/blob/main/README.md)
- [Hermes browser tools](https://github.com/NousResearch/hermes-agent/blob/main/website/docs/user-guide/features/browser.md)
- [OpenClaw browser tools](https://github.com/openclaw/openclaw/blob/main/docs/tools/browser.md)
- [OpenClaw computer use](https://github.com/openclaw/openclaw/blob/main/docs/nodes/computer-use.md)
- [Stagehand `observe`](https://docs.stagehand.dev/v3/basics/observe)
- [Browserbase contexts](https://docs.browserbase.com/platform/browser/core-features/contexts)
- [Browserbase live view](https://docs.browserbase.com/platform/browser/observability/session-live-view)
- [MCP tools specification](https://modelcontextprotocol.io/specification/2025-11-25/server/tools)
- [MCP tool annotations](https://blog.modelcontextprotocol.io/posts/2026-03-16-tool-annotations/)
- [W3C WebDriver BiDi](https://www.w3.org/TR/webdriver-bidi/)
