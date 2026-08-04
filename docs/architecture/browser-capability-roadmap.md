# Reliable Browser Capability Roadmap

## Status

Work derived from [`browser-capability.md`](browser-capability.md). B0, B1, N1,
and N2 are merged and deployed. B2 is admitted in
[Browser Capability B2 Admission](browser-capability-b2-admission.md), including
a prerequisite repair for consecutive managed sessions. Later milestones
remain proposals until an operator selects one and a separate admission fixes
its exact scope, authority, completion evidence, and stop conditions.

The roadmap is ordered by immediate risk reduction, operator value, and
security dependencies rather than calendar dates. Browser milestone labels use
`B0` through `B6` so they cannot be confused with node-companion priorities
`P0` through `P9`.

## Starting Point

The roadmap assumes the current deployment already provides:

- a dedicated browser specialist with its own workspace and job checkpoints;
- a working Playwright MCP server launched through `npx`;
- an installed Chrome browser and one persistent automation profile;
- real listing and search workflows;
- per-agent MCP server allowlists in the MintClaw runtime;
- MCP media normalization and local large-output artifact persistence;
- durable human-interaction support in the main agent;
- the node-companion target, policy, catalog, invocation, recovery, and audit
  foundations;
- node P2 file-transfer and artifact work proceeding as its own program.

The starting point is functional but not yet a first-party browser capability.
Browser sessions, profiles, actions, approvals, and mutation outcomes remain
partly implicit in the specialist, MCP process, and skill conventions.

## Relationship to the Node Companion Roadmap

This roadmap specializes the browser part of node-companion P7. It does not
replace or reorder the broader
[`node-companion-roadmap.md`](node-companion-roadmap.md).

The dependency mapping is:

| Browser milestone | Node dependency |
| --- | --- |
| B0 | Existing gateway runtime and current browser specialist |
| B1 | Node P0 discovery principles and existing invocation semantics, even though B1 runs locally |
| B2 | Node P2 artifact contract for remote binary transfer; gateway media support may be proven earlier |
| B3 | Node P7 admission for typed browser commands plus deployed P2 artifacts |
| B4 | B1-B3 profile/session authority; no new node milestone |
| B5 | Stable B1 worker/driver seam and B2 artifact/lifecycle behavior |
| B6 | Separate interactive-computer admission and, for workspace routing, node P8 |

Browser work must not duplicate P2 file transfer, create a second node
transport, or pre-admit P8 remote workspace routing.

## Roadmap Rules

Every browser milestone follows these rules:

1. **The browser broker owns authority.** A model, browser specialist, page,
   skill, MCP server, or driver cannot grant a target, profile, credential,
   domain, action, or approval.
2. **Drivers remain replaceable.** Playwright MCP is the initial implementation
   path, not the permanent product contract.
3. **A browser session is explicit.** Transport, process, browser context,
   persistent profile, browser session, job, and tab lifetimes are not
   conflated.
4. **Persistent profiles have one writer.** A worker acquires an exclusive
   lease before opening a persistent profile and fails closed on contention.
5. **A session stays on one target.** An active authenticated session never
   silently migrates or falls back between gateway, companion, and cloud.
6. **Page state is untrusted and versioned.** Element refs and coordinate
   frames are scoped to a session, tab, and fresh snapshot generation.
7. **One call performs one action.** A model-visible mutation does not hide an
   unbounded autonomous sequence or multiple external commits.
8. **Uncertain mutations are not replayed.** After acceptance, recovery returns
   a stored terminal result or an explicit `unknown` outcome.
9. **External commits are runtime decisions.** Publish, send, delete, purchase,
   booking, payment, and unknown submit semantics use policy and bound
   approval, not prompt-only instructions.
10. **Artifacts remain out of model JSON.** Screenshots, uploads, downloads,
    traces, HAR files, and recordings use bounded artifact references.
11. **Human takeover changes the controller.** It pauses agent action,
    invalidates old observations, and requires a fresh snapshot on resume.
12. **Browser and computer authority stay separate.** Screenshot-coordinate
    browser fallback does not grant arbitrary desktop input.
13. **Capabilities are discoverable without leaking secrets.** The model can
    learn safe target, profile, feature, and limit information, but not
    credentials, cookies, raw paths, endpoints, or hidden policy.
14. **Each milestone proves one vertical slice.** No general driver, provider,
    routing, or policy framework lands without an admitted user flow and
    real-process evidence.
15. **Deployment remains deny by default.** New profile classes, targets,
    privileged actions, cloud providers, and computer control require explicit
    operator configuration after merged-main validation.

## Operating Profiles

The browser capability supports distinct profiles over one broker and worker
contract:

- **Managed automation** uses a dedicated MintClaw browser profile and bounded
  domain/action policy. It is the default autonomous mode.
- **Ephemeral automation** starts an isolated context with no retained login
  state and destroys it at close.
- **Attached-user browser** connects to an existing signed-in browser after an
  explicit high-trust operator action. It is not the unattended default.
- **Cloud browser** runs on an explicitly configured provider while MintClaw
  retains authority, policy, and opaque provider mapping.
- **Human takeover** temporarily gives a person exclusive control of an
  existing session and is a state transition, not a profile or hidden
  approval.

Profile selection is an operator-owned alias. The model cannot supply a user
data directory, cookie file, CDP URL, provider key, credential value, or hidden
policy name.

## Priority Overview

| Priority | Milestone | Operator outcome | Depends on |
| --- | --- | --- | --- |
| B0 | Stabilize the current browser specialist | Keep today's useful workflows while removing immediate replay, concurrency, availability, and cleanup hazards | Current deployed specialist |
| B1 | First-party local browser capability | Use a stable MintClaw session/observe/action contract against a gateway browser | B0 evidence and an admitted browser threat model |
| B2 | Artifacts, diagnostics, and human handoff | Move screenshots and files safely, diagnose readiness, and let a person take over and resume | B1 and the relevant P2 artifact surface |
| B3 | Companion-hosted browser | Run the same browser contract on an explicitly selected local companion without exposing CDP or generic MCP forwarding | B1, B2, node P7 admission, and deployed P2 |
| B4 | Browser identity and attached-user profiles | Reuse selected logged-in browser identities through explicit credential/profile policy | Stable B1-B3 lifecycle and human handoff |
| B5 | Providers and repeatable workflow adapters | Add cloud browsers, alternative drivers, and cached site recipes without changing authority | Stable worker/driver seam and deployed lifecycle evidence |
| B6 | Computer fallback and workspace routing | Handle non-DOM surfaces under separate authority and optionally bind browser placement to a remote workspace | Separate computer threat model and node P8 |

Priorities describe dependency order. They do not commit MintClaw to implement
every milestone or prevent a later milestone from being deferred indefinitely.

## B0: Stabilize the Current Browser Specialist

### Current limitation

The deployed specialist performs useful work, but several safety properties are
conventions:

- the generic MCP layer may reconnect and repeat a mutating tool call;
- the persistent profile has no first-party cross-process lease;
- the browser agent is not yet restricted to a minimal browser-only tool
  catalog by a documented runtime contract;
- availability is inferred from tool discovery and subprocess failure;
- profile, output, and artifact cleanup is not a complete lifecycle;
- checkpoints cannot distinguish a failed action from an accepted action with
  a lost result;
- browser package and browser compatibility are operational facts rather than
  doctor-visible state.

### Operator outcome

The existing browser specialist keeps working with its current managed profile,
while an operator can determine whether it is ready and can trust that a lost
MCP connection will not blindly repeat a potentially external mutation.

### Proposed scope

B0 changes no model-visible browser workflow contract and adds no companion or
cloud placement. Its narrow scope is:

- classify the existing Playwright MCP tools as read, navigation, local edit,
  external commit, or privileged where possible;
- make reconnect behavior configurable per MCP server/tool and disable
  automatic replay for all browser mutations;
- return an explicit uncertain result when a mutation may have reached the MCP
  server but no result is available;
- give the managed persistent profile an exclusive runtime lock with clear
  lock-owner and stale-lock behavior;
- configure the browser specialist with an explicit Playwright MCP allowlist
  and deny unrelated high-authority tools;
- expose a bounded doctor check for MCP startup, browser compatibility,
  profile lock, output directory, and key browser features;
- pin the supported MCP package and browser compatibility in deployment
  configuration;
- add bounded cleanup and retention for browser outputs and MCP artifacts;
- preserve the current job checkpoint and delegation UX.

Tool risk classification in B0 is conservative. An unknown action is not
treated as read-only merely because an MCP annotation says so.

### Suggested delivery sequence

1. Record the exact deployed browser process, profile, tool catalog, and
   successful workflow baseline from merged `main`.
2. Add a generic MCP replay-policy seam whose default preserves existing
   non-browser behavior.
3. Mark the Playwright MCP server no-replay for mutations and prove uncertain
   result handling with a fake session loss.
4. Add exclusive profile locking, stale-lock diagnostics, and a second-opener
   rejection test.
5. Reduce the browser specialist to its explicit MCP/tool allowlist.
6. Add doctor and cleanup behavior without changing the active profile.
7. Deploy behind the current browser configuration and repeat one read-only
   flow plus one approval-stopped listing flow.

### Completion evidence

B0 is complete only when:

- a snapshot may recover safely, while an accepted click or submit is never
  automatically replayed after simulated session loss;
- an uncertain mutation is visible as uncertain and does not become a generic
  retryable tool error;
- a second process cannot open the same persistent profile;
- the browser specialist cannot discover or call unrelated denied MCP servers
  or tools;
- doctor distinguishes missing browser, MCP startup failure, incompatible
  browser, locked profile, and healthy readiness without exposing secrets;
- expired outputs and artifacts are removed under bounded retention without
  deleting an active job's retained evidence;
- the deployed read-only and dry-run workflows still complete through the
  dedicated specialist;
- no first-party companion, cloud, attached-user, or computer capability is
  accidentally exposed.

### Mandatory stop conditions

Stop B0 and require a new architecture decision if:

- the MCP client cannot distinguish a definitely unaccepted request from a
  potentially accepted mutation;
- profile locking would require changing or migrating the active profile
  without a proven rollback;
- a browser-only allowlist cannot be enforced independently of prompt text;
- cleanup cannot distinguish active and retained job artifacts.

## B1: First-Party Local Browser Capability

### Current limitation

Even after B0, the model still calls externally defined Playwright MCP tools.
MintClaw does not own a stable browser session, snapshot, action, policy, or
invocation schema. This prevents safe target parity and leaves browser-specific
approvals outside the core runtime.

### Operator outcome

An authorized browser specialist can open one managed gateway browser session,
observe a page, perform one typed action at a time, recover or report uncertain
outcomes, and close the session through a stable MintClaw-owned tool surface.

### Initial model-facing surface

B1 should expose:

- `browser_targets` for bounded local readiness and effective capability
  discovery;
- `browser_session` with `open`, `status`, and `close`;
- `browser_observe` for current URL, tabs, accessibility snapshot, screenshot,
  and dialogs;
- `browser_act` for one `navigate`, `click`, `fill`, `select`, `press`,
  `scroll`, or `dialog` action.

`browser_extract`, uploads, downloads, live view, attached-user profiles,
cloud targets, raw evaluation, CDP, and generic computer control remain out of
B1 unless a fresh admission explicitly narrows one of them into the vertical
slice.

### Session contract

The broker creates opaque profile, session, tab, snapshot, action, invocation,
and artifact references. It persists:

- session owner, target, driver, profile lease, controller, TTL, and state;
- tab identity and current snapshot generation;
- stable invocation ID and prepared action hash;
- accepted and terminal invocation states;
- terminal result, cancellation, failure, or explicit `unknown`;
- policy and catalog revisions needed for revalidation.

The gateway worker is the only B1 placement. It uses Playwright or Playwright
MCP behind a driver adapter and acquires the same local persistent-profile lock
proven in B0.

### Action and approval contract

Every action references the session, tab, and fresh snapshot generation.
Element refs are scoped and invalid after navigation or material state change.
Coordinate actions are excluded from B1 unless the milestone admits exact
frame binding.

B1 must establish runtime effect classes and preparation/commit behavior:

- read and bounded navigation may proceed under current policy;
- local edits may proceed under profile and domain policy;
- external commit and unknown submit semantics require bound approval unless
  an explicit configured policy says otherwise;
- privileged evaluation or browser/profile mutation remains unavailable.

Approval binds actor, agent, target, profile, session, tab, snapshot
generation, normalized action hash, destination origin, policy revision, and
expiry. Commit fails if any binding changes.

### Suggested delivery sequence

1. Admit the browser threat model, entities, schemas, effect classes, and exact
   B1 non-goals.
2. Implement the broker state machine and gateway worker interface without
   model-visible tools.
3. Add the Playwright driver adapter and prove open, observe, one action, and
   close against a deterministic local fixture.
4. Add profile leases, session TTL, worker heartbeat, cleanup, and restart
   recovery.
5. Add invocation acceptance, terminal-result recovery, and explicit
   `unknown` behavior.
6. Add snapshot generation, scoped refs, action preparation, and approval
   revalidation.
7. Register the small tool surface only for the browser specialist and migrate
   one existing workflow from raw Playwright MCP tools.
8. Deploy deny by default, explicitly enable the existing managed profile, and
   validate from merged `main`.

### Completion evidence

B1 is complete only when:

- one real model-to-specialist-to-broker-to-browser flow completes on the
  gateway;
- the browser specialist receives only the admitted browser surface;
- profile, session, job, tab, snapshot, and invocation lifetimes are visibly
  distinct;
- stale refs and stale approvals fail before action dispatch;
- disconnect-before-acceptance may retry, while disconnect-after-acceptance
  recovers or returns `unknown` without duplicate action;
- dry-run runtime policy denies an external commit despite contrary model or
  page instructions;
- an approved commit fails after the page, destination origin, action, target,
  profile, or policy changes;
- session close, expiry, and process restart release or recover leases
  correctly;
- current raw Playwright MCP tools are not simultaneously exposed as an
  ungoverned bypass to the migrated specialist.

### Post-B1 network policy sequence

The initial B1 deployment deliberately uses exact-origin admission. That mode
remains useful for tightly restricted agents: every destination origin,
including an origin reached by an HTTP redirect, must be listed explicitly.
It is a supported lockdown policy, not the final ceiling of the browser
capability.

Network reachability should expand through separate, ordered follow-ups. Each
follow-up requires its own admission, implementation PR, tests, and deployed
evidence; it must not be mixed into a driver-compatibility or observation-bound
fix.

| Order | Working mode | Reachability | Required behavior |
| --- | --- | --- | --- |
| N0 | `exact_origins` | Only explicitly configured HTTP or HTTPS origins | Keep the deployed default. Check every navigation and redirect hop; deny an unlisted redirect origin. |
| N1 | `public_web` | Any public HTTP or HTTPS destination | Admit ordinary public-web navigation and redirects without per-site entries. Resolve and revalidate every hop, and deny loopback, private, link-local, multicast, unspecified, and cloud-metadata destinations, including DNS-rebinding transitions. |
| N2 | `any_http` | Any syntactically valid HTTP or HTTPS destination, including loopback, private, link-local, and metadata endpoints | Require an explicit high-risk operator setting. Preserve URL normalization, bounded navigation, audit, profile policy, dry-run, effect classification, and approval rules; broaden only network destination admission. |

The implementation must keep these modes explicit and non-escalating:

- profile configuration selects one mode, and `browser_targets` reports the
  effective mode without exposing hidden policy data;
- a model or page cannot select a broader mode;
- an active session never silently falls back from `exact_origins` or
  `public_web` to `any_http`;
- policy reload cannot broaden an existing session without closing it and
  opening a new session under the new policy revision;
- redirect checks use the destination of every hop rather than trusting only
  the requested URL;
- non-HTTP schemes, embedded URL credentials, raw transport endpoints, and
  arbitrary CDP or MCP forwarding remain outside all three modes;
- tests cover public-to-private redirects, DNS rebinding, IPv4 and IPv6
  literals, loopback aliases, link-local and metadata endpoints, and mode
  changes during an active session.

The delivery order is N1 first and N2 only after N1 has merged and has live
gateway evidence. N2 must reuse the same broker and driver contract; it is a
clearly labeled destination-policy expansion, not a second browser stack.

N1 is now merged and has live gateway evidence for both public navigation and
loopback denial. N2 is admitted separately in
[Browser Capability N2 Admission](browser-capability-n2-admission.md).

### Mandatory stop conditions

Stop B1 if:

- the driver cannot expose a usable acceptance boundary for mutations;
- action semantics require arbitrary MCP schemas in the public contract;
- approval must trust model-supplied risk or target-element descriptions;
- persistent state cannot recover without exposing profile or transport
  secrets;
- the vertical slice expands to companion routing before local recovery and
  approval evidence exists.

## B2: Artifacts, Diagnostics, and Human Handoff

B2 is admitted as dependency-ordered, separately deployed vertical slices in
[Browser Capability B2 Admission](browser-capability-b2-admission.md). Its first
prerequisite is a B1 lifecycle repair proving consecutive managed sessions in
one gateway process without a restart.

### Operator outcome

An agent can exchange bounded browser files and images without putting binary
data in model JSON, an operator can diagnose browser readiness, and a person
can take exclusive control of a session and safely return it to automation.

### Artifact surface

B2 adds:

- screenshot artifacts with media type, size, digest, provenance, redaction,
  and expiry;
- upload from an authorized retained artifact;
- download into an opaque retained artifact;
- optional bounded Playwright trace or HAR evidence for explicit diagnostics;
- channel delivery through existing media/artifact integration.

Companion transfer uses the P2 artifact protocol. Gateway-local work may use
the same logical artifact contract before remote transfer is enabled, but it
must not invent an incompatible spool or reference format.

Routed screenshot delivery requires a durable outbound transaction. P2 owns
the retained bytes and exact delivery claim; the outbox intent is the single
recoverable owner of publication. The runtime admits that intent before it
claims the artifact, and an exact replay must be able to publish a pending
intent after interruption without creating a second claim. Gateway startup
first restores the intent's typed P2 claim prerequisite, then re-enqueues only
intents whose transport call never started. A rejected startup enqueue releases
in-process ownership while leaving the intent pending for the next restart.
Interrupted transport attempts become ambiguous and are not blindly retried. A
non-durable direct route must reject screenshot capture before bytes are retained.

Browser uploads do not accept arbitrary host paths. Downloads are not exposed
as arbitrary filesystem destinations. Both directions remain bound to actor,
agent, session, target, tab, size, media, digest, retention, and policy.

### Diagnostics

Bounded browser diagnostics report:

- broker, worker, driver, and browser readiness;
- compatible driver/browser versions;
- target and profile availability;
- active profile lease or safe locked state;
- headed, live-view, screenshot, upload, download, tracing, dialog, and
  coordinate-action support;
- session and artifact limits;
- redacted degraded or unavailable reasons.

Diagnostics remain passive and cannot renew a session lease, acknowledge an
invocation, retry a tool, or retain an artifact.

### Human takeover

`browser_session handoff`:

- pauses agent mutation authority;
- records a controller transition;
- routes a durable release question to the authenticated operator;
- admits the initial implementation only for an already visible local headed
  browser and creates no remote view credential;
- invalidates element refs, frame IDs, and prepared actions;
- allows explicit human interaction without revealing provider or CDP
  credentials.

For the local headed slice, the authenticated release answer is the operator's
attestation that physical input has stopped; MintClaw blocks its own worker but
does not sandbox a trusted desktop operator. Expiry or cancellation closes the
session. A remote provider must prove technical view revocation before it can
advertise handoff.

`resume`:

- revokes the takeover token;
- restores the agent controller only after the human releases control;
- produces a fresh snapshot generation;
- does not treat the human's actions as approval for a later agent commit.

An authenticated remote or companion-hosted view is a later slice. It must
reuse the same exclusive controller transitions and add bounded issuance,
revocation, expiry, disconnect, and routing evidence without exposing CDP or a
generic computer-control surface.

### Suggested delivery sequence

1. Map browser artifacts to the deployed P2/media contract and fix exact size,
   retention, redaction, and delivery limits.
2. Add screenshots, then upload and download, against gateway fixtures.
3. Add remote-transfer framing only after deployed P2 evidence exists.
4. Add optional trace/HAR capture with disabled defaults and sensitive-data
   warnings.
5. Add passive doctor output and degraded-state tests.
6. Add local headed takeover, controller transitions, token expiry, and resume.
7. Prove one image download and delivery plus one human-assisted login fixture.

### Completion evidence

B2 is complete only when:

- screenshot and binary download artifacts round-trip with matching digests;
- no binary file bytes appear in ordinary model, event, or node-command JSON;
- unauthorized artifact, cross-session upload, oversize file, expired
  reference, and unsupported media fail closed;
- downloads survive the browser-context cleanup only when explicitly retained;
- traces and recordings are disabled by default and obey sensitive retention;
- doctor is model-safe and passive;
- agent action is impossible during human control;
- takeover expiry and disconnect do not leave an uncontrolled session;
- resume invalidates old refs and requires a fresh observation.

### Mandatory stop conditions

Stop B2 if:

- remote browser artifacts require a second transfer protocol beside P2;
- live view exposes a raw unauthenticated browser, VNC, CDP, or provider URL;
- artifact retention cannot separate user deliverables from sensitive
  diagnostics;
- takeover cannot enforce one exclusive controller.

## B3: Companion-Hosted Browser

### Current limitation

The first-party B1 browser runs only on the gateway. It cannot use a browser,
login profile, local network, display, or operator presence available on a
paired companion.

### Operator outcome

An authorized browser specialist can select a configured companion target and
run the same session, observation, action, artifact, approval, and recovery
contract against a browser physically hosted on that node.

### Node capability surface

The first companion surface should remain typed and narrow:

- `browser.session.open.v1`;
- `browser.session.status.v1`;
- `browser.observe.v1`;
- `browser.act.v1`;
- `browser.session.close.v1`.

The node advertises schemas and safe feature descriptors. The gateway
intersects those claims with paired catalog approval, actor and agent target
policy, profile policy, and fresh node-local policy.

The companion worker owns the browser process, local profile lock, tabs,
driver, node-local accepted-invocation ledger, session heartbeat, and cleanup.
The gateway owns model-visible aliases, routed actor identity, approval,
artifact references, and durable orchestration state.

### Placement and failure semantics

- The target is chosen before session creation from a bounded alias.
- The model cannot provide an endpoint, node ID, CDP URL, profile path, or
  connection credential.
- Raw CDP and MCP stdio remain node-local.
- An active session never silently falls back to the gateway or another node.
- Node disconnect reports session state honestly; it does not create a fresh
  local browser.
- Reconnect recovers only when both ledgers prove the same accepted and
  terminal invocation state.
- Browser media uses P2 artifacts rather than base64 command results.

### Node-local policy

The node is final enforcement for:

- allowed browser drivers and executable identity;
- allowed profile aliases and their local storage;
- domain and private-network access;
- action classes and privileged features;
- headed display and human-takeover availability;
- session, tab, time, memory, output, and artifact limits;
- local credential integration;
- raw evaluation, CDP, extension, and computer-control denial.

Gateway approval can narrow but cannot broaden node-local authority.

### Suggested delivery sequence

1. Admit one concrete companion and one managed profile as the B3 vertical
   slice.
2. Add browser capability descriptors and node-local policy without model
   visibility.
3. Run the existing worker beside the node browser and reuse the B1 driver
   adapter.
4. Add open, observe, one non-commit action, and close over the production WSS
   path.
5. Add node/gateway invocation-ledger reconciliation and disconnect tests.
6. Add screenshot and download artifacts over deployed P2.
7. Add bound external-commit approval and one dry-run listing fixture.
8. Deploy deny by default, enable the selected target/profile explicitly, and
   validate from merged `main`.

### Completion evidence

B3 is complete only when:

- a real browser on a real paired companion completes the admitted workflow
  through model-visible MintClaw tools;
- the gateway and node enforce the same target, profile, domain, and action
  intersection;
- raw CDP, MCP endpoints, profile paths, cookies, and credentials never cross
  into model-visible state;
- disconnect before and after action acceptance has explicit tested outcomes;
- no accepted mutation is blindly replayed after node reconnect;
- a target disconnect never silently moves the session to the gateway;
- screenshots and files use P2 with matching digests and bounded retention;
- a stale profile lease, stale ref, stale approval, unauthorized actor, and
  unauthorized target all fail closed;
- the companion remains a capability host rather than another agent or
  workspace scheduler.

### Mandatory stop conditions

Stop B3 if:

- implementation requires generic `mcp.tools.call.v1` forwarding for browser
  behavior;
- raw CDP must cross the node transport;
- the worker cannot durably distinguish accepted, terminal, and unknown
  actions across disconnect;
- artifacts cannot use the deployed P2 contract;
- browser placement becomes implicit remote workspace routing.

## B4: Browser Identity and Attached-User Profiles

### Operator outcome

An operator can deliberately select a managed, ephemeral, cloud, or existing
signed-in browser identity without revealing profile storage or credentials to
the browser specialist.

### Profile authority

Profiles are operator-created aliases bound to:

- allowed actors and agents;
- allowed gateway, node, or provider targets;
- allowed origins and cross-origin transitions;
- allowed action classes;
- credential aliases and injection mode;
- unattended, operator-present, or bounded-arming requirements;
- persistence, backup, migration, and retention policy;
- headed, live-view, and takeover behavior.

Profile export, cookie extraction, storage-state retrieval, and raw credential
reads are denied model surfaces. Credential injection is origin-bound and
occurs inside the broker, worker, browser, or operating-system credential
facility.

### Attached-user browser

An existing Chrome session may be attached through an explicitly supported
Chrome DevTools MCP or browser extension flow. It is high-trust because the
profile may expose personal sessions beyond the current task.

The initial attached mode should require:

- explicit operator activation;
- visible browser selection and consent;
- a bounded arming window;
- domain and action restrictions narrower than the full profile;
- one active MintClaw controller;
- immediate revocation and detach;
- no unattended production default.

### Suggested delivery sequence

1. Define the profile schema, storage ownership, credential boundary, migration
   rules, and revocation behavior.
2. Move the existing managed profile behind an opaque alias without changing
   its data.
3. Add ephemeral profiles and prove complete cleanup.
4. Add origin-bound credential injection using one supported secret backend.
5. Add attached Chrome on one local platform with explicit operator consent.
6. Add companion attached-browser support only after local evidence and B3
   target policy are stable.

### Completion evidence

B4 is complete only when:

- the model cannot enumerate or read credentials, cookies, storage state,
  profile paths, or raw browser endpoints;
- two workers cannot attach to or mutate the same profile concurrently;
- profile revocation prevents new actions and closes or quarantines active
  sessions according to policy;
- credential injection refuses a mismatched origin;
- ephemeral profiles leave no retained login state;
- attached-user mode is visibly armed, bounded, revocable, and disabled by
  default;
- human takeover remains distinct from attached-profile approval.

## B5: Providers and Repeatable Workflow Adapters

### Operator outcome

MintClaw can select an explicitly configured cloud browser or alternative local
driver and can optimize stable site workflows without changing browser
authority, approval, or recovery semantics.

### Driver and provider seam

Candidate adapters include:

- Playwright library as an alternative to Playwright MCP process management;
- `agent-browser` for its daemon, named sessions, profiles, and live viewport;
- Browserbase for managed contexts, sessions, live view, and recording;
- another cloud provider when a concrete operator need justifies it;
- WebDriver BiDi when implementation maturity and browser coverage justify it.

Every adapter must map to the same MintClaw session, snapshot, action,
invocation, artifact, policy, and cleanup states. Provider-specific session IDs,
URLs, credentials, and billing details remain opaque.

### Repeatable workflow adapters

After deterministic actions are stable, site-specific recipes may add:

- typed extraction schemas;
- observed-action validation;
- cached selectors or semantic action plans;
- post-action invariants;
- versioned compatibility metadata;
- fallback to fresh observation when a cached action is stale.

Stagehand may supply `observe`, `act`, `extract`, and caching inside a driver or
recipe adapter. Its autonomous agent mode is not nested inside the MintClaw
browser specialist by default.

Recipes cannot:

- bypass runtime approval or dry-run policy;
- embed credentials or cookies;
- convert a multi-commit workflow into one opaque tool call;
- silently change target, profile, origin, or provider;
- treat a cache hit as proof that current page state is safe.

### Suggested delivery sequence

1. Define driver conformance tests from the deployed B1-B4 contract.
2. Admit one provider or driver based on a real use case, not abstraction
   completeness.
3. Prove session creation, observation, action, artifact, uncertain outcome,
   cleanup, and billing/resource limits.
4. Add one versioned repeatable recipe for an existing operator workflow.
5. Measure reliability, latency, model tokens, and recovery against the
   ordinary Playwright path.
6. Retain the adapter only if evidence justifies its operational cost.

### Completion evidence

B5 is complete only when:

- a new adapter passes the same authority and failure conformance suite as the
  initial driver;
- provider outage or quota exhaustion cannot trigger silent target fallback;
- provider URLs and credentials remain absent from model and audit output;
- session TTL and cleanup prevent orphaned billable browsers;
- a cached recipe validates current page state and safely falls back when
  stale;
- runtime approval and invocation recovery remain authoritative around every
  adapter.

## B6: Computer Fallback and Workspace Routing

### Computer fallback

Full desktop input is a separate interactive capability with its own threat
model, operator profile, platform permissions, arming, target policy, and node
commands. Browser authority does not imply desktop authority.

An admitted browser-coordinate fallback may remain within `browser.act.v1`
only when it:

- targets the browser viewport rather than an arbitrary display;
- binds one action to the exact node, browser session, tab, viewport, and fresh
  screenshot frame ID;
- rejects stale frames and coordinate mismatches;
- serializes input and returns a fresh screenshot;
- does not cross native application or operating-system boundaries.

Native dialogs or non-DOM surfaces require separately admitted
`computer.*.v1` capabilities with explicit operator arming.

### Remote workspace routing

After node P8 is admitted, a browser specialist or turn may select an explicit
remote workspace target and route compatible browser calls to that target.
This remains a routing layer over B3:

```text
browser specialist
    |
    v
workspace context: target=macbook
    |
    +-- browser tools -> admitted B3 browser capability
    +-- artifacts     -> P2 artifact capability
    +-- approvals     -> gateway authority
    +-- memory/channel/model session -> gateway
```

Unsupported browser profiles or tools fail explicitly. They do not run on the
gateway merely because the selected workspace target lacks them.

### Completion evidence

B6 is complete only when:

- browser-only coordinate actions cannot address another window or display;
- stale frame IDs fail closed before input;
- native computer control is independently configured, armed, and audited;
- workspace routing selects only compatible admitted browser targets;
- unsupported tools never silently run on another placement;
- moving a routing context does not migrate an active browser session;
- gateway approval, memory, model session, and channel delivery remain on the
  gateway.

## Cross-Milestone Evaluation

Every admitted milestone should add evidence to a common browser evaluation
suite:

- deterministic accessibility/DOM fixture workflows;
- vision and screenshot fixtures where DOM state is insufficient;
- stale element and stale frame rejection;
- profile lease contention and process restart;
- driver crash before and after action acceptance;
- gateway-node disconnect before and after acceptance;
- terminal result recovery and explicit unknown outcome;
- approval preparation and invalidation races;
- dry-run enforcement against page prompt injection;
- cross-origin, private-network, and credential-exfiltration attempts;
- artifact digest, size, expiry, cross-session access, and cleanup;
- human takeover, expiry, disconnect, and resume;
- browser and provider version skew;
- bounded token, observation, artifact, session, and concurrency limits;
- real-process tests through the same model-visible tools used in production.

Site-shaped fixtures should represent listing creation, messaging, ticket
search, booking, and purchasing. CI does not perform production-side external
commits.

## Milestone Admission Checklist

Before implementing any browser milestone:

- identify one concrete deployed operator workflow;
- name authenticated actors, agents, targets, and profile classes;
- state whether the browser runs on gateway, companion, or cloud;
- define the minimum typed model and worker command surfaces;
- document profile storage, credential, network, filesystem, and process
  authority;
- define snapshot/ref/frame freshness and invalidation;
- classify action effects and approval requirements;
- define prepare, accept, commit, cancel, timeout, recovery, and unknown
  boundaries;
- define session, tab, profile, invocation, artifact, and cleanup lifetimes;
- set domain, size, time, concurrency, output, and retention limits;
- identify the exact P2, P7, or P8 dependencies;
- define model-safe discovery and diagnostics;
- identify one real-process end-to-end test and relevant failure injection;
- list explicit non-goals and mandatory stop conditions;
- reject a generic framework without a consumer in the same milestone.

After implementation:

- validate merged `main` rather than only the feature branch;
- deploy with new browser authority disabled by default;
- enable only the admitted target and profile;
- verify real profile locking, cleanup, health, and artifact retention;
- inject disconnects around a mutation and prove no blind replay;
- verify logs and artifacts contain no unintended credentials or raw endpoints;
- record operational evidence and residual limitations;
- decide explicitly whether evidence supports admitting the next milestone.

## Roadmap Non-Goals

- replacing the existing browser specialist with browser calls in every main
  agent turn;
- building a browser engine;
- adopting an external CLI, MCP server, or cloud provider as MintClaw's
  authority boundary;
- forwarding arbitrary MCP tools through a companion;
- sending CDP endpoints, profile paths, credentials, cookies, or storage-state
  files through model-visible arguments;
- encoding screenshots, uploads, downloads, traces, or recordings as base64 in
  ordinary node JSON;
- blindly replaying accepted browser mutations;
- claiming exactly-once side effects from third-party websites;
- approving a mutation based only on model text or untrusted tool annotations;
- allowing page content to expand domains, profiles, targets, tools, or
  credentials;
- silently falling back or migrating active sessions between placements;
- enabling attached-user profiles, cloud providers, raw evaluation, CDP,
  computer control, or broad domains by default;
- treating human takeover as implicit approval for later automation;
- bypassing CAPTCHA, MFA, anti-bot controls, site policy, or legal
  restrictions;
- moving the full AgentLoop, memory, model session, approval authority, or
  channel delivery to a companion;
- treating this roadmap as release scheduling or implementation admission.
