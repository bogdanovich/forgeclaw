# Browser Capability B2 Admission

## Status And Decision

Browser milestone B2, **Artifacts, Diagnostics, and Human Handoff**, is
admitted as the dependency-ordered sequence in this document.

B2 may add:

- bounded screenshot artifacts and routed channel delivery;
- upload from, and download into, the deployed P2 retained-artifact contract;
- passive browser readiness diagnostics; and
- exclusive human takeover followed by a safe agent resume.

B2 does not add a browser engine, a second artifact spool, generic Playwright
or CDP forwarding, arbitrary host paths, credential management, companion
placement, or general computer control.

The currently observed consecutive-session regression is a prerequisite, not
part of an artifact or handoff slice. Before B2 code lands, the deployed B1
capability must prove two complete managed sessions in the same gateway
process:

```text
open -> observe -> close -> open -> observe -> close
```

The second session must not require a gateway restart, inherit a tab from the
first session, or fail because driver, proxy, profile, or policy state leaked
across the close boundary.

## Existing Evidence And Reused Contracts

B1 and its N1/N2 network-policy follow-ups are merged and deployed. The
first-party browser specialist can discover the managed target, open a broker-
owned session, observe, navigate through the worker-owned proxy, and close.
Live evidence covers public HTTPS and an operator-controlled loopback fixture
under the explicitly configured `any_http` mode.

Node-companion P2 is also merged and deployed. B2 reuses these P2 properties:

- opaque `transfer-artifact://` retained-artifact references;
- a bounded gateway transfer spool;
- digest, size, media type, filename, expiry, and terminal-state metadata;
- ownership bound to workspace, agent, actor, route, session, and tool call;
- authenticated upload and download framing for later companion placement;
- idempotent handoff from a retained artifact to the existing media store; and
- `media://` references for routed channel delivery.

Possessing either kind of reference is not authority. Each browser artifact
operation revalidates the current browser session, tab, profile revision,
authenticated route owner, and retained-artifact owner.

## Admitted Operator Workflows

### Screenshot and delivery

1. The browser specialist opens and observes an authorized session.
2. It requests one screenshot for the current tab and snapshot generation.
3. The worker captures bounded PNG bytes into the gateway transfer spool.
4. The tool result returns metadata and an opaque retained-artifact reference,
   never inline bytes or a host path.
5. When delivery is requested, the gateway claims the artifact once into the
   media store and routes the resulting media reference to the originating
   channel.
6. Closing the browser session does not remove an explicitly retained user
   deliverable before its artifact expiry.

### Upload

1. The specialist observes a fresh file-input element.
2. It selects one authorized, committed retained artifact.
3. A typed upload action binds the artifact digest and the fresh element ref to
   the prepared invocation.
4. The worker supplies those bytes to that exact input without exposing a host
   path to the model or page.
5. Form submission, publishing, or any other external commit remains a
   separate typed action with the existing risk and approval policy.

### Download

1. The specialist observes a fresh element that can initiate one download.
2. A typed download action establishes a bounded download expectation before
   dispatching the exact element action.
3. The worker accepts at most one regular download, streams it into the
   transfer spool, verifies the observed size and digest, and returns an opaque
   retained-artifact reference.
4. Optional channel delivery uses the same one-time media handoff as a
   screenshot.

The action does not select an arbitrary filesystem destination, recursively
download a tree, or hide a later submit or purchase.

### Passive readiness

An operator or specialist can inspect a bounded readiness projection before a
session is opened. Reading it cannot start a browser, renew a lease, retry an
invocation, capture bytes, or change controller state.

### Human takeover and resume

1. The specialist requests handoff for one ready session.
2. The broker pauses agent mutations, records the controller transition, and
   invalidates all observations and prepared actions.
3. The authenticated operator receives a short-lived view through routed
   delivery. The model receives only redacted handoff state, never the view
   token, provider credential, proxy address, or CDP endpoint.
4. Exactly one human controller may interact while the session is handed off.
5. Release, expiry, disconnect, or explicit cancellation revokes view access.
6. Resume succeeds only after human control is released, rotates the snapshot
   generation, and requires a new observation before any agent action.

Human actions are audit events, not approval for a later agent action.

## Model-Visible Surface

B2 extends the existing small first-party surface rather than exposing a
driver schema.

### `browser_observe`

The request gains an optional `screenshot` boolean. When false or omitted,
existing observation behavior is unchanged. When true, the same call captures
one screenshot after producing the observation and returns a bounded artifact
descriptor:

```json
{
  "ref": "transfer-artifact://opaque",
  "kind": "screenshot",
  "content_type": "image/png",
  "filename": "browser-screenshot.png",
  "size": 12345,
  "sha256": "hex-digest",
  "expires_at": 1770000000,
  "session_id": "session-alias",
  "tab_id": "tab-alias",
  "snapshot_generation": 4,
  "truncated": false
}
```

The descriptor contains aliases and safe metadata only. The screenshot is
bound to the observation's session, tab, and generation. A partial or
oversized capture fails without publishing a committed reference.

### `browser_act`

The typed action set gains:

- `upload`, with one fresh file-input element ref and one retained-artifact
  ref; and
- `download`, with one fresh element ref and optional routed delivery.

Both use the existing prepare, effect classification, approval, acceptance,
terminal recovery, and no-blind-replay machinery. `upload` is not itself an
external commit. `download` is read-only unless the broker cannot prove that
classification, in which case policy treats it as unknown rather than
silently weakening approval.

### `browser_targets`

Discovery gains a safe capability and limit projection for screenshots,
uploads, downloads, diagnostics, and handoff. Unsupported features are false;
their absence is not inferred from a failed model action.

### `browser_session`

The existing operation enum gains `handoff` and `resume`. Handoff's sensitive
operator view material is routed outside model-visible JSON. Status reports
only `agent`, `human_pending`, `human`, `resume_pending`, or terminal
controller state plus bounded expiry and recovery guidance.

No new generic browser, filesystem, media, or computer tool is admitted.

## Artifact Ownership And Lifecycle

The gateway transfer spool remains the source of truth for retained browser
bytes. Browser-created records use the existing P2 artifact format and add
browser provenance as bounded metadata. The authoritative ownership
intersection is:

1. authenticated actor and route;
2. workspace, selected agent, and routed session;
3. browser target and profile grant;
4. browser session, current controller, and policy revision;
5. tab and snapshot generation when an operation uses page state;
6. exact prepared invocation and tool call; and
7. committed artifact record, digest, expiry, and media policy.

A screenshot or download is staged while bytes arrive and becomes model-
visible only after atomic commit and digest verification. Failed, canceled,
oversized, expired, or restart-orphaned staging records are not reusable.

The initial limits are operator-owned configuration with conservative product
defaults and hard maxima. They cover screenshot bytes, upload bytes, download
bytes, artifacts per session, retained total bytes, retention duration, and
capture timeout. A model cannot raise them. Filenames are sanitized display
metadata, not paths.

User deliverables and sensitive diagnostics use distinct retention classes.
Trace, HAR, video, and live-view recording are disabled in the initial B2
slice. Adding any of them requires a later focused admission update that fixes
redaction and sensitive retention.

## Driver Boundary

The broker owns authorization, lifecycle, artifact records, action semantics,
and controller state. The replaceable worker interface may gain only typed
operations needed to:

- capture a PNG stream for a specified tab;
- provide a verified retained artifact to a specified file input;
- expect and stream one download caused by one action;
- report passive supported-feature and compatibility facts; and
- acquire and release an exclusive human controller.

The Playwright adapter may use driver-native primitives behind that interface.
Raw Playwright tool names, arbitrary JavaScript, CDP messages, browser paths,
proxy endpoints, and provider session URLs never cross the model-visible
contract.

## Passive Diagnostics Contract

The readiness projection may report:

- broker, worker, driver, browser, and enforcing-proxy readiness;
- compatible or incompatible driver/browser version state;
- configured target and profile availability;
- profile lease availability or a safe locked state;
- headed view, screenshot, upload, download, and handoff support;
- configured session and artifact limits; and
- a bounded degraded or unavailable code with operator recovery guidance.

It must redact executable paths, process arguments, environment variables,
credentials, cookies, headers, proxy addresses, profile directories, artifact
paths, view tokens, and complete URLs with queries. Passive diagnostics do not
open files, resolve artifact refs, retain output, or alter browser state.

## Controller State And Recovery

Agent actions are accepted only in `agent` controller state. Handoff first
persists `human_pending`, invalidates the current snapshot generation, and
then acquires the human view. It publishes `human` only after the view is
ready. Failure returns to a safe agent-paused or terminal state; it never
leaves two controllers active.

Resume first revokes the view and proves the human controller released the
worker. Only then may it publish `agent`, increment the snapshot generation,
and require a fresh observation. If release is uncertain, resume returns a
bounded unknown/lost result and keeps agent mutation denied.

Gateway restart, worker loss, token expiry, and operator disconnect reconcile
to one of two safe outcomes: exclusive human control with a bounded expiry, or
no controller with the session closed/lost. Recovery never assumes agent
control merely because an in-memory view record disappeared.

## Dependency-Ordered Delivery

1. Merge this admission document.
2. Fix the B1 consecutive-session lifecycle regression and deploy proof of two
   complete managed sessions without a gateway restart.
3. Add screenshot capture, P2 retention, and media delivery as one vertical
   slice; deploy and deliver one screenshot through the browser specialist.
4. Add upload and download over the same retained-artifact contract; deploy
   deterministic local round-trip and routed-delivery evidence.
5. Add passive readiness diagnostics and degraded-state tests; deploy a
   read-only diagnostic proof.
6. Add exclusive local human handoff and safe resume; deploy a deterministic
   human-assisted fixture proof.

Each numbered code slice is a separate focused pull request. A later slice
starts only after its dependency is merged, deployed, and live-validated.

## Exact Completion Gates

B2 is complete only when:

- the B1 consecutive-session prerequisite passes in tests and on the deployed
  gateway without a process restart;
- screenshot and binary download artifacts round-trip with matching size and
  digest;
- browser, event, node-command, trace, and model JSON contain no binary bytes;
- the artifact spool rejects wrong owner, route, agent, browser session, tab,
  generation, digest, expiry, media type, and size;
- a cross-session upload, arbitrary host path, unsupported file input,
  multiple download, expired reference, and oversize stream fail closed;
- a download survives browser cleanup only when committed for retention;
- channel delivery is owner-bound and idempotent through the existing media
  store;
- upload selection and later external commit remain separate actions;
- diagnostics are passive, bounded, redacted, and accurate in healthy,
  missing-driver, incompatible-driver, locked-profile, and degraded states;
- agent action is impossible while handoff is pending or human control is
  active;
- a model cannot read or replay the human view credential;
- takeover expiry, disconnect, worker loss, and restart never leave an
  uncontrolled session or two controllers;
- resume revokes human access, invalidates old refs and prepared actions, and
  requires a fresh observation; and
- merged-main deployment completes all four B2 live proofs with closed
  sessions, bounded traces, artifact cleanup evidence, and healthy services.

## Mandatory Stop Conditions

Stop the affected B2 slice and return to architecture if:

- browser bytes require a second spool, reference format, transfer protocol,
  node transport, or media store beside P2 and existing media delivery;
- the model must receive base64, a host path, executable driver schema, CDP
  endpoint, proxy address, provider URL, or view credential;
- upload or download cannot bind to the current authenticated owner, browser
  session, tab, profile revision, and exact action;
- a screenshot or download must be published before size and digest are known;
- diagnostics must mutate runtime state to determine readiness;
- live view exposes unauthenticated browser, VNC, CDP, or provider access;
- takeover cannot enforce exactly one controller or survive restart safely;
- resume must preserve old element refs, prepared actions, or approvals;
- the slice grants companion placement, attached-user identity, stored
  credentials, arbitrary filesystem access, or generic computer input; or
- live validation requires an irreversible external commit.

## Non-Goals

B2 does not admit companion-hosted browsers, attached-user profiles, cookie or
credential import, cloud browser providers, arbitrary request headers, client
certificates, site-specific adapters, generic JavaScript, raw Playwright MCP,
CDP forwarding, desktop control, VNC, remote workspace routing, directory
transfer, archive extraction, trace/HAR/video capture by default, purchases,
bookings, publication, or payment.
