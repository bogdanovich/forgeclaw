# Browser Capability B1 Admission

## Status And Decision

Browser milestone B1, **First-Party Local Browser Capability**, is admitted as
the dependency-ordered sequence in this document.

B1 replaces one deployed browser-specialist workflow's model-visible raw
Playwright MCP calls with four MintClaw-owned tools backed by one local gateway
worker and one operator-configured managed profile. The initial driver may use
the pinned Playwright MCP server internally, but its tool names, schemas,
transport, profile path, and process lifetime are not part of the MintClaw
contract.

The admitted model-visible tools are:

- `browser_targets`;
- `browser_session` with `open`, `status`, and `close`;
- `browser_observe`; and
- `browser_act` with one typed action per call.

The admitted action kinds are `navigate`, `click`, `fill`, `select`, `press`,
`scroll`, and `dialog`. They operate on accessibility observations and fresh,
scoped references. B1 does not admit screenshots, coordinate actions, uploads,
downloads, extraction, live view, human takeover, companion or cloud workers,
attached-user or ephemeral profiles, raw JavaScript, CDP, arbitrary MCP
forwarding, or generic computer control.

All new browser authority is disabled by default. Deployment enables only the
existing browser specialist, the local `gateway` target, one `managed` profile,
and the exact origins required by the selected workflow.

## Admission Evidence

B0 is complete and deployed:

- core merge `d733841788e54678e535aedf9353233d1c3383db` and workspace
  merge `02b93d7955b3e4088b971fb88fbe73c6183a10b3` are ancestors of the
  B1 base or deployed workspace state;
- the deployed Playwright server uses `session_loss_replay=never`;
- the managed profile has an exclusive OS-backed lease;
- the browser specialist has an explicit MCP-server allowlist;
- operator probes return bounded startup, busy, and compatibility state;
- MintClaw-owned MCP text artifacts have bounded cleanup;
- one read-only browser workflow completed through the specialist; and
- one listing workflow reached `awaiting_approval` with
  `external_mutation=false`.

B0 also established the residual limitation that motivates B1: the model still
sees external Playwright tool schemas, while browser sessions, observations,
effect classification, approvals, and accepted invocation outcomes are not
MintClaw-owned runtime entities.

## Admitted Operator Workflow

The first vertical slice is a managed-profile listing dry run:

1. the authenticated user asks the main agent to prepare an existing retained
   item for listing;
2. the main agent delegates synchronously to agent `browser`;
3. the specialist discovers the configured local target and managed profile;
4. it opens one session, navigates to an explicitly allowed origin, observes
   accessibility state, and performs one typed action per call;
5. local form edits may proceed when runtime policy permits them;
6. a submit-like or otherwise unknown action is prepared by the broker and
   stops for bound human approval, or is denied by dry-run policy;
7. the specialist saves its existing job checkpoint and closes the browser
   session; and
8. the main agent reports `completed`, `awaiting_approval`, `blocked`, or
   `failed` from the specialist's durable result.

CI proves the same sequence against a deterministic site-shaped fixture. The
merged-main deployment smoke test uses the existing managed profile and stops
before an external commit. No production listing, message, purchase, booking,
payment, deletion, or other external mutation is required as B1 evidence.

## Identities, Placement, And Sources Of Truth

### Authenticated identities

- **Actor:** the authenticated MintClaw inbound actor and conversation origin
  already carried by the turn runtime. A model-supplied actor is never used.
- **Requesting agent:** the registered `browser` specialist. The main agent may
  delegate, but it does not inherit browser tools.
- **Target:** the operator-configured alias `gateway`. It resolves only to the
  current gateway process.
- **Profile:** the operator-configured alias `managed`. It resolves to the
  existing dedicated automation profile and B0 lock configuration.
- **Controller:** `agent` for all B1 sessions. Human controller transitions are
  deferred to B2.

### Authoritative sources

| Decision | Authoritative source |
| --- | --- |
| Actor and conversation origin | Trusted turn execution context |
| Agent identity | Agent registry entry executing the tool |
| Target and profile aliases | Validated operator browser configuration |
| Agent browser grant | Effective agent tool policy intersected with browser configuration |
| Profile path and lease | Operator configuration and the B0 OS lock |
| Allowed origins and dry-run mode | Effective target/profile policy |
| Driver compatibility | Pinned driver adapter and live bounded handshake |
| Session, tab, and snapshot identity | Browser broker persistent state |
| Element semantics and current origin | Fresh driver observation, treated as untrusted input |
| Effect class | Broker classification of the typed action and fresh observation |
| Approval binding | Broker-prepared record plus existing durable approval grant |
| Action acceptance and terminal outcome | Worker invocation ledger |

Model text, page text, external MCP annotations, driver descriptions, a job
checkpoint, a URL string returned by a page, and a process PID are not sources
of authority.

## Threat Model And Protected Properties

The local managed browser may contain authenticated cookies and can create
external effects. Page content, accessibility labels, URLs, dialogs, and driver
results are untrusted. A browser or driver process may hang, crash, disconnect,
or return after the caller times out.

B1 protects these properties:

- only a configured agent can discover or invoke the first-party browser
  surface;
- an active session remains on the configured local target and profile;
- one managed profile has one writer;
- the model cannot provide profile paths, credentials, endpoints, process
  arguments, hidden policy names, or arbitrary driver operations;
- every element action uses a reference from a fresh observation of the same
  session, tab, and generation;
- every call performs at most one browser action;
- the broker, not the model or driver, assigns the action effect class;
- dry-run policy denies external commits even after approval;
- approval is bound to the exact prepared action and current authority;
- an accepted action is never automatically replayed;
- a lost accepted outcome is recovered from durable state or reported as
  `unknown`;
- close, expiry, and restart do not silently create a replacement session; and
- model-visible results, logs, events, and persistent records omit secrets and
  raw protected paths.

B1 does not promise exactly-once effects from a website. It promises a durable
MintClaw acceptance boundary and no blind replay after that boundary.

## Component And Ownership Boundaries

### Browser broker

A new browser package owns:

- validated target and profile projection;
- actor, agent, target, profile, origin, and action-policy intersection;
- opaque identifiers and persistent session metadata;
- profile-lease acquisition through the B0 lease primitive;
- tab and snapshot generations;
- effect classification and prepared-action hashing;
- approval revalidation;
- invocation acceptance and terminal recovery; and
- session expiry, close, restart recovery, and bounded audit events.

The broker does not implement a browser engine, parse arbitrary MCP schemas,
forward arbitrary tools, store credential values, or expose driver endpoints.

### Gateway worker

The B1 worker runs in the gateway process and owns one driver instance for one
session. Its internal typed interface is limited to:

```text
Open(profile lease, limits) -> worker session
Status(worker session) -> bounded status
Observe(worker session, tab) -> typed observation
Prepare(worker session, action) -> resolved action semantics
Execute(worker session, accepted invocation) -> terminal result
Close(worker session) -> close outcome
```

`Prepare` may inspect a fresh element and its form relationship, but it cannot
grant authority or lower risk. `Execute` receives only a broker-accepted typed
invocation. The worker never receives a model-selected executable, MCP server,
profile path, credential, or transport endpoint.

### Playwright driver adapter

The first adapter may call the configured Playwright MCP server behind an
explicit mapping from each admitted action and observation to the pinned
driver schema. Startup fails unavailable if the required catalog or schema is
incompatible. Unknown driver tools are ignored and are never forwarded.

The browser worker is the sole owner of that MCP client. It starts the client
on session open, acquires the B0 lock before the driver process starts, and
closes both on session close. The referenced server is not eagerly started by
the generic MCP manager and is not registered as raw tools for any agent. If
the implementation cannot make driver lifetime session-scoped without a
second process holding or bypassing the profile lock, the driver PR stops.

The adapter is not authoritative for target selection, profile selection,
origin policy, effect class, approval, retry, session recovery, or retention.
The B0 no-replay policy remains active below the adapter.

## Configuration And Deny-By-Default Rollout

The admitted configuration has this logical shape. Exact Go field placement
may follow existing `tools` configuration conventions, but the authority and
defaults below are fixed:

```json
{
  "tools": {
    "browser": {
      "enabled": false,
      "agents": ["browser"],
      "targets": {
        "gateway": {
          "enabled": false,
          "driver": "playwright_mcp",
          "driver_server": "playwright",
          "profiles": {
            "managed": {
              "enabled": false,
              "mode": "managed",
              "dry_run": true,
              "allowed_origins": ["https://example.invalid"]
            }
          }
        }
      }
    }
  }
}
```

Rules:

- omitted `tools.browser`, target, or profile enablement grants no authority;
- aliases are bounded opaque names and never expand to model-visible paths;
- `driver_server` must resolve to a configured local stdio MCP server template
  with `session_loss_replay=never` and an exclusive lock; the browser worker,
  not the generic MCP manager, owns its process and client lifetime;
- only `mode=managed` is valid in B1;
- allowed origins are normalized `http` or `https` origins without path,
  query, fragment, user information, or wildcard public suffix;
- the exact empty `about:blank` document is the sole runtime origin exception:
  it is not configurable, contains no title, snapshot content, references, or
  dialog, and its snapshot can authorize only navigation to an allowed HTTP(S)
  origin;
- exact allowed origins are also applied at the driver request boundary;
  redirects and every resulting document origin are rechecked before further
  page interaction;
- loopback, link-local, private, multicast, unspecified, and cloud-metadata
  destinations are denied by default, including DNS resolutions to them;
- deterministic tests may use an explicit test-only loopback policy that
  cannot be loaded by a production configuration;
- `dry_run=true` is the initial deployment default and cannot be weakened by
  a tool argument, page content, or approval; and
- configuration reload closes or expires sessions whose authority changed
  before the new policy becomes effective.

Rollback disables the browser surface, restores the specialist's raw
Playwright MCP allowlist, restarts the gateway, and leaves the existing profile
unchanged.

## Model-Visible Tool Contracts

All outputs are bounded JSON. Opaque IDs are non-secret, URL-safe strings of at
most 128 bytes. Unknown fields and unsupported operations fail validation.

### `browser_targets`

The tool accepts no model-controlled placement or filter. It returns only
targets and profiles effective for the authenticated agent:

```json
{
  "targets": [{
    "target": "gateway",
    "status": "ready",
    "profiles": [{"profile": "managed", "status": "ready", "dry_run": true}],
    "actions": ["navigate", "click", "fill", "select", "press", "scroll", "dialog"],
    "limits": {"sessions": 1, "tabs": 4, "snapshot_bytes": 262144}
  }]
}
```

Status is one of `ready`, `busy`, `degraded`, or `unavailable`. Reasons use a
small safe vocabulary such as `disabled`, `not_granted`, `profile_busy`,
`driver_unavailable`, `driver_incompatible`, or `recovery_required`.
Discovery does not start a browser, acquire or renew a lease, acknowledge an
invocation, or change retained state.

### `browser_session`

`open` accepts only `target` and `profile` aliases. It creates at most one
session and returns its ID, state, controller, expiry, and initial tab ID. A
new driver may start on an empty `about:blank` tab. The specialist observes
that tab before the first navigation so the navigation remains bound to fresh
snapshot authority.

`status` accepts `browser_session_id` and returns bounded session state and
tab summaries. It may reconcile already persisted worker state, but it does
not retry an action or silently open another browser.

`close` accepts `browser_session_id`. It is idempotent. It stops new actions,
waits a bounded interval for an in-flight call, closes the driver, releases the
profile lease, and records `closed`, `expired`, or `lost`. If an accepted
invocation cannot be proven terminal, that invocation becomes `unknown`.

### `browser_observe`

The tool accepts `browser_session_id` and an optional `tab_id`. It returns:

- the resolved session and tab IDs;
- the current normalized HTTP(S) URL and origin, or the exact empty
  `about:blank` bootstrap document;
- a new opaque snapshot ID and monotonically increasing generation;
- a bounded accessibility tree with scoped element references;
- bounded tab summaries and pending dialog metadata; and
- truncation and limit metadata.

When a syntactically valid driver snapshot exceeds the configured byte or
reference limit, the adapter returns a deterministic line-bounded prefix and
sets `truncated` to `true`. Only references retained in that prefix receive
broker authority; omitted references cannot be resolved or used by an action.
Malformed reference syntax still fails closed instead of being truncated.

B1 does not return screenshot bytes or screenshot artifact references.
Screenshots belong to B2. It also omits page HTML, cookies, storage state,
response bodies, arbitrary console output, and raw driver objects.

### `browser_act`

The tool accepts exactly one action:

```json
{
  "browser_session_id": "opaque",
  "tab_id": "opaque",
  "snapshot_id": "opaque",
  "snapshot_generation": 7,
  "action": {
    "kind": "click",
    "ref": "opaque"
  }
}
```

`navigate` uses a normalized URL instead of `ref`. `fill` contains one bounded
`value`; `select` contains one bounded option value; `press` contains one
admitted key; `scroll` contains a bounded direction and amount; and `dialog`
contains `accept` or `dismiss` plus an optional bounded prompt value. No call
contains multiple actions, a driver tool name, CSS/XPath selector, JavaScript,
coordinate, file path, credential, or retry flag.

An observation of the exact empty `about:blank` document can authorize only a
`navigate` action. Any title, snapshot content, reference, dialog, alternate
`about:` URL, or non-navigation action fails closed as driver-incompatible or
denied. The destination still passes the configured-origin and network checks
at preparation and immediately before dispatch.

The result contains the invocation ID, derived effect class, terminal state,
and a fresh observation when one can be obtained safely. A prepared action
that needs approval uses MintClaw's durable tool suspension path; the model
does not receive or submit an approval token.

## Snapshot And Reference Freshness

Each observation creates a new generation for one tab. A reference binds:

- actor and agent;
- browser session and target;
- profile and controller generation;
- tab identity;
- snapshot ID and generation;
- observed document origin; and
- a driver-local element identity usable only by that worker session.

All previous references become stale after navigation, reload, history change,
tab replacement, dialog-driven document change, material DOM change reported
by the driver, action execution, session recovery, driver restart, policy
reload, or close. B1 does not attempt heuristic reference rebinding.

The broker validates freshness before preparation and again immediately before
acceptance. A stale reference fails before dispatch and instructs the
specialist to observe again.

## Effect Classification And Runtime Policy

The broker assigns one of these effect classes:

- `read`: observe and bounded scroll;
- `navigation`: explicit navigation or a resolved ordinary link that cannot
  submit state;
- `local_edit`: fill, select, non-submit key input, and dialog dismissal;
- `external_commit`: a resolved submit control, form submission, confirmation,
  dialog acceptance, or an action known by configured site policy to publish,
  send, delete, purchase, book, pay, or confirm; or
- `unknown`: any action whose resolved semantics do not prove a lower class.

`unknown` receives the same approval and dry-run treatment as
`external_commit`. MCP annotations, page labels, model arguments, and skill
text cannot lower an effect class. Privileged operations are not admitted and
fail before preparation.

Navigation and local edits must remain within the configured origin policy.
Cross-origin redirects stop before further action until the destination is
independently authorized. Production B1 starts with `dry_run=true`, which
denies dispatch of `external_commit` and `unknown` actions even when a human
approves them.

## Prepare, Approval, Acceptance, And Recovery

### Prepared action

Preparation resolves the fresh element, form relationship, current origin,
destination origin when knowable, normalized typed action, effect class, and
current policy revision. It persists a canonical action hash bound to:

- actor, agent, target, profile, and browser session;
- controller generation, tab, snapshot ID, and snapshot generation;
- normalized action and resolved element semantics;
- current and destination origins;
- effect class and dry-run state;
- target, profile, catalog, and policy revisions; and
- creation time and expiry.

The existing durable approval mechanism stores and later consumes the trusted
prepared binding. Approval arguments expose only a bounded human-readable
preview and the opaque prepared-action identity. Resume never rebuilds or
refreshes expired authority.

### Commit and acceptance boundary

Immediately before dispatch, the broker revalidates every binding and obtains
the profile/session execution lease. It then writes the invocation record as
`accepted` durably before asking the worker to execute. The accepted record is
the no-replay boundary. A crash after that write may conservatively produce
`unknown` even if the driver never reached the page.

The worker executes an accepted invocation at most once in its lifetime. A
stored terminal result is returned idempotently for the same invocation ID.
No gateway restart, tool retry, approval resume, or MCP reconnect redispatches
an accepted invocation.

### State transitions

```text
prepared -> denied | expired | canceled-before-acceptance
prepared -> accepted -> succeeded | failed | unknown
accepted + stored terminal result -> return stored result
accepted + lost worker without terminal proof -> unknown
```

An ordinary validation, policy, or driver-readiness failure before acceptance
is definitely unaccepted and may be retried only through a new model call.
Cancellation before acceptance records `canceled`. Cancellation, timeout, or
disconnect after acceptance requests bounded worker cancellation but resolves
to a stored terminal result or `unknown`; it never proves rollback.

## Persistence And Lifecycle

Broker state is stored under the gateway's existing owner-only state root with
atomic replacement and bounded records. Model-visible results never contain
the backing paths. The persistent model separates:

- profile configuration and lease;
- browser session and controller generation;
- tab and latest snapshot generation;
- prepared action and expiry;
- invocation acceptance and terminal result; and
- browser-specialist job checkpoint.

A job checkpoint may reference a session but does not renew it and cannot
prove an external effect.

B1 uses these default ceilings, with configuration allowed only to reduce them
unless a later admission changes the hard bounds:

| Resource | B1 hard bound |
| --- | --- |
| Concurrent gateway sessions | 1 |
| Concurrent sessions per managed profile | 1 |
| Tabs per session | 4 |
| Session lifetime | 60 minutes |
| Idle timeout | 10 minutes |
| Prepared-action lifetime | 5 minutes |
| One driver action timeout | 60 seconds |
| Accessibility snapshot | 256 KiB and 500 refs |
| URL | 2,048 bytes |
| One text input | 16 KiB |
| Model-visible tool result | 320 KiB |
| Terminal invocation retention | 7 days |

On clean close or expiry, the broker prevents new actions, resolves in-flight
accepted work, closes the worker, releases the B0 profile lease, invalidates
tabs and snapshots, and removes ephemeral observation data. Terminal
invocation records remain for their retention window.

On gateway restart, sessions are not silently recreated. A persisted open
session becomes `recovering`; the broker may attach only if the same worker
identity and durable invocation ledger prove continuity. The initial in-process
worker cannot survive gateway exit, so B1 marks the session `lost`, marks any
unterminated accepted invocation `unknown`, and releases or reacquires the
profile lease only for a new explicit `open`.

## Diagnostics, Events, And Redaction

`browser_targets` and `browser_session status` expose bounded effective state.
Operator events distinguish discovery, open, ready, prepared, approval wait,
accepted, succeeded, failed, unknown, closing, closed, expired, and lost.

Model results, events, logs, traces, and persistent records exclude:

- credentials, cookies, storage state, form secrets, and browser history;
- raw profile, lock, executable, output, and state paths;
- MCP command arguments, environment, transport endpoints, and driver payloads;
- full page text beyond the bounded filtered accessibility observation; and
- hidden domain policy or denial details that would aid probing.

URLs are normalized and filtered. Sensitive query and fragment data are
omitted from ordinary events. Driver errors map to a bounded safe category;
full diagnostics remain available only through existing protected operator
traces with configured redaction.

## Validation And Failure Injection

### Contract and policy tests

- disabled and ungranted browser configuration registers no browser tools;
- only the browser specialist receives the four admitted tools;
- target/profile projection omits raw configuration and secrets;
- invalid aliases, origins, limits, and incompatible driver configuration fail
  closed;
- cross-origin redirects, private-network destinations, and DNS answers that
  resolve outside the admitted network policy are denied;
- unknown fields, actions, keys, and oversized inputs fail validation; and
- dry-run denial cannot be changed by tool arguments, page text, or approval.

### Lifecycle and concurrency tests

- a second session and a second profile opener are rejected busy;
- close is idempotent and releases the profile lease;
- idle and absolute expiry invalidate tabs, snapshots, and prepared actions;
- restart marks an in-process session lost and accepted unterminated actions
  unknown;
- configuration changes invalidate affected sessions; and
- concurrent close, action, and expiry have one authoritative outcome.

### Observation and action tests

- a deterministic real browser fixture proves open, navigate, observe, fill,
  click, and close through the first-party tools;
- element references fail across snapshot generations and material page
  changes;
- every action dispatch produces a new generation;
- effect classes come from resolved runtime semantics, not model claims;
- an unknown submit-like control requires approval and is denied in dry-run;
- changed origin, element, action, policy, profile, or session invalidates a
  prepared approval; and
- page prompt injection cannot broaden target, origin, profile, tools, or
  action policy.

### Acceptance and recovery tests

- failure before the durable accepted record is definitely unaccepted;
- disconnect before acceptance permits a new explicit attempt;
- disconnect after acceptance never calls the driver a second time;
- a stored terminal result is recovered idempotently;
- an accepted invocation without terminal proof becomes `unknown`;
- cancellation and timeout races after acceptance do not report a safe retry;
  and
- B0 MCP session loss below the adapter remains one call with no replay.

### Merged-main deployment evidence

- deploy the merged core and version-controlled workspace with backup and
  rollback instructions;
- explicitly enable only `browser`, `gateway`, and `managed`, initially in
  dry-run mode;
- prove the raw Playwright MCP tools are absent from the migrated specialist;
- run one read-only workflow and one approval-stopped listing dry run through
  the model-visible first-party tools;
- restart during a disposable fixture session and verify lost-session and
  lease behavior;
- inspect bounded logs, traces, state, and diagnostics for secret or endpoint
  leakage; and
- record residual limitations before deciding whether B2 is admissible.

## Dependency-Ordered Delivery

Each implementation item is a focused PR based on the latest merged `main`.
A dependent item starts only after its predecessor merges.

1. **Admission contract:** this architecture-only PR.
2. **Broker foundation:** configuration, entities, persistence interfaces,
   state machines, and an in-memory fake worker without model-visible tools.
3. **Local driver:** explicit Playwright adapter mappings and deterministic
   real-browser open, observe, one-action, and close coverage.
4. **Lifecycle and recovery:** B0 lease integration, TTL, heartbeat, cleanup,
   restart reconciliation, accepted invocation ledger, terminal recovery, and
   explicit `unknown`.
5. **Freshness and approval:** snapshot generations, scoped references,
   runtime effect classification, preparation, durable approval binding,
   revalidation, and dry-run enforcement.
6. **First-party tools:** register the four tools only for explicitly granted
   agents and remove raw Playwright MCP access from the migrated specialist.
7. **Workspace rollout and deployment evidence:** enable the one admitted
   target/profile, migrate one workflow, deploy merged `main`, run the required
   smoke and failure-injection checks, and record residual limitations.

No PR may absorb B2 artifact or handoff behavior, B3 companion placement, B4
identity work, B5 providers, or B6 computer control merely because the broker
creates a future seam for them.

## Exact Completion Gates

### Gate 1: authority and discovery

- browser authority is disabled by default;
- only explicitly granted agents see the first-party tools;
- discovery returns only effective local target/profile capability; and
- no model-visible value exposes paths, endpoints, credentials, or hidden
  policy.

### Gate 2: explicit lifetimes and freshness

- profile, session, job, tab, snapshot, prepared action, and invocation are
  distinct runtime and persistent entities;
- stale references fail before dispatch; and
- close, expiry, restart, and policy change invalidate dependent authority.

### Gate 3: one typed action and no bypass

- one `browser_act` call performs at most one admitted action;
- the driver adapter maps only explicit typed operations;
- arbitrary MCP, JavaScript, CDP, coordinates, and filesystem arguments are
  unavailable; and
- the migrated specialist cannot simultaneously call raw Playwright MCP tools.

### Gate 4: runtime effects and approval

- the broker derives effects from trusted action kind and fresh resolved page
  semantics;
- unknown and external-commit actions require bound approval;
- every binding is revalidated immediately before acceptance; and
- dry-run denies commit regardless of approval, model, page, or skill text.

### Gate 5: durable acceptance and recovery

- accepted state is durable before driver dispatch;
- an accepted invocation executes at most once;
- terminal results recover idempotently;
- lost accepted outcomes are explicitly `unknown`; and
- cancellation, timeout, disconnect, and restart never imply safe replay.

### Gate 6: local real-browser vertical slice

- the deterministic fixture completes through the same four tools exposed to
  the model;
- the managed profile lease rejects a second opener;
- bounded observations and diagnostics remain usable; and
- logs, events, state, and results pass redaction checks.

### Gate 7: merged-main deployment

- every focused PR has required tests, CI, review, and merge evidence;
- merged `main` is deployed with rollback available;
- only the admitted target/profile is enabled;
- read-only and approval-stopped workflows pass through the specialist;
- restart and post-acceptance disconnect evidence proves no blind replay; and
- residual limitations are recorded before B1 is declared complete.

## Mandatory Stop Conditions

Stop the affected implementation and return to architecture if:

- the driver adapter cannot expose the admitted typed operations without
  forwarding arbitrary MCP schemas;
- the broker cannot durably record acceptance before dispatch;
- a worker or driver can automatically repeat an accepted invocation;
- resolved page semantics cannot distinguish a safe action from unknown and
  unknown cannot be handled as external commit;
- approval must trust a model-supplied effect, element description, target, or
  origin;
- profile locking requires migrating or rewriting the deployed profile;
- restart recovery requires secrets or protected paths in model-visible state;
- the vertical slice requires screenshots, file transfer, human takeover,
  attached-user identity, cloud or companion routing, coordinates, raw
  evaluation, or generic computer authority; or
- validation requires a production external mutation.

## Non-Goals

- screenshots, video, traces, HAR, uploads, downloads, or binary artifacts;
- `browser_extract` or schema-driven bulk extraction;
- human takeover, headed live view, or remote view tokens;
- companion, cloud, automatic fallback, or workspace routing;
- attached-user, cloud, ephemeral, imported, or multiple managed profiles;
- credential filling, credential aliases, cookies, or storage-state transfer;
- raw Playwright MCP tools in the migrated specialist;
- arbitrary MCP proxying or model-selected driver operations;
- CSS, XPath, coordinates, JavaScript, CDP, extensions, network interception,
  clipboard, desktop, camera, microphone, or device control;
- exact-once claims for third-party website effects;
- automatic retry or heuristic reconciliation of accepted actions;
- production external commits as validation; and
- treating B1 admission as authorization for B2 or later milestones.
