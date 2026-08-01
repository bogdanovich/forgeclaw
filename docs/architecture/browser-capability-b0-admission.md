# Browser Capability B0 Admission

## Status And Decision

Browser milestone B0, **Stabilize the Current Browser Specialist**, is admitted
as the dependency-ordered sequence in this document.

The admitted work preserves the current dedicated browser specialist,
Playwright MCP driver, installed Chrome browser, persistent automation profile,
job checkpoints, and main-agent delegation model. It reduces immediate risks
without introducing the first-party browser broker or model-facing
`browser.*` tools planned for B1.

The implementation may add:

- an explicit per-server MCP session-loss replay policy;
- an explicit uncertain result when an MCP call may have reached the server
  but cannot be proven terminal;
- a configured cross-process exclusive lease for one stdio MCP server;
- MediaStore ownership and TTL cleanup for MintClaw-created large MCP text
  artifacts;
- a narrow MCP-server allowlist for the deployed browser specialist;
- operator diagnostics based on existing MCP probe, configuration, lock, and
  runtime-event surfaces; and
- merged-main deployment evidence for one read-only workflow and one
  approval-stopped listing workflow.

This admission does not add companion placement, named browser sessions,
browser profile selection, browser credentials, runtime domain policy,
first-party action classification, human takeover, cloud providers, raw CDP,
desktop control, or remote workspace routing.

Any implementation finding that requires a generic browser proxy, arbitrary
MCP forwarding through nodes, a new model-visible browser tool, a new
credential store, parsing Playwright-specific tool names in core policy,
automatic migration of the active profile, or cleanup of driver-created files
whose ownership cannot be proven stops the affected PR and returns to B1 or a
fresh admission decision.

## Concrete Operator Use Case

The admitted deployment is the existing main-profile browser specialist:

1. the main agent delegates a serious browser task to agent `browser`;
2. the specialist uses the configured `playwright` MCP server and the existing
   persistent Chrome profile;
3. the specialist maintains a job checkpoint and runs at most one independent
   top-level turn;
4. read-only navigation and observation continue to work;
5. a listing workflow may fill local page state but stops at its existing
   approval boundary before external publication; and
6. an operator can diagnose whether the MCP server, browser executable,
   configured profile lease, and required Playwright tool catalog are usable.

B0 does not claim that skill checkpoints prove browser side effects. Its
guarantee is narrower: MintClaw itself does not blindly repeat a Playwright MCP
call after losing the server session, and it does not start a second configured
MCP process while the same persistent-profile lease is held.

## Security Truth And Threat Model

The current Playwright MCP server controls an authenticated browser profile and
can publish listings, send messages, delete content, or perform other external
mutations. An MCP transport error does not prove whether the website action
occurred.

The protected properties are:

- absence of MintClaw-triggered duplicate browser effects after session loss;
- exclusive use of the configured persistent profile across MintClaw
  processes that share its lock file;
- continued availability of the MCP server after safe reconnection;
- explicit visibility of an uncertain call outcome to the browser specialist;
- denial of unrelated MCP servers to the browser specialist;
- bounded ownership and cleanup of MintClaw-created MCP artifacts;
- preservation of existing browser profile data and job checkpoints;
- logs and diagnostics that do not expose credentials, cookies, page content,
  tool arguments, or raw browser state.

The design assumes:

- a tool call may fail before dispatch, during transport, after server
  acceptance, or after the website effect;
- the MCP SDK may wrap session loss as a missing-session, closed-client, pipe,
  EOF, or related transport error;
- Playwright MCP tool names and annotations may change across package versions;
- MCP tool annotations and model descriptions are untrusted hints;
- another MintClaw gateway, CLI probe, or recovery process may start with the
  same configuration;
- a process may crash while holding a profile lease;
- a stale lock file may remain after process exit;
- browser and MCP output may contain personal data or credentials;
- the browser specialist may receive hostile page content encouraging it to
  retry, broaden tools, or ignore an approval boundary.

B0 does not guarantee exactly-once behavior from an external website and does
not prevent a human or a later independently authorized model decision from
issuing a new action. It guarantees that the MCP manager does not automatically
replay the uncertain call.

## Sources Of Truth

| Decision | Authoritative source |
| --- | --- |
| Browser specialist identity and workspace | Existing agent registry and workspace configuration |
| MCP servers usable by the specialist | `AGENT.md` `mcpServers` policy intersected with enabled global MCP configuration |
| Playwright package, browser executable, profile, and output arguments | Operator-owned `tools.mcp.servers.playwright` configuration |
| Session-loss replay behavior | Validated MCP server configuration and MCP manager implementation |
| Exclusive profile/process lease | OS lock on the configured absolute lock file |
| MCP process connection and discovered tools | Live MCP manager connection |
| Job workflow and pending approval | Existing browser specialist checkpoint |
| Human approval | Existing trusted main-agent approval and interaction path |
| MintClaw-created artifact lifetime | Persistent MediaStore index and configured cleanup policy |
| External website effect after uncertainty | Fresh browser observation or human verification, never the transport error |

A PID written into a lock file, a job owner marker, an MCP session identifier,
an error string, or a model statement is diagnostic data, not authority.

## Fixed Scope Decisions

### Server-wide session-loss policy

B0 does not classify individual Playwright MCP tools by name. Tool naming is an
external driver contract and cannot safely identify every mutation or future
tool.

The MCP server configuration gains:

```json
{
  "session_loss_replay": "once"
}
```

Supported values are:

- `once`: preserve the existing compatibility behavior by reconnecting and
  invoking the same call once;
- `never`: reconnect the server for future calls but never invoke the failed
  call on the replacement session.

An omitted value resolves to `once` for backward compatibility. Unknown values
fail configuration validation. The deployed `playwright` server explicitly
uses `never`.

The policy is server-wide because the current runtime cannot authoritatively
distinguish read-only Playwright operations from browser mutations. B0 accepts
that an interrupted snapshot may require a new model-issued observation rather
than risking duplicate external action.

### Explicit uncertain outcome

When a configured `never` server loses its session during `tools/call`:

1. the original call is never issued again;
2. the manager attempts one serialized reconnect so later calls can use a
   healthy server;
3. the current call returns a typed `CallOutcomeUncertainError` whether the
   reconnect succeeds or fails;
4. the error records safe server/tool identity and reconnect availability but
   excludes arguments, page content, credentials, endpoints, and hidden
   configuration;
5. the MCP tool wrapper returns an error result whose model-safe text says that
   the outcome is uncertain and must not be automatically repeated;
6. the runtime event records `outcome=uncertain` separately from ordinary
   failure.

Reconnect success means only that future calls may proceed. It does not change
the uncertain outcome of the original call.

The browser specialist workflow must treat uncertainty as a reconciliation
boundary: save the job state, observe current external state with a new call
when safe, and never repeat a publish/send/delete/purchase action merely
because the previous tool result was lost.

### Existing `once` behavior

Servers using `once` retain the current one-reconnect/one-replay behavior.
Concurrent calls encountering the same stale connection continue to serialize
replacement through the existing reconnect mutex and reuse the replacement
connection.

B0 does not change generic MCP retry policy globally, infer safety from MCP
annotations, or add more than one replay.

### Exclusive stdio server lease

The MCP server configuration may declare:

```json
{
  "exclusive_lock_file": "/absolute/operator-owned/path/playwright.lock"
}
```

Rules:

- the field is optional and applies only to stdio servers;
- a configured path must be absolute, clean, bounded in length, and have an
  existing parent directory;
- MintClaw opens the file with owner-only permissions and acquires a
  non-blocking OS-exclusive lock before starting the subprocess;
- lock contention fails server startup with a stable busy classification;
- the lock is held across MCP session reconnects;
- manager close releases the lock only after in-flight calls finish and the
  server session is closed;
- failed initial connection, canceled startup, and manager-close races release
  the lock;
- a retained lock-file inode or PID text is not treated as proof of a live
  owner; only the OS lock determines ownership;
- MintClaw never deletes a lock file while it may be locked;
- HTTP/SSE servers reject this field because it cannot protect a remote
  browser profile.

The deployed lock file is located beside, not inside, the active Chrome
user-data directory. No profile contents are migrated or rewritten.

The lease prevents cooperating MintClaw processes using the same configured
lock. It does not claim to prevent Chrome, Playwright, or a manually launched
process that ignores the lease from opening the profile. Native browser
profile locking remains an independent final defense.

### Browser specialist MCP boundary

The deployed browser `AGENT.md` declares an explicit MCP server allowlist:

```yaml
mcpServers:
  allow:
    - playwright
    - obsidian_personal
```

`playwright` is required for browser execution. `obsidian_personal` remains
temporarily admitted for the existing Vipassana workflow, which reads exact
operator-owned application notes before filling a form.

Other configured MCP servers, including research, media, drawing, inventory,
and alternate browser providers, are not registered for the browser
specialist. The existing explicit deny of nested delegation remains.

B0 does not introduce a complete built-in tool allowlist because current
checkpoint and workflow behavior still needs bounded workspace file tools.
Removing shell or broad filesystem authority requires an inventory of actual
browser workflows and belongs to the B1 specialist contract unless a separate
narrow workspace change proves it without regression.

### MintClaw-created MCP artifact cleanup

Large MCP text results currently create files under the agent workspace
`.artifacts/mcp`. B0 registers each successfully written file in the existing
persistent MediaStore with:

- content type `text/plain`;
- source server and tool identifiers;
- delete-on-cleanup ownership;
- a bounded per-call scope;
- the existing model-visible local artifact tag.

The existing `tools.media_cleanup` TTL and interval become authoritative for
these files. Registration failure deletes the newly created file and returns a
bounded omission result rather than leaving an unmanaged artifact.

Inline MCP image, audio, and binary resource content already uses MediaStore
and keeps that lifecycle.

B0 does not delete:

- active or retained browser job checkpoints;
- browser profile contents;
- user-provided uploads;
- Playwright driver output files that MintClaw did not create or register;
- artifacts with forget-only policy;
- files outside the exact registered path.

Playwright `--output-dir` cleanup is deferred because the current driver does
not provide enough ownership metadata to distinguish active, retained, and
discardable files. Silently sweeping that directory would violate the B0 stop
condition. B1/B2 must route driver output through explicit session artifacts.

### Operator diagnostics

B0 uses and extends bounded operator surfaces rather than creating
model-visible `browser_targets` prematurely.

Required operator evidence combines:

- `mintclaw mcp show playwright` for sanitized configuration state;
- `mintclaw mcp test playwright` for process startup, protocol handshake, and
  discovered tool catalog;
- explicit display of effective `session_loss_replay` and whether an exclusive
  lock is configured, without printing the lock path;
- a busy result when another process holds the lease;
- runtime events for connecting, connected, failed, call end, uncertain
  outcome, and reconnect result;
- an operator-side check of the configured Chrome executable and writable
  parent directories during deployment;
- one safe browser navigation/observation smoke through the browser
  specialist.

B0 does not parse Playwright command arguments in generic core doctor code,
launch a browser from passive `doctor`, expose raw executable/profile/output
paths to the model, or use diagnostics to acknowledge or retry workflow state.

## Configuration And Migration

Fresh and existing configurations preserve `session_loss_replay=once` when the
field is omitted. Existing stdio servers have no exclusive lease until an
operator configures one.

The B0 code may land before the deployed configuration changes:

- missing new fields preserve current behavior;
- invalid values fail validation before the affected server starts;
- adding `exclusive_lock_file` does not move or rewrite the profile;
- changing the Playwright server to `never` takes effect after the normal safe
  gateway restart;
- adding the browser `mcpServers` policy changes registration only for that
  agent;
- rollback removes the two server fields and workspace allowlist, then
  restarts the gateway using the previous binary.

Configuration tools that clone or rewrite MCP server entries must preserve the
new fields. The initial CLI need not add dedicated flags; advanced operators
may edit the raw config, while `mcp show` renders the effective safe state.

Example deployed shape:

```json
{
  "playwright": {
    "enabled": true,
    "deferred": false,
    "type": "stdio",
    "command": "npx",
    "args": [
      "-y",
      "@playwright/mcp@0.0.78",
      "--browser=chrome",
      "--executable-path=/usr/bin/google-chrome-stable",
      "--user-data-dir=/home/server/.mintclaw/main/browser-profile/mintclaw-browser-profile",
      "--output-dir=/home/server/.mintclaw/main/browser-output"
    ],
    "session_loss_replay": "never",
    "exclusive_lock_file": "/home/server/.mintclaw/main/browser-profile/playwright.lock"
  }
}
```

The absolute paths are operator configuration and are not copied into
model-visible discovery, tool results, or passive events.

## Invocation And Failure Semantics

The B0 call state is intentionally small:

```text
call started
    |
    +-- result received --------------------------> terminal
    |
    +-- ordinary non-session error --------------> failed
    |
    +-- session-loss error
            |
            +-- policy=once
            |       |
            |       +-- reconnect + one call ----> terminal or failed
            |
            +-- policy=never
                    |
                    +-- reconnect for future calls
                    +-- current call ------------> uncertain
```

The uncertain result is terminal for the MintClaw tool invocation. It is not a
background task, pending retry, or proof of failure. A later observation is a
new invocation with a new model decision.

Cancellation before or during the MCP call retains existing context semantics.
B0 does not claim that canceling a dispatched external browser action rolls it
back. If cancellation and session loss race after dispatch under `never`, the
reported action outcome remains uncertain.

## Runtime Events And Redaction

MCP call-end events may add a bounded outcome value:

- `succeeded`;
- `failed`;
- `uncertain`.

Uncertain event data may include:

- server name;
- tool name;
- duration;
- whether reconnect restored server availability;
- safe error category.

It excludes:

- tool arguments;
- URLs or page text derived from arguments or results;
- form values and credentials;
- cookies and browser storage;
- command environment values;
- raw profile, output, or lock paths;
- full transport errors when they contain endpoints or secrets.

Existing debug logs that include only server name, tool name, counts, and
bounded error text are reviewed in the implementation PR. Any path or endpoint
added by B0 is redacted or omitted from normal events.

## Validation Requirements

### Replay-policy tests

Tests use scripted MCP transports and deterministic barriers:

- default/`once` reconnects and invokes exactly once on the fresh session;
- `never` invokes only the stale session, reconnects once, and returns typed
  uncertainty;
- `never` remains uncertain when reconnect fails;
- an ordinary tool error does not reconnect or become uncertain;
- concurrent callers share one replacement session without replaying
  `never` calls;
- manager close racing reconnect does not issue a fresh call;
- config parsing rejects unknown policy values.

### Lease tests

Tests run real OS locks where supported:

- first manager acquires the configured lease and starts the fake stdio server;
- second manager fails busy without starting a subprocess;
- reconnect preserves the lease;
- close releases it for a later manager;
- initial-connect failure and canceled startup release it;
- a stale lock file without an OS lock does not block startup;
- relative path, missing parent, HTTP transport, and overlong path fail
  validation;
- Windows and Unix implementations follow the same non-blocking contract.

### Artifact tests

- a large MCP text result is written, registered, and returned with its local
  artifact tag;
- MediaStore expiry removes the exact file and index entry;
- registration failure removes the just-created file;
- an active MediaStore ref is not removed before TTL;
- small inline text and existing binary media behavior are unchanged;
- no cleanup follows symlinks or removes neighboring files.

### Workspace and deployment tests

- the browser agent registers only `playwright` and `obsidian_personal` MCP
  servers;
- main and other agents retain their independently configured MCP policy;
- `mcp show` omits lock path and renders effective replay policy;
- `mcp test playwright` succeeds when idle and reports busy while the deployed
  gateway owns the profile lease;
- one browser observation workflow completes;
- one listing/form fixture reaches `awaiting_approval` without external commit;
- a deterministic injected session-loss drill proves no second tool call;
- restart preserves the browser profile and releases/reacquires the lease;
- logs and events contain no browser credentials, form values, cookies, page
  content, or raw protected paths.

Production mutation is not required for B0 evidence.

## Dependency-Ordered Delivery

Each item is a separate merge unit based on the latest merged `main`. A
dependent item begins only after its predecessor merges.

1. **Admission contract:** this architecture-only PR.
2. **No-blind-replay contract:** add `session_loss_replay`, typed uncertainty,
   redacted outcome events, CLI safe projection, and deterministic manager/tool
   tests.
3. **Exclusive stdio lease:** add `exclusive_lock_file`, cross-platform
   non-blocking locking, reconnect ownership, CLI safe projection, and
   lifecycle tests.
4. **MCP artifact ownership:** register MintClaw-created large text artifacts
   with persistent MediaStore and prove bounded cleanup.
5. **Workspace policy rollout:** update the version-controlled browser
   specialist allowlist and Playwright configuration in the MintClaw
   workspaces repository after the required core binary is merged.
6. **Merged-main deployment evidence:** deploy the merged binary and workspace
   configuration, run the safe operational tests, record results and residual
   limitations, and decide whether every B0 gate is complete.

The no-replay and exclusive-lease changes remain separate because they alter
different invariants and failure states in the shared MCP manager. Artifact
cleanup remains separate because it changes MediaStore ownership rather than
MCP transport behavior.

No implementation PR may absorb B1 session, profile, action, approval, or
model-tool contracts merely because the browser deployment motivates it.

## Exact Definition Of Done

### Gate 1: backward-compatible configuration

- existing servers with omitted fields retain one replay and no exclusive
  lease;
- invalid values fail before server startup;
- config cloning, CLI edit, and serialization preserve the new fields;
- no fresh installation gains browser authority.

### Gate 2: no runtime blind replay

- deployed Playwright uses `session_loss_replay=never`;
- a session-loss call is not issued on the replacement session;
- the current tool result is explicitly uncertain;
- later calls may use the reconnected server;
- the browser workflow instructs reconciliation rather than automatic repeat.

### Gate 3: exclusive persistent-profile lease

- the deployed Playwright process acquires one non-blocking OS lease;
- a second cooperating MintClaw process cannot start it;
- reconnect keeps the lease and close releases it;
- profile contents are neither moved nor rewritten.

### Gate 4: specialist MCP boundary

- browser `AGENT.md` explicitly allows only `playwright` and
  `obsidian_personal` MCP servers;
- unrelated MCP tools are not registered for that agent;
- existing browser and Vipassana flows retain required sources;
- nested browser delegation remains denied.

### Gate 5: bounded artifact ownership

- every MintClaw-created large MCP text artifact in the normal gateway path is
  registered with persistent MediaStore;
- configured TTL cleanup deletes only the exact managed file;
- registration failure leaves no unmanaged new file;
- external Playwright outputs remain untouched and their residual retention
  limitation is recorded.

### Gate 6: bounded diagnostics

- operators can see effective replay and configured-lock state without the
  lock path;
- MCP probe distinguishes healthy, failed startup, and busy lease;
- deployed checks verify the exact browser executable and required tool
  catalog;
- diagnostic activity does not retry, acknowledge, or retain workflow state.

### Gate 7: validation and deployment

- all focused and broader affected tests pass on every code PR;
- required CI and review complete for non-docs PRs;
- merged `main` is deployed with backup and rollback available;
- read-only and approval-stopped workflows pass;
- injected uncertainty produces no duplicate tool call;
- service health, journal, profile integrity, cleanup, and absence of secret
  leakage are verified;
- evidence and residual limitations are recorded before B0 is declared
  complete.

## Mandatory Completion And Stop Condition

B0 remains incomplete until every gate above has merged-main and deployment
evidence.

Stop the affected work and return to architecture admission if:

- the MCP SDK cannot distinguish a call-level session-loss path from a safe
  pre-dispatch failure;
- reconnect necessarily reissues the original call;
- uncertainty cannot be represented distinctly to the tool wrapper and
  runtime events;
- the exclusive lease cannot be held across reconnect without duplicate lock
  ownership or unsafe lock-file replacement;
- browser workflow compatibility requires unrelated high-authority MCP
  servers;
- artifact cleanup cannot prove exact file ownership;
- validation requires mutating a real external listing, purchase, message, or
  form submission;
- a required fix creates a first-party browser broker, model-facing browser
  tool, companion route, profile selector, credential API, or desktop-control
  surface.

## Non-Goals

- first-party `browser_targets`, `browser_session`, `browser_observe`, or
  `browser_act` tools;
- named browser sessions or multiple browser profiles;
- companion, cloud, or automatic target selection;
- generic node-hosted MCP forwarding;
- runtime DOM-aware effect classification;
- two-phase browser action approval;
- exactly-once website effects;
- automatic model retry or reconciliation;
- raw CDP or JavaScript evaluation policy;
- browser credential injection or profile migration;
- complete built-in tool least privilege for the specialist;
- human live view or takeover;
- cleanup of unregistered driver output or existing historical files;
- arbitrary desktop, clipboard, camera, microphone, or device control;
- WebDriver BiDi or alternative driver adapters;
- remote workspace routing;
- treating this admission as authorization for B1 or later milestones.
