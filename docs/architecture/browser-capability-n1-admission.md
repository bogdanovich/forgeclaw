# Browser Capability N1 Admission

## Status And Decision

Browser network-policy follow-up N1, **Public Web**, is admitted as the
dependency-ordered change in this document.

N1 adds one operator-selected network mode to the existing B1 managed browser
profile:

- `exact_origins` remains the default and admits only configured origins; and
- `public_web` admits arbitrary public HTTP and HTTPS destinations.

N1 does not admit private, loopback, link-local, multicast, unspecified, or
cloud-metadata destinations. It also does not admit the later `any_http` mode.
The model and page cannot select or broaden the configured mode.

## Admission Evidence And Residual Gap

B1 is merged and deployed. A real gateway-channel flow delegated to the
existing browser specialist, used only `browser_targets`, `browser_session`,
`browser_observe`, and `browser_act`, followed the Craigslist cross-origin
redirect, returned a bounded observation, and closed the session.

The deployed exact-origin adapter passes Playwright MCP an origin allowlist and
rechecks the observed origin in the broker. The pinned Playwright MCP package
explicitly documents that its allowlist is not a security boundary and does
not affect redirects. Consequently, broker validation prevents further action
after a denied observed origin, but it cannot prove that a redirect or page
subresource was rejected before a network connection occurred.

N1 closes that pre-fetch enforcement gap for both `exact_origins` and
`public_web`. Driver allowlists remain defense in depth, not the authoritative
network boundary.

## Admitted Operator Workflow

The N1 vertical slice is a public-web browsing dry run:

1. the operator configures the existing `gateway/managed` profile with
   `network_mode: public_web`;
2. the main agent delegates once to the existing browser specialist;
3. the specialist discovers the effective mode through `browser_targets`;
4. it opens one broker-owned session and navigates to an arbitrary public
   HTTPS origin that is absent from `allowed_origins`;
5. a public cross-origin redirect and public subresources load through the
   same network enforcement boundary;
6. a redirect or direct request to a loopback or private fixture is rejected
   before a connection reaches that fixture;
7. the specialist observes the resulting public page and closes the session;
   and
8. bounded traces prove the selected mode, denial class, cleanup, and absence
   of model-visible proxy details.

No external mutation is required. Existing dry-run, effect, approval,
snapshot, invocation, and profile-lease behavior is unchanged.

## Configuration Contract

The existing profile gains one bounded field:

```json
{
  "mode": "managed",
  "network_mode": "public_web",
  "dry_run": true
}
```

Rules:

- an omitted `network_mode` means `exact_origins` for backward compatibility;
- `exact_origins` requires at least one normalized `allowed_origins` entry;
- `public_web` requires `allowed_origins` to be empty, avoiding an ambiguous
  intersection or union of policies;
- N1 rejects every other value, including `any_http`;
- the effective network mode and normalized exact origins participate in the
  browser policy revision;
- a session pins the effective network mode at open;
- reload closes or expires sessions whose network authority changes; and
- `browser_targets` reports only the effective mode, not resolver answers,
  proxy addresses, driver arguments, or hidden policy state.

## Authoritative Network Boundary

### Worker-owned enforcing proxy

Each Playwright worker owns one internal HTTP forward proxy for its exact
lifetime. The adapter:

1. binds an ephemeral loopback listener before launching the driver;
2. configures the pinned Playwright MCP process to use that proxy for browser
   traffic and removes Chromium's implicit loopback bypass;
3. rejects operator-supplied driver proxy, bypass, origin-policy, and config
   arguments or environment variables that could compete with managed policy;
4. starts the driver only after the proxy is ready;
5. stops the proxy during failed open, normal close, timeout cleanup, and
   gateway shutdown; and
6. never exposes the listener address or driver proxy arguments to the model.

The proxy is a narrow network-policy component, not a general browser engine,
remote proxy product, capture system, or model-visible tool. It does not log
request bodies, response bodies, cookies, authorization values, or query
strings.

### Request admission

Every browser HTTP request and HTTPS `CONNECT` request crosses the proxy.
Before dialing, the proxy:

- accepts only HTTP or HTTPS destination semantics;
- rejects user information, malformed authorities, invalid ports, and
  ambiguous numeric host forms;
- normalizes the destination origin with the same browser URL rules used by
  action preparation;
- in `exact_origins`, requires membership in the session's configured origin
  set;
- in `public_web`, permits a valid public DNS name or public IP literal;
- resolves DNS names itself and rejects an empty answer, resolver failure, or
  any answer set containing a non-public address; and
- dials a validated address directly without a second hostname lookup.

An HTTP redirect is a new browser request and therefore crosses the same
boundary before its destination is fetched. Page subresources, workers,
WebSockets, and new tabs receive the same destination checks. A rejection
returns a generic proxy failure and makes no connection to the denied address.

### DNS rebinding and connection reuse

Resolution and connection establishment are one authority decision:

- the proxy, not the browser or system HTTP transport, owns the authoritative
  lookup used for a new connection;
- all returned addresses must be public; mixed public/private answers fail
  closed;
- the dial target is one address from that validated result, so DNS cannot
  change between validation and connection;
- a later connection performs a fresh lookup and rejects a host that has
  rebound to a denied address; and
- reusing an existing connection retains the already validated peer and does
  not create new authority.

DNS aliases and CNAME processing are not trusted independently; only the final
address set authorizes a dial. A public service that intentionally proxies to
another service remains the public service's behavior and is outside N1's
network-layer guarantee.

## Broker And Session Invariants

The broker keeps its existing pre-action and post-observation checks:

- action preparation rejects a syntactically invalid or policy-denied
  navigation before invocation acceptance;
- the worker proxy independently enforces every actual browser request;
- observations are still normalized and checked against the pinned policy;
- an impossible denied observed origin quarantines the session as
  `network_denied` because it indicates driver-policy divergence; and
- accepted action no-replay and explicit unknown-outcome behavior are
  unchanged.

The proxy cannot approve an action, select a profile, alter dry-run, classify
an effect, mint a snapshot reference, or grant a model-visible capability.

## Failure Semantics And Diagnostics

Model-visible failures remain bounded and safe:

- invalid destination syntax: `invalid_request`;
- destination denied by the effective mode: `network_denied`;
- DNS failure or a mixed/denied answer set: `network_denied`;
- proxy startup or unexpected proxy exit: `driver_unavailable`;
- driver policy divergence after a successful request: session lost with
  `network_denied`; and
- cleanup failure: existing worker-unavailable lifecycle semantics.

Diagnostics may report target/profile readiness, effective network mode,
proxy readiness, a bounded denial class, and lifecycle state. They must not
report resolved addresses, proxy endpoints, request headers, credentials,
cookies, complete URLs with queries, or response content.

## Dependency-Ordered Delivery

1. Merge this admission document.
2. Add the validated configuration enum, backward-compatible default, policy
   revision, and discovery projection.
3. Add a driver-independent destination policy and resolver/dial coupling with
   deterministic unit tests.
4. Add the worker-owned loopback proxy and lifecycle integration.
5. Add real-browser fixtures for public redirects, denied private redirects,
   subresources, WebSockets, DNS rebinding, and cleanup.
6. Deploy merged `main`, enable `public_web` only for the existing managed
   profile, and collect live gateway-channel evidence.
7. Record N1 residual limitations before admitting N2.

Implementation may land in one focused code PR if configuration, policy,
proxy, and adapter changes remain one browser subsystem and the architecture
checkpoint does not trigger.

## Exact Completion Gates

N1 is complete only when:

- omitted configuration preserves deployed `exact_origins` behavior;
- discovery reports `exact_origins` or `public_web` accurately;
- exact-origin redirects are denied before reaching an unlisted fixture;
- public mode reaches unrelated public HTTP and HTTPS origins without
  per-origin configuration;
- public-to-private redirects and private subresources never reach their
  fixture handlers;
- public IP literals work and loopback, private, link-local, multicast,
  unspecified, metadata, and ambiguous numeric forms fail closed;
- mixed DNS answers and a public-to-private rebinding fail before dial;
- the proxy cannot be bypassed by implicit loopback rules or competing driver
  configuration;
- worker open, failed open, close, timeout, and gateway shutdown leave no
  proxy listener or driver process;
- existing B1 action, recovery, approval, dry-run, and observation tests remain
  green;
- local race tests and real pinned-driver tests pass; and
- merged-main deployment completes through the browser specialist with a new
  bounded diagnostic trace and clean service health.

## Mandatory Stop Conditions

Stop N1 and return to architecture if:

- the pinned browser can make HTTP, HTTPS, WebSocket, redirect, worker, or
  subresource connections that bypass the enforcing proxy;
- proxy configuration requires exposing an endpoint or credential to the
  model;
- DNS validation and the actual dial cannot share one resolved address set;
- exact-origin and public-web policy require separate driver or broker state
  machines;
- enforcement requires TLS interception, certificate installation, browser
  extension authority, raw CDP, or arbitrary Playwright evaluation;
- the change expands into artifact capture, human takeover, companion routing,
  attached-user identity, or generic computer control; or
- production validation requires an external mutation.

## Non-Goals And N2 Boundary

N1 does not admit:

- `any_http` or any private/loopback destination exception;
- per-request model-selected network modes;
- VPN, PAC, SOCKS, upstream proxy, or proxy credential configuration;
- TLS interception, content inspection, HAR capture, or traffic recording;
- arbitrary headers, cookies, credentials, client certificates, or DNS server
  selection;
- non-HTTP schemes, raw sockets, WebRTC, UDP, QUIC policy expansion, or generic
  network tools;
- screenshots, uploads, downloads, human handoff, companion placement, or
  computer control; or
- treating public-web admission as approval for external website mutations.

N2 may later admit `any_http` as an explicit high-risk operator mode. It must
reuse the same profile, session, action, and driver contracts, and it cannot be
implemented or enabled until N1 has merged and has live enforcement evidence.
