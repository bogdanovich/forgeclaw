# Browser Capability N2 Admission

## Status And Decision

Browser network-policy follow-up N2, **Any HTTP**, is admitted as the
dependency-ordered change in this document.

N2 adds one explicit, high-risk operator-selected network mode to the existing
B1 managed browser profile:

- `exact_origins` remains the default and admits only configured origins;
- `public_web` admits arbitrary public HTTP and HTTPS destinations while
  rejecting non-public addresses; and
- `any_http` admits any syntactically valid HTTP or HTTPS destination,
  including loopback, private, link-local, and cloud-metadata addresses.

The mode is intentionally suitable for a trusted personal agent that must use
local services and private networks. It is not selected automatically. The
model, page, browser driver, and individual action cannot select or broaden
the configured mode.

## Admission Evidence And Residual Gap

N1 is merged and deployed. A live gateway flow delegated to the existing
browser specialist, used only the first-party browser tools, navigated through
the managed worker proxy to `https://example.org/`, observed the resulting
page, and closed the session. A second live flow attempted
`http://127.0.0.1:18191/`; `public_web` denied the navigation with a safe policy
error before dial and closed the session. Bounded diagnostic traces record
both completed turns.

The residual gap is deliberate: the deployed modes cannot reach a service on
the operator's loopback interface, LAN, tailnet, private cloud network, or
metadata endpoint. N2 broadens only destination admission so those workflows
can use the same governed browser capability.

## Admitted Operator Workflow

The N2 vertical slice is a private-service browsing dry run:

1. the operator explicitly configures the existing `gateway/managed` profile
   with `network_mode: any_http`;
2. the main agent delegates once to the existing browser specialist;
3. the specialist discovers the effective mode through `browser_targets`;
4. it opens one broker-owned session and navigates to an operator-controlled
   loopback HTTP fixture;
5. the fixture proves that a request reached it and returns a bounded page;
6. the specialist observes the page and closes the session; and
7. a separate public HTTPS navigation proves that N2 does not regress ordinary
   public browsing.

No external mutation is required. Existing dry-run, effect, approval,
snapshot, invocation, and profile-lease behavior is unchanged.

## Configuration Contract

The existing profile accepts the additional enum value:

```json
{
  "mode": "managed",
  "network_mode": "any_http",
  "dry_run": true
}
```

Rules:

- an omitted `network_mode` continues to mean `exact_origins`;
- `exact_origins` requires at least one normalized `allowed_origins` entry;
- `public_web` and `any_http` require `allowed_origins` to be empty;
- every other value is rejected;
- the selected mode participates in the browser policy revision and is pinned
  when a session opens;
- reload closes or expires sessions whose network authority changes;
- a narrower mode never falls back to `any_http`; and
- `browser_targets` reports the effective mode without exposing resolver
  answers, proxy addresses, driver arguments, or hidden policy state.

Selecting `any_http` is the explicit high-risk acknowledgement. N2 does not add
a model-call parameter, wildcard origin, implicit development fallback, or
automatic retry under broader authority.

## Authoritative Network Boundary

N2 reuses the N1 worker-owned enforcing proxy. Every browser HTTP request,
HTTPS `CONNECT`, redirect, subresource, worker request, WebSocket handshake,
and new tab crosses that proxy. Driver allowlists remain defense in depth.

Before dialing in `any_http`, the proxy:

- accepts only normalized HTTP or HTTPS destination semantics;
- rejects user information, malformed authorities, invalid ports, ambiguous
  numeric host forms, and unsupported schemes;
- accepts standard IPv4 and IPv6 literals regardless of address scope;
- resolves DNS names itself with the existing bounded answer-set limit;
- rejects an empty answer set or resolver failure; and
- dials an address from that exact answer set without a second hostname
  lookup, trying the bounded candidates according to existing dial behavior.

Unlike `public_web`, `any_http` does not classify or reject an address because
it is loopback, private, link-local, multicast, unspecified, or a known
metadata endpoint. This is the admitted behavior, not a policy bypass.

The existing resolve-and-dial coupling remains useful even though all address
scopes are admitted: it keeps request behavior deterministic, preserves the
single proxy boundary, avoids an unreviewed second resolver path, and ensures
mode changes take effect on new connections. It is not presented as SSRF
protection in `any_http` mode.

## Authority That Does Not Expand

N2 changes only the destination predicate used by the existing broker and
proxy. It does not expand:

- which agents or profiles expose browser tools;
- session ownership, concurrency, lease, expiry, or cleanup behavior;
- the typed browser action set or allow arbitrary Playwright, MCP, CDP, shell,
  socket, or computer-control forwarding;
- accepted URL schemes beyond HTTP and HTTPS;
- dry-run behavior, effect classification, approval policy, or accepted-action
  replay rules;
- credentials, cookies, headers, client certificates, uploads, downloads, or
  profile selection;
- observation limits, screenshot or artifact authority, diagnostics, or audit
  redaction; or
- companion placement or remote-browser routing.

An HTTP endpoint being reachable does not authorize a state-changing action on
that endpoint. Existing action risk and approval rules remain authoritative.

## Failure Semantics And Diagnostics

Model-visible failures remain bounded and safe:

- invalid destination syntax or unsupported scheme: `invalid_request`;
- DNS failure or empty answer set: `network_denied`;
- connection failure after admission: the existing bounded driver/network
  failure;
- proxy startup or unexpected proxy exit: `driver_unavailable`;
- impossible policy divergence: session lost with `network_denied`; and
- cleanup failure: existing worker-unavailable lifecycle semantics.

Diagnostics may report target/profile readiness, `network_mode: any_http`,
proxy readiness, a bounded failure class, and lifecycle state. They must not
report resolved addresses, proxy endpoints, request headers, credentials,
cookies, complete URLs with queries, or response content.

## Dependency-Ordered Delivery

1. Merge this admission document after N1 live evidence exists.
2. Add the validated `any_http` configuration value, policy revision behavior,
   and discovery projection.
3. Extend the shared broker and proxy destination predicate without adding a
   second transport path.
4. Add deterministic unit, race, and real-browser tests for admitted private
   destinations and unchanged narrower modes.
5. Deploy merged `main`, explicitly enable `any_http` only for the existing
   managed profile, and collect live gateway evidence against public HTTPS and
   an operator-controlled loopback fixture.

Implementation may land in one focused code PR if it remains a destination-
policy expansion inside the existing browser subsystem.

## Exact Completion Gates

N2 is complete only when:

- omitted configuration still selects `exact_origins`;
- invalid mode values and ambiguous `allowed_origins` combinations fail;
- discovery accurately reports all three modes;
- `any_http` admits standard public, private, loopback, link-local, and
  metadata IPv4 and IPv6 literals;
- DNS names resolving to public, private, loopback, link-local, or mixed
  address sets are admitted and dialed from the bounded resolved set;
- malformed URLs, user information, ambiguous numeric hosts, invalid ports,
  and non-HTTP schemes remain rejected;
- `exact_origins` and `public_web` retain all N1 denial behavior;
- changing a profile to `any_http` cannot broaden an already open session;
- proxy lifecycle and driver-bypass tests remain green;
- existing B1 and N1 action, recovery, approval, dry-run, observation, race,
  and real-driver tests remain green; and
- merged-main deployment completes through the browser specialist with public
  success, loopback-fixture success, closed sessions, bounded traces, and clean
  service health.

## Mandatory Stop Conditions

Stop N2 and return to architecture if:

- private access requires bypassing or disabling the worker proxy;
- the mode must be exposed as a model-selected or per-action option;
- implementation requires a second browser, broker, resolver, or driver state
  machine;
- the change weakens narrower-mode enforcement or implicit default behavior;
- private reachability also grants arbitrary headers, credentials, raw sockets,
  MCP/CDP forwarding, or generic computer control;
- policy reload can broaden an existing session in place; or
- live validation requires a state-changing external action.

## Non-Goals

N2 does not admit per-destination exceptions, wildcard origin syntax, PAC,
SOCKS, VPN, upstream proxy credentials, DNS-server selection, non-HTTP
transports, TLS interception, traffic recording, screenshots, artifact capture,
uploads, downloads, human takeover, companion routing, attached-user identity,
or generic computer control. Those remain separate roadmap work.
