# Replay and Evaluation Completion Audit

Audit target: merged `bogdanovich/forgeclaw:main` at
`5a98ccbe82e6400ff42acf66d02c1f4c6f629acb`.

## Delivery status

| Layer | Merged evidence |
|---|---|
| Architecture and trace foundation | PR #209, merge `6e7f2d53d1b2032424d97431dd8aa3d42eb7cb1d` |
| Production capture | PR #211, merge `b6a00ec1f87a6e4c542c61e146e83b2a50cb560b` |
| Contract replay | PR #212, merge `ea19a97153918a9a7076f95e4ad2997bca633baf` |
| Isolated real-path scenarios | PR #213, merge `732e68b6690ea9464fed398cca5803d6cab3f7e7` |
| Evaluators, fixtures, CLI, and CI tests | PR #218, merge `5a98ccbe82e6400ff42acf66d02c1f4c6f629acb` |

## Capture evidence matrix

| Category | Production status | Source or reason |
|---|---|---|
| Turn start/end and final outcome | Captured | Agent runtime events plus trace outcome envelope |
| Model request/response/retry/fallback | Captured | Agent runtime events and observed fallback chain |
| Tool call/result/skip/loop decision | Captured | Tool pipeline runtime events with call IDs |
| Steering enqueue/injection/interrupt | Captured | Steering runtime events and bounded hashes/counts |
| Task and delivery transitions | Captured | Post-persistence task registry observer and channel events |
| Context compaction and snapshot | Captured | Context runtime events and final hashed snapshot |
| Context reconciliation | Unavailable | Context managers expose no typed revision transition event |
| Restart boundary | Unavailable | Runtime startup/reload exposes no typed boundary event |
| Inbound spool transition | Unavailable | Prepare/ack/release expose no typed transition event |
| Evolution records | Historical only | Runtime capture was removed with the rejected learning subsystem; schemas and fixtures remain readable |
| User correction | Fixture-only | ForgeClaw has no reliable automatic correction signal |

Unavailable categories are represented in the closed schema for future source
adapters and sanitized historical fixtures. Deterministic evaluators return
`not_evaluable` when required evidence is absent.

## Completion checks

- Trace schema, canonical ordering, bounds, redaction, secret leakage, atomic
  storage, and unsupported-version rejection have tests.
- `pkg/evalreplay` imports only `pkg/evaltrace` plus the standard library;
  production providers, tools, channels, gateways, shell, MCP, and network
  constructors are structurally absent from contract replay.
- `pkg/evalscenario` uses a minimal configuration, isolated production-tool
  bootstrap, sealed stubs, exact registry verification, and a disposable
  workspace while exercising the real inbound agent path.
- All eight named deterministic evaluators have sourced, sanitized passing and
  failing fixtures. The fixture matrix runs under the ordinary pull-request Go
  test job.
- `picoclaw eval --json` emits versioned machine-readable output and exits
  non-zero for fail/error findings. Missing evidence is `not_evaluable`.
- Operator and fixture-author guidance documents capture defaults, security,
  CLI behavior, fixture validation, limitations, and the deferred semantic
  grading decision.
- Model-assisted grading remains deferred because no concrete semantic rubric,
  provider, cost budget, or variance threshold is approved.

The implementation roadmap is complete for the deterministic scope. Future
work should add missing typed source events before claiming production coverage
for reconciliation, restart, or inbound spool transitions.
