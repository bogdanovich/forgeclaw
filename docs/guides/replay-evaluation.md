# Replay and Evaluation

ForgeClaw can capture bounded execution evidence, replay it without production
side effects, and run deterministic correctness evaluators over the resulting
projection. Capture is disabled by default.

For a plain-language explanation of when this is useful, what is automatic,
and the recommended setup, start with the
[practical overview](replay-evaluation-overview.md).

## Enable capture

Set `evaluation.trace_capture.enabled` to `true`. With the default empty
`state_dir`, traces are written to:

```text
WORKSPACE/state/evaluation/traces
```

Production capture stores filtered hashes, counts, statuses, opaque IDs, and
typed policy codes. It does not store raw prompts, tool arguments, tool results,
or steering text. Enabling an evaluation command never enables capture.

## Evaluate traces

Run every deterministic evaluator:

```bash
picoclaw eval WORKSPACE/state/evaluation/traces/TRACE_ID.json
```

Select one evaluator and produce stable JSON:

```bash
picoclaw eval --json \
  --evaluator delivery_reliability.v1 \
  WORKSPACE/state/evaluation/traces/TRACE_ID.json
```

The command exits non-zero when a finding is `fail` or `error`.
`not_evaluable` is not success: it means the trace lacks required evidence, but
it does not fail the command by itself. JSON output uses schema
`forgeclaw.eval_report.v1`.

Available evaluators:

- `delivery_reliability.v1`
- `duplicate_response.v1`
- `steering_correctness.v1`
- `restart_recovery.v1`
- `durable_interaction.v1`
- `compaction_retention.v1`
- `tool_loop_recovery.v1`
- `provider_failover.v1`

Each finding includes status, severity, relevant record sequences, the expected
invariant, the observed fact, and a remediation hint. There is no aggregate
quality score.

## Validate fixtures

The checked-in manifest contains sanitized passing and failing fixtures for
every deterministic evaluator:

```bash
picoclaw eval fixtures pkg/evalevaluator/testdata/historical_failures.json
```

Each fixture must declare an evaluator, expected status, historical source
commit or test, and normalized records. Fixture mode is accepted only for
explicit fixture traces; runtime capture cannot select it. Pull-request CI runs
manifest validation after the Go test suite.

## Scenario safety

`pkg/evalscenario` drives the real inbound agent path, but it does not construct
production tools or providers. Every run:

- uses a minimal configuration with production tool and MCP gates disabled;
- skips shared production state and tool bootstrap;
- registers only sealed text-result stubs named by the fixture;
- verifies the final registry exactly matches those stubs;
- confines agent-owned state writes to a temporary workspace;
- records a synthetic terminal delivery outcome through the runtime event bus;
- removes the workspace before returning.

Unknown tool names receive the normal deterministic missing-tool error. They
cannot cause a production tool to be resolved or executed.

Memory regressions use a separate deterministic component-replay matrix in
`pkg/agent/memory_replay_test.go`. These scenarios need real disposable
filesystem mutations and Seahorse SQLite queries, so they do not weaken the
generic scenario runner's sealed-stub boundary. Each memory scenario runs twice
and compares its canonical observation while asserting only operator-visible
surfaces: rendered prompt content, memory-tool outcomes, privacy-safe audit
events, retrieval results, and enabled tool capabilities. Raw private memory is
not added to evaluation traces.

## Limitations

- Historical traces can prove recorded invariants, not intent or answer quality.
- Missing or dropped evidence yields `not_evaluable` or `error`, never `pass`.
- User corrections must be explicit annotations; replies are not inferred to be
  corrections.
- Model-assisted grading is not part of deterministic CI and cannot override a
  deterministic finding.

Model-assisted grading is currently deferred because there is no approved
semantic rubric, provider, cost budget, or variance threshold. The current
failure classes are covered by deterministic evidence and do not justify model
calls.

The fixture matrix is part of the package test suite and therefore runs in
pull-request CI.

## Debug a human interaction

Question and approval lifecycles are captured automatically when trace capture
is enabled. Locate an `interaction-*` trace in the workspace trace directory,
then run:

```bash
picoclaw eval --evaluator durable_interaction.v1 \
  WORKSPACE/state/evaluation/traces/interaction-TRACE.json
```

The trace can identify duplicate answer claims, illegal transitions, an
interaction that never terminalized, duplicate successful prompt/final sends,
or an allowed-and-resolved approval that was not consumed exactly once. Allowed
approvals that fail or are cancelled before execution may remain unconsumed.
The evaluator requires `metadata.trace_kind: interaction`; partial interaction
evidence embedded in turn traces returns `not_evaluable`. It contains no raw
question, answer, approval summary, route, sender identity, or tool arguments.
Task/tool pairing spans separately persisted traces and is not evaluated by
this single-file command.
