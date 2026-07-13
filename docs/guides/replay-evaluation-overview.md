# Replay and Evaluation: Practical Overview

ForgeClaw's replay and evaluation system is a diagnostic and regression-testing
facility for agent runs. It answers questions such as:

- Was a required final response delivered exactly once?
- Was steering applied instead of silently lost?
- Did an interrupted task recover into a valid terminal state?
- Did context compaction retain the facts it was required to preserve?
- Did loop protection stop repeated tool calls?
- Did provider fallback stop after selecting a successful provider?
- Did an evolution candidate respect its safety requirements?

It does not improve an answer automatically, retry a failed task, or change the
agent's behavior based on a score. It records bounded evidence and lets an
operator or CI job check that evidence against explicit invariants.

## What works automatically

The deterministic evaluator implementation and its historical regression
fixtures are always available. Pull-request CI runs the fixture matrix as part
of the Go tests, so changes that break a known passing or failing case are
detected automatically.

After production trace capture is enabled, ForgeClaw automatically:

1. observes supported runtime, task, delivery, steering, compaction, fallback,
   tool-loop, and evolution events;
2. normalizes and redacts the permitted evidence;
3. writes bounded trace files after eligible agent turns;
4. expires old files according to the configured retention and count limits.

Capture does not automatically evaluate every new trace. Run `picoclaw eval`
manually or from a scheduled job or CI workflow when you want a report.

## What is disabled by default

Production trace capture is disabled by default. This avoids extra state files
and makes data collection an explicit operator decision. Running an evaluation
command does not turn capture on.

To enable conservative metadata-only capture, add this to the ForgeClaw
configuration:

```json
{
  "evaluation": {
    "trace_capture": {
      "enabled": true,
      "content_mode": "metadata_only"
    }
  }
}
```

Restart or reload ForgeClaw using the normal deployment procedure after
changing the configuration. With no custom `state_dir`, trace files are stored
under:

```text
WORKSPACE/state/evaluation/traces
```

`metadata_only` is the recommended starting mode. It stores typed statuses,
counts, hashes, opaque identifiers, and policy codes rather than raw prompts,
tool arguments, tool results, steering text, or draft bodies.

## How to use a captured trace

Run all deterministic checks for one trace:

```bash
picoclaw eval WORKSPACE/state/evaluation/traces/TRACE_ID.json
```

Run one check and emit stable JSON for automation:

```bash
picoclaw eval --json \
  --evaluator duplicate_response.v1 \
  WORKSPACE/state/evaluation/traces/TRACE_ID.json
```

The possible finding statuses are:

| Status | Meaning |
|---|---|
| `pass` | The trace contains enough evidence and satisfies the invariant. |
| `fail` | The trace proves that the invariant was violated. |
| `error` | The evidence is malformed or internally inconsistent. |
| `not_evaluable` | The trace does not contain enough evidence for this check. This is not a pass. |

The command exits non-zero for `fail` or `error`, which makes it suitable for
scripts and CI gates.

## Practical workflows

### Investigate a reported failure

When a user reports a duplicate response, missing final answer, lost steering,
or bad recovery after restart, locate the trace for that turn and run the
relevant evaluator. The report identifies the invariant, observed evidence,
relevant record sequences, and a remediation hint.

### Turn a fixed incident into a regression case

After fixing a real failure, create a sanitized fixture derived from the
historical evidence. Add both the failing shape and the corrected passing shape
when appropriate. Validate the manifest with:

```bash
picoclaw eval fixtures pkg/evalevaluator/testdata/historical_failures.json
```

That case then runs in ordinary pull-request CI and protects the behavior from
future regressions.

### Monitor an experimental deployment

Enable metadata-only capture for a bounded period, retain the default 24 hours
and 100 traces, and evaluate traces related to the subsystem being changed.
This gives evidence about runtime invariants without turning trace capture into
permanent conversation logging.

## What the system does not do

- It does not judge whether an answer is helpful, correct, or well written.
- It does not send trace contents to another model.
- It does not replay live network calls, shell commands, tools, or user
  deliveries.
- It does not infer a user correction from ordinary follow-up messages.
- It does not treat missing evidence as success.
- It does not autonomously modify skills or runtime policy from findings.

Model-assisted grading is deliberately deferred. The current checks concern
structural runtime correctness and are more reliably expressed as deterministic
invariants.

## Recommended starting point

For a normal ForgeClaw deployment:

1. Leave capture disabled if you do not currently need incident diagnosis.
2. Enable `metadata_only` capture when investigating runtime reliability or
   validating a risky orchestration change.
3. Keep the default bounds initially: 2 MiB per trace, 2,000 records, 24-hour
   retention, and at most 100 traces.
4. Run evaluators on relevant traces manually; automate them only after deciding
   which findings should gate your deployment.
5. Preserve sanitized incidents as checked-in fixtures so CI provides the
   long-term benefit even after production traces expire.

For exact configuration fields, commands, evaluator names, fixture rules, and
isolation guarantees, see [Replay and Evaluation](replay-evaluation.md) and the
[architecture contract](../architecture/replay-evaluation.md).
