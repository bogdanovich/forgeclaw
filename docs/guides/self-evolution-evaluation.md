# Self-Evolution Effectiveness Evaluation

ForgeClaw does not treat draft creation, model confidence, or successful file
writes as evidence that a learned skill is useful. The self-evolution evaluator
compares explicit task outcomes from baseline and candidate runs on held-out
cases.

It is an offline evidence gate. It does not generate, install, promote, or
delete skills, and it does not call a model.

## Run An Evaluation

Create a sanitized manifest following
`pkg/evolutioneval/testdata/held_out_example.json`, then run:

```bash
picoclaw eval evolution evaluation.json
picoclaw eval evolution --json evaluation.json
```

The JSON report uses schema `forgeclaw.evolution_eval_report.v1`.

## Evidence Contract

Each candidate declares the record IDs that generated it. Each held-out case
declares separate evidence IDs, weighted task criteria, protected invariants,
and paired baseline/candidate trials.

Trials are paired by an identical unique seed. Every trial must provide:

- a result for every task criterion;
- a result for every protected invariant;
- at least one trace, fixture, or other evidence reference;
- optional tool-call, token, and latency measurements.

The evaluator rejects a case as invalid when generation and held-out record IDs
overlap, trial seeds differ, evidence is missing, or the declared rubric is
incomplete. Invalid evidence is not a pass.

## Decisions

A case is `beneficial` only when its weighted candidate score improves over the
paired baseline by the configured threshold, every protected invariant passes,
and no required criterion regresses on a seed where baseline passed.

Candidate results are:

| Status | Meaning |
|---|---|
| `beneficial` | Every held-out case improves without a protected regression. |
| `not_beneficial` | Evidence is valid, but improvement is below policy. |
| `regression` | A protected invariant or required criterion regressed. |
| `invalid` | Evidence is incomplete, unpaired, malformed, or not held out. |

The report recommendation is deliberately conservative:

| Recommendation | Meaning |
|---|---|
| `retain_experiment` | Coverage and useful yield meet policy without regressions. |
| `redesign` | Some value may exist, but yield or regression results reject the current design. |
| `remove` | Adequately covered candidates produced no held-out improvement. |
| `insufficient_evidence` | Coverage, candidate count, or evidence validity is below policy. |

`retain_experiment` does not authorize automatic application. It only supports
continuing a bounded, human-reviewed experiment. Automatic mutation remains
disabled.

## Building Real Cases

Use a task that was not part of candidate generation. Run the same isolated
fixture with the same seeds against:

1. the unchanged baseline skill set;
2. the baseline plus exactly one candidate draft.

Derive deterministic criteria from observable state, tool results, and delivery
traces. Keep correctness and delivery checks protected. Preserve sanitized
evidence references so another operator can reproduce each boolean result.

Do not write a rubric after seeing candidate output, substitute a model's
opinion for an observable criterion, or reuse the task that generated the
draft. Those practices measure overfitting, not learning.
