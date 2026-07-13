# Self-Evolution Effectiveness Evaluation

ForgeClaw does not treat draft creation, model confidence, or successful file
writes as evidence that a learned skill is useful. The self-evolution evaluator
compares explicit task outcomes from baseline and candidate runs on held-out
cases.

It is an offline evidence gate. It does not generate, install, promote, or
delete skills, and it does not call a model. ForgeClaw no longer has a
self-evolution runtime; this command remains for auditing archived corpora and
evaluating bounded future experiments.

## Run An Evaluation

Create a sanitized manifest following
`pkg/evolutioneval/testdata/held_out_example.json`, then run:

```bash
picoclaw eval evolution evaluation.json
picoclaw eval evolution --json evaluation.json
```

The JSON report uses schema `forgeclaw.evolution_eval_report.v1`.

## Audit A Draft Corpus

Run content-free structural diagnostics over existing task, pattern, and draft
stores before selecting candidates for held-out evaluation:

```bash
picoclaw eval evolution corpus --json \
  --records WORKSPACE/state/evolution/task-records.jsonl \
  --records WORKSPACE/state/evolution/pattern-records.jsonl \
  --drafts WORKSPACE/state/evolution/skill-drafts.json
```

The `forgeclaw.evolution_corpus_report.v1` output contains candidate IDs, target
names, aggregate counts, and signal codes. It never emits record summaries,
final outputs, draft bodies, intended use cases, or review text.

Signals identify deterministic review targets: duplicate targets, known generic
templates, procedures copied from prior final output, oversized bodies, missing
provenance, absent ordered steps, and excessive source-skill references. They
are diagnostics, not usefulness scores. A draft with no signal still requires
paired held-out evidence, and a signaled draft may still contain a useful idea
that should be rewritten before evaluation.

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

### Running A Configured Model Safely

The `pkg/evalscenario` package exposes `RunWithProvider` for explicit
operator-run trials with a configured model. The runner creates a disposable
workspace, disables every production tool family and MCP server, and registers
only caller-supplied deterministic stub tools. A scenario may provide baseline
instructions, isolated skill files, the exact active-skill set, and realistic
tool descriptions and parameter schemas. Live scenarios must also declare a
context window, output-token limit, and tool-turn limit matching the evaluated
deployment when package defaults are not representative. The observation
includes tool-call counts and arguments so task criteria can be derived from
behavior rather than the model's explanation of what it did.

Use the same provider/model, prompt, tool schemas, stub results, and declared
seed for both sides of a pair. The candidate side must differ only by one
active draft. Run several pairs because a configured model is not deterministic
even when its request options include a seed. Do not register a production MCP
tool or point a scenario at a production database, vault, or workspace.

`RunWithProvider` does not run automatically, load production configuration, or
decide whether an observation passed. Operator code must load the provider
explicitly, convert observable results into the predeclared criteria, sanitize
evidence references, and pass the resulting paired trials to `picoclaw eval
evolution`. CI uses the scripted `Run` path and never makes model calls.

## Production Trial Evidence

The [2026-07-13 held-out nutrition trial](../evaluation/self-evolution-2026-07-13/README.md)
shows a complete operator-run use of this contract. Four selected production
drafts produced no beneficial candidates and one protected regression. The
sanitized manifest, deterministic report, and observations are committed next
to the methodology; raw model output remains owner-only and local.
