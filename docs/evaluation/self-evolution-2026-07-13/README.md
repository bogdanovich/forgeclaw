# Self-Evolution Held-Out Trial, 2026-07-13

## Purpose

This experiment tested whether the strongest plausible drafts found in the
production nutrition corpus improved agent behavior over the existing
instructions. It evaluates the current self-evolution implementation, not the
general possibility of learning useful procedures from prior work.

The four candidates were the score-3 drafts from the audit's initial
deterministic 24-draft sample. They were selected because they were the most
actionable and internally coherent candidates in that sample, not at random.
None had previously been shown to improve an outcome.

## Method

- Model: the nutrition profile's configured `gpt-5.6-terra` model.
- Baseline: current production nutrition instructions and existing active
  skills, including the existing reference-food skill where relevant.
- Candidate variant: the same baseline plus exactly one draft.
- Isolation: a disposable workspace with deterministic stub tools. No
  production database, vault, MCP server, or workspace was available.
- Tool contracts: realistic descriptions and JSON schemas; calls and arguments
  were captured for deterministic scoring.
- Repeats: three paired runs per case, with baseline/candidate order alternated.
- Prompts: synthetic held-out requests that were not source records for the
  evaluated drafts.
- Criteria: the requested operation completed through the correct mutation
  path.
- Protected invariants: one final delivery, bounded tool calls, and no wrong
  mutation.

Model calls are nondeterministic despite paired seed labels, so repeated trials
reduce but do not eliminate variance. Each run used a new provider instance.

## Results

| Candidate | Held-out behavior | Baseline | Candidate | Verdict |
|---|---|---:|---:|---|
| `draft-rule-28b32d9d847d` | Nearby additive meal update | 3/3 correct | 0/3 correct | Regression: all candidate runs created a separate meal instead of amending the existing meal. |
| `draft-rule-28b32d9d847d` | Explicit merge request | 3/3 correct | 3/3 correct | No benefit; candidate used more tool calls and was slower. |
| `draft-rule-60e30041ae5b` | Label correction | 3/3 correct | 3/3 correct | No benefit; candidate averaged two tool calls versus one. |
| `draft-rule-408db0ceaa7f` | Save reusable food reference | 3/3 correct | 3/3 correct | No measured benefit. |
| `draft-rule-26df4403edc5` | Correct meal and save reference | 3/3 correct | 3/3 correct | No measured benefit. |

Across four candidates and 30 model runs, zero candidates were beneficial and
one caused a protected regression. Coverage was 100%, useful yield was 0%, and
the evaluator recommendation was `redesign` because a regression takes
precedence over a no-benefit result.

## Decision

The current production self-evolution implementation does not justify its
runtime, storage, model cost, or mutation complexity. ForgeClaw consequently
removed recording, draft generation, configuration, and workspace mutation in
merge `7832e23f`. The generic trace, scenario, replay, corpus-audit, and
paired-evaluation tools remain: they are useful for diagnosing runtime behavior
and evaluating any future, separately designed learning experiment.

This is an evidence-based rejection of the current implementation. It is not a
claim that every possible learning design is ineffective. A future design must
start as an explicit, bounded experiment and demonstrate held-out improvement
before routine production capture or drafting is enabled.

## Evidence

- `manifest.json`: sanitized paired criteria, observations, and policy input.
- `report.json`: deterministic evaluator output.
- `observations.json`: sanitized model/tool behavior used to derive criteria.

SHA-256:

```text
c7133472a5314a3b64b4d900ac6c64f88fe7734bf7fc487844829af442e5017a  manifest.json
4ab50dd1b40f5c75b86b8cf7fe3478ccb54be6decbd45cd6a247b1191b176083  report.json
11a49605a8186c61f14746077efc45d3d0d9e13d99e0dba2994b097d7dfba1ea  observations.json
```

Raw model output and complete tool arguments remain in owner-only local
evidence and are intentionally not committed. Provider token usage was not
available, so token fields are zero and are not used in the verdict.

## Limits

The experiment covers one profile, one domain, one configured model, four
selected candidates, and one execution date. It does not estimate a
distribution-wide success rate or compare alternate generators. Selection was
deliberately favorable to the subsystem: only the strongest plausible sampled
drafts were tested. The result is sufficient to reject routine operation of the
current implementation, while broader claims would require broader trials.
