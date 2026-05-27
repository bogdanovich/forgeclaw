# Seahorse LCM Compatibility Plan

Seahorse already has the core pieces of a lossless-context-manager style system:
durable messages, leaf summaries, condensed summaries, parent links, context items,
and grep/expand tools. The current debt is not the data model. The debt is that
assembly and pressure compaction do not yet behave like a bounded LCM pipeline.

## Current Failure Mode

Large sessions can produce an oversized `CONTEXT_SUMMARY` system block even after
proactive compaction. The common pattern is:

- many leaf summaries remain active together;
- one or more condensed summaries are also active;
- summary token counts are estimated from raw `summary.content`, but the final
  prompt uses formatted XML;
- static prompt and tool schema budget are reserved outside Seahorse, after
  Seahorse has already selected context;
- the agent can still call the LLM after `still_overlimit=true`.

This means Seahorse can compact correctly at the storage layer while still
assembling too much summary text for the model request.

## Target Behavior

Seahorse should follow these LCM-like constraints:

1. Budget on formatted prompt items, not raw summary text.
2. Reserve non-Seahorse prompt/tool budget before assembly.
3. Keep active context to coverage roots plus fresh tail, not covered leaves plus
   their condensed parent.
4. Add summary-prefix pressure compaction so old summary prefix has its own cap.
5. Hard-cap oversized generated summaries.
6. Fail closed if context still does not fit after compaction.

The near-term goal is to fix Seahorse in place, not rewrite it as a new LCM
implementation. A Go LCM clone should only be considered if these compatibility
fixes still leave structural problems.

## Planned Incremental Commits

1. Add this architecture plan.
2. Make Seahorse count formatted summary XML when selecting context.
3. Reserve static prompt/tool budget before calling Seahorse assembly.
4. Add summary-prefix pressure compaction and tests.
5. Ensure active context assembly prefers coverage roots plus fresh tail.
6. Add hard caps for leaf/condensed summary output.
7. Relax XML escaping for text content while keeping strict attribute escaping.
8. Add fail-closed behavior when compaction still leaves the request over budget.

Each step should include targeted tests before moving to the next one.
