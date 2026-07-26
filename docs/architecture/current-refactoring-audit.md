# Current Refactoring Audit

This note captures the architecture risks found in the July 2026 static review.
It is intentionally scoped to near-term refactoring candidates rather than a
full design rewrite.

## Priority Findings

### Typed Outbound Metadata

Agent output metadata is currently passed through `bus.OutboundMessage.Raw` with
string keys such as `model_name`, `default_model_name`, and token usage fields.
The agent writes those keys and channel delivery code reads them directly.

This is flexible, but it has become a hidden cross-package protocol. Recent
footer regressions came from paths that did not preserve the same metadata
contract for final, streaming, and tool-loop responses.

Recommended direction:

- Add a typed outbound metadata API in the bus layer.
- Keep `Raw` as an edge compatibility layer, not the primary internal contract.
- Add contract tests for final responses, streaming finalization, tool-loop
  aggregation, and channel-independent footer rendering.

### Channel Manager Scope

`pkg/channels/manager.go` owns delivery queues, worker lifecycle, streaming,
placeholders, retries, tool feedback, reload behavior, and response footer
finalization. This makes unrelated changes interact in subtle ways.

One concrete lifecycle smell is that shutdown paths fail pending outbound queues
twice after receiving context cancellation. That is likely harmless because the
first drain empties the queue, but it points to lifecycle complexity.

Recommended direction:

- Split delivery queue ownership from stream coordination.
- Move placeholder/tool-feedback coordination out of the main manager.
- Centralize retry policy around a typed delivery result.

First boundary completed:

- Transient delivery interaction state and stream suppression state have
  dedicated owners embedded in `Manager` for compatibility.
- TTL eviction is implemented and tested by those owners instead of duplicated
  in manager tests.
- Queue admission uses a close signal and in-flight barrier so cancellation can
  wake blocked enqueuers before a single pending-outcome drain.

Remaining:

- Move queue registration and lifecycle operations behind a delivery registry.
- Move stream and tool-feedback operations onto their state owners rather than
  relying on promoted fields.

### Turn Execution State

`turnExecution` is a large mutable bag shared by LLM calls, tool execution,
streaming state, final rendering, persistence, token usage, and abort handling.
This makes phase boundaries implicit and puts too much weight on regression
tests to catch cross-phase invariant breaks.

Recommended direction:

- Introduce explicit phase outputs such as `LLMCallOutcome`,
  `ToolLoopOutcome`, and `FinalizationContext`.
- Keep mutation local to each phase and pass typed results forward.
- Add invariant tests around usage aggregation, final content selection,
  streaming completion, and compaction triggers.

### Delivery Retry Semantics

Text delivery, media delivery, and Telegram chunk finalization each carry related
but separate retry and partial-success logic. This makes platform-specific edge
cases easy to fix in one path while leaving another path inconsistent.

Recommended direction:

- Define a shared delivery result type with message IDs, partial-success state,
  retry-after, and remaining payload metadata.
- Let channel adapters map platform errors into this type.
- Keep retry decisions in one coordinator.

### Context Manager Migration

Resolved in the follow-up refactor:

- Seahorse is the explicit default.
- `none` is the deliberately stateless mode.
- The legacy implementation and fallback behavior are removed.
- Unknown managers and initialization failures stop startup.
- Migration guidance documents the prompt and persistence semantics.

### Session Persistence Error Handling

Some JSONL session APIs intentionally log write errors instead of returning them
to preserve a fire-and-forget contract. Several paths also use
`context.Background()` internally. This can hide persistence failures from
agent-critical flows and makes shutdown/cancellation boundaries less explicit.

Recommended direction:

- Move agent-critical writes toward error-returning APIs.
- Thread request/shutdown contexts through persistence operations.
- Keep fire-and-forget behavior only at outer compatibility boundaries.

### Provider Capabilities And Error Classification

Provider behavior is discovered through optional interfaces, and failover
classification relies heavily on string and regex matching of provider errors.
This is pragmatic but brittle as provider APIs and error text change.

Recommended direction:

- Add structured provider errors for rate limit, auth, billing, context overflow,
  timeout, and transient server failures.
- Add provider capability descriptors and provider contract tests.
- Keep string classification as a fallback for unknown providers.

## Suggested Order

1. Type outbound metadata and add channel-independent footer/usage contract
   tests.
2. Migrate context manager defaults away from legacy. Completed with Seahorse
   as the default and `none` as the named stateless mode.
3. Unify delivery retry results across text, media, and Telegram chunks.
4. Split the largest channel manager responsibilities.
5. Replace `turnExecution` phase mutation with typed phase outcomes.
