# Tool-Loop Stagnation Protection

## Status

This document is the implementation specification for generic tool-loop
stagnation protection in ForgeClaw. It describes required invariants and a
preferred design, not an obligation to preserve current internal boundaries.
Implementation may refactor or replace existing code when that produces a
clearer architecture. Material deviations must preserve the invariants below
and be explained in the pull request.

## Problem

ForgeClaw limits the number of model/tool iterations and has a special-case
breaker for repeated fatal MCP transport failures. It does not generally detect
an agent that repeatedly:

- submits the same failing call;
- keeps calling one failing tool with different arguments;
- repeats an idempotent read and receives identical information.

The first implementation should cover these common cases with a small,
auditable controller. Polling-specific, alternating ping-pong, global, and
post-compaction detectors are deferred until a concrete ForgeClaw trace and
regression test demonstrate that the smaller controller is insufficient.

## Current Runtime Constraints

### Primary pipeline

`pkg/agent/pipeline_execute.go` executes a provider tool-call batch
sequentially. Before execution, a call can be denied or modified by the turn
profile and hooks. After execution, hooks and synchronous delivery can replace
or wrap the `ToolResult`. Steering can stop the rest of a batch, but ForgeClaw
still appends a synthetic tool result for every skipped provider call.

Detector integration must use the effective name, arguments, and result after
these transformations. It must preserve the one-result-per-tool-call history
invariant.

### Reusable legacy loop

`pkg/tools/toolloop.go` executes calls in a provider batch concurrently and
appends results in provider order. Detector state must never be mutated by the
worker goroutines. Before-call decisions should be made before launch; results
should be observed in provider order after all workers complete.

### Turns and subturns

Each primary turn creates a `turnExecution`. Each SubTurn runs its own turn and
must have independent detector state. State must not leak between parent and
child, between sessions, across restarts, or into a later user turn.

### Existing failure handling

`ToolResult.IsError` is the structured failure signal. Generic detection must
not infer failure through broad matching of result prose. The existing fatal
MCP handling may remain as a faster specialized policy or be replaced by a
general mechanism if its fail-fast behavior is retained by tests.

## Required Invariants

1. Every provider tool call receives exactly one role=`tool` result with its
   original `tool_call_id`, including blocked and steering-skipped calls.
2. The detector never executes tools, mutates conversation history, publishes
   events, logs, or performs delivery.
3. Raw tool arguments and raw tool results are not retained or emitted by the
   detector.
4. Successful repeated output from a mutating or unknown tool is never blocked
   as read-only no-progress.
5. Detector state is bounded and scoped to one turn execution.
6. Warning annotations are not included in the result identity used by future
   observations.
7. Blocking a tool does not itself corrupt or abruptly truncate the provider
   turn. The model receives a synthetic result and can choose another strategy.
8. Existing maximum-iteration handling remains the final bound on model turns.

## Proposed Components

### Pure controller

Create a dependency-safe package under `pkg/tools`, tentatively
`pkg/tools/loopguard`. Its public surface should be equivalent to:

```go
type Action string // allow, warn, block, halt

type Decision struct {
    Action    Action
    Code      string
    Tool      string
    ArgsHash  string
    Count     int
    Threshold int
    Message   string
}

type Observation struct {
    Tool       string
    Args       map[string]any
    ResultText string
    Failed     bool
    Semantics  Semantics
}

type Controller struct { /* bounded per-turn state */ }

func (c *Controller) Before(tool string, args map[string]any, semantics Semantics) Decision
func (c *Controller) After(observation Observation) Decision
```

Exact names may change. The separation of responsibilities may not: the
controller returns decisions, while runtime adapters own synthetic results,
warning annotations, events, logs, and turn control.

### Signatures and retained state

Canonicalize arguments as deterministic JSON with recursively stable map-key
ordering, then retain only a SHA-256 digest. Hash normalized result identity as
well. Never include raw values in `Decision`, runtime events, logs, or persisted
records.

Keep only the bounded state needed for configured thresholds. Possible records
include:

- failure count by `(tool, args_hash)`;
- consecutive failure count for the current tool streak;
- latest `(result_hash, repeat_count)` by read-only call signature.

Do not retain unbounded maps for every call seen during a long turn. Evict old
entries or cap storage based on the largest active threshold.

### Tool semantics

Read-only no-progress detection requires an explicit classification. Prefer an
optional capability interface or equivalent registry metadata so the base
`Tool` interface does not require mechanical changes to every implementation.

Semantics should distinguish at least:

- `unknown`: safe default for MCP, dynamic, and unclassified tools;
- `read_only_idempotent`: eligible for identical-result detection;
- `mutating`: never eligible for repeated-success blocking.

Only audited built-in tools should opt into `read_only_idempotent`. The
implementation must not assume that a tool is read-only merely because no
`WriteAudit` was returned: absent audit is not proof that no side effect
occurred.

## Detection Rules

### Repeated exact failure

Increment when the same effective tool and argument hash returns
`ToolResult.IsError=true`. Warn before the configured block threshold. A
successful result for that signature clears its exact-failure count.

### Same-tool failure streak

Track consecutive failed executions of the same effective tool even when
arguments differ. A successful call or an intervening different tool ends the
streak. Warn before the configured halt threshold.

`halt` may stop further execution in the current batch or turn only if the
implementation can still append valid results for every provider call. A
synthetic blocked result followed by another model boundary is the preferred
recovery path.

### Identical read-only result

For `read_only_idempotent` tools only, compare the result hash for the same
effective tool and argument hash. Repeated identical successful results count
as no progress. A changed result resets the count. Errors use the failure rules
instead.

### Deferred rules

Do not initially implement:

- special polling-tool recognition;
- alternating A/B ping-pong recognition;
- a global all-tool circuit breaker;
- post-compaction replay detection.

Add one only with a captured or reproducible ForgeClaw failure that is not
handled by the initial rules.

## Runtime Integration

### Before execution

Run `Before` after:

1. turn-profile policy;
2. `BeforeTool` modification or response handling;
3. approval hooks;
4. final effective tool-name and argument resolution.

If blocked, do not execute the tool. Create a synthetic `ToolResult` marked as
an error, append it using the original provider `tool_call_id`, emit a safe
decision event, and continue through normal model-boundary logic.

Hook-provided responses should be observed by `After` because they are the
effective result seen by the model. Calls denied by policy or approval are not
tool failures and should not affect failure/no-progress counters.

### After execution

Observe after `AfterTool` and synchronous delivery transformations have
resolved the effective `ToolResult`, but before guardrail guidance is appended
to model-visible content. Hash the filtered model-visible result if filtering
is part of the final provider contract; do not keep the filtered text after
hashing.

If `After` returns `warn`, append concise recovery guidance to the current tool
message. The annotation must not mutate the `ToolResult` used for identity or
become input to the next comparison.

### Parallel legacy loop

For one provider batch:

1. normalize calls in provider order;
2. call `Before` serially for every call;
3. execute allowed calls concurrently;
4. synthesize results for blocked calls;
5. call `After` serially in provider order;
6. append all tool messages in provider order.

Two identical calls in the same previously unseen parallel batch may both run.
Avoiding that would require speculative intra-batch dependency semantics and is
not part of the first implementation.

## Configuration

Place configuration at the most coherent tools/runtime boundary. A nested
`tools.loop_detection` block is preferred unless implementation reveals a
better existing convention.

Required controls:

- enabled;
- warnings enabled;
- hard stops enabled;
- exact-failure warning and block thresholds;
- same-tool warning and halt thresholds;
- read-only no-progress warning and block thresholds;
- bounded history/state size if it is not derived from thresholds.

Initial defaults:

| Setting | Default |
|---|---:|
| enabled | true |
| warnings | true |
| hard stops | false |
| exact failure warn | 2 |
| exact failure block | 5 |
| same-tool failure warn | 3 |
| same-tool failure halt | 8 |
| read-only no-progress warn | 2 |
| read-only no-progress block | 5 |

These follow Hermes' conservative warning-first controller. Change them only
with a ForgeClaw-specific rationale and tests.

## Events and Diagnostics

Add typed warning and block runtime events, or one typed decision event with an
action field. Payloads may contain only:

- tool name;
- argument hash;
- decision code;
- action;
- count;
- threshold.

Do not include arguments, result content, result hash unless operationally
necessary, error prose, or previews. Runtime logging should use the same safe
payload.

## Validation

Required focused coverage:

- canonical stability for nested maps, arrays, numeric values, and Unicode;
- no secret values in decisions, events, logs, or serialized controller state;
- warning and optional hard-stop thresholds;
- exact-failure, same-tool, success-reset, intervening-tool, and changed-result
  behavior;
- mutating and unknown successful repeats remain allowed;
- blocked calls preserve provider history and all tool-call IDs;
- hook-modified names, arguments, and results;
- policy/approval denials do not count as execution failures;
- steering with remaining skipped calls;
- parent/SubTurn state isolation;
- deterministic legacy parallel-batch observation;
- existing fatal-MCP fail-fast intent;
- runtime event payload safety;
- configuration defaults and invalid-value normalization.

Run controller tests first, then agent pipeline, tools loop, events, config, and
provider-history tests. Run broader agent/tool/provider tests when shared
execution contracts change.

## Delivery Strategy

Prefer one coherent pull request because controller, integration, events,
configuration, documentation, and history-validity tests form one behavioral
unit. Split only when a prerequisite refactor is independently useful and can
merge without dormant or partially wired behavior.

The implementation PR must state:

- which current boundaries were retained or replaced;
- whether fatal-MCP handling was retained, generalized, or removed;
- which built-in tools were classified read-only and why;
- which detectors were deferred;
- validation performed and any remaining gap versus Hermes/OpenClaw.

## Implemented Architecture

The initial implementation retains the two existing execution boundaries and
adapts the same pure `pkg/tools/loopguard` controller to each one:

- the primary `pkg/agent` pipeline owns event publication, synthetic blocked
  results, warning guidance, and batch control;
- the reusable legacy `pkg/tools` loop evaluates calls before worker launch
  and observes completed results serially in provider order;
- each `turnExecution` and each legacy loop invocation owns a fresh controller,
  so state is never shared across turns, SubTurns, sessions, or restarts;
- tool classification is optional registry metadata. Missing, invalid, MCP,
  and dynamic classifications resolve to `unknown`.

The existing fatal-MCP transport breaker is retained unchanged as a faster
specialized fail-fast policy. Generic loop detection uses only structured
`ToolResult.IsError` state and does not inspect error prose.

The audited read-only/idempotent set is intentionally small:

- `read_file` in byte and line modes;
- `list_dir`;
- `search_files`;
- `short_grep`.

All other built-in tools currently remain `unknown` unless they explicitly
declare mutating semantics. This fail-closed classification prevents repeated
successful output from an unclassified tool from triggering no-progress
blocking.

Configuration lives under JSON `tools.loop_detection` and environment
variables. It is excluded from the legacy flattened YAML tools view because
that format has no nested tools block. Defaults enable detection and warnings
but leave hard stops opt-in. Invalid non-positive thresholds are normalized to
safe defaults, and block thresholds are never normalized below warning
thresholds.

The controller retains only SHA-256 argument and result identities in bounded
per-turn memory. Runtime decision events and logs expose the tool name,
argument hash, action, code, count, and threshold, but no raw arguments,
results, result hashes, or error prose. Provider history still receives one
role=`tool` message with the original ID for every provider call, including
blocked calls and calls skipped after a hard stop.

The legacy loop has no runtime event sink, so it applies decisions and
model-visible guidance without publishing `agent.tool.loop_decision`. Adding a
generic event sink to that reusable package is deferred until another legacy
consumer needs lifecycle observability. Polling-specific, alternating
ping-pong, global, and post-compaction detectors remain deferred as described
above; unlike Hermes and OpenClaw, this first version does not attempt to infer
those broader stagnation patterns without a ForgeClaw trace and regression
test.
