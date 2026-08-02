# Test Suite Performance Roadmap

Status: active implementation roadmap.

This roadmap records the August 2026 investigation into MintClaw's Go test
runtime and admits a bounded sequence of changes that reduce feedback time
without weakening correctness, durability, race, or integration coverage.

## Baseline

The investigation ran from a clean worktree at the then-current
`origin/main`, using the same build tags as the pull-request unit-test job:

```sh
go test -count=1 -tags goolm,stdjson ./...
```

The latest inspected tree contained 5,318 top-level tests in 505 test files.
An uncontended package profile identified this critical path:

| Package | Elapsed time | Primary cost |
| --- | ---: | --- |
| `pkg/agent` | 198 s | real retry delays, serial tests, durable writes |
| `pkg/cron` | 95 s | oversized concurrent persistence loop |
| `pkg/tools/fs` | 94 s | thousands of physical fixture files |
| `pkg/memory` | 88 s | hundreds of fsync-backed mutations |
| `pkg/gateway` | 41 s | repeated companion builds and real processes |
| `pkg/interactions` | 21 s | durable persistence and reload coverage |
| `pkg/seahorse` | 19 s | database and reopen coverage |
| `pkg/session` | 17 s | durable JSONL operations |

Package times are not additive because `go test ./...` runs packages in
parallel. `pkg/agent`, with 892 top-level tests and only four explicit
`t.Parallel` calls, is the principal default-job bottleneck.

A concurrent local race run increased one observed full-suite wall time to
633 seconds and caused timeout-sensitive failures that disappeared when the
affected packages were rerun alone. Completion therefore requires both lower
runtime and less sensitivity to unrelated machine load.

## Coverage Policy

Performance work must preserve these rules:

1. A test is removed only when it has no enforceable outcome or is fully
   subsumed by a stronger named test.
2. Real waiting is replaced with injected time, synchronization, or a bounded
   event signal. Assertions are not weakened to gain speed.
3. Durable storage contracts retain focused tests with real sync, rename,
   reopen, and fault behavior. Logical tests may use an explicitly injected
   no-sync test implementation.
4. Concurrency regressions retain deterministic correctness assertions and an
   appropriate `-race` execution path.
5. Process-level and vertical integration coverage may leave the default unit
   job only when another required job runs it.
6. Test parallelism is enabled only after auditing shared globals, ports,
   environment variables, process state, and persistent paths.

## Admitted Work

### 1. Remove or repair tests with no signal

- Remove `TestPipeline_CallLLM_ContextLengthError`, which accepts either
  success or failure and only logs the result. Existing context-overflow tests
  retain asserted coverage.
- Remove `TestSpawnDuringAbort_RaceCondition`, which accepts every result and
  has no bounded deadlock assertion.
- Remove `TestAsyncSubTurn_ParentFinishesEarly`, which waits five seconds and
  accepts cancellation, another error, or success. Existing finish,
  cancellation, orphan-delivery, and race tests retain the relevant contracts.
- Remove the `TestHandleReasoning/expired ctx` subtest because it uses a
  background context and duplicates the preceding Telegram publication case.
- Rewrite any additional no-signal case discovered during implementation when
  its lack of an enforceable outcome is demonstrated in the pull request.

### 2. Make timing deterministic

- Inject the LLM retry sleeper or clock so retry attempts, backoff values,
  cancellation, and event payloads can be asserted without wall-clock waits.
- Keep one end-to-end retry-loop case. Test individual network error shapes
  directly against the retry classifier instead of rerunning the full pipeline
  for every string.
- Replace fixed absence waits in reasoning, delivery, and retry tests with
  synchronous state checks or explicit completion signals.
- Configure agent-layer failure tests to avoid exercising channel retry delays
  already covered by channel-manager tests.

### 3. Reduce disk-heavy fixtures

- Reduce the no-history context fixture to the minimum history needed to prove
  that session assembly is skipped.
- Extract or expose a pure search-result rendering seam so byte and count
  truncation contracts can be tested from in-memory rows. Retain a small
  filesystem integration case for discovery and ignore behavior.
- Replace the cron concurrency stress loop with a bounded deterministic test
  that checks every error and final state.
- Reduce memory concurrency cardinality or use an injected no-sync test store.
  Preserve the historical concurrent add/read and summarize/truncate
  invariants.
- Keep a small, explicit set of real-durability tests for file sync, atomic
  replacement, reopen, recovery, and injected failure stages.

### 4. Separate integration and race coverage

- Build the companion binary once per integration execution rather than once
  per vertical test.
- Keep the gateway invocation and file-transfer vertical slices, but move them
  out of the default unit-test critical path only after a required Go
  integration job runs them.
- Add a targeted race path for the concurrency-sensitive packages or named
  regression groups changed by this roadmap.
- Do not move a test merely because it is slow; movement requires a clear
  unit/integration boundary and retained required execution.

### 5. Add safe parallelism and timing visibility

- Add `t.Parallel` only to audited tests with isolated dependencies.
- Publish per-package and slowest-test timing from CI so future regressions are
  visible.
- Establish timing budgets only after stable runner data exists. Timing
  reporting must not introduce flaky hard failures.

## Delivery Sequence

Implementation should use focused pull requests so performance changes remain
reviewable:

1. no-signal removal and deterministic agent timing;
2. disk-heavy search, cron, memory, and persistence fixtures;
3. integration/race job separation, shared companion build, audited
   parallelism, and timing reporting.

Dependent pull requests start from the merged predecessor and the latest
`origin/main`. Each pull request records its own before/after measurements and
lists remaining roadmap work.

## Done Criteria

This roadmap is complete only when all of the following are true:

- [ ] Every listed no-signal or duplicate test is removed or replaced by a
  stronger asserted test.
- [ ] LLM retry tests do not incur real multi-second backoff waits and still
  assert attempt count, delay sequence, classification, cancellation, and
  terminal outcome.
- [ ] Search truncation tests preserve byte-limit, count-limit, omitted-count,
  ignored-path, complete-row, and UTF-8 contracts without creating thousands
  of files.
- [ ] Cron and memory concurrency tests check operation errors and final
  invariants with bounded fixture sizes.
- [ ] Real durability remains covered for sync, atomic replacement, reopen,
  recovery, and relevant failure stages.
- [ ] Gateway companion vertical coverage is still required in CI, with
  redundant binary builds eliminated.
- [ ] A targeted race path covers the concurrency regressions retained by this
  work.
- [ ] Safe parallelism is audited rather than applied indiscriminately.
- [ ] CI reports package and slow-test timing without a flaky wall-clock gate.
- [ ] `pkg/agent` completes in at most 130 seconds in an uncached local profile
  comparable to the 198-second baseline.
- [ ] A warm CI-equivalent default Go test run completes in at most 120 seconds
  where the runner permits. If host or cold-build cost makes this threshold
  unattainable, the final pull request must provide separated compile and test
  evidence and may not weaken coverage to meet the number.
- [ ] The CI-equivalent suite, focused race tests, formatting, lint, and
  documentation validation pass.
- [ ] All admitted implementation pull requests are merged and their final
  heads are present on `origin/main`.

## Stop Conditions

This roadmap is not permission for broad production refactoring or arbitrary
test deletion. Work stops and returns to review if meeting a timing target
would require weakening a safety contract, making a formerly required test
optional, changing production durability semantics, or expanding beyond test
timing and the narrow testability seams described above.
