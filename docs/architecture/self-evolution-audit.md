# Self-Evolution Effectiveness And Safety Audit

Status: initial source and production evidence audit against
`bogdanovich/forgeclaw:main` at `70a64648`.

## Decision

The primary question is whether the subsystem learns reusable behavior that
measurably improves future tasks. Security and mutation controls are necessary
acceptance gates, but they cannot justify retaining a subsystem that produces
no useful improvement.

The current subsystem has not demonstrated that it improves agent outcomes. It
occasionally creates a concrete, potentially reusable procedure, but most
sampled production drafts are generic, mismatched, duplicated, or preserve a
task result instead of teaching an operation. No draft has a held-out baseline,
post-draft comparison, or production outcome attribution.

Self-evolution is therefore not approved for automatic application. `observe`
is the safe default. `draft` is useful only as an evaluation corpus until both
quality and security gates land. `apply` must remain unavailable to production
until a held-out replay gate demonstrates improvement and rollback is an
operator-usable, durable operation.

This audit does not propose more learning capability. It first measures whether
current drafts add value over the existing agent, then identifies where any
surviving useful path converts untrusted runtime text into durable records,
model prompts, candidate skills, workspace mutations, and lifecycle deletion.

## Scope And Method

The audit traces these authorities and stores:

| Boundary | Current authority | Persistence |
|---|---|---|
| Turn completion | `agent.TurnEndPayload.Status` | task learning record |
| Success admission | LLM judge, heuristic fallback | rewritten only in cold-path memory |
| Task clustering | LLM clusterer, heuristic fallback | pattern JSONL |
| Draft generation | LLM generator, deterministic fallback | `skill-drafts.json` |
| Draft review | structural validation and substring scanner | draft status/findings |
| Apply | `mode == apply` and candidate status | workspace `SKILL.md`, backup, profile |
| Lifecycle | idle age, retention score, origin | profile and possible file deletion |
| Evaluation | evolution trace transitions | deterministic projection |

Evidence sources include `pkg/evolution`, `pkg/agent/evolution_bridge.go`, config
defaults and migrations, evolution tests, replay/evaluation contracts, and a
content-free inspection of active profile configuration and state metadata.
Production content was not printed or copied into this document.

## Trust And Data Flow

1. A runtime turn-end event supplies user message, final content, completion
   status, tool summaries, skill names, session, agent, and workspace.
2. `Runtime.FinalizeTurn` treats status `completed` as initial success. It stores
   up to 160 runes of the user message and 1,200 runes of final output as plain
   text. The configured sensitive-data filter is not called.
3. In `draft` or `apply`, the cold path sends stored summaries and final-output
   excerpts to success judging, clustering, and draft generation. These prompts
   interpolate evidence as instructions without a data boundary.
4. A model response or deterministic fallback creates a draft. `ReviewDraft`
   validates shape and scans a few literal secret markers. A draft with no
   findings remains `candidate`.
5. In `apply`, a `candidate` is applied automatically. Human acceptance is not
   represented as a required authority; `accepted` is assigned after mutation.
6. Apply writes workspace skill content atomically and makes a backup for an
   existing file. The rollback closure is used for failures in the same apply
   transaction, not exposed as a later operator action.
7. Every draft/apply cold-path run also performs lifecycle maintenance. An
   evolved archived profile can transition to `deleted`, which removes
   `workspace/skills/NAME/SKILL.md` before saving the new profile state.

Untrusted fields are therefore both durable data and prompt input. A model's
JSON conformance does not make its proposed instructions trusted.

## Production Snapshot

Snapshot date: 2026-07-12 America/Los_Angeles.

| Profile | Mode | Task records | Candidate drafts | Profiles |
|---|---:|---:|---:|---:|
| `main` | `draft`, after each turn | 2,000 | 71 | 15 manual |
| `nutrition` | `draft`, after each turn | 720 | 21 | 3 manual |
| `spouse` | `draft`, after each turn | 269 | 29 | 11 manual |
| `family` | disabled | none | none | none |
| `reviewer` | disabled | none | none | none |

Task, pattern, draft, and profile files are mode `0644`. A bounded filename-only
scan found secret-like markers in three `main` evolution files and one `spouse`
file. This is not proof that every match is a live credential; it proves that
the current store and scanner cannot establish non-disclosure. Existing content
must be treated as sensitive and migrated or removed after a safe replacement
format exists.

The active profiles are manual-origin, so automatic lifecycle deletion does not
currently target those profile entries. This reduces immediate exposure but
does not make the code path safe for newly evolved skills.

## Does It Produce Useful Drafts?

There are 121 production candidates and no accepted production drafts. The
subsystem therefore provides evidence about generation output, not evidence
that generated skills improve later turns.

Corpus-level quality proxies:

| Profile | Candidates | Unique targets | Target-collision drafts | Generic shortcut template | Copies prior final output | Body over 5,000 chars |
|---|---:|---:|---:|---:|---:|---:|
| `main` | 71 | 59 | 15 | 70 | 0 | 20 |
| `nutrition` | 21 | 20 | 2 | 0 | 6 | 8 |
| `spouse` | 29 | 27 | 4 | 29 | 0 | 7 |

The generic shortcut template repeats a learned path and source-skill excerpts
but does not synthesize an executable procedure. In `main` and `spouse`, these
paths often contain many unrelated skills because the turn's loaded/attempted
skill context became the winning path. Output-copying drafts teach a previous
answer as the procedure; one production `/model list` candidate preserves a
failed refusal as the behavior to repeat.

A deterministic sample selected the first eight secret-pattern-free draft IDs
from each enabled profile. The rubric was applied without using target status:

| Score | Meaning | Count |
|---|---|---:|
| 0 | incoherent, contradictory, failed behavior, or task/evidence mismatch | 7 |
| 1 | restates the trigger or attaches generic/irrelevant routing; no usable procedure | 13 |
| 2 | plausible procedure but materially incomplete | 0 |
| 3 | actionable and internally coherent, but duplicated or not outcome-tested | 4 |
| 4 | actionable, novel, and proven better on held-out tasks | 0 |

The four score-3 candidates are nutrition procedures for correcting label-based
meal data, preserving append-only meal semantics, and storing reusable packaged
food references. They demonstrate that useful extraction is possible. They do
not demonstrate improvement: some overlap each other and existing nutrition
skills, none names a verified runtime tool contract, and none was tested against
baseline on unseen tasks.

Observed sample yield is therefore 4/24 potentially actionable and 0/24 proven
useful. This sample is diagnostic rather than a statistical quality estimate,
but it is sufficient to reject automatic apply and to require an explicit
effectiveness harness before retaining a mutation feature.

The content-free corpus counters are reproducible with `picoclaw eval evolution
corpus`. The command reports only aggregate counters, candidate IDs, target
names, and signal codes; it does not include captured task or draft content.
The paired effectiveness gate is available through `picoclaw eval evolution`.

## Findings

| ID | Severity | Finding | Mechanism and exposure | Disposition |
|---|---|---|---|---|
| EVO-001 | Critical | Raw untrusted and potentially secret text is persisted and propagated | User/final excerpts are stored without the configured redactor, mode `0644`, then sent to three model operations and retained in drafts. Production contains thousands of records and secret-like matches. | Fix now; default production to observe until migrated. |
| EVO-002 | Critical | Prompt injection can become durable skill instructions | Success, cluster, and draft prompts interpolate untrusted text without delimiting it as non-authoritative. Review scans only a few literals and does not detect instruction injection. `apply` accepts any clean candidate automatically. | Fix prompt/data boundary and policy; adversarial tests; keep apply unavailable. |
| EVO-003 | Critical | Workspace containment is not symlink-safe | Apply and lifecycle build lexical workspace paths, then read/write/remove through them. A workspace skill directory may be a symlink, as real deployments commonly use for shared skills, allowing mutation or deletion outside the workspace. | Fix now with no-follow containment and protected-source rules. |
| EVO-004 | High | Completion and model self-judgment are not reliable success evidence | Initial success is only `status == completed`. The judge sees summary, final output, and used skill names, but no delivery result, user correction/dissatisfaction, full tool failure state, or externally verified outcome. Its untrusted evidence can instruct the judge; fallback accepts any non-empty completed output. | Default fail-closed for drafting; add authoritative outcome evidence and tests. |
| EVO-005 | High | Clustering lacks user/channel/topic/task-family isolation | Workspace is the only durable isolation dimension. Records do not preserve user, channel, chat, or topic authority. Shared skills/tool paths can cluster semantically unrelated tasks, and production minimum count is two. | Add explicit scope and stronger admission; test contamination. |
| EVO-006 | Critical | `apply` has no human approval gate or improvement gate | Candidate means structurally unflagged, not reviewed. `mode == apply` writes it immediately and then labels it accepted. No held-out baseline, regression check, approval identity, or policy signature is required. | Disable/remove automatic apply until explicit review and replay gates exist. |
| EVO-007 | High | Successful apply has no usable durable rollback | Backups are path-based and transaction-local. Profiles record version/draft IDs but no backup reference, content digest, restore command, concurrency precondition, or operator-visible rollback status. | Add durable version manifest and explicit restore operation before any apply survives. |
| EVO-008 | Critical | Lifecycle can irreversibly delete a skill | After age/score transitions, deletion removes `SKILL.md` before profile save, creates no deletion backup, has no approval, and can traverse a symlink. A later state-write failure leaves the file deleted with stale profile state. | Remove automatic deletion; use reviewable archive/tombstone only. |
| EVO-009 | High | Improvement is not measured | Draft creation/application, use count, and retention score are not held-out outcome comparisons. No baseline, replay suite, regression threshold, or automatic rollback criterion exists. | Apply is unproven; define evaluation contract before reconsideration. |
| EVO-010 | Medium | Logs and persisted findings can repeat sensitive summaries | Cold-path debug fields include pattern summaries and draft metadata derived from raw evidence. Rollback reasons and scan findings are persisted without a common redaction contract. | Centralize sanitized projections and logging. |
| EVO-011 | High | Draft generation has low observed utility | In the 24-draft sample, 20 were unusable or generic, four were actionable but untested, and none proved improvement. Corpus proxies show repeated targets, oversized bodies, irrelevant all-skill paths, and copied final outputs. | Build a held-out baseline harness; stop routine after-turn drafting unless yield justifies it. |

## Required Safety Invariants

### Records And Secrets

- Production records use a versioned, allowlisted projection.
- Secret filtering happens before persistence, logs, or model prompts.
- Record files and drafts use owner-only permissions.
- Canary secrets never appear in serialized records, prompts captured by test
  providers, drafts, findings, logs, profiles, or traces.
- Existing raw stores are quarantined or migrated without re-exposing content.

### Success And Clustering

- A completed model turn is not sufficient success evidence.
- Missing delivery, failed required tools, explicit correction/dissatisfaction,
  unresolved task state, and unknown outcome are ineligible for drafting.
- Judge input is delimited as untrusted data and model output cannot upgrade an
  unknown authoritative outcome to success.
- Clusters cannot cross workspace, profile, user/chat/topic policy scope, or an
  explicit task-family boundary.
- Low-sample and ambiguous clusters remain observations, not draft sources.

### Drafts And Application

- Generated content is untrusted even when valid JSON.
- Draft scanning uses the same structural secret filter plus instruction and
  path policy; unknown cases quarantine.
- Human review is explicit, attributable, bound to draft content digest, and
  required before any mutation.
- Apply verifies a held-out replay result against baseline and refuses on
  missing evidence or regression.
- Draft admission requires a reusable operation, explicit executable steps,
  novelty versus existing skills, evidence consistency, and held-out benefit;
  trigger restatement and prior-output copying fail.
- Writes are no-follow, workspace-contained, protected-skill aware, locked, and
  compare the reviewed base digest before atomic replacement.

### Rollback And Deletion

- Every mutation has a durable before/after digest and recoverable content
  version before workspace change.
- Operator rollback is explicit, idempotent, observable, restart-safe, and
  refuses stale/concurrent state.
- Evolution never automatically deletes a skill file. Lifecycle may recommend
  archival or create a reviewable tombstone without mutating deployable content.
- Symlink, traversal, global, builtin, shared, manual, and invalid-name targets
  fail closed.

## Reproducible Test Plan

1. Seed direct, indirect, encoded, URL, environment, bearer, cookie, private-key,
   and tool-output canaries; assert absence across every projection and file.
2. Submit evidence containing instructions to approve success, combine unrelated
   records, override the draft schema, alter protected skills, or conceal a
   secret. Capture model prompts and prove the evidence is data-only; outputs
   remain quarantined.
3. Exercise completed-but-undelivered, partial tool failure, no-op, correction,
   user rejection, retry, heartbeat, and unknown-outcome turns.
4. Attempt clusters across workspace, profile, session owner, channel/chat/topic,
   task family, and shared skill path; test two-record adversarial near matches.
5. Attempt create/append/replace/merge and lifecycle deletion against symlinks,
   traversal names, manual/global/builtin/shared skills, stale base digests, and
   concurrent writers.
6. Interrupt each mutation before backup, after backup, after workspace write,
   and before profile commit; restart and prove deterministic recovery.
7. Compare candidate and baseline on held-out replay fixtures. Missing evidence,
   variance, or any protected-invariant regression rejects application.
8. Score a versioned, stratified draft corpus using the 0-4 rubric above. Track
   actionable yield, target duplication, existing-skill overlap, procedure
   completeness, evidence contradiction, and held-out win/regression rates.

## Evaluation Contract

Replay can deterministically evaluate provenance completeness, secret-canary
absence, approval/digest binding, workspace containment, deletion prohibition,
rollback completeness, and protected-invariant regression. It cannot by itself
judge general answer usefulness.

Effectiveness evaluation must execute the same held-out task twice in an
isolated scenario: baseline skill set and baseline plus candidate. It records
required tool/state outcomes, delivery correctness, protected regressions,
latency/tool-call/token deltas, and a task-specific result rubric. A candidate
is useful only when it improves the declared outcome without a protected
regression. Generation frequency, candidate status, or model confidence are not
effectiveness metrics.

Any semantic grader must be opt-in, use a declared held-out rubric, bounded
provider/cost, repeated trials with variance, and never override deterministic
safety failures. No such rubric currently exists, so semantic grading and
automatic apply remain deferred.

## Delivery Plan

1. Land this audit and threat/failure plan without behavior changes.
2. Introduce safe record projections, redaction, owner-only storage, safe
   defaults, prompt data boundaries, and adversarial tests. Quarantine existing
   raw stores before re-enabling draft generation.
3. Remove automatic deletion and automatic candidate apply; add no-follow
   containment and, only if justified, durable review/rollback primitives.
4. Use `picoclaw eval evolution` to evaluate paired baseline/candidate trials
   with held-out record isolation, explicit task criteria, protected invariants,
   matching seeds, and evidence references. Retain routine drafting only if
   measured yield justifies its storage, model cost, and maintenance burden.
   Otherwise keep a bounded manual experiment or remove the subsystem.

## Completion Evidence

The audit is complete only when every finding has a merged disposition, active
profiles use the approved policy, existing sensitive state is handled, focused
and full tagged tests pass, and a merged-main/deployed-runtime audit confirms
the effective configuration and absence of secret canaries.
