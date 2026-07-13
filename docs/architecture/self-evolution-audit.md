# Self-Evolution Effectiveness And Safety Audit

Status: final effectiveness verdict. The initial source and production audit
was performed at `70a64648`; safeguards and evaluation infrastructure were
merged through `3225ab1a`, followed by held-out trials on 2026-07-13. The
rejected learning runtime was removed in merge `7832e23f` and the resulting
runtime was deployed and verified the same day.

## Decision

The primary question is whether the subsystem learns reusable behavior that
measurably improves future tasks. Security and mutation controls are necessary
acceptance gates, but they cannot justify retaining a subsystem that produces
no useful improvement.

The current subsystem did not improve agent outcomes. Most sampled production
drafts were generic, mismatched, duplicated, or preserved a task result instead
of teaching an operation. The four strongest plausible nutrition candidates
were then tested in paired held-out trials: zero of four improved an outcome,
and one caused a protected wrong-mutation regression in all three candidate
runs while the baseline succeeded in all three.

Self-evolution is therefore rejected for routine production use. Runtime
application and automatic deletion were removed first; recording, draft
generation, configuration, runtime events, and workspace mutation code were
then removed together. Generic trace, scenario, replay, corpus-audit, and
paired-evaluation tools remain because they are independently useful.

This audit does not propose more learning capability. It first measures whether
current drafts add value over the existing agent, then identifies where any
surviving useful path converts untrusted runtime text into durable records,
model prompts, candidate skills, workspace mutations, and lifecycle deletion.

## Initial Scope And Method

The initial audit traced these authorities and stores before the fail-safe
changes:

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

## Original Trust And Data Flow

The following flow describes the implementation at the initial `70a64648`
snapshot. None of this learning flow is reachable after `7832e23f`; it is kept
here as the historical mechanism evaluated by the audit.

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

## Production Snapshots

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

After the fail-safe merge, the effective 2026-07-13 configuration is:

| Profile | Enabled | Effective mode | Cold-path trigger |
|---|---:|---|---|
| `main` | yes | `observe` | `manual` |
| `nutrition` | yes | `observe` | `manual` |
| `spouse` | yes | `observe` | `manual` |
| `family` | no | disabled | n/a |
| `reviewer` | no | disabled | n/a |

Legacy `apply` values fail closed to `observe`, automatic application is not
reachable from the runtime, automatic deletion is prohibited, stored records
are redacted before persistence, and evolution files are owner-only. These
changes contain the original safety exposure but do not create usefulness.

After the final removal and deployment, the top-level `evolution` configuration
is absent from the root config and all five service-profile configs. The five
agent services and web launcher run the removal build, and no active workspace
contains a `state/evolution` directory. The ten legacy state directories were
moved to an owner-only archive for historical evaluation; generic evaluation
traces remain in their normal bounded-retention stores.

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
food references. They showed that the generator could sometimes emit an
actionable-looking procedure; appearance alone was not evidence of usefulness.

Observed sample yield was therefore 4/24 potentially actionable and 0/24 proven
useful. All four plausible candidates were subsequently evaluated with the
configured nutrition model, three paired runs per held-out case, deterministic
stub tools, the current production instruction baseline, and one candidate as
the only variant. The result was 0/4 beneficial and 1/4 regressed across 30
model runs. The append-only candidate caused a new meal to be logged for a
nearby additive request in 3/3 candidate trials; baseline amended the existing
meal in 3/3 trials. The remaining cases tied the baseline, sometimes with more
tool calls or latency.

The sanitized manifest, evaluator report, observations, method, hashes, and
limitations are committed under the
[2026-07-13 held-out trial](../evaluation/self-evolution-2026-07-13/README.md).

The content-free corpus counters are reproducible with `picoclaw eval evolution
corpus`. The command reports only aggregate counters, candidate IDs, target
names, and signal codes; it does not include captured task or draft content.
The paired effectiveness gate is available through `picoclaw eval evolution`.

## Findings

| ID | Severity | Finding | Mechanism and exposure | Disposition |
|---|---|---|---|---|
| EVO-001 | Critical | Raw untrusted and potentially secret text was persisted and propagated | User/final excerpts were stored without the configured redactor, mode `0644`, then sent to model operations and retained in drafts. | Closed: the learning runtime was removed and legacy stores were isolated owner-only outside active workspaces. |
| EVO-002 | Critical | Prompt injection could become durable skill instructions | Success, cluster, and draft prompts interpolated untrusted text, while automatic apply accepted clean candidates. | Closed: generation, apply, and the runtime prompt path were removed. |
| EVO-003 | Critical | Workspace containment was not symlink-safe | Apply and lifecycle could reach workspace skill paths through symlinks. | Closed: all evolution workspace mutation and lifecycle deletion code was removed. |
| EVO-004 | High | Completion and model self-judgment were not reliable success evidence | Initial success was only `status == completed`. The judge lacked delivery results, user correction/dissatisfaction, full tool failure state, and externally verified outcomes. | Closed for the rejected design by removing success judging and drafting. Any future experiment must add authoritative outcomes before capture. |
| EVO-005 | High | Clustering lacked user/channel/topic/task-family isolation | Workspace was the only durable isolation dimension, allowing semantically unrelated tasks to share evidence. | Closed for the rejected design by removing clustering. Any future experiment must declare and test isolation scope. |
| EVO-006 | Critical | `apply` had no human approval or improvement gate | Candidate meant structurally unflagged, not reviewed. | Closed: runtime apply and its configuration were removed; the old config key is now rejected. Do not restore it. |
| EVO-007 | High | Successful apply had no usable durable rollback | Backups were path-based and transaction-local, with no operator restore contract. | Closed by removing apply and its mutation code rather than building rollback for a rejected subsystem. |
| EVO-008 | Critical | Lifecycle could irreversibly delete a skill | Automatic lifecycle deletion removed files without durable recovery. | Closed: deletion transitions were first rejected, then the lifecycle implementation was removed. |
| EVO-009 | High | Improvement was not measured | Draft status and retention were not outcome comparisons. | Closed: paired evaluation and live trials measured 0/4 useful and 1/4 regressed; the learning runtime was removed. |
| EVO-010 | Medium | Logs and persisted findings could repeat sensitive summaries | Cold-path debug fields included pattern summaries and draft metadata derived from raw evidence. | Closed for active production by removing the cold path and isolating legacy state; retained generic traces follow their separate capture policy. |
| EVO-011 | High | Draft generation had low observed utility | In the 24-draft sample, 20 were unusable or generic. All four actionable-looking candidates then failed to improve held-out outcomes, and one regressed. | Closed: production capture and drafting were removed. |

## Acceptance Invariants For Any Future Learning Runtime

The current runtime has no self-evolution path. These invariants are gates for
a separately designed future experiment, not descriptions of current behavior.

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

## Delivery Plan And Status

1. Completed: landed the initial audit and threat/failure plan.
2. Completed: introduced safe record projections, redaction, owner-only
   storage, safe defaults, prompt data boundaries, and adversarial tests.
3. Completed: removed automatic deletion and automatic candidate apply. A
   durable review/rollback path was intentionally not built because the
   subsystem still had to prove usefulness first.
4. Completed: paired held-out evaluation found 0/4 useful candidates and one
   protected regression.
5. Completed: removed the current learning runtime, configuration, runtime
   events, workspace mutation, and user-facing controls while preserving the
   independent evaluation infrastructure and sanitized evidence.
6. Completed: removed evolution configuration from every deployed profile,
   deployed the removal build, restarted all services, and isolated legacy
   state outside active workspaces.

## Completion Evidence

The audit is complete when this evidence PR merges:

- safety containment, corpus diagnostics, paired evaluation, isolated live
  trials, and the final verdict are reproducible from merged code and committed
  sanitized evidence;
- the best plausible candidates measured 0/4 useful and 1/4 regressed;
- merge `7832e23f` removes the rejected runtime and its configuration while
  retaining generic replay/evaluation packages and commands;
- the deployed checkout includes that merge, all six services are active, and
  recent error-priority journals are empty;
- the root and five profile configs have no `evolution` key, active workspaces
  have no evolution state directories, and legacy state is owner-only and
  outside the runtime path.
