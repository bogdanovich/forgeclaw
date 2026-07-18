# Memory system

ForgeClaw memory is a set of deliberately separate layers. They have different
owners, retention rules, privacy boundaries, and failure modes. They must not be
collapsed into one universal memory database.

## Layers and sources of truth

| Layer | Source of truth | Prompt behavior | Owner |
| --- | --- | --- | --- |
| Stable user facts | `USER.md` and curated `memory/MEMORY.md` entries | Bounded bootstrap context | Workspace operator and curated-memory tool |
| Stable agent and environment facts | `AGENT.md`, `SOUL.md`, `IDENTITY.md`, and other bootstrap files | Bounded bootstrap context | Workspace operator |
| Episodic notes | `memory/YYYYMM/YYYYMMDD.md` | Bounded recent window | Agent and workspace tools |
| Canonical session history | JSONL session store | Selected by the active context manager | Session subsystem |
| Derived recall and summaries | Seahorse SQLite | Bounded assembly and explicit retrieval tools | Seahorse; rebuildable from canonical history |
| Procedures | Installed `SKILL.md` files | Catalog plus explicit skill loading | Skill subsystem and workspace operator |
| Goals and task state | Goal and task stores | Explicit runtime context or tool access | Goal and task subsystems |

JSONL remains canonical for conversation history. Seahorse is a derived index
and summary store and may be rebuilt. Markdown memory files are workspace
knowledge, not a substitute for canonical history. Skills contain procedures,
not user facts. Goals and task records contain active work state, not durable
personal memory.

## Prompt budgets

Every prompt-injected memory source has an independent, configurable byte or
token budget. The complete model request remains subject to the Pipeline's
outer context-window budget.

Curated stable memory keeps its beginning and end when truncation is required:
the beginning usually carries headings and global rules, while the end usually
contains the newest additions. Episodic notes are selected newest first and
truncate older content before newer content. Truncation is deterministic and
visible in the rendered prompt; it must never silently make an oversized model
request.

`USER.md` and other bootstrap files follow the bootstrap-file budget policy.
Seahorse history and summary budgets are separate because they operate on
conversation records rather than workspace Markdown.

## Cache dependencies

The system-prompt cache tracks every file that can affect the rendered static
prompt:

- bootstrap files;
- curated `memory/MEMORY.md`;
- every selected daily-note path, including files that do not yet exist;
- the current local date, so midnight rollover changes the selected paths;
- skill roots and files.

A write through a first-class memory API invalidates the cache immediately.
External file edits are detected through the same existence and modification
baseline used for bootstrap files. A date rollover invalidates the cache even
when no file timestamp changes.

## Curated memory mutations

Stable-memory mutations use a small semantic contract:

- `add`: add a new stable entry only when its normalized content is absent;
- `replace`: replace one unambiguous existing entry;
- `remove`: remove one unambiguous existing entry.

Mutations are atomic, preserve owner-only file permissions, and reject
ambiguous replacements or removals. Duplicate additions are deterministic
no-ops. Runtime audit events record operation, target, outcome, and content
hashes or lengths, never raw private content.

Generic filesystem tools remain available for ordinary workspace files. The
curated-memory tool is the preferred path for durable stable facts because it
provides correction, deduplication, audit, and cache-invalidation semantics.

## Corrections and deletion

An explicit user correction supersedes the affected stable fact; it is not
appended as a competing fact. Deletion removes the curated entry and must not
be reconstructed from derived Seahorse summaries without a new explicit user
statement. Canonical session history remains an audit record unless the user
also requests session deletion through the session subsystem.

Episodic notes may retain what happened at the time, but prompt and retrieval
surfaces must prefer the corrected stable fact when both are relevant.

## Provenance and privacy

Prompt Markdown is scoped to one workspace. Seahorse retrieval is additionally
scoped by trusted runtime provenance; the model never supplies route keys,
agent IDs, sender IDs, or conversation IDs.

The runtime resolves these retrieval boundaries:

- `current_epoch`: the active lifecycle epoch;
- `conversation`: epochs with the same trusted route scope and agent;
- `workspace`: conversations owned by the current agent in the workspace.

An operator-configured maximum limits which boundary a model may request. The
safe multi-user default does not permit workspace-wide recall. Personal
single-user workspaces may opt in to `workspace`. Missing or conflicting
provenance fails closed, and `short_expand` enforces the same boundary used by
`short_grep`.

Session dimensions remain the primary isolation control. Include `sender` when
different users in one room must not share a routed conversation. Retrieval
policy is defense in depth, not a replacement for correct session scope.

## Failure behavior

- Missing memory files contribute no prompt content.
- Unreadable files are omitted and reported without corrupting cache state.
- An over-budget mandatory prompt fails before provider invocation when the
  outer model context cannot fit.
- Failed curated-memory writes leave the original file intact and emit a
  failed audit outcome.
- Seahorse-disabled operation retains Markdown memory and canonical JSONL
  history but has no derived historical retrieval tools.
- Broader retrieval without trusted provenance or operator permission fails
  closed with an actionable tool error.

## Evaluation contract

Deterministic scenarios cover:

- stale stable facts corrected without duplicates;
- explicit deletion;
- oversized curated and episodic memory;
- daily-note modification and local-date rollover;
- cross-user and cross-conversation recall isolation;
- operation with Seahorse disabled.

Scenarios assert rendered prompt content, tool outcomes, retrieval boundaries,
and audit events. They avoid assertions on private helper structure.

## Implementation status

Implemented:

- canonical JSONL history and Seahorse reconciliation;
- trusted `current_epoch`, `conversation`, and `workspace` resolution;
- bounded Seahorse retrieval output;
- absolute Seahorse history and summary budgets;
- tool-result projection and retention policy;
- bounded Markdown prompt memory and complete daily-note cache dependencies.

Planned by the follow-up implementation series:

- first-class curated-memory mutations and audit events;
- operator-configured maximum Seahorse retrieval scope;
- deterministic memory-specific evaluation scenarios.
