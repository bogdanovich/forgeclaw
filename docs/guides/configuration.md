# ⚙️ Configuration Guide

> Back to [README](../README.md)

## ⚙️ Configuration

Config file: `~/.picoclaw/config.json`

> **Security Configuration:** For storing API keys, tokens, and other sensitive data, see the [Security Configuration Guide](../security/security_configuration.md).

### Environment Variables

You can override default paths using environment variables. This is useful for portable installations, containerized deployments, or running picoclaw as a system service. These variables are independent and control different paths.

| Variable          | Description                                                                                                                             | Default Path              |
|-------------------|-----------------------------------------------------------------------------------------------------------------------------------------|---------------------------|
| `PICOCLAW_CONFIG` | Overrides the path to the configuration file. This directly tells picoclaw which `config.json` to load, ignoring all other locations. | `~/.picoclaw/config.json` |
| `PICOCLAW_HOME`   | Overrides the root directory for picoclaw data. This changes the default location of the `workspace` and other data directories.          | `~/.picoclaw`             |

**Examples:**

```bash
# Run picoclaw using a specific config file
# The workspace path will be read from within that config file
PICOCLAW_CONFIG=/etc/picoclaw/production.json picoclaw gateway

# Run picoclaw with all its data stored in /opt/picoclaw
# Config will be loaded from the default ~/.picoclaw/config.json
# Workspace will be created at /opt/picoclaw/workspace
PICOCLAW_HOME=/opt/picoclaw picoclaw agent

# Use both for a fully customized setup
PICOCLAW_HOME=/srv/picoclaw PICOCLAW_CONFIG=/srv/picoclaw/main.json picoclaw gateway
```

### Gateway Log Level

`gateway.log_level` controls Gateway log verbosity and is configurable in `config.json`.

```json
{
  "gateway": {
    "log_level": "warn"
  }
}
```

When omitted, the default is `warn`. Supported values: `debug`, `info`, `warn`, `error`, `fatal`.

You can also override this with the environment variable `PICOCLAW_LOG_LEVEL`.

### Workspace Layout

PicoClaw stores data in your configured workspace (default: `~/.picoclaw/workspace`):

```
~/.picoclaw/workspace/
├── sessions/          # Conversation sessions and history
├── memory/           # Long-term memory (MEMORY.md)
├── state/            # Persistent state (last channel, durable ingress spool, etc.)
├── cron/             # Scheduled jobs database
├── skills/           # Custom skills
├── AGENT.md          # Agent behavior guide
├── HEARTBEAT.md      # Periodic task prompts (checked every 30 min)
├── IDENTITY.md       # Agent identity
├── SOUL.md           # Agent soul
└── USER.md           # User preferences
```

> **Note:** Changes to `AGENT.md`, `SOUL.md`, `USER.md`, `memory/MEMORY.md`, and selected daily notes are automatically detected at runtime. The daily-note selection also refreshes when the local date changes. You do **not** need to restart the gateway after editing these files.

### Prompt Memory Budgets

Workspace Markdown memory is independently bounded before it is added to the system prompt:

```json
{
  "agents": {
    "defaults": {
      "prompt_memory": {
        "long_term_max_bytes": 32768,
        "daily_notes_max_bytes": 16384,
        "recent_days": 3
      }
    }
  }
}
```

- `long_term_max_bytes` bounds `memory/MEMORY.md`. Truncation preserves the beginning and end.
- `daily_notes_max_bytes` bounds the combined recent daily notes. Newer notes are retained before older notes.
- `recent_days` selects up to 31 local calendar days, including note paths that do not exist yet.

Zero or omitted values use the defaults shown above. Truncation is UTF-8 safe and inserts a visible marker. These limits
are separate from Seahorse history and summary budgets.

The native `memory` tool is enabled by default through `tools.memory.enabled`. It provides semantic `add`, `replace`,
and `remove` operations for `memory/MEMORY.md`. Duplicate additions are no-ops; replacements and removals require one
exact logical line or block match. Successful writes are atomic, invalidate the prompt cache, and emit
`agent.memory.mutation` audit metadata without raw memory content. Use ordinary filesystem tools for other workspace
files and daily notes.

### Tool-Loop Detection

`tools.loop_detection` detects repeated tool failures and repeated read-only
calls that return no new information. Warnings are enabled by default. Hard
stops are opt-in: when enabled, a blocked call is returned to the model as a
synthetic tool error so it can choose another strategy without breaking tool
call/result history.

```json
{
  "tools": {
    "loop_detection": {
      "enabled": true,
      "warnings_enabled": true,
      "hard_stops_enabled": false,
      "exact_failure_warn": 2,
      "exact_failure_block": 5,
      "same_tool_failure_warn": 3,
      "same_tool_failure_halt": 8,
      "no_progress_warn": 2,
      "no_progress_block": 5,
      "max_signatures": 64
    }
  }
}
```

| Field | Default | Description |
|-------|---------|-------------|
| `enabled` | `true` | Enables per-turn tool-loop observation. |
| `warnings_enabled` | `true` | Adds model-visible recovery guidance before blocking thresholds. |
| `hard_stops_enabled` | `false` | Blocks repeated calls after the configured threshold. |
| `exact_failure_warn` / `exact_failure_block` | `2` / `5` | Thresholds for the same tool and canonical argument hash failing repeatedly. |
| `same_tool_failure_warn` / `same_tool_failure_halt` | `3` / `8` | Thresholds for consecutive failures from one tool even when arguments change. |
| `no_progress_warn` / `no_progress_block` | `2` / `5` | Thresholds for explicitly read-only tools returning the same result for the same arguments. |
| `max_signatures` | `64` | Maximum call signatures retained within one turn. |

Arguments and results are represented internally by SHA-256 identities; raw
values are not retained in detector state or loop-decision events. Successful
repeated output from unclassified, MCP, dynamic, or mutating tools is never
treated as read-only no progress. Current audited read-only tools are
`read_file`, `list_dir`, `search_files`, and `short_grep`.

### Evaluation Trace Capture

The `evaluation.trace_capture` block controls bounded replay/evaluation trace
recording. Capture is disabled by default and enabling evaluation commands does
not enable recording.

```json
{
  "evaluation": {
    "trace_capture": {
      "enabled": false,
      "content_mode": "metadata_only",
      "state_dir": "",
      "max_trace_bytes": 2097152,
      "max_records": 2000,
      "max_record_bytes": 16384,
      "max_corrections": 8,
      "retention_hours": 24,
      "max_traces": 100
    }
  }
}
```

| Field | Default | Description |
|-------|---------|-------------|
| `enabled` | `false` | Explicitly enables production trace capture. Evaluation commands do not enable recording. |
| `content_mode` | `metadata_only` | `metadata_only` stores safe counts, statuses, hashes, and IDs. `redacted_content` permits explicitly allowlisted filtered content. Runtime configuration never accepts fixture mode. |
| `state_dir` | `""` | Optional trace directory. Empty selects the workspace evaluation state directory. |
| `max_trace_bytes` | `2097152` | Soft serialized size limit for one trace. Compiled hard ceilings still apply. |
| `max_records` | `2000` | Maximum normalized records per trace. |
| `max_record_bytes` | `16384` | Maximum redacted JSON payload size for one record. |
| `max_corrections` | `8` | Maximum explicit correction annotations. Corrections are never inferred from ordinary follow-ups. |
| `retention_hours` | `24` | Default trace retention period. |
| `max_traces` | `100` | Maximum retained traces per workspace. |

Trace files use owner-only permissions and atomic writes. Raw runtime-event
payloads, arbitrary attributes, credentials, provider options, and unrestricted
errors are not valid capture inputs. See
[`../architecture/replay-evaluation.md`](../architecture/replay-evaluation.md)
for the security and replay-isolation contract.

### Task Registry Retention

Each workspace stores durable async task state in
`state/task_registry.json`. The registry is bounded by age, record count,
event count, and its exact serialized snapshot size:

```json
{
  "task_registry": {
    "terminal_retention_hours": 168,
    "max_records": 1000,
    "max_events": 5000,
    "max_snapshot_bytes": 2097152
  }
}
```

| Field | Default | Description |
|-------|---------|-------------|
| `terminal_retention_hours` | `168` | Retention period for terminal tasks after final delivery. |
| `max_records` | `1000` | Maximum retained task projections. |
| `max_events` | `5000` | Maximum retained audit events. |
| `max_snapshot_bytes` | `2097152` | Maximum serialized registry size (2 MiB). Events are compacted first, then oldest eligible terminal records. |

Active, non-terminal, and terminal tasks that still require delivery are never
removed by retention. A registry may therefore remain above the byte limit; at
startup PicoClaw logs a warning that only protected records remain. Zero or
omitted values use the built-in defaults.

See [`replay-evaluation.md`](replay-evaluation.md) for trace evaluation,
fixture validation, CLI output, and scenario safety.
For a task-oriented introduction, see the
[`Replay and Evaluation: Practical Overview`](replay-evaluation-overview.md).

When `state_dir` is empty, traces are written under
`WORKSPACE/state/evaluation/traces`. Relative custom paths are resolved from the
workspace; absolute paths are used directly with a `traces` child directory.
Root turns and long-lived tasks receive separate linked traces so async task
completion does not keep a turn recorder open indefinitely.

### Request Context Policy

`turn_profile` is an optional request context policy under `agents.defaults.turn_profile`. Leave it unset or set `"enabled": false` to keep PicoClaw's normal behavior. When `"enabled": true`, the same policy applies to every new turn.

Each block uses the same `mode` values:

| Mode | Meaning |
| --- | --- |
| `default` | Keep PicoClaw's normal behavior for that block. Missing blocks and missing `mode` fields are treated as `default`. |
| `off` | Disable that block for the turn. |
| `custom` | Use an allow list. In this version, `custom` is supported only for `skills` and `tools`; using it for `history` or `system_prompt` is a validation error. |

Profile blocks:

| Block | What it controls |
| --- | --- |
| `history` | Whether the turn reads prior session history and summary, writes user/assistant/tool messages, ingests context, and runs compaction or summarization. |
| `system_prompt` | Whether PicoClaw injects its default identity, workspace instructions, memory, runtime context, and summary. External request system prompts are still allowed when this is `off`. |
| `skills` | Whether the skill catalog and active skill prompt content are loaded. `custom.allow` keeps only the listed skill names in prompt context. |
| `tools` | Which callable tools are exposed to the model and allowed at execution time. `custom.allow` keeps only listed registered tool names. |

When `system_prompt.mode` is `off`, tools are still visible, and no external system prompt is supplied, PicoClaw uses its existing tool-use rule as the minimal fallback prompt. If `tools.mode` is `off`, no fallback prompt is added.

Example clean web policy:

```json
{
  "agents": {
    "defaults": {
      "turn_profile": {
        "enabled": true,
        "history": { "mode": "off" },
        "system_prompt": { "mode": "off" },
        "skills": { "mode": "off" },
        "tools": {
          "mode": "custom",
          "allow": ["web_search", "web_fetch"]
        }
      }
    }
  }
}
```

### Web launcher dashboard

**picoclaw-launcher** serves a browser UI that requires password sign-in first. On first run, open `/launcher-setup` to create the dashboard password. Later manual sign-ins use `/launcher-login`.

- **Config file**: Same directory as `config.json` (or the file pointed to by `PICOCLAW_CONFIG`). The launcher-specific file is `launcher-config.json`.
- **Password storage**: On supported platforms, the password is stored as a bcrypt hash in `launcher-auth.db`. On platforms where the SQLite password store is unavailable, the bcrypt hash is stored in `launcher-config.json`.
- **Legacy migration**: Older `launcher_token` values are migrated once into password login and removed from saved launcher config.
- **Local auto-login**: When the launcher auto-opens a local browser after startup, it uses a one-shot loopback-only bootstrap endpoint to set the session cookie automatically.
- **Unsupported auth paths**: URL token login (`?token=...`), `PICOCLAW_LAUNCHER_TOKEN`, and `Authorization: Bearer` dashboard auth are no longer supported.
- **Sign-out**: Use **`POST /api/auth/logout`** with **`Content-Type: application/json`** (body may be `{}`). Do not rely on a GET URL for logout (CSRF-safe pattern).
- **Brute-force**: **`POST /api/auth/login`** is **rate-limited per client IP per minute** (HTTP 429 when exceeded).
- **Session lifetime**: The HttpOnly session cookie lasts about **31 days** by default, but sessions are invalidated when the launcher process restarts.

### Skill Sources

By default, skills are loaded from:

1. `~/.picoclaw/workspace/skills` (workspace)
2. `~/.picoclaw/skills` (global)
3. `<binary-embedded-path>/skills` (builtin, set at build time)

For advanced/test setups, you can override the builtin skills root with:

```bash
export PICOCLAW_BUILTIN_SKILLS=/path/to/skills
```

### Using Skills From Chat Channels

Once skills are installed, and MCP servers are configured, you can inspect and force them directly from a chat channel:

- `/model` shows the current effective model for the current chat.
- `/model use <name>` applies a conversation-scoped model override for the current chat.
- `/model clear` removes that conversation-scoped override.
- `/show model` displays the effective model, including any active conversation override.
- `/list models` shows the configured model aliases available for `/model use <name>`.
- `/list skills` shows the installed skill names available to the current agent.
- `/list mcp` shows configured MCP servers with enabled/deferred/connected status.
- `/show mcp <server>` shows the active tools exposed by a connected MCP server.
- `/use <skill> <message>` forces a specific skill for a single request.
- `/use <skill>` arms that skill for your next message in the same chat session.
- `/use clear` cancels a pending skill override created by `/use <skill>`.
- `/btw <question>` asks an immediate side question without changing the current session history. `/btw` is handled as a no-tool query and does not enter the normal tool-execution flow.

Examples:

```text
/model
/list models
/model use deepseek
/show model
/list skills
/list mcp
/show mcp github
/use git explain how to squash the last 3 commits
/btw remind me what we already decided about the deploy plan
/use italiapersonalfinance
dammi le ultime news
```

### Unified Command Execution Policy

- Generic slash commands are executed through a single path in `pkg/agent/agent_command.go` via `commands.Executor`.
- Channel adapters no longer consume generic commands locally; they forward inbound text to the bus/agent path. Telegram still auto-registers supported commands such as `/start`, `/help`, `/show`, `/list`, `/model`, `/use`, and `/btw` at startup.
- Unknown slash command (for example `/foo`) returns an explicit unknown-command error and does not fall through to normal LLM processing.
- Registered but unsupported command on the current channel (for example `/show` on WhatsApp) returns an explicit user-facing error and stops further processing.

### Session Isolation

Session scope controls how much memory is shared between chats, users, threads, and spaces.

- Use `session.dimensions` for the global default.
- Use `session_dimensions` on a dispatch rule for one routed exception.

For step-by-step recipes and isolation patterns, see the [Session Guide](session-guide.md).

`session.lifecycle` optionally rotates only conversation history and context. Supported strategies are:

- `never` or omitted: no automatic rotation
- `calendar`: `period` is `day`, `week`, or `month`, with a required IANA `timezone`
- `idle`: rotate after `idle_timeout_minutes` without activity
- `max_age`: rotate after `max_age_minutes` from epoch creation

Idle and max-age checkpoints persist across process restarts. See the Session Guide for complete examples and boundary
semantics.

### Absolute Seahorse Context Budgets

Seahorse can enforce predictable prompt budgets independently of the model's full context window:

```json
{
  "agents": {
    "defaults": {
      "context_manager": "seahorse",
      "context_manager_config": {
        "historyMaxTokens": 12000,
        "summaryMaxTokens": 3000,
        "recentTailTurns": 2
      }
    }
  }
}
```

- `historyMaxTokens` is the normal target for raw conversation messages selected for a turn.
- `summaryMaxTokens` caps the fully rendered Seahorse summary, including its XML and guidance text.
- `recentTailTurns` requests that the newest complete user turns remain raw, including assistant tool calls and their
  tool results, whenever they fit the model's hard context ceiling.

Each value is optional and zero disables that separate target. When any value is enabled, the runtime first reserves
the current system prompt, active skills, visible tool schemas, media, and `max_tokens`. The remaining model capacity
is the hard ceiling. The requested recent tail may exceed `historyMaxTokens` while it fits that ceiling. If it does not,
Seahorse removes its oldest complete turns until it fits, without splitting tool-call/result sequences. A turn fails
closed only when the mandatory prompt content itself cannot fit the model window.

Absolute pressure schedules background compaction even when the model context window is not close to full. Structured
logs and `agent.context.compress` events include reserves, source and selected token counts, requested and retained
tail turns, overflow tokens, hard-limit degradation, truncation, and the pressure reason. A degraded tail does not
schedule compaction because the configured raw-tail boundary still protects those turns; compaction resumes when that
window advances and older turns become eligible.

### Seahorse Recall Boundary

`agents.defaults.context_manager_config.maxRetrievalScope` sets the broadest boundary that `short_grep` and
`short_expand` may use. The allowed values, from narrowest to broadest, are `current_epoch`, `conversation`, and
`workspace`. The default is `conversation`, so a model cannot search unrelated routed conversations even if it asks for
`workspace`.

Personal single-user workspaces that intentionally need recall across chats or topics can opt in explicitly:

```json
{
  "agents": {
    "defaults": {
      "context_manager": "seahorse",
      "context_manager_config": {
        "maxRetrievalScope": "workspace"
      }
    }
  }
}
```

Set `maxRetrievalScope` to `current_epoch` for strict lifecycle-epoch isolation. Invalid values fail Seahorse startup.
This policy is enforced after the model call at the tool boundary and applies identically to search and expansion.
Session dimensions remain the primary identity boundary; include `sender` where users in one room must not share a
routed conversation.

### Tool Result Retention

Tool-result policy belongs to the tools layer, independently of the configured context manager. Seahorse consumes that
policy to project successful tool results into a smaller future prompt without changing canonical session history or
the stored audit message:

```json
{
  "tools": {
    "result_retention": {
      "get_day_summary": {
        "mode": "transient"
      },
      "lookup_reference_food": {
        "mode": "compact_receipt",
        "receipt": "Reference lookup completed; repeat the lookup if exact values are needed."
      },
      "log_meal": {
        "mode": "durable",
        "receipt": "Meal saved in the Nutrition database; query the database for current values."
      }
    }
  },
  "agents": {
    "defaults": {
      "context_manager": "seahorse"
    }
  }
}
```

`tools.result_retention` rules use exact canonical tool names and support:

- `preserve`: retain the full result; this is the default for tools without a rule.
- `compact_receipt`: replace a resolved successful result with the configured receipt in future prompts.
- `transient`: omit a resolved successful result and its matching assistant tool call from future prompts.
- `durable`: declare that the tool writes to an external source of truth and retain only the configured receipt.

There is no automatic migration from the former Seahorse-owned
`agents.defaults.context_manager_config.toolResultRetention` field. Move those rules to `tools.result_retention` before
starting the updated binary; stale configurations fail loading with an actionable error.

`compact_receipt` and `durable` require a non-empty receipt of at most 1024 bytes. Rules apply only when the runtime
persisted an explicit `success` status. Errors, async/unresolved results, legacy results with unknown status, and results
with media remain fully preserved regardless of configuration. The current active turn always sees the full result.
Projection also runs before Seahorse summary generation, so transient output is not reintroduced through a new summary.
Raw JSONL and Seahorse message rows remain unchanged and available to audit and scoped historical retrieval.

### Final Turn Render

`agents.defaults.final_turn_render_mode` controls an experimental final-response render pass for steering-heavy turns.

When enabled with value `llm`, PicoClaw may do one extra **same-agent** LLM pass after tool execution has already completed:

- it reuses the accumulated turn context
- it disables tool calling for that final pass
- it asks the same agent to answer the **full accumulated request chain**, not only the latest follow-up

This is intended for multi-message turns such as:

- `How much did I eat today?`
- `And yesterday?`
- `And the day before yesterday?`

Config:

```json
{
  "agents": {
    "defaults": {
      "final_turn_render_mode": "llm"
    }
  }
}
```

Notes:

- omitted or empty: disabled
- `llm`: enable same-agent final no-tools render for eligible steering-heavy turns
- this setting is experimental and is mainly useful when follow-up messages often extend the same in-flight turn
- this is separate from channel/message delivery behavior; it affects only how the final reply text is rendered

### Routing

Routing is configured through `agents.dispatch.rules`.

Each rule matches against the normalized inbound context produced by channels.
Rules are evaluated from top to bottom. The first matching rule wins. If no
rule matches, PicoClaw falls back to the configured default agent.

Supported match fields:

* `channel`
* `account`
* `space`
* `chat`
* `topic`
* `sender`
* `mentioned`

Match values use the same scope vocabulary as the session system:

* `space`: `workspace:t001`, `guild:123456`
* `chat`: `direct:user123`, `group:-100123`, `channel:c123`
* `topic`: `topic:42`
* `sender`: a normalized sender identifier for the platform

Rules may optionally override the global `session.dimensions` value through
`session_dimensions`. This allows routing and session allocation to stay aligned
without reintroducing the old `bindings` or `dm_scope` formats.

Example:

```json
{
  "agents": {
    "list": [
      { "id": "main", "default": true },
      { "id": "support" },
      { "id": "sales" }
    ],
    "dispatch": {
      "rules": [
        {
          "name": "vip in support group",
          "agent": "sales",
          "when": {
            "channel": "telegram",
            "chat": "group:-1001234567890",
            "sender": "12345"
          },
          "session_dimensions": ["chat", "sender"]
        },
        {
          "name": "telegram support group",
          "agent": "support",
          "when": {
            "channel": "telegram",
            "chat": "group:-1001234567890"
          },
          "session_dimensions": ["chat"]
        }
      ]
    }
  },
  "session": {
    "dimensions": ["chat"]
  }
}
```

In the example above, the VIP rule must appear before the broader group rule.
Because routing is strictly ordered, more specific rules should be placed
earlier and broader fallback rules later.

For more complete routing and model-tier examples, see the [Routing Guide](routing-guide.md).

### Agent Tool Allowlist

Per-agent tool declarations live in `AGENT.md` frontmatter, not in `config.json`.

If `tools` is omitted from frontmatter, the agent gets the normal globally enabled tool set. If `tools` is present, PicoClaw applies the declared tool policy during registration.

```md
---
name: Research Agent
description: Specialist for web research and in-depth analysis.
tools: [read_file, write_file, web_search, web_fetch, message]
skills: [deep-research]
mcpServers: [web-index]
---

You are the research agent.
```

Notes:

- List form is shorthand for an allow policy.
- Tool and MCP server names can be declared either as an exact-name list or as an `allow` / `deny` policy object.
- Pattern matching uses shell-style globs against the runtime tool name or MCP server name.
- Use runtime tool names such as `web_search`, `web_fetch`, `spawn`, `subagent`, `send_file`.
- Tool declarations in `AGENT.md` are used by runtime/tooling, but they are not injected into the discovery prompt.

Examples:

```md
---
tools:
  allow:
    - mcp_*
    - web_fetch
  deny:
    - mcp_gpt_researcher_*
mcpServers:
  allow:
    - github
    - filesystem
---
```

Policy rules:

- omitted field: no frontmatter restriction
- list form: allowlist shorthand
- `deny` is applied after `allow`
- `deny` wins on overlap
- explicit empty list blocks all values for that field

### Agent Discovery (Automatic)

When an agent has spawnable peers and can call `spawn`, PicoClaw injects a structured agent registry into that agent's system prompt on every turn. No extra `list_agents` tool call is required.

This registry is intended to make delegation concrete and reliable, especially when using `spawn` with a target `agent_id`.

Each entry includes:

| Field | Meaning |
|-------|---------|
| `id` | Stable agent id |
| `name` | Agent identity name from `AGENT.md` frontmatter |
| `description` | Agent identity description from `AGENT.md` frontmatter |

Important behavior:

- The discovery section appears only when the current agent has the `spawn` tool and includes only peer agents it is permitted to spawn via `subagents.allow_agents`.
- The current agent and non-spawnable peers are omitted, so the model does not plan against unavailable agents.
- Discovery is intentionally lightweight. It gives the model only the identity it needs to choose a peer: `id`, `name`, and `description`.
- `config.json` remains the infrastructure layer: workspace, default agent selection, routing, and subagent permissions. Those permissions also gate discovery visibility.
- `AGENT.md` remains the identity layer. Runtime/tool code can still use its `tools`, `skills`, `mcpServers`, and `model` fields when delegation happens.

Example injected shape:

```json
{
  "agents": [
    {
      "id": "research",
      "name": "Research Agent",
      "description": "Specialist for long-form investigation and web work."
    }
  ]
}
```

In practice, this means a generalist agent can choose a peer based on its role description, then call `spawn` with the peer's `agent_id`. The runtime resolves the rest.

### Per-agent tool filtering

Per-agent tool filtering is defined in `AGENT.md` frontmatter.

```md
---
tools:
  deny:
    - mcp_gpt_researcher_*
---

# Agent
```

This keeps capability policy attached to the agent definition itself, and runtime enforces it during tool registration.

Rules:

- If neither `allow` nor `deny` is set, the agent sees the normal tool set.
- If `allow` is set, it acts like a whitelist:
  - only tool names matching at least one `allow` glob remain visible.
- If `deny` is set, matching tools are removed from the visible set.
- If both are set:
  1. `allow` is applied first
  2. `deny` is applied second
  3. `deny` wins on overlap

Examples:

- `allow: ["mcp_*", "web_fetch"]`
  - agent sees only MCP tools plus `web_fetch`
- `deny: ["mcp_gpt_researcher_*"]`
  - agent sees everything except GPT Researcher tools
- `allow: ["mcp_*"]` + `deny: ["mcp_gpt_researcher_*"]`
  - agent sees only MCP tools, except GPT Researcher tools

Patterns use shell-style globs matched against the final runtime tool name, for example:

- `mcp_gpt_researcher_*`
- `mcp_inventorydb_*`
- `web_*`
- `spawn`

This filtering is enforced at tool registration time. Filtered tools do not appear in the agent's prompt/tool list and cannot be called by that agent.

### 🔒 Security Sandbox

PicoClaw runs in a sandboxed environment by default. The agent can only access files and execute commands within the configured workspace.

#### Default Configuration

```json
{
  "agents": {
    "defaults": {
      "workspace": "~/.picoclaw/workspace",
      "restrict_to_workspace": true
    }
  }
}
```

| Option                  | Default                 | Description                               |
| ----------------------- | ----------------------- | ----------------------------------------- |
| `workspace`             | `~/.picoclaw/workspace` | Working directory for the agent           |
| `restrict_to_workspace` | `true`                  | Restrict file/command access to workspace |

#### Protected Tools

When `restrict_to_workspace: true`, the following tools are sandboxed:

| Tool          | Function         | Restriction                            |
| ------------- | ---------------- | -------------------------------------- |
| `read_file`   | Read files       | Only files within workspace            |
| `write_file`  | Write files      | Only files within workspace            |
| `list_dir`    | List directories | Only directories within workspace      |
| `apply_patch` | Patch files      | Only files within workspace            |
| `append_file` | Append to files  | Only files within workspace            |
| `exec`        | Execute commands | Command paths must be within workspace |

#### Additional Exec Protection

Even with `restrict_to_workspace: false`, the `exec` tool blocks these dangerous commands:

* `rm -rf`, `del /f`, `rmdir /s` — Bulk deletion
* `format`, `mkfs`, `diskpart` — Disk formatting
* `dd if=` — Disk imaging
* Writing to `/dev/sd[a-z]` — Direct disk writes
* `shutdown`, `reboot`, `poweroff` — System shutdown
* Fork bomb `:(){ :|:& };:`

### File Access Control

| Config Key | Type | Default | Description |
|------------|------|---------|-------------|
| `tools.allow_read_paths` | string[] | `[]` | Additional paths allowed for reading outside workspace |
| `tools.allow_write_paths` | string[] | `[]` | Additional paths allowed for writing outside workspace |
| `tools.message.media_enabled` | bool | `false` | Allows the `message` tool to attach local media files by path. This is separate from `tools.send_file.enabled`; enable it only when unified text/media/caption delivery is intended. |

### Read File Mode

`read_file` has two mutually exclusive implementations selected by config. PicoClaw registers exactly one of them at startup:

| Config Key | Type | Default | Description |
|------------|------|---------|-------------|
| `tools.read_file.enabled` | bool | `true` | Enables the `read_file` tool |
| `tools.read_file.mode` | string | `bytes` | Selects the `read_file` implementation: `bytes` or `lines` |
| `tools.read_file.max_read_file_size` | int | `65536` | Maximum bytes returned by `read_file` |

#### Mode: `bytes`

Optimized for arbitrary files and binary-safe pagination.

Parameters:

* `path` (required): File path
* `offset` (optional): Starting byte offset, default `0`
* `length` (optional): Maximum number of bytes to read, default `max_read_file_size`

Use `bytes` when:

* You may read binary files
* You want deterministic byte-range pagination

#### Mode: `lines`

Text-oriented behavior, optimized for source files, markdown, logs, and configs. The tool reads sequentially by line and stops when the configured byte budget is reached.

Parameters:

* `path` (required): File path
* `start_line` (optional): Starting line number, 1-indexed and inclusive, default `1`
* `max_lines` (optional): Maximum number of lines to read, default = all remaining lines until EOF or byte budget

Behavior notes:

* Binary-looking files are rejected with guidance to switch `read_file` to `mode = bytes`
* Extremely long single lines are truncated rather than skipped

Use `mode = lines` when:

* The agent mostly reads text files
* You want line-based pagination in prompts and tool calls
* You want cleaner chunks for code review, logs, and documentation

#### Example

```json
{
  "tools": {
    "read_file": {
      "enabled": true,
      "mode": "lines",
      "max_read_file_size": 65536
    }
  }
}
```

### Exec Security

| Config Key | Type | Default | Description |
|------------|------|---------|-------------|
| `tools.exec.allow_remote` | bool | `false` | Allow exec tool from remote channels (Telegram/Discord etc.) |
| `tools.exec.enable_deny_patterns` | bool | `true` | Enable dangerous command interception |
| `tools.exec.permission_mode` | string | `""` | Optional exec permission mode. Set to `read_only` to allow only commands classified as read-only. |
| `tools.exec.custom_deny_patterns` | string[] | `[]` | Custom regex patterns to block |
| `tools.exec.custom_allow_patterns` | string[] | `[]` | Custom regex patterns to allow |

> **Security Note:** Symlink protection is enabled by default — all file paths are resolved through `filepath.EvalSymlinks` before whitelist matching, preventing symlink escape attacks.

`permission_mode = "read_only"` is conservative: unknown commands are blocked because the validator cannot prove they are safe. The empty default preserves the existing behavior and still applies deny patterns, allowlists, channel restrictions, and workspace path checks.

#### Known Limitation: Child Processes From Build Tools

The exec safety guard only inspects the command line PicoClaw launches directly. It does not recursively inspect child
processes spawned by allowed developer tools such as `make`, `go run`, `cargo`, `npm run`, or custom build scripts.

That means a top-level command can still compile or launch other binaries after it passes the initial guard check. In
practice, treat build scripts, Makefiles, package scripts, and generated binaries as executable code that needs the same
level of review as a direct shell command.

For higher-risk environments:

* Review build scripts before execution.
* Prefer approval/manual review for compile-and-run workflows.
* Run PicoClaw inside a container or VM if you need stronger isolation than the built-in guard provides.

#### Error Examples

```
[ERROR] tool: Tool execution failed
{tool=exec, error=Command blocked by safety guard (path outside working dir)}
```

```
[ERROR] tool: Tool execution failed
{tool=exec, error=Command blocked by safety guard (dangerous pattern detected)}
```

#### Disabling Restrictions (Security Risk)

If you need the agent to access paths outside the workspace:

**Method 1: Config file**

```json
{
  "agents": {
    "defaults": {
      "restrict_to_workspace": false
    }
  }
}
```

**Method 2: Environment variable**

```bash
export PICOCLAW_AGENTS_DEFAULTS_RESTRICT_TO_WORKSPACE=false
```

> ⚠️ **Warning**: Disabling this restriction allows the agent to access any path on your system. Use with caution in controlled environments only.

#### Security Boundary Consistency

The `restrict_to_workspace` setting applies consistently across all execution paths:

| Execution Path   | Security Boundary            |
| ---------------- | ---------------------------- |
| Main Agent       | `restrict_to_workspace` ✅   |
| Subagent / Spawn | Inherits same restriction ✅ |
| Heartbeat tasks  | Inherits same restriction ✅ |

All paths share the same workspace restriction — there's no way to bypass the security boundary through subagents or scheduled tasks.

### Heartbeat (Periodic Tasks)

PicoClaw can perform periodic tasks automatically. Create a `HEARTBEAT.md` file in your workspace:

```markdown
# Periodic Tasks

- Check my email for important messages
- Review my calendar for upcoming events
- Check the weather forecast
```

The agent will read this file every 30 minutes (configurable) and execute any tasks using available tools.

#### Async Tasks with Spawn

For long-running tasks (web search, API calls), use the `spawn` tool to create a **subagent**:

```markdown
# Periodic Tasks

## Quick Tasks (respond directly)

- Report current time

## Long Tasks (use spawn for async)

- Search the web for AI news and summarize
- Check email and report important messages
```

**Key behaviors:**

| Feature                 | Description                                                                 |
| ----------------------- | --------------------------------------------------------------------------- |
| **spawn**               | Creates an async task/subagent and records it in the durable task registry  |
| **Independent context** | Subagent has its own context, no session history                            |
| **Delivery mode**       | Completion can be delivered to the user, the parent agent, or both          |
| **Non-blocking**        | After spawning, heartbeat continues to next task                            |
| **Status**              | Use `task_status` for durable status; `spawn_status` is a spawn-only view   |

#### How Subagent Communication Works

```
Heartbeat triggers
    ↓
Agent reads HEARTBEAT.md
    ↓
For long task: spawn subagent
    ↓                           ↓
Continue to next task      Subagent works independently
    ↓                           ↓
All tasks done            Subagent completes task record
    ↓                           ↓
Respond HEARTBEAT_OK      Delivery coordinator routes result
```

The subagent has access to its configured tools, but completion delivery is owned by the async task delivery path. A terminal background task usually uses user delivery. A compositional task can route the completion back to the parent so the parent can synthesize the final user-facing answer.

**Configuration:**

```json
{
  "heartbeat": {
    "enabled": true,
    "interval": 30
  }
}
```

| Option     | Default | Description                        |
| ---------- | ------- | ---------------------------------- |
| `enabled`  | `true`  | Enable/disable heartbeat           |
| `interval` | `30`    | Check interval in minutes (min: 5) |

**Environment variables:**

* `PICOCLAW_HEARTBEAT_ENABLED=false` to disable
* `PICOCLAW_HEARTBEAT_INTERVAL=60` to change interval

### Providers

> [!NOTE]
> Groq provides free voice transcription via Whisper. If configured, audio messages from any channel will be automatically transcribed at the agent level.

| Provider     | Purpose                                 | Get API Key                                                  |
| ------------ | --------------------------------------- | ------------------------------------------------------------ |
| `gemini`     | LLM (Gemini direct)                     | [aistudio.google.com](https://aistudio.google.com)           |
| `zhipu`      | LLM (Zhipu direct)                      | [bigmodel.cn](https://bigmodel.cn)                           |
| `volcengine` | LLM (Volcengine direct)                 | [volcengine.com](https://www.volcengine.com/activity/codingplan?utm_campaign=PicoClaw&utm_content=PicoClaw&utm_medium=devrel&utm_source=OWO&utm_term=PicoClaw) |
| `openrouter` | LLM (recommended, access to all models) | [openrouter.ai](https://openrouter.ai)                       |
| `anthropic`  | LLM (Claude direct)                     | [console.anthropic.com](https://console.anthropic.com)       |
| `openai`     | LLM (GPT direct)                        | [platform.openai.com](https://platform.openai.com)           |
| `deepseek`   | LLM (DeepSeek direct)                   | [platform.deepseek.com](https://platform.deepseek.com)       |
| `qwen`       | LLM (Qwen direct)                       | [dashscope.console.aliyun.com](https://dashscope.console.aliyun.com) |
| `groq`       | LLM + **Voice transcription** (Whisper) | [console.groq.com](https://console.groq.com)                 |
| `cerebras`   | LLM (Cerebras direct)                   | [cerebras.ai](https://cerebras.ai)                           |
| `vivgrid`    | LLM (Vivgrid direct)                    | [vivgrid.com](https://vivgrid.com)                           |

### Model Configuration (model_list)

> **What's New?** PicoClaw now prefers explicit `provider` + native `model` configuration (for example `"provider": "zhipu", "model": "glm-4.7"`). The legacy single-field `provider/model` form remains supported for compatibility when `provider` is omitted.

This design also enables **multi-agent support** with flexible provider selection:

- **Different agents, different providers**: Each agent can use its own LLM provider
- **Model fallbacks**: Configure primary and fallback models for resilience
- **Load balancing**: Distribute requests across multiple endpoints or keys
- **Centralized configuration**: Manage all providers in one place
- **Model enable/disable**: Use the `enabled` field to temporarily disable a model without removing its configuration

#### Vision overrides for `load_image`

Image understanding is configured per model entry, not through the
`image_generate` tool config. By default, a turn with images uses the same
active chat model. If a model needs a different vision-capable backend for
`load_image`, add a `capabilities.vision` override on that model entry.
`model` and `fallbacks` must reference configured `model_name` aliases from
`model_list`:

```json
{
  "model_list": [
    {
      "model_name": "deepseek-main",
      "provider": "openrouter",
      "model": "deepseek/deepseek-chat",
      "capabilities": {
        "vision": {
          "model": "gemini-flash-lite",
          "fallbacks": ["gpt-5.4-medium"]
        }
      }
    }
  ]
}
```

Semantics:

- No `capabilities.vision`: `load_image` uses the same active model as the turn.
- `capabilities.vision.model` set: image turns use that model instead.
- `capabilities.vision.fallbacks`: only apply to the vision route.
- `tools.image_generate.model` remains separate and only controls image generation.

#### 🔒 Security Configuration (Recommended)

PicoClaw supports separating sensitive data (API keys, tokens, secrets) from your main configuration by storing them in a `.security.yml` file.

**Key Benefits:**
- **Security**: Sensitive data is never in your main config file
- **Easy sharing**: Share config.json without exposing API keys
- **Version control**: Add `.security.yml` to `.gitignore`
- **Flexible deployment**: Different environments can use different security files

**Quick Setup:**

1. Create `~/.picoclaw/.security.yml` with your API keys:
```yaml
model_list:
  gpt-5.4:
    api_keys:
      - "sk-proj-your-actual-openai-key"
  claude-sonnet-4.6:
    api_keys:
      - "sk-ant-your-actual-anthropic-key"
channels:
  telegram:
    token: "your-telegram-bot-token"
web:
  brave:
    api_keys:
      - "BSAyour-brave-api-key"
  glm_search:
    api_key: "your-glm-search-api-key"
```

2. Set proper permissions:
```bash
chmod 600 ~/.picoclaw/.security.yml
```

3. Remove sensitive fields from `config.json` (recommended):
```json
{
  "model_list": [
    {
      "model_name": "gpt-5.4",
      "provider": "openai",
      "model": "gpt-5.4"
      // api_key loaded from .security.yml
    }
  ],
  "channel_list": {
    "telegram": {
      "enabled": true,
      "type": "telegram",
      // token loaded from .security.yml
    }
  }
}
```

**How it works:**
- Values from `.security.yml` are automatically mapped to config fields
- No special syntax needed — just omit sensitive fields from config.json
- If a field exists in both files, `.security.yml` value takes precedence
- You can mix direct values in config.json with security values

For complete documentation, see [`../security/security_configuration.md`](../security/security_configuration.md).

#### All Supported Vendors

| Vendor                  | `provider` Value  | Default API Base                                    | Protocol  | API Key                                                          |
| ----------------------- | ----------------- | --------------------------------------------------- | --------- | ---------------------------------------------------------------- |
| **OpenAI**              | `openai`          | `https://api.openai.com/v1`                         | OpenAI    | [Get Key](https://platform.openai.com)                           |
| **Anthropic**           | `anthropic`       | `https://api.anthropic.com/v1`                      | Anthropic | [Get Key](https://console.anthropic.com)                         |
| **智谱 AI (GLM)**       | `zhipu`           | `https://open.bigmodel.cn/api/paas/v4`              | OpenAI    | [Get Key](https://open.bigmodel.cn/usercenter/proj-mgmt/apikeys) |
| **DeepSeek**            | `deepseek`        | `https://api.deepseek.com/v1`                       | OpenAI    | [Get Key](https://platform.deepseek.com)                         |
| **Google Gemini**       | `gemini`          | `https://generativelanguage.googleapis.com/v1beta`  | Gemini    | [Get Key](https://aistudio.google.com/api-keys)                  |
| **Groq**                | `groq`            | `https://api.groq.com/openai/v1`                    | OpenAI    | [Get Key](https://console.groq.com)                              |
| **Moonshot**            | `moonshot`        | `https://api.moonshot.cn/v1`                        | OpenAI    | [Get Key](https://platform.moonshot.cn)                          |
| **通义千问 (Qwen)**     | `qwen`            | `https://dashscope.aliyuncs.com/compatible-mode/v1` | OpenAI    | [Get Key](https://dashscope.console.aliyun.com)                  |
| **NVIDIA**              | `nvidia`          | `https://integrate.api.nvidia.com/v1`               | OpenAI    | [Get Key](https://build.nvidia.com)                              |
| **Ollama**              | `ollama`          | `http://localhost:11434/v1`                         | OpenAI    | Local (no key needed)                                            |
| **LM Studio**           | `lmstudio`        | `http://localhost:1234/v1`                          | OpenAI    | Optional (local default: no key)                                 |
| **OpenRouter**          | `openrouter`      | `https://openrouter.ai/api/v1`                      | OpenAI    | [Get Key](https://openrouter.ai/keys)                            |
| **LiteLLM Proxy**       | `litellm`         | `http://localhost:4000/v1`                          | OpenAI    | Your LiteLLM proxy key                                           |
| **VLLM**                | `vllm`            | `http://localhost:8000/v1`                          | OpenAI    | Local                                                            |
| **Cerebras**            | `cerebras`        | `https://api.cerebras.ai/v1`                        | OpenAI    | [Get Key](https://cerebras.ai)                                   |
| **VolcEngine (Doubao)** | `volcengine`      | `https://ark.cn-beijing.volces.com/api/v3`          | OpenAI    | [Get Key](https://www.volcengine.com/activity/codingplan?utm_campaign=PicoClaw&utm_content=PicoClaw&utm_medium=devrel&utm_source=OWO&utm_term=PicoClaw) |
| **神算云**              | `shengsuanyun`    | `https://router.shengsuanyun.com/api/v1`            | OpenAI    | —                                                                |
| **BytePlus**            | `byteplus`        | `https://ark.ap-southeast.bytepluses.com/api/v3`    | OpenAI    | [Get Key](https://www.byteplus.com)                              |
| **Vivgrid**             | `vivgrid`         | `https://api.vivgrid.com/v1`                        | OpenAI    | [Get Key](https://vivgrid.com)                                   |
| **LongCat**             | `longcat`         | `https://api.longcat.chat/openai`                   | OpenAI    | [Get Key](https://longcat.chat/platform)                         |
| **ModelScope (魔搭)**   | `modelscope`      | `https://api-inference.modelscope.cn/v1`            | OpenAI    | [Get Token](https://modelscope.cn/my/tokens)                     |
| **Antigravity**         | `antigravity`     | Google Cloud                                        | Custom    | OAuth only                                                       |
| **GitHub Copilot**      | `github-copilot`  | `localhost:4321`                                    | gRPC      | —                                                                |

#### Basic Configuration

```json
{
  "model_list": [
    {
      "model_name": "ark-code-latest",
      "provider": "volcengine",
      "model": "ark-code-latest",
      "api_keys": ["sk-your-api-key"]
    },
    {
      "model_name": "gpt-5.4",
      "provider": "openai",
      "model": "gpt-5.4",
      "api_keys": ["sk-your-openai-key"]
    },
    {
      "model_name": "claude-sonnet-4.6",
      "provider": "anthropic",
      "model": "claude-sonnet-4.6",
      "api_keys": ["sk-ant-your-key"]
    },
    {
      "model_name": "glm-4.7",
      "provider": "zhipu",
      "model": "glm-4.7",
      "api_keys": ["your-zhipu-key"]
    }
  ],
  "agents": {
    "defaults": {
      "model": "gpt-5.4"
    }
  }
}
```

> **Security Note**: You can remove `api_keys` fields from your config and store them in `.security.yml` instead. See [Security Configuration](#-security-configuration-recommended) above for details.
>
> **Note**: The `enabled` field can be set to `false` to disable a model entry without removing it. When omitted, it defaults to `true` during migration for models that have API keys.

Resolution rules:

- Prefer explicit `"provider": "openai", "model": "gpt-5.4"`.
- If `provider` is set, PicoClaw sends `model` unchanged.
- If `provider` is omitted, PicoClaw treats the first `/` segment in `model` as the provider and everything after that first `/` as the runtime model ID.
- This means `"model": "openrouter/openai/gpt-5.4"` still works as a compatibility form and sends `openai/gpt-5.4` to OpenRouter.

#### Streaming Configuration

Provider streaming uses a double opt-in and is disabled by default. The agent only tries streaming when the current channel has `settings.streaming.enabled: true`, the active model entry has `streaming.enabled: true`, and both the provider and channel support streaming. If any condition is missing, PicoClaw uses the normal non-streaming request path.

Pico WebUI is the first fully wired channel. Pico creates the first assistant message with the existing `message.create` wire message, then updates that same message with `message.update`; no new Pico wire message type is introduced.

Leave `streaming` unset when you do not want streaming. An omitted `streaming` block means disabled; you do not need to write `"streaming": {"enabled": false}`.

Opt-in example:

```json
{
  "model_list": [
    {
      "model_name": "gpt-5.4",
      "provider": "openai",
      "model": "gpt-5.4",
      "api_keys": ["sk-your-openai-key"],
      "streaming": {
        "enabled": true
      }
    }
  ],
  "channel_list": {
    "pico": {
      "enabled": true,
      "type": "pico",
      "settings": {
        "token": "YOUR_PICO_TOKEN",
        "streaming": {
          "enabled": true
        }
      }
    }
  }
}
```

| Field | Type | Default | Description |
| ----- | ---- | ------- | ----------- |
| `channel_list.<name>.settings.streaming.enabled` | bool | `false` | Allows this channel to display provider streaming output |
| `channel_list.<name>.settings.streaming.throttle_seconds` | int | Pico default after enabling: `0` | Minimum interval for intermediate updates; final content is always flushed |
| `channel_list.<name>.settings.streaming.min_growth_chars` | int | Pico default after enabling: `1` | Minimum character growth before sending an intermediate update; final content is always flushed |
| `model_list[].streaming.enabled` | bool | `false` | Allows this model entry to try provider streaming requests |

Legacy Telegram environment variables remain compatible: `PICOCLAW_CHANNELS_TELEGRAM_STREAMING_ENABLED`, `PICOCLAW_CHANNELS_TELEGRAM_STREAMING_THROTTLE_SECONDS`, and `PICOCLAW_CHANNELS_TELEGRAM_STREAMING_MIN_GROWTH_CHARS`. They only apply to Telegram settings and do not enable or modify Pico `settings.streaming`.

Telegram topic ownership filters can also be set through environment overrides:
`PICOCLAW_CHANNELS_TELEGRAM_ALLOWED_TOPIC_IDS` and
`PICOCLAW_CHANNELS_TELEGRAM_IGNORED_TOPIC_IDS`. Use comma-separated topic IDs
such as `3565,7777`.

For Telegram forum groups, you can restrict a workspace to specific topics:

```json
{
  "channel_list": {
    "telegram": {
      "enabled": true,
      "type": "telegram",
      "settings": {
        "allowed_topic_ids": ["3565"],
        "ignored_topic_ids": ["6"]
      }
    }
  }
}
```

- `allowed_topic_ids`: if non-empty, the workspace only accepts messages from those forum topic IDs.
- `ignored_topic_ids`: messages from these forum topic IDs are always dropped.
- The filter is applied before media download, suppressed-message observation, and session routing side effects.
- Non-forum chats and regular Telegram private chats are unaffected.

Failure behavior is intentionally conservative: if streaming fails before any visible chunk is sent, PicoClaw retries once through the normal `Chat()` path. If a chunk has already been shown to the user, PicoClaw does not send a second non-streaming answer, because that would duplicate visible output.

For model-specific TTS request fields such as custom speech `voice` names or
`response_format: "mp3"`, use `model_list[].extra_body`.

#### Vendor-Specific Examples

> **Tip**: You can omit `api_key` fields and store them in `.security.yml` for better security. See [Security Configuration](#-security-configuration-recommended).

<details>
<summary><b>OpenAI</b></summary>

```json
{
  "model_name": "gpt-5.4",
  "provider": "openai",
  "model": "gpt-5.4"
  // api_key: set in .security.yml
}
```

</details>

<details>
<summary><b>OpenRouter TTS (MAI Voice 2)</b></summary>

```json
{
  "model_name": "mai-voice-2",
  "provider": "openrouter",
  "model": "microsoft/mai-voice-2",
  "api_base": "https://openrouter.ai/api/v1",
  "extra_body": {
    "voice": "en-US-Harper:MAI-Voice-2",
    "response_format": "mp3"
  }
  // api_key: set in .security.yml
}
```

Pair this with:

```json
{
  "voice": {
    "tts_model_name": "mai-voice-2"
  }
}
```

</details>

<details>
<summary><b>VolcEngine (Doubao)</b></summary>

```json
{
  "model_name": "ark-code-latest",
  "provider": "volcengine",
  "model": "ark-code-latest"
  // api_key: set in .security.yml
}
```

</details>

<details>
<summary><b>智谱 AI (GLM)</b></summary>

```json
{
  "model_name": "glm-4.7",
  "provider": "zhipu",
  "model": "glm-4.7"
  // api_key: set in .security.yml
}
```

</details>

<details>
<summary><b>DeepSeek</b></summary>

```json
{
  "model_name": "deepseek-chat",
  "provider": "deepseek",
  "model": "deepseek-chat"
  // api_key: set in .security.yml
}
```

</details>

<details>
<summary><b>Anthropic</b></summary>

```json
{
  "model_name": "claude-sonnet-4.6",
  "provider": "anthropic",
  "model": "claude-sonnet-4.6"
  // api_key: set in .security.yml
}
```

> Run `picoclaw auth login --provider anthropic` to paste your API token.

For direct Anthropic API access or custom endpoints that only support Anthropic's native message format:

```json
{
  "model_name": "claude-opus-4-6",
  "provider": "anthropic-messages",
  "model": "claude-opus-4-6",
  "api_keys": ["sk-ant-your-key"],
  "api_base": "https://api.anthropic.com"
}
```

> Use `anthropic-messages` when the endpoint requires Anthropic's native `/v1/messages` format instead of OpenAI-compatible `/v1/chat/completions`.

</details>

<details>
<summary><b>Ollama (local)</b></summary>

```json
{
  "model_name": "llama3",
  "provider": "ollama",
  "model": "llama3"
}
```

</details>

<details>
<summary><b>LM Studio (local)</b></summary>

```json
{
  "model_name": "lmstudio-local",
  "provider": "lmstudio",
  "model": "openai/gpt-oss-20b"
}
```

`api_base` defaults to `http://localhost:1234/v1`. API key is optional unless your LM Studio server enables authentication.<br/>
With explicit `provider`, PicoClaw sends `openai/gpt-oss-20b` unchanged to LM Studio. The legacy compatibility form `"model": "lmstudio/openai/gpt-oss-20b"` still resolves to the same upstream model ID when `provider` is omitted.

</details>

<details>
<summary><b>Custom Proxy / LiteLLM</b></summary>

```json
{
  "model_name": "my-custom-model",
  "provider": "openai",
  "model": "custom-model",
  "api_base": "https://my-proxy.com/v1"
  // api_key: set in .security.yml
}
```

With explicit `provider`, PicoClaw sends `model` unchanged. That means `"provider": "litellm", "model": "lite-gpt4"` sends `lite-gpt4`, while `"provider": "litellm", "model": "openai/gpt-4o"` sends `openai/gpt-4o`. The legacy compatibility forms `litellm/lite-gpt4` and `litellm/openai/gpt-4o` still resolve the same way when `provider` is omitted.

</details>

#### Load Balancing

Configure multiple endpoints for the same model name — PicoClaw will automatically round-robin between them:

**Option 1: Multiple API Keys in .security.yml (Recommended)**

```yaml
# .security.yml
model_list:
  gpt-5.4:
    api_keys:
      - "sk-proj-key-1"
      - "sk-proj-key-2"
```

```json
// config.json
{
  "model_list": [
    {
      "model_name": "gpt-5.4",
      "provider": "openai",
      "model": "gpt-5.4",
      "api_base": "https://api.openai.com/v1"
      // api_keys loaded from .security.yml
    }
  ]
}
```

**Option 2: Multiple Model Entries**

```json
{
  "model_list": [
    {
      "model_name": "gpt-5.4",
      "provider": "openai",
      "model": "gpt-5.4",
      "api_base": "https://api1.example.com/v1",
      "api_keys": ["sk-key1"]
    },
    {
      "model_name": "gpt-5.4",
      "provider": "openai",
      "model": "gpt-5.4",
      "api_base": "https://api2.example.com/v1",
      "api_keys": ["sk-key2"]
    }
  ]
}
```

#### Migration from Legacy `providers` Config

The old `providers` configuration is **deprecated** and has been removed in V2. Existing V0/V1 configs are auto-migrated. See [docs/migration/model-list-migration.md](../migration/model-list-migration.md) for the full guide.

### Provider Architecture

PicoClaw routes providers by protocol family:

- **OpenAI-compatible**: OpenRouter, Groq, Zhipu, vLLM-style endpoints, and most others.
- **Gemini native**: Google Gemini via the native `models/*:generateContent` and `models/*:streamGenerateContent` endpoints.
- **Anthropic**: Claude-native API behavior.
- **Codex/OAuth**: OpenAI OAuth/token authentication route.

This keeps the runtime lightweight while making new OpenAI-compatible backends mostly a config operation (`api_base` + `api_keys`).

<details>
<summary><b>Zhipu (legacy providers format)</b></summary>

```json
{
  "agents": {
    "defaults": {
      "workspace": "~/.picoclaw/workspace",
      "model": "glm-4.7",
      "max_tokens": 8192,
      "temperature": 0.7,
      "max_tool_iterations": 20,
      "max_parallel_turns": 1
    }
  },
  "providers": {
    "zhipu": {
      "api_key": "Your API Key",
      "api_base": "https://open.bigmodel.cn/api/paas/v4"
    }
  }
}
```

> **Note**: The `providers` format is deprecated. Use the new `model_list` format with `.security.yml` for better security.
>
> **`max_parallel_turns`**: Controls concurrent processing of messages from different sessions. `1` (default) = sequential; `>1` = parallel. Messages from the same session are always serialized. See [Steering docs](../architecture/steering.md) for details.

</details>

<details>
<summary><b>Full config example</b></summary>

```json
{
  "agents": {
    "defaults": {
      "model_name": "claude-opus-4-5"
    }
  },
  "session": {
    "dm_scope": "per-channel-peer",
    "backlog_limit": 20
  },
  "channel_list": {
    "telegram": {
      "enabled": true,
      "type": "telegram",
      // token: set in .security.yml
      "allow_from": ["123456789"]
    }
  },
  "tools": {
    "web": {
      "duckduckgo": {
        "enabled": true,
        "max_results": 5
      }
    }
  },
  "heartbeat": {
    "enabled": true,
    "interval": 30
  }
}
```

> **Note**: Sensitive fields (`api_key`, `token`, etc.) can be omitted and stored in `.security.yml` for better security.

</details>

### Scheduled Tasks / Reminders

PicoClaw supports cron-style scheduled tasks via the `cron` tool. The agent can set, list, and cancel reminders or recurring jobs that trigger at specified times.

```json
{
  "tools": {
    "cron": {
      "enabled": true,
      "exec_timeout_minutes": 5,
      "allow_command": true,
      "command_allowed_remotes": []
    }
  }
}
```

Scheduled tasks persist across restarts and are stored in `~/.picoclaw/workspace/cron/`.

Command cron jobs can execute shell commands. By default, remote channels cannot schedule command jobs. To allow specific remote channels, set `command_allowed_remotes` to entries such as `"telegram"` or `"telegram:1234567890"`; use `"*"` only if every non-empty channel should be allowed. The `"*"` wildcard is potentially dangerous because any remote channel that can talk to PicoClaw can schedule shell commands. This does not bypass `allow_command`, `command_confirm`, or exec safety checks.

### Advanced Topics

| Topic | Description |
| ----- | ----------- |
| [Security Configuration](../security/security_configuration.md) | Store API keys and secrets in separate `.security.yml` file |
| [Sensitive Data Filtering](../security/sensitive_data_filtering.md) | Filter API keys and tokens from tool results before sending to LLM |
| [Hook System](../architecture/hooks/README.md) | Event-driven hooks: observers, interceptors, approval hooks |
| [Steering](../architecture/steering.md) | Inject messages into a running agent loop between tool calls |
| [SubTurn](../architecture/subturn.md) | Subagent coordination, concurrency control, lifecycle |
