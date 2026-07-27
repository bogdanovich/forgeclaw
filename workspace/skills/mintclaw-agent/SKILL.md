---
name: mintclaw-agent
description: "Configure, extend, debug, or contribute to MintClaw itself. Use when the task is about MintClaw CLI commands, config.json, gateway, auth, models, skills, MCP servers, cron, routing, sessions, built-in slash commands, or repository internals. Use MintClaw-native workflows, terminology, paths, and configuration."
metadata: {"nanobot":{"emoji":"🦞"}}
---

# MintClaw Agent

MintClaw is a lightweight personal AI assistant and agent framework with a native CLI, chat gateway, MCP integration, installable skills, session routing, and scheduled jobs.

Use this skill when the job is about **MintClaw itself**: onboarding, configuration, debugging, adding features, extending the CLI, changing routing/session behavior, working on skills or MCP support, or contributing to this repository.

## Operating Stance

When this skill is active, stay fully subordinate to MintClaw's real architecture:

- Prefer MintClaw commands, config keys, workspace layout, and docs.
- Follow MintClaw source and docs for behavior, naming, and workflows.
- Treat repository code and checked-in docs as the source of truth.

## Quick Start

```bash
# Initialize ~/.mintclaw/config.json and ~/.mintclaw/workspace
mintclaw onboard

# Authenticate a provider
mintclaw auth login --provider openai

# Inspect or switch the default model
mintclaw model
mintclaw model my-default-model

# One-shot prompt
mintclaw agent -m "Hello"

# Interactive CLI chat
mintclaw agent

# Start the gateway for chat channels
mintclaw gateway

# Inspect runtime/config health
mintclaw status

# Explore installed skills and MCP servers
mintclaw skills list
mintclaw mcp list
```

## Command Surface


### Core CLI

```bash
mintclaw onboard
mintclaw agent [-m MESSAGE] [--session KEY] [--model MODEL] [--debug]
mintclaw gateway [--debug] [--no-truncate] [--allow-empty] [--host HOST]
mintclaw status
mintclaw version
mintclaw migrate
```

### Configuration and Models

```bash
# Show or change the default configured model alias
mintclaw model
mintclaw model <model_name>

# Add a model from an OpenAI-compatible endpoint
mintclaw model add --api-base URL --api-key KEY
mintclaw model add -b http://localhost:8000/v1 -k dummy -m my-model -n local

# Reset config to factory defaults (preserves sensitive keys)
mintclaw config reset
mintclaw config reset --force
```

Notes:

- There is **no** general `mintclaw config edit` or `mintclaw config set`.
- Advanced edits are usually done by editing `~/.mintclaw/config.json` directly.

### Authentication

```bash
mintclaw auth login --provider openai
mintclaw auth login --provider anthropic --setup-token
mintclaw auth login --provider antigravity --device-code
mintclaw auth models
mintclaw auth status
mintclaw auth logout --provider openai
mintclaw auth weixin
mintclaw auth wecom --timeout 10m
```

### Skills

```bash
mintclaw skills list
mintclaw skills show <name>
mintclaw skills search "query"
mintclaw skills install owner/repo/path
mintclaw skills install --registry clawhub <slug>
mintclaw skills remove <name>
mintclaw skills list-builtin
mintclaw skills install-builtin
```

Skill loading priority is:

1. `~/.mintclaw/workspace/skills`
2. `~/.mintclaw/skills`
3. builtin embedded skills

### MCP

```bash
mintclaw mcp add filesystem -- npx -y @modelcontextprotocol/server-filesystem /tmp
mintclaw mcp add --deferred github --env-file .env.github -- npx -y @modelcontextprotocol/server-github
mintclaw mcp list
mintclaw mcp list --status
mintclaw mcp show github
mintclaw mcp test github
mintclaw mcp edit
mintclaw mcp remove github
```

Notes:

- `mintclaw mcp` is a **configuration manager** for `tools.mcp.servers`.
- It does **not** keep servers running by itself; the host/gateway loads them later.

### Cron

```bash
mintclaw cron list
mintclaw cron add --name "Daily summary" --message "Summarize today's logs" --cron "0 18 * * *"
mintclaw cron add --name "Ping" --message "heartbeat" --every 300 --deliver
mintclaw cron enable <job-id>
mintclaw cron disable <job-id>
mintclaw cron remove <job-id>
```

Notes:

- The current CLI `mintclaw cron add` supports recurring jobs only: `--every` or `--cron`.
- One-shot `at_seconds` jobs exist in the cron system, but not as a first-class CLI flag today.

## Providers

MintClaw supports 30+ LLM providers through `model_list`.

Credential patterns in MintClaw are:

- `model_list[].api_keys` for most hosted APIs
- `mintclaw auth login --provider ...` for the built-in auth helper flows (`openai`, `anthropic`, `antigravity`)
- local or self-hosted endpoints for providers like `ollama`, `lmstudio`, `vllm`, and `litellm`
- external platform credentials for providers like `bedrock`, `azure`, and `github-copilot`

`mintclaw auth login` does **not** cover every provider. For most providers, the normal path is adding a `model_list` entry with `provider`, `model`, and `api_keys`.

### Common Provider Matrix

| Provider | `provider` value | Auth path in MintClaw |
| --- | --- | --- |
| OpenAI | `openai` | OAuth helper via `mintclaw auth login --provider openai`, or `model_list[].api_keys` |
| Anthropic | `anthropic` | API key in `model_list[].api_keys`, or helper flow via `mintclaw auth login --provider anthropic` |
| Anthropic Messages API | `anthropic-messages` | API key in `model_list[].api_keys` |
| Google Gemini | `gemini` | API key in `model_list[].api_keys` |
| OpenRouter | `openrouter` | API key in `model_list[].api_keys` |
| Zhipu / GLM | `zhipu` | API key in `model_list[].api_keys` |
| DeepSeek | `deepseek` | API key in `model_list[].api_keys` |
| VolcEngine / Doubao | `volcengine` | API key in `model_list[].api_keys` |
| Qwen / DashScope | `qwen` | API key in `model_list[].api_keys` |
| Moonshot / Kimi | `moonshot` | API key in `model_list[].api_keys` |
| MiniMax | `minimax` | API key in `model_list[].api_keys` |
| Mistral | `mistral` | API key in `model_list[].api_keys` |
| Groq | `groq` | API key in `model_list[].api_keys` |
| NVIDIA NIM | `nvidia` | API key in `model_list[].api_keys` |
| Cerebras | `cerebras` | API key in `model_list[].api_keys` |
| Azure OpenAI | `azure` | `api_key` in `model_list`, or Microsoft Entra ID if built with `azidentity` support |
| AWS Bedrock | `bedrock` | AWS credentials plus Bedrock-enabled build (`go build -tags bedrock`) |
| Antigravity | `antigravity` | OAuth helper via `mintclaw auth login --provider antigravity` |
| GitHub Copilot | `github-copilot` | External Copilot gRPC endpoint, default `localhost:4321` |
| Ollama | `ollama` | Local endpoint, no API key required |
| LM Studio | `lmstudio` | Local endpoint, API key optional |
| vLLM | `vllm` | Local OpenAI-compatible endpoint |
| LiteLLM | `litellm` | Proxy endpoint and whichever credential model the proxy expects |
| Claude CLI | `claude-cli` | Local Claude CLI provider, configured by workspace/runtime |
| Codex CLI | `codex-cli` | Local Codex CLI provider, configured by workspace/runtime |

### Additional OpenAI-Compatible Vendors

MintClaw also carries first-class metadata or routing support for additional vendors such as:

- `venice`
- `vivgrid`
- `longcat`
- `modelscope`
- `mimo`
- `novita`
- `byteplus`
- `shengsuanyun`
- `avian`
- `zai-coding`
- `gpt4free`

For the full provider matrix, default API bases, protocol families, and vendor-specific examples, read `docs/guides/providers.md`.

## Built-in Tool Families

MintClaw tools are configured under `tools` in `config.json` and registered dynamically at runtime.

Important activation rules:

- most tools are enabled or disabled individually through `tools.*`
- agent-level allowlists can further restrict tool visibility
- `turn_profile.tools` can narrow tool exposure for a request or agent
- deferred MCP tools can stay hidden until unlocked by tool discovery

### Runtime Tool Families

| Family | Runtime tool names | What they provide |
| --- | --- | --- |
| Filesystem | `read_file`, `write_file`, `list_dir`, `append_file`, `apply_patch` | Read, write, list, and patch workspace files |
| Web | `web_search`, `web_fetch` | Search the web and fetch readable page content |
| Command execution | `exec` | Shell command execution with deny-pattern guardrails |
| Scheduling | `cron` | Scheduled jobs, reminders, recurring tasks, and command jobs |
| Skills registry | `find_skills`, `install_skill` | Search and install skills from configured registries |
| MCP | `mcp_<server>_<tool>` | Tools contributed by connected MCP servers |
| MCP discovery | `tool_search_tool_bm25`, `tool_search_tool_regex` | Discover deferred hidden MCP tools on demand |
| Hardware | `i2c`, `spi`, `serial` | Hardware access for supported devices and boards |
| Messaging | `message`, `reaction` | Send outbound messages and reactions through channel integrations |
| Media | `send_file`, `load_image`, `send_tts` | Send files, load local images into context, generate TTS output |
| Subagents | `spawn`, `subagent`, `spawn_status`, `delegate` | Background tasks, synchronous sub-turns, task status, multi-agent delegation |

### Tool Registration Notes

- `send_tts` is registered only when a TTS provider is available.
- `spawn` and `spawn_status` require `subagent` support to be enabled.
- `delegate` is auto-registered only when more than one agent exists.
- MCP discovery tools are relevant only when deferred MCP discovery is enabled.
- `message` can be configured for outbound media as well as plain text.

For per-tool configuration, read `docs/reference/tools_configuration.md`.

## Specialized Subagents and Spawn

MintClaw has a first-class subagent model for long-running work, isolated subproblems, and multi-agent specialization.

The core idea is:

- use `spawn` for background work that should continue without blocking the current turn
- use `subagent` for an isolated synchronous sub-task when the parent needs the result now
- use `delegate` to hand a task to a specific peer agent with its own identity, model, workspace, and tools
- use `spawn_status` or `/subagents` to inspect what is currently running

### Choosing the Right Subagent Tool

| Tool | Execution style | Best use |
| --- | --- | --- |
| `spawn` | Async background task | Web research, API polling, long scans, work that can report back later |
| `subagent` | Sync isolated sub-turn | Focused analysis, transformation, or verification that must return before the parent continues |
| `delegate` | Sync handoff to named peer agent | Work that should run as a specialized agent rather than as a generic child turn |
| `spawn_status` | Inspection/status only | Check running, completed, failed, or canceled spawned tasks |

### Specialized Peer Agents

Specialized subagents are configured through the multi-agent system, not through ad hoc prompts alone.

The two important layers are:

- `config.json` defines which peer agents exist and which ones a given agent is allowed to spawn via `subagents.allow_agents`
- each agent's `AGENT.md` defines the identity that makes that peer worth spawning: `name`, `description`, tools, skills, MCP servers, and optional model overrides

Minimal shape:

```json
{
  "agents": {
    "list": [
      {
        "id": "main",
        "default": true,
        "subagents": {
          "allow_agents": ["research"]
        }
      },
      {
        "id": "research"
      }
    ]
  }
}
```

Example `AGENT.md` for a specialist:

```md
---
name: Research Agent
description: Specialist for deep web research, evidence gathering, and synthesis.
tools: [web_search, web_fetch, message]
skills: [deep-research]
---
```

### Automatic Agent Discovery

When an agent has the `spawn` tool and at least one allowed peer, MintClaw injects a lightweight agent registry into the system prompt automatically.

That means:

- the model can see eligible peer agents without calling a separate `list_agents` tool
- only spawnable peers are shown
- the current agent is omitted
- discovery uses the peer agent's stable `id`, `name`, and `description`

In practice, this is what makes targeted `spawn(..., agent_id="research")` or `delegate(agent_id="research", ...)` reliable.

### Operational Behavior

Subagent execution semantics that matter:

- subagents run in isolated ephemeral session history, so their reasoning and intermediate steps do not pollute the parent conversation
- `spawn` returns immediately and launches background work in a goroutine
- `subagent` waits for completion and returns the result directly
- `delegate` is synchronous and runs as the target peer agent instead of a generic child task
- `spawn_status` is scoped to the current conversation when channel/chat context exists
- all subagents still share the same workspace security boundary; they do not bypass sandbox or path restrictions

Runtime limits and lifecycle rules:

- nested sub-turn depth is limited to 3
- concurrency is limited to 5 sub-turns per parent turn
- waiting for a concurrency slot times out after 30 seconds
- spawned background tasks are marked critical so they can survive graceful parent completion
- hard aborts still cascade to child and grandchild sub-turns

### Practical Patterns

Use `spawn` when:

- the task will take a while
- the result can arrive later
- the current turn should keep moving

Use `subagent` when:

- you need isolation from the parent context
- you want an independent attempt at a bounded subproblem
- the parent must wait for the answer before planning the next step

Use `delegate` when:

- a named peer agent is clearly better suited for the task
- that peer has a narrower tool/skill/model setup
- you want the task to run in the peer's own workspace/runtime identity

### Examples

Background specialist research:

```text
spawn(
  task="Search the web for the latest MintClaw MCP integration patterns and summarize them.",
  label="mcp-research",
  agent_id="research"
)
```

Synchronous isolated check:

```text
subagent(
  task="Review this config for risky tool exposure and return only the concrete findings."
)
```

Synchronous handoff to a named peer:

```text
delegate(
  agent_id="research",
  task="Collect three primary-source references for current provider authentication behavior."
)
```

### Observability

For live visibility:

- call `spawn_status` to inspect one task or list visible tasks in the current conversation
- use `/subagents` in chat channels to show the active subagent tree for the current session

## Voice, Transcription, and TTS

MintClaw can transcribe inbound audio and synthesize outbound speech, but voice setup is model-driven like the rest of the runtime.

The important pattern is:

- ASR uses `voice.model_name`
- TTS uses `voice.tts_model_name`
- both resolve through named entries in `model_list`
- secrets belong in `.security.yml`, not inline in `voice`

### STT (Voice -> Text)

Voice and audio messages from supported channels can be transcribed automatically at the agent level.

Recommended setup:

1. add an ASR-capable model entry to `model_list`
2. set `voice.model_name` to that entry's `model_name`
3. store the matching API key in `.security.yml`
4. optionally set `voice.echo_transcription` if you want the transcript echoed back in chat

Example:

```json
{
  "model_list": [
    {
      "model_name": "voice-groq",
      "model": "groq/whisper-large-v3-turbo"
    }
  ],
  "voice": {
    "model_name": "voice-groq",
    "echo_transcription": true
  }
}
```

```yaml
model_list:
  voice-groq:
    api_keys:
      - "gsk_your_groq_key"
```

### Common ASR Routes

| Route | Example model | Notes |
| --- | --- | --- |
| Groq Whisper | `groq/whisper-large-v3-turbo` | Fast OpenAI-compatible Whisper transcription and a common default choice |
| OpenAI Whisper | `openai/whisper-1` | Standard Whisper transcription through the OpenAI-compatible audio endpoint |
| ElevenLabs Scribe | `provider: elevenlabs`, `model: scribe_v1` | Uses MintClaw's dedicated ElevenLabs transcription path |
| Audio-capable chat models | `gemini/gemini-2.5-flash`, `openai/gpt-4o-audio-preview` | Multimodal audio transcription path; some model combinations are still evolving |

Detection behavior that matters:

- `voice.model_name` is the preferred and recommended path
- if it resolves to an ElevenLabs model, MintClaw uses the ElevenLabs transcriber
- if it resolves to a Whisper-compatible model, MintClaw uses the Whisper transcription path
- if it resolves to an audio-capable multimodal model, MintClaw can use audio-model transcription
- if `voice.model_name` is omitted, MintClaw still performs compatibility scanning across `model_list` for legacy auto-detected ASR entries

### TTS (Text -> Voice)

Outbound speech is driven by `voice.tts_model_name` and exposed through `send_tts` when a provider is available.

Recommended setup:

1. add a TTS-capable model entry to `model_list`
2. set `voice.tts_model_name` to that entry's `model_name`
3. store the API key in `.security.yml`
4. if the provider needs model-specific TTS fields, add them under `model_list[].extra_body`
5. enable `send_tts` in the tool configuration if you want the agent to emit speech files

Example:

```json
{
  "model_list": [
    {
      "model_name": "openai-tts",
      "model": "openai/tts-1"
    }
  ],
  "voice": {
    "tts_model_name": "openai-tts"
  }
}
```

```yaml
model_list:
  openai-tts:
    api_keys:
      - "sk-openai-your-key"
```

Example with OpenRouter MAI Voice 2:

```json
{
  "model_list": [
    {
      "model_name": "mai-voice-2",
      "provider": "openrouter",
      "model": "microsoft/mai-voice-2",
      "api_base": "https://openrouter.ai/api/v1",
      "extra_body": {
        "voice": "en-US-Harper:MAI-Voice-2",
        "response_format": "mp3"
      }
    }
  ],
  "voice": {
    "tts_model_name": "mai-voice-2"
  }
}
```

```yaml
model_list:
  mai-voice-2:
    api_keys:
      - "sk-or-your-openrouter-key"
```

### Current TTS Provider Paths

| Provider path | Example model | Notes |
| --- | --- | --- |
| OpenAI-compatible speech | `openai/tts-1` | Best-supported path; MintClaw sends an OpenAI-style `/audio/speech` request |
| Xiaomi MiMo | `mimo/mimo-v2-tts` | Dedicated MiMo TTS provider path with MP3 output |

Operational notes:

- the preferred selection path is `voice.tts_model_name`
- if that is missing, MintClaw can still scan `model_list` for the first API-backed model whose ID contains `tts`
- the current OpenAI-style TTS request defaults to `voice: alloy` and `response_format: opus`
- you can override `voice` and `response_format` for a specific TTS model through `model_list[].extra_body`
- if a provider rejects `response_format`, MintClaw retries once without that field
- `send_tts` is only registered when TTS detection succeeds


## In-Session Slash Commands

MintClaw's shared slash command registry lives under `pkg/commands`.

Use these when helping users inside chat channels:

```text
/start
/help
/show model
/show channel
/show agents
/show mcp <server>
/list models
/list channels
/list agents
/list skills
/list mcp
/subagents
/use <skill> [message]
/use clear
/btw <question>
```

Semantics that matter:

- `/use <skill> <message>` forces one installed skill for a single request.
- `/use <skill>` arms that skill for the **next** message in the same chat session.
- `/use clear` cancels the pending skill override.
- `/btw <question>` asks an isolated side question without mutating the main session history.
- `/subagents` shows the currently active subagent tree for the session.
- Unknown slash commands pass through to normal LLM handling instead of hard-failing.

Telegram auto-registers supported top-level commands like `/start`, `/help`, `/show`, `/list`, `/use`, and `/btw`.

## Key Paths and Environment

### Important Files

```text
~/.mintclaw/config.json          Main config
~/.mintclaw/.security.yml       Sensitive values stored outside config.json
~/.mintclaw/auth.json           OAuth/token store
~/.mintclaw/workspace/          Default workspace
~/.mintclaw/workspace/skills/   Workspace skills
~/.mintclaw/workspace/sessions/ Session history
~/.mintclaw/workspace/cron/     Scheduled jobs store
```

Default workspace layout:

```text
~/.mintclaw/workspace/
├── sessions/
├── memory/
├── state/
├── cron/
├── skills/
├── AGENT.md
├── HEARTBEAT.md
├── IDENTITY.md
├── SOUL.md
└── USER.md
```

### Important Environment Variables

```bash
MINTCLAW_CONFIG=/path/to/config.json
MINTCLAW_HOME=/path/to/mintclaw-home
MINTCLAW_BUILTIN_SKILLS=/path/to/custom-builtin-skills
MINTCLAW_LOG_LEVEL=debug
MINTCLAW_GATEWAY_HOST=0.0.0.0
```

Use `MINTCLAW_CONFIG` when the user reports "wrong config file" behavior.
Use `MINTCLAW_HOME` when the user wants a portable or service-managed install.

## MintClaw-Native Concepts

### Model Configuration

MintClaw is model-centric. The key fields are:

- `agents.defaults.model_name`
- `model_list`
- optional `provider`
- runtime `model`

Important behavior:

- `agents.defaults.model_name` must match a `model_name` entry in `model_list`.
- If `provider` is set, MintClaw sends `model` to that provider unchanged.
- If `provider` is omitted, legacy `provider/model` parsing is still supported.

### Sessions and Routing

Session behavior is configured primarily through:

- `session.dimensions`
- `session.identity_links`
- `agents.dispatch.rules[*].session_dimensions`

Available dimensions:

- `space`
- `chat`
- `topic`
- `sender`

Baseline separation still includes:

- agent
- channel
- account

So even a tiny `session.dimensions` list does **not** create one giant global memory across every platform.

### Skills

Skills are plain directories with `SKILL.md`. The loader requires:

- a valid lowercase-or-hyphen skill name
- a non-empty description
- a `SKILL.md` file in the skill directory

Prefer this format:

```text
workspace/skills/<skill-name>/SKILL.md
```

MintClaw only relies on `name` and `description` frontmatter fields for loading and matching.

### MCP Discovery

MintClaw supports always-loaded and deferred MCP tools.

Use:

- `--deferred` when tools should stay hidden until explicitly discovered
- `mintclaw mcp show <name>` to inspect active tools
- `/list mcp` and `/show mcp <server>` from chat channels when debugging live agents

## Debugging Workflow

Start with the most MintClaw-native path:

1. Check `mintclaw status`.
2. Confirm which config file is active.
3. Inspect `agents.defaults.model_name` and `model_list`.
4. Run `mintclaw gateway --debug` for runtime visibility.
5. Add `--no-truncate` only when full prompt or tool payload inspection is necessary.
6. For skill issues, inspect the skill directory and frontmatter.
7. For MCP issues, use `mintclaw mcp list`, `show`, and `test`.
8. For routing/session issues, inspect `session.dimensions` and `agents.dispatch.rules`.

Useful runtime facts:

- `--no-truncate` only works with `--debug`.
- gateway health endpoints expose `/health`, `/ready`, and `/reload`.
- `tool_feedback` can publish visible tool-execution notices directly into chats.

### Where to Find Logs

For gateway and runtime debugging, MintClaw writes logs under its home directory:

```text
~/.mintclaw/logs/gateway.log
~/.mintclaw/logs/gateway_panic.log
```

If `MINTCLAW_HOME` is overridden, use:

```text
$MINTCLAW_HOME/logs/gateway.log
$MINTCLAW_HOME/logs/gateway_panic.log
```

In practice, check these places first:

- `~/.mintclaw/logs/` for persisted gateway logs
- the terminal running `mintclaw gateway` or `mintclaw agent`
- Docker stdout/stderr via `docker compose -f docker/docker-compose.yml logs -f`
- launcher or service logs if MintClaw is being run under another supervisor

Useful controls:

- `mintclaw gateway --debug` for detailed runtime logs
- `mintclaw gateway --debug --no-truncate` for full prompt/tool payload inspection
- `gateway.log_level` or `MINTCLAW_LOG_LEVEL` to raise verbosity to `debug` or `info`

Special case:

- the standalone `mintclaw agent` command uses console logging unless `MINTCLAW_LOG_FILE` is set explicitly
- process hooks can write JSONL file logs when `MINTCLAW_HOOK_LOG_FILE` is set; this is hook-specific and separate from the main gateway log path

## Repository Map

When contributing code, these paths matter most:

- `cmd/mintclaw/main.go` — root CLI wiring
- `cmd/mintclaw/internal/agent/` — direct CLI agent command
- `cmd/mintclaw/internal/gateway/` — gateway startup and flags
- `cmd/mintclaw/internal/auth/` — auth flows, QR onboarding
- `cmd/mintclaw/internal/model/` — default model switching and `model add`
- `cmd/mintclaw/internal/skills/` — install/list/show/remove/search commands
- `cmd/mintclaw/internal/mcp/` — MCP CLI configuration manager
- `cmd/mintclaw/internal/cron/` — cron CLI
- `pkg/commands/` — shared slash command registry
- `pkg/agent/` — prompt assembly, sessions, routing, tool execution, hooks
- `pkg/skills/` — skill loading, metadata, registry installs
- `pkg/mcp/` — MCP runtime integration
- `pkg/config/` — config schema, defaults, migration, persistence
- `docs/guides/configuration.md` — user-facing config reference
- `docs/guides/session-guide.md` — session behavior recipes
- `docs/reference/mcp-cli.md` — authoritative MCP CLI behavior
- `docs/reference/cron.md` — cron behavior and limitations
- `docs/operations/debug.md` — debugging workflow
- `docs/operations/troubleshooting.md` — known misconfiguration patterns

## Contribution Rules

When changing MintClaw:

- Prefer extending existing CLI groups and shared registries instead of adding parallel one-off flows.
- Keep docs aligned with code for CLI flags, slash commands, and config behavior.
- If you add or change a slash command, inspect `pkg/commands` and the chat-channel docs that mention command availability.
- If you touch skills behavior, validate load order, naming rules, and frontmatter assumptions.
- If you touch routing or session logic, re-check both `docs/guides/session-guide.md` and architecture docs so behavior and docs stay consistent.
- If you touch MCP, remember the CLI manages config while the runtime host manages execution.

## Common Troubleshooting

### "model ... not found in model_list"

Check that:

- `agents.defaults.model_name` matches a configured `model_name`
- the target `model_list` entry is enabled
- the `provider` and `model` fields use MintClaw's model-centric rules

### OpenRouter `free is not a valid model ID`

Prefer explicit provider config:

```json
{
  "provider": "openrouter",
  "model": "free"
}
```

Not:

```json
{
  "model": "free"
}
```

### Skill not appearing

Check:

- directory name is a valid skill name
- `SKILL.md` exists
- frontmatter `name` and `description` are present and sane
- the skill lives under workspace, global, or builtin roots

### MCP server exists but tools do not show up

Check:

- `tools.mcp.enabled` is true
- the server is enabled
- deferred discovery settings match expectations
- `mintclaw mcp test <name>` succeeds
- `/show mcp <server>` or `mintclaw mcp show <name>` exposes tools

### Agent remembers too much or too little

Check:

- `session.dimensions`
- any per-rule `session_dimensions`
- whether the issue is really session isolation versus summarization

### Config edits do not seem to apply

Check:

- `MINTCLAW_CONFIG`
- `MINTCLAW_HOME`
- whether the user edited `config.json` or `.security.yml`
- whether they are testing the CLI path, gateway path, or both

## Load These Docs Next

Read these only when the task needs them:

- `docs/guides/configuration.md` for config, routing, skills, and turn profiles
- `docs/guides/session-guide.md` for session isolation recipes
- `docs/reference/tools_configuration.md` for tool-specific config
- `docs/reference/mcp-cli.md` for MCP CLI flags and storage behavior
- `docs/reference/cron.md` for schedule types and security gates
- `docs/operations/debug.md` for runtime inspection
- `docs/operations/troubleshooting.md` for common provider/model mistakes

If the task is code-level rather than user-facing, read the matching package under `cmd/mintclaw/internal/`, `pkg/commands/`, `pkg/agent/`, `pkg/skills/`, or `pkg/mcp/` before proposing behavior changes.
