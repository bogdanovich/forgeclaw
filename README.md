<div align="center">

<img src="assets/brand/mintclaw-wordmark-1600.png" alt="MintClaw" width="720">

### A durable, operator-controlled agent runtime for personal automation

Keep work moving across chat turns, restarts, subagents, and paired machines.

<p>
  <img src="https://img.shields.io/badge/Go-1.26+-00ADD8?style=flat&logo=go&logoColor=white" alt="Go 1.26+">
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-35c98a" alt="MIT License"></a>
</p>

<p>
  <a href="#quick-start">Quick start</a> ·
  <a href="#why-mintclaw">Why MintClaw</a> ·
  <a href="docs/README.md">Documentation</a>
</p>

</div>

---

MintClaw is a Go-native personal agent runtime for workflows that need to stay
understandable and recoverable after the current chat turn ends. Connect the
models, chat apps, MCP servers, and skills you already use; delegate work to
durable background tasks; steer an active run; pause for an authorized human;
and extend execution to explicitly paired machines.

## Why MintClaw

- **Work that survives interruption.** Async tasks, inbound work, user
  interactions, session goals, and remote-node operations have explicit
  persisted state and restart behavior.
- **Control while the agent is working.** Steering lets a newer message change
  direction between tool calls, with undispatched side effects skipped safely
  and represented in model context.
- **Human checkpoints without holding the process open.** A foreground turn or
  background task can pause for authorized input or approval and resume the
  exact call after an answer—even across a restart.
- **Compositional multi-agent workflows.** Spawned and delegated work has
  durable status, bounded delivery destinations, model policy, and parent/user
  handoff semantics.
- **Remote execution with explicit authority.** The slim `mintclaw-node`
  companion pairs over WSS and is constrained by both gateway and node-local
  policy, with approval and no-blind-replay behavior for uncertain operations.
- **Open extension surfaces.** Use built-in tools, MCP servers, workspace
  skills, provider routing, scheduled jobs, chat channels, the CLI, or the Web
  launcher without tying the runtime to one model vendor.

## From PicoClaw to MintClaw

MintClaw is a downstream fork of [PicoClaw](https://github.com/sipeed/picoclaw).
It keeps the practical Go foundation while intentionally diverging around
workflow durability, delivery ownership, multi-agent control, context
management, and operations. Treat MintClaw as its own runtime rather than as a
drop-in PicoClaw build.

PicoClaw remains the better starting point when staying close to upstream and
targeting the smallest practical hardware are the main priorities. MintClaw
accepts a larger operational surface in exchange for explicit contracts around
long-running work, human decisions, restart recovery, and paired machines.

Read [MintClaw and PicoClaw](docs/guides/picoclaw-lineage.md) for the fork
relationship, the intentional divergence, and the trade-offs behind it.

## Quick start

### Build the CLI

Prerequisites: Go 1.26+. Node.js 22+ and pnpm 10.33.0+ are needed only for the
Web launcher.

```bash
git clone https://github.com/bogdanovich/mintclaw.git
cd mintclaw
make deps
make build

./build/mintclaw onboard
./build/mintclaw agent -m "What should we automate first?"
```

`onboard` creates the local configuration and workspace. Add a provider key,
then use interactive CLI mode with `./build/mintclaw agent` or start chat-app
integrations with `./build/mintclaw gateway`.

### Launch the Web console

```bash
(cd web/frontend && pnpm install --frozen-lockfile)
make build-launcher
./build/mintclaw-launcher
# Open http://localhost:18800
```

The first visit creates a dashboard password. Do not expose the launcher to an
untrusted network; see the [Web launcher security notes](docs/guides/configuration.md#web-launcher-dashboard).

Prefer containers? Follow the [Docker and launcher guide](docs/guides/docker.md).
Running on an older Android phone? Use the [Termux guide](docs/guides/android-termux.md).

> [!CAUTION]
> MintClaw is under rapid development and may contain unresolved security
> issues. Review sender allowlists and tool policies, keep the launcher private,
> and run `mintclaw doctor --strict` before exposing a deployment. Do not treat a
> pre-1.0 build as a hardened multi-user service.

## What you can build

- An always-on assistant reached through Telegram, Discord, WhatsApp, Slack,
  Matrix, and other supported chat apps.
- A personal operations agent that schedules work, delegates long tasks, asks
  for decisions, and reports completion to the right conversation.
- A model-agnostic tool host using local tools, web search, MCP servers, and
  installable workspace skills.
- A small multi-machine setup where a central gateway invokes only approved
  capabilities on paired Linux or macOS nodes.
- A context-aware assistant using Seahorse-backed history, bounded prompt
  assembly, session routing, and durable per-conversation goals.

## Run it your way

| Surface | Best for | Start here |
| --- | --- | --- |
| CLI | One-shot work, interactive chat, scripting, and diagnostics | `mintclaw onboard`, `mintclaw agent`, `mintclaw doctor` |
| Web launcher | Browser-based setup, configuration, and chat | [Docker and launcher guide](docs/guides/docker.md) |
| Gateway | Always-on chat apps, scheduled work, and live agent sessions | [Chat apps](docs/guides/chat-apps.md) |
| Docker | Reproducible server or local deployment | [Docker Compose](docs/guides/docker.md#docker-compose) |
| Android / Termux | Reuse an ARM64 phone as a small always-on host | [Android Termux guide](docs/guides/android-termux.md) |
| Node companion | Add policy-constrained execution on another machine | [Node companion guide](docs/guides/node-companion.md) |

## Documentation

The README stays intentionally high level. Detailed provider lists, channel
setup, tool configuration, and CLI behavior live in the documentation.

| Need | Documentation |
| --- | --- |
| Install and configure | [Docker and quick start](docs/guides/docker.md) · [Configuration](docs/guides/configuration.md) |
| Connect models and chat apps | [Providers and models](docs/guides/providers.md) · [Chat apps](docs/guides/chat-apps.md) |
| Run durable workflows | [Spawn and async tasks](docs/guides/spawn-tasks.md) · [Human interaction](docs/guides/human-interaction.md) · [Cron](docs/reference/cron.md) |
| Extend the runtime | [Tools](docs/reference/tools_configuration.md) · [MCP CLI](docs/reference/mcp-cli.md) · [Node companion](docs/guides/node-companion.md) |
| Understand behavior | [Architecture](docs/architecture/README.md) · [Sessions](docs/guides/session-guide.md) · [Steering](docs/architecture/steering.md) |
| Operate safely | [Doctor](docs/reference/doctor.md) · [Security](docs/security/README.md) · [Troubleshooting](docs/operations/troubleshooting.md) |

The full documentation index is at [docs/README.md](docs/README.md).

## Contributing

Contributions are welcome. Start with [CONTRIBUTING.md](CONTRIBUTING.md) for
the local development workflow and validation requirements.

MintClaw is available under the [MIT License](LICENSE).
