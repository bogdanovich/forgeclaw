# Choosing MintClaw

MintClaw, PicoClaw, OpenClaw, ZeroClaw, and Hermes Agent can all run a
tool-using personal assistant. They are not interchangeable, though: each
project puts its engineering effort in a different place.

This guide compares design centers and operational trade-offs. It is not a
performance benchmark, a security certification, or a promise of feature
parity. The external project descriptions below were last checked against
their official documentation on 2026-08-02.

## The Short Version

| Project | Implementation and lineage | Primary design center | Consider it when... |
| --- | --- | --- | --- |
| **MintClaw** | Go; downstream fork of PicoClaw | Durable, inspectable automation with explicit operator control | Work spans subagents, human decisions, restarts, delivery destinations, or paired machines. |
| [**PicoClaw**](https://github.com/sipeed/picoclaw) | Go; MintClaw's upstream source project | An ultra-efficient assistant deployable on inexpensive hardware | Staying small, close to upstream, and friendly to constrained devices is the priority. |
| [**OpenClaw**](https://github.com/openclaw/openclaw) | TypeScript/Node.js | A polished, always-on personal assistant across devices and chat apps | You value its companion apps, voice, Canvas, channels, and extension ecosystem. |
| [**ZeroClaw**](https://github.com/zeroclaw-labs/zeroclaw) | Rust | Modular agent infrastructure with security profiles and selectable features | You want Rust, OS sandboxing, hardware peripherals, ACP, or its SOP engine. |
| [**Hermes Agent**](https://github.com/NousResearch/hermes-agent) | Python | An agent that learns reusable skills and user context over time | Skill evolution, conversation recall, rich terminal/desktop UX, or Nous Portal is central. |

## MintClaw And PicoClaw

MintClaw is a fork of PicoClaw, not a ground-up alternative. The projects share
a Go foundation and many familiar concepts around configuration, workspaces,
providers, channels, tools, and CLI operation. PicoClaw remains the upstream
source project; MintClaw is independently maintained and intentionally
divergent.

The main MintClaw divergence is not a longer integration checklist. It is a set
of explicit runtime contracts for work that outlives one model turn:

- **Durable tasks and delivery.** Spawned work has persisted status,
  completion records, deliverables, and parent/user delivery ownership. See
  [Spawn and async tasks](spawn-tasks.md) and
  [Async task delivery](../architecture/async-task-delivery.md).
- **Mid-run correction.** New user input can steer a running loop at defined
  boundaries. Pending calls are classified so unsafe, undispatched side
  effects can be skipped without hiding what happened from the model. See
  [Steering](../architecture/steering.md).
- **Durable human decisions.** Foreground turns and background tasks can pause
  for an authorized answer or approval, release runtime resources, survive a
  process restart, and resume the bound tool call once. See
  [Durable human interaction](human-interaction.md).
- **Restart-aware state.** Inbound messages, sessions, interactions, async
  tasks, and node operations have documented source-of-truth and recovery
  boundaries. Start with [Durable ingress](../architecture/durable-ingress.md),
  [Session system](../architecture/session-system.md), and
  [Safe restart and deploy](../architecture/safe-restart-and-deploy.md).
- **Policy-constrained remote machines.** `mintclaw-node` is a separate slim
  companion with explicit pairing, gateway and node-local policy, approvals,
  durable operation status, and conservative replay behavior. See the
  [Node companion guide](node-companion.md).
- **Bounded context and objectives.** Seahorse manages prompt assembly and
  retrieval under explicit budgets, while durable session goals remain
  separate from chat history and task execution. See the
  [Session guide](session-guide.md) and
  [Session goals](../architecture/session-goals.md).

That divergence has costs. MintClaw has a larger behavioral and operational
surface to understand, and its configuration and runtime semantics can move
away from PicoClaw. MintClaw also does not claim PicoClaw's published memory or
boot-time figures; use measurements from the exact build and workload you plan
to deploy.

Choose PicoClaw when upstream compatibility and constrained-device efficiency
matter more than MintClaw's additional workflow contracts. Choose MintClaw when
those contracts are the reason you are deploying an agent in the first place.

Official source: [PicoClaw README](https://github.com/sipeed/picoclaw).

## MintClaw And OpenClaw

OpenClaw describes itself as a local, always-on personal assistant. Its public
experience emphasizes broad chat reach, native companion apps, voice, and a
live Canvas, backed by a large Node.js project and extension ecosystem.

MintClaw does not try to promise parity with that application ecosystem. It is
a Go runtime whose differentiating work is concentrated in durable execution,
delivery ownership, steering, restart recovery, and policy-constrained remote
operations.

Choose OpenClaw when its device experience, native apps, Canvas, voice, or
existing plugins are the deciding factors. Choose MintClaw when you prefer its
Go lineage and need its specific workflow and operator-control semantics.

Official sources: [OpenClaw README](https://github.com/openclaw/openclaw) and
[OpenClaw website](https://openclaw.ai/).

## MintClaw And ZeroClaw

ZeroClaw is a Rust agent runtime built around modular crates and traits. Its
current documentation highlights feature-selected builds, supervised risk
profiles, OS-level sandbox options, cryptographic tool receipts, hardware
peripherals, ACP integration, and an event-triggered SOP engine. Current
releases also include multi-agent and durable task/run control planes.

Because ZeroClaw and MintClaw both cover multi-agent and durable work, a simple
feature checklist is misleading. The practical distinction is architectural:
ZeroClaw centers a modular Rust platform with risk profiles, sandboxing,
hardware, and SOPs; MintClaw centers the PicoClaw-derived Go runtime and its
specific task, delivery, interaction, restart, and node-operation contracts.

Choose ZeroClaw when its Rust architecture, selectable build surface,
sandboxing model, hardware integration, ACP support, or SOP workflow is the
better fit. Choose MintClaw when its Go ecosystem and documented durability
semantics match the system you need to operate.

Official sources: [ZeroClaw README](https://github.com/zeroclaw-labs/zeroclaw)
and [ZeroClaw releases](https://github.com/zeroclaw-labs/zeroclaw/releases).

## MintClaw And Hermes Agent

Hermes Agent describes its core value as learning from use: creating and
improving skills, preserving knowledge, searching past conversations, and
building a richer user model across sessions. It provides a terminal UI,
messaging gateway, native desktop application, broad model support, and an
optional Nous Portal path that bundles models and hosted tools.

MintClaw has skills, memory, model routing, chat gateways, and a Web launcher,
but it does not position itself around an automatic self-improvement loop or
Nous's integrated service. Its distinguishing contracts are durable workflow
state, explicit delivery and recovery behavior, steerable execution, durable
human interaction, and paired node operations.

Choose Hermes when learning-oriented memory and skill evolution, its polished
terminal/desktop experience, or Nous Portal are central. Choose MintClaw when a
Go runtime and its operator-visible workflow lifecycle are the stronger fit.

Official sources: [Hermes Agent README](https://github.com/NousResearch/hermes-agent)
and [Hermes Agent documentation](https://hermes-agent.nousresearch.com/docs/).

## A Better Evaluation Than Feature Counting

Before choosing a runtime, test the behavior that will matter after the demo:

1. Interrupt a multi-tool turn and verify which side effects still occur.
2. Restart during a background task and during a human approval wait.
3. Disconnect a remote executor immediately after dispatch and inspect how an
   uncertain outcome is represented.
4. Route completion to a parent agent and directly to a chat conversation.
5. Run the same representative workload on the actual target hardware and
   measure memory, startup time, latency, and storage growth.
6. Review sender policy, tool approval, secret storage, network exposure, and
   recovery procedures for the intended deployment.

For MintClaw, continue with [Doctor](../reference/doctor.md),
[Tools configuration](../reference/tools_configuration.md), and the
[Architecture index](../architecture/README.md).
