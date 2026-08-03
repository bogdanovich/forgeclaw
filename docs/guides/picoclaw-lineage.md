# MintClaw And PicoClaw

MintClaw is a downstream fork of
[PicoClaw](https://github.com/sipeed/picoclaw), not a ground-up alternative.
The projects share a Go foundation and many familiar concepts around
configuration, workspaces, providers, channels, tools, and CLI operation.

PicoClaw remains the upstream source project. MintClaw is independently
maintained and intentionally divergent, so it should be treated as its own
runtime rather than as a drop-in PicoClaw build.

## Why MintClaw Forked

MintClaw's divergence is not primarily a longer integration checklist. It is a
set of explicit runtime contracts for work that outlives one model turn:

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

## The Trade-Off

MintClaw has a larger behavioral and operational surface to understand, and
its configuration and runtime semantics can move away from PicoClaw. MintClaw
also does not claim PicoClaw's published memory or boot-time figures; measure
the exact build and workload you plan to deploy.

Choose PicoClaw when upstream compatibility and constrained-device efficiency
matter more than MintClaw's additional workflow contracts. Choose MintClaw when
those contracts are the reason you are deploying an agent in the first place.

The authoritative source for current upstream behavior and requirements is the
[PicoClaw README](https://github.com/sipeed/picoclaw).
