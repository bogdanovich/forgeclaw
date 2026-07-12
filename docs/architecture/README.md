# Architecture

Internal architecture notes for major runtime mechanisms and subsystem design.

- [Steering](steering.md): injecting messages into a running agent loop between tool calls.
- [AgentLoop Runtime Host](agentloop-runtime.md): AgentLoop/Pipeline split, inbound scheduling, session claims, recovery, and intentional coupling.
- [Async Task Delivery](async-task-delivery.md): durable task/completion/delivery model, deliverables, and current source-of-truth boundaries.
- [SubTurn Mechanism](subturn.md): sub-agent coordination, concurrency control, and lifecycle handling.
- [Subagent Model Policy](subagent-model-policy.md): child-run model selection, inherited session override modes, and precedence.
- [Session System](session-system.md): session scope allocation, JSONL persistence, alias compatibility, and migration.
- [Session Goals](session-goals.md): durable per-conversation objectives, command and tool interfaces, prompt injection, and reset semantics.
- [Routing System](routing-system.md): agent dispatch, session policy selection, and light/heavy model routing.
- [Durable Ingress](durable-ingress.md): normalized inbound message spool and restart replay semantics.
- [Safe Restart And Deploy](safe-restart-and-deploy.md): bounded restart/deploy handoff, shared binary targets, and durability boundaries.
- [Inbound Message Relations](inbound-message-relations.md): explicit relation typing for replies, adjacent follow-ups, media-only turns, and platform-native grouping.
- [Runtime Events](runtime-events.md): runtime event envelope, centralized event logging, filters, and examples.
- [Channel Lifecycle](channel-lifecycle.md): conservative channel reload policy, delivery ownership invariants, and the roadmap for any future hot-replacement work.
- [Workspace Temp Directory](workspace-temp.md): standard scratch path, `PICOCLAW_WORKSPACE_TMP`, and where temporary files should go.
- [Shellguard](shellguard.md): reusable shell command validation, command classification, permission modes, and path-scope limits.
- [Tool-Loop Stagnation Protection](tool-loop-stagnation.md): warning-first repeated failure and read-only no-progress detection with hash-safe state and events.
- [Agent Self-Evolution](agent-self-evolution.md): learning records, draft generation, application modes, and state layout.
- [Hook System Guide](hooks/README.md): current hook architecture and protocol details.
